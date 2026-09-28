"""The notebook-surface milestone (plan section 7) over the committed corpus.

For every corpus flow: render it, clean-run the cells through the in-process runner seeded from the canvas
(the push path's snapshot), and reconcile against the canvas. A flow with no LOSSY node reconciles to no
operations; a flow whose every node is EXACT does so even when every cell counts as edited. A one-cell edit
pushed through ``POST /editor/notebook/push/`` touches exactly that cell's nodes (plus reconnects), keeps the
hash of every node that is neither edited nor downstream of the edit, and keeps every downstream node's settings.
"""

from __future__ import annotations

import json
import re

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.notebook import bridge
from flowfile_core.notebook.push import live_cells, seed_snapshot
from flowfile_core.notebook.reconcile import reconcile
from flowfile_core.notebook.render import render
from tests.notebook.test_ledger import grade

OWNER_ID = 1
_COMPARISON = re.compile(r"(>=|<=|>|<|==)\s*(\d+)")


def _provenance(graph, rendering) -> dict[str, list[tuple[str, int]]]:
    return {
        cell.cell_id: [(graph.get_node(n).node_type, n) for n in cell.node_ids]
        for cell in rendering.cells
        if cell.node_ids
    }


def _clean_run(graph, cells, provenance) -> bridge.CleanRunResult:
    ceiling = max((node.node_id for node in graph.nodes), default=0)
    request = bridge.CleanRunRequest(cells=cells, provenance=provenance, ceiling=ceiling, snapshot=seed_snapshot(graph))
    return bridge.InProcessCleanRunner().clean_run(OWNER_ID, graph.flow_id, request)


def _reconcile(graph, result, provenance, changed):
    live = graph.get_flowfile_data().model_dump(mode="json")
    names = {n.node_id: r for n in graph.nodes if (r := getattr(n.setting_input, "node_reference", None))}
    return reconcile(
        live,
        result.flowfile_data,
        changed,
        result.node_ids_by_cell,
        names,
        result.names,
        live_cells=live_cells(provenance),
    )


@pytest.fixture(scope="module")
def milestone(notebook_corpus):
    """``{name: (graph, rendering, provenance, clean-run result, grades)}`` computed once."""
    rows = {}
    for name, graph in notebook_corpus:
        rendering = render(graph)
        provenance = _provenance(graph, rendering)
        result = _clean_run(graph, [(c.cell_id, c.code) for c in rendering.cells], provenance)
        grades = (
            {}
            if result.error
            else grade(graph, {"flowfile_data": result.flowfile_data, "cells": result.node_ids_by_cell})
        )
        rows[name] = (graph, rendering, provenance, result, grades)
    return rows


def test_every_corpus_flow_clean_runs_through_the_runner(milestone):
    failed = {name: row[3].error for name, row in milestone.items() if row[3].error}
    assert not failed, json.dumps(failed, indent=1)


def test_flows_without_a_lossy_node_reconcile_to_no_ops(milestone):
    stray = {}
    for name, (graph, _, provenance, result, grades) in milestone.items():
        if result.error or "LOSSY" in grades.values():
            continue
        plan = _reconcile(graph, result, provenance, changed=[])
        if plan.operations:
            stray[name] = [op.model_dump(mode="json") for op in plan.operations]
    assert not stray, json.dumps(stray, indent=1, default=str)


def test_exact_flows_reconcile_to_no_ops_with_every_cell_edited(milestone):
    stray = {}
    for name, (graph, rendering, provenance, result, grades) in milestone.items():
        if result.error or set(grades.values()) != {"EXACT"}:
            continue
        plan = _reconcile(graph, result, provenance, changed=[c.cell_id for c in rendering.cells])
        if plan.operations:
            stray[name] = [op.model_dump(mode="json") for op in plan.operations]
    assert not stray, json.dumps(stray, indent=1, default=str)


def _editable_cell(graph, rendering):
    """The first code cell holding one filter node and an integer comparison to bump, with that filter's id."""
    for cell in rendering.cells:
        filters = [n for n in cell.node_ids if graph.get_node(n).node_type == "filter"]
        if cell.status == "code" and len(filters) == 1 and ".filter(" in cell.code and _COMPARISON.search(cell.code):
            return cell, filters[0]
    return None, None


def _bump(code: str) -> str:
    return _COMPARISON.sub(lambda m: f"{m.group(1)} {int(m.group(2)) + 1}", code, count=1)


def _downstream(graph, node_id) -> set[int]:
    return set(graph.get_node(node_id).get_all_dependent_node_ids())


@pytest.fixture
def client():
    user = PydanticUser(username="nb_milestone", id=OWNER_ID, disabled=False, is_admin=True)
    main.app.dependency_overrides[get_current_active_user] = lambda: user
    main.app.dependency_overrides[get_current_user] = lambda: user
    before_runner = bridge._runner
    bridge.set_clean_runner(bridge.InProcessCleanRunner())
    yield TestClient(main.app)
    bridge.set_clean_runner(before_runner)
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)


def test_one_cell_edit_touches_only_that_cells_nodes(milestone, client, tmp_path):
    edited = 0
    for name, (graph, _, _, result, grades) in milestone.items():
        if result.error or "LOSSY" in grades.values():
            continue
        path = tmp_path / f"{name}.yaml"
        graph.save_flow(str(path))
        live = open_flow(path)
        rendering = render(live)
        cell, node_id = _editable_cell(live, rendering)
        if cell is None:
            continue
        flow_file_handler._flows[live.flow_id] = live
        flow_file_handler._register_user_session(OWNER_ID, live.flow_id)
        try:
            before = {n.node_id: (n.hash, n.setting_input.model_dump(mode="json")) for n in live.nodes}
            downstream = _downstream(live, node_id)
            body = {
                "flow_id": live.flow_id,
                "cells": [[c.cell_id, _bump(c.code) if c.cell_id == cell.cell_id else c.code] for c in rendering.cells],
                "changed_cell_ids": [cell.cell_id],
                "provenance": _provenance(live, rendering),
                "code_fingerprint": rendering.code_fingerprint,
                "client_max_node_id": max(before),
            }
            plan = client.post("/notebook/plan", json=body)
            assert plan.status_code == 200, (name, plan.text)
            ops = plan.json()["operations"]
            assert ops, name
            for op in ops:
                touched = {op.get("node_id"), (op.get("settings") or {}).get("node_id")}
                if "connection" in op:
                    touched.add(op["connection"]["input_connection"]["node_id"])
                assert node_id in touched, (name, op)
            pushed = client.post("/editor/notebook/push/", json=body)
            assert pushed.status_code == 200, (name, pushed.text)
            for nid, (node_hash, node_settings) in before.items():
                if nid == node_id:
                    continue
                node = live.get_node(nid)
                if nid in downstream:
                    assert node.setting_input.model_dump(mode="json") == node_settings, (name, nid)
                else:
                    assert node.hash == node_hash, (name, nid)
            edited += 1
        finally:
            flow_file_handler._unregister_user_session(OWNER_ID, live.flow_id)
            flow_file_handler.delete_flow(live.flow_id)
    assert edited >= 3, edited
