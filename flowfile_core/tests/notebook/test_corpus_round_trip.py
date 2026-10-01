"""End-to-end notebook round trip (render, clean run, reconcile, push) over the committed corpus.

For every corpus flow and each runner (the production interpreting runner and the test-only ``exec`` one):
render it, clean-run the cells on a sync of the canvas (the push path's snapshot), and reconcile against the
canvas. A flow with no LOSSY node reconciles to no operations; a flow whose every node is EXACT does so even when
every cell counts as edited. A one-cell edit pushed through ``POST /editor/notebook/push/`` touches exactly that
cell's nodes (plus reconnects), keeps the hash of every node that is neither edited nor downstream of the edit,
and keeps every downstream node's settings. The two runners build the same flow: equal payloads up to what a run
mints, and node by node the same settings hash. Each runner clean-runs the corpus once per session
(``corpus_runs``), shared with the ledger.
"""

from __future__ import annotations

import copy
import json
import re

import pytest

from flowfile_core import flow_file_handler
from flowfile_core.flowfile.flow_node.flow_node import _settings_for_hash
from flowfile_core.flowfile.manage.io_flowfile import _flowfile_data_to_flow_information, open_flow
from flowfile_core.flowfile.utils import get_hash
from flowfile_core.notebook.push import live_cells
from flowfile_core.notebook.reconcile import reconcile
from flowfile_core.notebook.render import render
from flowfile_core.schemas import schemas
from tests.notebook.conftest import RUNNER_KINDS, cell_provenance, masked_payload
from tests.notebook.test_ledger import grade

OWNER_ID = 1
_COMPARISON = re.compile(r"(>=|<=|>|<|==)\s*(\d+)")


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


@pytest.fixture
def corpus_round_trips(corpus_runs, runner_kind):
    """``{name: (graph, rendering, provenance, clean-run result, grades)}`` of one runner's corpus pass."""
    rows = {}
    for name, run in corpus_runs[runner_kind].runs.items():
        result = run.result
        grades = (
            {}
            if result.error
            else grade(run.graph, {"flowfile_data": result.flowfile_data, "cells": result.node_ids_by_cell})
        )
        rows[name] = (run.graph, run.rendering, run.provenance, result, grades)
    return rows


def test_every_corpus_flow_clean_runs_through_the_runner(corpus_round_trips):
    failed = {name: row[3].traceback or row[3].error for name, row in corpus_round_trips.items() if row[3].error}
    assert not failed, json.dumps(failed, indent=1)


def test_flows_without_a_lossy_node_reconcile_to_no_ops(corpus_round_trips):
    stray = {}
    for name, (graph, _, provenance, result, grades) in corpus_round_trips.items():
        if result.error or "LOSSY" in grades.values():
            continue
        plan = _reconcile(graph, result, provenance, changed=[])
        if plan.operations:
            stray[name] = [op.model_dump(mode="json") for op in plan.operations]
    assert not stray, json.dumps(stray, indent=1, default=str)


def test_exact_flows_reconcile_to_no_ops_with_every_cell_edited(corpus_round_trips):
    stray = {}
    for name, (graph, rendering, provenance, result, grades) in corpus_round_trips.items():
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
def client(runner_kind, request, client_as):
    """A client whose pushes go through the runner of ``runner_kind``, installed for the test."""
    request.getfixturevalue("runner" if runner_kind == "interpreting" else "exec_runner")
    return client_as(OWNER_ID)


def test_one_cell_edit_touches_only_that_cells_nodes(corpus_round_trips, client, tmp_path):
    edited = 0
    for name, (graph, _, _, result, grades) in corpus_round_trips.items():
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
                "provenance": cell_provenance(live, rendering),
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


def _pinned(value, key=None):
    """``value`` with what a clean run mints fixed: the flow's id and name and every Python Script cell id."""
    if isinstance(value, dict):
        fixed = {"flow_id": 0, "flowfile_id": 0, "flowfile_name": "flow"}
        return {k: fixed[k] if k in fixed else _pinned(v, k) for k, v in value.items()}
    if isinstance(value, list):
        if key == "cells":
            return [{**_pinned(cell), "id": str(index)} for index, cell in enumerate(value)]
        return [_pinned(item) for item in value]
    return value


def _settings_hashes(flowfile_data: dict) -> dict[int, str]:
    """Per node, the settings hash ``FlowNode.calculate_hash`` folds in, of the model opening the payload builds."""
    payload = schemas.FlowfileData.model_validate(_pinned(copy.deepcopy(flowfile_data)))
    nodes = _flowfile_data_to_flow_information(payload).data
    return {node_id: get_hash(_settings_for_hash(node.setting_input)) for node_id, node in nodes.items()}


def test_both_runners_build_the_same_flow(corpus_runs):
    interpreted, executed = (corpus_runs[kind].runs for kind in RUNNER_KINDS)
    assert interpreted.keys() == executed.keys()
    failed = {
        name: run.result.error for runs in (interpreted, executed) for name, run in runs.items() if run.result.error
    }
    assert not failed, json.dumps(failed, indent=1)
    differ = {}
    for name, run in interpreted.items():
        mine, theirs = run.result, executed[name].result
        if masked_payload(mine.flowfile_data) != masked_payload(theirs.flowfile_data):
            differ[name] = "flowfile_data"
        elif (mine.node_ids_by_cell, mine.names, mine.refusals, mine.warnings) != (
            theirs.node_ids_by_cell,
            theirs.names,
            theirs.refusals,
            theirs.warnings,
        ):
            differ[name] = "cells, names, refusals or warnings"
        else:
            ours, exec_hashes = _settings_hashes(mine.flowfile_data), _settings_hashes(theirs.flowfile_data)
            assert ours, name
            if ours != exec_hashes:
                differ[name] = sorted(node_id for node_id in ours if ours[node_id] != exec_hashes.get(node_id))
    assert not differ, differ
