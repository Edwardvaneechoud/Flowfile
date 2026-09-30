"""The exactness ledger: render, clean run, relabel and compare over the corpus.

For every corpus flow ``g`` and each runner (the production interpreting runner and the test-only ``exec``
one): ``R = render(g)`` raises nothing and its placeholders stay inside the manifest; the cells clean-run on a
sync of the canvas (the push path's snapshot), which writes nothing and starts no kernel manager; every canvas
node is looked up on the relabelled result and graded EXACT (same type, settings equal under
:mod:`flowfile_core.notebook.compare`), DIFFER (same type, settings differ) or LOSSY (a different type, a
missing node, or new nodes in its cell). Rows aggregate worst-case per node type into the committed
``ledger.json``, which is written when absent and may only improve afterwards (set
``FLOWFILE_UPDATE_NOTEBOOK_LEDGER=1`` to record an improvement). A flow whose rows are all EXACT must also
render back to the same cell text. Each runner clean-runs the corpus once per session (``corpus_runs``), shared
with the round trip.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.notebook.bridge import CleanRunResult
from flowfile_core.notebook.compare import parameters_equal, settings_equal
from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import NotebookRendering, render
from flowfile_frame import notebook
from flowfile_frame.notebook_cells import seed_session
from tests.notebook.conftest import NOTEBOOK_OWNER_ID

LEDGER_FILE = Path(__file__).parent / "ledger.json"
RANK = {"EXACT": 0, "DIFFER": 1, "LOSSY": 2}


def _rerender(graph: FlowGraph, result: CleanRunResult) -> NotebookRendering:
    """Render the relabelled clean-run payload as a graph, rebuilt the way a session seeds it."""
    payload = copy.deepcopy(result.flowfile_data)
    bound = seed_session(
        payload,
        payload["flowfile_settings"]["parameters"],
        {},
        seed_snapshot(graph)["schemas"],
        user_id=NOTEBOOK_OWNER_ID,
    )
    try:
        return render(bound["flow"])
    finally:
        notebook.exit()


def grade(graph: FlowGraph, result: dict) -> dict[int, str]:
    """Per canvas node: EXACT, DIFFER or LOSSY against the relabelled clean-run payload."""
    rebuilt = {node["id"]: node for node in result["flowfile_data"]["nodes"]}
    canvas = {node["id"]: node for node in graph.get_flowfile_data().model_dump(mode="json")["nodes"]}
    cell_of = {node_id: cell_id for cell_id, node_ids in result["cells"].items() for node_id in node_ids}
    extra = {cell_id for cell_id, node_ids in result["cells"].items() if set(node_ids) - set(canvas)}
    grades = {}
    for node_id, node in canvas.items():
        twin = rebuilt.get(node_id)
        if twin is None or twin["type"] != node["type"] or cell_of.get(node_id) in extra:
            grades[node_id] = "LOSSY"
        elif not settings_equal(node["setting_input"], twin["setting_input"], node["type"]):
            grades[node_id] = "DIFFER"
        elif sorted(_inputs(node), key=str) != sorted(_inputs(twin), key=str):
            grades[node_id] = "DIFFER"
        else:
            grades[node_id] = "EXACT"
    return grades


def _inputs(node: dict) -> list[tuple]:
    ids = list(node.get("input_ids") or []) + [node.get("left_input_id"), node.get("right_input_id")]
    keyed = [(c["from_id"], c.get("input_handle")) for c in node.get("input_connections") or []]
    return [(i, None) for i in ids if i is not None] + keyed


def _worst(a: str | None, b: str) -> str:
    return b if a is None or RANK[b] > RANK[a] else a


@pytest.fixture
def ledger_rows(corpus_runs, runner_kind):
    """``{flow name: {rendering, placeholders, result, graph}}`` of one runner's corpus pass, plus what it wrote."""
    corpus_pass = corpus_runs[runner_kind]
    flows = {}
    for name, run in corpus_pass.runs.items():
        placeholders = sorted(n for cell in run.rendering.cells if cell.status != "code" for n in cell.node_ids)
        flows[name] = {
            "rendering": run.rendering,
            "placeholders": placeholders,
            "result": run.result,
            "graph": run.graph,
        }
    flows["__files__"] = corpus_pass.files
    flows["__kernel_calls__"] = corpus_pass.kernel_calls
    return flows


def _flows(ledger_rows):
    return {name: flow for name, flow in ledger_rows.items() if not name.startswith("__")}


def test_placeholders_stay_inside_the_manifest(ledger_rows, expected_placeholders):
    for name, flow in _flows(ledger_rows).items():
        assert set(flow["placeholders"]) <= set(expected_placeholders[name]), (name, flow["placeholders"])


def test_every_flow_clean_runs(ledger_rows):
    flows = _flows(ledger_rows).items()
    failed = {name: flow["result"].traceback or flow["result"].error for name, flow in flows if flow["result"].error}
    assert not failed, json.dumps(failed, indent=1)


def test_clean_runs_write_nothing_and_start_no_kernel(ledger_rows):
    assert ledger_rows["__files__"] == set()
    assert ledger_rows["__kernel_calls__"] == []


def test_clean_runs_keep_the_parameters(ledger_rows):
    for name, flow in _flows(ledger_rows).items():
        if not flow["result"].error:
            params = flow["result"].flowfile_data["flowfile_settings"]["parameters"]
            assert parameters_equal(flow["graph"].flow_settings.parameters, params), name


def _grades(ledger_rows) -> dict[str, dict[int, tuple[str, str]]]:
    out = {}
    for name, flow in _flows(ledger_rows).items():
        graph = flow["graph"]
        if flow["result"].error:
            out[name] = {node.node_id: (node.node_type, "LOSSY") for node in graph.nodes}
            continue
        payload = {"flowfile_data": flow["result"].flowfile_data, "cells": flow["result"].node_ids_by_cell}
        out[name] = {nid: (graph.get_node(nid).node_type, g) for nid, g in grade(graph, payload).items()}
    return out


def test_ledger_rows_only_improve(ledger_rows):
    ledger: dict[str, str] = {}
    for rows in _grades(ledger_rows).values():
        for node_type, status in rows.values():
            ledger[node_type] = _worst(ledger.get(node_type), status)
    ledger = dict(sorted(ledger.items()))
    if not LEDGER_FILE.exists() or os.environ.get("FLOWFILE_UPDATE_NOTEBOOK_LEDGER"):
        LEDGER_FILE.write_text(json.dumps(ledger, indent=2) + "\n")
    committed = json.loads(LEDGER_FILE.read_text())
    worse = {t: (committed.get(t), s) for t, s in ledger.items() if RANK[s] > RANK[committed.get(t, "LOSSY")]}
    assert not worse, f"ledger rows got worse (committed, now): {worse}"


def test_no_lossy_row_for_a_node_type_the_demo_uses(ledger_rows):
    """The corpus-wide row of every node type the demo uses is at least DIFFER.

    A canvas join that keeps its right keys rebuilds through ``join(..., keep_right_keys=True)`` and
    a Polars-code node with no input through ``fl.polars_code(fn)``.
    """
    grades = _grades(ledger_rows)
    demo_types = {node_type for node_type, _ in grades["demo"].values()}
    lossy = sorted(
        {t for rows in grades.values() for t, status in rows.values() if t in demo_types and status == "LOSSY"}
    )
    assert not lossy, f"LOSSY rows for node types the demo uses: {lossy}"


def test_demo_uses_no_lossy_node_type(ledger_rows):
    demo = _grades(ledger_rows)["demo"]
    lossy = sorted({node_type for node_type, status in demo.values() if status == "LOSSY"})
    assert not lossy, f"LOSSY node types in the demo: {lossy}"


def test_exact_flows_render_back_to_the_same_cells(ledger_rows):
    grades = _grades(ledger_rows)
    unstable = {}
    for name, flow in _flows(ledger_rows).items():
        if flow["result"].error or any(status != "EXACT" for _, status in grades[name].values()):
            continue
        again = _rerender(flow["graph"], flow["result"])
        before = {c.cell_id: c.code for c in flow["rendering"].cells}
        after = {c.cell_id: c.code for c in again.cells}
        keys = sorted(set(before) | set(after))
        diff = {k: (before.get(k), after.get(k)) for k in keys if before.get(k) != after.get(k)}
        if diff:
            unstable[name] = diff
    assert not unstable, json.dumps(unstable, indent=1)
