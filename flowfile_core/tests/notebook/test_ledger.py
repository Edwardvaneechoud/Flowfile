"""The exactness ledger (plan sections 3 item 5 and 7): render, clean run, relabel and compare over the corpus.

For every corpus flow ``g``: ``R = render(g)`` raises nothing and its placeholders stay inside the manifest;
the cells run as a clean run in notebook mode (in process), which writes nothing and starts no kernel
manager; every canvas node is looked up on the relabelled result and graded EXACT (same type, settings
equal under :mod:`flowfile_core.notebook.compare`), DIFFER (same type, settings differ) or LOSSY (a
different type, a missing node, or new nodes in its cell). Rows aggregate worst-case per node type into
the committed ``ledger.json``, which is written when absent and may only improve afterwards (set
``FLOWFILE_UPDATE_NOTEBOOK_LEDGER=1`` to record an improvement). A flow whose rows are all EXACT must
also render back to the same cell text.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.notebook.compare import parameters_equal, settings_equal
from flowfile_core.notebook.render import NotebookRendering, render
from flowfile_frame import notebook
from flowfile_frame.notebook_cells import clean_run, seed_session
from shared.storage_config import storage
from tests.notebook.conftest import no_kernel_manager

LEDGER_FILE = Path(__file__).parent / "ledger.json"
RANK = {"EXACT": 0, "DIFFER": 1, "LOSSY": 2}


def _files() -> set[Path]:
    return {
        path
        for path in Path(storage.base_directory).rglob("*")
        if path.is_file()
        and not any(part.endswith("logs") or part == "__pycache__" for part in path.parts)
        and ".db" not in path.name
    }


def _payload(graph: FlowGraph) -> dict:
    return graph.get_flowfile_data().model_dump(mode="json")


def _clean_run(graph: FlowGraph, rendering: NotebookRendering) -> dict:
    """Clean-run the rendered cells; the session is seeded from ``graph`` first only when ``fl.canvas_node`` needs it.

    Seeding rebuilds the canvas graph, which runs schema prediction (a pivot caches its input), so it
    is kept out of the no-write check unless a placeholder cell needs the snapshot.
    """
    payload = _payload(graph)
    parameters = list(graph.flow_settings.parameters)
    placeholders = any(cell.status != "code" for cell in rendering.cells)
    if placeholders:
        seed_session(payload, parameters, {}, {})
    try:
        cells = [(cell.cell_id, cell.code) for cell in rendering.cells]
        provenance = {
            cell.cell_id: [(graph.get_node(node_id).node_type, node_id) for node_id in cell.node_ids]
            for cell in rendering.cells
            if cell.node_ids
        }
        ceiling = max((node.node_id for node in graph.nodes), default=0)
        return clean_run(cells, ceiling, provenance)
    finally:
        notebook.exit()


def _canvas_schemas(graph: FlowGraph) -> dict[int, dict[str, list[dict]]]:
    """Per-node, per-handle schemas of the canvas graph, as the session seed message carries them (plan 2.6)."""
    schemas = {}
    for node in graph.nodes:
        named = getattr(node, "_named_schemas", None) or {}
        if named:
            schemas[node.node_id] = {
                handle: [{"name": c.column_name, "data_type": c.data_type} for c in columns]
                for handle, columns in named.items()
            }
    return schemas


def _rerender(graph: FlowGraph, result: dict) -> NotebookRendering:
    """Render the relabelled clean-run payload as a graph, rebuilt the way a session seeds it."""
    payload = copy.deepcopy(result["flowfile_data"])
    bound = seed_session(payload, payload["flowfile_settings"]["parameters"], {}, _canvas_schemas(graph))
    try:
        return render(bound["flow"])
    finally:
        notebook.exit()


def grade(graph: FlowGraph, result: dict) -> dict[int, str]:
    """Per canvas node: EXACT, DIFFER or LOSSY against the relabelled clean-run payload."""
    rebuilt = {node["id"]: node for node in result["flowfile_data"]["nodes"]}
    canvas = {node["id"]: node for node in _payload(graph)["nodes"]}
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


@pytest.fixture(scope="module")
def ledger_rows(notebook_corpus, expected_placeholders):
    """``{flow name: {node_id: (node_type, grade, detail)}}`` plus the rendering checks, computed once."""
    before = _files()
    flows = {}
    with no_kernel_manager() as kernel_calls:
        for name, graph in notebook_corpus:
            rendering = render(graph)
            placeholders = sorted(n for cell in rendering.cells if cell.status != "code" for n in cell.node_ids)
            result = _clean_run(graph, rendering)
            flows[name] = {"rendering": rendering, "placeholders": placeholders, "result": result, "graph": graph}
    flows["__files__"] = _files() - before
    flows["__kernel_calls__"] = list(kernel_calls)
    return flows


def _flows(ledger_rows):
    return {name: flow for name, flow in ledger_rows.items() if not name.startswith("__")}


def test_placeholders_stay_inside_the_manifest(ledger_rows, expected_placeholders):
    for name, flow in _flows(ledger_rows).items():
        assert set(flow["placeholders"]) <= set(expected_placeholders[name]), (name, flow["placeholders"])


def test_every_flow_clean_runs(ledger_rows):
    flows = _flows(ledger_rows).items()
    failed = {name: flow["result"].get("error") for name, flow in flows if not flow["result"]["ok"]}
    assert not failed, json.dumps(failed, indent=1)


def test_clean_runs_write_nothing_and_start_no_kernel(ledger_rows):
    assert ledger_rows["__files__"] == set()
    assert ledger_rows["__kernel_calls__"] == []


def test_clean_runs_keep_the_parameters(ledger_rows):
    for name, flow in _flows(ledger_rows).items():
        if flow["result"]["ok"]:
            params = flow["result"]["flowfile_data"]["flowfile_settings"]["parameters"]
            assert parameters_equal(flow["graph"].flow_settings.parameters, params), name


def _grades(ledger_rows) -> dict[str, dict[int, tuple[str, str]]]:
    out = {}
    for name, flow in _flows(ledger_rows).items():
        graph = flow["graph"]
        if not flow["result"]["ok"]:
            out[name] = {node.node_id: (node.node_type, "LOSSY") for node in graph.nodes}
            continue
        out[name] = {nid: (graph.get_node(nid).node_type, g) for nid, g in grade(graph, flow["result"]).items()}
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
    """Plan 2b's done-when: the corpus-wide row of every node type the demo uses is at least DIFFER.

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
        if not flow["result"]["ok"] or any(status != "EXACT" for _, status in grades[name].values()):
            continue
        again = _rerender(flow["graph"], flow["result"])
        before = {c.cell_id: c.code for c in flow["rendering"].cells}
        after = {c.cell_id: c.code for c in again.cells}
        keys = sorted(set(before) | set(after))
        diff = {k: (before.get(k), after.get(k)) for k in keys if before.get(k) != after.get(k)}
        if diff:
            unstable[name] = diff
    assert not unstable, json.dumps(unstable, indent=1)
