"""Run over the committed corpus: every flow's rendered cells clean-run twice through the in-process runner,
seeded from the canvas (the push path's snapshot), with the same result both times."""

from __future__ import annotations

import json

from flowfile_core.notebook.bridge import CleanRunRequest, CleanRunResult
from flowfile_core.notebook.compare import normalise
from flowfile_core.notebook.push import seed_snapshot
from flowfile_core.notebook.render import render
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, InProcessCleanRunner


def _comparable(result: CleanRunResult) -> dict:
    """The result as a push compares it: node settings normalised, and without the id and name every clean run
    mints for its fresh session graph."""
    data = {k: v for k, v in result.flowfile_data.items() if k not in ("flowfile_id", "flowfile_name")}
    data["nodes"] = [
        {**node, "setting_input": normalise(node.get("setting_input"), node["type"])} for node in data.get("nodes", [])
    ]
    return {**result.model_dump(), "flowfile_data": data}


def test_every_corpus_flow_clean_runs_twice_with_the_same_result(notebook_corpus):
    runner = InProcessCleanRunner()
    failed: dict[str, list[str]] = {}
    for name, graph in notebook_corpus:
        rendering = render(graph)
        request = CleanRunRequest(
            cells=[(cell.cell_id, cell.code) for cell in rendering.cells],
            provenance={
                cell.cell_id: [(graph.get_node(node_id).node_type, node_id) for node_id in cell.node_ids]
                for cell in rendering.cells
                if cell.node_ids
            },
            ceiling=max((node.node_id for node in graph.nodes), default=0),
            snapshot=seed_snapshot(graph),
        )
        first, second = (runner.clean_run(NOTEBOOK_OWNER_ID, graph.flow_id, request) for _ in range(2))
        for attempt, result in ((1, first), (2, second)):
            if result.error:
                failed.setdefault(name, []).append(f"{attempt}: {result.error.splitlines()[-1]}")
        if not failed.get(name) and _comparable(first) != _comparable(second):
            failed[name] = ["the second clean run differs from the first"]
    assert not failed, json.dumps(failed, indent=1)
