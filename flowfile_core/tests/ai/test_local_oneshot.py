"""Tests for one-shot local-model flow generation.

Covers JSON extraction, the topological insertion planner, and end-to-end
staging against a real (empty) ``FlowGraph`` — no network, no model.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from flowfile_core.ai import diff
from flowfile_core.ai.local_model import oneshot
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import schemas


@pytest.fixture(autouse=True)
def _reset_diff_store() -> Iterator[None]:
    diff.clear_for_tests()
    yield
    diff.clear_for_tests()


def _empty_flow(flow_id: int = 1) -> FlowGraph:
    return FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=flow_id,
            execution_mode="Performance",
            execution_location="local",
            path="/tmp/test_oneshot",
        ),
        name="oneshot_test",
    )


# --------------------------------------------------------------------------- #
# extract_flow_json                                                           #
# --------------------------------------------------------------------------- #


def test_extract_flow_json_direct():
    spec = oneshot.extract_flow_json('{"nodes":[{"id":"n1","type":"read"}],"edges":[]}')
    assert spec["nodes"][0]["type"] == "read"


def test_extract_flow_json_fenced():
    text = 'Here you go:\n```json\n{"nodes":[{"id":"a","type":"filter"}],"edges":[]}\n```\nDone.'
    spec = oneshot.extract_flow_json(text)
    assert spec["nodes"][0]["type"] == "filter"


def test_extract_flow_json_rejects_garbage():
    with pytest.raises(oneshot.OneShotError):
        oneshot.extract_flow_json("sorry, I cannot help with that")


# --------------------------------------------------------------------------- #
# _plan_insertions                                                            #
# --------------------------------------------------------------------------- #


def test_plan_insertions_linear_toposort_and_ids():
    spec = {
        "nodes": [
            {"id": "c", "type": "sort", "settings": {}},
            {"id": "a", "type": "read", "settings": {}},
            {"id": "b", "type": "filter", "settings": {}},
        ],
        "edges": [{"source": "a", "target": "b"}, {"source": "b", "target": "c"}],
    }
    by_type = {p.node_type: p for p in oneshot._plan_insertions(spec, start_id=5)}
    assert by_type["read"].node_id == 5
    assert by_type["filter"].node_id == 6
    assert by_type["sort"].node_id == 7
    assert by_type["read"].upstream_ids == []
    assert by_type["filter"].upstream_ids == [5]
    assert by_type["sort"].upstream_ids == [6]
    assert by_type["filter"].pos_x > by_type["read"].pos_x


def test_plan_insertions_join_splits_right_input():
    spec = {
        "nodes": [
            {"id": "l", "type": "read", "settings": {}},
            {"id": "r", "type": "read", "settings": {}},
            {"id": "j", "type": "join", "settings": {}},
        ],
        "edges": [{"source": "l", "target": "j"}, {"source": "r", "target": "j"}],
    }
    j = next(p for p in oneshot._plan_insertions(spec, start_id=1) if p.node_type == "join")
    assert len(j.upstream_ids) == 1
    assert j.right_input_id is not None
    assert j.right_input_id != j.upstream_ids[0]


# --------------------------------------------------------------------------- #
# _stage_flow (integration against a real flow)                               #
# --------------------------------------------------------------------------- #


def test_stage_flow_builds_diff():
    flow = _empty_flow()
    spec = {
        "nodes": [
            {
                "id": "a",
                "type": "manual_input",
                "settings": {
                    "raw_data_format": {
                        "columns": [
                            {"name": "status", "data_type": "String"},
                            {"name": "amount", "data_type": "Int64"},
                        ],
                        "data": [["paid", "open"], [100, 50]],
                    }
                },
            },
            {
                "id": "b",
                "type": "filter",
                "settings": {"filter_input": {"mode": "advanced", "advanced_filter": "[status] = 'paid'"}},
            },
            {"id": "c", "type": "sort", "settings": {"sort_input": [{"column": "amount", "how": "desc"}]}},
        ],
        "edges": [{"source": "a", "target": "b"}, {"source": "b", "target": "c"}],
    }
    result = oneshot._stage_flow(flow=flow, flow_id=1, user_id=1, spec=spec)
    assert result["op_count"] == 3, result["warnings"]
    graph_diff = diff.get_diff(result["diff_id"])
    assert graph_diff is not None
    assert [a.node_type for a in graph_diff.additions] == ["manual_input", "filter", "sort"]


def test_stage_flow_skips_writer_nodes():
    flow = _empty_flow()
    spec = {
        "nodes": [
            {
                "id": "a",
                "type": "manual_input",
                "settings": {
                    "raw_data_format": {"columns": [{"name": "x", "data_type": "Int64"}], "data": [[1, 2, 3]]}
                },
            },
            {
                "id": "z",
                "type": "output",
                "settings": {
                    "output_settings": {
                        "name": "out.csv",
                        "directory": ".",
                        "file_type": "csv",
                        "write_mode": "overwrite",
                        "table_settings": {"file_type": "csv"},
                    }
                },
            },
        ],
        "edges": [{"source": "a", "target": "z"}],
    }
    result = oneshot._stage_flow(flow=flow, flow_id=1, user_id=1, spec=spec)
    created_types = [c["type"] for c in result["created"]]
    assert "manual_input" in created_types
    assert "output" not in created_types
    assert any("output" in w for w in result["warnings"])
