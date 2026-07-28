"""The Explore Data node's worker-backed compute path.

Mirrors `test_catalog_visualizations.py`'s dispatch tests: the worker is patched
out, and what is asserted is the source descriptor core emits.
"""
from unittest.mock import patch

import pytest

from flowfile_core.flowfile.analytics import node_viz
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.schemas import input_schema

from .test_analytics_processor import (
    add_manual_input,
    add_node_promise_on_type,
    create_graph,
)

_AGG_PAYLOAD = {
    "workflow": [
        {"type": "view", "query": [{"op": "aggregate", "groupBy": ["g"],
                                    "measures": [{"field": "v", "agg": "sum", "asFieldKey": "v_sum"}]}]}
    ]
}


def _run_explore_flow():
    graph = create_graph()
    add_manual_input(graph, data=FlowDataEngine.create_random(50).to_raw_data())
    add_node_promise_on_type(graph, "explore_data", 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.run_graph()
    return graph, graph.get_node(2)


@pytest.fixture
def fake_worker():
    """Capture what core ships to the worker instead of calling it."""
    captured = {}

    def fake_resolve(table_id, plan_bytes, source_versions_hash, target="virtual_results", cache_key=None):
        captured["resolve"] = {
            "table_id": table_id,
            "hash": source_versions_hash,
            "cache_key": cache_key,
            "plan_len": len(plan_bytes),
        }
        return {"ipc_path": f"{cache_key}-{source_versions_hash[:16]}.arrow", "mtime": 1234.5, "row_count": 50}

    def fake_query(worker_source, payload, max_rows):
        captured["source"] = worker_source
        captured["payload"] = payload
        captured["max_rows"] = max_rows
        return {"rows": [], "total_rows": 0, "truncated": False, "elapsed_ms": 0.0, "cache_hit": False}

    def fake_fields(worker_source):
        captured["fields_source"] = worker_source
        return {"fields": [{"fid": "g"}], "cache_hit": False}

    with (
        patch.object(node_viz, "trigger_resolve_virtual_table", side_effect=fake_resolve),
        patch.object(node_viz, "trigger_visualize_query", side_effect=fake_query),
        patch.object(node_viz, "trigger_visualize_fields", side_effect=fake_fields),
    ):
        yield captured


def test_compute_emits_an_ipc_path_source(fake_worker):
    graph, node = _run_explore_flow()

    resp = node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=1000)

    assert resp.error is None
    source = fake_worker["source"]
    assert source["kind"] == "ipc_path"
    # Keyed on the versions hash, not the file mtime — mtime is second-resolution.
    assert source["session_key"] == f"node:{graph.flow_id}:2:{fake_worker['resolve']['hash'][:16]}"
    assert source["ipc_path"].startswith("node-1-2-")
    assert fake_worker["resolve"]["cache_key"] == "node-1-2"
    assert fake_worker["resolve"]["plan_len"] > 0, "core must ship a serialised plan, not data"
    assert fake_worker["payload"] is _AGG_PAYLOAD, "the GW payload is passed through opaquely"


def test_fields_uses_the_same_source(fake_worker):
    graph, node = _run_explore_flow()

    node_viz.get_node_fields(graph, node)

    assert fake_worker["fields_source"]["kind"] == "ipc_path"
    assert fake_worker["fields_source"]["session_key"].startswith(f"node:{graph.flow_id}:2:")


def test_rerunning_the_flow_invalidates_the_materialised_file(fake_worker):
    """A serialised plan's hash doesn't move when its data does — the run token must."""
    graph, node = _run_explore_flow()
    node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)
    first_hash = fake_worker["resolve"]["hash"]

    first_session_key = fake_worker["source"]["session_key"]

    # Sub-second: two runs inside the same second must still invalidate.
    graph.latest_run_info.start_time = graph.latest_run_info.start_time.replace(microsecond=999_999)
    node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)

    assert fake_worker["resolve"]["hash"] != first_hash
    assert fake_worker["source"]["session_key"] != first_session_key
    assert fake_worker["resolve"]["cache_key"] == "node-1-2", "the stem is stable across runs"


def test_compute_refuses_a_node_that_has_not_run(fake_worker):
    graph = create_graph()
    add_manual_input(graph, data=FlowDataEngine.create_random(10).to_raw_data())
    add_node_promise_on_type(graph, "explore_data", 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    with pytest.raises(node_viz.NodeNotRunError):
        node_viz.compute_node_rows(graph, graph.get_node(2), _AGG_PAYLOAD, max_rows=None)
    assert "resolve" not in fake_worker, "an un-run node must not trigger a materialisation"


@pytest.mark.parametrize(
    "requested,expected",
    [(None, 100_000), (0, 100_000), (-5, 100_000), (500, 500), (10_000_000, 500_000)],
)
def test_max_rows_is_clamped_to_the_catalog_limits(requested, expected):
    assert node_viz.clamp_max_rows(requested) == expected
