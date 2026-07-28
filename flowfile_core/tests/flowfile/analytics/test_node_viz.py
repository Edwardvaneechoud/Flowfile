"""The Explore Data node's worker-backed compute path.

Mirrors `test_catalog_visualizations.py`'s dispatch tests: the worker is patched
out, and what is asserted is the source descriptor core emits.
"""
import io
from base64 import b64decode
from unittest.mock import patch

import polars as pl
import pytest

from flowfile_core.flowfile.analytics import node_viz
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import (
    subprocess_operations as subprocess_operations_module,
)
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

    def fake_query(worker_source, payload, max_rows):
        captured["source"] = worker_source
        captured["payload"] = payload
        captured["max_rows"] = max_rows
        return {"rows": [], "total_rows": 0, "truncated": False, "elapsed_ms": 0.0, "cache_hit": False}

    def fake_fields(worker_source):
        captured["fields_source"] = worker_source
        return {"fields": [{"fid": "g"}], "cache_hit": False}

    with (
        patch.object(node_viz, "trigger_visualize_query", side_effect=fake_query),
        patch.object(node_viz, "trigger_visualize_fields", side_effect=fake_fields),
        patch.object(subprocess_operations_module, "trigger_resolve_virtual_table") as resolve_mock,
    ):
        captured["_resolve_mock"] = resolve_mock
        yield captured


def test_compute_ships_the_plan_and_never_materialises(fake_worker):
    """The regression this path was rewritten for: no IPC file, ever.

    Materialising a node's result cost 4.9 GB of uncompressed Arrow for a 294 MB
    Parquet source, to answer queries Polars serves off the Parquet in ~0.04 s.
    """
    graph, node = _run_explore_flow()

    resp = node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=1000)

    assert resp.error is None
    source = fake_worker["source"]
    assert source["kind"] == "plan"
    assert "ipc_path" not in source and "mtime" not in source
    assert fake_worker["_resolve_mock"].call_count == 0, "node viz must never materialise to IPC"
    assert fake_worker["payload"] is _AGG_PAYLOAD, "the GW payload is passed through opaquely"

    # The plan must round-trip back into a LazyFrame the worker child can hold.
    restored = pl.LazyFrame.deserialize(io.BytesIO(b64decode(source["plan_bytes"])))
    assert restored.collect_schema().names() == node.get_resulting_data().columns


def test_fields_uses_the_same_source(fake_worker):
    graph, node = _run_explore_flow()

    node_viz.get_node_fields(graph, node)
    fields_key = fake_worker["fields_source"]["session_key"]
    node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)

    assert fake_worker["fields_source"]["kind"] == "plan"
    assert fake_worker["source"]["session_key"] == fields_key, "fields and compute share one child"


def test_session_key_is_content_addressed_and_stable(fake_worker):
    """Repeated drawer opens must reuse one warm child, so the key can't drift."""
    graph, node = _run_explore_flow()

    node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)
    first = fake_worker["source"]["session_key"]
    node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)
    assert fake_worker["source"]["session_key"] == first

    # flow_id is regenerated on every flow open, so it must not be key material.
    graph.flow_id = graph.flow_id + 1
    node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)
    assert fake_worker["source"]["session_key"] == first


def test_rerunning_the_flow_rotates_the_session_key(fake_worker):
    """A serialised plan's hash doesn't move when its data does — the run token must."""
    graph, node = _run_explore_flow()
    node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)
    first = fake_worker["source"]["session_key"]

    # Sub-second: two runs inside the same second must still rotate the key.
    graph.latest_run_info.start_time = graph.latest_run_info.start_time.replace(microsecond=999_999)
    node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)

    assert fake_worker["source"]["session_key"] != first


def test_cloud_backed_plans_are_refused(fake_worker):
    """Polars inlines storage_options, so a cloud plan carries live credentials."""
    graph, node = _run_explore_flow()
    cloud_plan = pl.scan_parquet("s3://bucket/secret.parquet").serialize()

    with patch.object(node.results.resulting_data.data_frame, "serialize", return_value=cloud_plan):
        with pytest.raises(node_viz.CloudPlanNotVisualizableError):
            node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)

    assert "source" not in fake_worker, "nothing may reach the worker once refused"


def test_compute_refuses_a_node_that_has_not_run(fake_worker):
    graph = create_graph()
    add_manual_input(graph, data=FlowDataEngine.create_random(10).to_raw_data())
    add_node_promise_on_type(graph, "explore_data", 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    with pytest.raises(node_viz.NodeNotRunError):
        node_viz.compute_node_rows(graph, graph.get_node(2), _AGG_PAYLOAD, max_rows=None)
    assert "source" not in fake_worker


@pytest.mark.parametrize(
    "requested,expected",
    [(None, 100_000), (0, 100_000), (-5, 100_000), (500, 500), (10_000_000, 500_000)],
)
def test_max_rows_is_clamped_to_the_catalog_limits(requested, expected):
    assert node_viz.clamp_max_rows(requested) == expected
