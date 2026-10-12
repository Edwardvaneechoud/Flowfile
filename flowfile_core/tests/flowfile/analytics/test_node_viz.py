"""The Explore Data node's worker-backed compute path.

Mirrors `test_catalog_visualizations.py`'s dispatch tests: the worker's
visualisation routes are patched out where the worker backend calls them, and
what is asserted is the source descriptor core emits through the backend's
``viz_session`` operation.
"""
import dataclasses
import hashlib
import io
from base64 import b64decode
from unittest.mock import patch

import polars as pl
import pytest
import requests

from flowfile_core.flowfile.analytics import node_viz
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.context import ExecutionContext, use_context
from flowfile_core.flowfile.execution.errors import ComputeUnavailable, OperationUnsupported
from flowfile_core.flowfile.execution.payloads import VizSessionPayload, op_payload
from flowfile_core.flowfile.execution.protocol import OperationKind
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
        patch.object(worker_module, "trigger_visualize_query", side_effect=fake_query),
        patch.object(worker_module, "trigger_visualize_fields", side_effect=fake_fields),
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


def test_the_session_key_names_the_plan_the_worker_receives(fake_worker):
    graph, node = _run_explore_flow()

    node_viz.compute_node_rows(graph, node, _AGG_PAYLOAD, max_rows=None)

    source = fake_worker["source"]
    plan_key = hashlib.sha256(b64decode(source["plan_bytes"])).hexdigest()[:16]
    assert source["session_key"].startswith(f"node:{plan_key}:")


def test_a_local_flow_still_charts_on_the_worker(fake_worker):
    """The visualisation child exists only on the worker, so a flow running in core charts there too."""
    graph, node = _run_explore_flow()

    with use_context(ExecutionContext(registry.LOCAL_BACKEND_ID, parallelism=1, preview_rows=None)):
        fields = node_viz.get_node_fields(graph, node)

    assert fields.fields == [{"fid": "g"}]
    assert fake_worker["fields_source"]["kind"] == "plan"


# The viz_session operation


def _viz_payload(**overrides) -> VizSessionPayload:
    return VizSessionPayload(
        **{
            "flow_id": 7,
            "node_id": 3,
            "plan": pl.LazyFrame({"g": ["a", "b"], "v": [1, 2]}),
            "session_key": "node:abc:1.0",
            "request": "query",
            "query": _AGG_PAYLOAD,
            "max_rows": 500,
            **overrides,
        }
    )


def test_a_query_on_the_worker_backend_sends_the_serialised_plan_under_its_session_key(fake_worker):
    request = _viz_payload()

    result = WorkerBackend().run_op(OperationKind.viz_session, op_payload(request))

    source = fake_worker["source"]
    assert source == {
        "kind": "plan",
        "session_key": "node:abc:1.0",
        "plan_bytes": source["plan_bytes"],
    }
    assert b64decode(source["plan_bytes"]) == request.plan.serialize()
    assert fake_worker["payload"] is _AGG_PAYLOAD
    assert fake_worker["max_rows"] == 500
    assert result.refs == {}
    assert result.metadata == {"rows": [], "total_rows": 0, "truncated": False, "elapsed_ms": 0.0, "cache_hit": False}


def test_a_fields_request_on_the_worker_backend_asks_for_the_field_schema(fake_worker):
    request = _viz_payload(request="fields", query=None, max_rows=None)

    result = WorkerBackend().run_op(OperationKind.viz_session, op_payload(request))

    assert fake_worker["fields_source"]["session_key"] == "node:abc:1.0"
    assert pl.LazyFrame.deserialize(io.BytesIO(b64decode(fake_worker["fields_source"]["plan_bytes"]))).collect()[
        "v"
    ].to_list() == [1, 2]
    assert "source" not in fake_worker, "no chart query is sent"
    assert result.metadata == {"fields": [{"fid": "g"}], "cache_hit": False}


def test_an_unreachable_worker_fails_a_viz_session_typed():
    with (
        patch.object(worker_module, "trigger_visualize_query", side_effect=requests.ConnectionError("refused")),
        pytest.raises(ComputeUnavailable, match="refused"),
    ):
        WorkerBackend().run_op(OperationKind.viz_session, op_payload(_viz_payload()))


def test_a_worker_error_answer_reaches_the_route_as_before():
    """The analysis routes answer the transport's RuntimeError with a 502."""
    failure = RuntimeError("Worker visualize_query failed: boom")
    with (
        patch.object(worker_module, "trigger_visualize_query", side_effect=failure),
        pytest.raises(RuntimeError, match="boom"),
    ):
        WorkerBackend().run_op(OperationKind.viz_session, op_payload(_viz_payload()))


def test_core_refuses_a_viz_session():
    with pytest.raises(OperationUnsupported, match="viz_session"):
        LocalBackend().run_op(OperationKind.viz_session, op_payload(_viz_payload()))


def test_a_viz_session_payload_is_frozen_and_keyword_only():
    request = _viz_payload()

    with pytest.raises(dataclasses.FrozenInstanceError):
        request.session_key = "another"
    with pytest.raises(TypeError):
        VizSessionPayload(7, 3)
    assert len(request.job_id) == 32


@pytest.mark.parametrize(
    "overrides",
    [
        {"query": None},
        {"max_rows": None},
        {"request": "fields"},
        {"request": "fields", "query": None},
        {"request": "chart"},
    ],
    ids=["query_without_query", "query_without_max_rows", "fields_with_both", "fields_with_max_rows", "unknown"],
)
def test_a_viz_session_payload_carries_exactly_what_its_request_needs(overrides):
    with pytest.raises(ValueError, match="Graphic Walker"):
        _viz_payload(**overrides)
