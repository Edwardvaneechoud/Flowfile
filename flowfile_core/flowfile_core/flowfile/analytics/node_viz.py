"""Worker-backed Graphic Walker compute for the Explore Data node.

Mirrors the catalog visualization path (``catalog/services/visualizations.py``)
so a chart built on a flow node and the same chart built on a catalog table run
through one aggregation implementation — ``polars_gw`` inside a spawned worker
child — and agree.

Core never collects here, and nothing is materialised: it ships the node's
serialised LazyFrame and the worker child holds it lazily, exactly as the
catalog's ``physical`` kind holds a ``scan_delta``. Polars then pushes each
chart's projection into the node's own sources — on a 294 MB Parquet that
answers a group-by in ~0.04 s, where materialising it first cost 4.9 GB.
"""

from __future__ import annotations

import hashlib
from base64 import b64encode
from typing import TYPE_CHECKING

from flowfile_core.catalog.constants import DEFAULT_VISUALIZATION_ROWS, MAX_VISUALIZATION_ROWS
from flowfile_core.catalog.storage_backend import serialized_frame_uses_cloud
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_visualize_fields,
    trigger_visualize_query,
)
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas.catalog_schema import VisualizationComputeResponse, VisualizationFieldsResponse

if TYPE_CHECKING:  # flow_graph imports analytics.utils; keep this edge type-only
    from flowfile_core.flowfile.flow_graph import FlowGraph

viz_logger = logger.getChild("node_viz")


class NodeNotRunError(Exception):
    """The node has no result to visualize yet."""


class CloudPlanNotVisualizableError(Exception):
    """The node's plan embeds an object-storage scan, so it cannot be shipped."""


def clamp_max_rows(requested: int | None) -> int:
    if requested is None or requested <= 0:
        return min(DEFAULT_VISUALIZATION_ROWS, MAX_VISUALIZATION_ROWS)
    return min(requested, MAX_VISUALIZATION_ROWS)


def _run_token(flow: FlowGraph) -> str:
    """A value that changes on every flow run.

    A serialised plan records source paths, not data snapshots, so its hash does
    not move when the underlying data does (the same trap documented for the
    worker's ``kernel_shared`` target). Folding the run's start time into the
    session key lets repeated drawer opens reuse one warm child, while a re-run
    rotates the key and gets a fresh one.
    """
    run_info = getattr(flow, "latest_run_info", None)
    start_time = getattr(run_info, "start_time", None)
    # Microseconds, not whole seconds: two runs starting inside the same second
    # would otherwise produce the same key and the second one's data would never
    # reach the chart.
    return f"{start_time.timestamp():.6f}" if start_time else "norun"


def has_result_to_visualize(node: FlowNode) -> bool:
    """Whether *node* has produced a result plan a chart can read.

    Deliberately not ``node_stats.has_completed_last_run``: that flag is only
    set when ``performance_mode`` is off, so a Performance-mode local run leaves
    it False on a node that ran perfectly well. ``results.resulting_data`` is
    populated by every execution strategy and stays None until one runs —
    ``get_predicted_schema()`` and the explorer's own setup route don't set it.
    """
    return node.results.resulting_data is not None


def resolve_node_viz_source(flow: FlowGraph, node: FlowNode) -> dict:
    """Describe *node*'s result as a VizWorkerSource, without materialising it."""
    if not has_result_to_visualize(node):
        raise NodeNotRunError("The data is not refreshed and available for analysis")

    resulting_data = node.get_resulting_data()
    if resulting_data is None:
        raise NodeNotRunError("The data is not refreshed and available for analysis")

    plan_bytes = resulting_data.data_frame.serialize()
    if serialized_frame_uses_cloud(plan_bytes):
        # Polars inlines storage_options, so a cloud scan's plan carries the
        # source's decrypted credentials. Shipping that per request into a
        # long-lived session child is not something to do quietly.
        raise CloudPlanNotVisualizableError(
            "Visualizing a node that reads from object storage (cloud) is not supported; "
            "its query plan embeds the connection's credentials. Write the result to a "
            "local file or catalog table and explore that instead."
        )

    # Content-addressed: flow_id is regenerated on every flow open, and there is
    # no file to take an mtime from. The run token still rotates the key when the
    # data behind an unchanged plan changes.
    plan_key = hashlib.sha256(plan_bytes).hexdigest()[:16]
    return {
        "kind": "plan",
        "session_key": f"node:{plan_key}:{_run_token(flow)}",
        "plan_bytes": b64encode(plan_bytes).decode("ascii"),
    }


def compute_node_rows(
    flow: FlowGraph, node: FlowNode, payload: dict, max_rows: int | None
) -> VisualizationComputeResponse:
    """Run a Graphic Walker IDataQueryPayload against *node*'s result."""
    source = resolve_node_viz_source(flow, node)
    viz_logger.info(
        "dispatch node compute flow_id=%s node_id=%s session_key=%s max_rows=%s",
        flow.flow_id,
        node.node_id,
        source["session_key"],
        max_rows,
    )
    data = trigger_visualize_query(source, payload, clamp_max_rows(max_rows))
    return VisualizationComputeResponse(**data)


def get_node_fields(flow: FlowGraph, node: FlowNode) -> VisualizationFieldsResponse:
    """Return the Graphic Walker field schema for *node*'s result."""
    source = resolve_node_viz_source(flow, node)
    viz_logger.info(
        "dispatch node fields flow_id=%s node_id=%s session_key=%s",
        flow.flow_id,
        node.node_id,
        source["session_key"],
    )
    data = trigger_visualize_fields(source)
    return VisualizationFieldsResponse(**data)
