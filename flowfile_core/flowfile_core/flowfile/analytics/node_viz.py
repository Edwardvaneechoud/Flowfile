"""Worker-backed Graphic Walker compute for the Explore Data node.

Mirrors the catalog visualization path (``catalog/services/visualizations.py``)
so a chart built on a flow node and the same chart built on a catalog table run
through one aggregation implementation — ``polars_gw`` inside a spawned worker
child — and agree.

Core never collects here: it serialises the node's LazyFrame and ships the plan
bytes; the worker materialises to Arrow IPC and holds the warm frame.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

from flowfile_core.catalog.constants import DEFAULT_VISUALIZATION_ROWS, MAX_VISUALIZATION_ROWS
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    trigger_resolve_virtual_table,
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


def clamp_max_rows(requested: int | None) -> int:
    if requested is None or requested <= 0:
        return min(DEFAULT_VISUALIZATION_ROWS, MAX_VISUALIZATION_ROWS)
    return min(requested, MAX_VISUALIZATION_ROWS)


def _run_token(flow: FlowGraph) -> str:
    """A value that changes on every flow run.

    A serialised plan records source paths, not data snapshots, so its hash does
    not move when the underlying data does (the same trap documented for the
    worker's ``kernel_shared`` target). Folding the run's start time into the
    version hash keeps repeated drawer opens on the worker's exists() fast path
    while a re-run produces a new file, a new session key, and a fresh child.
    """
    run_info = getattr(flow, "latest_run_info", None)
    start_time = getattr(run_info, "start_time", None)
    # Microseconds, not whole seconds: two runs starting inside the same second
    # would otherwise hash identically and the second one's data would never
    # reach the chart (the worker would serve the first run's cached file).
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
    """Materialise *node*'s result to IPC and describe it as a VizWorkerSource."""
    if not has_result_to_visualize(node):
        raise NodeNotRunError("The data is not refreshed and available for analysis")

    resulting_data = node.get_resulting_data()
    if resulting_data is None:
        raise NodeNotRunError("The data is not refreshed and available for analysis")

    versions_hash = hashlib.sha256(f"{node.hash}|{_run_token(flow)}".encode()).hexdigest()
    cache_key = f"node-{flow.flow_id}-{node.node_id}"
    result = trigger_resolve_virtual_table(
        table_id=node.node_id,
        plan_bytes=resulting_data.data_frame.serialize(),
        source_versions_hash=versions_hash,
        cache_key=cache_key,
    )
    return {
        "kind": "ipc_path",
        # Keyed on the versions hash rather than the file mtime: mtime has
        # one-second resolution, so two runs could share a session key while
        # pointing at different files and the pool would serve the stale child.
        "session_key": f"node:{flow.flow_id}:{node.node_id}:{versions_hash[:16]}",
        "ipc_path": result["ipc_path"],
        "mtime": result["mtime"],
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
