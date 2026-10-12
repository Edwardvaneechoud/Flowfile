"""Worker-backed Graphic Walker compute for the Explore Data node.

Mirrors the catalog visualization path (``catalog/services/visualizations.py``)
so a chart built on a flow node and the same chart built on a catalog table run
through one aggregation implementation — ``polars_gw`` inside a spawned worker
child — and agree. Each request is a ``viz_session`` operation on the worker
backend (``registry.WORKER_BACKEND_ID``) whatever the flow's execution location,
since the visualisation child exists only on the worker; core's own backend
refuses the operation.

Core never collects here, and nothing is materialised: the operation ships the
node's LazyFrame, serialised, and the worker child holds it lazily, exactly as
the catalog's ``physical`` kind holds a ``scan_delta``. Polars then pushes each
chart's projection into the node's own sources, which answers a group-by on a
294 MB Parquet in ~0.04 s; snapshotting that source first would cost 4.9 GB.
The cloud-plan refusal and the session key are decided here, before anything
is sent.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any, Literal

from flowfile_core.catalog.constants import DEFAULT_VISUALIZATION_ROWS, MAX_VISUALIZATION_ROWS
from flowfile_core.catalog.storage_backend import serialized_frame_uses_cloud
from flowfile_core.configs import logger
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.payloads import VizSessionPayload, op_payload
from flowfile_core.flowfile.execution.protocol import OperationKind
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
    # Microseconds: two runs starting inside the same second must not share a key.
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


def viz_request(
    flow: FlowGraph,
    node: FlowNode,
    request: Literal["fields", "query"],
    query: dict[str, Any] | None = None,
    max_rows: int | None = None,
) -> VizSessionPayload:
    """The ``viz_session`` payload of one Graphic Walker request over *node*'s result, without materialising it.

    The plan is the node's result LazyFrame. The session key is content-addressed, ``sha256`` of the serialised
    plan, plus the run token, never ``flow_id`` (regenerated on every flow open), so repeated requests on one run
    reuse one warm child and a re-run gets a fresh one.

    Raises:
        NodeNotRunError: The node has no result yet.
        CloudPlanNotVisualizableError: The plan reads object storage; nothing is sent.
    """
    if not has_result_to_visualize(node):
        raise NodeNotRunError("The data is not refreshed and available for analysis")

    resulting_data = node.get_resulting_data()
    if resulting_data is None:
        raise NodeNotRunError("The data is not refreshed and available for analysis")

    plan = resulting_data.data_frame.lazy()
    plan_bytes = plan.serialize()
    if serialized_frame_uses_cloud(plan_bytes):
        # The plan freezes build-time credentials ($ffsec$ ciphertext, some still inline); viz children live long.
        raise CloudPlanNotVisualizableError(
            "Visualizing a node that reads from object storage (cloud) is not supported; "
            "its query plan embeds the connection's credentials from when the node ran. Write the result to a "
            "local file or catalog table and explore that instead."
        )

    plan_key = hashlib.sha256(plan_bytes).hexdigest()[:16]
    return VizSessionPayload(
        flow_id=flow.flow_id,
        node_id=node.node_id,
        plan=plan,
        session_key=f"node:{plan_key}:{_run_token(flow)}",
        request=request,
        query=query,
        max_rows=max_rows,
    )


def _answer(request: VizSessionPayload) -> dict[str, Any]:
    """The worker's visualisation session's answer to *request*."""
    worker = registry.backend(registry.WORKER_BACKEND_ID)
    return worker.run_op(OperationKind.viz_session, op_payload(request)).metadata


def compute_node_rows(
    flow: FlowGraph, node: FlowNode, payload: dict, max_rows: int | None
) -> VisualizationComputeResponse:
    """Run a Graphic Walker IDataQueryPayload against *node*'s result."""
    request = viz_request(flow, node, "query", query=payload, max_rows=clamp_max_rows(max_rows))
    viz_logger.info(
        "dispatch node compute flow_id=%s node_id=%s session_key=%s max_rows=%s",
        flow.flow_id,
        node.node_id,
        request.session_key,
        max_rows,
    )
    return VisualizationComputeResponse(**_answer(request))


def get_node_fields(flow: FlowGraph, node: FlowNode) -> VisualizationFieldsResponse:
    """Return the Graphic Walker field schema for *node*'s result."""
    request = viz_request(flow, node, "fields")
    viz_logger.info(
        "dispatch node fields flow_id=%s node_id=%s session_key=%s",
        flow.flow_id,
        node.node_id,
        request.session_key,
    )
    return VisualizationFieldsResponse(**_answer(request))
