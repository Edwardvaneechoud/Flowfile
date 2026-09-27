"""Plan a notebook push: seed snapshot, fingerprint precondition, clean run through the runner, refusals, reconcile.

:func:`plan_push` is everything a push does before it touches the canvas (plan section 2.5 steps 1 to 3 and
5, section 2.7): it refuses with 409 when the cells were rendered from another state of the flow, runs the
cells through the installed :class:`~flowfile_core.notebook.bridge.CleanRunner`, refuses with 422 what the
canvas cannot hold, and reconciles. ``POST /notebook/plan`` returns the result; ``POST
/editor/notebook/push/`` applies its operations as one ``apply_operations`` transaction.
"""

from __future__ import annotations

from collections.abc import Callable

from fastapi import HTTPException, status
from pydantic import BaseModel, Field

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE, output_handle
from flowfile_core.notebook.bridge import CleanRunRequest, CleanRunResult, get_clean_runner
from flowfile_core.notebook.reconcile import ReconcilePlan, reconcile
from flowfile_core.notebook.render import code_fingerprint

LAZY_FRAME_REFUSAL = (
    "Node {node_id} holds an in-memory Polars LazyFrame, which cannot be pushed; "
    "replace it on the canvas with a reader or a Manual Input node."
)
CUSTOM_CLASS_REFUSAL = (
    "Node {node_id} uses the custom node class {node_type!r}, defined in a cell and not installed; "
    "install it from a script with fl.custom_nodes.install(...) and push again."
)
INLINE_SECRET_REFUSAL = (
    "Node {node_id} (REST API reader) carries an inline secret, which is never stored on the canvas; "
    "save the credential as a secret and pass secret_name=... instead."
)


class NotebookPushRequest(BaseModel):
    """Body of ``POST /editor/notebook/push/`` and ``POST /notebook/plan``."""

    flow_id: int
    cells: list[tuple[str, str]]
    changed_cell_ids: list[str] = Field(default_factory=list)
    provenance: dict[str, list[tuple[str, int]]] = Field(default_factory=dict)
    code_fingerprint: str
    client_max_node_id: int = 0


class NotebookPlanResponse(BaseModel):
    """The ops a push would apply and what the review dialog shows."""

    operations: list[dict]
    warnings: list[str]
    deletions: list[int]
    parameter_changes: bool
    node_ids_by_cell: dict[str, list[int]]


def _columns(columns) -> list[dict[str, str]]:
    return [{"name": c.column_name, "data_type": c.data_type} for c in columns or []]


def node_schemas(node: FlowNode) -> dict[str, list[dict[str, str]]]:
    """Per-handle schema of one node: the last run's, else the declared output schemas, else the predicted one."""
    if node._named_schemas:
        return {handle: _columns(columns) for handle, columns in node._named_schemas.items()}
    declared = getattr(node.setting_input, "output_schemas", None)
    names = getattr(node.setting_input, "output_names", None) or []
    if declared and names:
        return {
            output_handle(index): [{"name": f.name, "data_type": f.data_type} for f in declared[name]]
            for index, name in enumerate(names)
            if name in declared
        }
    result = node.node_schema.result_schema
    if result:
        return {DEFAULT_OUTPUT_HANDLE: _columns(result)}
    return {DEFAULT_OUTPUT_HANDLE: _columns(node.schema)}


def seed_snapshot(flow: FlowGraph) -> dict:
    """What a session seeds from (plan section 2.6): the save-format payload, parameters, names and schemas."""
    schemas: dict[int, dict[str, list[dict[str, str]]]] = {}
    for node in flow.nodes:
        try:
            schemas[node.node_id] = node_schemas(node)
        except Exception:
            logger.debug(f"notebook seed: no schema for node {node.node_id}", exc_info=True)
    return {
        "flowfile_data": flow.get_flowfile_data().model_dump(mode="json"),
        "parameters": [p.model_dump(mode="json") for p in flow.flow_settings.parameters],
        "names": {
            node.node_id: ref for node in flow.nodes if (ref := getattr(node.setting_input, "node_reference", None))
        },
        "schemas": schemas,
    }


def _installed_custom_node(node_type: str) -> bool:
    from flowfile_core.flowfile.user_defined.registry import registry

    return registry.get(node_type) is not None


def push_refusals(live: dict, session: dict, installed: Callable[[str], bool] = _installed_custom_node) -> list[str]:
    """What the canvas cannot hold: in-memory LazyFrames, custom node classes a cell defined, inline REST secrets."""
    refusals = []
    live_types = {node["type"] for node in live.get("nodes") or []}
    for payload in (live, session):
        refusals.extend(
            LAZY_FRAME_REFUSAL.format(node_id=node["id"])
            for node in payload.get("nodes") or []
            if node["type"] == "polars_lazy_frame"
        )
    for node in session.get("nodes") or []:
        settings = node.get("setting_input")
        if not isinstance(settings, dict):
            continue
        if settings.get("is_user_defined") and node["type"] not in live_types and not installed(node["type"]):
            refusals.append(CUSTOM_CLASS_REFUSAL.format(node_id=node["id"], node_type=node["type"]))
        if node["type"] == "rest_api_reader":
            auth = (settings.get("rest_api_settings") or {}).get("auth") or {}
            inline = auth.get("secret") or (auth.get("auth_type", "none") != "none" and not auth.get("secret_name"))
            if inline:
                refusals.append(INLINE_SECRET_REFUSAL.format(node_id=node["id"]))
    return list(dict.fromkeys(refusals))


def _owned_kernel_ids(user_id: int) -> set[str] | None:
    try:
        from flowfile_core.database.connection import get_db_context
        from flowfile_core.database.models import Kernel

        with get_db_context() as db:
            return {row[0] for row in db.query(Kernel.id).filter(Kernel.user_id == user_id).all()}
    except Exception:
        logger.debug("notebook push: could not read the kernel rows", exc_info=True)
        return None


def kernel_warnings(session: dict, user_id: int) -> list[str]:
    """A Python Script or custom node naming a kernel the user owns no row for (checked without Docker)."""
    named = {}
    for node in session.get("nodes") or []:
        settings = node.get("setting_input") or {}
        if not isinstance(settings, dict):
            continue
        kernel_id = (settings.get("python_script_input") or {}).get("kernel_id") or settings.get("kernel_id")
        if kernel_id:
            named[node["id"]] = kernel_id
    if not named:
        return []
    owned = _owned_kernel_ids(user_id)
    if owned is None:
        return []
    return [
        f"Node {node_id} names kernel {kernel_id!r}, which is not one of your kernels."
        for node_id, kernel_id in sorted(named.items())
        if kernel_id not in owned
    ]


def live_cells(provenance: dict[str, list[tuple[str, int]]]) -> dict[int, str]:
    """Live node id -> the cell whose provenance lists it."""
    return {int(node_id): cell_id for cell_id, entries in provenance.items() for _, node_id in entries}


def plan_push(flow: FlowGraph, user, request: NotebookPushRequest) -> tuple[ReconcilePlan, CleanRunResult]:
    """Everything a push does before it mutates the canvas; raises 409, 422 or 503 as ``HTTPException``."""
    live_fingerprint = code_fingerprint(flow)
    if request.code_fingerprint != live_fingerprint:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            detail={
                "message": "The canvas changed since these cells were rendered.",
                "code_fingerprint": live_fingerprint,
            },
        )
    snapshot = seed_snapshot(flow)
    live = snapshot["flowfile_data"]
    ceiling = max([request.client_max_node_id, *(node.node_id for node in flow.nodes)], default=0)
    runner = get_clean_runner()
    result = runner.clean_run(
        user.id,
        flow.flow_id,
        CleanRunRequest(cells=request.cells, provenance=request.provenance, ceiling=ceiling, snapshot=snapshot),
    )
    if result.error is not None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=result.error)
    refusals = list(result.refusals) + push_refusals(live, result.flowfile_data)
    if refusals:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail="\n".join(dict.fromkeys(refusals)))
    plan = reconcile(
        live,
        result.flowfile_data,
        request.changed_cell_ids,
        result.node_ids_by_cell,
        snapshot["names"],
        result.names,
        live["flowfile_settings"]["parameters"],
        result.flowfile_data["flowfile_settings"]["parameters"],
        live_cells=live_cells(request.provenance),
    )
    plan.warnings.extend(result.warnings)
    plan.warnings.extend(kernel_warnings(result.flowfile_data, user.id))
    return plan, result


def plan_response(plan: ReconcilePlan, result: CleanRunResult) -> NotebookPlanResponse:
    return NotebookPlanResponse(
        operations=[op.model_dump(mode="json") for op in plan.operations],
        warnings=plan.warnings,
        deletions=plan.deletions,
        parameter_changes=plan.parameter_changes,
        node_ids_by_cell=result.node_ids_by_cell,
    )
