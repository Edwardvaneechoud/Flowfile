"""Plan a notebook push: seed snapshot, fingerprint precondition, clean run through the runner, refusals, reconcile.

:func:`plan_push` is everything a push does before it touches the canvas: it refuses with 409 when the cells
were rendered from another state of the flow, runs the cells through the installed
:class:`~flowfile_core.notebook.bridge.CleanRunner`, refuses with 422 what the canvas cannot hold, and
reconciles. ``POST /notebook/plan`` returns the result; ``POST /editor/notebook/push/`` applies its operations
as one ``apply_operations`` transaction, unless its ``trigger`` names a sync whose plan
:func:`needs_confirmation`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal

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
    "install it from a script with ff.custom_nodes.install(...) and push again."
)
INLINE_SECRET_REFUSAL = (
    "Node {node_id} (REST API reader) carries an inline secret, which is never stored on the canvas; "
    "save the credential as a secret and pass secret_name=... instead."
)


class NotebookPushRequest(BaseModel):
    """Body of ``POST /editor/notebook/push/`` and ``POST /notebook/plan``.

    ``trigger`` (push only) names the client action behind the sync; a plan that action must review first is
    returned unapplied. ``None`` applies unconditionally.
    """

    flow_id: int
    cells: list[tuple[str, str]]
    changed_cell_ids: list[str] = Field(default_factory=list)
    provenance: dict[str, list[tuple[str, int]]] = Field(default_factory=dict)
    code_fingerprint: str
    client_max_node_id: int = 0
    trigger: Literal["push", "run"] | None = None


class NotebookPlanResponse(BaseModel):
    """The ops a push would apply and what the review dialog shows."""

    operations: list[dict]
    warnings: list[str]
    deletions: list[int]
    parameter_changes: bool
    node_ids_by_cell: dict[str, list[int]]


def _columns(columns) -> list[dict[str, str]]:
    return [c.get_minimal_field_info().model_dump() for c in columns or []]


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
    """What a session seeds from: the save-format payload, parameters, names and schemas."""
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
    """Whether ``node_type`` is an installed custom node that loads; a file with a scan or placement error is not."""
    from flowfile_core.flowfile.user_defined.registry import registry

    entry = registry.get(node_type)
    if entry is None or entry.load_error is not None:
        registry.refresh()
        entry = registry.get(node_type)
    return entry is not None and entry.load_error is None


def refused_nodes(
    live: dict, session: dict, installed: Callable[[str], bool] = _installed_custom_node
) -> list[tuple[str, int]]:
    """``(message, node id)``, in order, for every node the canvas cannot hold: in-memory LazyFrames, custom node
    classes that are not installed or do not load, inline REST secrets."""
    refused = []
    live_types = {node["type"] for node in live.get("nodes") or []}
    for payload in (live, session):
        refused.extend(
            (LAZY_FRAME_REFUSAL.format(node_id=node["id"]), node["id"])
            for node in payload.get("nodes") or []
            if node["type"] == "polars_lazy_frame"
        )
    for node in session.get("nodes") or []:
        settings = node.get("setting_input")
        if not isinstance(settings, dict):
            continue
        if settings.get("is_user_defined") and node["type"] not in live_types and not installed(node["type"]):
            refused.append((CUSTOM_CLASS_REFUSAL.format(node_id=node["id"], node_type=node["type"]), node["id"]))
        if node["type"] == "rest_api_reader":
            auth = (settings.get("rest_api_settings") or {}).get("auth") or {}
            inline = auth.get("secret") or (auth.get("auth_type", "none") != "none" and not auth.get("secret_name"))
            if inline:
                refused.append((INLINE_SECRET_REFUSAL.format(node_id=node["id"]), node["id"]))
    return refused


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


def _refusing_cell(
    refused: list[tuple[str, int | None]],
    node_ids_by_cell: dict[str, list[int]],
    provenance: dict[str, list[tuple[str, int]]],
) -> str | None:
    """The cell of the first refused node: the cell that built it in this run, else the cell it renders in."""
    built = {node_id: cell_id for cell_id, node_ids in node_ids_by_cell.items() for node_id in node_ids}
    rendered = live_cells(provenance)
    for _, node_id in refused:
        cell_id = built.get(node_id) or rendered.get(node_id)
        if cell_id is not None:
            return cell_id
    return None


def node_id_ceiling(flow: FlowGraph, client_max_node_id: int) -> int:
    """The highest node id either the canvas or the client has seen; new nodes number above it."""
    return max([client_max_node_id, *(node.node_id for node in flow.nodes)], default=0)


def plan_push(flow: FlowGraph, user, request: NotebookPushRequest) -> tuple[ReconcilePlan, CleanRunResult]:
    """Everything a push does before it mutates the canvas; raises 409, 422 or 503 as ``HTTPException``.

    A 422's detail is ``{message, cell_id, line, kind}``: a failing cell's message, cell, 1-based line and
    kind (``needs_kernel``, ``refused`` or ``error``), or, for nodes the canvas cannot hold, their joined
    messages with kind ``refused`` and the cell of the first refused node.
    """
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
    ceiling = node_id_ceiling(flow, request.client_max_node_id)
    runner = get_clean_runner()
    result = runner.clean_run(
        user.id,
        flow.flow_id,
        CleanRunRequest(cells=request.cells, provenance=request.provenance, ceiling=ceiling, snapshot=snapshot),
    )
    if result.error is not None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": result.error, "cell_id": result.cell_id, "line": result.line, "kind": result.kind},
        )
    refused = [(message, None) for message in result.refusals] + refused_nodes(live, result.flowfile_data)
    if refused:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "\n".join(dict.fromkeys(message for message, _ in refused)),
                "cell_id": _refusing_cell(refused, result.node_ids_by_cell, request.provenance),
                "line": None,
                "kind": "refused",
            },
        )
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


def needs_confirmation(plan: ReconcilePlan, trigger: Literal["push", "run"]) -> bool:
    """Whether the user reviews ``plan`` before it applies: a run's sync only for deletions, a push for any
    deletion, parameter change or warning (the warnings already name every deletion and parameter change)."""
    if trigger == "run":
        return bool(plan.deletions)
    return bool(plan.deletions or plan.parameter_changes or plan.warnings)


def plan_response(plan: ReconcilePlan, result: CleanRunResult) -> NotebookPlanResponse:
    return NotebookPlanResponse(
        operations=[op.model_dump(mode="json") for op in plan.operations],
        warnings=plan.warnings,
        deletions=plan.deletions,
        parameter_changes=plan.parameter_changes,
        node_ids_by_cell=result.node_ids_by_cell,
    )
