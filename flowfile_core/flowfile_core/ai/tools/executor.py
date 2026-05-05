"""Tool executor with prospective schema validation — W31.

Per plan §6.3, ``execute_tool_call`` is the single entry point for the LLM's
typed tool calls. The executor:

1. Parses the MCP-shaped tool name (``flowfile.<domain>.<op>``).
2. Resolves the target ``FlowGraph`` via ``flow_file_handler.get_flow``.
3. Dispatches per-domain (``graph`` / ``schema`` / ``codegen`` / ``meta``).
4. For ``graph.add_<node_type>``: validates settings via the Pydantic class,
   refuses on network egress (W25 §9.6), resolves upstream schemas via the
   D011 tier handler, validates column refs, predicts output schema (mirror
   for static/source/passthrough; kernel dry-run for dynamic — D003), then
   either applies via ``getattr(flow, f"add_{node_type}")(settings)`` or
   stages the payload for W41 to compose into a GraphDiff.
5. Emits one :class:`AuditEvent` per call (W15) with secrets redacted.

The executor does NOT do its own ``pl.scan_*`` calls — per the project rule
"the collect of polars data only takes place in the worker — use nodes
already", all data-touching paths go through the existing ``add_<node_type>``
infrastructure (which is worker-aware) or through ``kernel_runtime``.

Coordination with W41: ``mode="stage"`` returns ``staged_node_payload`` with
the validated settings + predicted schema; W41 composes a list of those into
a ``GraphDiff`` and wires the accept path through
``HistoryManager.capture_if_changed`` for a single undo point. W31 does NOT
call ``audit.update_diff_action`` — that's W41's contract.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Final, Literal

from pydantic import BaseModel, Field, ValidationError

from flowfile_core.ai import audit, safety
from flowfile_core.ai.tools.classification import classify_node_type
from flowfile_core.ai.tools.dry_run import DryRunCache, dry_run_code
from flowfile_core.ai.tools.predictor import (
    _resolve_upstream_schemas,
    collect_column_refs,
    predict_schema_via_mirror,
    schema_to_dict_list,
)
from flowfile_core.ai.tools.registry import pick_category as _pick_category_heuristic
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import get_settings_class_for_node_type

logger = logging.getLogger(__name__)

ExecutionMode = Literal["apply", "stage"]
ResultStatus = Literal["applied", "staged", "warned", "rejected"]

_TOOL_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^flowfile\.(graph|schema|codegen|meta)\.(.+)$")

#: Settings field paths for code-bearing nodes — used for the network-egress
#: check (§9.6). Each tuple is ``(node_type, attr_path)``.
_CODE_BEARING: Final[dict[str, tuple[str, ...]]] = {
    "polars_code": ("polars_code_input", "polars_code"),
    "python_script": ("python_script_input", "code"),
    "sql_query": ("sql_query_input", "sql_code"),
}


class InsertionContext(BaseModel):
    """Where a new node attaches to the existing graph.

    The executor doesn't auto-layout — ``pos_x`` / ``pos_y`` come from the
    caller (frontend, agent prompt). ``upstream_node_ids`` are connected to
    ``input-0`` (main); the optional ``right_input_node_id`` is connected to
    ``input-1`` (right) for joins.
    """

    upstream_node_ids: list[int] = Field(default_factory=list)
    right_input_node_id: int | None = None
    pos_x: float = 0.0
    pos_y: float = 0.0


class ToolExecutionResult(BaseModel):
    """Outcome of one ``execute_tool_call``.

    * ``applied`` — node was added/wired to the real graph (mode=apply, no
      D011 warnings).
    * ``staged`` — settings + predicted schema captured for W41 (mode=stage),
      no real-graph mutation.
    * ``warned`` — tool ran but produced D011 warnings (e.g. upstream schema
      unknown; column-ref validation deferred until run).
    * ``rejected`` — refusal (Pydantic validation, network egress, unknown
      columns, ...).
    """

    status: ResultStatus
    tool_name: str
    node_id: int | None = None
    predicted_output_schema: list[dict[str, Any]] | None = None
    refusal_reason: safety.RefusalReason | None = None
    refusal_detail: str | None = None
    warnings: list[str] = Field(default_factory=list)
    audit_id: int | None = None
    executed_args: dict[str, Any] | None = None
    staged_node_payload: dict[str, Any] | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


def execute_tool_call(
    *,
    flow_id: int,
    tool_name: str,
    tool_args: dict[str, Any],
    insertion_context: InsertionContext,
    session_id: str,
    user_id: int,
    mode: ExecutionMode = "apply",
    flow=None,
    dry_run_cache: DryRunCache | None = None,
    llm_provided_node_id: int | None = None,
    audit_meta: dict[str, Any] | None = None,
) -> ToolExecutionResult:
    """Validate, predict, and dispatch a single LLM tool call.

    ``flow`` is optional — if not provided, looked up via
    ``flow_file_handler.get_flow(flow_id)``. Tests can pass an explicit flow
    to avoid touching the global handler.

    ``dry_run_cache`` is the per-session :class:`DryRunCache`. If ``None``, a
    fresh cache is created (no cross-call hit). Long-running planner sessions
    should reuse one cache so identical proposals don't re-pay the kernel cost.

    ``llm_provided_node_id`` and ``audit_meta`` are W54 surfaces. The planner
    sets ``llm_provided_node_id`` when the LLM emitted ``node_id`` itself
    (instead of letting the planner allocate); the executor then validates
    the id is fresh + non-self-looping. ``audit_meta`` rides on every
    ``add_*`` audit row under ``tool_args["__planner_meta__"]`` so future
    self-loops are diagnosable from the audit row alone (the existing
    ``AuditEvent.extra`` field is dropped before persistence — see
    plan §6).
    """
    redacted_args = safety.redact_secrets(tool_args) if tool_args else {}

    match = _TOOL_NAME_RE.match(tool_name)
    if match is None:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow_id,
            refusal_reason=None,
            refusal_detail=f"invalid tool name: {tool_name!r}",
        )

    domain, op = match.group(1), match.group(2)

    if flow is None:
        flow = _resolve_flow(flow_id)
        if flow is None:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=f"flow {flow_id} not found",
            )

    if dry_run_cache is None:
        dry_run_cache = DryRunCache()

    if domain == "graph":
        return _handle_graph(
            op=op,
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            insertion_context=insertion_context,
            flow=flow,
            session_id=session_id,
            user_id=user_id,
            mode=mode,
            dry_run_cache=dry_run_cache,
            llm_provided_node_id=llm_provided_node_id,
            audit_meta=audit_meta,
        )

    if domain == "schema":
        return _handle_schema(
            op=op,
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            flow=flow,
            session_id=session_id,
            user_id=user_id,
        )

    if domain == "codegen":
        return _handle_codegen(
            op=op,
            tool_name=tool_name,
            redacted_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
        )

    if domain == "meta":
        return _handle_meta(
            op=op,
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
        )

    return _reject_and_audit(
        tool_name=tool_name,
        tool_args=redacted_args,
        session_id=session_id,
        user_id=user_id,
        flow_id=flow.flow_id,
        refusal_reason=None,
        refusal_detail=f"unknown domain: {domain!r}",
    )


def _resolve_flow(flow_id: int):
    """Look up a ``FlowGraph`` via the global handler. Lazy import to keep the
    executor's module-level imports light (the handler pulls in DB session
    machinery which other AI tests don't need)."""
    from flowfile_core.flowfile.handler import flow_file_handler

    return flow_file_handler.get_flow(flow_id)


def _handle_graph(
    *,
    op: str,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    insertion_context: InsertionContext,
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
    dry_run_cache: DryRunCache,
    llm_provided_node_id: int | None = None,
    audit_meta: dict[str, Any] | None = None,
) -> ToolExecutionResult:
    if op.startswith("add_"):
        node_type = op[len("add_") :]
        return _handle_add_node(
            node_type=node_type,
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            insertion_context=insertion_context,
            flow=flow,
            session_id=session_id,
            user_id=user_id,
            mode=mode,
            dry_run_cache=dry_run_cache,
            llm_provided_node_id=llm_provided_node_id,
            audit_meta=audit_meta,
        )

    if op == "connect":
        return _handle_connect(tool_name, tool_args, redacted_args, flow, session_id, user_id, mode)
    if op == "delete_node":
        return _handle_delete_node(tool_name, tool_args, redacted_args, flow, session_id, user_id, mode)
    if op == "delete_connection":
        return _handle_delete_connection(tool_name, tool_args, redacted_args, flow, session_id, user_id, mode)

    # ``update_node_settings`` / ``run_node`` / ``propose_subgraph`` were
    # removed from the catalog in W46 (graph_ops.py 2026-05-05) so the LLM
    # never sees them. This rejection branch stays as defence-in-depth in
    # case a future workstream re-adds them to the catalog before wiring an
    # implementation. ``update_node_settings`` is tracked for proper
    # implementation under W47 (depends on ``GraphDiff.modifications``).
    return _reject_and_audit(
        tool_name=tool_name,
        tool_args=redacted_args,
        session_id=session_id,
        user_id=user_id,
        flow_id=flow.flow_id,
        refusal_reason=None,
        refusal_detail=f"graph op {op!r} is not in the agent's catalog",
    )


def _handle_add_node(
    *,
    node_type: str,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    insertion_context: InsertionContext,
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
    dry_run_cache: DryRunCache,
    llm_provided_node_id: int | None = None,
    audit_meta: dict[str, Any] | None = None,
) -> ToolExecutionResult:
    # W54 — every audit row this function emits piggybacks on tool_args
    # under the namespaced ``__planner_meta__`` key. Rebind ``redacted_args``
    # once at the top so all downstream rejection / record_event sites pick
    # it up automatically. (Why tool_args and not AuditEvent.extra: the
    # extra field is silently dropped by record_event — the AiAuditEvent
    # ORM has no ``extra`` column. See plan §6.)
    if audit_meta is not None:
        redacted_args = {**redacted_args, "__planner_meta__": audit_meta}

    settings_cls = get_settings_class_for_node_type(node_type)
    if settings_cls is None:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"unknown node type: {node_type!r}",
        )

    # --- W54 stage 0: LLM-provided node_id validation ---
    # Only fires when the planner observed the LLM emit ``node_id`` itself
    # rather than letting the planner allocate. The id must be fresh
    # (not in the live graph), not a duplicate of another agent-staged id,
    # and not equal to any of its own upstream / right_input — that last
    # rule overlaps with the planner-side guard in defence-in-depth.
    if llm_provided_node_id is not None:
        live_ids: set[int] = set()
        for node in flow.nodes:
            try:
                live_ids.add(int(node.node_id))
            except (TypeError, ValueError, AttributeError):
                continue
        staged_ids_meta = (audit_meta or {}).get("staged_node_ids_at_stage") or []
        staged_ids = {sid for sid in staged_ids_meta if isinstance(sid, int)}
        upstream_ids_set = {uid for uid in insertion_context.upstream_node_ids if isinstance(uid, int)}
        if insertion_context.right_input_node_id is not None:
            upstream_ids_set.add(insertion_context.right_input_node_id)

        violation: str | None = None
        if llm_provided_node_id in live_ids:
            violation = f"already exists in the live graph (live_node_ids={sorted(live_ids)})"
        elif llm_provided_node_id in staged_ids:
            violation = f"already staged this session (staged_node_ids={sorted(staged_ids)})"
        elif llm_provided_node_id in upstream_ids_set:
            violation = f"equals one of its own upstream / right_input ids ({sorted(upstream_ids_set)}) — would create a self-loop"
        if violation is not None:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="self_loop_prevented",
                refusal_detail=f"LLM-provided node_id {llm_provided_node_id} is invalid: {violation}",
            )

    # --- Refusal stage 1: Pydantic shape ---
    try:
        settings = settings_cls.model_validate(tool_args)
    except ValidationError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"settings validation failed: {exc}",
        )

    # --- Refusal stage 2: network egress (code-bearing nodes only) ---
    code = _extract_code(node_type, settings)
    if code is not None:
        egress_labels = safety.detect_network_egress(code)
        if egress_labels:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="network_egress",
                refusal_detail=f"blocked: {', '.join(egress_labels)}",
            )

    # --- Resolve upstream schemas (D011 tiers 0-1, warn on tier 2) ---
    upstream_ids = list(insertion_context.upstream_node_ids)
    if insertion_context.right_input_node_id is not None and insertion_context.right_input_node_id not in upstream_ids:
        upstream_ids.append(insertion_context.right_input_node_id)
    upstream_schemas, warnings = _resolve_upstream_schemas(flow, upstream_ids)

    # --- Refusal stage 3: column refs ---
    refs = collect_column_refs(node_type, settings)
    if refs and upstream_schemas:
        available: list[str] = []
        seen: set[str] = set()
        for cols in upstream_schemas.values():
            for col in cols:
                if col.column_name not in seen:
                    seen.add(col.column_name)
                    available.append(col.column_name)
        missing = safety.validate_column_references(refs, available)
        if missing:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="unknown_columns",
                refusal_detail=f"missing columns: {', '.join(missing)}",
                extra={"missing_columns": missing, "available_columns": available},
            )

    # --- Predict output schema ---
    node_class = classify_node_type(node_type)
    predicted: list[Any] | None = None
    if node_class == "dynamic":
        if code is None:
            warnings.append(f"dynamic node {node_type} has no code-bearing field; output schema cannot be predicted")
        elif not upstream_schemas and insertion_context.upstream_node_ids:
            warnings.append("upstream schema unresolved — kernel dry-run skipped; output schema deferred until run")
        elif not insertion_context.upstream_node_ids:
            warnings.append(f"dynamic node {node_type} has no upstream; output schema cannot be predicted")
        else:
            try:
                predicted = dry_run_code(
                    flow=flow,
                    node_id=settings.node_id,
                    upstream_node_ids=insertion_context.upstream_node_ids,
                    code=code,
                    output_names=_resolve_output_names(node_type, settings),
                    cache=dry_run_cache,
                    upstream_schemas=upstream_schemas,
                )
            except Exception as exc:
                logger.warning("dry-run failed for %s/%s: %s", node_type, settings.node_id, exc)
                warnings.append(f"kernel dry-run failed: {exc}; output schema deferred")
    else:
        predicted = predict_schema_via_mirror(
            node_type,
            settings,
            upstream_schemas,
            right_input_node_id=insertion_context.right_input_node_id,
        )

    # --- Apply or stage ---
    if mode == "stage":
        payload = {
            "node_type": node_type,
            "settings": settings.model_dump(mode="json"),
            "insertion_context": insertion_context.model_dump(),
            "predicted_output_schema": schema_to_dict_list(predicted),
        }
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow.flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="staged",
            tool_name=tool_name,
            predicted_output_schema=schema_to_dict_list(predicted),
            warnings=warnings,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload=payload,
        )

    # mode == "apply"
    try:
        _apply_add_node(flow, node_type, settings, insertion_context)
    except Exception as exc:
        logger.warning("apply failed for %s/%s: %s", node_type, settings.node_id, exc)
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"apply failed: {exc}",
        )

    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow.flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    status: ResultStatus = "warned" if warnings else "applied"
    return ToolExecutionResult(
        status=status,
        tool_name=tool_name,
        node_id=settings.node_id,
        predicted_output_schema=schema_to_dict_list(predicted),
        warnings=warnings,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )


def _apply_add_node(flow, node_type: str, settings: BaseModel, ctx: InsertionContext) -> None:
    """Real-graph mutation: dispatch ``add_<node_type>`` then wire connections."""
    from flowfile_core.flowfile.flow_graph import add_connection

    add_method = getattr(flow, f"add_{node_type}")
    add_method(settings)

    target_id = settings.node_id
    main_ids = [uid for uid in ctx.upstream_node_ids if uid != ctx.right_input_node_id]
    for uid in main_ids:
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=uid, to_id=target_id, input_type="input-0", output_handle="output-0"
        )
        add_connection(flow, connection)
    if ctx.right_input_node_id is not None:
        right_connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=ctx.right_input_node_id, to_id=target_id, input_type="input-1", output_handle="output-0"
        )
        add_connection(flow, right_connection)


def _handle_connect(
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
) -> ToolExecutionResult:
    """Wire a connection between two existing nodes."""
    try:
        connection = input_schema.NodeConnection.model_validate(tool_args)
    except ValidationError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"connection validation failed: {exc}",
        )

    if mode == "stage":
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow.flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="staged",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload={"connection": connection.model_dump()},
        )

    from flowfile_core.flowfile.flow_graph import add_connection

    try:
        add_connection(flow, connection)
    except Exception as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"connect failed: {exc}",
        )

    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow.flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    return ToolExecutionResult(
        status="applied",
        tool_name=tool_name,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )


def _handle_delete_node(
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
) -> ToolExecutionResult:
    node_id = tool_args.get("node_id")
    if not isinstance(node_id, int):
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail="delete_node requires an integer node_id",
        )

    if mode == "stage":
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow.flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="staged",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload={"delete_node_id": node_id},
        )

    try:
        flow.delete_node(node_id)
    except Exception as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"delete_node failed: {exc}",
        )

    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow.flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    return ToolExecutionResult(
        status="applied",
        tool_name=tool_name,
        node_id=node_id,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )


def _handle_delete_connection(
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
) -> ToolExecutionResult:
    try:
        connection = input_schema.NodeConnection.model_validate(tool_args)
    except ValidationError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"connection validation failed: {exc}",
        )

    if mode == "stage":
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow.flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="staged",
            tool_name=tool_name,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload={"delete_connection": connection.model_dump()},
        )

    from flowfile_core.flowfile.flow_graph import delete_connection

    try:
        delete_connection(flow, connection)
    except Exception as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"delete_connection failed: {exc}",
        )

    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow.flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    return ToolExecutionResult(
        status="applied",
        tool_name=tool_name,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )


def _handle_schema(
    *,
    op: str,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
) -> ToolExecutionResult:
    """Read-only introspection ops — never mutate the graph."""
    node_id = tool_args.get("node_id")
    if not isinstance(node_id, int):
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"{op} requires an integer node_id",
        )

    node = flow.get_node(node_id)
    if node is None:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"node {node_id} not found in flow {flow.flow_id}",
        )

    if op == "read_node_schema":
        try:
            schema = node.get_predicted_schema(force=False)
        except Exception as exc:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason=None,
                refusal_detail=f"schema read failed: {exc}",
            )
        audit_row = _record_event(
            session_id=session_id,
            user_id=user_id,
            tool_name=tool_name,
            flow_id=flow.flow_id,
            tool_args=redacted_args,
            result_status="success",
        )
        return ToolExecutionResult(
            status="applied",
            tool_name=tool_name,
            node_id=node_id,
            predicted_output_schema=schema_to_dict_list(schema),
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
        )

    if op == "read_node_preview":
        # Preview is metadata-only in W31 — actual sample-row return belongs
        # to a later workstream that wires the existing GET /node?get_data
        # path through the audit pipeline.
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail="schema.read_node_preview not implemented in W31; use the existing GET /node?get_data=true",
        )

    return _reject_and_audit(
        tool_name=tool_name,
        tool_args=redacted_args,
        session_id=session_id,
        user_id=user_id,
        flow_id=flow.flow_id,
        refusal_reason=None,
        refusal_detail=f"unknown schema op: {op!r}",
    )


def _handle_codegen(
    *,
    op: str,
    tool_name: str,
    redacted_args: dict[str, Any],
    session_id: str,
    user_id: int,
    flow_id: int,
) -> ToolExecutionResult:
    """Codegen tools are LLM-facing proposals — the LLM should call
    ``flowfile.graph.add_polars_code`` etc. directly with its generated code.
    W31 returns a friendly hint; W40's planner replaces this stub with actual
    LLM-driven code generation."""
    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    return ToolExecutionResult(
        status="staged",
        tool_name=tool_name,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
        staged_node_payload={
            "codegen_op": op,
            "hint": (
                "Codegen tools are not directly executed in W31. Generate the code "
                f"yourself and call flowfile.graph.add_{op.replace('generate_', '').replace('_code', '_code')} "
                "with the proposed snippet."
            ),
        },
    )


def _handle_meta(
    *,
    op: str,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    session_id: str,
    user_id: int,
    flow_id: int,
) -> ToolExecutionResult:
    if op != "pick_category":
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow_id,
            refusal_reason=None,
            refusal_detail=f"unknown meta op: {op!r}",
        )
    intent = tool_args.get("intent", "")
    category = _pick_category_heuristic(str(intent))
    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow_id,
        tool_args=redacted_args,
        result_status="success",
    )
    return ToolExecutionResult(
        status="applied",
        tool_name=tool_name,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
        extra={"category": category, "rationale": "heuristic keyword match (W31)"},
    )


def _extract_code(node_type: str, settings: BaseModel) -> str | None:
    path = _CODE_BEARING.get(node_type)
    if path is None:
        return None
    obj: Any = settings
    for attr in path:
        obj = getattr(obj, attr, None)
        if obj is None:
            return None
    return obj if isinstance(obj, str) else None


def _resolve_output_names(node_type: str, settings: BaseModel) -> list[str]:
    """Return the output names the kernel dry-run should produce.

    ``python_script`` and ``user_defined`` declare ``output_names`` explicitly;
    everything else has a single ``main`` output.
    """
    if node_type in ("python_script", "user_defined"):
        names = getattr(settings, "output_names", None) or ["main"]
        return list(names)
    return ["main"]


def _record_event(
    *,
    session_id: str,
    user_id: int,
    tool_name: str,
    flow_id: int,
    tool_args: dict[str, Any] | None,
    result_status: audit.ResultStatus,
    error: str | None = None,
):
    """Persist an audit event; swallow audit-side exceptions so a DB hiccup
    doesn't take down a tool call. Returns the row or ``None``."""
    try:
        return audit.record_event(
            audit.AuditEvent(
                session_id=session_id,
                user_id=user_id,
                tool_name=tool_name,
                result_status=result_status,
                flow_id=flow_id,
                tool_args=tool_args,
                error=error,
            )
        )
    except Exception as exc:
        logger.warning("audit.record_event failed for %s: %s", tool_name, exc)
        return None


def _reject_and_audit(
    *,
    tool_name: str,
    tool_args: dict[str, Any],
    session_id: str,
    user_id: int,
    flow_id: int,
    refusal_reason: safety.RefusalReason | None,
    refusal_detail: str,
    extra: dict[str, Any] | None = None,
) -> ToolExecutionResult:
    audit_row = _record_event(
        session_id=session_id,
        user_id=user_id,
        tool_name=tool_name,
        flow_id=flow_id,
        tool_args=tool_args,
        result_status="rejected",
        error=refusal_detail,
    )
    return ToolExecutionResult(
        status="rejected",
        tool_name=tool_name,
        refusal_reason=refusal_reason,
        refusal_detail=refusal_detail,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=tool_args,
        extra=extra or {},
    )


__all__ = [
    "execute_tool_call",
    "ToolExecutionResult",
    "InsertionContext",
    "ExecutionMode",
    "ResultStatus",
]
