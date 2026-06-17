"""``add_<node_type>`` handler — the heaviest path in the executor.

Validates settings, runs network-egress + sandbox checks for code-bearing
nodes, resolves upstream schemas, predicts output schema, and either
applies via ``flow.add_<node_type>(settings)`` or stages the payload for
the diff layer.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel, ValidationError

from flowfile_core.ai import safety
from flowfile_core.ai.tools.classification import classify_node_type
from flowfile_core.ai.tools.dry_run import DryRunCache, dry_run_code
from flowfile_core.ai.tools.predictor import (
    _resolve_upstream_schemas,
    collect_column_refs,
    predict_schema_via_mirror,
    schema_to_dict_list,
)
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import polars_code_parser
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import get_settings_class_for_node_type

from .._internal import (
    _POLARS_CODE_IMPORT_REFUSAL,
    ExecutionMode,
    InsertionContext,
    ResultStatus,
    ToolExecutionResult,
    _extract_code,
    _record_event,
    _reject_and_audit,
    _resolve_insertion_position,
    _resolve_output_names,
)
from ..manual_input import _normalize_manual_input_args
from ..refusals import (
    _detect_sink_upstreams,
    _format_settings_validation_refusal,
    _format_sink_upstream_refusal,
)

logger = logging.getLogger(__name__)


def _validate_python_script_body_or_reject(
    *,
    node_type: str,
    code: str,
    tool_name: str,
    redacted_args: dict[str, Any],
    session_id: str,
    user_id: int,
    flow_id: int,
) -> ToolExecutionResult | None:
    """Pre-flight python_script AST validation.

    Blocks ``__import__``, ``importlib``, dunders, ``eval``/``exec``/``compile``
    in python_script code. Mirrors the polars_code gate using the same
    ``polars_code_parser.validate_code`` AST walker (which blocks the same
    dangerous builtins and import patterns).

    Returns ``None`` when clean or when ``node_type`` isn't ``python_script``;
    returns a rejected ``ToolExecutionResult`` otherwise.
    """
    if node_type != "python_script":
        return None
    try:
        polars_code_parser.validate_code(code)
    except ValueError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow_id,
            refusal_reason="python_script_validation",
            refusal_detail=f"python_script rejected: {exc}",
        )
    return None


def _validate_polars_code_body_or_reject(
    *,
    node_type: str,
    code: str,
    tool_name: str,
    redacted_args: dict[str, Any],
    session_id: str,
    user_id: int,
    flow_id: int,
) -> ToolExecutionResult | None:
    """Pre-flight polars_code sandbox validation.

    Catches forbidden imports (and any other sandbox violation
    ``polars_code_parser`` raises) BEFORE ``flow.add_polars_code``
    swallows the ValueError into ``node.results.errors``. Without
    this the LLM never sees the rejection and the failure surfaces
    only at run-time.

    Returns ``None`` when the body is clean (or when ``node_type``
    isn't ``polars_code``); returns a ``ToolExecutionResult`` with
    ``status="rejected"`` otherwise.
    """
    if node_type != "polars_code":
        return None
    try:
        polars_code_parser.validate_code(code)
    except ValueError as exc:
        err_text = str(exc)
        if "Import" in err_text:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason="polars_code_import_forbidden",
                refusal_detail=_POLARS_CODE_IMPORT_REFUSAL,
            )
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow_id,
            refusal_reason="polars_code_validation",
            refusal_detail=f"polars_code rejected: {err_text}",
        )
    return None


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
    staged_offset_index: int = 0,
    extra_upstream_positions: dict[int, tuple[float, float]] | None = None,
    extra_upstream_schemas: dict[int, Any] | None = None,
) -> ToolExecutionResult:
    # Every audit row this function emits piggybacks on tool_args under
    # the namespaced ``__planner_meta__`` key. Rebind ``redacted_args``
    # once at the top so all downstream rejection / record_event sites
    # pick it up automatically. (Why tool_args and not AuditEvent.extra:
    # the extra field is silently dropped by record_event — the
    # AiAuditEvent ORM has no ``extra`` column.)
    if audit_meta is not None:
        redacted_args = {**redacted_args, "__planner_meta__": audit_meta}

    # Auto-layout: when the caller didn't supply pos_x/pos_y (both
    # ``None``), derive them from the upstream's canvas position so
    # AI-staged nodes don't pile up at (0, 0). When the caller passed
    # explicit floats — including ``0.0`` — they win verbatim.
    if insertion_context.pos_x is None and insertion_context.pos_y is None:
        resolved_x, resolved_y = _resolve_insertion_position(
            flow,
            insertion_context.upstream_node_ids,
            staged_offset_index=staged_offset_index,
            extra_upstream_positions=extra_upstream_positions,
        )
        insertion_context = insertion_context.model_copy(update={"pos_x": resolved_x, "pos_y": resolved_y})

    # Agent surfaces are not allowed to stage writer-shaped node types
    # (output / database_writer / cloud_storage_writer / catalog_writer).
    # The catalog filter in registry.build_tool_catalog already hides
    # them from the LLM-facing tool list, but the LLM can hallucinate a
    # call name (or a future regression could re-expose the tool);
    # refuse here so the safety property holds regardless of how the
    # call reached us.
    if node_type in safety.AGENT_BLOCKED_NODE_TYPES:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="writer_blocked",
            refusal_detail=(
                f"node_type {node_type!r} is blocked for AI agent use. "
                "Writer nodes (output / database_writer / cloud_storage_writer / "
                "catalog_writer) and kernel-executed nodes (python_script) "
                "cannot be staged by the agent — the user adds these "
                "manually. Suggest the node to the user instead, or pick "
                "a transformation node (filter, sort, group_by, formula, "
                "polars_code, select, …) for the next step."
            ),
        )

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

    # --- Stage 0: LLM-provided node_id validation ---
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
            violation = (
                f"equals one of its own upstream / right_input ids "
                f"({sorted(upstream_ids_set)}) — would create a self-loop"
            )
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
    # manual_input has a columnar ``raw_data_format`` schema but LLMs
    # naturally emit row-oriented data and dict-of-types columns.
    # Normalize before validation so the strict refusal path stays for
    # genuinely bad payloads.
    if node_type == "manual_input":
        tool_args = _normalize_manual_input_args(tool_args)
    try:
        settings = settings_cls.model_validate(tool_args)
    except ValidationError as exc:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="settings_validation",
            refusal_detail=_format_settings_validation_refusal(exc=exc, settings_cls=settings_cls, node_type=node_type),
        )

    # --- Refusal stage 1.5: upstream sink validation ---
    # The Tier-6 resolver fallback already filters sinks (see planner.py).
    # This guard catches the explicit case where the LLM named a sink in
    # ``upstream_node_ids`` directly (Tier 1) — the resolver respects
    # explicit intent, so the only place to refuse is here.
    upstream_check_ids = [uid for uid in (insertion_context.upstream_node_ids or []) if isinstance(uid, int)]
    if isinstance(insertion_context.right_input_node_id, int):
        upstream_check_ids.append(insertion_context.right_input_node_id)
    sink_upstreams = _detect_sink_upstreams(flow, upstream_check_ids)
    if sink_upstreams:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason="upstream_is_sink",
            refusal_detail=_format_sink_upstream_refusal(flow, sink_upstreams),
        )

    # --- Refusal stage 1.6: join-shaped wire validation ---
    # Defense-in-depth: the pick_upstream tool spec REQUIRES
    # left_input_node_id + right_input_node_id for join-shaped types,
    # and the planner's post-pick_upstream check enforces shape. But
    # providers (OpenRouter, Groq) don't always validate ``required``
    # strictly, and the LLM occasionally ignores the spec and emits the
    # legacy ``upstream_node_ids: [left, right]`` shape with
    # right_input_node_id null — both ids in the list. The downstream
    # ``flow_node.add_node_connection`` then silently OVERWRITES
    # ``main_inputs`` on the second main-port call (the input <= 2
    # branch sets ``main_inputs = [from_node]`` which clobbers
    # previous), leaving the join with only one wire (last write wins)
    # and the LLM gets *"applied"* without any error. Refuse here so
    # the error reaches the LLM and the retry path produces correct
    # shape.
    from flowfile_core.ai.tools.meta_ops import JOIN_SHAPED_NODE_TYPES

    if node_type in JOIN_SHAPED_NODE_TYPES:
        upstream_ids_for_check = list(insertion_context.upstream_node_ids or [])
        right_id_for_check = insertion_context.right_input_node_id
        violation: str | None = None
        if right_id_for_check is None:
            violation = (
                f"join-shaped node `{node_type}` requires a RIGHT input. Pick the "
                "right upstream via ``right_input_node_id`` (the spec for "
                f"{node_type} exposes ``left_input_node_id`` + ``right_input_node_id``, "
                "both required scalars). The LEFT input goes in "
                "``upstream_node_ids[0]``; do NOT put both ids in "
                "``upstream_node_ids`` — they go in separate fields. "
                "``left_input_node_id`` and ``right_input_node_id`` are "
                "ENVELOPE-LEVEL scalars: keys at the SAME level as the "
                f"``{node_type}_input`` settings object (a sibling, NOT a "
                "key inside it)."
            )
        elif len(upstream_ids_for_check) != 1:
            violation = (
                f"join-shaped node `{node_type}` requires exactly ONE LEFT upstream "
                f"(``upstream_node_ids`` must have one element); got "
                f"{upstream_ids_for_check!r}. The right input goes in "
                "``right_input_node_id`` (separate scalar field), not as a second "
                "entry in ``upstream_node_ids``. "
                "``left_input_node_id`` and ``right_input_node_id`` are "
                "ENVELOPE-LEVEL scalars: keys at the SAME level as the "
                f"``{node_type}_input`` settings object (a sibling, NOT a "
                "key inside it)."
            )
        elif upstream_ids_for_check[0] == right_id_for_check:
            violation = (
                f"join-shaped node `{node_type}` cannot use the same id "
                f"({right_id_for_check}) for both LEFT and RIGHT inputs — a node "
                "cannot join to itself. Pick two different upstream ids. "
                "``left_input_node_id`` and ``right_input_node_id`` are "
                "ENVELOPE-LEVEL scalars: keys at the SAME level as the "
                f"``{node_type}_input`` settings object (a sibling, NOT a "
                "key inside it)."
            )
        if violation is not None:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow.flow_id,
                refusal_reason="join_wire_invalid",
                refusal_detail=violation,
            )

    # Stamp the resolved layout coordinates onto the settings object.
    # The apply path (``_apply_add_node`` →
    # ``flow.add_<node_type>(settings)``) reads ``settings.pos_x`` /
    # ``settings.pos_y`` (via ``set_node_information``) when stamping the
    # canvas position; the ``InsertionContext`` itself is only consulted
    # for connection wiring. Only stamp when the LLM did NOT include
    # pos_x / pos_y in its tool_args — explicit caller intent (even
    # ``0.0``) wins. Detect via key presence in ``tool_args`` rather
    # than ``settings.pos_x == 0`` because ``NodeBase`` defaults pos_x /
    # pos_y to 0 and we can't distinguish "LLM said 0" from "LLM
    # omitted it" once Pydantic has run.
    settings_pos_x_explicit = "pos_x" in tool_args and tool_args["pos_x"] is not None
    settings_pos_y_explicit = "pos_y" in tool_args and tool_args["pos_y"] is not None
    if (
        insertion_context.pos_x is not None
        and insertion_context.pos_y is not None
        and hasattr(settings, "pos_x")
        and hasattr(settings, "pos_y")
        and not settings_pos_x_explicit
        and not settings_pos_y_explicit
    ):
        try:
            settings.pos_x = insertion_context.pos_x
            settings.pos_y = insertion_context.pos_y
        except (TypeError, ValueError, AttributeError):
            # Defensive — settings classes are Pydantic so assignment should
            # always succeed, but if a future class freezes the field we don't
            # want to fail the whole tool call over a layout cosmetic.
            logger.debug("could not stamp pos_x/pos_y on settings for %s", node_type)

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

        # --- Refusal stage 2.5: polars_code sandbox validation ---
        polars_rejection = _validate_polars_code_body_or_reject(
            node_type=node_type,
            code=code,
            tool_name=tool_name,
            redacted_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
        )
        if polars_rejection is not None:
            return polars_rejection

    # --- Resolve upstream schemas (tiers 0-1, warn on tier 2) ---
    upstream_ids = list(insertion_context.upstream_node_ids)
    if insertion_context.right_input_node_id is not None and insertion_context.right_input_node_id not in upstream_ids:
        upstream_ids.append(insertion_context.right_input_node_id)
    upstream_schemas, warnings = _resolve_upstream_schemas(flow, upstream_ids, staged_schemas=extra_upstream_schemas)

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
    # ``NodeConnection.create_from_simple_input`` takes the SEMANTIC
    # ``input_type`` ("main" / "right" / "left"), NOT the
    # connection-class string ("input-0" / "input-1"). Passing
    # ``input_type="input-0"`` / ``"input-1"`` falls through to the
    # default ``_ → "input-0"`` in the match block, so BOTH the main and
    # right wires would get connection_class "input-0" —
    # ``add_node_connection`` then routes both to ``main_inputs`` and
    # the second call silently overwrites the first (the ``input <= 2``
    # branch in flow_node.py clobbers main_inputs each time). Pass the
    # semantic names so the connection class lands correctly.
    for uid in main_ids:
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=uid, to_id=target_id, input_type="main", output_handle="output-0"
        )
        add_connection(flow, connection)
    if ctx.right_input_node_id is not None:
        right_connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=ctx.right_input_node_id, to_id=target_id, input_type="right", output_handle="output-0"
        )
        add_connection(flow, right_connection)
