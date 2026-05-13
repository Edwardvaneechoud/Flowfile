"""``update_node_settings`` handler.

Mirrors :func:`_handle_add_node`'s shape: validates new settings via the
Pydantic class for the live node's type, runs the network-egress check
for code-bearing settings, resolves upstream schemas via the existing
wiring (modifications never rewire), validates column references,
predicts the new output schema, then either stages the modification
for a :class:`GraphDiff` or re-fires the production ``add_<node_type>``
path so the live node inherits the new settings.
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
from flowfile_core.schemas.schemas import get_settings_class_for_node_type

from .._internal import (
    ExecutionMode,
    ResultStatus,
    ToolExecutionResult,
    _extract_code,
    _record_event,
    _reject_and_audit,
    _resolve_output_names,
)
from ..coercions import _coerce_to_int_or_none
from ..refusals import _format_settings_validation_refusal
from .add import _validate_polars_code_body_or_reject, _validate_python_script_body_or_reject

logger = logging.getLogger(__name__)


def _handle_update_node_settings(
    *,
    tool_name: str,
    tool_args: dict[str, Any],
    redacted_args: dict[str, Any],
    flow,
    session_id: str,
    user_id: int,
    mode: ExecutionMode,
    dry_run_cache: DryRunCache,
    extra_upstream_schemas: dict[int, Any] | None = None,
) -> ToolExecutionResult:
    """Modify an existing node's settings.

    The stage payload carries ``kind="modification"`` plus old and new
    settings dicts so the diff preview can render an old-vs-new view;
    the old-settings capture happens at stage time (not apply time) so a
    rejected diff reverts to what the user was looking at when reviewing.
    """
    node_id_raw = tool_args.get("node_id")
    node_id = _coerce_to_int_or_none(node_id_raw)
    if node_id is None:
        got_type = type(node_id_raw).__name__
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=(
                f"update_node_settings requires an integer node_id (got {got_type}). "
                'Example payload: {"flow_id": 1, "node_id": 5, "settings": {...}}. '
                "Pass node ids as integers, not JSON-encoded strings."
            ),
        )

    settings_payload = tool_args.get("settings")
    if not isinstance(settings_payload, dict):
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail="update_node_settings requires a settings object",
        )

    target_node = flow.get_node(node_id)
    if target_node is None:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"node {node_id} not found in flow {flow.flow_id}",
        )

    node_type = getattr(target_node, "node_type", None)
    if not isinstance(node_type, str) or not node_type:
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail=f"node {node_id} has no resolvable node_type",
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

    # Capture the live (pre-change) settings BEFORE validation. This is the
    # truth the diff-preview UI renders against and the value Reject would
    # restore — we want it stable even if the new settings later fail.
    old_settings_dict: dict[str, Any] = {}
    current_settings = getattr(target_node, "setting_input", None)
    if isinstance(current_settings, BaseModel):
        try:
            old_settings_dict = current_settings.model_dump(mode="json")
        except Exception:  # noqa: BLE001 — defensive serialisation
            logger.warning("could not serialise existing settings for node %s", node_id)

    # --- Refusal stage 1: Pydantic shape ---
    try:
        new_settings = settings_cls.model_validate(settings_payload)
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

    # --- Refusal stage 2: network egress (code-bearing nodes only) ---
    code = _extract_code(node_type, new_settings)
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

        # --- Refusal stage 2.6: python_script sandbox validation ---
        script_rejection = _validate_python_script_body_or_reject(
            node_type=node_type,
            code=code,
            tool_name=tool_name,
            redacted_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
        )
        if script_rejection is not None:
            return script_rejection

    # --- Resolve upstream schemas from the live node's existing wiring ---
    # Modifications never rewire the topology — the LLM must use connect /
    # delete_connection for that — so we read upstream ids off the live
    # node rather than from the new settings (which might carry a stale
    # ``depending_on_id`` we don't want to honour for schema resolution).
    main_input_ids: list[int] = []
    main_inputs = getattr(target_node.node_inputs, "main_inputs", None) or []
    for upstream in main_inputs:
        try:
            main_input_ids.append(int(upstream.node_id))
        except (TypeError, ValueError, AttributeError):
            continue
    right_input_obj = getattr(target_node.node_inputs, "right_input", None)
    right_input_id: int | None = None
    if right_input_obj is not None:
        try:
            right_input_id = int(right_input_obj.node_id)
        except (TypeError, ValueError, AttributeError):
            right_input_id = None

    upstream_ids = list(main_input_ids)
    if right_input_id is not None and right_input_id not in upstream_ids:
        upstream_ids.append(right_input_id)
    upstream_schemas, warnings = _resolve_upstream_schemas(flow, upstream_ids, staged_schemas=extra_upstream_schemas)

    # --- Refusal stage 3: column refs ---
    refs = collect_column_refs(node_type, new_settings)
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
        elif not upstream_schemas and main_input_ids:
            warnings.append("upstream schema unresolved — kernel dry-run skipped; output schema deferred until run")
        elif not main_input_ids:
            warnings.append(f"dynamic node {node_type} has no upstream; output schema cannot be predicted")
        else:
            try:
                predicted = dry_run_code(
                    flow=flow,
                    node_id=node_id,
                    upstream_node_ids=main_input_ids,
                    code=code,
                    output_names=_resolve_output_names(node_type, new_settings),
                    cache=dry_run_cache,
                    upstream_schemas=upstream_schemas,
                )
            except Exception as exc:
                logger.warning("dry-run failed for %s/%s on update: %s", node_type, node_id, exc)
                warnings.append(f"kernel dry-run failed: {exc}; output schema deferred")
    else:
        predicted = predict_schema_via_mirror(
            node_type,
            new_settings,
            upstream_schemas,
            right_input_node_id=right_input_id,
        )

    # --- Audit annotation: flag database_reader query changes as high-risk ---
    db_query_changed = False
    if node_type == "database_reader":
        old_db = old_settings_dict.get("database_settings") or {}
        new_db = new_settings.model_dump(mode="json").get("database_settings") or {}
        old_query = old_db.get("query")
        new_query = new_db.get("query")
        if old_query is not None and new_query is not None and old_query != new_query:
            db_query_changed = True
            logger.warning(
                "high-risk: database_reader query changed by AI agent",
                extra={"node_id": node_id, "session_id": session_id},
            )
            warnings.append("high-risk: database_reader query was modified by the agent")

    # --- Apply or stage ---
    if mode == "stage":
        payload = {
            "kind": "modification",
            "node_id": node_id,
            "node_type": node_type,
            "old_settings": old_settings_dict,
            "new_settings": new_settings.model_dump(mode="json"),
            "predicted_output_schema": schema_to_dict_list(predicted),
            "high_risk": db_query_changed,
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
            node_id=node_id,
            predicted_output_schema=schema_to_dict_list(predicted),
            warnings=warnings,
            audit_id=audit_row.id if audit_row is not None else None,
            executed_args=redacted_args,
            staged_node_payload=payload,
        )

    # mode == "apply"
    try:
        _apply_update_node_settings(flow, node_type, new_settings)
    except Exception as exc:
        logger.warning("apply failed for update_node_settings %s/%s: %s", node_type, node_id, exc)
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
        node_id=node_id,
        predicted_output_schema=schema_to_dict_list(predicted),
        warnings=warnings,
        audit_id=audit_row.id if audit_row is not None else None,
        executed_args=redacted_args,
    )


def _apply_update_node_settings(flow, node_type: str, settings: BaseModel) -> None:
    """Real-graph mutation: re-fire ``add_<node_type>`` so the existing node
    inherits the new settings.

    The same path the production UI takes from
    ``POST /editor/update_settings/`` (``routes.py:887``) — ``add_node_step``
    detects the existing node id and routes through ``existing_node.update_node``
    rather than constructing a fresh :class:`FlowNode`. Wiring is preserved
    (``existing_node.all_inputs`` is kept verbatim — the modification never
    rewires upstream / right-input).
    """
    add_method = getattr(flow, f"add_{node_type}", None)
    if add_method is None:
        raise ValueError(f"flow has no add_{node_type} method")
    add_method(settings)
