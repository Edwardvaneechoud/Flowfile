"""Staged meta ops — classify_intent, pick_node_type, pick_upstream, etc.

All meta ops follow the same shape: validate the LLM-provided value
against the schema (enum / type), emit one ``AuditEvent``, return
``status="applied"`` with the chosen value(s) on ``extra`` so the
planner can mutate session state.
"""

from __future__ import annotations

from typing import Any

from flowfile_core.ai import safety
from flowfile_core.ai.tools.meta_ops import OP_KIND_NAMES
from flowfile_core.schemas.schemas import (
    NODE_TYPE_TO_SETTINGS_CLASS,
    get_settings_class_for_node_type,
)

from .._internal import ToolExecutionResult, _record_event, _reject_and_audit
from ..coercions import _coerce_to_int_list_or_self, _coerce_to_int_or_self


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
    """Dispatch the staged meta ops.

    * ``classify_intent`` — stage 0: returns ``extra["op_kind"]``.
    * ``pick_node_type`` — stage 1: returns ``extra["node_type"]``.
    * ``pick_upstream`` — stage 2: returns
      ``extra["upstream_node_ids"]`` and ``extra["right_input_node_id"]``.
    """
    if op == "emit_plan":
        plan = tool_args.get("plan")
        rationale = tool_args.get("rationale", "")
        if not isinstance(plan, str) or not plan.strip():
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=(
                    "emit_plan: ``plan`` must be a non-empty string "
                    "containing the markdown plan (numbered list of "
                    "≤6 steps; each step names a node_type and a "
                    "one-sentence description)."
                ),
            )
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
            extra={"plan": plan, "rationale": str(rationale or "")},
        )

    if op == "classify_intent":
        op_kind = tool_args.get("op_kind")
        rationale = tool_args.get("rationale", "")
        if not isinstance(op_kind, str) or op_kind not in OP_KIND_NAMES:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=(
                    f"classify_intent: op_kind {op_kind!r} not one of "
                    f"{list(OP_KIND_NAMES)}"
                ),
            )
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
            extra={"op_kind": op_kind, "rationale": str(rationale or "")},
        )

    if op == "verify_completion":
        # Opt-in verify-completion gate. The LLM certifies whether every
        # step of the user's plan has a corresponding successful tool
        # call. is_complete=True → loop terminates; False → planner
        # restarts at classify for the missing steps.
        is_complete = tool_args.get("is_complete")
        rationale = tool_args.get("rationale", "")
        if not isinstance(is_complete, bool):
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=(
                    f"verify_completion: is_complete must be a boolean "
                    f"(true/false); got {type(is_complete).__name__} "
                    f"{is_complete!r}."
                ),
            )
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
            extra={"is_complete": is_complete, "rationale": str(rationale or "")},
        )

    if op == "pick_node_type":
        node_type = tool_args.get("node_type")
        rationale = tool_args.get("rationale", "")
        if not isinstance(node_type, str) or get_settings_class_for_node_type(node_type) is None:
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=(
                    f"pick_node_type: node_type {node_type!r} is not a registered "
                    f"Flowfile node type. Known: {sorted(NODE_TYPE_TO_SETTINGS_CLASS.keys())}"
                ),
            )
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
            extra={"node_type": node_type, "rationale": str(rationale or "")},
        )

    if op == "pick_upstream":
        # For join-shaped node types the spec uses a symmetric scalar
        # pair (``left_input_node_id`` + ``right_input_node_id``). When
        # the LLM emits that shape, translate to the legacy
        # list+scalar representation BEFORE the coercion / validation
        # runs so the downstream consumers (planner session state,
        # ``_handle_add_node`` insertion context) see the canonical
        # form.
        raw_left_input = tool_args.get("left_input_node_id")
        if raw_left_input is not None:
            raw_left_coerced = _coerce_to_int_or_self(raw_left_input)
            if isinstance(raw_left_coerced, int) and not isinstance(raw_left_coerced, bool):
                tool_args = dict(tool_args)  # copy, then translate
                tool_args["upstream_node_ids"] = [raw_left_coerced]
                tool_args.pop("left_input_node_id", None)
                redacted_args = safety.redact_secrets(tool_args)
        raw_upstream = tool_args.get("upstream_node_ids")
        raw_right = tool_args.get("right_input_node_id")
        rationale = tool_args.get("rationale", "")

        # Coerce common llama-70b mis-shapes back to the expected
        # list[int]. Without this, every llama-3.3-70b run spends its
        # retry budget on the same predictable type errors. Three
        # mistakes seen in practice:
        #   1. ``upstream_node_ids: 4`` (single int, not wrapped in a list)
        #   2. ``upstream_node_ids: "4"`` (JSON-encoded as string)
        #   3. ``upstream_node_ids: "[4]"`` or ``"4,5"`` (CSV string)
        # Each gets coerced to ``[4]`` / ``[4, 5]`` rather than
        # rejected. We only coerce — if the result still doesn't match
        # list[int] the original rejection fires below.
        raw_upstream = _coerce_to_int_list_or_self(raw_upstream)
        raw_right = _coerce_to_int_or_self(raw_right)

        if not isinstance(raw_upstream, list):
            return _reject_and_audit(
                tool_name=tool_name,
                tool_args=redacted_args,
                session_id=session_id,
                user_id=user_id,
                flow_id=flow_id,
                refusal_reason=None,
                refusal_detail=(
                    "pick_upstream: upstream_node_ids must be a list of integers, "
                    f"got {type(raw_upstream).__name__}. Pass a list like [3] "
                    "or [3, 4]; not a string, not a single integer."
                ),
            )
        upstream_ids: list[int] = []
        for uid in raw_upstream:
            if isinstance(uid, int) and not isinstance(uid, bool):
                upstream_ids.append(uid)
            else:
                return _reject_and_audit(
                    tool_name=tool_name,
                    tool_args=redacted_args,
                    session_id=session_id,
                    user_id=user_id,
                    flow_id=flow_id,
                    refusal_reason=None,
                    refusal_detail=(
                        f"pick_upstream: upstream_node_ids contains non-integer {uid!r}. "
                        "Each entry must be one of the live node ids in the enum."
                    ),
                )

        right_input_id: int | None = None
        if raw_right is not None:
            if isinstance(raw_right, int) and not isinstance(raw_right, bool):
                right_input_id = raw_right
            else:
                return _reject_and_audit(
                    tool_name=tool_name,
                    tool_args=redacted_args,
                    session_id=session_id,
                    user_id=user_id,
                    flow_id=flow_id,
                    refusal_reason=None,
                    refusal_detail=(
                        "pick_upstream: right_input_node_id must be an integer or null, "
                        f"got {type(raw_right).__name__}"
                    ),
                )

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
            extra={
                "upstream_node_ids": upstream_ids,
                "right_input_node_id": right_input_id,
                "rationale": str(rationale or ""),
            },
        )

    return _reject_and_audit(
        tool_name=tool_name,
        tool_args=redacted_args,
        session_id=session_id,
        user_id=user_id,
        flow_id=flow_id,
        refusal_reason=None,
        refusal_detail=f"unknown meta op: {op!r}",
    )
