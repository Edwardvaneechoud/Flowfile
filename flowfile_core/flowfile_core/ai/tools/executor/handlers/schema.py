"""Read-only schema introspection ops — ``read_node_schema`` etc."""

from __future__ import annotations

from typing import Any

from flowfile_core.ai.tools.predictor import schema_to_dict_list

from .._internal import ToolExecutionResult, _record_event, _reject_and_audit
from ..coercions import _coerce_to_int_or_none


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
                f"{op} requires an integer node_id (got {got_type}). "
                'Example payload: {"flow_id": 1, "node_id": 5}. '
                "Pass node ids as integers, not JSON-encoded strings."
            ),
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
        # Preview is metadata-only — actual sample-row return is not
        # wired here. Use the existing GET /node?get_data=true path for
        # row data.
        return _reject_and_audit(
            tool_name=tool_name,
            tool_args=redacted_args,
            session_id=session_id,
            user_id=user_id,
            flow_id=flow.flow_id,
            refusal_reason=None,
            refusal_detail="schema.read_node_preview not implemented; use the existing GET /node?get_data=true",
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
