"""``flowfile.codegen.*`` ops.

Codegen tools are LLM-facing proposals — the LLM should call
``flowfile.graph.add_polars_code`` etc. directly with its generated
code. This stub returns a friendly hint.
"""

from __future__ import annotations

from typing import Any

from .._internal import ToolExecutionResult, _record_event


def _handle_codegen(
    *,
    op: str,
    tool_name: str,
    redacted_args: dict[str, Any],
    session_id: str,
    user_id: int,
    flow_id: int,
) -> ToolExecutionResult:
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
                "Codegen tools are not directly executed. Generate the code "
                f"yourself and call flowfile.graph.add_{op.replace('generate_', '').replace('_code', '_code')} "
                "with the proposed snippet."
            ),
        },
    )
