"""Top-level dispatch — :func:`execute_tool_call` and ``_handle_graph``.

Parses the MCP-shaped tool name, resolves the target ``FlowGraph``, and
hands off to the per-domain handler. Apart from JSON-string unwrap and
secret redaction, this module does no validation of its own — every
refusal lands in a handler.
"""

from __future__ import annotations

from typing import Any

from flowfile_core.ai import safety
from flowfile_core.ai.tools.dry_run import DryRunCache

from ._internal import (
    _TOOL_NAME_RE,
    ExecutionMode,
    InsertionContext,
    ToolExecutionResult,
    _reject_and_audit,
    _resolve_flow,
)
from .coercions import _unwrap_json_string_values
from .handlers.add import _handle_add_node
from .handlers.codegen import _handle_codegen
from .handlers.connections import (
    _handle_connect,
    _handle_delete_connection,
    _handle_delete_node,
)
from .handlers.meta import _handle_meta
from .handlers.schema import _handle_schema
from .handlers.update import _handle_update_node_settings


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
    staged_offset_index: int = 0,
    extra_upstream_positions: dict[int, tuple[float, float]] | None = None,
    extra_upstream_schemas: dict[int, Any] | None = None,
) -> ToolExecutionResult:
    """Validate, predict, and dispatch a single LLM tool call.

    ``flow`` is optional — if not provided, looked up via
    ``flow_file_handler.get_flow(flow_id)``. Tests can pass an explicit flow
    to avoid touching the global handler.

    ``dry_run_cache`` is the per-session :class:`DryRunCache`. If ``None``, a
    fresh cache is created (no cross-call hit). Long-running planner sessions
    should reuse one cache so identical proposals don't re-pay the kernel cost.

    The planner sets ``llm_provided_node_id`` when the LLM emitted
    ``node_id`` itself (instead of letting the planner allocate); the
    executor then validates the id is fresh + non-self-looping.
    ``audit_meta`` rides on every ``add_*`` audit row under
    ``tool_args["__planner_meta__"]`` so future self-loops are
    diagnosable from the audit row alone (the existing
    ``AuditEvent.extra`` field is dropped before persistence).

    ``staged_offset_index`` is the count of prior in-batch staged adds
    anchored at the same upstream. Callers (planner / Cmd+K) thread it
    so fan-outs from one upstream stack vertically instead of overlapping.
    Only consulted when ``insertion_context.pos_x`` / ``pos_y`` are both
    ``None``.

    ``extra_upstream_positions`` is a caller-supplied
    ``{node_id: (pos_x, pos_y)}`` map merged into the upstream lookup
    before the live graph. The planner uses this to anchor chained adds
    onto prior in-batch staged-but-unapplied upstreams (which by
    definition aren't in ``flow.nodes`` yet).
    """
    # Universal lenient JSON-string unwrap for CONTAINERS. Smaller
    # open-weights models (llama-3.3-70b, in particular) routinely pass
    # structured tool args as JSON-encoded strings rather than native
    # objects / arrays. ``upstream_node_ids: "[3]"``, ``groupby_input:
    # "{\"agg_cols\": ...}"`` — Pydantic cannot reverse-coerce these and
    # burns retry budget on a recoverable type-wrap mistake. Apply the
    # unwrap pass at the top of dispatch so every handler (add_*,
    # update_node_settings, the meta ops, schema ops) gets the same
    # forgiveness uniformly.
    #
    # Scalar str→int / str→float coercion (``node_id: "5"``) is left to
    # Pydantic's lax-mode model validation — see
    # :func:`_unwrap_json_string_values` for the rationale (eager scalar
    # unwrapping corrupted str-typed fields with numeric content like
    # ``BasicFilter.value``).
    if tool_args:
        tool_args = _unwrap_json_string_values(tool_args)
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
            staged_offset_index=staged_offset_index,
            extra_upstream_positions=extra_upstream_positions,
            extra_upstream_schemas=extra_upstream_schemas,
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
    staged_offset_index: int = 0,
    extra_upstream_positions: dict[int, tuple[float, float]] | None = None,
    extra_upstream_schemas: dict[int, Any] | None = None,
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
            staged_offset_index=staged_offset_index,
            extra_upstream_positions=extra_upstream_positions,
            extra_upstream_schemas=extra_upstream_schemas,
        )

    if op == "connect":
        return _handle_connect(
            tool_name,
            tool_args,
            redacted_args,
            flow,
            session_id,
            user_id,
            mode,
            audit_meta=audit_meta,
        )
    if op == "delete_node":
        return _handle_delete_node(tool_name, tool_args, redacted_args, flow, session_id, user_id, mode)
    if op == "delete_connection":
        return _handle_delete_connection(tool_name, tool_args, redacted_args, flow, session_id, user_id, mode)
    if op == "update_node_settings":
        return _handle_update_node_settings(
            tool_name=tool_name,
            tool_args=tool_args,
            redacted_args=redacted_args,
            flow=flow,
            session_id=session_id,
            user_id=user_id,
            mode=mode,
            dry_run_cache=dry_run_cache,
            extra_upstream_schemas=extra_upstream_schemas,
        )

    # ``run_node`` / ``propose_subgraph`` are not in the catalog —
    # autonomous run-node is unsafe (worker collects, user code,
    # external systems); propose_subgraph is redundant with the
    # planner's per-step staging. This rejection branch stays as
    # defence-in-depth in case a future workstream re-adds either before
    # wiring an implementation.
    return _reject_and_audit(
        tool_name=tool_name,
        tool_args=redacted_args,
        session_id=session_id,
        user_id=user_id,
        flow_id=flow.flow_id,
        refusal_reason=None,
        refusal_detail=f"graph op {op!r} is not in the agent's catalog",
    )
