"""Shared types, constants, audit helpers, and small utilities used across
the executor package.

Single internal module that other executor files (and handlers) import
from. Keeps the dependency DAG acyclic — handlers depend on this, not
on each other; ``dispatch`` depends on this + handlers.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any, Final, Literal

from pydantic import BaseModel, Field

from flowfile_core.ai import audit, safety

if TYPE_CHECKING:
    pass

logger = logging.getLogger("flowfile_core.ai.tools.executor")


ExecutionMode = Literal["apply", "stage"]
ResultStatus = Literal["applied", "staged", "warned", "rejected"]

_TOOL_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^flowfile\.(graph|schema|codegen|meta)\.(.+)$")


#: Settings field paths for code-bearing nodes — used for the network-egress
#: check. Each tuple is ``(node_type, attr_path)``.
_CODE_BEARING: Final[dict[str, tuple[str, ...]]] = {
    "polars_code": ("polars_code_input", "polars_code"),
    "python_script": ("python_script_input", "code"),
    "sql_query": ("sql_query_input", "sql_code"),
}

# Refusal text for polars_code bodies that include ``import`` statements.
# Only rendered to the LLM when the LLM has already made the mistake;
# the standing prompt has no mention of the imports rule, so we don't
# burn context on every polars_code-related round.
#
# Rationale: ``polars_code_parser._validate_code`` rejects ALL
# ``ast.Import`` / ``ast.ImportFrom`` nodes. ``flow.add_polars_code``
# swallows the ValueError into ``node.results.errors`` — the LLM never
# sees it and the failure surfaces only at run time. The pre-flight in
# ``_handle_add_node`` / ``_handle_update_node_settings`` short-circuits
# with this refusal so the LLM gets one round to fix the body using the
# pre-bound names.
_POLARS_CODE_IMPORT_REFUSAL: Final[str] = (
    "polars_code rejected: imports are forbidden in this sandbox. "
    "``pl`` is already available — drop ``import polars as pl`` "
    "(and any ``from polars import ...``) and resubmit."
)


#: Layout offsets used by :func:`_resolve_insertion_position` when
#: ``InsertionContext.pos_x`` / ``pos_y`` are unset. Mirrors the canonical
#: spacings of :func:`flowfile_core.flowfile.util.calculate_layout.calculate_layered_layout`
#: (``x_spacing=250, y_spacing=100, initial_y=50``) so AI-staged nodes lay out
#: with the same density the auto-layout helper would produce.
_AUTO_LAYOUT_X_SPACING: Final[float] = 250.0
_AUTO_LAYOUT_Y_SPACING: Final[float] = 100.0
_AUTO_LAYOUT_FALLBACK_X: Final[float] = 50.0
_AUTO_LAYOUT_FALLBACK_Y: Final[float] = 50.0


class InsertionContext(BaseModel):
    """Where a new node attaches to the existing graph.

    ``upstream_node_ids`` are connected to ``input-0`` (main); the optional
    ``right_input_node_id`` is connected to ``input-1`` (right) for joins.

    ``pos_x`` / ``pos_y`` may be ``None`` to ask the executor to derive a
    layout position from the upstream's canvas coordinates. When the
    caller wants (0, 0) literally, it must pass ``0.0`` explicitly — the
    sentinel-vs-default distinction is the whole point of the ``None`` shape.
    """

    upstream_node_ids: list[int] = Field(default_factory=list)
    right_input_node_id: int | None = None
    pos_x: float | None = None
    pos_y: float | None = None


def _resolve_insertion_position(
    flow,
    upstream_node_ids: list[int],
    *,
    staged_offset_index: int = 0,
    extra_upstream_positions: dict[int, tuple[float, float]] | None = None,
) -> tuple[float, float]:
    """Derive ``(pos_x, pos_y)`` for an AI-staged node from the live graph.

    The most-recent upstream node anchors the new node; the helper offsets
    horizontally by :data:`_AUTO_LAYOUT_X_SPACING` and vertically by
    ``staged_offset_index * _AUTO_LAYOUT_Y_SPACING``. ``staged_offset_index``
    is the count of prior in-batch staged adds anchored at the same upstream;
    callers (planner / Cmd+K) thread it so fan-outs from one upstream stack
    instead of overlapping.

    ``extra_upstream_positions`` is a caller-supplied lookup
    ``{node_id: (pos_x, pos_y)}`` consulted before the live graph. The
    planner threads its in-batch staged-but-unapplied adds through here
    because chained transformations (filter → sort) anchor on the prior
    staged add, which by definition hasn't been applied to ``flow.nodes``
    yet.

    Cold flow (``upstream_node_ids`` empty or no live upstream resolves):
    fall back to ``(_AUTO_LAYOUT_FALLBACK_X, _AUTO_LAYOUT_FALLBACK_Y)``
    plus the staged-offset y-stack so multiple cold-flow adds in one batch
    don't collapse onto each other either.

    The helper reads ``setting_input.pos_x`` / ``pos_y`` off
    :class:`flowfile_core.flowfile.flow_node.flow_node.FlowNode` because that
    is the persistent canvas position carried on every node settings model
    (``NodeBase.pos_x`` / ``pos_y``). Live ``node_information`` mirrors the
    same value but only as ``int``.
    """
    upstream_pos: tuple[float, float] | None = None
    for uid in reversed(upstream_node_ids):
        if uid is None:
            continue
        if extra_upstream_positions and uid in extra_upstream_positions:
            cand = extra_upstream_positions[uid]
            if (
                isinstance(cand, tuple)
                and len(cand) == 2
                and isinstance(cand[0], int | float)
                and isinstance(cand[1], int | float)
            ):
                upstream_pos = (float(cand[0]), float(cand[1]))
                break
        node = flow.get_node(uid)
        if node is None:
            continue
        setting_input = getattr(node, "setting_input", None)
        if setting_input is None:
            continue
        ux = getattr(setting_input, "pos_x", None)
        uy = getattr(setting_input, "pos_y", None)
        if isinstance(ux, int | float) and isinstance(uy, int | float):
            upstream_pos = (float(ux), float(uy))
            break

    if upstream_pos is None:
        return (
            _AUTO_LAYOUT_FALLBACK_X,
            _AUTO_LAYOUT_FALLBACK_Y + staged_offset_index * _AUTO_LAYOUT_Y_SPACING,
        )

    base_x, base_y = upstream_pos
    return (
        base_x + _AUTO_LAYOUT_X_SPACING,
        base_y + staged_offset_index * _AUTO_LAYOUT_Y_SPACING,
    )


class ToolExecutionResult(BaseModel):
    """Outcome of one ``execute_tool_call``.

    * ``applied`` — node was added/wired to the real graph (mode=apply, no
      schema warnings).
    * ``staged`` — settings + predicted schema captured for the diff
      layer (mode=stage), no real-graph mutation.
    * ``warned`` — tool ran but produced schema warnings (e.g. upstream
      schema unknown; column-ref validation deferred until run).
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


def _resolve_flow(flow_id: int):
    """Look up a ``FlowGraph`` via the global handler. Lazy import to keep the
    executor's module-level imports light (the handler pulls in DB session
    machinery which other AI tests don't need)."""
    from flowfile_core.flowfile.handler import flow_file_handler

    return flow_file_handler.get_flow(flow_id)
