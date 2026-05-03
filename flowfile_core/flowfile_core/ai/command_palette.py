"""Command-palette logic — owned by W33.

Pipes the cmd_k surface through the same composition every other AI
surface uses: W22 prompt context → W30 tool catalog → W11 provider call
under W14 scheduler → W31 executor (mode="stage") → W41 :class:`GraphDiff`
register. The headline UX is "type a short imperative on the canvas, get
a staged diff to accept".

Soft failures (timeout, no tool calls, parse error, provider error,
all-refused) become a :class:`CommandPaletteResponse` with ``degraded=True``
and a stable ``reason`` string — never 5xx — to mirror W34's autocomplete
posture. The function never raises.

Why a non-streaming POST and not SSE: cmd_k is a single round-trip with
a sub-1s TTFB target (D010). Streaming overhead (header flush, framing,
client SSE parser) is more expensive than a JSON response the client
just inspects. SSE remains the right shape for chat (W20), run-failure
explanations (W23), inline ✨ actions (W21), docgen (W50), and lineage
(W51) — long-form text the user reads as it arrives.

Surface lockstep: ``"cmd_k"`` is already part of W30's :data:`SurfaceLiteral`,
W22's :data:`SURFACE_TO_LEVEL` (→ ``"copilot"``), W22's per-surface
budget table, and every provider's ``surface_models``. W33 only consumes;
no new surface vocabulary is introduced.

The lazy-litellm contract from W11 / W12 / W13 is preserved — this module
must not import ``litellm`` at module load time. Provider calls flow
through the :class:`flowfile_core.ai.providers.base.Provider` Protocol seam.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from flowfile_core.ai.context import render_prompt_context
from flowfile_core.ai.diff import (
    GraphDiff,
    StagedAddition,
    StagedConnection,
    StagedDeletion,
    StagedInsertionContext,
    register_diff,
)
from flowfile_core.ai.metrics import record_autocomplete_call
from flowfile_core.ai.providers.base import Message, Provider, ToolCall
from flowfile_core.ai.scheduler import RateLimitScheduler, default_scheduler
from flowfile_core.ai.tools import (
    InsertionContext,
    ToolExecutionResult,
    build_tool_catalog,
    execute_tool_call,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


SURFACE: Final[str] = "cmd_k"
"""The :data:`SurfaceLiteral` value W33 emits — kept as a constant so call
sites and test assertions stay in lock-step."""

DEFAULT_TIMEOUT_SECONDS: Final[float] = 8.0
"""Hard timeout per call. D010 target is sub-1s TTFB / <3s total but BYOK
quality varies — 8s caps a slow provider before the user gives up."""

DEFAULT_MAX_TOKENS: Final[int] = 1024
"""Generous ceiling for a short imperative answer. Used when the request
body omits ``max_tokens``."""

MAX_OPS_PER_REQUEST: Final[int] = 6
"""Upper bound on tool calls processed per request. The cmd_k preset has
~6 tools; if the LLM emits more than that we cap to keep latency bounded."""

DegradedReason = Literal[
    "timeout",
    "no_tool_calls",
    "provider_error",
    "all_refused",
    "empty_catalog",
]


# --------------------------------------------------------------------------- #
# Wire types                                                                   #
# --------------------------------------------------------------------------- #


class CommandPaletteInsertionContext(BaseModel):
    """Where the proposed nodes attach to the existing graph.

    Mirrors W31's :class:`flowfile_core.ai.tools.executor.InsertionContext`
    field-for-field so the request body can carry the same shape the
    executor expects without an adapter. Defaults align with cmd_k:
    ``upstream_node_ids=[]`` lets the LLM propose a source node;
    practical use threads the user's selection or the canvas-context-menu
    target node id in here.
    """

    model_config = ConfigDict(extra="forbid")

    upstream_node_ids: list[int] = Field(default_factory=list)
    right_input_node_id: int | None = None
    pos_x: float = 0.0
    pos_y: float = 0.0


class CommandPaletteRequest(BaseModel):
    """Body for ``POST /ai/command_palette``."""

    model_config = ConfigDict(extra="forbid")

    flow_id: int = Field(ge=0)
    prompt: str = Field(min_length=1, max_length=2_000)
    provider: str = Field(min_length=1)
    model: str | None = None
    selected_node_ids: list[int] | None = None
    insertion_context: CommandPaletteInsertionContext | None = None
    max_tokens: int | None = Field(default=None, gt=0)
    session_id: str | None = Field(default=None, min_length=1, max_length=128)
    timeout: float | None = Field(default=None, gt=0.0, le=30.0)


class RefusedToolCall(BaseModel):
    """A tool call that was rejected by the W31 executor.

    The frontend surfaces these inline so the user understands why a
    proposed action didn't make it into the staged diff (e.g. "missing
    columns: foo"). ``warnings`` carries any D011 deferred-validation
    notes.
    """

    model_config = ConfigDict(extra="forbid")

    tool_name: str
    refusal_reason: str | None = None
    refusal_detail: str | None = None
    warnings: list[str] = Field(default_factory=list)


class CommandPaletteResponse(BaseModel):
    """Response for both the route and :func:`run_command_palette`.

    On success: ``diff_id`` and ``diff`` are populated; the frontend feeds
    ``diff`` into :class:`useAiDiffStore.setCurrentDiff` and the user
    accepts/rejects via the existing W35 panel.

    On soft failure: ``degraded=True`` and ``reason`` is set; ``diff_id``
    and ``diff`` are ``None``. The frontend renders the rationale (if
    any) plus the degraded reason.
    """

    model_config = ConfigDict(extra="forbid")

    diff_id: str | None = None
    op_count: int = 0
    rationale: str | None = None
    degraded: bool = False
    reason: DegradedReason | None = None
    diff: GraphDiff | None = None
    refused: list[RefusedToolCall] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Prompt composition                                                           #
# --------------------------------------------------------------------------- #


_SYSTEM_PROMPT_SUFFIX: Final[str] = (
    "You are answering a Cmd+K command-palette request. The user typed a short "
    "imperative instruction (for example, 'filter to last 30 days', 'rename "
    "snake_case', 'select id and amount only'). Propose ONE OR MORE typed tool "
    "calls that, when staged, will produce the requested mutation.\n"
    "\n"
    "Hard rules:\n"
    "- Only call tools from the surface offered to you. Do NOT invent tool names.\n"
    "- Reference only columns that appear in the upstream schema for the target node.\n"
    "- Set every settings dict's `flow_id` and `node_id` to 0 — the server reassigns them.\n"
    "- Set `insertion_context` so newly added nodes chain after the upstream node id you were told to use.\n"
    "- Prefer fewer ops. Single-op responses are best for cmd_k.\n"
    "- If you cannot construct a valid tool call (missing column, ambiguous instruction, "
    "or the request needs a tool that isn't in your surface), respond with NO tool calls "
    "and explain in plain text why.\n"
    "\n"
    "Use your message content as a one-line rationale the user will see next to the diff."
)


def _resolve_pinned_node_ids(
    *,
    selected_node_ids: list[int] | None,
    insertion_context: CommandPaletteInsertionContext | None,
) -> list[int]:
    """Compose the W22 ``pinned_node_ids`` set from the request inputs.

    Order: explicit selection wins; the insertion-context upstreams + right
    input get appended deduplicated. Empty result lets the W22 builder
    fall back to ``@flow`` so the LLM still sees the whole graph instead
    of an empty subgraph.
    """
    pinned: list[int] = []
    seen: set[int] = set()
    if selected_node_ids:
        for nid in selected_node_ids:
            if nid not in seen:
                seen.add(nid)
                pinned.append(nid)
    if insertion_context is not None:
        for nid in insertion_context.upstream_node_ids:
            if nid not in seen:
                seen.add(nid)
                pinned.append(nid)
        rid = insertion_context.right_input_node_id
        if rid is not None and rid not in seen:
            seen.add(rid)
            pinned.append(rid)
    return pinned


def _build_messages_for_palette(
    *,
    graph: FlowGraph,
    prompt: str,
    selected_node_ids: list[int] | None,
    insertion_context: CommandPaletteInsertionContext | None,
) -> list[Message]:
    """Compose ``[system, user]`` for the cmd_k request.

    The system message stacks W22's layered prompt (base + copilot suffix,
    per :data:`SURFACE_TO_LEVEL["cmd_k"]`) on top of
    :data:`_SYSTEM_PROMPT_SUFFIX`. The user message is W22's deterministic
    subgraph block plus a ``## User request`` paragraph carrying the
    typed prompt + the insertion context the server already knows about.
    """
    pinned = _resolve_pinned_node_ids(
        selected_node_ids=selected_node_ids,
        insertion_context=insertion_context,
    )
    mention_text = "@flow" if not pinned else None

    ctx = render_prompt_context(
        graph,
        pinned,
        surface=SURFACE,
        samples_mode="off",  # D009 default; D012-clean.
        mentions=mention_text,
    )

    system_content = "\n".join([ctx.system, "", _SYSTEM_PROMPT_SUFFIX])

    user_lines = [
        ctx.user,
        "",
        "## User request",
        "",
        prompt.strip(),
    ]
    if insertion_context is not None and insertion_context.upstream_node_ids:
        user_lines.append("")
        user_lines.append(
            "Insertion context: any new node should attach after upstream node "
            f"{insertion_context.upstream_node_ids}."
        )
        if insertion_context.right_input_node_id is not None:
            user_lines.append(f"For joins, use right_input_node_id={insertion_context.right_input_node_id}.")
    user_content = "\n".join(user_lines)

    return [
        Message(role="system", content=system_content),
        Message(role="user", content=user_content),
    ]


# --------------------------------------------------------------------------- #
# Provider call wrapper                                                        #
# --------------------------------------------------------------------------- #


async def _call_provider_for_tools(
    *,
    provider: Provider,
    messages: list[Message],
    tool_specs: list,
    max_tokens: int,
    timeout: float,
    scheduler: RateLimitScheduler,
) -> tuple[Any | None, str | None]:
    """Invoke ``provider.chat`` with tools enabled and a hard timeout.

    Returns ``(response, error_reason)``. ``response`` is the LLM's
    :class:`ChatResponse` when successful; ``error_reason`` is non-None
    for soft failures (``"timeout"`` / ``"provider_error"``). Retry/backoff
    is intentionally bypassed — cmd_k is fail-fast (no retries) so the W14
    scheduler is consulted only for RPM tracking via ``acquire``. Same
    posture as W34's autocomplete.
    """

    async def _do_call():
        async with scheduler.acquire(provider.name, surface=SURFACE):
            return await provider.chat(
                messages=messages,
                tools=tool_specs,
                max_tokens=max_tokens,
            )

    try:
        response = await asyncio.wait_for(_do_call(), timeout=timeout)
    except asyncio.TimeoutError:
        return None, "timeout"
    except Exception as exc:  # noqa: BLE001 — collapse to a single reason
        logger.warning("command_palette provider call failed: %s", exc, exc_info=False)
        return None, "provider_error"
    return response, None


# --------------------------------------------------------------------------- #
# Tool-call dispatch + diff assembly                                           #
# --------------------------------------------------------------------------- #


_GRAPH_PREFIX: Final[str] = "flowfile.graph."
_ADD_PREFIX: Final[str] = "flowfile.graph.add_"
_CONNECT_NAME: Final[str] = "flowfile.graph.connect"
_DELETE_NODE_NAME: Final[str] = "flowfile.graph.delete_node"
_DELETE_CONNECTION_NAME: Final[str] = "flowfile.graph.delete_connection"


def _is_graph_mutation(tool_name: str) -> bool:
    """Filter the LLM's tool calls down to staged-diff candidates.

    Read-only tools in the surface (e.g. ``flowfile.schema.read_node_schema``)
    are silently dropped — schemas are already in the W22 user message,
    so the LLM rarely needs the call, and the executor's read path doesn't
    feed a diff. Any future graph-op (``update_node_settings`` /
    ``run_node`` / ``propose_subgraph``) would need to be added here when
    W31's executor stops refusing them.
    """
    if tool_name.startswith(_ADD_PREFIX):
        return True
    return tool_name in {_CONNECT_NAME, _DELETE_NODE_NAME, _DELETE_CONNECTION_NAME}


def _next_free_node_id(graph: FlowGraph) -> int:
    """Smallest integer id strictly greater than every existing node id.

    Used as the base for sequential id assignment across an LLM response
    that emits multiple ``add_*`` tool calls. Non-integer node ids
    (string-keyed user-defined nodes are rare but legal) are skipped so
    they don't pollute the ``max(...)`` arithmetic.
    """
    existing = [int(n.node_id) for n in graph.nodes if isinstance(n.node_id, int)]
    return (max(existing) + 1) if existing else 1


def _inject_id_overrides(
    *,
    tool_name: str,
    tool_args: dict[str, Any],
    flow_id: int,
    fresh_node_id: int,
) -> dict[str, Any]:
    """Stamp server-controlled ids onto an ``add_*`` tool call's settings dict.

    The system prompt tells the LLM to use placeholders for ``flow_id`` /
    ``node_id``; the server is the source of truth for both. For
    non-``add_*`` graph ops (``connect`` / ``delete_node`` /
    ``delete_connection``) we leave the LLM's ids untouched — those
    operations reference *existing* node ids from the W22 prompt context,
    not synthetic ones the server is about to create.
    """
    if not tool_name.startswith(_ADD_PREFIX):
        return dict(tool_args)
    out = dict(tool_args)
    out["flow_id"] = flow_id
    out["node_id"] = fresh_node_id
    return out


def _bin_tool_results_to_diff(
    *,
    session_id: str,
    flow_id: int,
    rationale: str | None,
    results: list[ToolExecutionResult],
) -> GraphDiff:
    """Compose a :class:`GraphDiff` from W31 staged tool results.

    Mirrors :func:`flowfile_core.ai.diff_routes._bin_staged_results` —
    same shape so a follow-up cleanup PR could lift the helper to a
    shared module without touching call sites. Replicating inline keeps
    W33 self-contained and avoids importing a private helper from the
    diff_routes module (which is route-layer code).
    """
    additions: list[StagedAddition] = []
    connections_added: list[StagedConnection] = []
    deletions: list[StagedDeletion] = []
    connections_removed: list[StagedConnection] = []

    for result in results:
        if result.staged_node_payload is None:
            continue
        payload = result.staged_node_payload
        tool_name = result.tool_name

        if tool_name.startswith(_ADD_PREFIX):
            node_type = tool_name[len(_ADD_PREFIX) :]
            additions.append(
                StagedAddition(
                    node_type=payload.get("node_type", node_type),
                    settings=payload.get("settings", {}),
                    insertion_context=StagedInsertionContext(**payload.get("insertion_context", {})),
                    predicted_output_schema=payload.get("predicted_output_schema"),
                    audit_id=result.audit_id,
                )
            )
        elif tool_name == _CONNECT_NAME:
            connections_added.append(
                StagedConnection(
                    connection=payload.get("connection", {}),
                    audit_id=result.audit_id,
                )
            )
        elif tool_name == _DELETE_NODE_NAME:
            node_id = payload.get("delete_node_id")
            if isinstance(node_id, int):
                deletions.append(
                    StagedDeletion(
                        delete_node_id=node_id,
                        audit_id=result.audit_id,
                    )
                )
        elif tool_name == _DELETE_CONNECTION_NAME:
            connections_removed.append(
                StagedConnection(
                    connection=payload.get("delete_connection", {}),
                    audit_id=result.audit_id,
                )
            )

    return GraphDiff(
        session_id=session_id,
        flow_id=flow_id,
        rationale=rationale,
        additions=additions,
        connections_added=connections_added,
        deletions=deletions,
        connections_removed=connections_removed,
    )


# --------------------------------------------------------------------------- #
# Public entry point                                                           #
# --------------------------------------------------------------------------- #


async def run_command_palette(
    graph: FlowGraph,
    *,
    prompt: str,
    provider: Provider,
    session_id: str,
    user_id: int,
    selected_node_ids: list[int] | None = None,
    insertion_context: CommandPaletteInsertionContext | None = None,
    max_tokens: int | None = None,
    scheduler: RateLimitScheduler | None = None,
    timeout: float | None = None,
) -> CommandPaletteResponse:
    """Run one Cmd+K command-palette request end to end.

    Pipeline:

    1. W22 :func:`render_prompt_context(surface="cmd_k", samples_mode="off")`
       for the layered system prompt + flow-context user block.
    2. W30 :func:`build_tool_catalog(surface="cmd_k")` for the per-surface
       tool preset (filter / select / sort / unique / record_id +
       ``read_node_schema``).
    3. W11 :meth:`Provider.chat(messages, tools=..., max_tokens=...)` under
       W14's :meth:`RateLimitScheduler.acquire(surface="cmd_k")` and a
       hard :func:`asyncio.wait_for` (D010 fail-fast).
    4. W31 :func:`execute_tool_call(mode="stage", ...)` per LLM tool call —
       the executor handles refusal pipeline, schema prediction, and
       audit emission. We don't double-audit here.
    5. W41 :func:`register_diff(GraphDiff(...))` — return the ``diff_id``.

    Soft failures (timeout, no tool calls, provider error, all-refused)
    become a :class:`CommandPaletteResponse` with ``degraded=True`` and a
    stable ``reason`` string. The function never raises — even a
    pathologically misbehaving provider can't take the route down.
    """
    started = time.monotonic()
    sched = scheduler or default_scheduler()
    effective_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT_SECONDS
    effective_max_tokens = max_tokens if max_tokens is not None else DEFAULT_MAX_TOKENS
    sid = session_id or "cmdk-anonymous"
    flow_id = int(graph.flow_id)

    # W30 preset
    tool_specs = list(build_tool_catalog(surface=SURFACE))
    if not tool_specs:
        # Defence-in-depth: ``SURFACE_PRESETS["cmd_k"]`` is non-empty, but a
        # registry-coverage drift would land here rather than 5xx the user.
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=int((time.monotonic() - started) * 1000),
            suggestion_count=0,
            degraded_reason="empty_catalog",
        )
        return CommandPaletteResponse(degraded=True, reason="empty_catalog")

    # W22 context + user instruction
    messages = _build_messages_for_palette(
        graph=graph,
        prompt=prompt,
        selected_node_ids=selected_node_ids,
        insertion_context=insertion_context,
    )

    # W11 provider call (W14 scheduler acquired inside)
    response, error = await _call_provider_for_tools(
        provider=provider,
        messages=messages,
        tool_specs=tool_specs,
        max_tokens=effective_max_tokens,
        timeout=effective_timeout,
        scheduler=sched,
    )
    if error is not None:
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=int((time.monotonic() - started) * 1000),
            suggestion_count=0,
            degraded_reason=error,
        )
        return CommandPaletteResponse(
            degraded=True,
            reason=error,
        )

    rationale = (response.content or "").strip() or None
    tool_calls: list[ToolCall] = list(response.tool_calls or [])
    mutation_calls = [tc for tc in tool_calls if _is_graph_mutation(tc.name)]
    mutation_calls = mutation_calls[:MAX_OPS_PER_REQUEST]

    if not mutation_calls:
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=int((time.monotonic() - started) * 1000),
            suggestion_count=0,
            degraded_reason="no_tool_calls",
        )
        return CommandPaletteResponse(
            degraded=True,
            reason="no_tool_calls",
            rationale=rationale,
        )

    # Per-call execution. Each ``add_*`` gets a fresh sequential node_id so
    # the LLM's placeholder ids in different tool calls don't collide.
    base_fresh_id = _next_free_node_id(graph)
    fresh_id_offset = 0
    base_ic_obj = InsertionContext(**(insertion_context.model_dump() if insertion_context is not None else {}))

    staged_results: list[ToolExecutionResult] = []
    refused: list[RefusedToolCall] = []

    for call in mutation_calls:
        fresh_id = base_fresh_id + fresh_id_offset
        adjusted_args = _inject_id_overrides(
            tool_name=call.name,
            tool_args=dict(call.arguments or {}),
            flow_id=flow_id,
            fresh_node_id=fresh_id,
        )
        try:
            result = execute_tool_call(
                flow_id=flow_id,
                tool_name=call.name,
                tool_args=adjusted_args,
                insertion_context=base_ic_obj,
                session_id=sid,
                user_id=user_id,
                mode="stage",
                flow=graph,
            )
        except Exception as exc:  # noqa: BLE001 — defensive; executor catches its own raises but we never want a route-level 500.
            logger.warning(
                "command_palette executor raised on %s: %s",
                call.name,
                exc,
                exc_info=False,
            )
            refused.append(
                RefusedToolCall(
                    tool_name=call.name,
                    refusal_reason=None,
                    refusal_detail=str(exc),
                )
            )
            continue

        if result.status == "rejected":
            refused.append(
                RefusedToolCall(
                    tool_name=call.name,
                    refusal_reason=str(result.refusal_reason) if result.refusal_reason else None,
                    refusal_detail=result.refusal_detail,
                    warnings=list(result.warnings),
                )
            )
            continue

        staged_results.append(result)
        if call.name.startswith(_ADD_PREFIX):
            fresh_id_offset += 1

    if not staged_results:
        record_autocomplete_call(
            surface=SURFACE,
            provider=provider.name,
            latency_ms=int((time.monotonic() - started) * 1000),
            suggestion_count=0,
            degraded_reason="all_refused",
        )
        return CommandPaletteResponse(
            degraded=True,
            reason="all_refused",
            rationale=rationale,
            refused=refused,
        )

    graph_diff = _bin_tool_results_to_diff(
        session_id=sid,
        flow_id=flow_id,
        rationale=rationale,
        results=staged_results,
    )
    diff_id = register_diff(graph_diff)
    op_count = (
        len(graph_diff.additions)
        + len(graph_diff.connections_added)
        + len(graph_diff.deletions)
        + len(graph_diff.connections_removed)
    )

    record_autocomplete_call(
        surface=SURFACE,
        provider=provider.name,
        latency_ms=int((time.monotonic() - started) * 1000),
        suggestion_count=op_count,
        degraded_reason=None,
    )

    return CommandPaletteResponse(
        diff_id=diff_id,
        op_count=op_count,
        rationale=rationale,
        degraded=False,
        reason=None,
        diff=graph_diff,
        refused=refused,
    )


__all__ = [
    "DEFAULT_MAX_TOKENS",
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_OPS_PER_REQUEST",
    "SURFACE",
    "CommandPaletteInsertionContext",
    "CommandPaletteRequest",
    "CommandPaletteResponse",
    "DegradedReason",
    "RefusedToolCall",
    "run_command_palette",
]
