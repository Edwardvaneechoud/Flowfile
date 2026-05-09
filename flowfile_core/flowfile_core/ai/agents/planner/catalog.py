"""Per-stage tool catalog selection + stage-transition logging.

The agent_staged surface narrows the LLM's tool catalog to one tool per
stage so smaller models comply with the function-calling API rather
than emitting text-JSON. ``pick_upstream`` and ``fill_settings`` need
per-turn dynamic specs because their schemas depend on session state
(upstream id enum, picked node type).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from flowfile_core.ai import sessions
from flowfile_core.ai.tools.meta_ops import build_pick_upstream_spec
from flowfile_core.ai.tools.registry import build_staged_fill_tool_spec

from ._internal import (
    _AGENT_STAGED_OP_TO_SURFACE,
    _STAGED_STATE_MACHINE_SURFACES,
    _collect_live_node_ids,
    logger,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph


def _resolve_current_surface(session: sessions.AgentSession) -> str:
    """Map the session's surface + state to a tool-catalog surface key.

    * ``surface="agent_complex"`` — one-shot full catalog (single round
      with every tool exposed). Big-model power-user path.
    * ``surface="agent_staged"`` — multi-stage state machine. The tool
      catalog is per-stage:
      - ``classify`` → ``"staged_classify"``.
      - ``pick_type`` → ``"staged_pick_type"``.
      - ``pick_upstream`` → ``"staged_pick_upstream"`` (planner overrides
        with a per-turn dynamic spec via
        ``build_pick_upstream_spec``).
      - ``fill_settings`` → telemetry-only key (planner overrides with
        ``build_staged_fill_tool_spec(picked_node_type)``).
      - ``single_stage_op`` → ``"staged_modify"`` / ``"staged_delete"``
        / ``"staged_connect"`` / ``"staged_disconnect"`` per
        ``picked_op_kind``.
    """
    if session.surface == "agent_complex":
        return "agent_complex"
    # session.surface in _STAGED_STATE_MACHINE_SURFACES — only remaining
    # alternative. Plan stage runs once at session start.
    if session.stage == "plan":
        return "staged_plan"
    if session.stage == "single_stage_op" and session.picked_op_kind is not None:
        mapped = _AGENT_STAGED_OP_TO_SURFACE.get(session.picked_op_kind)
        if mapped is not None:
            return mapped
        # Defensive — picked_op_kind in {add, other} never reaches this
        # branch (add advances through pick_type stages; other terminates
        # the loop). Fall through to classify so the loop self-heals.
        return "staged_classify"
    if session.stage == "pick_type":
        return "staged_pick_type"
    if session.stage == "pick_upstream":
        return "staged_pick_upstream"
    if session.stage == "fill_settings":
        # Telemetry-only: the planner overrides the catalog with a
        # per-turn ``build_staged_fill_tool_spec`` call before dispatch.
        return "staged_pick_upstream"
    # Opt-in verify-completion gate. Reached only when classify picked
    # op_kind="other" AND session.verify_plan_completion AND not already
    # consumed this loop.
    if session.stage == "verify_completion":
        return "staged_verify_completion"
    return "staged_classify"


def _build_staged_tool_catalog(
    session: sessions.AgentSession,
    flow: FlowGraph,
) -> tuple[list, str | None]:
    """Build the per-turn tool catalog for ``agent_staged``.

    Returns ``(tools, error_detail)``. Most stages dispatch through the
    static ``SURFACE_PRESETS`` lookup; two stages need per-turn dynamic
    specs because their schemas depend on session state:

    * ``pick_upstream`` — the upstream-id enum is the union of currently-
      live node ids and ids the agent has staged this session. Building
      per-turn means a multi-node turn (filter → sort) sees the prior
      add's id in the picker enum without history pruning.
    * ``fill_settings`` — the single tool exposed is the
      ``flowfile.graph.add_<picked_type>`` Pydantic schema with planner-
      injected fields stripped. ``picked_node_type`` is set by the prior
      stage; defensive ``None`` fallback returns an error string instead
      of an empty catalog so the loop refuses cleanly.
    """
    if session.stage == "fill_settings":
        if not session.picked_node_type:
            return [], "agent_staged: stage=fill_settings without picked_node_type"
        spec = build_staged_fill_tool_spec(session.picked_node_type)
        if spec is None:
            return [], (
                f"agent_staged: picked_node_type {session.picked_node_type!r} "
                "has no registered settings class"
            )
        return [spec], None

    if session.stage == "pick_upstream":
        live_ids = _collect_live_node_ids(flow)
        # Pass session.picked_node_type so the spec makes
        # ``right_input_node_id`` REQUIRED for join-shaped node types
        # (join / cross_join / fuzzy_match). Without this, cross_join
        # could stage with only one upstream wire and the second input
        # would dangle.
        spec = build_pick_upstream_spec(
            live_ids,
            list(session.staged_node_ids),
            picked_node_type=session.picked_node_type,
        )
        return [spec], None

    # All other stages map to a static surface preset via _resolve_current_surface.
    return [], None


def _log_stage_transition(
    session: sessions.AgentSession,
    *,
    from_stage: str,
    to_stage: str,
    tool_name: str,
    op_kind: str | None = None,
    node_type: str | None = None,
    upstream_node_ids: list[int] | None = None,
    completed_op: str | None = None,
) -> None:
    """Emit a structured INFO line per stage transition.

    Pairs with the prompt log (``FLOWFILE_AI_LOG_PROMPTS=true``) to give
    a complete debugging picture without enabling DEBUG: the prompt log
    captures the raw LLM input/output per stage; this line captures the
    state-machine transition itself, with the session id so multiple
    concurrent sessions stay disambiguated.

    Format: ``planner.staged session=<sid> from=<from> to=<to>
    tool=<name> op_kind=<ok> node_type=<nt> upstream=<ids>
    completed_op=<co>``. Tail with::

        tail -F ~/.flowfile/logs/flowfile_core.log | grep planner.staged
    """
    parts: list[str] = [
        f"session={session.session_id[:8]}",
        f"from={from_stage}",
        f"to={to_stage}",
        f"tool={tool_name}",
    ]
    if op_kind:
        parts.append(f"op_kind={op_kind}")
    if node_type:
        parts.append(f"node_type={node_type}")
    if upstream_node_ids is not None:
        parts.append(f"upstream={upstream_node_ids}")
    if completed_op:
        parts.append(f"completed_op={completed_op}")
    logger.info("planner.staged %s", " ".join(parts))
