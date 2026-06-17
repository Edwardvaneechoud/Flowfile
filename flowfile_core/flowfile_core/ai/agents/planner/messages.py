"""Per-stage user / system message assembly for the agent_staged surface.

The initial system + user pair is built in :func:`_build_initial_messages`.
On stage transitions, :func:`_refresh_system_prompt_for_stage` swaps
``messages[0]`` and may slim ``messages[1]`` to keep the per-stage
prompt focused (full subgraph noise hurts smaller models at stage 3).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flowfile_core.ai import sessions
from flowfile_core.ai.context.builder import render_prompt_context
from flowfile_core.ai.providers.base import Message

from ._internal import _ADD_PREFIX, _STAGED_STATE_MACHINE_SURFACES

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph


def _build_initial_messages(flow: FlowGraph, session: sessions.AgentSession) -> list[Message]:
    """Build ``[system, user]`` from the prompt context + the user's goal.

    The system block comes from ``assemble_system_prompt(surface)`` (via
    ``render_prompt_context``) — ``base.md`` + ``planner.md``. The user
    block is the deterministic subgraph snapshot followed by a ``## Goal``
    block.

    Pass ``mentions="@flow"`` so the resolver expands to all current
    nodes — without that the user block would render as
    ``## Subgraph (empty)`` regardless of canvas state and the agent
    would refuse every cold-flow request even when nodes existed.
    """
    ctx = render_prompt_context(
        flow,
        [],
        surface=session.surface,
        samples_mode=session.samples_mode,
        mentions="@flow",
        stage=session.stage if session.surface in _STAGED_STATE_MACHINE_SURFACES else None,
        picked_node_type=session.picked_node_type if session.surface in _STAGED_STATE_MACHINE_SURFACES else None,
    )
    user_text = f"{ctx.user}\n\n## Goal\n\n{session.user_prompt}".strip()
    return [
        Message(role="system", content=ctx.system),
        Message(role="user", content=user_text),
    ]


def _build_fill_settings_user_message(
    session: sessions.AgentSession, flow: FlowGraph
) -> str | None:
    """Focused user message for the ``fill_settings`` stage.

    By the time we reach stage 3, the only context the LLM needs is:

    1. **The user's actual ask** (their original prompt; for auto-promote
       sessions this includes the chat transcript that produced the
       intent).
    2. **The picked upstream's column schema** so the LLM can reference
       valid column names in the new node's settings. Reading the rest
       of the subgraph at this point is a distraction.

    Without this slim, ~4.5k chars of irrelevant subgraph + every other
    node's settings dict ride into stage 3 and small models like
    llama-3.3-70b end up writing a rationale instead of calling the
    only-tool-in-its-array.

    Returns ``None`` when there's no picked upstream (i.e. the helper
    was called outside fill_settings or stage 2 didn't produce one).
    """
    from flowfile_core.ai.context.builder import _safe_get_predicted_schema

    upstream_ids = list(session.picked_upstream_ids or [])
    if session.picked_right_input_id is not None:
        upstream_ids.append(session.picked_right_input_id)
    if not upstream_ids:
        return None

    lines: list[str] = ["## Your task", "", session.user_prompt.strip(), ""]

    # Helper to render one upstream's column block from staged_results
    # when the node isn't in flow.nodes yet (chained add within one
    # user turn — the prior add hasn't been applied because this
    # session's diff is still being assembled).
    def _staged_schema_for(node_id: int) -> list[dict[str, Any]] | None:
        for entry in session.staged_results:
            payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else None
            if payload is None:
                continue
            settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
            staged_id = settings.get("node_id") if isinstance(settings, dict) else None
            if staged_id != node_id:
                continue
            preds = payload.get("predicted_output_schema")
            if isinstance(preds, list):
                return preds
            return None
        return None

    for uid in upstream_ids:
        node = flow.get_node(uid)
        cols_text: list[str] = []
        if node is not None:
            predicted = _safe_get_predicted_schema(node)
            if predicted:
                for col in predicted:
                    name = getattr(col, "column_name", "?")
                    dtype = getattr(col, "data_type", "Unknown")
                    cols_text.append(f"- {name}: {dtype}")
        else:
            staged_preds = _staged_schema_for(uid)
            if staged_preds is not None:
                for col in staged_preds:
                    if isinstance(col, dict):
                        cols_text.append(
                            f"- {col.get('name', '?')}: {col.get('data_type', 'Unknown')}"
                        )

        label_kind = (
            "Right input"
            if session.picked_right_input_id is not None and uid == session.picked_right_input_id
            else "Upstream"
        )
        if cols_text:
            lines.append(f"## {label_kind} node {uid} columns")
            lines.append("")
            lines.extend(cols_text)
            lines.append("")
        else:
            # Schema-unknown path. Emit the marker so the LLM knows to
            # refuse rather than hallucinate column names. Mirrors the
            # ``schema: unknown`` posture of ``render_user_message``.
            lines.append(f"## {label_kind} node {uid} columns")
            lines.append("")
            lines.append("schema: unknown — upstream has no predicted schema")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _build_pick_upstream_staged_addendum(
    session: sessions.AgentSession,
) -> str | None:
    """Render a *"## Staged this session"* block listing each prior
    in-batch staged ``add_*`` with its node id, type, and predicted
    output schema.

    The pick_upstream user message at session start carries the live
    subgraph but says NOTHING about nodes the agent has staged earlier
    in the same session. The staged ids ARE in the
    ``pick_upstream_node_ids`` enum (``meta_ops.build_pick_upstream_spec``
    walks ``session.staged_node_ids``), so the LLM can pick them — but
    blind, without column context. On long chains that hurts accuracy.

    Returns ``None`` when there are no staged-this-session add_* entries
    so the caller can leave ``messages[1]`` untouched.
    """
    blocks: list[str] = []
    for entry in session.staged_results:
        if not entry.tool_name.startswith(_ADD_PREFIX):
            continue
        node_type = entry.tool_name.removeprefix(_ADD_PREFIX)
        payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else None
        if payload is None:
            continue
        settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
        nid = settings.get("node_id") if isinstance(settings, dict) else None
        if not isinstance(nid, int):
            continue
        preds = payload.get("predicted_output_schema")
        cols_text: list[str] = []
        if isinstance(preds, list):
            for col in preds:
                if isinstance(col, dict):
                    cols_text.append(
                        f"  - {col.get('name', '?')}: {col.get('data_type', 'Unknown')}"
                    )
        header = f"- node {nid} ({node_type})"
        if cols_text:
            blocks.append(header + "\n" + "\n".join(cols_text))
        else:
            blocks.append(header + "\n  - schema: unknown")

    # Cap to the 10 most-recent staged entries to bound the prompt on
    # very long sessions. Oldest entries fall out — by then they're
    # already accepted or rejected and downstream context is what
    # matters for the next pick.
    if not blocks:
        return None
    blocks = blocks[-10:]
    return "## Staged this session\n\n" + "\n".join(blocks) + "\n"


def _refresh_system_prompt_for_stage(
    session: sessions.AgentSession, flow: FlowGraph | None = None
) -> None:
    """Replace ``session.messages[0]`` with a freshly-rendered system
    prompt for the current stage.

    No-op when the surface is not ``agent_staged`` or when the session
    doesn't have a system message yet (the initial system prompt is built
    by :func:`_build_initial_messages` on first entry to the loop).

    Stages have different suffix files and different catalog scopes, so
    the system prompt must be re-assembled when the stage changes.
    Updating ``messages[0]`` in place keeps the rest of the conversation
    history intact (assistant turns, tool replies). The prompt cache is
    invalidated by the change but per-stage prompts are smaller, so the
    re-keyed cache is cheap to fill.

    When ``flow`` is provided AND the stage is ``fill_settings``, also
    rewrite ``session.messages[1]`` (the user message) with a focused
    mini-prompt that contains only the user's goal + the picked
    upstream's column schema. The full subgraph embedded by
    :func:`_build_initial_messages` is irrelevant once the upstream is
    locked in; keeping it bloats the prompt and confuses smaller models
    like llama-3.3-70b.
    """
    if session.surface not in _STAGED_STATE_MACHINE_SURFACES:
        return
    if not session.messages or session.messages[0].role != "system":
        return
    from flowfile_core.ai.context.builder import assemble_system_prompt

    new_system = assemble_system_prompt(
        session.surface,
        stage=session.stage,
        picked_node_type=session.picked_node_type,
    )
    session.messages[0] = Message(role="system", content=new_system)

    if (
        session.stage == "fill_settings"
        and flow is not None
        and len(session.messages) >= 2
        and session.messages[1].role == "user"
    ):
        slim_user = _build_fill_settings_user_message(session, flow)
        if slim_user is not None:
            session.messages[1] = Message(role="user", content=slim_user)
        return

    # At the pick_upstream stage, append a *"## Staged this session"*
    # block to the user message so the LLM can see the predicted columns
    # of nodes it staged earlier in the same session (those ids appear
    # in the upstream-id enum but otherwise have no schema context).
    # Rebuild from the original subgraph + goal each time so re-entries
    # to pick_upstream within one session don't accumulate stale staged
    # blocks.
    if (
        session.stage == "pick_upstream"
        and flow is not None
        and len(session.messages) >= 2
        and session.messages[1].role == "user"
    ):
        addendum = _build_pick_upstream_staged_addendum(session)
        if addendum is None:
            return
        # Re-render the original user message (live subgraph + goal)
        # to drop any stale addendum from a prior pick_upstream
        # iteration in the same session.
        rebuilt = _build_initial_messages(flow, session)
        base_user = rebuilt[1].content if len(rebuilt) >= 2 else session.messages[1].content
        session.messages[1] = Message(
            role="user",
            content=base_user.rstrip() + "\n\n" + addendum,
        )
