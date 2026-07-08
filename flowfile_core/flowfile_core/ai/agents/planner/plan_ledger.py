"""Host-tracked plan ledger for the staged / live planner.

The staged planner plans a multi-node pipeline via ``emit_plan`` but then
relies 100% on the model to *voluntarily* re-classify the next step after
each staged op. Small models emit a prose wrap-up instead of the next
``classify_intent`` and the run terminates after one node.

This module parses the ``emit_plan`` markdown into a per-session checklist
(prevention: the model sees a fresh ``[done]``/``[pending]`` block every
classify round) and lets the loop nudge instead of terminating when the
model stalls mid-plan (correction). It is **pure scaffolding + a nudge
heuristic** — it never gates what tool the model may call.

Import contract: stdlib + ``flowfile_core.ai.sessions`` types only. No
litellm, no other planner modules — keep it importable on the cheap paths.
Question-only plans parse to ``[]`` and every gate below is then inert, so
behavior is identical to before the ledger existed.
"""

from __future__ import annotations

import re

from flowfile_core.ai.sessions import AgentSession, CompletedOpEntry

# Local copies of the two tool-name shapes we categorize. Duplicated (not
# imported from ``._internal``) to honor the stdlib-plus-sessions-only import
# contract — these strings are stable graph-tool names.
_ADD_PREFIX = "flowfile.graph.add_"
_REWIRE_TOOLS = frozenset({"flowfile.graph.connect", "flowfile.graph.delete_connection"})
_GRAPH_PREFIX = "flowfile.graph."

# Bounds so a runaway/verbose plan can't bloat the per-round prompt.
_MAX_STEPS = 24
_MAX_STEP_CHARS = 240

# A numbered list item: ``1. <text>`` (optionally indented).
_NUMBERED_LINE = re.compile(r"^\s*\d+\.\s+(.*\S)\s*$")
# ``## Proposed changes`` header (case-insensitive), and the next h2 header.
_PROPOSED_HEADER = re.compile(r"(?im)^\s*##\s+proposed\s+changes\s*$")
_NEXT_H2 = re.compile(r"(?m)^\s*##\s+")
# A ``(re-wire)`` / ``(rewire)`` leading token.
_REWIRE_STEP = re.compile(r"^\(?\s*re-?wire", re.IGNORECASE)
# Leading node-type token before an em-dash / hyphen separator.
_ADD_STEP = re.compile(r"^([a-zA-Z_][a-zA-Z0-9_]*)\s*[—\-]")


def parse_plan_steps(plan_markdown: str | None) -> list[str]:
    """Parse the numbered ``## Proposed changes`` list into step strings.

    Takes the first (numbered) physical line of each step verbatim, minus the
    ``N.`` marker; indented ``*Why*`` / wrapped continuation lines are
    dropped. Prefers the ``## Proposed changes`` section (sliced to the next
    ``## `` header). The whole-text fallback applies ONLY to fully headerless
    plans (no ``## `` lines at all — the condensed shape); a plan that has
    headers but no ``## Proposed changes`` returns ``[]`` rather than parsing
    numbered narration (e.g. a ``## Current state`` list) as steps.
    Question-only plans (no numbered list) return ``[]`` — every downstream
    gate is then inert.
    """
    if not plan_markdown or not plan_markdown.strip():
        return []

    header = _PROPOSED_HEADER.search(plan_markdown)
    if header is not None:
        section_start = header.end()
        nxt = _NEXT_H2.search(plan_markdown, section_start)
        section = plan_markdown[section_start : nxt.start() if nxt else len(plan_markdown)]
    elif _NEXT_H2.search(plan_markdown) is not None:
        # Headers exist but none is ``## Proposed changes`` — numbered lines
        # elsewhere are narration, not steps.
        return []
    else:
        # Fully headerless / condensed plan — scan the whole text.
        section = plan_markdown

    steps: list[str] = []
    for line in section.splitlines():
        m = _NUMBERED_LINE.match(line)
        if m is None:
            continue
        text = m.group(1).strip()
        if not text:
            continue
        if len(text) > _MAX_STEP_CHARS:
            text = text[:_MAX_STEP_CHARS].rstrip()
        steps.append(text)
        if len(steps) >= _MAX_STEPS:
            break
    return steps


def step_kind(step: str) -> tuple[str, str | None]:
    """Classify one plan step for the greedy annotate heuristic.

    Returns ``("rewire", None)`` for a ``(re-wire)`` step, ``("add",
    node_type)`` when the leading token is a plausible node-type name before
    an em-dash/hyphen, else ``("unknown", None)``.
    """
    stripped = step.strip()
    if _REWIRE_STEP.match(stripped):
        return "rewire", None
    m = _ADD_STEP.match(stripped)
    if m is not None:
        return "add", m.group(1).lower()
    return "unknown", None


def _categorize_op(op: CompletedOpEntry) -> str:
    if op.tool_name.startswith(_ADD_PREFIX):
        return "add"
    if op.tool_name == "flowfile.graph.connect":
        return "connect"
    if op.tool_name == "flowfile.graph.delete_connection":
        return "delete_connection"
    return "other"


def annotate_done(plan_steps: list[str], completed_ops: list[CompletedOpEntry]) -> list[bool]:
    """Greedy multiset match of completed ops onto plan steps.

    An ``add`` step consumes one ``add_*`` op. A ``rewire`` step consumes a
    PAIR — up to one ``connect`` AND up to one ``delete_connection`` — because
    a real re-wire executes as two recorded ops; letting each op satisfy a
    separate rewire step would over-count and silently end the run early. A
    rewire is done ONLY once its ``connect`` half landed; a delete-only match
    (disconnect done, reconnect still pending — the exact mid-plan stall this
    ledger catches) consumes the delete from the pool but leaves the step
    ``[pending]``, consistent with the under-count bias below. An ``unknown``
    step consumes any leftover op. The heuristic deliberately biases toward under-counting
    (``[pending]``) over over-counting (``[done]``): a wrongly-pending step
    costs one nudge round; a wrongly-done step ends the run one op short.
    Never raises, never gates.
    """
    done = [False] * len(plan_steps)
    if not plan_steps or not completed_ops:
        return done

    pools = {"add": 0, "connect": 0, "delete_connection": 0, "other": 0}
    for op in completed_ops:
        pools[_categorize_op(op)] += 1

    # Pass 1: typed steps consume their own category, in order. Rewire
    # steps take one connect AND one delete_connection when available.
    for i, step in enumerate(plan_steps):
        kind, _ = step_kind(step)
        if kind == "add" and pools["add"] > 0:
            pools["add"] -= 1
            done[i] = True
        elif kind == "rewire":
            consumed_connect = False
            if pools["connect"] > 0:
                pools["connect"] -= 1
                consumed_connect = True
            if pools["delete_connection"] > 0:
                pools["delete_connection"] -= 1
            done[i] = consumed_connect

    # Pass 2: unknown steps consume any leftover op, in order.
    for i, step in enumerate(plan_steps):
        if done[i]:
            continue
        kind, _ = step_kind(step)
        if kind != "unknown":
            continue
        for pool in ("other", "add", "connect", "delete_connection"):
            if pools[pool] > 0:
                pools[pool] -= 1
                done[i] = True
                break
    return done


def record_completed_op(
    session: AgentSession,
    *,
    tool_name: str,
    tool_args: dict | None,
    node_id: int | None,
) -> None:
    """Append a :class:`CompletedOpEntry` with a terse summary.

    Called by the loop after each staged / applied single op. The summary is
    render-only ("add_filter — staged as node 5", "connect 5→2").
    """
    op_short = tool_name.removeprefix(_GRAPH_PREFIX)
    args = tool_args or {}
    if tool_name.startswith(_ADD_PREFIX) and node_id is not None:
        summary = f"{op_short} — staged as node {node_id}"
    elif tool_name in _REWIRE_TOOLS:
        frm = args.get("from_node_id") or args.get("upstream_node_id")
        to = args.get("to_node_id") or args.get("downstream_node_id")
        if isinstance(frm, int) and isinstance(to, int):
            summary = f"{op_short} {frm}→{to}"
        else:
            summary = op_short
    elif node_id is not None:
        summary = f"{op_short} on node {node_id}"
    else:
        summary = op_short
    session.plan_completed_ops.append(CompletedOpEntry(tool_name=tool_name, node_id=node_id, summary=summary))


def has_pending_steps(session: AgentSession) -> bool:
    """True when the plan parsed at least one step and not all are matched."""
    if not session.plan_steps:
        return False
    done = annotate_done(session.plan_steps, session.plan_completed_ops)
    return not all(done)


def build_plan_progress_block(session: AgentSession) -> str | None:
    """The ``## Plan progress (host-tracked)`` checklist, or ``None``.

    Rebuilt fresh each classify round: numbered steps annotated
    ``[done]``/``[pending]``, the completed-ops list, and an imperative
    footer. ``None`` when no plan parsed (question-only runs)."""
    if not session.plan_steps:
        return None
    done = annotate_done(session.plan_steps, session.plan_completed_ops)

    lines: list[str] = ["## Plan progress (host-tracked)", ""]
    for i, (step, is_done) in enumerate(zip(session.plan_steps, done, strict=False), start=1):
        mark = "[done]" if is_done else "[pending]"
        lines.append(f"{i}. {mark} {step}")
    lines.append("")

    if session.plan_completed_ops:
        lines.append("Completed ops:")
        for op in session.plan_completed_ops:
            lines.append(f"- {op.summary}")
    else:
        lines.append("Completed ops: (none yet)")
    lines.append("")

    lines.append(
        "If any step is [pending], call `classify_intent` NOW with the "
        'op_kind for the NEXT pending step. Only pick op_kind="other" when '
        "every step is [done] or no longer applicable — say why in the "
        "rationale."
    )
    return "\n".join(lines) + "\n"


def build_plan_nudge(session: AgentSession) -> str:
    """Terse host note (modeled on ``_build_join_pivot_steer``).

    Injected as a ``role="user"`` message when the model stalls mid-plan.
    Kept short — it is re-injected on stalls, so verbose prose would eat a
    small model's context budget.
    """
    done = annotate_done(session.plan_steps, session.plan_completed_ops)
    total = len(session.plan_steps)
    done_count = sum(done)
    first_pending = next(
        (step for step, is_done in zip(session.plan_steps, done, strict=False) if not is_done),
        "",
    )
    return (
        f"NOTE FROM HOST: your plan is NOT finished — {done_count}/{total} "
        f"steps have matching completed ops. Pending: {first_pending} Do not "
        f"stop or summarize. Call `classify_intent` now with the op_kind for "
        f'that step. Pick op_kind="other" ONLY if the pending steps are '
        f"already covered or impossible — say why in your rationale."
    )


def reset_plan_ledger(session: AgentSession) -> None:
    """Clear the three ledger fields (new goal → fresh plan)."""
    session.plan_markdown = None
    session.plan_steps = []
    session.plan_completed_ops = []
