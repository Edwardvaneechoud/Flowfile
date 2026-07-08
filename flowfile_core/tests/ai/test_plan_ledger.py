"""Pure unit tests for the host-tracked plan ledger.

No I/O, no scripted provider — exercises ``plan_ledger``'s parse / classify
/ greedy-annotate / summary helpers directly. These are the zero-risk gates
that make every downstream loop nudge inert on question-only runs.
"""

from __future__ import annotations

from flowfile_core.ai import sessions
from flowfile_core.ai.agents.planner import plan_ledger


# The first worked example from ``prompts/stage_plan.md`` verbatim.
_WORKED_EXAMPLE = """\
## Current state

The flow reads customers, groups by city to count rows per city
(group_by#2), counts total rows (record_count#3), broadcasts the
total via cross_join#4, and formula#5 computes per-city
percentages.

## Proposed changes

1. unique — drop duplicate rows by email.
   *Why*: removes the source of overcounting; downstream
   aggregations now count each customer once.
2. (re-wire) — disconnect read#1 → group_by#2, reconnect new
   unique → group_by#2. *Why*: per-city counts now reflect
   unique customers.
3. (re-wire) — disconnect read#1 → record_count#3, reconnect new
   unique → record_count#3. *Why*: total count denominator now
   reflects unique customers, matching the numerators.

## Edge cases / caveats

- Assuming `email` is the dedup key.
- The re-wires are required.
"""

_CONDENSED_HEADERLESS = """\
1. sort — order rows by amount descending.
2. filter — keep only EU rows.
"""

_QUESTION_ONLY = "User asked a question; will respond without staging changes."


# parse_plan_steps


def test_parse_worked_example_three_steps_why_lines_dropped() -> None:
    steps = plan_ledger.parse_plan_steps(_WORKED_EXAMPLE)
    assert len(steps) == 3, steps
    assert steps[0] == "unique — drop duplicate rows by email."
    # Continuation + *Why* lines dropped; only the numbered physical line kept.
    assert steps[1].startswith("(re-wire) — disconnect read#1 → group_by#2")
    assert steps[2].startswith("(re-wire) — disconnect read#1 → record_count#3")
    assert all("*Why*" not in s for s in steps)
    # The Edge cases bullet list must not leak in as steps.
    assert not any("dedup key" in s for s in steps)


def test_parse_headerless_condensed_plan() -> None:
    steps = plan_ledger.parse_plan_steps(_CONDENSED_HEADERLESS)
    assert steps == [
        "sort — order rows by amount descending.",
        "filter — keep only EU rows.",
    ]


def test_parse_question_only_returns_empty() -> None:
    assert plan_ledger.parse_plan_steps(_QUESTION_ONLY) == []
    assert plan_ledger.parse_plan_steps("") == []
    assert plan_ledger.parse_plan_steps(None) == []


def test_parse_headers_without_proposed_changes_returns_empty() -> None:
    """The whole-text fallback applies only to fully headerless plans. A plan
    with other ``## `` headers but no ``## Proposed changes`` must NOT parse
    numbered narration (e.g. inside ``## Current state``) as steps."""
    body = (
        "## Current state\n"
        "\n"
        "The flow has three problems:\n"
        "1. duplicates in the read node.\n"
        "2. the group_by counts them twice.\n"
        "\n"
        "## Edge cases / caveats\n"
        "\n"
        "- No caveats.\n"
    )
    assert plan_ledger.parse_plan_steps(body) == []


def test_parse_step_cap() -> None:
    body = "## Proposed changes\n\n" + "\n".join(
        f"{i}. filter — step {i}" for i in range(1, 31)
    )
    steps = plan_ledger.parse_plan_steps(body)
    assert len(steps) == plan_ledger._MAX_STEPS == 24


def test_parse_step_char_cap() -> None:
    long_step = "x" * 500
    body = f"## Proposed changes\n\n1. {long_step}"
    steps = plan_ledger.parse_plan_steps(body)
    assert len(steps[0]) <= plan_ledger._MAX_STEP_CHARS


# step_kind


def test_step_kind_add_rewire_unknown() -> None:
    assert plan_ledger.step_kind("unique — drop dupes by email.") == ("add", "unique")
    assert plan_ledger.step_kind("group_by — count rows") == ("add", "group_by")
    assert plan_ledger.step_kind("(re-wire) — disconnect a, reconnect b") == ("rewire", None)
    assert plan_ledger.step_kind("(rewire) something") == ("rewire", None)
    # No node-type token before a separator → unknown.
    assert plan_ledger.step_kind("group_by#2 change agg") == ("unknown", None)


# annotate_done


def test_annotate_rewire_consumes_pair_not_two_steps() -> None:
    """A real re-wire executes as TWO recorded ops (delete_connection +
    connect). One completed re-wire must satisfy ONE rewire step, not two —
    otherwise the ledger over-counts, has_pending_steps flips False early,
    and the run stops one rewire short (the original early-stop bug)."""
    steps = plan_ledger.parse_plan_steps(_WORKED_EXAMPLE)
    completed = [
        sessions.CompletedOpEntry(tool_name="flowfile.graph.add_unique", node_id=6, summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.delete_connection", summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.connect", summary="x"),
    ]
    assert plan_ledger.annotate_done(steps, completed) == [True, True, False]


def test_annotate_connect_only_rewire_marks_one_step() -> None:
    """A connect-only re-wire (no delete recorded) still satisfies one rewire
    step; the second rewire stays pending."""
    steps = plan_ledger.parse_plan_steps(_WORKED_EXAMPLE)
    completed = [
        sessions.CompletedOpEntry(tool_name="flowfile.graph.add_unique", node_id=6, summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.connect", summary="x"),
    ]
    assert plan_ledger.annotate_done(steps, completed) == [True, True, False]


def test_annotate_two_full_rewire_pairs_all_done() -> None:
    steps = plan_ledger.parse_plan_steps(_WORKED_EXAMPLE)
    completed = [
        sessions.CompletedOpEntry(tool_name="flowfile.graph.add_unique", node_id=6, summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.delete_connection", summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.connect", summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.delete_connection", summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.connect", summary="x"),
    ]
    assert plan_ledger.annotate_done(steps, completed) == [True, True, True]


def test_annotate_deviation_leaves_pending() -> None:
    steps = plan_ledger.parse_plan_steps(_WORKED_EXAMPLE)
    # Only the add happened — both rewire steps stay pending.
    completed = [
        sessions.CompletedOpEntry(tool_name="flowfile.graph.add_unique", node_id=6, summary="x"),
    ]
    assert plan_ledger.annotate_done(steps, completed) == [True, False, False]


def test_annotate_unknown_step_consumes_leftover() -> None:
    steps = ["group_by#2 change agg to count"]  # unknown kind
    completed = [
        sessions.CompletedOpEntry(
            tool_name="flowfile.graph.update_node_settings", node_id=2, summary="x"
        ),
    ]
    assert plan_ledger.annotate_done(steps, completed) == [True]


def test_annotate_never_raises_on_empty() -> None:
    assert plan_ledger.annotate_done([], []) == []
    assert plan_ledger.annotate_done(["sort — x"], []) == [False]


# record_completed_op + summary shapes


def test_record_completed_op_summary_shapes() -> None:
    session = _bare_session()
    plan_ledger.record_completed_op(
        session, tool_name="flowfile.graph.add_filter", tool_args={}, node_id=5
    )
    plan_ledger.record_completed_op(
        session,
        tool_name="flowfile.graph.connect",
        tool_args={"from_node_id": 5, "to_node_id": 2},
        node_id=None,
    )
    plan_ledger.record_completed_op(
        session,
        tool_name="flowfile.graph.delete_connection",
        tool_args={"upstream_node_id": 1, "downstream_node_id": 3},
        node_id=None,
    )
    plan_ledger.record_completed_op(
        session,
        tool_name="flowfile.graph.update_node_settings",
        tool_args={},
        node_id=9,
    )
    summaries = [op.summary for op in session.plan_completed_ops]
    assert summaries == [
        "add_filter — staged as node 5",
        "connect 5→2",
        "delete_connection 1→3",
        "update_node_settings on node 9",
    ]


# has_pending_steps + block + nudge + reset


def test_has_pending_steps() -> None:
    session = _bare_session()
    assert plan_ledger.has_pending_steps(session) is False  # no plan
    session.plan_steps = plan_ledger.parse_plan_steps(_WORKED_EXAMPLE)
    assert plan_ledger.has_pending_steps(session) is True
    # One full re-wire pair done — the second rewire step is still pending.
    session.plan_completed_ops = [
        sessions.CompletedOpEntry(tool_name="flowfile.graph.add_unique", node_id=6, summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.delete_connection", summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.connect", summary="x"),
    ]
    assert plan_ledger.has_pending_steps(session) is True
    # Second pair lands — now everything is done.
    session.plan_completed_ops += [
        sessions.CompletedOpEntry(tool_name="flowfile.graph.delete_connection", summary="x"),
        sessions.CompletedOpEntry(tool_name="flowfile.graph.connect", summary="x"),
    ]
    assert plan_ledger.has_pending_steps(session) is False


def test_build_plan_progress_block() -> None:
    session = _bare_session()
    assert plan_ledger.build_plan_progress_block(session) is None  # no plan
    session.plan_steps = plan_ledger.parse_plan_steps(_WORKED_EXAMPLE)
    session.plan_completed_ops = [
        sessions.CompletedOpEntry(
            tool_name="flowfile.graph.add_unique", node_id=6, summary="add_unique — staged as node 6"
        ),
    ]
    block = plan_ledger.build_plan_progress_block(session)
    assert block is not None
    assert "## Plan progress (host-tracked)" in block
    assert "1. [done] unique" in block
    assert "2. [pending] (re-wire)" in block
    assert "add_unique — staged as node 6" in block
    assert "classify_intent" in block


def test_build_plan_nudge() -> None:
    session = _bare_session()
    session.plan_steps = plan_ledger.parse_plan_steps(_WORKED_EXAMPLE)
    nudge = plan_ledger.build_plan_nudge(session)
    assert "NOTE FROM HOST" in nudge
    assert "0/3" in nudge
    assert "unique — drop duplicate rows by email." in nudge


def test_reset_plan_ledger() -> None:
    session = _bare_session()
    session.plan_markdown = "x"
    session.plan_steps = ["a"]
    session.plan_completed_ops = [
        sessions.CompletedOpEntry(tool_name="flowfile.graph.add_filter", node_id=1, summary="x")
    ]
    plan_ledger.reset_plan_ledger(session)
    assert session.plan_markdown is None
    assert session.plan_steps == []
    assert session.plan_completed_ops == []


def _bare_session() -> sessions.AgentSession:
    snapshot = sessions.GraphSnapshot(flow_id=1, node_ids=(1,), node_types={1: "manual_input"})
    return sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="fake",
        snapshot=snapshot,
    )
