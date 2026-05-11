""":mod:`flowfile_core.ai.agents.planner` tests.

Cases covered:

* happy-path single tool call → diff registered, status=completed
* multi-step (filter → sort) → diff has 2 additions; second's insertion_context chains the first
* surface=agent two-stage routing — pick_category surfaces only meta tool, then narrows
* surface=agent_complex one-shot — full catalog from step 1
* retry on rejection: 2 rejections then success → step succeeds, retries reset
* 3 consecutive rejections → status=failed
* drift mid-stream → drift_detected + paused, status=paused_drift, generator exits
* abort flag → emits abort event and exits
* max_steps cap → status=failed with max_steps reason
* no tool_calls and empty staged_results → completed with diff_id=None
* `bundle_staged_results` integration: end-to-end through register_diff
* lazy-litellm contract
* surface lockstep
* insertion_context derived from prior staged add_*
* _allocate_node_id collision-free across live nodes + staged adds
* _resolve_insertion_context honors LLM-supplied upstream override
* tool dispatch raising → treated as rejected
* completion event has diff_payload echoed for frontend reconstruction
"""

from __future__ import annotations

import asyncio
import json
import sys
from collections.abc import Iterator
from typing import Any

import pytest

from flowfile_core.ai import audit, sessions
from flowfile_core.ai import diff as diff_module
from flowfile_core.ai.agents.planner import (
    PlannerEvent,
    _allocate_node_id,
    _arg_summary,
    _capture_rationale,
    _classify_op_kind,
    _resolve_insertion_context,
    _summarise_result_for_llm,
    run_planner_session,
)
from flowfile_core.ai.tools.executor import ToolExecutionResult
from flowfile_core.ai.context.builder import SURFACE_TO_LEVEL
from flowfile_core.ai.providers.base import ChatResponse, Message, ToolCall, Usage
from flowfile_core.ai.scheduler import RateLimitScheduler
from flowfile_core.ai.tools.registry import SURFACE_PRESETS
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas

# --------------------------------------------------------------------------- #
# Test helpers #
# --------------------------------------------------------------------------- #


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_w40_planner",
    )


def _add_orders(flow: FlowGraph, node_id: int = 1) -> None:
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
                input_schema.MinimalFieldInfo(name="amount", data_type="Double"),
            ],
            data=[[1, 2, 3], ["EU", "US", "EU"], [10.0, 20.0, 30.0]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(node_id).get_predicted_schema()


def _make_flow(flow_id: int = 1) -> FlowGraph:
    flow = FlowGraph(flow_settings=_flow_settings(flow_id), name="w40_planner_test")
    _add_orders(flow)
    return flow


def _make_session(flow: FlowGraph, *, surface: str = "agent_complex", user_id: int = 1) -> sessions.AgentSession:
    snapshot = sessions.capture_graph_snapshot(flow)
    return sessions.AgentSession(
        flow_id=flow.flow_id,
        user_id=user_id,
        user_prompt="filter to EU then sort by amount desc",
        surface=surface,  # type: ignore[arg-type]
        provider_name="fake",
        snapshot=snapshot,
        max_steps=8,
    )


def _filter_args(*, value: str = "EU", node_id: int | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "filter_input": {
            "filter_type": "advanced",
            "advanced_filter": f"[region]=='{value}'",
        },
    }
    if node_id is not None:
        out["node_id"] = node_id
    return out


def _bad_filter_args_basic_unknown_column() -> dict[str, Any]:
    """Filter args that the executor will reject via column-ref validation."""
    return {
        "filter_input": {
            "filter_type": "basic",
            "basic_filter": {
                "field": "ghost_column",
                "operator": "==",
                "value": "x",
            },
        },
    }


def _select_args(*, drop_old: bool = False) -> dict[str, Any]:
    return {
        "select_input": [
            {
                "old_name": "region",
                "new_name": "region",
                "data_type": "String",
                "data_type_change": False,
                "join_key": False,
                "is_altered": False,
                "position": 0,
                "is_available": True,
                "keep": True,
            },
        ],
        "keep_missing": True,
    }


class _Step:
    """One scripted provider response (assistant-turn)."""

    def __init__(
        self,
        *,
        tool_calls: list[ToolCall] | None = None,
        content: str | None = None,
        finish_reason: str = "tool_calls",
    ) -> None:
        self.tool_calls = tool_calls or []
        self.content = content
        self.finish_reason = finish_reason


class _ScriptedProvider:
    """Returns one ``_Step`` per call in declaration order; raises on overflow."""

    name = "fake"
    model = "fake-default"
    supports_tools = True
    supports_streaming = True

    def __init__(self, steps: list[_Step]) -> None:
        self._steps = list(steps)
        self.calls: list[dict[str, Any]] = []

    async def chat(
        self,
        messages: list[Any],
        tools: list[Any] | None = None,
        max_tokens: int | None = None,
        response_format: dict[str, Any] | None = None,
        *,
        surface: str | None = None,
        session_id: str | None = None,
        user_id: int | None = None,
    ) -> ChatResponse:
        if not self._steps:
            raise AssertionError("scripted provider exhausted")
        step = self._steps.pop(0)
        self.calls.append(
            {
                "messages": list(messages),
                "tools": list(tools) if tools else [],
                "max_tokens": max_tokens,
                "response_format": response_format,
            }
        )
        return ChatResponse(
            model=self.model,
            content=step.content,
            tool_calls=step.tool_calls,
            finish_reason=step.finish_reason,
            usage=Usage(),
        )

    def stream(self, *_a, **_k):  # pragma: no cover — planner uses chat only
        raise AssertionError("planner must use chat(), not stream()")


def _no_wait_scheduler() -> RateLimitScheduler:
    return RateLimitScheduler(time_source=lambda: 0.0, sleep=lambda *_a, **_k: asyncio.sleep(0))


async def _drain(gen) -> list[PlannerEvent]:
    out: list[PlannerEvent] = []
    async for ev in gen:
        out.append(ev)
    return out


# --------------------------------------------------------------------------- #
# Fixtures #
# --------------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def _reset_stores() -> Iterator[None]:
    sessions.clear_for_tests()
    diff_module.clear_for_tests()
    yield
    sessions.clear_for_tests()
    diff_module.clear_for_tests()


# --------------------------------------------------------------------------- #
# Lazy-litellm contract + surface lockstep #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract() -> None:
    # If litellm was already imported by a prior test, drop it before re-import.
    sys.modules.pop("litellm", None)
    sys.modules.pop("flowfile_core.ai.agents.planner", None)
    from flowfile_core.ai.agents import planner as _planner  # noqa: F401

    assert "litellm" not in sys.modules


def test_surface_lockstep() -> None:
    """legacy ``"agent"`` removed; ``agent_complex`` and
    ``agent_staged`` are the planner surfaces."""
    assert {"agent_complex", "agent_staged"} <= SURFACE_PRESETS.keys()
    assert SURFACE_TO_LEVEL["agent_complex"] == "planner"
    assert SURFACE_TO_LEVEL["agent_staged"] == "planner"


# --------------------------------------------------------------------------- #
# Pure helpers #
# --------------------------------------------------------------------------- #


def test_allocate_node_id_collision_free() -> None:
    flow = _make_flow()  # node 1 occupied
    sess = _make_session(flow)
    # Pre-stage one additional add_filter with node_id=2 — allocator should pick 3.
    sess.staged_results.append(
        diff_module.StagedToolEntry(
            tool_name="flowfile.graph.add_filter",
            audit_id=None,
            staged_node_payload={
                "node_type": "filter",
                "settings": {"node_id": 2, "flow_id": 1},
                "insertion_context": {"upstream_node_ids": [1]},
            },
        )
    )
    nid = _allocate_node_id(flow, sess)
    assert nid == 3


def test_resolve_insertion_context_uses_llm_override() -> None:
    flow = _make_flow()
    sess = _make_session(flow)
    tc = ToolCall(
        id="t1",
        name="flowfile.graph.add_filter",
        arguments={"upstream_node_ids": [42], "pos_x": 100.0, "pos_y": 200.0},
    )
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [42]
    assert ctx.pos_x == 100.0
    assert ambiguous is None


def test_resolve_insertion_context_chains_from_staged_addition() -> None:
    flow = _make_flow()
    sess = _make_session(flow)
    sess.staged_results.append(
        diff_module.StagedToolEntry(
            tool_name="flowfile.graph.add_filter",
            audit_id=None,
            staged_node_payload={
                "node_type": "filter",
                "settings": {"node_id": 7, "flow_id": 1},
                "insertion_context": {"upstream_node_ids": [1]},
            },
        )
    )
    tc = ToolCall(id="t2", name="flowfile.graph.add_select", arguments={})
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [7]
    assert ambiguous is None


def test_resolve_insertion_context_falls_back_to_live_node() -> None:
    """Tier 7 — single live node is unambiguous, falls back to that node."""
    flow = _make_flow()
    sess = _make_session(flow)
    tc = ToolCall(id="t3", name="flowfile.graph.add_filter", arguments={})
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [1]
    assert ambiguous is None


# --------------------------------------------------------------------------- #
# settings-field hint, selection, pin, ambiguity refusal #
# --------------------------------------------------------------------------- #


def test_insertion_uses_settings_field_when_no_upstream_param() -> None:
    """Tier 2 — LLM provided ``depending_on_id=2`` in args but no
    ``upstream_node_ids``. Replicates the live audit-trail trace where
    ``live=[1..7]`` and the buggy resolver returned ``[7]``; with the
    settings-field tier, the resolver canonicalises to ``[2]``.
    """
    flow = _make_flow()
    _add_orders(flow, node_id=2)
    _add_orders(flow, node_id=3)
    _add_orders(flow, node_id=7)
    sess = _make_session(flow)
    tc = ToolCall(
        id="settings-field",
        name="flowfile.graph.add_record_count",
        arguments={"depending_on_id": 2, "node_id": 8, "flow_id": 1},
    )
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [2]
    assert ambiguous is None


def test_settings_field_loses_to_explicit_upstream_param() -> None:
    """Tier 1 wins over Tier 2: when both ``upstream_node_ids`` and
    ``depending_on_id`` are present, the planner param is canonical."""
    flow = _make_flow()
    _add_orders(flow, node_id=2)
    _add_orders(flow, node_id=3)
    sess = _make_session(flow)
    tc = ToolCall(
        id="both",
        name="flowfile.graph.add_filter",
        arguments={"upstream_node_ids": [3], "depending_on_id": 2},
    )
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [3]
    assert ambiguous is None


def test_insertion_uses_selection_when_no_llm_override() -> None:
    """Tier 4 — session has selected_node_ids; LLM provided no upstream
    AND no settings-field hint → selection wins over the buggy
    last-node fallback."""
    flow = _make_flow()
    _add_orders(flow, node_id=2)
    _add_orders(flow, node_id=3)
    sess = _make_session(flow)
    sess.selected_node_ids = [2]
    tc = ToolCall(id="sel", name="flowfile.graph.add_filter", arguments={})
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [2]
    assert ambiguous is None


def test_insertion_falls_back_to_last_live_node_when_multiple_present() -> None:
    """Tier 6 — multiple live nodes, no selection, no pin, no LLM override,
    no settings-field hint → fall back to ``live_nodes[-1]``. (Was a
    refusal-on-ambiguity in; reverted 2026-05-07 because the refusal
    blocked the agent on simple prompts in multi-node flows.)"""
    flow = _make_flow()
    _add_orders(flow, node_id=2)
    _add_orders(flow, node_id=3)
    sess = _make_session(flow)
    tc = ToolCall(id="fallback", name="flowfile.graph.add_filter", arguments={})
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    # Falls back to the most-recently-added live node (node 3).
    assert ctx.upstream_node_ids == [3]
    # ``ambiguous_detail`` is always None post-revert; tuple shape preserved.
    assert ambiguous is None


def test_insertion_falls_back_to_last_node_when_single_live_node() -> None:
    """Tier 7 — exactly one live node, no other signal → safe fallback.
    Cold-flow first step is unambiguous, so the old fallback survives."""
    flow = _make_flow()
    sess = _make_session(flow)
    tc = ToolCall(id="single", name="flowfile.graph.add_filter", arguments={})
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [1]
    assert ambiguous is None


def test_insertion_fallback_skips_terminal_explore_data_node() -> None:
    """Tier 6 walks live_nodes in reverse and skips sinks (template.output==0).

    Reproduces the customer_deduplication dogfood failure: the template ends in an ``explore_data`` node, the LLM
    omitted ``upstream_node_ids``, and the resolver's ``flow.nodes[-1]``
    blind fallback attached the staged group_by downstream of the sink.
    The fix walks in reverse and picks the most-recent non-sink — node 3
    (the second manual_input) here, just like a real flow ending in a
    select before explore_data.
    """
    flow = _make_flow()  # node 1: manual_input (output > 0)
    _add_orders(flow, node_id=2)  # node 2: manual_input
    _add_orders(flow, node_id=3)  # node 3: manual_input
    flow.add_explore_data(
        input_schema.NodeExploreData(
            flow_id=flow.flow_id,
            node_id=4,  # node 4: explore_data (template.output == 0)
        )
    )
    sess = _make_session(flow)
    tc = ToolCall(id="skip-sink", name="flowfile.graph.add_filter", arguments={})
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    # Skips node 4 (sink), falls back to node 3 (last non-sink).
    assert ctx.upstream_node_ids == [3]
    assert ambiguous is None


def test_llm_override_wins_over_selection() -> None:
    """Tier 1 wins over Tier 4: explicit upstream_node_ids beats selection."""
    flow = _make_flow()
    _add_orders(flow, node_id=2)
    sess = _make_session(flow)
    sess.selected_node_ids = [2]
    tc = ToolCall(
        id="override",
        name="flowfile.graph.add_filter",
        arguments={"upstream_node_ids": [1]},
    )
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [1]
    assert ambiguous is None


def test_in_batch_staged_wins_over_selection() -> None:
    """Tier 3 wins over Tier 4: chained add_filter → add_sort still chains
    onto the most-recent staged add even when selection is set."""
    flow = _make_flow()
    _add_orders(flow, node_id=2)
    sess = _make_session(flow)
    sess.selected_node_ids = [2]
    sess.staged_results.append(
        diff_module.StagedToolEntry(
            tool_name="flowfile.graph.add_filter",
            audit_id=None,
            staged_node_payload={
                "node_type": "filter",
                "settings": {"node_id": 5, "flow_id": 1},
                "insertion_context": {"upstream_node_ids": [1]},
            },
        )
    )
    tc = ToolCall(id="chain", name="flowfile.graph.add_sort", arguments={})
    ctx, ambiguous = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [5]
    assert ambiguous is None


# --------------------------------------------------------------------------- #
# Loop happy-path #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_happy_path_single_tool_call() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                content="adding a filter",
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], content="done.", finish_reason="stop"),
        ]
    )

    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    assert "tool_call_proposed" in names
    assert "tool_call_staged" in names
    complete = [e for e in events if e.event == "complete"][0]
    assert complete.payload["op_count"] == 1
    assert complete.payload["diff_id"] is not None
    assert complete.payload["diff_payload"] is not None
    assert sess.status == "completed"
    assert sess.diff_id is not None
    # Live graph wasn't mutated — staging only.
    assert flow.get_node(2) is None


@pytest.mark.asyncio
async def test_happy_path_multi_step_chains_upstream() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    provider = _ScriptedProvider(
        [
            # Step 1: add filter
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            # Step 2: add select on top of the filter
            _Step(
                tool_calls=[ToolCall(id="t2", name="flowfile.graph.add_select", arguments=_select_args())],
                finish_reason="tool_calls",
            ),
            # Step 3: done
            _Step(tool_calls=[], content="all done", finish_reason="stop"),
        ]
    )

    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    staged_events = [e for e in events if e.event == "tool_call_staged"]
    assert len(staged_events) == 2
    assert sess.status == "completed"
    diff = diff_module.get_diff(sess.diff_id)
    assert diff is not None
    assert len(diff.additions) == 2
    # Second addition's insertion_context.upstream_node_ids = [first addition's node_id]
    first_add_id = diff.additions[0].settings["node_id"]
    assert diff.additions[1].insertion_context.upstream_node_ids == [first_add_id]


@pytest.mark.asyncio
async def test_w62_chained_agent_stages_have_non_overlapping_coords() -> None:
    """chained add_filter → add_select staged by the agent must produce
    non-overlapping canvas coords. previously both landed at (0, 0) because
    ``InsertionContext.pos_x`` / ``pos_y`` defaulted to ``0.0`` and the LLM
    never invents screen coordinates.
    """
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            _Step(
                tool_calls=[ToolCall(id="t2", name="flowfile.graph.add_select", arguments=_select_args())],
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )

    await _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))

    diff = diff_module.get_diff(sess.diff_id)
    assert diff is not None
    assert len(diff.additions) == 2
    coord_a = (diff.additions[0].insertion_context.pos_x, diff.additions[0].insertion_context.pos_y)
    coord_b = (diff.additions[1].insertion_context.pos_x, diff.additions[1].insertion_context.pos_y)
    # previously regression: both at (0, 0).
    assert coord_a != (0.0, 0.0)
    assert coord_b != (0.0, 0.0)
    # Chained: each new node lays out further to the right than its upstream.
    assert coord_b[0] > coord_a[0]
    # Settings.pos_x is what the apply path actually consumes (via
    # ``set_node_information``); make sure it carries the same coord.
    assert diff.additions[0].settings["pos_x"] == coord_a[0]
    assert diff.additions[1].settings["pos_x"] == coord_b[0]


# --------------------------------------------------------------------------- #
# Surface routing #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_surface_agent_complex_uses_full_catalog_from_step_one() -> None:
    """legacy two-stage ``surface="agent"`` was removed
    (the failure mode that motivated). ``agent_complex`` keeps its
    single-shot full-catalog behavior for big-model power users; this
    test pins the catalog shape."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    provider = _ScriptedProvider(
        [
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )

    await _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))

    first_tools = {tool.name for tool in provider.calls[0]["tools"]}
    # Full catalog: every add_* node-type tool plus universal ops.
    assert "flowfile.graph.add_filter" in first_tools
    assert "flowfile.graph.add_select" in first_tools
    assert "flowfile.graph.add_join" in first_tools
    assert "flowfile.schema.read_node_schema" in first_tools
    # The legacy ``flowfile.meta.pick_category`` was removed alongside
    # the ``surface="agent"`` flow; assert it's no longer in the catalog.
    assert "flowfile.meta.pick_category" not in first_tools


# --------------------------------------------------------------------------- #
# Retry on rejection #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_retry_then_success_resets_counter() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    bad_args = _bad_filter_args_basic_unknown_column()

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=bad_args)],
                finish_reason="tool_calls",
            ),
            _Step(
                tool_calls=[ToolCall(id="t2", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )

    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    assert "tool_call_rejected" in names
    assert "tool_call_staged" in names
    assert sess.status == "completed"


@pytest.mark.asyncio
async def test_three_consecutive_rejections_fail() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    bad_args = _bad_filter_args_basic_unknown_column()

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id=f"t{i}", name="flowfile.graph.add_filter", arguments=bad_args)],
                finish_reason="tool_calls",
            )
            for i in range(5)  # plenty of attempts
        ]
    )

    events = await _drain(
        run_planner_session(
            session=sess,
            flow=flow,
            provider=provider,
            scheduler=_no_wait_scheduler(),
            max_retries_per_step=3,
        )
    )
    assert sess.status == "failed"
    assert any(e.event == "error" and "rejected" in str(e.payload).lower() for e in events)


# --------------------------------------------------------------------------- #
# Drift #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_drift_mid_stream_pauses_session() -> None:
    """External addition mid-stream → drift fires (W45 — id-set only)."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    # User adds an external node before the drift check fires.
    _add_orders(flow, node_id=42)

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    assert "drift_detected" in names
    assert "paused" in names
    assert sess.status == "paused_drift"
    assert sess.drift_detail is not None
    assert 42 in sess.drift_detail.external_added_node_ids


@pytest.mark.asyncio
async def test_drift_missing_external_deletion() -> None:
    """user deleting a node mid-run still fires drift via missing_node_ids."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    flow.delete_node(1)

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    assert any(e.event == "drift_detected" for e in events)
    assert sess.status == "paused_drift"
    assert sess.drift_detail is not None
    assert 1 in sess.drift_detail.missing_node_ids


@pytest.mark.asyncio
async def test_three_consecutive_stages_does_not_self_drift() -> None:
    """Q1 acceptance #1 — agent stages 3 nodes back-to-back without drift.

    previously the agent self-drifted because ``_resolve_upstream_schemas``
    warmed live ``predicted_schema`` as a side-effect, and the (then) hash-
    based ``schema_changed`` bucket flagged that as drift. dropped the
    hash buckets and tracks the agent's own staged ids — the agent's own
    additions are excluded from the new ``external_added_node_ids`` bucket
    so subsequent iterations see no drift.
    """
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            _Step(
                tool_calls=[ToolCall(id="t2", name="flowfile.graph.add_filter", arguments=_filter_args(value="US"))],
                finish_reason="tool_calls",
            ),
            _Step(
                tool_calls=[ToolCall(id="t3", name="flowfile.graph.add_filter", arguments=_filter_args(value="UK"))],
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], content="done.", finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    assert "drift_detected" not in names
    assert "paused" not in names
    assert sess.status == "completed"
    assert names.count("tool_call_staged") == 3
    assert len(sess.staged_node_ids) == 3
    # Allocator picks 2 / 3 / 4 in order (live had id 1; in-batch chained adds).
    assert sess.staged_node_ids == [2, 3, 4]


@pytest.mark.asyncio
async def test_planner_records_staged_node_ids_for_add_calls() -> None:
    """Q1 — every successful add_<node_type> stage appends to staged_node_ids."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )
    await _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))
    assert sess.staged_node_ids == [2]


@pytest.mark.asyncio
async def test_external_drift_still_detected_after_one_stage() -> None:
    """Q1 acceptance #3 — external mutation between iterations still pauses.

    The provider's first ``chat`` call simulates the user adding node 99 to
    the canvas while the LLM is "thinking" (after iteration 1's drift check
    has already cleared) and returns an ``add_filter`` tool call. The
    planner stages node 2 from that tool call, and on iteration 2's drift
    check, only node 99 (not 2) appears in ``external_added_node_ids``.
    """
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    class _ScriptedWithSideEffect:
        name = "fake"
        model = "fake"
        supports_tools = True
        supports_streaming = True

        def __init__(self) -> None:
            self.call_count = 0

        async def chat(self, *_a, **_k):
            self.call_count += 1
            if self.call_count == 1:
                # User adds an external node mid-LLM-thinking. The agent's
                # next drift check (start of iteration 2) should see it.
                _add_orders(flow, node_id=99)
                # Explicit ``upstream_node_ids`` so the test doesn't depend on
                # the resolver's live-graph fallback (which would attach to
                # node 99 — the externally-added one — and confuse the test
                # intent). The ambiguity-refusal that this comment used to
                # work around was reverted 2026-05-07; the explicit override
                # stays for test stability.
                args = _filter_args()
                args["upstream_node_ids"] = [1]
                return ChatResponse(
                    model="fake",
                    content="staging",
                    tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=args)],
                    finish_reason="tool_calls",
                    usage=Usage(),
                )
            return ChatResponse(  # pragma: no cover — planner pauses before reaching here
                model="fake",
                content="unreachable",
                tool_calls=[],
                finish_reason="stop",
                usage=Usage(),
            )

        def stream(self, *_a, **_k):
            raise AssertionError

    provider = _ScriptedWithSideEffect()
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    assert any(e.event == "drift_detected" for e in events)
    assert sess.status == "paused_drift"
    assert sess.drift_detail is not None
    assert 99 in sess.drift_detail.external_added_node_ids
    # The planner recorded its own stage in staged_node_ids — whatever id
    # got allocated, it must NOT also appear in external_added.
    assert sess.staged_node_ids
    for sid in sess.staged_node_ids:
        assert sid not in sess.drift_detail.external_added_node_ids


# --------------------------------------------------------------------------- #
# Abort #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_abort_during_run_emits_abort_event() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    sess.status = "aborted"  # simulate external abort before first iteration

    provider = _ScriptedProvider([_Step(tool_calls=[], finish_reason="stop")])
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    assert "abort" in names


# --------------------------------------------------------------------------- #
# Misc edge cases #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_max_steps_caps_loop() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    sess.max_steps = 2

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id=f"t{i}",
                        name="flowfile.graph.add_filter",
                        arguments=_filter_args(node_id=10 + i),
                    )
                ],
                finish_reason="tool_calls",
            )
            for i in range(5)
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    assert sess.status == "failed"
    assert any(e.event == "error" and "max_steps" in str(e.payload).lower() for e in events)


@pytest.mark.asyncio
async def test_empty_response_completes_with_no_diff() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    provider = _ScriptedProvider([_Step(tool_calls=[], content="nothing to do", finish_reason="stop")])
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    complete = [e for e in events if e.event == "complete"][0]
    assert complete.payload["diff_id"] is None
    assert complete.payload["op_count"] == 0
    assert sess.status == "completed"


@pytest.mark.asyncio
async def test_provider_error_fails_session() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    class _Boom:
        name = "fake"
        model = "fake"
        supports_tools = True
        supports_streaming = True

        async def chat(self, *a, **k):
            raise RuntimeError("provider down")

        def stream(self, *_a, **_k):
            raise AssertionError

    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=_Boom(), scheduler=_no_wait_scheduler())
    )
    assert any(e.event == "error" for e in events)
    assert sess.status == "failed"


@pytest.mark.asyncio
async def test_complete_event_carries_diff_payload() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    complete = [e for e in events if e.event == "complete"][0]
    payload = complete.payload["diff_payload"]
    assert payload is not None
    # Frontend can synthesise a GraphDiffPayload from this directly.
    assert payload["additions"][0]["node_type"] == "filter"
    assert payload["additions"][0]["insertion_context"]["upstream_node_ids"] == [1]


@pytest.mark.asyncio
async def test_cannot_run_failed_session() -> None:
    """``completed`` is now a followup-resumable entry state, but
    ``failed`` / ``aborted`` (and other non-resumable states) still error."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    sess.status = "failed"
    provider = _ScriptedProvider([_Step(tool_calls=[], finish_reason="stop")])
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    assert events[0].event == "error"
    assert "failed" in str(events[0].payload).lower()


@pytest.mark.asyncio
async def test_resume_from_paused_drift_resnapshots() -> None:
    """Resume after drift re-snapshots so the next drift_detect compares fresh."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    sess.status = "paused_drift"
    sess.drift_detail = sessions.DriftDetail(missing_node_ids=[42])
    sess.pause_reason = "graph_changed"

    provider = _ScriptedProvider([_Step(tool_calls=[], content="resumed", finish_reason="stop")])
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    assert "info" in names  # the "resumed; re-snapshotted" event
    assert sess.status == "completed"
    assert sess.drift_detail is None
    assert sess.pause_reason is None


# --------------------------------------------------------------------------- #
# Step narration, op_kind classification, meta-op suppression #
# --------------------------------------------------------------------------- #


def test_w38_classify_op_kind_meta() -> None:
    assert _classify_op_kind("flowfile.meta.classify_intent") == "meta"


def test_w38_classify_op_kind_graph() -> None:
    assert _classify_op_kind("flowfile.graph.add_filter") == "graph"
    assert _classify_op_kind("flowfile.graph.connect") == "graph"
    assert _classify_op_kind("flowfile.graph.delete_node") == "graph"


def test_w38_classify_op_kind_schema() -> None:
    assert _classify_op_kind("flowfile.schema.read_node_schema") == "schema"
    assert _classify_op_kind("flowfile.schema.read_node_preview") == "schema"


def test_w38_classify_op_kind_codegen() -> None:
    assert _classify_op_kind("flowfile.codegen.generate_polars_code") == "codegen"


def test_w38_classify_op_kind_unknown_falls_through() -> None:
    assert _classify_op_kind("vendor.thing.do_stuff") == "unknown"


def test_w38_capture_rationale_strips_whitespace() -> None:
    assert _capture_rationale("  Filtering null regions.  \n") == "Filtering null regions."


def test_w38_capture_rationale_returns_none_for_empty() -> None:
    assert _capture_rationale("") is None
    assert _capture_rationale("   ") is None
    assert _capture_rationale(None) is None


def test_w38_capture_rationale_clips_overlong_text() -> None:
    long_text = "Sentence one. " + ("padding " * 80) + "Sentence two."
    out = _capture_rationale(long_text)
    assert out is not None
    assert len(out) <= 281  # cap + optional ellipsis byte
    # Should prefer to clip on a sentence boundary when possible.
    assert out.endswith(".") or out.endswith("…")


def test_w38_arg_summary_filter_renders_predicate() -> None:
    args = {"settings_input": {"filter_input": {"advanced_filter": "[region]=='EU'"}}}
    summary = _arg_summary("flowfile.graph.add_filter", args)
    assert summary is not None
    assert "[region]=='EU'" in summary


def test_w38_arg_summary_join_renders_keys_and_how() -> None:
    args = {
        "settings_input": {
            "join_input": {
                "how": "left",
                "join_mapping": [
                    {"left_col": "region_code", "right_col": "code"},
                ],
            },
        },
    }
    summary = _arg_summary("flowfile.graph.add_join", args)
    assert summary is not None
    assert "region_code=code" in summary
    assert summary.lower().startswith("left join")


def test_w38_arg_summary_meta_returns_none() -> None:
    assert _arg_summary("flowfile.meta.classify_intent", {"op_kind": "add"}) is None


def test_w38_arg_summary_connect() -> None:
    summary = _arg_summary(
        "flowfile.graph.connect",
        {"upstream_node_id": 5, "downstream_node_id": 7},
    )
    assert summary == "Connecting node 5 → node 7"


def test_w38_arg_summary_falls_back_to_generic_for_unknown_node_type() -> None:
    summary = _arg_summary("flowfile.graph.add_some_unknown_type", {})
    assert summary == "Adding some unknown type"


@pytest.mark.asyncio
async def test_w38_tool_call_proposed_carries_op_kind_rationale_arg_summary() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                content="Filtering to EU rows so the join doesn't drop unrelated data.",
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    proposed = [e for e in events if e.event == "tool_call_proposed"]
    assert len(proposed) == 1
    payload = proposed[0].payload
    assert payload["op_kind"] == "graph"
    assert payload["rationale"] == "Filtering to EU rows so the join doesn't drop unrelated data."
    assert payload["arg_summary"] is not None
    assert "[region]=='EU'" in payload["arg_summary"]


@pytest.mark.asyncio
async def test_w38_tool_call_staged_carries_rationale_and_op_kind() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                content="Filtering null regions.",
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    staged = [e for e in events if e.event == "tool_call_staged"]
    assert len(staged) == 1
    payload = staged[0].payload
    assert payload["op_kind"] == "graph"
    assert payload["rationale"] == "Filtering null regions."
    assert payload["arg_summary"] is not None


@pytest.mark.asyncio
async def test_w38_rationale_falls_back_to_none_when_no_preamble() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                content=None,  # no preamble
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    staged = [e for e in events if e.event == "tool_call_staged"]
    payload = staged[0].payload
    assert payload["rationale"] is None
    # arg_summary still populates so the frontend has something to show.
    assert payload["arg_summary"] is not None
    assert "[region]=='EU'" in payload["arg_summary"]


# — ``test_w38_meta_pick_category_carries_op_kind_meta_no_rationale``
# was deleted. It tested the legacy two-stage ``flowfile.meta.pick_category``
# meta-routing dance, which is gone. The op_kind="meta" rationale-suppression
# contract still applies and is now exercised by ``test_planner_staged.py``
# against the staged meta tools (classify_intent / pick_node_type /
# pick_upstream).


@pytest.mark.asyncio
async def test_w38_rationale_attached_to_each_call_in_multi_call_round() -> None:
    """A single assistant turn that emits multiple tool calls shares one
    preamble — every event in the round carries the same captured rationale."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args()),
                    ToolCall(id="t2", name="flowfile.graph.add_select", arguments=_select_args()),
                ],
                content="Trimming rows then narrowing columns.",
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    proposed = [e for e in events if e.event == "tool_call_proposed"]
    assert [p.payload["rationale"] for p in proposed] == [
        "Trimming rows then narrowing columns.",
        "Trimming rows then narrowing columns.",
    ]


@pytest.mark.asyncio
async def test_w38_rejected_event_carries_op_kind_for_ui_styling() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t1",
                        name="flowfile.graph.add_filter",
                        arguments=_bad_filter_args_basic_unknown_column(),
                    )
                ],
                content="Filtering on a column that doesn't exist (this will be rejected).",
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    rejected = [e for e in events if e.event == "tool_call_rejected"]
    assert rejected, "expected at least one rejection"
    payload = rejected[0].payload
    assert payload["op_kind"] == "graph"
    # Rationale + arg_summary still attach so the frontend can render the
    # failed step the same way it renders a successful one.
    assert payload["rationale"] == "Filtering on a column that doesn't exist (this will be rejected)."
    assert payload["arg_summary"] is not None


# --------------------------------------------------------------------------- #
# self-loop prevention + resume staged_results hygiene #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_allocate_id_collides_with_resolved_upstream_is_refused() -> None:
    """AC1 — proposed ``node_id`` ∈ resolved upstream → ``self_loop_prevented``.

    Reproduces the user's transcript root cause via the LLM-collision path:
    LLM emits ``add_filter(node_id=3, upstream_node_ids=[3])``. The planner-
    side guard (which runs before ``execute_tool_call``) refuses cleanly.

    Asserts the full contract: rejected event, no staged event for that
    call, ``staged_results``/``staged_node_ids`` unchanged, audit row with
    ``__planner_meta__`` populated.
    """
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    pre_staged_results = list(sess.staged_results)
    pre_staged_node_ids = list(sess.staged_node_ids)

    bad_args = _filter_args(node_id=3)
    bad_args["upstream_node_ids"] = [3]
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=bad_args)],
                content="filtering rows that don't exist yet",
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(
            session=sess,
            flow=flow,
            provider=provider,
            scheduler=_no_wait_scheduler(),
            max_retries_per_step=3,
        )
    )

    # (a) tool_call_rejected event with self_loop_prevented reason — for THIS call id.
    rejections_for_t1 = [e for e in events if e.event == "tool_call_rejected" and e.payload.get("id") == "t1"]
    assert rejections_for_t1, "expected a tool_call_rejected event for t1"
    assert rejections_for_t1[0].payload["reason"] == "self_loop_prevented"
    assert "self-loop" in rejections_for_t1[0].payload["detail"]

    # (b) NO tool_call_staged for t1.
    staged_for_t1 = [e for e in events if e.event == "tool_call_staged" and e.payload.get("id") == "t1"]
    assert staged_for_t1 == [], "self_loop_prevented call must not stage"

    # (c) + (d) — staged_results / staged_node_ids unchanged across the call.
    assert sess.staged_results == pre_staged_results
    assert sess.staged_node_ids == pre_staged_node_ids

    # (e) Audit row written; tool_args carries __planner_meta__ with provenance.
    rows = audit.query_events(session_id=sess.session_id, limit=20)
    matching = [r for r in rows if r.tool_name == "flowfile.graph.add_filter" and r.result_status == "rejected"]
    assert matching, "audit row for the rejection must be present"
    args_blob = matching[0].tool_args
    assert args_blob, "audit row tool_args must not be empty"
    parsed = json.loads(args_blob)
    meta = parsed.get("__planner_meta__")
    assert isinstance(meta, dict)
    assert meta["llm_provided_node_id"] == 3
    # allocated_node_id is None when LLM provided one (allocator skipped).
    assert meta["allocated_node_id"] is None
    assert meta["resolved_upstream_node_ids"] == [3]
    assert meta["live_node_ids_at_stage"] == [1]
    assert meta["staged_node_ids_at_stage"] == []


@pytest.mark.asyncio
async def test_resume_drops_stale_staged_results_referencing_dead_upstream() -> None:
    """AC3 — pre-pause stale upstream reference is dropped on resume; next add chains cleanly."""
    flow = _make_flow()  # contains node 1 only
    sess = _make_session(flow, surface="agent_complex")
    # Simulate: pre-pause the agent staged node 7 chained off node 5; user
    # then deleted node 5 during the pause (we represent that by simply not
    # adding it to the live flow).
    sess.staged_results = [
        diff_module.StagedToolEntry(
            tool_name="flowfile.graph.add_filter",
            audit_id=None,
            staged_node_payload={
                "node_type": "filter",
                "settings": {"node_id": 7, "flow_id": 1},
                "insertion_context": {
                    "upstream_node_ids": [5],
                    "right_input_node_id": None,
                    "pos_x": 0.0,
                    "pos_y": 0.0,
                },
                "predicted_output_schema": [],
            },
        )
    ]
    sess.staged_node_ids = [7]
    sess.status = "paused_drift"
    sess.drift_detail = sessions.DriftDetail(missing_node_ids=[5])

    # Provider: post-resume the LLM emits one fresh add_filter with NO
    # explicit upstream — we exercise the 3rd-tier fallback to live_nodes[-1].
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], content="done.", finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )

    # Stale entry dropped from both lists.
    assert sess.staged_results, "expected the new t1 stage; the stale entry should be gone"
    assert all(e.staged_node_payload.get("settings", {}).get("node_id") != 7 for e in sess.staged_results)
    assert 7 not in sess.staged_node_ids

    # Drop info event surfaced.
    drop_events = [e for e in events if e.event == "info" and e.payload.get("dropped_count")]
    assert drop_events, "expected an info event noting the drop"
    assert drop_events[0].payload["dropped_count"] == 1
    assert drop_events[0].payload["drop_reasons"] == ["upstream_missing"]

    # Next-add semantics: with stale entry gone and no LLM-provided upstream,
    # resolution falls through to live_nodes[-1] == 1.
    new_entry = sess.staged_results[-1]
    assert new_entry.staged_node_payload["insertion_context"]["upstream_node_ids"] == [1]
    assert sess.status == "completed"


@pytest.mark.asyncio
async def test_resume_drops_stale_staged_results_with_now_live_id() -> None:
    """AC4 — pre-pause staged ``node_id`` is now live → entry dropped, audit row written."""
    flow = _make_flow()  # node 1
    # User manually added a node that got id 3 during the pause.
    _add_orders(flow, node_id=3)

    sess = _make_session(flow, surface="agent_complex")
    sess.staged_results = [
        diff_module.StagedToolEntry(
            tool_name="flowfile.graph.add_filter",
            audit_id=None,
            staged_node_payload={
                "node_type": "filter",
                "settings": {"node_id": 3, "flow_id": 1},
                "insertion_context": {
                    "upstream_node_ids": [1],
                    "right_input_node_id": None,
                    "pos_x": 0.0,
                    "pos_y": 0.0,
                },
                "predicted_output_schema": [],
            },
        )
    ]
    sess.staged_node_ids = [3]
    sess.status = "paused_drift"
    # Re-snapshot so detect_drift on the post-resume loop doesn't fire on the
    # already-known external addition (node 3) — the test is about staged
    # hygiene, not drift detection.
    sess.snapshot = sessions.capture_graph_snapshot(flow)
    sess.drift_detail = sessions.DriftDetail(external_added_node_ids=[3])

    provider = _ScriptedProvider([_Step(tool_calls=[], content="done.", finish_reason="stop")])
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )

    assert sess.staged_results == []
    assert sess.staged_node_ids == []

    drop_events = [e for e in events if e.event == "info" and e.payload.get("dropped_count")]
    assert drop_events
    assert drop_events[0].payload["drop_reasons"] == ["live_id_collision"]

    rows = audit.query_events(session_id=sess.session_id, limit=20)
    drop_rows = [r for r in rows if r.tool_name == "internal.staged_drop_on_resume"]
    assert drop_rows, "expected an audit row for the staged drop"
    assert drop_rows[0].result_status == "error"
    assert drop_rows[0].error and drop_rows[0].error.startswith("staged_drop_on_resume:live_id_collision")


@pytest.mark.asyncio
async def test_audit_row_for_add_includes_node_id_instrumentation() -> None:
    """AC5 — happy-path ``add_*`` audit row carries ``__planner_meta__`` instrumentation."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], content="done.", finish_reason="stop"),
        ]
    )
    await _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))
    rows = audit.query_events(session_id=sess.session_id, limit=20)
    add_rows = [r for r in rows if r.tool_name == "flowfile.graph.add_filter" and r.result_status == "success"]
    assert add_rows, "expected a successful add_filter audit row"

    parsed = json.loads(add_rows[0].tool_args or "{}")
    meta = parsed.get("__planner_meta__")
    assert isinstance(meta, dict), "expected __planner_meta__ instrumentation under tool_args"
    # All six keys present and non-None (lists may be empty, but the keys must exist).
    assert meta["allocated_node_id"] == 2  # planner-allocated since LLM didn't emit node_id
    assert meta["llm_provided_node_id"] is None
    assert meta["resolved_upstream_node_ids"] == [1]
    assert meta["right_input_node_id"] is None
    assert meta["live_node_ids_at_stage"] == [1]
    assert meta["staged_node_ids_at_stage"] == []


# --------------------------------------------------------------------------- #
# planner sees the per-node-type catalog block in its system prompt #
# --------------------------------------------------------------------------- #


def test_w56_planner_system_prompt_includes_node_catalog() -> None:
    """the planner system prompt for the agent surface includes the
    catalog header plus a representative slice of node-type docs.

    This doesn't test the *content* of the docs (that's test_tool_registry's
    job); it tests that the docs are wired through to the planner's system
    prompt so the model actually sees them. Without this, the catalog could
    land in the registry but never reach the LLM.
    """
    from flowfile_core.ai.context.builder import assemble_system_prompt

    # — legacy ``"agent"`` surface removed; assert the catalog
    # is wired into ``agent_complex`` (the only full-catalog planner
    # surface left).
    prompt = assemble_system_prompt("agent_complex")
    assert "## Tool catalog" in prompt, "catalog header missing from planner prompt"

    # At least three representative tools across categories must surface so
    # we catch a partial wiring (e.g. only graph-ops were fed in).
    representative = [
        "### flowfile.graph.add_filter",
        "### flowfile.graph.add_join",
        "### flowfile.graph.add_group_by",
    ]
    for heading in representative:
        assert heading in prompt, f"{heading} missing from planner system prompt"

    # Pointer line from prompts/planner.md must precede the catalog so the
    # model knows the section is coming.
    pointer_idx = prompt.find("Tool catalog")
    catalog_idx = prompt.find("## Tool catalog\n")
    assert pointer_idx >= 0, "pointer line from planner.md missing"
    assert catalog_idx > pointer_idx, "catalog block must follow the pointer line"


# --------------------------------------------------------------------------- #
# planner prompt warns against the redundant-connect pattern #
# --------------------------------------------------------------------------- #


def test_w70_planner_prompt_warns_against_redundant_connect() -> None:
    """the planner system prompt for the agent surface includes
    the connection-discipline section that tells the model not to emit
    a follow-up ``connect`` call after ``add_*`` (the executor already
    auto-wires) and never to invent a ``to_node_id`` integer.

    Substring assertions only — this doesn't test the LLM's behaviour,
    just that the new paragraph is wired through the layered prompt
    assembly so the model actually sees it.
    """
    from flowfile_core.ai.context.builder import assemble_system_prompt

    # — legacy ``"agent"`` surface removed; the paragraph
    # ships with the ``agent_complex`` surface and the
    # ``single_stage_op`` stage of ``agent_staged`` (both surfaces use
    # the legacy ``planner.md`` suffix).
    prompt = assemble_system_prompt("agent_complex")
    assert "## Connection discipline" in prompt, "W70 connection-discipline header missing"
    assert "Do NOT emit a follow-up" in prompt, "W70 redundant-connect refusal phrase missing"
    assert "never invent a fresh integer" in prompt, "W70 invent-id refusal phrase missing"

    prompt_staged_single_op = assemble_system_prompt(
        "agent_staged", stage="single_stage_op"
    )
    assert "## Connection discipline" in prompt_staged_single_op


# --------------------------------------------------------------------------- #
# Post-completion followup re-entry #
# --------------------------------------------------------------------------- #


def test_w49_looks_like_question_trailing_qmark() -> None:
    from flowfile_core.ai.agents.planner import _looks_like_question

    assert _looks_like_question("Should I drop nulls first?") is True
    assert _looks_like_question("Done.") is False
    assert _looks_like_question(None) is False
    assert _looks_like_question("   ") is False


def test_w49_looks_like_question_interrogative_token() -> None:
    """No trailing ``?`` but interrogative token wins."""
    from flowfile_core.ai.agents.planner import _looks_like_question

    assert _looks_like_question("Which column do you want me to filter on") is True
    assert _looks_like_question("Do you want me to also sort the result") is True
    assert _looks_like_question("This is how the join works") is True  # false-positive tolerated


def test_w49_inject_followup_message_user_message() -> None:
    """``user_message`` action appends the text verbatim as a user turn."""
    from flowfile_core.ai.agents.planner import inject_followup_message
    from flowfile_core.ai.providers.base import Message

    flow = _make_flow()
    sess = _make_session(flow)
    sess.messages = [
        Message(role="system", content="sys"),
        Message(role="user", content="goal"),
        Message(role="assistant", content="What columns?"),
    ]
    msg = inject_followup_message(sess, action="user_message", message="amount and region")
    assert msg.role == "user"
    assert msg.content == "amount and region"
    assert sess.messages[-1] is msg


def test_w49_inject_followup_message_user_message_empty_raises() -> None:
    from flowfile_core.ai.agents.planner import inject_followup_message

    flow = _make_flow()
    sess = _make_session(flow)
    with pytest.raises(ValueError):
        inject_followup_message(sess, action="user_message", message=None)
    with pytest.raises(ValueError):
        inject_followup_message(sess, action="user_message", message="   ")


def test_w49_inject_followup_message_rejected_diff_with_note() -> None:
    """Rejection note is included verbatim in the synthetic user turn."""
    from flowfile_core.ai.agents.planner import inject_followup_message

    flow = _make_flow()
    sess = _make_session(flow)
    sess.diff_id = "diff-abc"
    msg = inject_followup_message(
        sess,
        action="rejected_diff",
        message="please use the read node directly, not after the filter",
    )
    assert msg.role == "user"
    assert "please use the read node directly" in msg.content
    assert "rejected" in msg.content.lower()
    assert "diff-abc" in msg.content


def test_w49_inject_followup_message_rejected_diff_generic_when_no_note() -> None:
    """Generic fallback when the user clicks Reject without a note."""
    from flowfile_core.ai.agents.planner import (
        _REJECTED_DIFF_DEFAULT_NOTE,
        inject_followup_message,
    )

    flow = _make_flow()
    sess = _make_session(flow)
    sess.diff_id = "diff-xyz"
    msg = inject_followup_message(sess, action="rejected_diff", message=None)
    assert _REJECTED_DIFF_DEFAULT_NOTE in msg.content


def test_w49_inject_followup_message_rejected_diff_uses_explicit_diff_id() -> None:
    """Explicit ``rejected_diff_id`` wins over session.diff_id."""
    from flowfile_core.ai.agents.planner import inject_followup_message

    flow = _make_flow()
    sess = _make_session(flow)
    sess.diff_id = "diff-from-session"
    msg = inject_followup_message(
        sess,
        action="rejected_diff",
        rejected_diff_id="diff-from-request",
    )
    assert "diff-from-request" in msg.content
    assert "diff-from-session" not in msg.content


@pytest.mark.asyncio
async def test_w49_planner_emits_awaiting_user_input_on_question() -> None:
    """No tool calls, no staged_results, last assistant ends in ``?`` →
    ``awaiting_user_input`` (not ``complete``); session status flips to
    ``awaiting_user_input``."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[],
                content="Which column do you want me to group by?",
                finish_reason="stop",
            ),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    assert "awaiting_user_input" in names
    assert "complete" not in names
    assert sess.status == "awaiting_user_input"
    awaiting = [e for e in events if e.event == "awaiting_user_input"][0]
    assert awaiting.payload["session_id"] == sess.session_id
    assert "group by" in awaiting.payload["question"].lower()


@pytest.mark.asyncio
async def test_w49_planner_emits_complete_for_no_question_no_ops() -> None:
    """Mode 3 — no staged_results, last message is NOT a question → still
    ``complete`` so the legacy path stays intact for declarative finishes."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    provider = _ScriptedProvider([_Step(tool_calls=[], content="Here's an explanation. Done.", finish_reason="stop")])
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    assert "complete" in names
    assert "awaiting_user_input" not in names
    assert sess.status == "completed"


@pytest.mark.asyncio
async def test_w49_followup_re_entry_from_completed_resnapshots_and_resets() -> None:
    """Mode 1 entry path — a completed session with a registered diff_id
    re-enters the planner via the followup-resume preamble; the preamble
    re-snapshots the graph and drops the prior diff bookkeeping."""
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    sess.status = "completed"
    sess.diff_id = "old-diff"
    sess.rationale = "old rationale"
    sess.staged_results = [
        diff_module.StagedToolEntry(
            tool_name="flowfile.graph.add_filter",
            audit_id=None,
            staged_node_payload={"settings": {"node_id": 99}},
        )
    ]
    sess.staged_node_ids = [99]
    sess.last_assistant_text = "old"

    # Route would inject the synthetic message before calling run_planner_session.
    from flowfile_core.ai.agents.planner import inject_followup_message

    inject_followup_message(sess, action="user_message", message="please continue")

    provider = _ScriptedProvider([_Step(tool_calls=[], content="acknowledged.", finish_reason="stop")])
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    # First event is the followup info preamble.
    info = [e for e in events if e.event == "info" and e.payload.get("previous_status")]
    assert info, "expected a followup info preamble"
    assert info[0].payload["previous_status"] == "completed"

    # Bookkeeping was reset before the new round.
    assert sess.diff_id is None
    assert sess.staged_results == []
    assert sess.staged_node_ids == []
    assert sess.rationale is None
    # Run terminated as expected.
    assert "complete" in names or "awaiting_user_input" in names


@pytest.mark.asyncio
async def test_w49_followup_re_snapshots_before_drift_check() -> None:
    """the followup-resume preamble must re-snapshot the graph so a
    subsequent ``detect_drift`` compares against the *current* state, not
    the snapshot the original ``/start`` took.

    Concretely: after a session reaches ``completed`` with snapshot S0, the
    user manually adds node 2. Without the re-snapshot, the next followup
    iteration would fire ``drift_detected`` immediately on node 2 (an
    "external addition"). With the re-snapshot, the resumed run sees a
    fresh baseline and proceeds normally.
    """
    from flowfile_core.ai.agents.planner import inject_followup_message

    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    sess.status = "completed"

    # Original snapshot at /start time captured only node 1.
    sess.snapshot = sessions.capture_graph_snapshot(flow)

    # User added node 2 manually after the original session completed.
    _add_orders(flow, node_id=2)
    assert {n.node_id for n in flow.nodes} == {1, 2}

    inject_followup_message(sess, action="user_message", message="continue")

    provider = _ScriptedProvider([_Step(tool_calls=[], content="ack", finish_reason="stop")])
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]

    # Drift must NOT fire — the preamble re-snapshotted to include node 2.
    assert "drift_detected" not in names
    assert "complete" in names

    # Snapshot now contains node 2 — proof the preamble took a fresh capture.
    assert 2 in sess.snapshot.node_ids


@pytest.mark.asyncio
async def test_w49_followup_after_question_re_enters_with_user_message() -> None:
    """Mode 2 — session in ``awaiting_user_input``, route injects user
    message, planner re-enters and produces tool calls."""
    from flowfile_core.ai.agents.planner import inject_followup_message

    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    sess.status = "awaiting_user_input"
    sess.last_assistant_text = "Which column?"
    sess.messages = [
        Message(role="system", content="sys"),
        Message(role="user", content="filter to EU"),
        Message(role="assistant", content="Which column?"),
    ]

    inject_followup_message(sess, action="user_message", message="filter on region")

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    names = [e.event for e in events]
    assert "tool_call_staged" in names
    assert sess.status == "completed"


@pytest.mark.asyncio
async def test_w49_followup_synthetic_rejection_appears_in_conversation() -> None:
    """Acceptance test 1 — the synthetic rejection user-message is in the
    planner conversation history when the resumed loop calls the provider."""
    from flowfile_core.ai.agents.planner import inject_followup_message

    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    sess.status = "completed"
    sess.diff_id = "rejected-diff"
    sess.messages = [
        Message(role="system", content="sys"),
        Message(role="user", content="goal"),
    ]

    inject_followup_message(
        sess,
        action="rejected_diff",
        message="please use the read node directly",
        rejected_diff_id="rejected-diff",
    )

    provider = _ScriptedProvider([_Step(tool_calls=[], content="ok", finish_reason="stop")])
    await _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))

    # The provider received messages including the synthetic rejection user
    # message. Confirm both the rejection sigil and the user note arrived.
    assert provider.calls
    sent_messages = provider.calls[0]["messages"]
    rejection_msgs = [m for m in sent_messages if m.role == "user" and "rejected" in (m.content or "").lower()]
    assert rejection_msgs, "synthetic rejection user message missing from conversation"
    assert "please use the read node directly" in rejection_msgs[-1].content


# --------------------------------------------------------------------------- #
# modification dispatch #
# --------------------------------------------------------------------------- #


def _make_flow_with_filter(flow_id: int = 1) -> FlowGraph:
    """Mirrors ``_make_flow`` but adds a filter at id=2 with explicit
    upstream wiring so ``update_node_settings`` has a live target.
    """
    from flowfile_core.flowfile.flow_graph import add_connection
    from flowfile_core.schemas import transform_schema

    flow = _make_flow(flow_id=flow_id)
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(
                filter_type="advanced", advanced_filter="[region]=='EU'"
            ),
        )
    )
    add_connection(
        flow,
        input_schema.NodeConnection.create_from_simple_input(
            from_id=1, to_id=2, input_type="input-0", output_handle="output-0"
        ),
    )
    flow.get_node(2).get_predicted_schema()
    return flow


@pytest.mark.asyncio
async def test_planner_stages_modification_with_existing_node_target() -> None:
    """planner dispatches a single ``update_node_settings`` call,
    stages the modification, and the bundled diff has 1 modification, 0
    additions. ``staged_node_ids`` is unchanged because modifications
    target *existing* node ids (W45 drift-detection invariant).
    """
    from flowfile_core.schemas import transform_schema

    flow = _make_flow_with_filter()
    sess = _make_session(flow, surface="agent_complex")

    new_settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            filter_type="advanced", advanced_filter="[amount] > 50"
        ),
    ).model_dump(mode="json")

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t1",
                        name="flowfile.graph.update_node_settings",
                        arguments={"flow_id": 1, "node_id": 2, "settings": new_settings},
                    )
                ],
                content="tightening the filter",
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], content="done.", finish_reason="stop"),
        ]
    )

    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )

    staged = [e for e in events if e.event == "tool_call_staged"]
    assert len(staged) == 1
    assert staged[0].payload["name"] == "flowfile.graph.update_node_settings"
    assert staged[0].payload["node_id"] == 2

    # drift-detection invariant — modifications target existing ids,
    # so the planner must NOT append to staged_node_ids.
    assert sess.staged_node_ids == []

    # The bundled diff has the modification + zero additions.
    diff = diff_module.get_diff(sess.diff_id)
    assert diff is not None
    assert len(diff.modifications) == 1
    assert len(diff.additions) == 0
    mod = diff.modifications[0]
    assert mod.node_id == 2
    assert mod.node_type == "filter"
    assert mod.new_settings["filter_input"]["advanced_filter"] == "[amount] > 50"
    # Old settings reflect the pre-change state.
    assert mod.old_settings["filter_input"]["advanced_filter"] == "[region]=='EU'"

    # Live graph wasn't mutated — staging only.
    assert flow.get_node(2).setting_input.filter_input.advanced_filter == "[region]=='EU'"


@pytest.mark.asyncio
async def test_planner_modification_does_not_pollute_staged_node_ids_when_chained() -> None:
    """back-to-back modifications on the same flow keep
    ``staged_node_ids`` empty. Defends the drift invariant against
    accidental drift from modification-heavy sessions.
    """
    from flowfile_core.schemas import transform_schema
    from flowfile_core.flowfile.flow_graph import add_connection

    flow = _make_flow_with_filter()
    # Add a second filter at id=3 so we have two modification targets.
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=3,
            depending_on_id=2,
            filter_input=transform_schema.FilterInput(
                filter_type="advanced", advanced_filter="[order_id] > 0"
            ),
        )
    )
    add_connection(
        flow,
        input_schema.NodeConnection.create_from_simple_input(
            from_id=2, to_id=3, input_type="input-0", output_handle="output-0"
        ),
    )
    sess = _make_session(flow, surface="agent_complex")

    def _new_filter_dump(node_id: int, dep_id: int, expr: str) -> dict[str, Any]:
        return input_schema.NodeFilter(
            flow_id=1,
            node_id=node_id,
            depending_on_id=dep_id,
            filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter=expr),
        ).model_dump(mode="json")

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="m1",
                        name="flowfile.graph.update_node_settings",
                        arguments={
                            "flow_id": 1,
                            "node_id": 2,
                            "settings": _new_filter_dump(2, 1, "[amount] > 50"),
                        },
                    )
                ],
                content="filter on amount",
                finish_reason="tool_calls",
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="m2",
                        name="flowfile.graph.update_node_settings",
                        arguments={
                            "flow_id": 1,
                            "node_id": 3,
                            "settings": _new_filter_dump(3, 2, "[amount] > 100"),
                        },
                    )
                ],
                content="and the downstream one too",
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], content="done.", finish_reason="stop"),
        ]
    )

    await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    assert sess.staged_node_ids == []
    diff = diff_module.get_diff(sess.diff_id)
    assert diff is not None
    assert len(diff.modifications) == 2
    assert {m.node_id for m in diff.modifications} == {2, 3}


# --------------------------------------------------------------------------- #
# staged settings echo in tool reply #
# --------------------------------------------------------------------------- #


def test_summarise_echoes_staged_settings_for_add() -> None:
    """The LLM tool reply for ``add_*`` includes the staged settings JSON
    so a follow-up ``update_node_settings`` call can copy current values
    rather than hallucinate them. Without this, the LLM had only
    ``predicted columns: ...`` to work from and invented fields like
    ``pos_x`` / ``user_id`` / ``depending_on_id`` (live trace 13:56).
    """
    result = ToolExecutionResult(
        status="staged",
        tool_name="flowfile.graph.add_group_by",
        predicted_output_schema=[{"name": "city"}, {"name": "customer_count"}],
        staged_node_payload={
            "node_type": "group_by",
            "settings": {
                "node_id": 5,
                "flow_id": 2,
                "groupby_input": {"agg_cols": [{"old_name": "city", "agg": "groupby"}]},
            },
            "predicted_output_schema": [{"name": "city"}, {"name": "customer_count"}],
        },
    )
    summary = _summarise_result_for_llm(result)
    assert "predicted columns: city, customer_count" in summary
    assert "settings: " in summary
    assert '"node_id":5' in summary
    assert '"groupby_input"' in summary


def test_summarise_echoes_settings_for_update_node_settings() -> None:
    """The same echo applies to modification payloads (settings under
    ``new_settings`` rather than ``settings``)."""
    result = ToolExecutionResult(
        status="staged",
        tool_name="flowfile.graph.update_node_settings",
        staged_node_payload={
            "kind": "modification",
            "node_id": 5,
            "node_type": "group_by",
            "old_settings": {"node_id": 5, "groupby_input": {"agg_cols": []}},
            "new_settings": {"node_id": 5, "groupby_input": {"agg_cols": [{"old_name": "city", "agg": "groupby"}]}},
        },
    )
    summary = _summarise_result_for_llm(result)
    assert "settings: " in summary
    assert '"groupby_input"' in summary
    # Old settings are NOT echoed — only the new state is what subsequent
    # tool calls should anchor on.
    assert summary.count("settings: ") == 1


def test_summarise_omits_settings_for_connect_payload() -> None:
    """Connect payloads have no settings dict — the echo must not fire."""
    result = ToolExecutionResult(
        status="staged",
        tool_name="flowfile.graph.connect",
        staged_node_payload={"connection": {"input_connection": {}, "output_connection": {}}},
    )
    summary = _summarise_result_for_llm(result)
    assert "settings: " not in summary


def test_summarise_omits_settings_on_rejected_status() -> None:
    """Rejection paths shouldn't echo phantom settings (the staged_node_payload
    on a rejection — if any — represents an unstaged attempt)."""
    result = ToolExecutionResult(
        status="rejected",
        tool_name="flowfile.graph.add_filter",
        refusal_reason="settings_validation",
        refusal_detail="bad shape",
        staged_node_payload={"settings": {"node_id": 99}},
    )
    summary = _summarise_result_for_llm(result)
    assert "settings: " not in summary


def test_summarise_truncates_oversized_settings() -> None:
    """Pathologically large settings get truncated so a single staging
    can't blow the context budget."""
    huge_settings = {"node_id": 5, "blob": "x" * 5000}
    result = ToolExecutionResult(
        status="staged",
        tool_name="flowfile.graph.add_polars_code",
        staged_node_payload={"settings": huge_settings},
    )
    summary = _summarise_result_for_llm(result)
    assert "settings: " in summary
    assert "[truncated]" in summary
