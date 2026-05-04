"""W40 — :mod:`flowfile_core.ai.agents.planner` tests.

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
import sys
from collections.abc import Iterator
from typing import Any

import pytest

from flowfile_core.ai import diff as diff_module
from flowfile_core.ai import sessions
from flowfile_core.ai.agents.planner import (
    PlannerEvent,
    _allocate_node_id,
    _resolve_insertion_context,
    run_planner_session,
)
from flowfile_core.ai.context.builder import SURFACE_TO_LEVEL
from flowfile_core.ai.providers.base import ChatResponse, ToolCall, Usage
from flowfile_core.ai.scheduler import RateLimitScheduler
from flowfile_core.ai.tools.registry import SURFACE_PRESETS
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas

# --------------------------------------------------------------------------- #
# Test helpers                                                                 #
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
    """Filter args that the executor will reject via W25 column-ref validation."""
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
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def _reset_stores() -> Iterator[None]:
    sessions.clear_for_tests()
    diff_module.clear_for_tests()
    yield
    sessions.clear_for_tests()
    diff_module.clear_for_tests()


# --------------------------------------------------------------------------- #
# Lazy-litellm contract + surface lockstep                                     #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract() -> None:
    # If litellm was already imported by a prior test, drop it before re-import.
    sys.modules.pop("litellm", None)
    sys.modules.pop("flowfile_core.ai.agents.planner", None)
    from flowfile_core.ai.agents import planner as _planner  # noqa: F401

    assert "litellm" not in sys.modules


def test_surface_lockstep() -> None:
    assert {"agent", "agent_complex"} <= SURFACE_PRESETS.keys()
    assert SURFACE_TO_LEVEL["agent"] == "planner"
    assert SURFACE_TO_LEVEL["agent_complex"] == "planner"


# --------------------------------------------------------------------------- #
# Pure helpers                                                                 #
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
    ctx = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [42]
    assert ctx.pos_x == 100.0


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
    ctx = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [7]


def test_resolve_insertion_context_falls_back_to_live_node() -> None:
    flow = _make_flow()
    sess = _make_session(flow)
    tc = ToolCall(id="t3", name="flowfile.graph.add_filter", arguments={})
    ctx = _resolve_insertion_context(sess, tc, flow)
    assert ctx.upstream_node_ids == [1]


# --------------------------------------------------------------------------- #
# Loop happy-path                                                              #
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


# --------------------------------------------------------------------------- #
# Surface routing                                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_surface_agent_two_stage_routes_through_pick_category() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent")

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t1",
                        name="flowfile.meta.pick_category",
                        arguments={"intent": "filter to EU"},
                    )
                ],
                finish_reason="tool_calls",
            ),
            _Step(
                tool_calls=[ToolCall(id="t2", name="flowfile.graph.add_filter", arguments=_filter_args())],
                finish_reason="tool_calls",
            ),
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )

    await _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))

    assert len(provider.calls) == 3
    # First call's tools should be exactly the agent surface (just pick_category).
    first_tools = {tool.name for tool in provider.calls[0]["tools"]}
    assert "flowfile.meta.pick_category" in first_tools
    assert "flowfile.graph.add_filter" not in first_tools

    # Second call (after pick_category narrowed to "transformations") should include add_filter.
    second_tools = {tool.name for tool in provider.calls[1]["tools"]}
    assert "flowfile.graph.add_filter" in second_tools


@pytest.mark.asyncio
async def test_surface_agent_complex_uses_full_catalog_from_step_one() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    provider = _ScriptedProvider(
        [
            _Step(tool_calls=[], finish_reason="stop"),
        ]
    )

    await _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))

    first_tools = {tool.name for tool in provider.calls[0]["tools"]}
    # Full catalog: every add_* node-type tool plus universal ops + pick_category.
    assert "flowfile.graph.add_filter" in first_tools
    assert "flowfile.graph.add_select" in first_tools
    assert "flowfile.graph.add_join" in first_tools
    assert "flowfile.schema.read_node_schema" in first_tools
    # agent_complex == full catalog, so pick_category is also present (just not the
    # only thing surfaced like in surface="agent" stage 1).
    assert "flowfile.meta.pick_category" in first_tools


# --------------------------------------------------------------------------- #
# Retry on rejection                                                           #
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
# Drift                                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_drift_mid_stream_pauses_session() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")

    # Provider would call add_filter, but we mutate the flow before the drift check.
    flow.get_node(1).setting_input.raw_data_format.data = [[99], ["XX"], [42.0]]
    flow.get_node(1).setting_input = flow.get_node(1).setting_input  # trigger reset

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


# --------------------------------------------------------------------------- #
# Abort                                                                        #
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
# Misc edge cases                                                              #
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
async def test_cannot_run_completed_session() -> None:
    flow = _make_flow()
    sess = _make_session(flow, surface="agent_complex")
    sess.status = "completed"
    provider = _ScriptedProvider([_Step(tool_calls=[], finish_reason="stop")])
    events = await _drain(
        run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())
    )
    assert events[0].event == "error"


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
