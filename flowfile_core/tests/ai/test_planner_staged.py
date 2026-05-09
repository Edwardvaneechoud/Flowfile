"""W71 — agent_staged multi-stage state machine tests.

Each stage exposes exactly one tool to the function-calling API:

* ``classify`` — ``flowfile.meta.classify_intent(op_kind)``
* ``pick_type`` — ``flowfile.meta.pick_node_type(node_type)``
* ``pick_upstream`` — ``flowfile.meta.pick_upstream(upstream_node_ids[],
  right_input_node_id?)``  (per-turn dynamic enum)
* ``fill_settings`` — the picked node type's
  ``flowfile.graph.add_<type>`` tool with planner-injected fields
  stripped (per-turn dynamic spec)
* ``single_stage_op`` — one of update_node_settings / delete_node /
  connect / delete_connection per ``picked_op_kind``

Reuses the ``_ScriptedProvider`` fixture pattern from
``test_planner.py`` so the assertions check what the planner actually
emits to / consumes from the LLM, not just internal state mutations.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from typing import Any

import pytest

from flowfile_core.ai import diff as diff_module
from flowfile_core.ai import sessions
from flowfile_core.ai.agents.planner import (
    PlannerEvent,
    run_planner_session,
)
from flowfile_core.ai.providers.base import ChatResponse, ToolCall, Usage
from flowfile_core.ai.scheduler import RateLimitScheduler
from flowfile_core.ai.tools.meta_ops import (
    CLASSIFY_INTENT_TOOL_NAME,
    PICK_NODE_TYPE_TOOL_NAME,
    PICK_UPSTREAM_TOOL_NAME,
    VERIFY_COMPLETION_TOOL_NAME,
)
from flowfile_core.ai.tools.registry import (
    build_staged_fill_tool_spec,
    get_staged_fill_inner_field_name,
)
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas


# --------------------------------------------------------------------------- #
# Test helpers (mirrored from test_planner.py to keep the fixture stack
# self-contained — these tests use a different surface and a different
# scripted-provider story, so a shared helper module would be over-engineering.)
# --------------------------------------------------------------------------- #


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_w71_planner_staged",
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
    flow = FlowGraph(flow_settings=_flow_settings(flow_id), name="w71_planner_staged_test")
    _add_orders(flow)
    return flow


def _make_session(flow: FlowGraph, *, user_id: int = 1) -> sessions.AgentSession:
    snapshot = sessions.capture_graph_snapshot(flow)
    sess = sessions.AgentSession(
        flow_id=flow.flow_id,
        user_id=user_id,
        user_prompt="filter to EU rows",
        surface="agent_staged",
        provider_name="fake",
        snapshot=snapshot,
        max_steps=16,
    )
    # W71 v2.4 — session default is now ``stage="plan"`` for the
    # multi-stage state machine, but most tests script from
    # ``classify`` onward. Skip the plan stage by default so existing
    # tests don't need to prepend an emit_plan step. v2.4-specific
    # tests opt back into ``stage="plan"`` explicitly.
    sess.stage = "classify"
    return sess


class _Step:
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
    """Returns one ``_Step`` per call in declaration order. Records every
    call's ``tools`` list so tests can assert what the LLM saw at each stage."""

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

    def stream(self, *_a, **_k):  # pragma: no cover
        raise AssertionError("planner must use chat(), not stream()")


def _no_wait_scheduler() -> RateLimitScheduler:
    return RateLimitScheduler(time_source=lambda: 0.0, sleep=lambda *_a, **_k: asyncio.sleep(0))


async def _drain(gen) -> list[PlannerEvent]:
    out: list[PlannerEvent] = []
    async for ev in gen:
        out.append(ev)
    return out


@pytest.fixture(autouse=True)
def _reset_stores() -> Iterator[None]:
    sessions.clear_for_tests()
    diff_module.clear_for_tests()
    yield
    sessions.clear_for_tests()
    diff_module.clear_for_tests()


# --------------------------------------------------------------------------- #
# Stage 0 — classify_intent                                                    #
# --------------------------------------------------------------------------- #


def test_stage_classify_exposes_only_classify_intent_tool() -> None:
    """Stage 0 surfaces exactly one tool: ``classify_intent``. The
    function-calling-API compliance fix relies on the tools array being
    1-element so smaller models invoke it correctly."""

    flow = _make_flow()
    sess = _make_session(flow)
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-classify-1",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "other", "rationale": "user is asking a question"},
                    )
                ]
            ),
            # Second round: LLM has nothing more to do; loop exits as
            # ``complete`` (or ``awaiting_user_input`` if rationale ends ?).
            _Step(content="Here is my answer to your question.", finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # Round 1 (classify) saw exactly one tool: classify_intent.
    round1_tools = provider.calls[0]["tools"]
    assert len(round1_tools) == 1, f"expected 1 tool at classify, got {len(round1_tools)}"
    assert round1_tools[0].name == CLASSIFY_INTENT_TOOL_NAME

    assert sess.picked_op_kind == "other"
    assert sess.stage == "classify", "stage stays at classify after op_kind='other'"


def test_stage_classify_op_kind_add_advances_to_pick_type() -> None:
    flow = _make_flow()
    sess = _make_session(flow)
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-classify-1",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "add", "rationale": "user wants to filter"},
                    )
                ]
            ),
            # Round 2: at pick_type the LLM has nothing to do here (test only
            # the classify→pick_type transition); emit no tool call to exit.
            _Step(content=None, finish_reason="stop"),
        ]
    )

    events = asyncio.run(
        _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))
    )

    assert sess.picked_op_kind == "add"
    assert sess.stage == "pick_type"
    # Emitted at least one stage_advanced event.
    advances = [e for e in events if e.event == "stage_advanced"]
    assert any(
        e.payload.get("from") == "classify" and e.payload.get("to") == "pick_type" for e in advances
    ), f"expected classify→pick_type stage_advanced; got {[(e.payload.get('from'), e.payload.get('to')) for e in advances]}"


@pytest.mark.parametrize(
    "op_kind, expected_surface_tool",
    [
        ("modify", "flowfile.graph.update_node_settings"),
        ("delete", "flowfile.graph.delete_node"),
        ("connect", "flowfile.graph.connect"),
        ("disconnect", "flowfile.graph.delete_connection"),
    ],
)
def test_stage_classify_routes_non_add_to_single_stage_op(
    op_kind: str, expected_surface_tool: str
) -> None:
    """Each non-add op_kind transitions stage→single_stage_op and the
    next round exposes exactly the matching ops tool."""
    flow = _make_flow()
    sess = _make_session(flow)
    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-classify-1",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": op_kind, "rationale": "test"},
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    assert sess.picked_op_kind == op_kind
    assert sess.stage == "single_stage_op"

    # Round 2 saw exactly one tool: the matching ops tool.
    round2_tools = provider.calls[1]["tools"]
    assert len(round2_tools) == 1
    assert round2_tools[0].name == expected_surface_tool


# --------------------------------------------------------------------------- #
# Stage 1 — pick_node_type                                                     #
# --------------------------------------------------------------------------- #


def test_stage_pick_type_advances_to_pick_upstream() -> None:
    flow = _make_flow()
    sess = _make_session(flow)
    provider = _ScriptedProvider(
        [
            # Round 1: classify add
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t1",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "add", "rationale": "filter"},
                    )
                ]
            ),
            # Round 2: pick filter
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t2",
                        name=PICK_NODE_TYPE_TOOL_NAME,
                        arguments={"node_type": "filter", "rationale": "row predicate"},
                    )
                ]
            ),
            # Round 3: terminate
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    assert sess.picked_node_type == "filter"
    assert sess.stage == "pick_upstream"

    # Round 2 saw exactly one tool: pick_node_type, with all 40 types in the enum.
    round2_tools = provider.calls[1]["tools"]
    assert len(round2_tools) == 1
    assert round2_tools[0].name == PICK_NODE_TYPE_TOOL_NAME
    enum_values = round2_tools[0].parameters["properties"]["node_type"]["enum"]
    assert "filter" in enum_values
    assert "group_by" in enum_values
    assert "join" in enum_values
    assert len(enum_values) >= 30, f"expected at least 30 node types in enum, got {len(enum_values)}"


# --------------------------------------------------------------------------- #
# Stage 2 — pick_upstream                                                      #
# --------------------------------------------------------------------------- #


def test_stage_pick_upstream_enum_includes_live_and_staged_ids() -> None:
    """The upstream picker is built per-turn so chained adds (filter →
    sort within one user turn) can pick the prior staged add as upstream
    even though it isn't in ``flow.nodes`` yet."""
    flow = _make_flow()
    sess = _make_session(flow)
    # Pretend the agent has already staged a node id 5 in this session;
    # the next round's pick_upstream enum should include it alongside
    # the live ids (just node 1 in our test flow).
    sess.staged_node_ids = [5]
    sess.stage = "pick_upstream"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "filter"

    provider = _ScriptedProvider(
        [
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    round1_tools = provider.calls[0]["tools"]
    assert len(round1_tools) == 1
    assert round1_tools[0].name == PICK_UPSTREAM_TOOL_NAME
    items_enum = round1_tools[0].parameters["properties"]["upstream_node_ids"]["items"]["enum"]
    assert 1 in items_enum, "live node id missing from pick_upstream enum"
    assert 5 in items_enum, "staged node id missing from pick_upstream enum"


def test_stage_pick_upstream_advances_to_fill_settings() -> None:
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "pick_upstream"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "filter"

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-up-1",
                        name=PICK_UPSTREAM_TOOL_NAME,
                        arguments={
                            "upstream_node_ids": [1],
                            "right_input_node_id": None,
                            "rationale": "the only data node",
                        },
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    assert sess.picked_upstream_ids == [1]
    assert sess.picked_right_input_id is None
    assert sess.stage == "fill_settings"


# --------------------------------------------------------------------------- #
# Stage 3 — fill_settings (stripped tool spec)                                 #
# --------------------------------------------------------------------------- #


def test_build_staged_fill_tool_spec_strips_planner_injected_fields() -> None:
    """The fill_settings tool schema removes planner-injected fields so
    the LLM only sees the type-specific settings shape."""
    spec = build_staged_fill_tool_spec("filter")
    assert spec is not None
    props = spec.parameters.get("properties", {})
    for stripped in ("flow_id", "node_id", "depending_on_id", "depending_on_ids"):
        assert stripped not in props, f"field {stripped!r} should be stripped from fill_settings tool"
    # The settings field that's actually load-bearing for filter MUST stay.
    assert "filter_input" in props


def test_build_staged_fill_tool_spec_unknown_node_type() -> None:
    """Returns ``None`` for unknown node types so the planner can refuse
    cleanly rather than dispatching against a missing settings class."""
    assert build_staged_fill_tool_spec("not_a_real_node_type") is None


def test_build_staged_fill_tool_spec_uses_inner_for_group_by() -> None:
    """W71 v1.2 — single-input node types expose the inner-input class
    directly, so the LLM sees ``agg_cols`` at top level without the
    ``groupby_input`` envelope or NodeBase metadata noise."""
    spec = build_staged_fill_tool_spec("group_by")
    assert spec is not None
    props = spec.parameters.get("properties", {})
    assert "agg_cols" in props, "group_by should expose inner GroupByInput shape"
    # No envelope, no NodeBase noise.
    for absent in (
        "groupby_input",
        "flow_id",
        "node_id",
        "output_field_config",
        "pos_x",
        "pos_y",
        "cache_results",
        "depending_on_id",
    ):
        assert absent not in props, f"{absent!r} should not appear at the top level"
    assert get_staged_fill_inner_field_name("group_by") == "groupby_input"


def test_build_staged_fill_tool_spec_falls_back_to_flat_for_join() -> None:
    """W71 v1.2 — multi-field node types (join has join_input plus
    auto_keep_left/right and friends) cannot be flattened to a single
    inner class, so the spec stays at the wrapper level with planner-
    injected fields and NodeBase noise stripped. The real type-specific
    knobs MUST still appear so the LLM can configure the join."""
    spec = build_staged_fill_tool_spec("join")
    assert spec is not None
    props = spec.parameters.get("properties", {})
    # The inner envelope and the auto_keep_* knobs are real configuration
    # surfaces — they must be visible.
    assert "join_input" in props
    assert "auto_keep_left" in props
    assert "auto_keep_right" in props
    # Planner-injected fields and NodeBase noise are still stripped.
    for absent in (
        "flow_id",
        "node_id",
        "output_field_config",
        "pos_x",
        "pos_y",
        "cache_results",
        "depending_on_ids",
    ):
        assert absent not in props, f"{absent!r} should be stripped on the flat-fallback path"
    assert get_staged_fill_inner_field_name("join") is None


@pytest.mark.parametrize(
    "raw_input, expected",
    [
        ([4], [4]),  # already correct
        ([4, 5], [4, 5]),  # multiple ids, correct shape
        (4, [4]),  # bare int → wrap
        ("4", [4]),  # JSON-encoded scalar string
        ("[4]", [4]),  # JSON-encoded array string
        ("[4, 5]", [4, 5]),  # JSON array of multiple
        ("4, 5", [4, 5]),  # CSV string
        ("4,5", [4, 5]),  # CSV string no spaces
    ],
)
def test_pick_upstream_coerces_common_llama_misshapes(
    raw_input: Any, expected: list[int]
) -> None:
    """W71 v1.3 — llama-3.3-70b emits ``upstream_node_ids`` as scalars,
    JSON-encoded strings, or CSV strings depending on the prompt. Each
    of these now gets recovered into ``list[int]`` rather than rejected,
    saving the retry budget for genuine semantic mistakes (wrong id
    chosen) instead of trivial type wrapping."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "pick_upstream"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "filter"

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-up-coerce",
                        name=PICK_UPSTREAM_TOOL_NAME,
                        arguments={
                            "upstream_node_ids": raw_input,
                            "right_input_node_id": None,
                            "rationale": "test coercion",
                        },
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    assert sess.picked_upstream_ids == expected, (
        f"raw {raw_input!r} should have coerced to {expected}, "
        f"got {sess.picked_upstream_ids}"
    )


def test_fill_settings_user_message_drops_full_subgraph() -> None:
    """W71 v1.5 — at the fill_settings stage the planner replaces the
    user message with a focused mini-prompt that contains only the
    user's goal and the picked upstream's column schema. The 4.5k-char
    full subgraph that rides into stage 1 / stage 2 is dropped — it
    just confuses smaller models when the upstream is already chosen."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "fill_settings"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "filter"
    sess.picked_upstream_ids = [1]

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-fill-slim",
                        name="flowfile.graph.add_filter",
                        arguments={
                            "filter_input": {
                                "filter_type": "advanced",
                                "advanced_filter": "[region]=='EU'",
                            }
                        },
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # Inspect the user message the planner sent on the fill_settings
    # round (provider.calls[0]). It MUST contain the goal + the picked
    # upstream's columns, and NOT the other nodes' settings dicts /
    # the full subgraph header.
    user_msg = provider.calls[0]["messages"][1]
    body = user_msg.content or ""
    # Goal text from session.user_prompt is preserved.
    assert "filter to EU rows" in body, body
    # Picked upstream (node 1, manual_input) columns are present.
    for col in ("order_id", "region", "amount"):
        assert col in body, f"expected upstream column {col!r} in slim user message; got: {body}"
    # Markers from the full subgraph rendering are absent.
    assert "## Subgraph" not in body, "stage 3 should drop the full subgraph"
    # The slim user message is markedly smaller than the full subgraph
    # the planner ships at earlier stages — set a generous ceiling so
    # the test doesn't break on tiny copy tweaks but still catches a
    # regression that re-bloats the prompt.
    assert len(body) < 1500, f"slim user message bloated to {len(body)} chars"


def test_text_json_tool_call_recovery_at_fill_settings() -> None:
    """W71 v1.6 — llama-3.3-70b occasionally emits the function call as
    text content rather than via the function-calling API at stage 3.
    The recovery path parses the call out of content, synthesizes a
    real ``ToolCall``, and the dispatch proceeds normally — saving the
    user from a silent termination with no diff to accept.

    Reproduces the 2026-05-08 PM dogfood symptom: at fill_settings the
    LLM emits *"Sorting by the number of customers per city
    flowfile.graph.add_sort({...})"* with no real tool_calls, and the
    chat trail shows no *"Staged sort"* line. After v1.6, the call is
    recovered and stages cleanly."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "fill_settings"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "sort"
    sess.picked_upstream_ids = [1]

    # The LLM emits NO tool_calls; instead the call invocation rides on
    # the assistant's prose content. v1.6 picks it up.
    text_content = (
        "Sorting by the number of customers per city\n"
        'flowfile.graph.add_sort({"sort_input": [{"column": "amount", "how": "desc"}]})'
    )
    provider = _ScriptedProvider(
        [
            _Step(content=text_content, finish_reason="stop"),
            # After successful (recovered) add, the planner resets to
            # classify and asks again; emit no tool call to exit.
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # The recovery path successfully staged the sort.
    assert len(sess.staged_results) == 1, (
        f"recovery should have produced one staged op; got "
        f"{[r.tool_name for r in sess.staged_results]}"
    )
    assert sess.staged_results[0].tool_name == "flowfile.graph.add_sort"
    payload = sess.staged_results[0].staged_node_payload or {}
    settings = payload.get("settings") or {}
    sort_input = settings.get("sort_input")
    assert isinstance(sort_input, list) and len(sort_input) == 1
    assert sort_input[0].get("column") == "amount"
    assert sort_input[0].get("how") == "desc"


def test_text_json_recovery_handles_json_object_shape_at_classify() -> None:
    """W71 v1.8 — llama-3.3-8b emits the function call as a JSON object
    in its content (the OpenAI envelope: ``{"name": ..., "parameters":
    ...}``) even at the simplest classify stage. Recovery now applies
    at all agent_staged stages and parses the JSON-object shape so the
    8b run advances instead of terminating immediately."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "classify"

    text_content = (
        '{"name": "flowfile.meta.classify_intent", "parameters": '
        '{"op_kind": "add", "rationale": "Implement the suggested nodes."}}'
    )
    provider = _ScriptedProvider(
        [
            _Step(content=text_content, finish_reason="stop"),
            # After classify advances, exit cleanly at pick_type.
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # classify_intent dispatched and advanced the stage to pick_type.
    assert sess.picked_op_kind == "add"
    assert sess.stage == "pick_type"


def test_text_json_recovery_handles_arguments_alias() -> None:
    """W71 v1.8 — some models prefer ``"arguments"`` (OpenAI's older key
    name) over ``"parameters"``. Recovery accepts both."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "classify"

    text_content = (
        '{"name": "flowfile.meta.classify_intent", "arguments": '
        '{"op_kind": "add", "rationale": "Yes, please add it."}}'
    )
    provider = _ScriptedProvider(
        [
            _Step(content=text_content, finish_reason="stop"),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))
    assert sess.picked_op_kind == "add"


def test_text_json_recovery_declines_when_name_not_in_expected_set() -> None:
    """W71 v1.8 — the name must be in the expected catalog. If a model
    emits ``add_group_by`` while the catalog only exposes
    ``classify_intent`` (i.e. wrong stage), recovery declines rather
    than misfiring."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "classify"

    # The catalog at classify exposes ONLY classify_intent. A
    # JSON-object call for add_group_by should not be recovered here.
    text_content = (
        '{"name": "flowfile.graph.add_group_by", "parameters": '
        '{"agg_cols": [{"old_name": "city", "agg": "groupby"}]}}'
    )
    provider = _ScriptedProvider(
        [
            _Step(content=text_content, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # Nothing got staged — recovery correctly declined.
    assert sess.staged_results == []
    # Stage is unchanged (LLM didn't classify; loop terminated).
    assert sess.picked_op_kind is None


def test_silent_termination_routes_to_retry_at_fill_settings() -> None:
    """W71 v1.7 — at fill_settings the LLM emits prose with no
    tool_call AND no parseable text-JSON shape (the llama-70b
    'altimoreFiltering...' token-corruption case). Instead of the
    legacy 'Agent finished — nothing to stage' termination, the loop
    appends a synthetic 'you must call the tool' reminder and routes
    through the retry budget."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "fill_settings"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "filter"
    sess.picked_upstream_ids = [1]

    provider = _ScriptedProvider(
        [
            # Round 1: garbled prose, no tool_call.
            _Step(content="altimoreFiltering to rows from EU only", finish_reason="stop"),
            # Round 2 (after retry reminder): emit a real call.
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-fill-retry",
                        name="flowfile.graph.add_filter",
                        arguments={
                            "filter_input": {
                                "filter_type": "advanced",
                                "advanced_filter": "[region]=='EU'",
                            }
                        },
                    )
                ]
            ),
            # Round 3: classify after reset; nothing more.
            _Step(content=None, finish_reason="stop"),
        ]
    )

    events = asyncio.run(
        _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))
    )

    # The retry path fired between round 1 and round 2.
    retry_events = [e for e in events if e.event == "retry"]
    assert len(retry_events) >= 1, "expected a retry event after silent stage-3 termination"

    # Round 2's real tool call staged the filter.
    assert len(sess.staged_results) == 1
    assert sess.staged_results[0].tool_name == "flowfile.graph.add_filter"


def test_silent_termination_max_retries_exhausted_fails() -> None:
    """W71 v1.7 — three consecutive empty-tool-call rounds at stage 3
    exhaust the retry budget and fail the run with a clear error."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "fill_settings"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "filter"
    sess.picked_upstream_ids = [1]

    provider = _ScriptedProvider(
        [
            _Step(content="garbage 1", finish_reason="stop"),
            _Step(content="garbage 2", finish_reason="stop"),
            _Step(content="garbage 3", finish_reason="stop"),
        ]
    )

    events = asyncio.run(
        _drain(
            run_planner_session(
                session=sess,
                flow=flow,
                provider=provider,
                scheduler=_no_wait_scheduler(),
                max_retries_per_step=3,
            )
        )
    )

    error_events = [e for e in events if e.event == "error"]
    assert error_events, "expected an error event after retries exhausted"
    assert sess.status == "failed"
    assert sess.staged_results == []


def test_classify_silent_termination_still_terminates() -> None:
    """W71 v1.7 — preserve the existing termination semantics at the
    classify stage. The LLM emitting prose-only at classify is a
    valid 'I'm done responding' signal (e.g. op_kind='other' was
    intended). It must NOT loop into retry hell."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "classify"

    provider = _ScriptedProvider(
        [
            _Step(content="Here is my answer about the flow.", finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # Loop terminated cleanly; no retry, no failure, no staged ops.
    assert sess.staged_results == []
    assert sess.status in ("completed", "awaiting_user_input")


def test_text_json_tool_call_recovery_skipped_at_classify() -> None:
    """W71 v1.6 — recovery does NOT fire at the classify stage. After a
    successful add, the LLM is reset to classify and sometimes emits a
    "summary" of past calls in its content. Recovering those would
    re-execute already-staged ops. The classify-stage summary should
    end the loop cleanly with the prose surfaced as a thinking event."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "classify"

    # Summary-style content that mentions multiple flowfile.graph.add_*
    # tool names — the kind of prose the LLM emits AFTER it just ran a
    # successful chain. A naive recovery would fire on each name and
    # double-stage; the gating by stage + the "exactly one tool match"
    # rule prevents it.
    summary_content = (
        "I performed the following calls:\n"
        'flowfile.meta.classify_intent({"op_kind": "add", "rationale": "x"})\n'
        'flowfile.graph.add_sort({"sort_input": [{"column": "amount", "how": "desc"}]})\n'
    )
    provider = _ScriptedProvider(
        [
            _Step(content=summary_content, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # Nothing got staged — the loop exited as a no-op.
    assert sess.staged_results == [], (
        f"classify-stage summary should not be recovered; got staged_results "
        f"{[r.tool_name for r in sess.staged_results]}"
    )


def test_universal_json_string_unwrap_handles_misencoded_add_args() -> None:
    """W71 v1.4 — the executor's universal unwrap pass recovers
    ``add_*`` calls that arrive with structured fields encoded as JSON
    strings. Without the unwrap, llama-3.3-70b's typical mistake of
    passing ``groupby_input: "{\\"agg_cols\\": [...]}"`` rejects with a
    Pydantic ``expected object, got string`` error and burns retry
    budget. Verifies the recovery end-to-end by feeding a scripted
    add_group_by where the inner object IS the JSON-encoded string,
    and asserting the staged settings dict has the parsed structure."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "fill_settings"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "group_by"
    sess.picked_upstream_ids = [1]

    # Fake out the inner-input wrapping: pass an envelope-shape payload
    # where ``groupby_input`` is a JSON-encoded string. The unwrap pass
    # at execute_tool_call's entry should recover the inner object
    # before Pydantic validation runs.
    json_encoded_inner = json.dumps(
        {
            "agg_cols": [
                {"old_name": "region", "agg": "groupby"},
                {"old_name": "amount", "agg": "sum", "new_name": "total"},
            ]
        }
    )

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-fill-jsonstr",
                        name="flowfile.graph.add_group_by",
                        # The LLM emits the inner-input wrapper field as
                        # a JSON-encoded string — a shape v1.2's planner
                        # wrapping would accept (since the wrap happens
                        # before Pydantic), but Pydantic would have
                        # rejected the string. v1.4's universal unwrap
                        # parses it back to the native object first.
                        arguments={"groupby_input": json_encoded_inner},
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    assert len(sess.staged_results) == 1, (
        f"expected 1 staged result; got {[(r.tool_name, r.staged_node_payload) for r in sess.staged_results]}"
    )
    payload = sess.staged_results[0].staged_node_payload or {}
    settings = payload.get("settings") or {}
    inner = settings.get("groupby_input") or {}
    # Inner object was parsed from the JSON-encoded string, not stored as a string.
    assert isinstance(inner, dict)
    assert "agg_cols" in inner
    assert any(c.get("agg") == "sum" for c in inner.get("agg_cols", []))


def test_fill_settings_dispatch_wraps_inner_args_for_group_by() -> None:
    """W71 v1.2 — when the LLM emits inner-shape args for a single-input
    node type, the planner wraps them under the wrapper field name
    before the executor's settings validation runs. End-to-end smoke:
    feed a scripted ``add_group_by({agg_cols: [...]})`` call and assert
    the staged settings dict contains a ``groupby_input.agg_cols``
    structure (i.e. the executor received the wrapped envelope)."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "fill_settings"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "group_by"
    sess.picked_upstream_ids = [1]

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-fill-gb",
                        name="flowfile.graph.add_group_by",
                        # Inner-shape args — no ``groupby_input`` envelope, the
                        # planner adds it.
                        arguments={
                            "agg_cols": [
                                {"old_name": "region", "agg": "groupby"},
                                {
                                    "old_name": "amount",
                                    "agg": "sum",
                                    "new_name": "total",
                                },
                            ],
                        },
                    )
                ]
            ),
            # After successful add, planner resets to classify; emit no
            # tool call to exit.
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    assert len(sess.staged_results) == 1
    payload = sess.staged_results[0].staged_node_payload or {}
    settings = payload.get("settings") or {}
    # The executor received the FULL envelope: wrapper field name + inner
    # shape, plus planner-injected flow_id / node_id.
    assert "groupby_input" in settings, (
        f"expected wrapped settings; got top-level keys {list(settings.keys())}"
    )
    inner = settings.get("groupby_input") or {}
    assert "agg_cols" in inner
    assert any(c.get("agg") == "sum" for c in inner.get("agg_cols", []))


def test_fill_settings_formula_bare_string_coerces_to_function_input() -> None:
    """W71 v1.13B — when the LLM emits the formula tool with only the
    bare expression string at the wrapper key (the most common
    confusion: outer ``function`` parameter holds a FunctionInput
    object, but the LLM puts the inner ``function`` STRING value
    there), the planner auto-coerces the missing ``field`` descriptor
    so the call stages cleanly instead of refusing 3× and failing.
    """
    flow = _make_flow()
    sess = _make_session(flow)
    sess.user_prompt = "add a derived greeting column from name"
    sess.stage = "fill_settings"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "formula"
    sess.picked_upstream_ids = [1]

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-fill-bare-formula",
                        name="flowfile.graph.add_formula",
                        # Bare-string emission: only the expression
                        # under ``function``, no ``field`` object.
                        arguments={"function": "'Hi ' + [region]"},
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    assert len(sess.staged_results) == 1, (
        "v1.13B: bare-string formula must auto-coerce to a valid "
        f"FunctionInput envelope; got {sess.staged_results}"
    )
    payload = sess.staged_results[0].staged_node_payload or {}
    settings = payload.get("settings") or {}
    function_input = settings.get("function") or {}
    assert isinstance(function_input, dict), function_input
    assert function_input.get("function") == "'Hi ' + [region]"
    field = function_input.get("field") or {}
    assert isinstance(field, dict)
    # Best-effort name extraction from "a derived greeting column"
    # → "greeting" (matches the ``a <name> column`` pattern). If the
    # extraction picks up something else, accept the fallback as long
    # as it's a non-empty identifier.
    name = field.get("name")
    assert isinstance(name, str) and name, field


def test_fill_settings_formula_refusal_text_disambiguates_function_field() -> None:
    """W71 v1.13B — when the auto-coerce can't apply (e.g. the LLM
    sent something other than a single-key bare-string shape), the
    refusal text must NOT use the misread-prone *"not as a JSON-
    encoded string"* phrasing. Instead it should name the OUTER
    function (parameter, FunctionInput object) vs INNER function
    (expression string) explicitly.
    """
    from flowfile_core.ai.tools import executor as executor_module
    from flowfile_core.schemas.input_schema import NodeFormula
    from pydantic import ValidationError

    # Reproduce the validation error: NodeFormula.function must be a
    # FunctionInput object, but we feed a bare string. Then call the
    # refusal helper directly and assert its text.
    try:
        NodeFormula.model_validate({
            "flow_id": 1,
            "node_id": 99,
            "function": "[first] + ' ' + [last]",
        })
    except ValidationError as exc:
        text = executor_module._format_settings_validation_refusal(
            exc=exc, settings_cls=NodeFormula, node_type="formula"
        )
    else:
        raise AssertionError("expected ValidationError for bare-string function")

    # The disambiguation block must be present.
    assert "OBJECT with two keys" in text, text
    assert "`field`" in text and "`function`" in text, text
    assert "[column_name]" in text or "[col" in text, text
    # The misread-prone phrasing is gone for this branch.
    assert "JSON-encoded string" not in text, (
        "v1.13B: FunctionInput refusal must drop the ambiguous "
        f"'not as a JSON-encoded string' clause; got: {text!r}"
    )


def test_stage_fill_settings_injects_session_upstream_into_insertion_context() -> None:
    """At fill_settings, the planner pre-resolves InsertionContext from
    session state (picked_upstream_ids, picked_right_input_id) — the LLM
    doesn't see those fields in its stripped schema."""
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "fill_settings"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "filter"
    sess.picked_upstream_ids = [1]

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-fill-1",
                        name="flowfile.graph.add_filter",
                        arguments={
                            "filter_input": {
                                "filter_type": "advanced",
                                "advanced_filter": "[region]=='EU'",
                            }
                        },
                    )
                ]
            ),
            # After stage 3 success, the planner resets to classify and
            # the LLM is asked again; emit no tool call to exit.
            _Step(content=None, finish_reason="stop"),
        ]
    )

    events = asyncio.run(
        _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))
    )

    # The staged add's upstream resolved from session state, not from LLM args.
    assert len(sess.staged_results) == 1
    payload = sess.staged_results[0].staged_node_payload or {}
    insertion = payload.get("insertion_context") or {}
    assert insertion.get("upstream_node_ids") == [1]

    # Stage was reset back to classify after the successful add.
    assert sess.stage == "classify"
    assert sess.picked_node_type is None

    # And the dispatched tool spec at stage 3 was the stripped variant.
    round1_tools = provider.calls[0]["tools"]
    assert len(round1_tools) == 1
    assert round1_tools[0].name == "flowfile.graph.add_filter"
    assert "flow_id" not in round1_tools[0].parameters.get("properties", {})

    # Stage_advanced fired with completed_op telemetry.
    advances = [e for e in events if e.event == "stage_advanced"]
    assert any(e.payload.get("completed_op") == "flowfile.graph.add_filter" for e in advances)


# --------------------------------------------------------------------------- #
# Multi-node turn                                                              #
# --------------------------------------------------------------------------- #


def test_multi_node_turn_serializes_through_two_classify_cycles() -> None:
    """*"filter then sort"* runs the four-stage cycle twice in one
    session: 8 LLM rounds, 2 staged adds, sort's upstream is the
    not-yet-applied filter staged in the prior cycle."""
    flow = _make_flow()
    sess = _make_session(flow)

    provider = _ScriptedProvider(
        [
            # Cycle 1: filter
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-classify",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "add", "rationale": "first add: filter"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-pick-type",
                        name=PICK_NODE_TYPE_TOOL_NAME,
                        arguments={"node_type": "filter", "rationale": "row predicate"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-pick-up",
                        name=PICK_UPSTREAM_TOOL_NAME,
                        arguments={
                            "upstream_node_ids": [1],
                            "right_input_node_id": None,
                            "rationale": "from manual_input",
                        },
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-fill",
                        name="flowfile.graph.add_filter",
                        arguments={
                            "filter_input": {
                                "filter_type": "advanced",
                                "advanced_filter": "[region]=='EU'",
                            }
                        },
                    )
                ]
            ),
            # Cycle 2: sort
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c2-classify",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "add", "rationale": "second add: sort"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c2-pick-type",
                        name=PICK_NODE_TYPE_TOOL_NAME,
                        arguments={"node_type": "sort", "rationale": "order by amount"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c2-pick-up",
                        name=PICK_UPSTREAM_TOOL_NAME,
                        arguments={
                            # The sort attaches to the prior staged filter
                            # (node id allocated by the planner during cycle 1).
                            "upstream_node_ids": [2],
                            "right_input_node_id": None,
                            "rationale": "downstream of the filter",
                        },
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c2-fill",
                        name="flowfile.graph.add_sort",
                        arguments={
                            "sort_input": [
                                {"column": "amount", "how": "desc"},
                            ]
                        },
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # Two staged add operations; second's upstream is the first's id.
    assert len(sess.staged_results) == 2
    first_id = (sess.staged_results[0].staged_node_payload or {}).get("settings", {}).get("node_id")
    assert isinstance(first_id, int)
    second_insertion = (sess.staged_results[1].staged_node_payload or {}).get("insertion_context", {})
    assert second_insertion.get("upstream_node_ids") == [first_id]

    # Cycle 2's pick_upstream call saw the staged first_id in its enum.
    cycle2_pick_up_call = provider.calls[6]
    assert cycle2_pick_up_call["tools"][0].name == PICK_UPSTREAM_TOOL_NAME
    enum_values = cycle2_pick_up_call["tools"][0].parameters["properties"]["upstream_node_ids"]["items"]["enum"]
    assert first_id in enum_values, (
        f"staged node {first_id} missing from cycle-2 pick_upstream enum {enum_values}"
    )


def test_plan_stage_runs_before_classify_and_advances() -> None:
    """W71 v2.4 — agent_staged sessions now start at ``stage="plan"``.
    The LLM emits a brief markdown plan via
    ``flowfile.meta.emit_plan``; the planner records the plan,
    advances stage to ``classify``, then the normal cycle runs.
    Multi-node turns don't re-plan (plan fires ONCE per session).
    """
    from flowfile_core.ai.tools.meta_ops import EMIT_PLAN_TOOL_NAME

    flow = _make_flow()
    sess = _make_session(flow)
    # v2.4 tests opt back into the default-of-record so we exercise
    # the full state machine including the plan stage. ``_make_session``
    # otherwise overrides to ``"classify"`` for back-compat.
    sess.stage = "plan"

    plan_md = (
        "1. group_by — group by city, count customer_id as customer_count.\n"
        "2. sort — order by customer_count descending."
    )

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-plan",
                        name=EMIT_PLAN_TOOL_NAME,
                        arguments={
                            "plan": plan_md,
                            "rationale": "Group by city, then sort.",
                        },
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-classify",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "add", "rationale": "first add"},
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    events = asyncio.run(_drain(run_planner_session(
        session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()
    )))

    # Plan round advertised emit_plan only.
    plan_round_tools = provider.calls[0]["tools"]
    assert len(plan_round_tools) == 1
    assert plan_round_tools[0].name == EMIT_PLAN_TOOL_NAME
    # Stage advanced — second round sees classify_intent.
    classify_round_tools = provider.calls[1]["tools"]
    assert classify_round_tools[0].name == CLASSIFY_INTENT_TOOL_NAME
    # Plan came through as a stage_advanced event with op_kind_meta=plan.
    plan_events = [
        e for e in events
        if e.event == "stage_advanced"
        and e.payload.get("op_kind_meta") == "plan"
    ]
    assert len(plan_events) == 1, f"expected one plan stage_advanced; got {plan_events!r}"
    assert plan_events[0].payload.get("plan") == plan_md
    # Plan fires only once — no re-entry.
    plan_advance_count = sum(
        1 for e in events
        if e.event == "stage_advanced" and e.payload.get("from") == "plan"
    )
    assert plan_advance_count == 1


def test_emit_plan_tool_spec_has_plan_and_rationale_fields() -> None:
    """W71 v2.4 — the emit_plan ToolSpec exposes ``plan`` and
    ``rationale`` as required string fields. Regression-protects
    the contract the planner dispatch + chat-trail renderer rely on.
    """
    from flowfile_core.ai.tools.meta_ops import (
        EMIT_PLAN_TOOL_NAME,
        META_OPS_TOOLS,
    )

    spec = next(s for s in META_OPS_TOOLS if s.name == EMIT_PLAN_TOOL_NAME)
    props = spec.parameters["properties"]
    assert "plan" in props and props["plan"]["type"] == "string"
    assert "rationale" in props and props["rationale"]["type"] == "string"
    required = spec.parameters.get("required", [])
    assert "plan" in required and "rationale" in required


def test_pick_upstream_user_message_includes_staged_session_block() -> None:
    """W71 v1.12A — at the second cycle's pick_upstream stage, the user
    message must surface the staged-this-session node's id, type, and
    predicted columns. Without this, the LLM picks an upstream id from
    the enum blind (it knows id 2 is in the enum but has no idea which
    columns it exposes), which hurts accuracy on long chains.
    """
    flow = _make_flow()
    sess = _make_session(flow)

    provider = _ScriptedProvider(
        [
            # Cycle 1: filter (stages node 2 — output schema mirrors node 1).
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-classify",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "add", "rationale": "first add"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-pick-type",
                        name=PICK_NODE_TYPE_TOOL_NAME,
                        arguments={"node_type": "filter", "rationale": "row predicate"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-pick-up",
                        name=PICK_UPSTREAM_TOOL_NAME,
                        arguments={
                            "upstream_node_ids": [1],
                            "right_input_node_id": None,
                            "rationale": "from manual_input",
                        },
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-fill",
                        name="flowfile.graph.add_filter",
                        arguments={
                            "filter_input": {
                                "filter_type": "advanced",
                                "advanced_filter": "[region]=='EU'",
                            }
                        },
                    )
                ]
            ),
            # Cycle 2: sort downstream of the staged filter.
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c2-classify",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "add", "rationale": "second add"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c2-pick-type",
                        name=PICK_NODE_TYPE_TOOL_NAME,
                        arguments={"node_type": "sort", "rationale": "order by amount"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c2-pick-up",
                        name=PICK_UPSTREAM_TOOL_NAME,
                        arguments={
                            "upstream_node_ids": [2],
                            "right_input_node_id": None,
                            "rationale": "downstream of filter",
                        },
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c2-fill",
                        name="flowfile.graph.add_sort",
                        arguments={"sort_input": [{"column": "amount", "how": "desc"}]},
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # Cycle 2's pick_upstream is provider.calls[6] (0-indexed: c1×4 + c2-classify + c2-pick-type).
    cycle2_pick_up_call = provider.calls[6]
    assert cycle2_pick_up_call["tools"][0].name == PICK_UPSTREAM_TOOL_NAME
    user_msg = cycle2_pick_up_call["messages"][1].content or ""
    assert "## Staged this session" in user_msg, (
        "v1.12A: pick_upstream user message must include the staged-this-session "
        f"block on cycle 2; got: {user_msg!r}"
    )
    # The staged filter (node 2) and at least one of its predicted columns
    # (region, inherited from manual_input) must appear in the addendum.
    assert "node 2" in user_msg, user_msg
    assert "filter" in user_msg, user_msg
    assert "region" in user_msg, (
        "addendum should surface the staged node's predicted columns so the "
        f"LLM can verify the upstream is appropriate; got: {user_msg!r}"
    )


def test_pick_upstream_user_message_unchanged_when_no_staged_results() -> None:
    """W71 v1.12A — first-cycle pick_upstream (no prior staged adds)
    must NOT carry a *"## Staged this session"* block. The block only
    appears when there's something staged in the same session to show.
    """
    flow = _make_flow()
    sess = _make_session(flow)

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-classify",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "add", "rationale": "first add"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-pick-type",
                        name=PICK_NODE_TYPE_TOOL_NAME,
                        arguments={"node_type": "filter", "rationale": "row predicate"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-pick-up",
                        name=PICK_UPSTREAM_TOOL_NAME,
                        arguments={
                            "upstream_node_ids": [1],
                            "right_input_node_id": None,
                            "rationale": "from manual_input",
                        },
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    cycle1_pick_up_call = provider.calls[2]
    assert cycle1_pick_up_call["tools"][0].name == PICK_UPSTREAM_TOOL_NAME
    user_msg = cycle1_pick_up_call["messages"][1].content or ""
    assert "## Staged this session" not in user_msg, (
        "v1.12A: addendum must not appear when no add_* has been staged this session"
    )


# --------------------------------------------------------------------------- #
# AgentSession.reset_stage_state                                               #
# --------------------------------------------------------------------------- #


def test_reset_stage_state_clears_picked_fields() -> None:
    flow = _make_flow()
    sess = _make_session(flow)
    sess.stage = "fill_settings"
    sess.picked_op_kind = "add"
    sess.picked_node_type = "filter"
    sess.picked_upstream_ids = [1, 2]
    sess.picked_right_input_id = 3

    sessions.reset_stage_state(sess)

    assert sess.stage == "classify"
    assert sess.picked_op_kind is None
    assert sess.picked_node_type is None
    assert sess.picked_upstream_ids == []
    assert sess.picked_right_input_id is None


# --------------------------------------------------------------------------- #
# W71 v2.0 — agent_live REPL surface                                           #
# --------------------------------------------------------------------------- #


def _make_live_session(flow: FlowGraph, *, user_id: int = 1) -> sessions.AgentSession:
    snapshot = sessions.capture_graph_snapshot(flow)
    sess = sessions.AgentSession(
        flow_id=flow.flow_id,
        user_id=user_id,
        user_prompt="filter to EU rows",
        surface="agent_live",
        provider_name="fake",
        snapshot=snapshot,
        max_steps=16,
    )
    # W71 v2.4 — same skip-plan-by-default posture as
    # ``_make_session`` so v2.0 agent_live tests don't have to
    # prepend an emit_plan step. v2.4 plan tests reset
    # ``sess.stage = "plan"`` explicitly.
    sess.stage = "classify"
    return sess


def _filter_cycle(*, prefix: str, node_type: str, upstream: int) -> list[_Step]:
    """Helper — produce the 4 scripted steps for a single classify→
    pick_type→pick_upstream→fill_settings cycle."""
    return [
        _Step(
            tool_calls=[
                ToolCall(
                    id=f"{prefix}-classify",
                    name=CLASSIFY_INTENT_TOOL_NAME,
                    arguments={"op_kind": "add", "rationale": "live add"},
                )
            ]
        ),
        _Step(
            tool_calls=[
                ToolCall(
                    id=f"{prefix}-pick-type",
                    name=PICK_NODE_TYPE_TOOL_NAME,
                    arguments={"node_type": node_type, "rationale": f"add {node_type}"},
                )
            ]
        ),
        _Step(
            tool_calls=[
                ToolCall(
                    id=f"{prefix}-pick-up",
                    name=PICK_UPSTREAM_TOOL_NAME,
                    arguments={
                        "upstream_node_ids": [upstream],
                        "right_input_node_id": None,
                        "rationale": f"upstream {upstream}",
                    },
                )
            ]
        ),
    ]


def test_agent_live_applies_each_step_to_live_graph() -> None:
    """W71 v2.0 — agent_live applies every staged add LIVE; each
    new node lands in ``flow.nodes`` immediately, not in
    ``staged_results``. The session's ``applied_results`` carries
    one record per successfully-applied node and ``staged_results``
    stays empty (no batch diff)."""
    flow = _make_flow()
    sess = _make_live_session(flow)

    provider = _ScriptedProvider(
        [
            *_filter_cycle(prefix="c1", node_type="filter", upstream=1),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-fill",
                        name="flowfile.graph.add_filter",
                        arguments={
                            "filter_input": {
                                "filter_type": "advanced",
                                "advanced_filter": "[region]=='EU'",
                            }
                        },
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # The filter node is on the LIVE graph — not just staged.
    new_node_ids = [n.node_id for n in flow.nodes]
    assert 2 in new_node_ids, f"agent_live must apply live; got nodes {new_node_ids!r}"
    # No staged_results bundle — agent_live uses applied_results instead.
    assert sess.staged_results == [], sess.staged_results
    assert len(sess.applied_results) == 1
    rec = sess.applied_results[0]
    assert rec.tool_name == "flowfile.graph.add_filter"
    assert rec.node_id == 2
    assert rec.node_type == "filter"
    # Output schema captured from the live observation.
    assert any(c.get("name") == "region" for c in rec.output_schema)


def test_agent_live_undoes_failed_step_and_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    """W71 v2.0 — when the post-apply observation fails (runtime
    error from the actual polars run), the just-added node is
    deleted from the live graph (``flow.nodes`` returns to the
    last successful state), the error is fed back to the LLM as
    the next round's tool reply, and the per-step retry budget
    advances. After max_retries_per_step the run fails cleanly."""
    from flowfile_core.ai.agents import live_observation

    # Force the observation to fail every time. This simulates a
    # runtime polars error (e.g. ColumnNotFoundError) without
    # depending on the real polars path. ``observe_after_apply`` is
    # async so the planner awaits it; the stub mirrors that.
    async def _always_fail(flow_arg, node_id):
        return live_observation.ObservationResult(
            success=False,
            node_id=node_id,
            node_type="filter",
            error_kind="ColumnNotFoundError",
            error_message="'amout' not found. Available: order_id, region, amount",
        )

    monkeypatch.setattr(live_observation, "observe_after_apply", _always_fail)

    flow = _make_flow()
    sess = _make_live_session(flow)

    # Three consecutive fill attempts will all observe-fail; the
    # planner should auto-undo each time and exhaust the retry
    # budget on the third failure.
    fill_attempts = [
        _Step(
            tool_calls=[
                ToolCall(
                    id=f"fill-attempt-{i}",
                    name="flowfile.graph.add_filter",
                    arguments={
                        "filter_input": {
                            "filter_type": "advanced",
                            "advanced_filter": "[amout]=='EU'",
                        }
                    },
                )
            ]
        )
        for i in range(3)
    ]

    provider = _ScriptedProvider(
        [
            *_filter_cycle(prefix="c1", node_type="filter", upstream=1),
            *fill_attempts,
            _Step(content=None, finish_reason="stop"),
        ]
    )

    events = asyncio.run(_drain(run_planner_session(
        session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()
    )))

    # Live graph is back to its pre-run state — the failing node
    # was auto-undone every time.
    new_node_ids = sorted(n.node_id for n in flow.nodes)
    assert new_node_ids == [1], (
        f"agent_live must auto-undo the failing node; got nodes {new_node_ids!r}"
    )
    # No applied_results recorded (no successful step).
    assert sess.applied_results == []
    # Run terminated as failed after the third retry.
    assert sess.status == "failed"
    error_events = [e for e in events if e.event == "error"]
    assert error_events, f"expected error event; got events {[e.event for e in events]}"
    assert "agent_live" in (error_events[-1].payload.get("message") or "")
    # The runtime error reached the LLM via the tool replies that
    # rode each retry round.
    tool_msgs = [m for m in sess.messages if m.role == "tool"]
    assert any("ColumnNotFoundError" in (m.content or "") for m in tool_msgs), (
        "runtime error must appear in the tool replies the LLM saw"
    )


def test_agent_live_observation_includes_schema_in_tool_reply() -> None:
    """W71 v2.0 — on a successful apply, the post-apply tool reply
    embeds the output schema so the LLM can reason about what
    columns the new node produced. (Sample rows are best-effort
    and may be empty in Development mode; schema is the
    load-bearing observation.)"""
    flow = _make_flow()
    sess = _make_live_session(flow)

    provider = _ScriptedProvider(
        [
            *_filter_cycle(prefix="c1", node_type="filter", upstream=1),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-fill",
                        name="flowfile.graph.add_filter",
                        arguments={
                            "filter_input": {
                                "filter_type": "advanced",
                                "advanced_filter": "[region]=='EU'",
                            }
                        },
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    # The tool reply for the add_filter call must carry the
    # observation block.
    tool_replies = [
        m for m in sess.messages
        if m.role == "tool" and m.name == "flowfile.graph.add_filter"
    ]
    assert tool_replies, f"expected at least one tool reply; got {[m.role for m in sess.messages]}"
    content = tool_replies[-1].content or ""
    assert "Output schema:" in content, content
    assert "region" in content, "schema columns from live observation must reach the LLM"


def test_agent_live_does_not_use_staged_results() -> None:
    """W71 v2.0 — agent_live multi-add runs leave
    ``staged_results`` empty and populate ``applied_results``
    instead. Confirms the surface flips the entire post-apply
    bookkeeping path."""
    flow = _make_flow()
    sess = _make_live_session(flow)

    provider = _ScriptedProvider(
        [
            *_filter_cycle(prefix="c1", node_type="filter", upstream=1),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c1-fill",
                        name="flowfile.graph.add_filter",
                        arguments={
                            "filter_input": {
                                "filter_type": "advanced",
                                "advanced_filter": "[region]=='EU'",
                            }
                        },
                    )
                ]
            ),
            *_filter_cycle(prefix="c2", node_type="sort", upstream=2),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="c2-fill",
                        name="flowfile.graph.add_sort",
                        arguments={
                            "sort_input": [{"column": "amount", "how": "desc"}],
                        },
                    )
                ]
            ),
            _Step(content=None, finish_reason="stop"),
        ]
    )

    asyncio.run(_drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler())))

    assert sess.staged_results == [], (
        f"agent_live must NOT use staged_results; got {len(sess.staged_results)} entries"
    )
    assert len(sess.applied_results) == 2, (
        f"expected one applied_results entry per add; got {len(sess.applied_results)}"
    )
    assert sess.applied_results[0].node_type == "filter"
    assert sess.applied_results[1].node_type == "sort"
    # Both nodes are live on the canvas.
    live_ids = sorted(n.node_id for n in flow.nodes)
    assert live_ids == [1, 2, 3], live_ids


# --------------------------------------------------------------------------- #
# W71 v2.12 — opt-in verify-completion gate                                    #
# --------------------------------------------------------------------------- #


def test_verify_completion_stage_loops_back_to_classify() -> None:
    """W71 v2.12 — opt-in verify gate. After classify picks
    ``op_kind="other"``, the planner advances to ``verify_completion``.
    If the LLM returns ``is_complete=False``, the stage resets to
    ``classify`` so the next round can pick the missing op_kind.
    """
    flow = _make_flow()
    sess = _make_session(flow)
    sess.verify_plan_completion = True

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-classify-1",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "other", "rationale": "I think I'm done"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-verify-1",
                        name=VERIFY_COMPLETION_TOOL_NAME,
                        arguments={
                            "is_complete": False,
                            "rationale": "missing connect from new unique to group_by",
                        },
                    )
                ]
            ),
            # Round 3: at the new classify stage, exit the test by
            # emitting nothing. We only need to confirm the stage
            # transitions; how the LLM picks the next op is exercised
            # by other tests.
            _Step(content=None, finish_reason="stop"),
        ]
    )

    events = asyncio.run(
        _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))
    )

    advances = [e for e in events if e.event == "stage_advanced"]
    transitions = [(e.payload.get("from"), e.payload.get("to")) for e in advances]

    assert ("classify", "verify_completion") in transitions, (
        f"missing classify→verify_completion transition; got {transitions}"
    )
    assert ("verify_completion", "classify") in transitions, (
        f"missing verify_completion→classify transition (after is_complete=false); "
        f"got {transitions}"
    )
    # One-shot guard set so a stubborn is_complete=false can't ping-pong.
    assert sess.verify_round_consumed is True


def test_verify_completion_stage_terminates_when_complete() -> None:
    """W71 v2.12 — when the verify-completion LLM returns
    ``is_complete=True``, the planner routes back to ``classify`` (the
    natural empty-tool-call termination path) and the loop terminates
    cleanly.
    """
    flow = _make_flow()
    sess = _make_session(flow)
    sess.verify_plan_completion = True

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-classify-1",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "other", "rationale": "all done"},
                    )
                ]
            ),
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-verify-1",
                        name=VERIFY_COMPLETION_TOOL_NAME,
                        arguments={
                            "is_complete": True,
                            "rationale": "all 3 plan steps applied",
                        },
                    )
                ]
            ),
            # Round 3: LLM has nothing more; the loop exits naturally
            # via the non-mandatory empty-tool-call break path.
            _Step(content="The plan is complete.", finish_reason="stop"),
        ]
    )

    events = asyncio.run(
        _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))
    )

    advances = [e for e in events if e.event == "stage_advanced"]
    transitions = [(e.payload.get("from"), e.payload.get("to")) for e in advances]

    assert ("classify", "verify_completion") in transitions
    assert ("verify_completion", "classify") in transitions
    assert sess.verify_round_consumed is True
    # Loop exited cleanly — completed (no staged ops in this scripted
    # scenario, so the terminal handler routes through the empty-diff
    # ``complete`` path) or awaiting_user_input.
    assert sess.status in ("completed", "awaiting_user_input")


def test_verify_completion_skipped_when_flag_off() -> None:
    """W71 v2.12 — default behavior: with
    ``verify_plan_completion=False`` (the field default),
    ``op_kind="other"`` terminates the loop directly without entering
    the verify_completion stage. The new gate is fully opt-in.
    """
    flow = _make_flow()
    sess = _make_session(flow)
    # Default value — assert it explicitly so a future flip of the
    # default surfaces in this test rather than as a behavior shift.
    assert sess.verify_plan_completion is False

    provider = _ScriptedProvider(
        [
            _Step(
                tool_calls=[
                    ToolCall(
                        id="t-classify-1",
                        name=CLASSIFY_INTENT_TOOL_NAME,
                        arguments={"op_kind": "other", "rationale": "user is asking a question"},
                    )
                ]
            ),
            # Round 2: LLM emits no tool call — loop terminates via the
            # non-mandatory empty-tool-call break path.
            _Step(content="Here is my answer.", finish_reason="stop"),
        ]
    )

    events = asyncio.run(
        _drain(run_planner_session(session=sess, flow=flow, provider=provider, scheduler=_no_wait_scheduler()))
    )

    advances = [e for e in events if e.event == "stage_advanced"]
    transitions = [(e.payload.get("from"), e.payload.get("to")) for e in advances]

    assert ("classify", "verify_completion") not in transitions, (
        f"verify-completion gate must NOT fire when the flag is off; "
        f"got transitions {transitions}"
    )
    # Stage stays at classify (existing op_kind="other" termination path).
    assert sess.stage == "classify"
    assert sess.verify_round_consumed is False
