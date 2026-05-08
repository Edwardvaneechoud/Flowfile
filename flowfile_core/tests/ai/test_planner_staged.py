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
    return sessions.AgentSession(
        flow_id=flow.flow_id,
        user_id=user_id,
        user_prompt="filter to EU rows",
        surface="agent_staged",
        provider_name="fake",
        snapshot=snapshot,
        max_steps=16,
    )


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
