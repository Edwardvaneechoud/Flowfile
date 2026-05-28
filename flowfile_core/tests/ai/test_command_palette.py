"""Cmd+K command palette tests.

Coverage (~14 cases on :func:`run_command_palette`):

* Surface vocabulary in lockstep across / / providers.
* Tools are passed to the provider (cmd_k surface uses tool calls; not JSON-mode).
* Tool-list size matches ``SURFACE_PRESETS["cmd_k"]``.
* Server-assigned ``node_id`` injection on ``add_*`` tool calls.
* Server-assigned ``flow_id`` injection on ``add_*`` tool calls.
* Happy path: single ``add_filter`` tool call → staged diff registered.
* Read-only tools (``flowfile.schema.read_node_schema``) silently dropped.
* Multi-op sequence: two ``add_*`` calls each get distinct fresh node ids.
* Soft failures: timeout / provider error / no tool calls / all-refused →
  ``degraded=True`` with a stable ``reason`` string.
* Partial refusal: one valid + one invalid → diff with the valid op +
  ``refused`` list carries the invalid one.
* Lazy-litellm contract.
* audit emission isn't double-counted by.
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from typing import Any

import pytest

from flowfile_core.ai import command_palette as cmdk_mod
from flowfile_core.ai.command_palette import (
    CommandPaletteInsertionContext,
    CommandPaletteResponse,
    RefusedToolCall,
    run_command_palette,
)
from flowfile_core.ai.diff import clear_for_tests, get_diff
from flowfile_core.ai.providers.base import ChatResponse, ToolCall, Usage
from flowfile_core.ai.scheduler import RateLimitScheduler
from flowfile_core.ai.tools import build_tool_catalog
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas, transform_schema

# --------------------------------------------------------------------------- #
# Test helpers #
# --------------------------------------------------------------------------- #


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_ai_cmdk",
    )


def _flow_with_orders(flow_id: int = 1) -> FlowGraph:
    """Build a flow with one ``manual_input`` node id=1 carrying 4 columns."""
    flow = FlowGraph(flow_settings=_flow_settings(flow_id), name="cmdk_test")
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="customer_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="amount", data_type="Double"),
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
            ],
            data=[[1, 2, 3, 4], [10, 20, 30, 40], [100.0, 200.0, 50.0, 75.0], ["EU", "US", "EU", "US"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(1).name = "orders"
    # Force schema prediction so the upstream has predicted_schema cached.
    flow.get_node(1).get_predicted_schema()
    return flow


def _filter_settings_for_region(node_id: int = 99) -> dict[str, Any]:
    """Settings dict an LLM would emit for ``add_filter`` against ``region``."""
    settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=node_id,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[region]=='EU'"),
    )
    return settings.model_dump(mode="json")


def _filter_settings_for_unknown(node_id: int = 99) -> dict[str, Any]:
    """Settings dict referencing a column the upstream doesn't have."""
    settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=node_id,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            filter_type="basic",
            basic_filter=transform_schema.BasicFilter(
                field="not_a_real_column",
                operator="equals",
                value="x",
            ),
        ),
    )
    return settings.model_dump(mode="json")


class _FakeProvider:
    """Records the kwargs the module called ``chat()`` with.

    Matches /'s ``_FakeProvider`` shape — sleep-before-response
    enables timeout testing; ``raise_exc`` exercises the provider-error
    branch; ``tool_calls`` lets each test set the LLM's response shape.
    """

    name: str = "fake-cmdk"
    model: str = "fake-default"
    supports_tools: bool = True
    supports_streaming: bool = True

    def __init__(
        self,
        *,
        content: str | None = None,
        tool_calls: list[ToolCall] | None = None,
        sleep_before_response: float = 0.0,
        raise_exc: BaseException | None = None,
    ) -> None:
        self._content = content
        self._tool_calls = list(tool_calls or [])
        self._sleep = sleep_before_response
        self._raise = raise_exc
        self.last_call_kwargs: dict[str, Any] = {}
        self.call_count: int = 0

    async def chat(
        self,
        messages: list[Any],
        tools: list[Any] | None = None,
        max_tokens: int | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> ChatResponse:
        self.call_count += 1
        self.last_call_kwargs = {
            "messages": messages,
            "tools": tools,
            "max_tokens": max_tokens,
            "response_format": response_format,
        }
        if self._sleep > 0:
            await asyncio.sleep(self._sleep)
        if self._raise is not None:
            raise self._raise
        return ChatResponse(
            model=self.model,
            content=self._content,
            tool_calls=list(self._tool_calls),
            finish_reason="tool_calls" if self._tool_calls else "stop",
            usage=Usage(),
        )

    def stream(self, *_a: Any, **_kw: Any):  # pragma: no cover - we never stream cmd_k
        raise AssertionError("stream() should not be called by run_command_palette")


def _scheduler() -> RateLimitScheduler:
    """Scheduler with monotonic-zero time-source so RPM never blocks tests."""
    return RateLimitScheduler(time_source=lambda: 0.0, sleep=lambda *_a, **_k: asyncio.sleep(0))


@pytest.fixture(autouse=True)
def _clear_diff_store() -> None:
    """Wipe the in-memory diff store between cases.

    ``register_diff`` is a process-local dict; without a clean slate the
    integration tests bleed diff_ids into each other and assertions on
    "the diff was just registered" become noisy.
    """
    clear_for_tests()


# --------------------------------------------------------------------------- #
# 1. Surface vocabulary lockstep #
# --------------------------------------------------------------------------- #


def test_cmd_k_surface_in_lockstep() -> None:
    """``cmd_k`` exists in ``SurfaceLiteral`` × ``SURFACE_PRESETS`` × ``SURFACE_TO_LEVEL`` × every provider's ``surface_models``."""
    from typing import get_args as _get_args

    from flowfile_core.ai.context import builder as ctx_builder
    from flowfile_core.ai.tools import registry as tool_registry

    assert "cmd_k" in _get_args(tool_registry.SurfaceLiteral)
    assert "cmd_k" in ctx_builder.SURFACE_TO_LEVEL
    assert ctx_builder.SURFACE_TO_LEVEL["cmd_k"] == "copilot"
    assert "cmd_k" in tool_registry.SURFACE_PRESETS
    # cmd_k preset is non-empty (~6 tools per).
    assert tool_registry.SURFACE_PRESETS["cmd_k"]
    tool_registry._check_preset_coverage()  # must not raise

    from flowfile_core.ai.providers import (
        AnthropicProvider,
        GoogleProvider,
        GroqProvider,
        OllamaProvider,
        OpenAIProvider,
        OpenRouterProvider,
    )

    for provider_cls in (
        AnthropicProvider,
        OpenAIProvider,
        GoogleProvider,
        GroqProvider,
        OpenRouterProvider,
        OllamaProvider,
    ):
        assert "cmd_k" in provider_cls.surface_models, f"{provider_cls.__name__} missing cmd_k in surface_models"


# --------------------------------------------------------------------------- #
# 2. Provider-call wiring: tools must be passed #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_tools_passed_to_provider() -> None:
    """Cmd+K is a tool-call surface; the provider must receive non-empty tools.

    This is the inverse of W20/W23/W34 which all assert ``tools=None``.
    Regression guard: if a refactor accidentally drops the catalog, the LLM
    can't propose anything and every cmd_k call would silently degrade.
    """
    flow = _flow_with_orders()
    provider = _FakeProvider(tool_calls=[])
    await run_command_palette(
        flow,
        prompt="filter eu",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
    )
    tools_passed = provider.last_call_kwargs.get("tools")
    assert tools_passed is not None
    assert isinstance(tools_passed, list)
    assert len(tools_passed) >= 1


@pytest.mark.asyncio
async def test_tools_match_cmd_k_preset_size() -> None:
    """Tool count passed to the provider equals the cmd_k preset size."""
    flow = _flow_with_orders()
    expected = list(build_tool_catalog(surface="cmd_k"))
    provider = _FakeProvider(tool_calls=[])
    await run_command_palette(
        flow,
        prompt="anything",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
    )
    assert len(provider.last_call_kwargs["tools"]) == len(expected)


# --------------------------------------------------------------------------- #
# 3. Server-assigned id injection #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_node_id_assigned_server_side() -> None:
    """The fresh server-assigned ``node_id`` overrides whatever the LLM set.

    The LLM is told to use placeholders; the staged settings carry an id
    that doesn't collide with any existing node.
    """
    flow = _flow_with_orders()
    # LLM sends node_id=0 and flow_id=0 placeholders.
    bad_args = _filter_settings_for_region(node_id=0)
    bad_args["flow_id"] = 0
    bad_args["node_id"] = 0
    tool_call = ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=bad_args)

    provider = _FakeProvider(tool_calls=[tool_call])
    response = await run_command_palette(
        flow,
        prompt="filter eu",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
    )
    assert response.degraded is False, response.refused
    assert response.diff is not None
    assert len(response.diff.additions) == 1
    staged = response.diff.additions[0]
    # Existing node id is 1; the server must have assigned 2.
    assert staged.settings["node_id"] == 2
    assert staged.settings["flow_id"] == flow.flow_id


@pytest.mark.asyncio
async def test_multi_op_sequence_assigns_distinct_ids() -> None:
    """Two ``add_*`` calls each get a fresh sequential node_id."""
    flow = _flow_with_orders()
    args1 = _filter_settings_for_region()
    args2 = _filter_settings_for_region()
    provider = _FakeProvider(
        tool_calls=[
            ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=args1),
            ToolCall(id="t2", name="flowfile.graph.add_filter", arguments=args2),
        ]
    )
    response = await run_command_palette(
        flow,
        prompt="filter eu twice",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
    )
    assert response.degraded is False, response.refused
    assert len(response.diff.additions) == 2
    ids = sorted(int(a.settings["node_id"]) for a in response.diff.additions)
    assert ids == [2, 3]


# --------------------------------------------------------------------------- #
# 4. Happy path: diff registered + accept-ready #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_happy_path_single_op_registers_diff() -> None:
    """End-to-end: single ``add_filter`` tool call → ``diff_id`` retrievable
    from the store, ready for the accept route."""
    flow = _flow_with_orders()
    tool_call = ToolCall(
        id="t1",
        name="flowfile.graph.add_filter",
        arguments=_filter_settings_for_region(),
    )
    provider = _FakeProvider(
        content="Filter rows where region == 'EU'.",
        tool_calls=[tool_call],
    )
    response = await run_command_palette(
        flow,
        prompt="filter to eu only",
        provider=provider,
        session_id="cmdk-test",
        user_id=42,
        scheduler=_scheduler(),
    )
    assert isinstance(response, CommandPaletteResponse)
    assert response.degraded is False, response.refused
    assert response.diff_id is not None
    assert response.op_count == 1
    assert response.rationale == "Filter rows where region == 'EU'."

    # Diff must be retrievable via the store — that's how accepts it.
    stored = get_diff(response.diff_id)
    assert stored is not None
    assert stored.session_id == "cmdk-test"
    assert stored.flow_id == flow.flow_id
    assert len(stored.additions) == 1
    assert stored.additions[0].node_type == "filter"


# --------------------------------------------------------------------------- #
# 5. Read-only tool calls dropped silently #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_read_only_tool_calls_dropped() -> None:
    """``flowfile.schema.read_node_schema`` is in the cmd_k preset (so the
    LLM CAN call it for grounding) but it's read-only — never feeds a diff.
    The path silently drops these without affecting the staged-op count.
    """
    flow = _flow_with_orders()
    add_call = ToolCall(
        id="t1",
        name="flowfile.graph.add_filter",
        arguments=_filter_settings_for_region(),
    )
    schema_call = ToolCall(
        id="t2",
        name="flowfile.schema.read_node_schema",
        arguments={"node_id": 1},
    )
    provider = _FakeProvider(tool_calls=[schema_call, add_call])
    response = await run_command_palette(
        flow,
        prompt="filter eu",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
    )
    assert response.degraded is False
    assert response.op_count == 1
    assert len(response.diff.additions) == 1
    # The schema call did not appear in `refused` either — silent drop.
    assert all(r.tool_name != "flowfile.schema.read_node_schema" for r in response.refused)


# --------------------------------------------------------------------------- #
# 6. Soft failure paths #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_degrades_on_no_tool_calls() -> None:
    """LLM returned text only — no ``add_*`` ops to stage."""
    flow = _flow_with_orders()
    provider = _FakeProvider(content="I can't do that.", tool_calls=[])
    response = await run_command_palette(
        flow,
        prompt="something the LLM refuses",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
    )
    assert response.degraded is True
    assert response.reason == "no_tool_calls"
    assert response.diff is None
    assert response.diff_id is None
    assert response.rationale == "I can't do that."


@pytest.mark.asyncio
async def test_degrades_on_provider_timeout() -> None:
    flow = _flow_with_orders()
    provider = _FakeProvider(tool_calls=[], sleep_before_response=2.0)
    response = await run_command_palette(
        flow,
        prompt="anything",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
        timeout=0.05,
    )
    assert response.degraded is True
    assert response.reason == "timeout"


@pytest.mark.asyncio
async def test_degrades_on_provider_error() -> None:
    flow = _flow_with_orders()
    provider = _FakeProvider(raise_exc=RuntimeError("boom"))
    response = await run_command_palette(
        flow,
        prompt="anything",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
    )
    assert response.degraded is True
    assert response.reason == "provider_error"


@pytest.mark.asyncio
async def test_degrades_when_all_calls_refused() -> None:
    """Every LLM tool call is rejected by the executor (e.g. unknown column)
    → ``degraded=true, reason="all_refused"``; ``refused`` carries the details
    so the frontend can show why.

    Insertion context with ``upstream_node_ids=[1]`` is required to trigger's column-ref validation pipeline — the executor only validates refs
    when it has an upstream schema to validate against (D011 tier 1+).
    """
    flow = _flow_with_orders()
    bad_call = ToolCall(
        id="t1",
        name="flowfile.graph.add_filter",
        arguments=_filter_settings_for_unknown(),
    )
    provider = _FakeProvider(tool_calls=[bad_call])
    response = await run_command_palette(
        flow,
        prompt="filter by something missing",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
        insertion_context=CommandPaletteInsertionContext(upstream_node_ids=[1]),
    )
    assert response.degraded is True
    assert response.reason == "all_refused"
    assert response.diff is None
    assert len(response.refused) == 1
    assert response.refused[0].tool_name == "flowfile.graph.add_filter"
    assert response.refused[0].refusal_reason == "unknown_columns"


# --------------------------------------------------------------------------- #
# 7. Partial refusal still stages the valid ops #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_partial_refusal_stages_valid_ops() -> None:
    """One valid + one bad → diff carries the valid op; ``refused`` lists the
    bad one. Mirrors how a user-trusting cmd_k surface should behave: do
    what we can, report what we can't.

    Insertion context with ``upstream_node_ids=[1]`` is required so the
    executor can validate column refs (D011 tier 1+).
    """
    flow = _flow_with_orders()
    good = ToolCall(id="g", name="flowfile.graph.add_filter", arguments=_filter_settings_for_region())
    bad = ToolCall(id="b", name="flowfile.graph.add_filter", arguments=_filter_settings_for_unknown())
    provider = _FakeProvider(tool_calls=[good, bad])
    response = await run_command_palette(
        flow,
        prompt="mixed",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
        insertion_context=CommandPaletteInsertionContext(upstream_node_ids=[1]),
    )
    assert response.degraded is False
    assert response.op_count == 1
    assert len(response.diff.additions) == 1
    assert len(response.refused) == 1
    refused = response.refused[0]
    assert isinstance(refused, RefusedToolCall)
    assert refused.refusal_reason == "unknown_columns"


# --------------------------------------------------------------------------- #
# 8. Insertion context flows from the request #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_w62_default_insertion_context_resolves_non_zero_coords() -> None:
    """when the frontend leaves ``insertion_context.pos_x`` /
    ``pos_y`` unset (or doesn't pass an ``insertion_context`` at all), the
    executor's auto-layout resolver fills in coords derived from the
    upstream. previously the staged ops always landed at (0, 0) so the user
    had to manually drag them apart. Multi-op fan-out from the same
    upstream stacks vertically via ``staged_offset_index``.
    """
    flow = _flow_with_orders()
    flow.get_node(1).setting_input.pos_x = 100.0
    flow.get_node(1).setting_input.pos_y = 200.0

    # Two add_filter calls — both anchored at upstream node 1 by passing
    # ``upstream_node_ids=[1]`` but leaving pos_x / pos_y unset.
    tool_calls = [
        ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_settings_for_region()),
        ToolCall(id="t2", name="flowfile.graph.add_filter", arguments=_filter_settings_for_region(node_id=42)),
    ]
    provider = _FakeProvider(tool_calls=tool_calls)
    response = await run_command_palette(
        flow,
        prompt="filter twice",
        provider=provider,
        session_id="sid-w62",
        user_id=1,
        scheduler=_scheduler(),
        insertion_context=CommandPaletteInsertionContext(upstream_node_ids=[1]),
    )
    assert response.degraded is False, response.refused
    additions = response.diff.additions
    assert len(additions) == 2

    coord_a = (additions[0].insertion_context.pos_x, additions[0].insertion_context.pos_y)
    coord_b = (additions[1].insertion_context.pos_x, additions[1].insertion_context.pos_y)
    # previously regression: both at (0, 0).
    assert coord_a != (0.0, 0.0)
    assert coord_b != (0.0, 0.0)
    # Cmd+K fan-out: both anchored at node 1, so same x and the second
    # offsets vertically via ``staged_offset_index``.
    assert coord_a[0] == coord_b[0]
    assert coord_b[1] > coord_a[1]


@pytest.mark.asyncio
async def test_insertion_context_threaded_into_executor() -> None:
    """The request's ``insertion_context.upstream_node_ids`` lands on the
    staged addition's ``insertion_context``."""
    flow = _flow_with_orders()
    tool_call = ToolCall(
        id="t1",
        name="flowfile.graph.add_filter",
        arguments=_filter_settings_for_region(),
    )
    provider = _FakeProvider(tool_calls=[tool_call])
    response = await run_command_palette(
        flow,
        prompt="filter eu",
        provider=provider,
        session_id="sid",
        user_id=1,
        scheduler=_scheduler(),
        insertion_context=CommandPaletteInsertionContext(
            upstream_node_ids=[1],
            pos_x=200.0,
            pos_y=400.0,
        ),
    )
    assert response.degraded is False, response.refused
    staged = response.diff.additions[0]
    assert staged.insertion_context.upstream_node_ids == [1]
    assert staged.insertion_context.pos_x == 200.0
    assert staged.insertion_context.pos_y == 400.0


# --------------------------------------------------------------------------- #
# 9. Lazy-litellm contract #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract() -> None:
    """``import flowfile_core.ai.command_palette`` mustn't drag in litellm.

    Mirror of / / / / / / — the / /
    contract is process-wide.
    """
    litellm_already_loaded = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)

    saved = {k: v for k, v in sys.modules.items() if k.startswith("flowfile_core.ai.command_palette")}
    for k in list(saved):
        sys.modules.pop(k, None)
    try:
        importlib.import_module("flowfile_core.ai.command_palette")
        if not litellm_already_loaded:
            assert (
                "litellm" not in sys.modules
            ), "flowfile_core.ai.command_palette pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)
        # Restore the cached canonical instance.
        sys.modules["flowfile_core.ai.command_palette"] = cmdk_mod
