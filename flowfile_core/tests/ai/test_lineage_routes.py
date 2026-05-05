"""W51 — "Lineage Q&A across runs" endpoint tests.

Cases:

* ``test_lineage_question_emits_provider_chunks`` — happy path with
  injected run history: builds a tiny linear flow, monkeypatches
  ``_collect_run_history`` to return a deterministic window, POSTs, and
  verifies the SSE response carries ``event: chunk`` blocks +
  ``event: done``. Inspects the captured ``messages`` argument to
  ``provider.stream()`` to assert the W22 system prompt + the
  ``## Run history`` + ``## Question`` blocks all reach the LLM.
* ``test_lineage_question_pins_all_nodes_when_no_focus`` — capture
  ``render_prompt_context`` args; assert all flow node ids are passed
  as ``pinned_node_ids`` when ``focus_node_id`` is omitted.
* ``test_lineage_question_pins_focus_node_only`` — capture args; assert
  pinned set is exactly ``[focus_node_id]``.
* ``test_lineage_question_focus_node_not_in_flow_returns_422`` — bogus
  ``focus_node_id`` → 422.
* ``test_lineage_question_includes_run_history_block_format`` — happy
  path; assert the ``## Run history`` block contains the canonical
  table headers + per-node aggregates.
* ``test_lineage_question_no_history_block_when_window_empty`` — empty
  window → "no run history available" branch in the user-message
  composition.
* ``test_lineage_question_falls_back_to_in_memory_run_info`` — no
  ``source_registration_id`` but ``flow.latest_run_info`` is populated
  → the in-memory window is rendered. Exercised end-to-end through the
  real ``_collect_run_history``.
* ``test_lineage_question_uses_lineage_surface`` — capture
  ``render_prompt_context`` args; assert ``surface="lineage"`` is
  passed.
* ``test_lineage_question_samples_mode_forwarded`` — body's
  ``samples_mode`` reaches W22's ``render_prompt_context`` call.
* ``test_lineage_question_history_limit_forwarded`` — body's
  ``history_limit`` is threaded through to
  ``catalog_service.list_runs(limit=...)``.
* ``test_lineage_question_flow_not_found_returns_422`` — bogus
  ``flow_id`` → 422.
* ``test_lineage_question_empty_flow_returns_422`` — flow exists but
  has no nodes → 422 with the "no nodes" detail.
* ``test_lineage_question_unknown_provider_returns_404`` — bogus
  ``provider`` → 404.
* ``test_lineage_question_unconfigured_returns_409`` — no BYOK row +
  no env var → 409 with the ``ProviderNotConfiguredError`` message.
* ``test_lineage_question_disabled_returns_503`` — flipping
  ``FEATURE_FLAG_AI`` off short-circuits at W17's gate.
* ``test_lineage_question_validates_required_fields`` — request
  validation on missing ``flow_id`` / ``provider`` / ``question``,
  including parametrised "blank question".
* ``test_lineage_surface_in_lockstep`` — ``"lineage"`` is registered in
  ``tools/registry.py`` ``SurfaceLiteral`` + ``SURFACE_PRESETS``,
  ``context/builder.py`` ``SurfaceLiteral`` + ``SURFACE_TO_LEVEL``,
  ``context/budget.py`` ``_SURFACE_BUDGETS``, and every provider's
  ``surface_models``.
* ``test_format_run_history_block_renders_table_and_aggregates`` —
  pure-function tests on ``_format_run_history_block``: per-run table,
  per-node aggregates with median run-time, focus-node subsetting.
* ``test_aggregate_node_results_handles_skip_and_failure`` — pure
  ``_aggregate_node_results`` tests: success/failure/skip counters,
  most-recent-error pointer, run-time list excludes failures.
* ``test_parse_node_results_json_is_defensive`` — corrupt JSON / non-
  list / non-dict items → empty list, no raise.
* ``test_lazy_litellm_import_for_lineage_routes`` — importing
  ``flowfile_core.ai.lineage_routes`` doesn't pull ``litellm`` into
  ``sys.modules``.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timezone
from typing import Any, get_args

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import lineage_routes as lineage_routes_module
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.context import budget as context_budget
from flowfile_core.ai.context import builder as context_builder
from flowfile_core.ai.lineage_routes import (
    _aggregate_node_results,
    _format_run_history_block,
    _LineageWindow,
    _NodeAggregate,
    _parse_node_results_json,
    _RunSummary,
)
from flowfile_core.ai.providers import PROVIDERS
from flowfile_core.ai.providers.base import StreamChunk
from flowfile_core.ai.tools import registry as tool_registry
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, output_model, schemas, transform_schema

# --------------------------------------------------------------------------- #
# Shared fixtures                                                              #
# --------------------------------------------------------------------------- #


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    fake_user = PydanticUser(id=1, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


class FakeProvider:
    """Bare ``Provider``-shaped stub.

    Records the kwargs ``stream()`` is called with so tests can inspect
    the messages that reach the LLM (system prompt + the W22 user body
    + the W51 ``## Run history`` + ``## Question`` blocks).
    """

    def __init__(self, chunks: list[StreamChunk] | None = None) -> None:
        self.chunks = chunks or [
            StreamChunk(content_delta="Looking at the last 3 runs..."),
            StreamChunk(content_delta=" the column went null at run #43."),
            StreamChunk(finish_reason="stop"),
        ]
        self.last_call_kwargs: dict[str, Any] = {}
        self.name = "fake"
        self.model = "fake-default"
        self.supports_tools = True
        self.supports_streaming = True

    def stream(self, **kwargs: Any) -> AsyncIterator[StreamChunk]:
        self.last_call_kwargs = kwargs

        async def _gen() -> AsyncIterator[StreamChunk]:
            for chunk in self.chunks:
                yield chunk

        return _gen()


@pytest.fixture
def patch_get_configured_provider(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeProvider]:
    fake = FakeProvider()

    def _factory(*_args: Any, **_kwargs: Any) -> FakeProvider:
        return fake

    monkeypatch.setattr(lineage_routes_module, "get_configured_provider", _factory)
    yield fake


# --------------------------------------------------------------------------- #
# Flow fixtures                                                                #
# --------------------------------------------------------------------------- #


_FLOW_ID = 9951  # avoid clashing with W23/W50 flow ids


def _flow_settings(*, name: str = "w51_test", source_registration_id: int | None = None) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=_FLOW_ID,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_w51_lineage",
        name=name,
        source_registration_id=source_registration_id,
    )


def _build_linear_flow(*, name: str = "w51_test", source_registration_id: int | None = None) -> FlowGraph:
    """``orders (1) → filter_eu (2)`` — same shape as W23/W50 fixtures."""

    flow = FlowGraph(
        flow_settings=_flow_settings(name=name, source_registration_id=source_registration_id),
        name=name,
    )

    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
            ],
            data=[[1, 2], ["EU", "US"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(1).name = "orders"

    filter_node = input_schema.NodeFilter(
        flow_id=flow.flow_id,
        node_id=2,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(
            filter_type="advanced",
            advanced_filter="[region]=='EU'",
        ),
    )
    flow.add_filter(filter_node)
    add_connection(
        flow,
        node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2),
    )
    flow.get_node(2).name = "filter_eu"

    for node in flow.nodes:
        node.get_predicted_schema()
    return flow


def _build_empty_flow() -> FlowGraph:
    return FlowGraph(flow_settings=_flow_settings(name="empty_w51"), name="empty_w51")


def _make_window(*, with_runs: bool = True, focus_two_only: bool = False) -> _LineageWindow:
    """Hand-built ``_LineageWindow`` for happy-path tests."""

    if not with_runs:
        return _LineageWindow(flow_name="w51_test", registration_id=42, runs=[], per_node={})

    runs = [
        _RunSummary(
            run_id=45,
            started_at=datetime(2026, 5, 3, 9, 14, 2, tzinfo=timezone.utc),
            ended_at=datetime(2026, 5, 3, 9, 14, 5, tzinfo=timezone.utc),
            duration_seconds=3.2,
            success=True,
            nodes_completed=2,
            number_of_nodes=2,
            run_type="in_designer_run",
        ),
        _RunSummary(
            run_id=44,
            started_at=datetime(2026, 5, 2, 14, 0, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 5, 2, 14, 0, 3, tzinfo=timezone.utc),
            duration_seconds=2.9,
            success=True,
            nodes_completed=2,
            number_of_nodes=2,
            run_type="scheduled",
        ),
        _RunSummary(
            run_id=43,
            started_at=datetime(2026, 5, 2, 9, 0, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 5, 2, 9, 0, 1, tzinfo=timezone.utc),
            duration_seconds=1.1,
            success=False,
            nodes_completed=1,
            number_of_nodes=2,
            run_type="scheduled",
        ),
    ]
    per_node = {
        1: _NodeAggregate(
            node_id=1,
            node_name="orders",
            node_type="manual_input",
            success_count=3,
            run_times_ms=[150, 140, 130],
        ),
        2: _NodeAggregate(
            node_id=2,
            node_name="filter_eu",
            node_type="filter",
            success_count=2,
            failure_count=1,
            most_recent_error=(43, "Column 'customer_id' not found in upstream schema"),
            run_times_ms=[180, 175],
        ),
    }
    if focus_two_only:
        per_node = {2: per_node[2]}
    return _LineageWindow(flow_name="w51_test", registration_id=42, runs=runs, per_node=per_node)


@pytest.fixture
def patch_collect_run_history(monkeypatch: pytest.MonkeyPatch) -> Iterator[dict[str, Any]]:
    """Replace ``_collect_run_history`` with a deterministic 3-run window.

    Captures the call kwargs (``limit=...``) so tests can assert
    ``history_limit`` is threaded through. The yielded dict has both
    ``window`` and ``calls`` keys.
    """

    captured: dict[str, Any] = {"calls": []}
    window = _make_window()

    def _stub(flow: Any, flow_id: int, catalog_service: Any, *, limit: int) -> _LineageWindow:
        captured["calls"].append({"flow_id": flow_id, "limit": limit})
        return window

    monkeypatch.setattr(lineage_routes_module, "_collect_run_history", _stub)
    captured["window"] = window
    yield captured


@pytest.fixture
def registered_flow() -> Iterator[FlowGraph]:
    """Register a small flow so route-side ``flow_file_handler.get_flow``
    finds it. Cleans up after the test to keep the singleton tidy."""

    flow = _build_linear_flow()
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


@pytest.fixture
def registered_empty_flow() -> Iterator[FlowGraph]:
    flow = _build_empty_flow()
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


@pytest.fixture
def registered_flow_with_in_memory_run() -> Iterator[FlowGraph]:
    """Flow without ``source_registration_id``, but with
    ``flow.latest_run_info`` populated — exercises the in-memory
    fallback branch of ``_collect_run_history``.
    """

    flow = _build_linear_flow(source_registration_id=None)
    info = flow.create_initial_run_information(number_of_nodes=len(flow.nodes), run_type="full_run")
    info.start_time = datetime(2026, 5, 3, 9, 0, 0, tzinfo=timezone.utc)
    info.end_time = datetime(2026, 5, 3, 9, 0, 2, tzinfo=timezone.utc)
    info.success = False
    info.nodes_completed = 1
    info.node_step_result.append(
        output_model.NodeResult(
            node_id=1,
            node_name="orders",
            success=True,
            run_time_ms=140,
            is_running=False,
        )
    )
    info.node_step_result.append(
        output_model.NodeResult(
            node_id=2,
            node_name="filter_eu",
            success=False,
            error="Column 'customer_id' not found",
            is_running=False,
        )
    )
    flow.latest_run_info = info
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


# --------------------------------------------------------------------------- #
# 1. Happy path                                                                #
# --------------------------------------------------------------------------- #


def test_lineage_question_emits_provider_chunks(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "question": "Why is column customer_id null since Tuesday?",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")

    body = response.text
    assert 'event: chunk\ndata: {"content_delta": "Looking at the last 3 runs..."}' in body
    assert 'event: done\ndata: {"finish_reason": "stop"}' in body

    captured = patch_get_configured_provider.last_call_kwargs["messages"]
    assert len(captured) == 2
    system_msg, user_msg = captured

    assert system_msg.role == "system"
    assert system_msg.content.strip(), "system prompt must not be empty"

    assert user_msg.role == "user"
    # W22's render_prompt_context surfaces "## Subgraph" for the deterministic body.
    assert "## Subgraph" in user_msg.content
    # The W51 history + question blocks land verbatim.
    assert "## Run history" in user_msg.content
    assert "Showing last 3 run(s)" in user_msg.content
    assert "## Question" in user_msg.content
    assert "Why is column customer_id null since Tuesday?" in user_msg.content
    assert "read-only assist" in user_msg.content
    assert "Do not propose graph mutations" in user_msg.content

    # Read-only contract: tools=None.
    assert patch_get_configured_provider.last_call_kwargs.get("tools") is None


# --------------------------------------------------------------------------- #
# 2. Pinned-node-id contract                                                   #
# --------------------------------------------------------------------------- #


def test_lineage_question_pins_all_nodes_when_no_focus(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, Any] = {}
    real = lineage_routes_module.render_prompt_context

    def _spy(graph: Any, pinned_node_ids: Any, **kwargs: Any) -> Any:
        captured_kwargs["pinned_node_ids"] = list(pinned_node_ids)
        captured_kwargs.update(kwargs)
        return real(graph, pinned_node_ids, **kwargs)

    monkeypatch.setattr(lineage_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "question": "What does this flow do?",
        },
    )
    assert response.status_code == 200

    expected = sorted(node.node_id for node in registered_flow.nodes)
    assert sorted(captured_kwargs["pinned_node_ids"]) == expected
    assert captured_kwargs["surface"] == "lineage"


def test_lineage_question_pins_focus_node_only(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, Any] = {}
    real = lineage_routes_module.render_prompt_context

    def _spy(graph: Any, pinned_node_ids: Any, **kwargs: Any) -> Any:
        captured_kwargs["pinned_node_ids"] = list(pinned_node_ids)
        captured_kwargs.update(kwargs)
        return real(graph, pinned_node_ids, **kwargs)

    monkeypatch.setattr(lineage_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "question": "Why is filter_eu failing?",
            "focus_node_id": 2,
        },
    )
    assert response.status_code == 200
    assert captured_kwargs["pinned_node_ids"] == [2]


def test_lineage_question_focus_node_not_in_flow_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "question": "Why is X failing?",
            "focus_node_id": 999,
        },
    )
    assert response.status_code == 422
    assert "999" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 3. Prompt block format                                                       #
# --------------------------------------------------------------------------- #


def test_lineage_question_includes_run_history_block_format(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "question": "Why?",
        },
    )
    assert response.status_code == 200
    user_msg = patch_get_configured_provider.last_call_kwargs["messages"][1]
    text = user_msg.content
    # Per-run table headers
    assert "| Run | Started | Ended | Duration | Success | Nodes | Type |" in text
    # Per-node aggregates
    assert "### Per-node behaviour (all nodes)" in text
    assert "Node 2 — `filter_eu` (filter)" in text
    assert "Most recent error (run #43)" in text
    assert "Column 'customer_id' not found in upstream schema" in text
    # Successful runtimes show median + range
    assert "Run-time:" in text
    # Registration id surfaces
    assert "registration_id=42" in text


def test_lineage_question_no_history_block_when_window_empty(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No registration_id and no in-memory run → empty window branch."""

    empty_window = _make_window(with_runs=False)

    def _stub(flow: Any, flow_id: int, catalog_service: Any, *, limit: int) -> _LineageWindow:
        return empty_window

    monkeypatch.setattr(lineage_routes_module, "_collect_run_history", _stub)

    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "question": "When was this last run?",
        },
    )
    assert response.status_code == 200
    user_msg = patch_get_configured_provider.last_call_kwargs["messages"][1]
    assert "no run history available" in user_msg.content


def test_lineage_question_falls_back_to_in_memory_run_info(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow_with_in_memory_run: FlowGraph,
) -> None:
    """No ``source_registration_id`` but ``latest_run_info`` is set → the
    real ``_collect_run_history`` should render the in-memory window
    (``run_id=0`` / "live"). End-to-end without monkeypatching the
    history builder."""

    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "question": "Why did filter_eu fail?",
        },
    )
    assert response.status_code == 200
    user_msg = patch_get_configured_provider.last_call_kwargs["messages"][1]
    text = user_msg.content
    assert "## Run history" in text
    assert "no registration_id (in-memory or unsaved flow)" in text
    # In-memory run has run_id=0, rendered as the "live" label.
    assert "| live |" in text
    assert "Most recent error (run live)" in text
    assert "Column 'customer_id' not found" in text


# --------------------------------------------------------------------------- #
# 4. Surface contract                                                          #
# --------------------------------------------------------------------------- #


def test_lineage_question_uses_lineage_surface(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, Any] = {}
    real = lineage_routes_module.render_prompt_context

    def _spy(graph: Any, pinned_node_ids: Any, **kwargs: Any) -> Any:
        captured_kwargs.update(kwargs)
        return real(graph, pinned_node_ids, **kwargs)

    monkeypatch.setattr(lineage_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/lineage_question",
        json={"flow_id": _FLOW_ID, "provider": "anthropic", "question": "?"},
    )
    assert response.status_code == 200
    assert captured_kwargs["surface"] == "lineage"


def test_lineage_question_samples_mode_forwarded(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, Any] = {}
    real = lineage_routes_module.render_prompt_context

    def _spy(graph: Any, pinned_node_ids: Any, **kwargs: Any) -> Any:
        captured_kwargs.update(kwargs)
        return real(graph, pinned_node_ids, **kwargs)

    monkeypatch.setattr(lineage_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "question": "?",
            "samples_mode": "regex",
        },
    )
    assert response.status_code == 200
    assert captured_kwargs["samples_mode"] == "regex"


def test_lineage_question_history_limit_forwarded(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "question": "?",
            "history_limit": 25,
        },
    )
    assert response.status_code == 200
    assert patch_collect_run_history["calls"][-1]["limit"] == 25


# --------------------------------------------------------------------------- #
# 4b. W48 — prospective schema reaches the lineage surface                     #
# --------------------------------------------------------------------------- #


_W48_LINEAGE_FLOW_ID = 9947


@pytest.fixture
def registered_cold_flow_for_w48() -> Iterator[FlowGraph]:
    """Same shape as ``registered_flow`` but with the filter's
    ``predicted_schema`` cleared post-construction so it lands cold —
    the W48 path must auto-resolve it when ``render_prompt_context``
    walks the subgraph from the lineage route.
    """

    flow = _build_linear_flow(name="w48_lineage")
    # Override the flow_id so we don't clash with the W51 ``_FLOW_ID`` fixture
    # if both fixtures are alive at the same time. The setter cascades to
    # flow_settings + every node's setting_input.
    flow.flow_id = _W48_LINEAGE_FLOW_ID
    flow.get_node(2).node_schema.predicted_schema = None
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


def test_lineage_question_user_block_contains_columns_for_un_run_static_upstream(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_cold_flow_for_w48: FlowGraph,
) -> None:
    """W48 — a lineage question over a flow whose static upstream nodes
    are un-run (``predicted_schema=None``) → the W22 user block carries
    real column names instead of the ``schema: unknown`` sentinel. W51
    inherits the W22 fix transparently."""

    response = authed_client.post(
        "/ai/lineage_question",
        json={
            "flow_id": _W48_LINEAGE_FLOW_ID,
            "provider": "anthropic",
            "question": "what columns flow through the filter?",
        },
    )
    assert response.status_code == 200

    captured = patch_get_configured_provider.last_call_kwargs["messages"]
    assert len(captured) == 2  # system + user (W22 + history + question)
    _system_msg, user_msg = captured

    # The lineage user message is W22's body + history block + question.
    # The filter's section must surface its columns — the bug-of-record
    # was the assistant seeing "schema: unknown" for the filter and
    # asking the user to run the upstream nodes.
    assert "filter_eu" in user_msg.content
    assert "order_id" in user_msg.content
    assert "region" in user_msg.content

    filter_block_start = user_msg.content.find("### filter_eu")
    assert filter_block_start != -1, "expected ### filter_eu header in W22 user block"
    # Find where the filter block ends — at the next ### header or at the
    # ## Run history block if no further nodes.
    next_header = user_msg.content.find("\n##", filter_block_start + 1)
    filter_block = (
        user_msg.content[filter_block_start:next_header]
        if next_header != -1
        else user_msg.content[filter_block_start:]
    )
    assert "schema: unknown" not in filter_block, (
        f"filter block still reports schema: unknown — W48 fix not applied via lineage route. "
        f"Block:\n{filter_block[:500]}"
    )


# --------------------------------------------------------------------------- #
# 5. Error mapping                                                             #
# --------------------------------------------------------------------------- #


def test_lineage_question_flow_not_found_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
) -> None:
    response = authed_client.post(
        "/ai/lineage_question",
        json={"flow_id": 999_999, "provider": "anthropic", "question": "?"},
    )
    assert response.status_code == 422
    assert "flow" in response.json()["detail"].lower()


def test_lineage_question_empty_flow_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_empty_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/lineage_question",
        json={"flow_id": _FLOW_ID, "provider": "anthropic", "question": "?"},
    )
    assert response.status_code == 422
    assert "no nodes" in response.json()["detail"].lower()


def test_lineage_question_unknown_provider_returns_404(
    authed_client: TestClient,
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/lineage_question",
        json={"flow_id": _FLOW_ID, "provider": "imaginary", "question": "?"},
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


def test_lineage_question_unconfigured_returns_409(
    authed_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
) -> None:
    def _raise(*_args: Any, **_kwargs: Any) -> None:
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(lineage_routes_module, "get_configured_provider", _raise)

    response = authed_client.post(
        "/ai/lineage_question",
        json={"flow_id": _FLOW_ID, "provider": "anthropic", "question": "?"},
    )
    assert response.status_code == 409
    assert "anthropic" in response.json()["detail"]


def test_lineage_question_disabled_returns_503(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    patch_collect_run_history: dict[str, Any],
    registered_flow: FlowGraph,
) -> None:
    """Inheriting W17's router-level dependency means flipping the flag
    off must return 503 here too."""

    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/lineage_question",
            json={"flow_id": _FLOW_ID, "provider": "anthropic", "question": "?"},
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503
    assert "AI features are disabled" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 6. Request validation                                                        #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "missing_field, payload",
    [
        ("flow_id", {"provider": "anthropic", "question": "x"}),
        ("provider", {"flow_id": _FLOW_ID, "question": "x"}),
        ("question", {"flow_id": _FLOW_ID, "provider": "anthropic"}),
    ],
    ids=["missing_flow_id", "missing_provider", "missing_question"],
)
def test_lineage_question_validates_required_fields(
    authed_client: TestClient, missing_field: str, payload: dict[str, Any]
) -> None:
    response = authed_client.post("/ai/lineage_question", json=payload)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any(missing_field in str(item) for item in detail)


def test_lineage_question_rejects_blank_question(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/lineage_question",
        json={"flow_id": _FLOW_ID, "provider": "anthropic", "question": ""},
    )
    assert response.status_code == 422


def test_lineage_question_rejects_oversize_history_limit(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/lineage_question",
        json={"flow_id": _FLOW_ID, "provider": "anthropic", "question": "?", "history_limit": 999},
    )
    assert response.status_code == 422


# --------------------------------------------------------------------------- #
# 7. Surface lockstep                                                          #
# --------------------------------------------------------------------------- #


def test_lineage_surface_in_lockstep() -> None:
    """``"lineage"`` must appear in every place that knows about
    surfaces, otherwise downstream callers (W11/W22/W30 + budget) will
    fall back to defaults silently."""

    assert "lineage" in get_args(tool_registry.SurfaceLiteral)
    assert "lineage" in tool_registry.SURFACE_PRESETS
    assert tool_registry.SURFACE_PRESETS["lineage"] == frozenset()

    assert "lineage" in get_args(context_builder.SurfaceLiteral)
    assert context_builder.SURFACE_TO_LEVEL["lineage"] == "assist"

    assert "lineage" in context_budget._SURFACE_BUDGETS
    prompt_budget, response_budget = context_budget._SURFACE_BUDGETS["lineage"]
    assert prompt_budget >= 16_000  # enough room for the run-history block
    assert response_budget >= 1_000

    for name, provider_cls in PROVIDERS.items():
        models = getattr(provider_cls, "surface_models", {})
        assert "lineage" in models, f"provider {name!r} missing surface_models['lineage']"


# --------------------------------------------------------------------------- #
# 8. Pure-function tests                                                       #
# --------------------------------------------------------------------------- #


def test_format_run_history_block_renders_table_and_aggregates() -> None:
    window = _make_window()
    block = _format_run_history_block(window, focus_node_id=None)
    # Header and per-run table
    assert block.startswith("## Run history\n\n")
    assert "registration_id=42" in block
    assert "| Run | Started | Ended | Duration | Success | Nodes | Type |" in block
    assert "| #45 | 2026-05-03T09:14:02Z | 2026-05-03T09:14:05Z | 3.2s | OK | 2/2 | in_designer_run |" in block
    assert "| #44 |" in block
    assert "| #43 |" in block and "FAIL" in block
    # Per-node section
    assert "### Per-node behaviour (all nodes)" in block
    assert "Node 1 — `orders` (manual_input)" in block
    assert "Node 2 — `filter_eu` (filter)" in block
    # Median runtime is computed
    assert "median 175ms" in block or "median 177ms" in block  # statistics.median([180, 175])
    # Most recent error included
    assert "Most recent error (run #43)" in block


def test_format_run_history_block_focus_node_subsets() -> None:
    window = _make_window()
    block = _format_run_history_block(window, focus_node_id=2)
    assert "### Per-node behaviour (focus on node 2)" in block
    assert "Node 2 — `filter_eu` (filter)" in block
    # Node 1 must not appear in the per-node section.
    assert "Node 1 —" not in block


def test_format_run_history_block_empty_window() -> None:
    window = _make_window(with_runs=False)
    block = _format_run_history_block(window, focus_node_id=None)
    assert "no run history available" in block
    # Per-node section must not render when there are no runs.
    assert "### Per-node behaviour" not in block


def test_aggregate_node_results_handles_skip_and_failure() -> None:
    runs = [
        _RunSummary(
            run_id=10,
            started_at=None,
            ended_at=None,
            duration_seconds=None,
            success=False,
            nodes_completed=1,
            number_of_nodes=2,
            run_type="in_designer_run",
        ),
        _RunSummary(
            run_id=9,
            started_at=None,
            ended_at=None,
            duration_seconds=None,
            success=True,
            nodes_completed=2,
            number_of_nodes=2,
            run_type="in_designer_run",
        ),
    ]
    node_results_by_run = {
        10: [
            {"node_id": 1, "node_name": "orders", "success": True, "run_time_ms": 100, "error": ""},
            {"node_id": 2, "node_name": "filter", "success": False, "run_time_ms": -1, "error": "boom"},
        ],
        9: [
            {"node_id": 1, "node_name": "orders", "success": True, "run_time_ms": 90, "error": ""},
            {"node_id": 2, "node_name": "filter", "success": True, "run_time_ms": 200, "error": ""},
        ],
    }

    class _Stub:
        def get_node(self, node_id: int) -> Any:  # pragma: no cover — defensive shape
            return None

    aggregates = _aggregate_node_results(_Stub(), runs, node_results_by_run)
    assert aggregates[1].success_count == 2
    assert aggregates[1].failure_count == 0
    assert aggregates[1].run_times_ms == [100, 90]
    assert aggregates[2].success_count == 1
    assert aggregates[2].failure_count == 1
    # Most recent error comes from run 10 (newest-first iteration).
    assert aggregates[2].most_recent_error == (10, "boom")
    # Failed runs don't contribute run-times.
    assert aggregates[2].run_times_ms == [200]


def test_aggregate_node_results_handles_skip_status() -> None:
    runs = [
        _RunSummary(
            run_id=10,
            started_at=None,
            ended_at=None,
            duration_seconds=None,
            success=False,
            nodes_completed=0,
            number_of_nodes=2,
            run_type="in_designer_run",
        ),
    ]
    node_results_by_run = {
        10: [
            {"node_id": 1, "success": False, "run_time_ms": -1, "error": "upstream failed"},
            {"node_id": 2, "success": None, "run_time_ms": -1, "error": ""},
        ],
    }

    class _Stub:
        def get_node(self, node_id: int) -> Any:
            return None

    aggregates = _aggregate_node_results(_Stub(), runs, node_results_by_run)
    assert aggregates[1].failure_count == 1
    # Node 2 was skipped (success=None).
    assert aggregates[2].skip_count == 1
    assert aggregates[2].failure_count == 0


def test_parse_node_results_json_is_defensive() -> None:
    assert _parse_node_results_json(None) == []
    assert _parse_node_results_json("") == []
    assert _parse_node_results_json("   ") == []
    # Corrupt JSON → empty list, no raise.
    assert _parse_node_results_json("{not-json") == []
    # Non-list payload → empty list.
    assert _parse_node_results_json('{"a": 1}') == []
    # Mixed types in list → only dicts kept.
    assert _parse_node_results_json('[{"node_id": 1}, "junk", null, 42]') == [{"node_id": 1}]


# --------------------------------------------------------------------------- #
# 9. Lazy-litellm contract                                                     #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_import_for_lineage_routes() -> None:
    """``import flowfile_core.ai.lineage_routes`` mustn't pull litellm.

    Same contract as W11/W12/W13/W20/W23/W50 — the module sits behind
    the BYOK seam, not the ``provider_factory`` bootstrap, so the heavy
    SDK stays out of the import graph until a real call happens.

    Caveat: a sibling test may have already imported ``litellm`` in
    this process (the suite shares a Python interpreter). When that's
    the case we can't observe the lazy contract, so the assertion is
    gated on a clean snapshot — same posture as W12 / W20 / W23 / W50.
    """

    litellm_already_loaded = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)

    saved = {k: v for k, v in sys.modules.items() if k.startswith("flowfile_core.ai.lineage_routes")}
    for k in list(saved):
        sys.modules.pop(k, None)
    try:
        import importlib

        importlib.import_module("flowfile_core.ai.lineage_routes")
        if not litellm_already_loaded:
            assert (
                "litellm" not in sys.modules
            ), "flowfile_core.ai.lineage_routes pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)
