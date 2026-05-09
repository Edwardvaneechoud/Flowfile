"""read-only chat-stream endpoint tests.

Cases:

* ``test_chat_stream_emits_provider_chunks`` — happy path: mock provider
  yields three content deltas + finish_reason; SSE response carries the
  matching ``event: chunk`` blocks plus ``event: done``.
* ``test_chat_stream_unknown_provider_404`` — bogus provider name returns
  404 with the supported-list detail.
* ``test_chat_stream_unconfigured_returns_409`` — no BYOK row + no env var
  + not ollama → 409 with the ``ProviderNotConfiguredError`` message.
* ``test_chat_stream_disabled_returns_503`` — ``FEATURE_FLAG_AI=False``
  short-circuits with's 503 (the gate inherited from ``ai_router``).
* ``test_chat_stream_validates_messages_present`` — empty ``messages``
  list returns 422 from Pydantic.
* ``test_chat_stream_validates_role_whitelist`` — ``role: "tool"`` is
  rejected at the request boundary; is read-only, no tool messages.
* ``test_chat_stream_passes_max_tokens_through`` — body's ``max_tokens``
  reaches the provider stub.
* ``test_chat_stream_no_tools_passed_to_provider`` — the route passes
  ``tools=None``; tool execution is's job.
* ``test_lazy_litellm_import_for_chat_routes`` — importing
  ``flowfile_core.ai.chat_routes`` doesn't pull ``litellm`` into
  ``sys.modules``. cases:

* ``test_chat_stream_prepends_assist_prompt_for_surface_explain`` — when
  ``surface="explain"``, the provider sees ``[system (W22 assist), user]``.
* ``test_chat_stream_prepends_default_prompt_when_surface_missing`` — no
  ``surface`` field → falls back to the default surface (``"explain"`` →
  ``assist`` per's ``SURFACE_TO_LEVEL``).
* ``test_chat_stream_falls_back_when_surface_unknown`` — ``surface="bogus"``
  is silently coerced to the default (we don't 422 a chat call).
* ``test_chat_stream_uses_copilot_prompt_for_surface_cmd_k`` — covers a
  non-default surface so the surface→level dispatch is exercised.
* ``test_chat_stream_server_prompt_precedes_client_system_message`` — when
  the client supplies its own ``system`` message, the server prompt is
  prepended in front of it (server first, client second, then user).
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import chat_routes as chat_routes_module
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.providers.base import StreamChunk
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema

# ---------- shared fixtures ----------


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    """TestClient with auth overridden to a synthetic local user.

    Same shape as ``test_byok.py``'s ``authed_client`` — bypasses the
    /auth/token round-trip that needs ``FLOWFILE_MODE=electron``.
    """
    fake_user = PydanticUser(id=1, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


class FakeProvider:
    """Bare ``Provider``-shaped stub.

    Records the kwargs ``stream()`` is called with so tests can assert the
    route's wiring (e.g. ``tools=None``, ``max_tokens`` passthrough).
    """

    def __init__(
        self,
        chunks: list[StreamChunk] | None = None,
    ) -> None:
        self.chunks = chunks or [
            StreamChunk(content_delta="hello "),
            StreamChunk(content_delta="world"),
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
def patch_get_configured_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[FakeProvider]:
    """Replace ``chat_routes.get_configured_provider`` with a fake."""
    fake = FakeProvider()

    def _factory(*_args: Any, **_kwargs: Any) -> FakeProvider:
        return fake

    monkeypatch.setattr(chat_routes_module, "get_configured_provider", _factory)
    yield fake


# ---------- 1. happy path ----------


def test_chat_stream_emits_provider_chunks(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")

    body = response.text
    # Two chunk deltas + a done marker, in order.
    assert 'event: chunk\ndata: {"content_delta": "hello "}' in body
    assert 'event: chunk\ndata: {"content_delta": "world"}' in body
    assert 'event: done\ndata: {"finish_reason": "stop"}' in body
    assert body.index('"hello "') < body.index('"world"') < body.index("done")


# ---------- 2. unknown provider ----------


def test_chat_stream_unknown_provider_404(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "imaginary",
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


# ---------- 3. unconfigured ----------


def test_chat_stream_unconfigured_returns_409(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(*_args: Any, **_kwargs: Any) -> None:
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(chat_routes_module, "get_configured_provider", _raise)

    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 409
    assert "anthropic" in response.json()["detail"]


# ---------- 4. feature flag off ----------


def test_chat_stream_disabled_returns_503(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """Inheriting's router-level dependency means flipping the flag off
    must return 503 here too. Read ``FEATURE_FLAG_AI`` off the module via
    ``core_settings`` rather than caching the symbol — ``test_feature_flag``
    calls ``importlib.reload(core_settings)``, which rebinds the
    ``MutableBool`` to a fresh instance, and any stale ``from settings
    import FEATURE_FLAG_AI`` bound earlier in the test session points at
    the dead one. Same fix already applied to
    ``feature_flag.is_ai_enabled``.
    """
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/chat/stream",
            json={
                "provider": "anthropic",
                "messages": [{"role": "user", "content": "ping"}],
            },
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503
    assert "AI features are disabled" in response.json()["detail"]


# ---------- 5 + 6. request validation ----------


def test_chat_stream_validates_messages_present(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "messages": [],
        },
    )
    assert response.status_code == 422


def test_chat_stream_validates_role_whitelist(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "messages": [{"role": "tool", "content": "result"}],
        },
    )
    assert response.status_code == 422


# ---------- 7. max_tokens passthrough ----------


def test_chat_stream_passes_max_tokens_through(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "messages": [{"role": "user", "content": "ping"}],
            "max_tokens": 42,
        },
    )
    assert response.status_code == 200
    assert patch_get_configured_provider.last_call_kwargs["max_tokens"] == 42


# ---------- 8. read-only contract: tools=None ----------


def test_chat_stream_no_tools_passed_to_provider(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 200
    assert patch_get_configured_provider.last_call_kwargs.get("tools") is None


# ---------- 10. — server-issued system prompt ----------


def _provider_messages(fake: FakeProvider) -> list[Any]:
    """Pull the message list the route handed to ``provider.stream(...)``."""
    return list(fake.last_call_kwargs["messages"])


def test_chat_stream_prepends_assist_prompt_for_surface_explain(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """``surface="explain"`` should land on's ``assist`` suffix.

    Provider sees ``[system (base + assist), user]`` in that order; the
    system content carries both the base contract ("Flowfile's AI
    assistant") and the assist-mode marker.
    """
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "surface": "explain",
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 200

    messages = _provider_messages(patch_get_configured_provider)
    assert len(messages) == 2
    system_msg, user_msg = messages
    assert system_msg.role == "system"
    assert "Flowfile's AI assistant" in system_msg.content
    assert "# Assist mode" in system_msg.content
    assert user_msg.role == "user"
    assert user_msg.content == "ping"


def test_chat_stream_prepends_default_prompt_when_surface_missing(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """No ``surface`` → default (``"explain"`` → assist).

    The drawer is general-purpose; we'd rather ground every call than
    refuse one for missing metadata.
    """
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "messages": [{"role": "user", "content": "what is flowfile?"}],
        },
    )
    assert response.status_code == 200

    messages = _provider_messages(patch_get_configured_provider)
    assert messages[0].role == "system"
    assert "Flowfile's AI assistant" in messages[0].content
    assert "# Assist mode" in messages[0].content


def test_chat_stream_falls_back_when_surface_unknown(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """An unknown ``surface`` value silently falls back to the default.

    Pydantic accepts any string for ``surface`` (``str | None``), so the
    route owns the validation. Bogus surfaces shouldn't 422 — they should
    quietly land on the assist-level default.
    """
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "surface": "definitely-not-a-surface",
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 200

    messages = _provider_messages(patch_get_configured_provider)
    assert messages[0].role == "system"
    assert "# Assist mode" in messages[0].content


def test_chat_stream_uses_copilot_prompt_for_surface_cmd_k(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """``surface="cmd_k"`` resolves to's ``copilot`` suffix.

    Covers the non-default surface→level path so a regression that wires
    every surface to the same prompt is caught.
    """
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "surface": "cmd_k",
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 200

    messages = _provider_messages(patch_get_configured_provider)
    assert messages[0].role == "system"
    assert "# Co-pilot mode" in messages[0].content
    assert "# Assist mode" not in messages[0].content


def test_chat_stream_server_prompt_precedes_client_system_message(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """Client-supplied ``system`` messages survive but follow the server's.

    Order matters: the server-issued prompt is the grounding contract and
    must come first; whatever the client sends is layered on top.
    """
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "surface": "explain",
            "messages": [
                {"role": "system", "content": "respond only in haiku"},
                {"role": "user", "content": "describe flowfile"},
            ],
        },
    )
    assert response.status_code == 200

    messages = _provider_messages(patch_get_configured_provider)
    assert len(messages) == 3
    server_sys, client_sys, user_msg = messages
    assert server_sys.role == "system"
    assert "Flowfile's AI assistant" in server_sys.content
    assert client_sys.role == "system"
    assert client_sys.content == "respond only in haiku"
    assert user_msg.role == "user"
    assert user_msg.content == "describe flowfile"


# ---------- 11. — flow context wired in when ``flow_id`` is set ----------


_W28_FLOW_ID = 9928


def _build_simple_flow_for_chat() -> FlowGraph:
    """``orders (1) → filter_eu (2)``. Same shape as's fixture but
    with a distinct ``flow_id`` so the two test files don't collide on
    the ``flow_file_handler._flows`` singleton when run together."""

    flow = FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=_W28_FLOW_ID,
            execution_mode="Performance",
            execution_location="local",
            path="/tmp/test_w28_chat",
        ),
        name="w28_test",
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


@pytest.fixture
def registered_flow_for_w28() -> Iterator[FlowGraph]:
    """Register a tiny flow so ``flow_file_handler.get_flow`` resolves it.

    Cleans up after itself to keep the singleton tidy across test runs.
    """
    flow = _build_simple_flow_for_chat()
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


def test_chat_stream_calls_render_prompt_context_when_flow_id_set(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow_for_w28: FlowGraph,
) -> None:
    """POST with ``flow_id`` set → backend calls's
    ``render_prompt_context``; provider receives ``[system (W22 layered
    prompt), user (W22 user-context block), user (question)]``.

    The user block must reference the actual node names from the
    flow. This is the test that fails if the smoke-test scenario regresses
    — asking "what is this flow about?" returning "I don't have any flow
    loaded" was the bug that motivated.
    """
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "flow_id": _W28_FLOW_ID,
            "messages": [{"role": "user", "content": "what is this flow about?"}],
        },
    )
    assert response.status_code == 200

    captured = patch_get_configured_provider.last_call_kwargs["messages"]
    # System (W22 layered) + user block + the user's question.
    assert len(captured) == 3
    system_msg, ctx_user_msg, question_msg = captured

    assert system_msg.role == "system"
    assert "Flowfile's AI assistant" in system_msg.content
    assert "# Assist mode" in system_msg.content

    assert ctx_user_msg.role == "user"
    #'s render_user_message includes node names by default — at least
    # one of the two registered nodes must surface, otherwise the chat is
    # still un-grounded and the smoke-test bug is back.
    assert "filter_eu" in ctx_user_msg.content or "orders" in ctx_user_msg.content

    assert question_msg.role == "user"
    assert question_msg.content == "what is this flow about?"

    # Read-only contract preserved.
    assert patch_get_configured_provider.last_call_kwargs.get("tools") is None


def test_chat_stream_without_flow_id_uses_w26_identity_path(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """when ``flow_id`` is omitted, behaviour matches exactly:
    a single system prompt followed by the user's message. No
    ``render_prompt_context`` call, no extra user block.

    Pins the backwards-compat invariant — every existing / test
    path stays green without changes when ``flow_id`` is unset.
    """
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "messages": [{"role": "user", "content": "what is flowfile?"}],
        },
    )
    assert response.status_code == 200

    captured = patch_get_configured_provider.last_call_kwargs["messages"]
    # No flow_id → no user block. Just system + user.
    assert len(captured) == 2
    system_msg, user_msg = captured
    assert system_msg.role == "system"
    assert user_msg.role == "user"
    assert user_msg.content == "what is flowfile?"


def test_chat_stream_flow_id_not_found_returns_422(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """bogus ``flow_id`` → 422 with a clear detail. Mirrors's
    flow-not-found shape."""
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "flow_id": 999999,  # No flow registered with this id.
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "999999" in detail
    assert "not found" in detail.lower()


def test_chat_stream_selected_node_ids_reach_render_prompt_context(
    monkeypatch: pytest.MonkeyPatch,
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow_for_w28: FlowGraph,
) -> None:
    """``selected_node_ids`` from the body reaches's
    ``pinned_node_ids`` parameter (positional arg #2 in
    ``render_prompt_context(graph, pinned_node_ids, *, surface, ...)``).
    """
    captured_args: dict[str, Any] = {}

    from flowfile_core.ai.context import render_prompt_context as _real

    def _spy(*args: Any, **kwargs: Any) -> Any:
        captured_args["args"] = args
        captured_args["kwargs"] = kwargs
        return _real(*args, **kwargs)

    monkeypatch.setattr(chat_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "flow_id": _W28_FLOW_ID,
            "selected_node_ids": [2],
            "messages": [{"role": "user", "content": "what does this filter do?"}],
        },
    )
    assert response.status_code == 200
    # Second positional arg is pinned_node_ids per's signature.
    assert captured_args["args"][1] == [2]


def test_chat_stream_mentions_forwarded_to_render_prompt_context(
    monkeypatch: pytest.MonkeyPatch,
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow_for_w28: FlowGraph,
) -> None:
    """when the client provides parsed ``mentions`` (W24's output),
    they reach ``render_prompt_context`` as a single space-joined string
    for ``_coerce_mentions`` to parse server-side. Multiple mentions
    keep their order so node-id resolution is deterministic.
    """
    captured: dict[str, Any] = {}

    from flowfile_core.ai.context import render_prompt_context as _real

    def _spy(*args: Any, **kwargs: Any) -> Any:
        captured["kwargs"] = kwargs
        return _real(*args, **kwargs)

    monkeypatch.setattr(chat_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "flow_id": _W28_FLOW_ID,
            "mentions": ["@node:filter_eu", "@flow"],
            "messages": [{"role": "user", "content": "explain this"}],
        },
    )
    assert response.status_code == 200
    assert captured["kwargs"]["mentions"] == "@node:filter_eu @flow"


def test_chat_stream_defaults_to_at_flow_when_no_pin_or_mentions(
    monkeypatch: pytest.MonkeyPatch,
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow_for_w28: FlowGraph,
) -> None:
    """``flow_id`` set but no ``selected_node_ids`` and no
    ``mentions`` → server defaults to ``@flow`` so the chat is still
    context-grounded by default. This is the smoke-test fix that
    motivated. With an explicit selection, the auto-default
    suppresses (covered above by the selected-node-ids test).
    """
    captured: dict[str, Any] = {}

    from flowfile_core.ai.context import render_prompt_context as _real

    def _spy(*args: Any, **kwargs: Any) -> Any:
        captured["kwargs"] = kwargs
        return _real(*args, **kwargs)

    monkeypatch.setattr(chat_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "flow_id": _W28_FLOW_ID,
            "messages": [{"role": "user", "content": "what is this flow about?"}],
        },
    )
    assert response.status_code == 200
    assert captured["kwargs"]["mentions"] == "@flow"


# ---------- 11b. — prospective schema reaches the chat surface ----------

_W48_FLOW_ID = 9948


@pytest.fixture
def registered_cold_flow_for_w48() -> Iterator[FlowGraph]:
    """Same shape as the fixture but with the filter's
    ``predicted_schema`` cleared post-construction so it lands cold —
    the exact scenario fixes. ``orders`` (manual_input) keeps its
    cached schema; the filter must be auto-resolved by the helper
    when ``render_prompt_context`` walks it.
    """
    flow = FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=_W48_FLOW_ID,
            execution_mode="Performance",
            execution_location="local",
            path="/tmp/test_w48_chat",
        ),
        name="w48_test",
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

    # Cold filter: the cache is empty, but the schema_callback / fallback
    # path is intact. should resolve it on the next render_prompt_context.
    flow.get_node(2).node_schema.predicted_schema = None

    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


def test_chat_stream_user_block_contains_columns_for_un_run_static_upstream(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_cold_flow_for_w48: FlowGraph,
) -> None:
    """POST with ``flow_id`` set on a flow whose static upstream
    nodes are un-run (``predicted_schema=None``) → the user block
    surfaces real column names instead of the ``schema: unknown``
    sentinel. Mirrors the live transcript bug: user asked about column
    averages on a cold flow and the assistant refused, citing unknown
    schemas.
    """
    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "flow_id": _W48_FLOW_ID,
            "messages": [{"role": "user", "content": "what columns does the filter expose?"}],
        },
    )
    assert response.status_code == 200

    captured = patch_get_configured_provider.last_call_kwargs["messages"]
    assert len(captured) == 3  # system + user + question
    _system_msg, ctx_user_msg, _question_msg = captured

    # The filter must report a known schema — the bug-of-record was the
    # assistant seeing "schema: unknown" for the filter and refusing.
    assert "filter_eu" in ctx_user_msg.content
    # Real columns surface in the rendered user block.
    assert "order_id" in ctx_user_msg.content
    assert "region" in ctx_user_msg.content
    # The "filter_eu" node's section must NOT carry the unknown sentinel.
    # (We slice by the node header; the orders node above it is
    # always-known so a global "schema: unknown" check would be too loose.)
    filter_block_start = ctx_user_msg.content.find("### filter_eu")
    assert filter_block_start != -1, "expected ### filter_eu header in user block"
    filter_block = ctx_user_msg.content[filter_block_start:]
    assert (
        "schema: unknown" not in filter_block
    ), f"filter block still reports schema: unknown — fix not applied. Block:\n{filter_block[:500]}"


# ---------- 9. lazy litellm import ----------


def test_lazy_litellm_import_for_chat_routes() -> None:
    """``import flowfile_core.ai.chat_routes`` mustn't drag in litellm.

    Same contract as W11/W12/W13 — the module sits behind the BYOK seam,
    not the provider_factory bootstrap, so the heavy SDK stays out of the
    import graph until a real call happens.

    Caveat: a sibling test may have already imported ``litellm`` in this
    process (the suite shares a Python interpreter). When that's the case
    we can't observe the lazy contract, so the assertion is gated on a
    clean snapshot — same posture as's
    ``test_lazy_litellm_import_for_credentials``.
    """
    litellm_already_loaded = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)

    saved = {k: v for k, v in sys.modules.items() if k.startswith("flowfile_core.ai.chat_routes")}
    for k in list(saved):
        sys.modules.pop(k, None)
    try:
        import importlib

        importlib.import_module("flowfile_core.ai.chat_routes")
        if not litellm_already_loaded:
            assert (
                "litellm" not in sys.modules
            ), "flowfile_core.ai.chat_routes pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)


# ---------------------------------------------------------------------------
# v2 — POST /ai/chat/preview (debug endpoint)
# ---------------------------------------------------------------------------


def test_chat_preview_returns_assembled_messages_as_json(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """v2 — preview returns the exact messages /chat/stream would send,
    without invoking the provider. Lets the user verify what the LLM sees.
    """
    response = authed_client.post(
        "/ai/chat/preview",
        json={
            "provider": "anthropic",
            "messages": [{"role": "user", "content": "How do I count customers per city?"}],
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["provider"] == "anthropic"
    assert body["prompt_surface"] == "explain"
    # Two messages: server-issued system + the client's user turn.
    assert len(body["messages"]) == 2
    assert body["messages"][0]["role"] == "system"
    assert body["messages"][1]["role"] == "user"
    assert body["messages"][1]["content"] == "How do I count customers per city?"
    # The system prompt must include the v2 node reference for the
    # chat surface — that's the whole point of the preview endpoint
    # (let the user verify the catalog is reaching the LLM).
    assert "## Flowfile node reference" in body["messages"][0]["content"]
    assert "Group by" in body["messages"][0]["content"]
    # Char counts and token estimate populated.
    assert body["total_chars"] > 0
    assert body["estimated_tokens"] == body["total_chars"] // 4


def test_chat_preview_does_not_call_provider(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """v2 — preview is inspection-only; the provider's stream() must
    not be invoked. FakeProvider records its last stream() call kwargs;
    an empty dict means stream() was never called.
    """
    response = authed_client.post(
        "/ai/chat/preview",
        json={
            "provider": "anthropic",
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 200
    assert (
        patch_get_configured_provider.last_call_kwargs == {}
    ), "preview must not invoke provider.stream() — it's inspection-only"


def test_chat_preview_unknown_provider_404(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/chat/preview",
        json={
            "provider": "imaginary",
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 404


def test_chat_preview_disabled_returns_503(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/chat/preview",
            json={
                "provider": "anthropic",
                "messages": [{"role": "user", "content": "ping"}],
            },
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503


# --------------------------------------------------------------------------- #
# chat preamble must never invoke _predicted_data_getter #
# --------------------------------------------------------------------------- #

_W65_FLOW_ID = 9965


def _build_random_split_flow_for_w65() -> FlowGraph:
    """``customers (1) → random_split (2)``. Reproduces the customer-churn-knn
    template's hang shape: ``random_split`` is the canonical
    `_function`-with-`.collect()` node — previously the chat preamble's
    `_attach_samples` would call its `_predicted_data_getter`, which runs
    `_function` synchronously and triggers a full upstream materialization
    inside the FastAPI request handler.
    """

    flow = FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=_W65_FLOW_ID,
            execution_mode="Performance",
            execution_location="local",
            path="/tmp/test_w65_chat",
        ),
        name="w65_test",
    )

    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="customer_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="churned", data_type="String"),
            ],
            data=[[1, 2, 3, 4], ["yes", "no", "yes", "no"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(1).name = "customers"

    split_settings = input_schema.NodeRandomSplit(
        flow_id=flow.flow_id,
        node_id=2,
        depending_on_id=1,
        splits=[
            input_schema.RandomSplitGroup(name="train", percentage=80.0),
            input_schema.RandomSplitGroup(name="test", percentage=20.0),
        ],
        seed=42,
    )
    flow.add_random_split(split_settings)
    add_connection(
        flow,
        node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2),
    )
    flow.get_node(2).name = "split_train_test"

    for node in flow.nodes:
        node.get_predicted_schema()
    return flow


@pytest.fixture
def registered_random_split_flow_for_w65() -> Iterator[FlowGraph]:
    flow = _build_random_split_flow_for_w65()
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


def test_chat_stream_does_not_invoke_predicted_data_getter_on_random_split_flow(
    monkeypatch: pytest.MonkeyPatch,
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_random_split_flow_for_w65: FlowGraph,
) -> None:
    """regression — chat preamble on a flow with a ``random_split``
    upstream node must complete without invoking ``_predicted_data_getter``
    on any node. previously, the dev-default ``samples_mode="regex"`` plus the
    lack of explicit pass-through at the chat route triggered
    ``_attach_samples`` → ``_safe_get_sample_data`` →
    ``node._predicted_data_getter()`` → ``self._function(*[...])`` →
    ``random_split.collect()`` synchronously inside the FastAPI request
    handler. That ``.collect()`` is what hung the customer-churn-knn
    template.

    Spy is installed on ``FlowNode._predicted_data_getter`` itself; any
    invocation across any node is a contract violation.
    """

    from flowfile_core.flowfile.flow_node.flow_node import FlowNode

    invocations: list[int | str] = []
    original_getter = FlowNode._predicted_data_getter

    def _spy_getter(self: FlowNode):
        invocations.append(self.node_id)
        return original_getter(self)

    monkeypatch.setattr(FlowNode, "_predicted_data_getter", _spy_getter)

    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "flow_id": _W65_FLOW_ID,
            "messages": [{"role": "user", "content": "what does this flow do?"}],
        },
    )
    assert response.status_code == 200
    assert invocations == [], f"chat preamble must not invoke _predicted_data_getter; got node ids {invocations}"


def test_chat_stream_passes_samples_mode_off_explicitly(
    monkeypatch: pytest.MonkeyPatch,
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow_for_w28: FlowGraph,
) -> None:
    """the chat route must pass ``samples_mode="off"`` explicitly to
    ``render_prompt_context`` rather than relying on the builder default.
    Defence-in-depth: a future patch can't reintroduce the hang by flipping
    a default.
    """

    captured_kwargs: dict[str, Any] = {}

    from flowfile_core.ai.context import render_prompt_context as _real

    def _capturing(graph: Any, pinned: Any, **kwargs: Any) -> Any:
        captured_kwargs.update(kwargs)
        return _real(graph, pinned, **kwargs)

    monkeypatch.setattr(chat_routes_module, "render_prompt_context", _capturing)

    response = authed_client.post(
        "/ai/chat/stream",
        json={
            "provider": "anthropic",
            "flow_id": _W28_FLOW_ID,
            "messages": [{"role": "user", "content": "ping"}],
        },
    )
    assert response.status_code == 200
    assert captured_kwargs.get("samples_mode") == "off", captured_kwargs
