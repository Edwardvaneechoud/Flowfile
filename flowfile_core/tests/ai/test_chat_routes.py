"""W20 — read-only chat-stream endpoint tests.

Cases:

* ``test_chat_stream_emits_provider_chunks`` — happy path: mock provider
  yields three content deltas + finish_reason; SSE response carries the
  matching ``event: chunk`` blocks plus ``event: done``.
* ``test_chat_stream_unknown_provider_404`` — bogus provider name returns
  404 with the supported-list detail.
* ``test_chat_stream_unconfigured_returns_409`` — no BYOK row + no env var
  + not ollama → 409 with the ``ProviderNotConfiguredError`` message.
* ``test_chat_stream_disabled_returns_503`` — ``FEATURE_FLAG_AI=False``
  short-circuits with W17's 503 (the gate inherited from ``ai_router``).
* ``test_chat_stream_validates_messages_present`` — empty ``messages``
  list returns 422 from Pydantic.
* ``test_chat_stream_validates_role_whitelist`` — ``role: "tool"`` is
  rejected at the request boundary; W20 is read-only, no tool messages.
* ``test_chat_stream_passes_max_tokens_through`` — body's ``max_tokens``
  reaches the provider stub.
* ``test_chat_stream_no_tools_passed_to_provider`` — the route passes
  ``tools=None``; tool execution is W31's job.
* ``test_lazy_litellm_import_for_chat_routes`` — importing
  ``flowfile_core.ai.chat_routes`` doesn't pull ``litellm`` into
  ``sys.modules``.

W26 cases:

* ``test_chat_stream_prepends_assist_prompt_for_surface_explain`` — when
  ``surface="explain"``, the provider sees ``[system (W22 assist), user]``.
* ``test_chat_stream_prepends_default_prompt_when_surface_missing`` — no
  ``surface`` field → falls back to the default surface (``"explain"`` →
  ``assist`` per W22's ``SURFACE_TO_LEVEL``).
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

from flowfile_core import main
from flowfile_core.ai import chat_routes as chat_routes_module
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.providers.base import StreamChunk
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings

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
    """Inheriting W17's router-level dependency means flipping the flag off
    must return 503 here too. Read ``FEATURE_FLAG_AI`` off the module via
    ``core_settings`` rather than caching the symbol — ``test_feature_flag``
    calls ``importlib.reload(core_settings)``, which rebinds the
    ``MutableBool`` to a fresh instance, and any stale ``from settings
    import FEATURE_FLAG_AI`` bound earlier in the test session points at
    the dead one. Same fix W17 already applied to
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


# ---------- 10. W26 — server-issued system prompt ----------


def _provider_messages(fake: FakeProvider) -> list[Any]:
    """Pull the message list the route handed to ``provider.stream(...)``."""
    return list(fake.last_call_kwargs["messages"])


def test_chat_stream_prepends_assist_prompt_for_surface_explain(
    authed_client: TestClient, patch_get_configured_provider: FakeProvider
) -> None:
    """``surface="explain"`` should land on W22's ``assist`` suffix.

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
    """``surface="cmd_k"`` resolves to W22's ``copilot`` suffix.

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


# ---------- 9. lazy litellm import ----------


def test_lazy_litellm_import_for_chat_routes() -> None:
    """``import flowfile_core.ai.chat_routes`` mustn't drag in litellm.

    Same contract as W11/W12/W13 — the module sits behind the BYOK seam,
    not the provider_factory bootstrap, so the heavy SDK stays out of the
    import graph until a real call happens.

    Caveat: a sibling test may have already imported ``litellm`` in this
    process (the suite shares a Python interpreter). When that's the case
    we can't observe the lazy contract, so the assertion is gated on a
    clean snapshot — same posture as W12's
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
