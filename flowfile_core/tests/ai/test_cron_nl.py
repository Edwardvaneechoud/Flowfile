"""Tests for natural-language → cron generation (logic, routes, lockstep)."""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
from collections.abc import Iterator
from typing import Any, get_args

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import cron_nl as cron_mod
from flowfile_core.ai import cron_routes
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.cron_nl import generate_cron_expression
from flowfile_core.ai.providers.base import ChatResponse, Usage
from flowfile_core.ai.scheduler import RateLimitScheduler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings

# --------------------------------------------------------------------------- #
# Fakes #
# --------------------------------------------------------------------------- #


class _FakeProvider:
    """Provider stub for chat-only (non-stream) call paths. Records the kwargs
    ``chat()`` was called with so tests can assert ``response_format`` / ``tools``."""

    name: str = "fake"
    model: str = "fake-default"
    supports_tools: bool = True
    supports_streaming: bool = True

    def __init__(
        self,
        *,
        content: str | None = None,
        sleep_before_response: float = 0.0,
        raise_exc: BaseException | None = None,
    ) -> None:
        self._content = content
        self._sleep = sleep_before_response
        self._raise = raise_exc
        self.last_call_kwargs: dict[str, Any] = {}

    async def chat(
        self,
        messages: list[Any],
        tools: list[Any] | None = None,
        max_tokens: int | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> ChatResponse:
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
            tool_calls=[],
            finish_reason="stop",
            usage=Usage(),
        )

    def stream(self, *_a: Any, **_kw: Any):
        raise AssertionError("stream() should not be called by cron generation")


def _scheduler() -> RateLimitScheduler:
    """A scheduler whose ``time_source`` is monotonic-zero-ish so RPM never
    blocks tests. Each test gets a fresh instance."""

    return RateLimitScheduler(time_source=lambda: 0.0, sleep=lambda *_a, **_k: asyncio.sleep(0))


def _payload(cron_expression: str, explanation: str | None = None) -> str:
    obj: dict[str, Any] = {"cron_expression": cron_expression}
    if explanation is not None:
        obj["explanation"] = explanation
    return json.dumps(obj)


# --------------------------------------------------------------------------- #
# 1. Surface vocabulary lockstep #
# --------------------------------------------------------------------------- #


def test_cron_surface_in_lockstep() -> None:
    """``"cron"`` registered across SurfaceLiteral × SURFACE_PRESETS ×
    SURFACE_TO_LEVEL × every provider's ``surface_models``."""
    from flowfile_core.ai.context import builder as ctx_builder
    from flowfile_core.ai.providers import (
        AnthropicProvider,
        GoogleProvider,
        GroqProvider,
        OllamaProvider,
        OpenAIProvider,
        OpenRouterProvider,
    )
    from flowfile_core.ai.tools import registry as tool_registry

    assert cron_mod.SURFACE == "cron"

    # 1. Both SurfaceLiteral definitions + SURFACE_TO_LEVEL.
    assert "cron" in get_args(tool_registry.SurfaceLiteral)
    assert "cron" in get_args(ctx_builder.SurfaceLiteral)
    assert ctx_builder.SURFACE_TO_LEVEL["cron"] == "copilot"

    # 2. SURFACE_PRESETS carries the empty frozenset; coverage check passes.
    assert tool_registry.SURFACE_PRESETS["cron"] == frozenset()
    tool_registry._check_preset_coverage()  # must not raise
    assert tool_registry.build_tool_catalog(surface="cron") == []  # no tools surfaced

    # 3. Every provider routes the surface to a model.
    for provider_cls in (
        AnthropicProvider,
        OpenAIProvider,
        GoogleProvider,
        GroqProvider,
        OpenRouterProvider,
        OllamaProvider,
    ):
        assert "cron" in provider_cls.surface_models, f"{provider_cls.__name__} missing cron in surface_models"


# --------------------------------------------------------------------------- #
# 2. Generation logic #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_generate_happy_path() -> None:
    provider = _FakeProvider(content=_payload("0 9 * * 1-5", "At 09:00, Monday through Friday"))
    resp = await generate_cron_expression(
        "every weekday at 9am",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert resp.degraded is False
    assert resp.cron_expression == "0 9 * * 1-5"
    assert resp.explanation == "At 09:00, Monday through Friday"
    assert resp.reason is None


@pytest.mark.asyncio
async def test_generate_strips_surrounding_whitespace() -> None:
    provider = _FakeProvider(content=_payload("  0 9 * * *  "))
    resp = await generate_cron_expression("daily at 9am", provider=provider, scheduler=_scheduler())
    assert resp.degraded is False
    assert resp.cron_expression == "0 9 * * *"


@pytest.mark.asyncio
async def test_generate_invalid_cron() -> None:
    # 4-field output (missing day-of-week) — a realistic LLM slip the seam
    # validator must catch before it reaches the frontend.
    provider = _FakeProvider(content=_payload("0 9 * *"))
    resp = await generate_cron_expression("weird schedule", provider=provider, scheduler=_scheduler())
    assert resp.degraded is True
    assert resp.reason == "invalid_cron"
    assert resp.cron_expression is None


@pytest.mark.asyncio
async def test_generate_no_expression() -> None:
    provider = _FakeProvider(content=_payload("", "Not a schedule description"))
    resp = await generate_cron_expression("hello there", provider=provider, scheduler=_scheduler())
    assert resp.degraded is True
    assert resp.reason == "no_expression"
    assert resp.cron_expression is None
    # The model's explanation is forwarded so the UI can say *why*.
    assert resp.explanation == "Not a schedule description"


@pytest.mark.asyncio
async def test_generate_empty_description_short_circuits() -> None:
    provider = _FakeProvider(content=_payload("0 9 * * *"))
    resp = await generate_cron_expression("   ", provider=provider, scheduler=_scheduler())
    assert resp.degraded is True
    assert resp.reason == "empty_description"
    # The provider is never consulted for a blank description.
    assert provider.last_call_kwargs == {}


@pytest.mark.asyncio
async def test_generate_parse_error() -> None:
    provider = _FakeProvider(content="not json at all")
    resp = await generate_cron_expression("daily", provider=provider, scheduler=_scheduler())
    assert resp.degraded is True
    assert resp.reason == "parse_error"


@pytest.mark.asyncio
async def test_generate_validation_error() -> None:
    # Well-formed JSON but missing the required ``cron_expression`` field.
    provider = _FakeProvider(content=json.dumps({"explanation": "no expression field"}))
    resp = await generate_cron_expression("daily", provider=provider, scheduler=_scheduler())
    assert resp.degraded is True
    assert resp.reason == "validation_error"


@pytest.mark.asyncio
async def test_generate_provider_error() -> None:
    provider = _FakeProvider(raise_exc=RuntimeError("boom"))
    resp = await generate_cron_expression("daily", provider=provider, scheduler=_scheduler())
    assert resp.degraded is True
    assert resp.reason == "provider_error"


@pytest.mark.asyncio
async def test_generate_timeout() -> None:
    provider = _FakeProvider(content=_payload("0 9 * * *"), sleep_before_response=2.0)
    resp = await generate_cron_expression(
        "daily at 9",
        provider=provider,
        scheduler=_scheduler(),
        timeout=0.05,
    )
    assert resp.degraded is True
    assert resp.reason == "timeout"


@pytest.mark.asyncio
async def test_response_format_threaded_through() -> None:
    provider = _FakeProvider(content=_payload("0 9 * * *"))
    await generate_cron_expression("daily at 9am", provider=provider, scheduler=_scheduler())
    assert provider.last_call_kwargs.get("response_format") == {"type": "json_object"}
    assert provider.last_call_kwargs.get("tools") is None


@pytest.mark.asyncio
async def test_markdown_fenced_json_is_unwrapped() -> None:
    fenced = "```json\n" + _payload("*/15 * * * *", "Every 15 minutes") + "\n```"
    provider = _FakeProvider(content=fenced)
    resp = await generate_cron_expression("every 15 minutes", provider=provider, scheduler=_scheduler())
    assert resp.degraded is False
    assert resp.cron_expression == "*/15 * * * *"


# --------------------------------------------------------------------------- #
# 3. Routes #
# --------------------------------------------------------------------------- #


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    fake_user = PydanticUser(id=1, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def test_route_generate_cron_happy_path(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _FakeProvider(content=_payload("0 9 * * 1-5", "At 09:00, Monday through Friday"))
    monkeypatch.setattr(cron_routes, "get_configured_provider", lambda *_a, **_kw: provider)

    response = authed_client.post(
        "/ai/generate_cron",
        json={"description": "every weekday at 9am", "provider": "anthropic"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["degraded"] is False
    assert body["cron_expression"] == "0 9 * * 1-5"
    assert body["explanation"] == "At 09:00, Monday through Friday"


def test_route_generate_cron_404_on_unknown_provider(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/generate_cron",
        json={"description": "daily at 9am", "provider": "imaginary"},
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


def test_route_generate_cron_409_on_unconfigured_provider(
    authed_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(*_a: Any, **_kw: Any):
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(cron_routes, "get_configured_provider", _raise)
    response = authed_client.post(
        "/ai/generate_cron",
        json={"description": "daily at 9am", "provider": "anthropic"},
    )
    assert response.status_code == 409


def test_route_generate_cron_422_on_blank_description(authed_client: TestClient) -> None:
    response = authed_client.post("/ai/generate_cron", json={"description": ""})
    assert response.status_code == 422


def test_route_generate_cron_503_when_flag_off(authed_client: TestClient) -> None:
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post("/ai/generate_cron", json={"description": "daily at 9am"})
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503


# --------------------------------------------------------------------------- #
# 4. Lazy-litellm contract #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract() -> None:
    """``import flowfile_core.ai.cron_nl`` mustn't drag in litellm."""
    litellm_already_loaded = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)

    saved = {k: v for k, v in sys.modules.items() if k.startswith("flowfile_core.ai.cron_nl")}
    for k in list(saved):
        sys.modules.pop(k, None)
    try:
        importlib.import_module("flowfile_core.ai.cron_nl")
        if not litellm_already_loaded:
            assert (
                "litellm" not in sys.modules
            ), "flowfile_core.ai.cron_nl pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)
        sys.modules["flowfile_core.ai.cron_nl"] = cron_mod
