"""settings autocomplete tests.

Cases:

* ``test_settings_autocomplete_surface_in_lockstep`` — six provider files
  carry the surface; ``SURFACE_TO_LEVEL`` maps it; ``SURFACE_PRESETS`` has
  it as empty frozenset; ``_check_preset_coverage`` doesn't raise at import.
* ``test_suggest_join_keys_filters_unmatched_columns`` — bogus left or right
  ref → drop. Valid pair survives sorted by confidence.
* ``test_suggest_join_keys_degrades_when_either_schema_none`` — either side
  cold → ``degraded=True``.
* ``test_response_format_threaded_through`` — provider receives
  ``response_format={"type":"json_object"}``.
* ``test_timeout_yields_degraded`` — provider exceeds the configured
  timeout → ``degraded=True, reason="timeout"``.
* ``test_provider_error_yields_degraded`` / ``test_parse_error_yields_degraded``
  / ``test_markdown_fenced_json_is_unwrapped`` — shared provider-call wiring.
* ``test_route_join_keys_404_on_missing_flow`` /
  ``test_route_join_keys_404_on_unknown_provider`` /
  ``test_route_join_keys_409_on_unconfigured_provider`` /
  ``test_route_join_keys_503_when_flag_off`` /
  ``test_route_join_keys_validation_error``.
* ``test_route_join_keys_happy_path`` — full integration with a monkeypatched
  provider.
* ``test_lazy_litellm_contract`` — importing ``flowfile_core.ai.autocomplete``
  must not pull ``litellm``.
"""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import autocomplete as autocomplete_mod
from flowfile_core.ai import autocomplete_routes
from flowfile_core.ai.autocomplete import suggest_join_keys
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.providers.base import ChatResponse, Usage
from flowfile_core.ai.scheduler import RateLimitScheduler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings

# Fakes


class _FakeColumn:
    """Minimal stand-in for ``FlowfileColumn`` — autocomplete only reads
    ``column_name``."""

    def __init__(self, name: str) -> None:
        self.column_name = name


def _columns(*names: str) -> list[_FakeColumn]:
    return [_FakeColumn(n) for n in names]


def _make_node(node_id: int | str, *, predicted_schema: list[Any] | None, all_inputs: list[Any] | None = None) -> Any:
    schema = SimpleNamespace(predicted_schema=predicted_schema)
    node = SimpleNamespace(
        node_id=node_id,
        node_schema=schema,
        all_inputs=all_inputs or [],
    )
    return node


class _FakeGraph:
    """Bare ``graph.get_node()`` shim — sufficient for the autocomplete API."""

    def __init__(self, nodes: dict[int | str, Any]) -> None:
        self._nodes = nodes

    def get_node(self, node_id: int | str | None = None):
        return self._nodes.get(node_id)


class _FakeProvider:
    """Provider stub for chat-only (non-stream) call paths.

    Records the kwargs the autocomplete module called ``chat()`` with so
    tests can assert ``response_format`` plumbing, ``tools=None``, etc.
    """

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
        raise AssertionError("stream() should not be called by autocomplete")


def _scheduler() -> RateLimitScheduler:
    """
    Create a rate-limit scheduler configured for testing.
    
    Returns:
        RateLimitScheduler: A scheduler with a constant time source and no-op sleep,
            preventing rate-limit enforcement from blocking test execution.
    """

    return RateLimitScheduler(time_source=lambda: 0.0, sleep=lambda *_a, **_k: asyncio.sleep(0))


def _join_payload(pairs: list[dict[str, Any]]) -> str:
    """
    Serialize key-pair candidates to JSON.
    
    Parameters:
    	pairs (list[dict[str, Any]]): Key-pair candidate dictionaries.
    
    Returns:
    	str: JSON string containing the key pairs under the "key_pairs" key.
    """
    return json.dumps({"key_pairs": pairs})


def _matched_graph() -> _FakeGraph:
    """
    Create a test graph with two nodes whose predicted schemas each contain columns "a" and "b".
    
    Returns:
    	_FakeGraph: A graph containing nodes "L" and "R" with matching overlapping schemas.
    """

    left = _make_node("L", predicted_schema=_columns("a", "b"))
    right = _make_node("R", predicted_schema=_columns("a", "b"))
    return _FakeGraph({"L": left, "R": right})


# 1. Surface vocabulary lockstep


def test_settings_autocomplete_surface_in_lockstep() -> None:
    """Triple-locked: SurfaceLiteral × SURFACE_PRESETS × SURFACE_TO_LEVEL ×
    every provider's ``surface_models``."""
    from flowfile_core.ai.context import budget as ctx_budget
    from flowfile_core.ai.context import builder as ctx_builder
    from flowfile_core.ai.tools import registry as tool_registry

    assert "settings_autocomplete" in tool_registry.get_args(tool_registry.SurfaceLiteral)
    assert "settings_autocomplete" in ctx_builder.SURFACE_TO_LEVEL
    assert ctx_builder.SURFACE_TO_LEVEL["settings_autocomplete"] == "copilot"

    assert tool_registry.SURFACE_PRESETS["settings_autocomplete"] == frozenset()

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
        assert (
            "settings_autocomplete" in provider_cls.surface_models
        ), f"{provider_cls.__name__} missing settings_autocomplete in surface_models"

    auto_budget = ctx_budget.surface_budget("settings_autocomplete")
    cmd_k_budget = ctx_budget.surface_budget("cmd_k")
    assert auto_budget[0] <= cmd_k_budget[0]
    assert auto_budget[1] <= cmd_k_budget[1]


# 2. Join keys: filtering + degraded


@pytest.mark.asyncio
async def test_suggest_join_keys_filters_unmatched_columns() -> None:
    left = _make_node("L", predicted_schema=_columns("user_id", "name"))
    right = _make_node("R", predicted_schema=_columns("user_id", "email"))
    graph = _FakeGraph({"L": left, "R": right})

    provider = _FakeProvider(
        content=_join_payload(
            [
                {"left_col": "user_id", "right_col": "user_id", "confidence": 0.95},
                # bogus left column → should be dropped
                {"left_col": "phantom", "right_col": "user_id", "confidence": 0.4},
                # bogus right column → should be dropped
                {"left_col": "name", "right_col": "phantom", "confidence": 0.3},
                # plausible secondary
                {"left_col": "name", "right_col": "email", "confidence": 0.6},
            ]
        )
    )
    response = await suggest_join_keys(
        graph,
        "L",
        "R",
        how="inner",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert response.degraded is False
    assert len(response.key_pairs) == 2
    # Sorted by confidence descending.
    assert response.key_pairs[0].left_col == "user_id"
    assert response.key_pairs[0].right_col == "user_id"
    assert response.key_pairs[1].left_col == "name"


@pytest.mark.asyncio
async def test_suggest_join_keys_degrades_when_either_schema_none() -> None:
    left = _make_node("L", predicted_schema=None)
    right = _make_node("R", predicted_schema=_columns("user_id", "email"))
    graph = _FakeGraph({"L": left, "R": right})

    provider = _FakeProvider(content=_join_payload([]))
    response = await suggest_join_keys(
        graph,
        "L",
        "R",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert response.degraded is True
    assert response.key_pairs == []
    assert provider.last_call_kwargs == {}


# 3. Provider call wiring


@pytest.mark.asyncio
async def test_response_format_threaded_through() -> None:
    provider = _FakeProvider(content=_join_payload([]))
    await suggest_join_keys(
        _matched_graph(),
        "L",
        "R",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert provider.last_call_kwargs.get("response_format") == {"type": "json_object"}
    # Tools must be None — autocomplete is text-only.
    assert provider.last_call_kwargs.get("tools") is None


@pytest.mark.asyncio
async def test_timeout_yields_degraded() -> None:
    # Sleep much longer than the timeout; the wait_for guard should fire.
    provider = _FakeProvider(content=_join_payload([]), sleep_before_response=2.0)
    response = await suggest_join_keys(
        _matched_graph(),
        "L",
        "R",
        provider=provider,
        scheduler=_scheduler(),
        timeout=0.05,
    )
    assert response.degraded is True
    assert response.reason == "timeout"
    assert response.key_pairs == []


@pytest.mark.asyncio
async def test_provider_error_yields_degraded() -> None:
    provider = _FakeProvider(raise_exc=RuntimeError("boom"))
    response = await suggest_join_keys(
        _matched_graph(),
        "L",
        "R",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert response.degraded is True
    assert response.reason == "provider_error"


@pytest.mark.asyncio
async def test_parse_error_yields_degraded() -> None:
    provider = _FakeProvider(content="not json at all")
    response = await suggest_join_keys(
        _matched_graph(),
        "L",
        "R",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert response.degraded is True
    assert response.reason == "parse_error"


@pytest.mark.asyncio
async def test_markdown_fenced_json_is_unwrapped() -> None:
    """Some providers wrap JSON in code fences even with ``response_format`` —
    the fallback parser should accept it."""
    fenced = "```json\n" + _join_payload([{"left_col": "a", "right_col": "a", "confidence": 0.9}]) + "\n```"
    provider = _FakeProvider(content=fenced)
    response = await suggest_join_keys(
        _matched_graph(),
        "L",
        "R",
        provider=provider,
        scheduler=_scheduler(),
    )
    assert response.degraded is False
    assert len(response.key_pairs) == 1
    assert response.key_pairs[0].left_col == "a"


# 4. Routes


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    """
    Pytest fixture providing a TestClient with mocked authentication.
    
    Overrides the `get_current_active_user` dependency to always return a
    test user, and cleans up the override after the test completes.
    
    Yields:
    	TestClient: The FastAPI test client configured with mocked authentication.
    """
    fake_user = PydanticUser(id=1, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def _join_body(**overrides: Any) -> dict[str, Any]:
    """
    Build a join-keys request body with customizable field values.
    
    Returns:
    	dict[str, Any]: A request body dictionary for join-keys autocomplete tests with fields: flow_id, left_node_id, right_node_id, and how.
    """
    body: dict[str, Any] = {
        "flow_id": 1,
        "left_node_id": "L",
        "right_node_id": "R",
        "how": "inner",
    }
    body.update(overrides)
    return body


def test_route_join_keys_404_on_missing_flow(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(autocomplete_routes.flow_file_handler, "get_flow", lambda _id: None)
    response = authed_client.post(
        "/ai/autocomplete/join_keys",
        json=_join_body(flow_id=99),
    )
    assert response.status_code == 404
    assert "99" in response.json()["detail"]


def test_route_join_keys_404_on_unknown_provider(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/autocomplete/join_keys",
        json=_join_body(provider="imaginary"),
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


def test_route_join_keys_409_on_unconfigured_provider(
    authed_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        autocomplete_routes.flow_file_handler,
        "get_flow",
        lambda _id: object(),  # truthy
    )

    def _raise(*_a: Any, **_kw: Any):
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(autocomplete_routes, "get_configured_provider", _raise)

    response = authed_client.post(
        "/ai/autocomplete/join_keys",
        json=_join_body(provider="anthropic"),
    )
    assert response.status_code == 409


def test_route_join_keys_503_when_flag_off(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/autocomplete/join_keys",
            json=_join_body(),
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503
    assert "AI features are disabled" in response.json()["detail"]


def test_route_join_keys_validation_error(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/autocomplete/join_keys",
        json=_join_body(flow_id=-1),
    )
    assert response.status_code == 422


def test_route_join_keys_happy_path(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    left = _make_node("L", predicted_schema=_columns("user_id"))
    right = _make_node("R", predicted_schema=_columns("user_id"))
    graph = _FakeGraph({"L": left, "R": right})

    monkeypatch.setattr(autocomplete_routes.flow_file_handler, "get_flow", lambda _id: graph)
    provider = _FakeProvider(
        content=_join_payload([{"left_col": "user_id", "right_col": "user_id", "confidence": 0.99}])
    )
    monkeypatch.setattr(
        autocomplete_routes,
        "get_configured_provider",
        lambda *_a, **_kw: provider,
    )

    response = authed_client.post(
        "/ai/autocomplete/join_keys",
        json={
            "flow_id": 1,
            "left_node_id": "L",
            "right_node_id": "R",
            "how": "inner",
            "provider": "google",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)
    assert body["degraded"] is False
    assert len(body["key_pairs"]) == 1
    assert body["key_pairs"][0]["left_col"] == "user_id"


def test_route_formula_404_gone(authed_client: TestClient) -> None:
    """The formula autocomplete endpoint was removed; POSTing to it 404s."""
    response = authed_client.post(
        "/ai/autocomplete/formula",
        json={"flow_id": 1, "node_id": "L"},
    )
    assert response.status_code == 404


# 5. Lazy-litellm contract


def test_lazy_litellm_contract() -> None:
    """``import flowfile_core.ai.autocomplete`` mustn't drag in litellm."""
    litellm_already_loaded = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)

    saved = {k: v for k, v in sys.modules.items() if k.startswith("flowfile_core.ai.autocomplete")}
    for k in list(saved):
        sys.modules.pop(k, None)
    try:
        importlib.import_module("flowfile_core.ai.autocomplete")
        if not litellm_already_loaded:
            assert (
                "litellm" not in sys.modules
            ), "flowfile_core.ai.autocomplete pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)
        # Restore the cached canonical instance — sibling tests in this file
        # have already imported the real module.
        sys.modules["flowfile_core.ai.autocomplete"] = autocomplete_mod
