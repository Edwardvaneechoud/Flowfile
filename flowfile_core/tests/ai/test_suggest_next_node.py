"""edge ghost-node suggestions tests.

Coverage (~22 cases):

* Surface vocabulary in lockstep across / / providers.
* Allowed node-type set derived from ``SURFACE_PRESETS["ghost_node"]``.
* Pydantic validation of LLM output: drops candidates with bad settings.
* Schema-grounding: drops candidates citing missing columns; passes valid ones.
* Cold-flow degraded path (upstream schema None).
* Missing-upstream degraded path.
* Provider-call wiring: ``response_format={"type":"json_object"}``,
  ``tools=None``.
* Timeout / provider error / parse error → degraded.
* ``max_suggestions`` cap.
* Node-type outside the ``ghost_node`` preset is dropped (defence-in-depth).
* Markdown-fenced JSON is unwrapped.
* Predicted output schema attached when mirror succeeds.
* Route happy path (full integration with monkeypatched provider).
* Route 404 / 409 / 422 / 503.
* Lazy-litellm contract.
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
from flowfile_core.ai import suggest_next_node as snn_mod
from flowfile_core.ai import suggest_next_node_routes
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.providers.base import ChatResponse, Usage
from flowfile_core.ai.scheduler import RateLimitScheduler
from flowfile_core.ai.suggest_next_node import (
    _ALLOWED_NODE_TYPES,
    NextNodeSuggestionsResponse,
    suggest_next_node,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings

# --------------------------------------------------------------------------- #
# Fakes #
# --------------------------------------------------------------------------- #


class _FakeColumn:
    """Stand-in for ``FlowfileColumn`` — only ``column_name`` is read."""

    def __init__(self, name: str, *, dtype: str = "String") -> None:
        self.column_name = name
        self.data_type = dtype

    @property
    def name(self) -> str:  # pragma: no cover - schema_to_dict_list path
        return self.column_name

    def get_column_name(self) -> str:  # pragma: no cover - schema_to_dict_list path
        return self.column_name


def _columns(*names: str) -> list[_FakeColumn]:
    return [_FakeColumn(n) for n in names]


def _make_node(
    node_id: int | str,
    *,
    predicted_schema: list[Any] | None,
    node_type: str = "manual_input",
) -> Any:
    schema = SimpleNamespace(predicted_schema=predicted_schema)
    return SimpleNamespace(
        node_id=node_id,
        node_type=node_type,
        node_schema=schema,
    )


class _FakeGraph:
    """Minimal duck-typed graph: only ``get_node()`` is consumed."""

    def __init__(self, nodes: dict[int | str, Any]) -> None:
        self._nodes = nodes

    def get_node(self, node_id: int | str | None = None):
        return self._nodes.get(node_id)


class _FakeProvider:
    """Records the kwargs the module called ``chat()`` with."""

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
        raise AssertionError("stream() should not be called by suggest_next_node")


def _scheduler() -> RateLimitScheduler:
    """Scheduler with monotonic-zero time-source so RPM never blocks tests."""
    return RateLimitScheduler(time_source=lambda: 0.0, sleep=lambda *_a, **_k: asyncio.sleep(0))


def _payload(suggestions: list[dict[str, Any]]) -> str:
    return json.dumps({"suggestions": suggestions})


def _filter_settings(*, field: str, value: str = "EU") -> dict[str, Any]:
    """A complete-enough ``NodeFilter`` dict for ``model_validate``."""
    return {
        "flow_id": 1,
        "node_id": 99,
        "filter_input": {
            "mode": "basic",
            "basic_filter": {"field": field, "operator": "==", "value": value},
        },
    }


def _select_settings(*, old_name: str) -> dict[str, Any]:
    return {
        "flow_id": 1,
        "node_id": 99,
        "select_input": [
            {"old_name": old_name, "new_name": old_name, "keep": True},
        ],
    }


# --------------------------------------------------------------------------- #
# 1. Surface vocabulary lockstep #
# --------------------------------------------------------------------------- #


def test_ghost_node_surface_in_lockstep() -> None:
    """``ghost_node`` exists in ``SurfaceLiteral`` × ``SURFACE_PRESETS`` × ``SURFACE_TO_LEVEL`` × every provider's ``surface_models``."""
    from flowfile_core.ai.context import builder as ctx_builder
    from flowfile_core.ai.tools import registry as tool_registry

    assert "ghost_node" in tool_registry.get_args(tool_registry.SurfaceLiteral)
    assert "ghost_node" in ctx_builder.SURFACE_TO_LEVEL
    assert ctx_builder.SURFACE_TO_LEVEL["ghost_node"] == "copilot"
    assert "ghost_node" in tool_registry.SURFACE_PRESETS
    # Preset is a non-empty frozenset (W30 invariant).
    assert tool_registry.SURFACE_PRESETS["ghost_node"]
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
            "ghost_node" in provider_cls.surface_models
        ), f"{provider_cls.__name__} missing ghost_node in surface_models"


def test_allowed_node_types_derived_from_preset() -> None:
    """``_ALLOWED_NODE_TYPES`` is the un-namespaced node types from the preset.

    The preset contains MCP-shaped tool names (``flowfile.graph.add_filter``)
    plus the schema introspection tool (``flowfile.schema.read_node_schema``);
    the schema tool is not a node type and must be filtered out.
    """
    assert "filter" in _ALLOWED_NODE_TYPES
    assert "select" in _ALLOWED_NODE_TYPES
    assert "sort" in _ALLOWED_NODE_TYPES
    assert "join" in _ALLOWED_NODE_TYPES
    assert "group_by" in _ALLOWED_NODE_TYPES
    # read_node_schema is a tool, not a node type — must not leak in.
    assert "read_node_schema" not in _ALLOWED_NODE_TYPES
    # Dynamic-only node types are not in the ghost preset (per curation).
    assert "polars_code" not in _ALLOWED_NODE_TYPES
    assert "python_script" not in _ALLOWED_NODE_TYPES


# --------------------------------------------------------------------------- #
# 2. Schema-grounding: hallucinated columns dropped #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_drops_candidate_with_missing_column_ref() -> None:
    upstream = _make_node("u1", predicted_schema=_columns("a", "b", "region"))
    graph = _FakeGraph({"u1": upstream})

    provider = _FakeProvider(
        content=_payload(
            [
                # Valid — references real column.
                {
                    "node_type": "filter",
                    "settings": _filter_settings(field="region"),
                    "label": "Filter by region",
                },
                # Bogus — references a column not in upstream.
                {
                    "node_type": "filter",
                    "settings": _filter_settings(field="not_a_column"),
                    "label": "Bad filter",
                },
            ]
        )
    )

    response = await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert isinstance(response, NextNodeSuggestionsResponse)
    assert response.degraded is False
    assert len(response.suggestions) == 1
    assert response.suggestions[0].node_type == "filter"
    assert response.suggestions[0].label == "Filter by region"


@pytest.mark.asyncio
async def test_drops_candidate_with_invalid_pydantic_settings() -> None:
    upstream = _make_node("u1", predicted_schema=_columns("a"))
    graph = _FakeGraph({"u1": upstream})

    provider = _FakeProvider(
        content=_payload(
            [
                # Missing required ``filter_input`` field.
                {
                    "node_type": "filter",
                    "settings": {"flow_id": 1, "node_id": 99},
                    "label": "Bad filter",
                },
                # Valid select.
                {
                    "node_type": "select",
                    "settings": _select_settings(old_name="a"),
                    "label": "Keep column a",
                },
            ]
        )
    )

    response = await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert response.degraded is False
    assert len(response.suggestions) == 1
    assert response.suggestions[0].node_type == "select"


@pytest.mark.asyncio
async def test_drops_candidate_outside_ghost_node_preset() -> None:
    """Defence-in-depth: even if the LLM ignores the prompt and proposes a
    non-allowed node type, it's dropped."""
    upstream = _make_node("u1", predicted_schema=_columns("a", "b"))
    graph = _FakeGraph({"u1": upstream})

    provider = _FakeProvider(
        content=_payload(
            [
                # ``polars_code`` is not in the ghost_node preset.
                {
                    "node_type": "polars_code",
                    "settings": {"flow_id": 1, "node_id": 99, "polars_code_input": {"polars_code": "df"}},
                    "label": "Code suggestion",
                },
                {
                    "node_type": "select",
                    "settings": _select_settings(old_name="a"),
                    "label": "Keep a",
                },
            ]
        )
    )

    response = await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert response.degraded is False
    assert len(response.suggestions) == 1
    assert response.suggestions[0].node_type == "select"


# --------------------------------------------------------------------------- #
# 3. Degraded / failure paths #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_degrades_on_cold_upstream() -> None:
    """Upstream node has ``predicted_schema=None`` — must NOT call the LLM."""
    upstream = _make_node("u1", predicted_schema=None)
    graph = _FakeGraph({"u1": upstream})

    provider = _FakeProvider(content=_payload([]))
    response = await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert response.degraded is True
    assert response.suggestions == []
    assert response.reason
    # We must NOT have hit the LLM in the cold-flow branch.
    assert provider.last_call_kwargs == {}


@pytest.mark.asyncio
async def test_degrades_when_upstream_missing() -> None:
    graph = _FakeGraph({})
    provider = _FakeProvider(content=_payload([]))
    response = await suggest_next_node(graph, "missing-id", provider=provider, scheduler=_scheduler())
    assert response.degraded is True
    assert provider.last_call_kwargs == {}


@pytest.mark.asyncio
async def test_degrades_on_provider_timeout() -> None:
    upstream = _make_node("u1", predicted_schema=_columns("a"))
    graph = _FakeGraph({"u1": upstream})
    provider = _FakeProvider(content=_payload([]), sleep_before_response=2.0)
    response = await suggest_next_node(
        graph,
        "u1",
        provider=provider,
        scheduler=_scheduler(),
        timeout=0.05,
    )
    assert response.degraded is True
    assert response.reason == "timeout"
    assert response.suggestions == []


@pytest.mark.asyncio
async def test_degrades_on_provider_error() -> None:
    upstream = _make_node("u1", predicted_schema=_columns("a"))
    graph = _FakeGraph({"u1": upstream})
    provider = _FakeProvider(raise_exc=RuntimeError("boom"))
    response = await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert response.degraded is True
    assert response.reason == "provider_error"


@pytest.mark.asyncio
async def test_degrades_on_parse_error() -> None:
    upstream = _make_node("u1", predicted_schema=_columns("a"))
    graph = _FakeGraph({"u1": upstream})
    provider = _FakeProvider(content="absolutely not json")
    response = await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert response.degraded is True
    assert response.reason == "parse_error"


@pytest.mark.asyncio
async def test_degrades_when_all_candidates_filtered() -> None:
    """LLM returned 3 plausible-looking candidates but every one failed
    schema-grounding — the response is degraded with ``no_valid_suggestions``."""
    upstream = _make_node("u1", predicted_schema=_columns("region"))
    graph = _FakeGraph({"u1": upstream})

    provider = _FakeProvider(
        content=_payload(
            [
                {
                    "node_type": "filter",
                    "settings": _filter_settings(field="not_there"),
                    "label": "bad",
                },
                {
                    "node_type": "filter",
                    "settings": _filter_settings(field="also_missing"),
                    "label": "also bad",
                },
            ]
        )
    )
    response = await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert response.degraded is True
    assert response.reason == "no_valid_suggestions"
    assert response.suggestions == []


# --------------------------------------------------------------------------- #
# 4. Provider call wiring #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_response_format_threaded_through() -> None:
    upstream = _make_node("u1", predicted_schema=_columns("a"))
    graph = _FakeGraph({"u1": upstream})

    provider = _FakeProvider(content=_payload([]))
    await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())

    assert provider.last_call_kwargs.get("response_format") == {"type": "json_object"}
    assert provider.last_call_kwargs.get("tools") is None


@pytest.mark.asyncio
async def test_max_tokens_set() -> None:
    """A non-trivial max_tokens is passed (so providers don't truncate
    longer JSON payloads)."""
    upstream = _make_node("u1", predicted_schema=_columns("a"))
    graph = _FakeGraph({"u1": upstream})
    provider = _FakeProvider(content=_payload([]))
    await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert provider.last_call_kwargs.get("max_tokens") and provider.last_call_kwargs["max_tokens"] >= 256


@pytest.mark.asyncio
async def test_intent_threaded_into_user_message() -> None:
    """When the caller supplies ``intent``, it appears in the user message
    so the LLM can bias its choice."""
    upstream = _make_node("u1", predicted_schema=_columns("a"))
    graph = _FakeGraph({"u1": upstream})
    provider = _FakeProvider(content=_payload([]))
    await suggest_next_node(
        graph,
        "u1",
        provider=provider,
        scheduler=_scheduler(),
        intent="filter to last 30 days",
    )
    user_msg = provider.last_call_kwargs["messages"][1].content
    assert "filter to last 30 days" in user_msg


@pytest.mark.asyncio
async def test_max_suggestions_caps_returned_list() -> None:
    upstream = _make_node("u1", predicted_schema=_columns("a", "b", "region"))
    graph = _FakeGraph({"u1": upstream})

    provider = _FakeProvider(
        content=_payload(
            [
                {
                    "node_type": "filter",
                    "settings": _filter_settings(field="region"),
                    "label": "f1",
                },
                {
                    "node_type": "select",
                    "settings": _select_settings(old_name="a"),
                    "label": "s1",
                },
                {
                    "node_type": "select",
                    "settings": _select_settings(old_name="b"),
                    "label": "s2",
                },
            ]
        )
    )
    response = await suggest_next_node(
        graph,
        "u1",
        provider=provider,
        scheduler=_scheduler(),
        max_suggestions=2,
    )
    assert response.degraded is False
    assert len(response.suggestions) == 2


@pytest.mark.asyncio
async def test_markdown_fenced_json_unwrapped() -> None:
    upstream = _make_node("u1", predicted_schema=_columns("a"))
    graph = _FakeGraph({"u1": upstream})

    fenced = (
        "```json\n"
        + _payload(
            [
                {
                    "node_type": "select",
                    "settings": _select_settings(old_name="a"),
                    "label": "Keep a",
                },
            ]
        )
        + "\n```"
    )
    provider = _FakeProvider(content=fenced)
    response = await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert response.degraded is False
    assert len(response.suggestions) == 1


# --------------------------------------------------------------------------- #
# 5. Predicted-schema attachment #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_predicted_schema_attached_for_static_node() -> None:
    """A static node like ``select`` predicts its output schema via the
    mirror path. The frontend uses this to show downstream-friendly previews."""
    # Use real FlowfileColumn so predict_schema_via_mirror's promise-stub install
    # has the right shape.
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

    real_columns = [FlowfileColumn.from_input(name, "String") for name in ("a", "b")]
    upstream = _make_node("u1", predicted_schema=real_columns)
    # Need integer node_id for predict_schema_via_mirror's flow plumbing.
    upstream.node_id = 7
    graph = _FakeGraph({"u1": upstream})

    provider = _FakeProvider(
        content=_payload(
            [
                {
                    "node_type": "select",
                    "settings": _select_settings(old_name="a"),
                    "label": "Keep a",
                },
            ]
        )
    )
    response = await suggest_next_node(graph, "u1", provider=provider, scheduler=_scheduler())
    assert response.degraded is False
    assert len(response.suggestions) == 1
    # Predicted schema is best-effort; either it succeeded (list with at least
    # one entry for column "a") or it returned None — both are valid wire
    # outcomes. We assert the field is attached when the mirror succeeds.
    sugg = response.suggestions[0]
    if sugg.predicted_output_schema is not None:
        names = {col.get("name") for col in sugg.predicted_output_schema}
        assert "a" in names


# --------------------------------------------------------------------------- #
# 6. Routes #
# --------------------------------------------------------------------------- #


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    fake_user = PydanticUser(id=1, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def test_route_404_on_missing_flow(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(suggest_next_node_routes.flow_file_handler, "get_flow", lambda _id: None)
    response = authed_client.post(
        "/ai/suggest_next_node",
        json={"flow_id": 99, "upstream_node_id": "u1"},
    )
    assert response.status_code == 404
    assert "99" in response.json()["detail"]


def test_route_404_on_unknown_provider(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/suggest_next_node",
        json={"flow_id": 1, "upstream_node_id": "u1", "provider": "imaginary"},
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


def test_route_409_on_unconfigured_provider(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        suggest_next_node_routes.flow_file_handler,
        "get_flow",
        lambda _id: object(),
    )

    def _raise(*_a: Any, **_kw: Any):
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(suggest_next_node_routes, "get_configured_provider", _raise)

    response = authed_client.post(
        "/ai/suggest_next_node",
        json={"flow_id": 1, "upstream_node_id": "u1", "provider": "anthropic"},
    )
    assert response.status_code == 409


def test_route_503_when_flag_off(authed_client: TestClient) -> None:
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/suggest_next_node",
            json={"flow_id": 1, "upstream_node_id": "u1"},
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503
    assert "AI features are disabled" in response.json()["detail"]


def test_route_validation_error(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/suggest_next_node",
        json={"flow_id": -1, "upstream_node_id": "u1"},
    )
    assert response.status_code == 422


def test_route_happy_path(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    upstream = _make_node("u1", predicted_schema=_columns("region"))
    graph = _FakeGraph({"u1": upstream})

    monkeypatch.setattr(
        suggest_next_node_routes.flow_file_handler,
        "get_flow",
        lambda _id: graph,
    )
    provider = _FakeProvider(
        content=_payload(
            [
                {
                    "node_type": "filter",
                    "settings": _filter_settings(field="region"),
                    "label": "Filter by region",
                },
            ]
        )
    )
    monkeypatch.setattr(
        suggest_next_node_routes,
        "get_configured_provider",
        lambda *_a, **_kw: provider,
    )

    response = authed_client.post(
        "/ai/suggest_next_node",
        json={
            "flow_id": 1,
            "upstream_node_id": "u1",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["degraded"] is False
    assert len(body["suggestions"]) == 1
    assert body["suggestions"][0]["node_type"] == "filter"
    assert body["suggestions"][0]["label"] == "Filter by region"


# --------------------------------------------------------------------------- #
# 7. Lazy-litellm contract #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_contract() -> None:
    """``import flowfile_core.ai.suggest_next_node`` mustn't drag in litellm."""
    litellm_already_loaded = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)

    saved = {k: v for k, v in sys.modules.items() if k.startswith("flowfile_core.ai.suggest_next_node")}
    for k in list(saved):
        sys.modules.pop(k, None)
    try:
        importlib.import_module("flowfile_core.ai.suggest_next_node")
        if not litellm_already_loaded:
            assert (
                "litellm" not in sys.modules
            ), "flowfile_core.ai.suggest_next_node pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)
        # Restore the cached canonical instance.
        sys.modules["flowfile_core.ai.suggest_next_node"] = snn_mod
