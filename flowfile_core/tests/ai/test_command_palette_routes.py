"""Cmd+K command-palette route tests.

Coverage (~7 cases):

* Happy path — TestClient end-to-end with a stubbed provider returning a
  single ``add_filter`` tool call → ``200`` with ``diff_id``.
* ``404`` on unknown provider name.
* ``404`` on unconfigured provider (BYOK row missing) → defence-in-depth:
  the route itself maps :class:`ProviderNotConfiguredError` to ``409``.
* ``422`` on missing flow.
* ``422`` on Pydantic validation (blank prompt).
* ``503`` when the feature flag is off.
* Soft failures pass through as 200 with ``degraded=true``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import command_palette_routes
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.diff import clear_for_tests
from flowfile_core.ai.providers.base import ChatResponse, ToolCall, Usage
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas, transform_schema

# Fixtures


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_ai_cmdk_routes",
    )


def _flow_with_orders(flow_id: int = 1) -> FlowGraph:
    flow = FlowGraph(flow_settings=_flow_settings(flow_id), name="cmdk_route_test")
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
            ],
            data=[["EU"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(1).get_predicted_schema()
    return flow


def _filter_args() -> dict[str, Any]:
    settings = input_schema.NodeFilter(
        flow_id=1,
        node_id=99,
        depending_on_id=1,
        filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[region]=='EU'"),
    )
    return settings.model_dump(mode="json")


class _FakeProvider:
    name: str = "fake-cmdk-route"
    model: str = "fake-default"
    supports_tools: bool = True
    supports_streaming: bool = True

    def __init__(
        self,
        *,
        tool_calls: list[ToolCall] | None = None,
        content: str | None = None,
    ) -> None:
        self._tool_calls = list(tool_calls or [])
        self._content = content

    async def chat(
        self,
        messages: list[Any],
        tools: list[Any] | None = None,
        max_tokens: int | None = None,
        response_format: dict[str, Any] | None = None,
    ) -> ChatResponse:
        return ChatResponse(
            model=self.model,
            content=self._content,
            tool_calls=list(self._tool_calls),
            finish_reason="tool_calls" if self._tool_calls else "stop",
            usage=Usage(),
        )

    def stream(self, *_a: Any, **_kw: Any):  # pragma: no cover
        raise AssertionError("stream() should not be called by /ai/command_palette")


@pytest.fixture(autouse=True)
def _clear_diff_store() -> None:
    clear_for_tests()


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    fake_user = PydanticUser(id=1, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


# 1. Happy path


def test_route_happy_path(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    flow = _flow_with_orders()
    monkeypatch.setattr(
        command_palette_routes.flow_file_handler,
        "get_flow",
        lambda _id: flow,
    )
    provider = _FakeProvider(
        tool_calls=[
            ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args()),
        ],
        content="Filter rows where region == 'EU'.",
    )
    monkeypatch.setattr(
        command_palette_routes,
        "get_configured_provider",
        lambda *_a, **_kw: provider,
    )

    response = authed_client.post(
        "/ai/command_palette",
        json={
            "flow_id": 1,
            "prompt": "filter to EU",
            "provider": "anthropic",
            "insertion_context": {"upstream_node_ids": [1], "pos_x": 100.0, "pos_y": 100.0},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["degraded"] is False
    assert body["diff_id"]
    assert body["op_count"] == 1
    assert body["diff"]["additions"][0]["node_type"] == "filter"


# 2. 404 unknown provider


def test_route_404_unknown_provider(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/command_palette",
        json={
            "flow_id": 1,
            "prompt": "anything",
            "provider": "imaginary",
        },
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


# 3. 409 unconfigured provider


def test_route_409_unconfigured(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    flow = _flow_with_orders()
    monkeypatch.setattr(
        command_palette_routes.flow_file_handler,
        "get_flow",
        lambda _id: flow,
    )

    def _raise(*_a: Any, **_kw: Any):
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(command_palette_routes, "get_configured_provider", _raise)

    response = authed_client.post(
        "/ai/command_palette",
        json={
            "flow_id": 1,
            "prompt": "anything",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 409


# 4. 422 flow not found


def test_route_422_flow_not_found(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        command_palette_routes.flow_file_handler,
        "get_flow",
        lambda _id: None,
    )
    response = authed_client.post(
        "/ai/command_palette",
        json={
            "flow_id": 999,
            "prompt": "anything",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 422
    assert "999" in response.json()["detail"]


# 5. 422 validation


@pytest.mark.parametrize(
    "payload",
    [
        # Empty prompt — Pydantic ``min_length=1`` rejects.
        {"flow_id": 1, "prompt": "", "provider": "anthropic"},
        # Missing required `provider`.
        {"flow_id": 1, "prompt": "x"},
        # Negative flow_id.
        {"flow_id": -1, "prompt": "x", "provider": "anthropic"},
        # Timeout out of bounds.
        {"flow_id": 1, "prompt": "x", "provider": "anthropic", "timeout": 999.0},
    ],
)
def test_route_422_validation(authed_client: TestClient, payload: dict[str, Any]) -> None:
    response = authed_client.post("/ai/command_palette", json=payload)
    assert response.status_code == 422


# 6. 503 gate


def test_route_503_when_flag_off(authed_client: TestClient) -> None:
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/command_palette",
            json={"flow_id": 1, "prompt": "x", "provider": "anthropic"},
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503
    assert "AI features are disabled" in response.json()["detail"]


# 7. Soft failure passes through as 200 + degraded


def test_route_degraded_passes_through(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """When :func:`run_command_palette` returns ``degraded=True``, the route
    must surface it as a 200 — the frontend only renders one error path."""
    flow = _flow_with_orders()
    monkeypatch.setattr(
        command_palette_routes.flow_file_handler,
        "get_flow",
        lambda _id: flow,
    )
    # Provider returns no tool calls at all → degraded("no_tool_calls").
    provider = _FakeProvider(tool_calls=[], content="I cannot do that.")
    monkeypatch.setattr(
        command_palette_routes,
        "get_configured_provider",
        lambda *_a, **_kw: provider,
    )

    response = authed_client.post(
        "/ai/command_palette",
        json={
            "flow_id": 1,
            "prompt": "do something impossible",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["degraded"] is True
    assert body["reason"] == "no_tool_calls"
    assert body["diff_id"] is None
    assert body["rationale"] == "I cannot do that."
