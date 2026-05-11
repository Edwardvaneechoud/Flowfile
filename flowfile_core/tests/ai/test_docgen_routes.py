""""Generate documentation" endpoint tests.

Cases:

* ``test_generate_documentation_emits_provider_chunks`` — happy path:
  builds a tiny linear flow, POSTs, and verifies the SSE response
  carries ``event: chunk`` blocks + ``event: done``. Inspects the
  captured ``messages`` argument to ``provider.stream()`` to assert the system prompt + the ``## Documentation request`` block + the
  flow-name + the read-only contract all reach the LLM.
* ``test_generate_documentation_pins_all_nodes`` — capture
  ``render_prompt_context`` args; assert all flow node ids are passed
  as ``pinned_node_ids``.
* ``test_generate_documentation_uses_flow_name_in_prompt`` — a flow
  with a non-empty ``flow_settings.name`` puts the name into the
  Markdown title hint verbatim.
* ``test_generate_documentation_falls_back_when_flow_name_empty`` —
  empty ``flow_settings.name`` → ``flow-{flow_id}`` fallback.
* ``test_generate_documentation_flow_not_found_returns_422`` — bogus
  ``flow_id`` → 422.
* ``test_generate_documentation_empty_flow_returns_422`` — flow exists
  but has no nodes → 422 with the "no nodes" detail.
* ``test_generate_documentation_unknown_provider_returns_404`` — bogus
  ``provider`` → 404.
* ``test_generate_documentation_unconfigured_returns_409`` — no BYOK
  row + no env var → 409 with the ``ProviderNotConfiguredError``
  message.
* ``test_generate_documentation_disabled_returns_503`` — flipping
  ``FEATURE_FLAG_AI`` off short-circuits at's gate.
* ``test_generate_documentation_validates_required_fields`` — request
  validation on missing ``flow_id`` / ``provider``.
* ``test_generate_documentation_samples_mode_forwarded`` — body's
  ``samples_mode`` reaches's ``render_prompt_context`` call.
* ``test_lazy_litellm_import_for_docgen_routes`` — importing
  ``flowfile_core.ai.docgen_routes`` doesn't pull ``litellm`` into
  ``sys.modules``.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import docgen_routes as docgen_routes_module
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.providers.base import StreamChunk
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema

# --------------------------------------------------------------------------- #
# Shared fixtures #
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
    the messages that reach the LLM (system prompt + the user body
    + the ``## Documentation request`` block).
    """

    def __init__(self, chunks: list[StreamChunk] | None = None) -> None:
        self.chunks = chunks or [
            StreamChunk(content_delta="# Flow: w50_test\n\n"),
            StreamChunk(content_delta="This flow filters EU orders."),
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

    monkeypatch.setattr(docgen_routes_module, "get_configured_provider", _factory)
    yield fake


# --------------------------------------------------------------------------- #
# Flow fixtures #
# --------------------------------------------------------------------------- #


_FLOW_ID = 9950  # avoid clashing with any flow another test left around


def _flow_settings(*, name: str = "w50_test") -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=_FLOW_ID,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_w50_docgen",
        name=name,
    )


def _build_linear_flow(*, name: str = "w50_test") -> FlowGraph:
    """``orders (1) → filter_eu (2)`` — same shape as's fixture."""

    flow = FlowGraph(flow_settings=_flow_settings(name=name), name=name)

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
    return FlowGraph(flow_settings=_flow_settings(name="empty_w50"), name="empty_w50")


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
def registered_unnamed_flow() -> Iterator[FlowGraph]:
    """A flow whose ``flow_settings.name`` is empty — the route should
    fall back to ``f"flow-{flow_id}"``."""

    flow = _build_linear_flow(name="")
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


# --------------------------------------------------------------------------- #
# 1. Happy path #
# --------------------------------------------------------------------------- #


def test_generate_documentation_emits_provider_chunks(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/generate_documentation",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")

    body = response.text
    assert 'event: chunk\ndata: {"content_delta": "# Flow: w50_test\\n\\n"}' in body
    assert 'event: chunk\ndata: {"content_delta": "This flow filters EU orders."}' in body
    assert 'event: done\ndata: {"finish_reason": "stop"}' in body

    captured = patch_get_configured_provider.last_call_kwargs["messages"]
    assert len(captured) == 2
    system_msg, user_msg = captured

    assert system_msg.role == "system"
    assert system_msg.content.strip(), "system prompt must not be empty"

    assert user_msg.role == "user"
    #'s render_prompt_context surfaces "## Subgraph" for the deterministic body.
    assert "## Subgraph" in user_msg.content
    # The documentation block is appended verbatim.
    assert "## Documentation request" in user_msg.content
    assert "Flow name: `w50_test`" in user_msg.content
    assert "# Flow: w50_test" in user_msg.content
    assert "read-only assist" in user_msg.content
    assert "Do not wrap in code fences" in user_msg.content

    # Read-only contract: tools=None.
    assert patch_get_configured_provider.last_call_kwargs.get("tools") is None


# --------------------------------------------------------------------------- #
# 2. All nodes are pinned #
# --------------------------------------------------------------------------- #


def test_generate_documentation_pins_all_nodes(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``render_prompt_context`` must receive every flow node id as
    pinned, not just sinks/leaves. Capture the call to verify."""

    captured_kwargs: dict[str, Any] = {}
    real = docgen_routes_module.render_prompt_context

    def _spy(graph: Any, pinned_node_ids: Any, **kwargs: Any) -> Any:
        captured_kwargs["pinned_node_ids"] = list(pinned_node_ids)
        captured_kwargs.update(kwargs)
        return real(graph, pinned_node_ids, **kwargs)

    monkeypatch.setattr(docgen_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/generate_documentation",
        json={"flow_id": _FLOW_ID, "provider": "anthropic"},
    )
    assert response.status_code == 200

    expected = sorted(node.node_id for node in registered_flow.nodes)
    assert sorted(captured_kwargs["pinned_node_ids"]) == expected
    assert captured_kwargs["surface"] == "docgen"


# --------------------------------------------------------------------------- #
# 3. Flow name in prompt #
# --------------------------------------------------------------------------- #


def test_generate_documentation_uses_flow_name_in_prompt(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/generate_documentation",
        json={"flow_id": _FLOW_ID, "provider": "anthropic"},
    )
    assert response.status_code == 200
    user_msg = patch_get_configured_provider.last_call_kwargs["messages"][1]
    assert "Flow name: `w50_test`" in user_msg.content
    # The synthesised h1 hint also carries the name.
    assert "# Flow: w50_test" in user_msg.content


# --------------------------------------------------------------------------- #
# 4. Fallback when flow name is empty #
# --------------------------------------------------------------------------- #


def test_generate_documentation_falls_back_when_flow_name_empty(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_unnamed_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/generate_documentation",
        json={"flow_id": _FLOW_ID, "provider": "anthropic"},
    )
    assert response.status_code == 200
    user_msg = patch_get_configured_provider.last_call_kwargs["messages"][1]
    fallback = f"flow-{_FLOW_ID}"
    assert f"Flow name: `{fallback}`" in user_msg.content
    assert f"# Flow: {fallback}" in user_msg.content


# --------------------------------------------------------------------------- #
# 5. Flow not found → 422 #
# --------------------------------------------------------------------------- #


def test_generate_documentation_flow_not_found_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
) -> None:
    response = authed_client.post(
        "/ai/generate_documentation",
        json={"flow_id": 999_999, "provider": "anthropic"},
    )
    assert response.status_code == 422
    assert "flow" in response.json()["detail"].lower()


# --------------------------------------------------------------------------- #
# 6. Empty flow → 422 #
# --------------------------------------------------------------------------- #


def test_generate_documentation_empty_flow_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_empty_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/generate_documentation",
        json={"flow_id": _FLOW_ID, "provider": "anthropic"},
    )
    assert response.status_code == 422
    assert "no nodes" in response.json()["detail"].lower()


# --------------------------------------------------------------------------- #
# 7. Unknown provider → 404 #
# --------------------------------------------------------------------------- #


def test_generate_documentation_unknown_provider_returns_404(
    authed_client: TestClient,
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/generate_documentation",
        json={"flow_id": _FLOW_ID, "provider": "imaginary"},
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 8. Provider not configured → 409 #
# --------------------------------------------------------------------------- #


def test_generate_documentation_unconfigured_returns_409(
    authed_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    registered_flow: FlowGraph,
) -> None:
    def _raise(*_args: Any, **_kwargs: Any) -> None:
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(docgen_routes_module, "get_configured_provider", _raise)

    response = authed_client.post(
        "/ai/generate_documentation",
        json={"flow_id": _FLOW_ID, "provider": "anthropic"},
    )
    assert response.status_code == 409
    assert "anthropic" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 9. Feature flag off → 503 #
# --------------------------------------------------------------------------- #


def test_generate_documentation_disabled_returns_503(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
) -> None:
    """Inheriting's router-level dependency means flipping the flag
    off must return 503 here too. Read ``FEATURE_FLAG_AI`` off the
    module via ``core_settings`` (same posture as / /)."""

    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/generate_documentation",
            json={"flow_id": _FLOW_ID, "provider": "anthropic"},
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503
    assert "AI features are disabled" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 10-11. Request validation #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "missing_field, payload",
    [
        ("flow_id", {"provider": "anthropic"}),
        ("provider", {"flow_id": _FLOW_ID}),
    ],
    ids=["missing_flow_id", "missing_provider"],
)
def test_generate_documentation_validates_required_fields(
    authed_client: TestClient, missing_field: str, payload: dict[str, Any]
) -> None:
    response = authed_client.post("/ai/generate_documentation", json=payload)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any(missing_field in str(item) for item in detail)


# --------------------------------------------------------------------------- #
# 12. samples_mode forwarded to #
# --------------------------------------------------------------------------- #


def test_generate_documentation_samples_mode_forwarded(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: dict[str, Any] = {}
    real = docgen_routes_module.render_prompt_context

    def _spy(graph: Any, pinned_node_ids: Any, **kwargs: Any) -> Any:
        captured_kwargs.update(kwargs)
        return real(graph, pinned_node_ids, **kwargs)

    monkeypatch.setattr(docgen_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/generate_documentation",
        json={
            "flow_id": _FLOW_ID,
            "provider": "anthropic",
            "samples_mode": "regex",
        },
    )
    assert response.status_code == 200
    assert captured_kwargs["samples_mode"] == "regex"


# --------------------------------------------------------------------------- #
# 13. Lazy-litellm contract #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_import_for_docgen_routes() -> None:
    """``import flowfile_core.ai.docgen_routes`` mustn't pull litellm.

    Same contract as W11/W12/W13/W20/W23 — the module sits behind the
    BYOK seam, not the ``provider_factory`` bootstrap, so the heavy SDK
    stays out of the import graph until a real call happens.

    Caveat: a sibling test may have already imported ``litellm`` in this
    process (the suite shares a Python interpreter). When that's the
    case we can't observe the lazy contract, so the assertion is gated
    on a clean snapshot — same posture as / /.
    """

    litellm_already_loaded = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)

    saved = {k: v for k, v in sys.modules.items() if k.startswith("flowfile_core.ai.docgen_routes")}
    for k in list(saved):
        sys.modules.pop(k, None)
    try:
        import importlib

        importlib.import_module("flowfile_core.ai.docgen_routes")
        if not litellm_already_loaded:
            assert (
                "litellm" not in sys.modules
            ), "flowfile_core.ai.docgen_routes pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)
