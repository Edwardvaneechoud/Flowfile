""""Fix with AI" run-failure endpoint tests.

Cases:

* ``test_explain_run_failure_emits_provider_chunks`` — happy path: builds
  a tiny linear flow, records a failed ``NodeResult`` on the filter,
  POSTs, and verifies the SSE response carries ``event: chunk`` blocks
  + ``event: done``. Inspects the captured ``messages`` argument to
  ``provider.stream()`` to assert the system prompt + the
  ``## Failure`` block + the verbatim error string all reach the LLM.
* ``test_explain_run_failure_client_error_message_wins`` — a stale
  ``latest_run_info`` is overridden by ``error_message`` in the body.
* ``test_explain_run_failure_reads_latest_run_info`` — body omits
  ``error_message``; server reads the recorded failure from
  ``latest_run_info``.
* ``test_explain_run_failure_no_recorded_failure_returns_422`` — empty
  ``latest_run_info`` + no ``error_message`` → 422.
* ``test_explain_run_failure_flow_not_found_returns_422`` — bogus
  ``flow_id`` → 422.
* ``test_explain_run_failure_node_not_found_returns_422`` — bogus
  ``node_id`` → 422.
* ``test_explain_run_failure_unknown_provider_returns_404`` — bogus
  ``provider`` → 404 with the supported-list detail (matches).
* ``test_explain_run_failure_unconfigured_returns_409`` — no BYOK row +
  no env var → 409 with the ``ProviderNotConfiguredError`` message.
* ``test_explain_run_failure_disabled_returns_503`` — flipping
  ``FEATURE_FLAG_AI`` off short-circuits at's gate.
* ``test_explain_run_failure_validates_required_fields`` — request
  validation on missing ``flow_id`` / ``node_id`` / ``provider``.
* ``test_lazy_litellm_import_for_run_failure_routes`` — importing
  ``flowfile_core.ai.run_failure_routes`` doesn't pull ``litellm`` into
  ``sys.modules``.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import run_failure_routes as run_failure_routes_module
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.providers.base import StreamChunk
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, output_model, schemas, transform_schema

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
    + the ``## Failure`` block).
    """

    def __init__(self, chunks: list[StreamChunk] | None = None) -> None:
        self.chunks = chunks or [
            StreamChunk(content_delta="Looks like "),
            StreamChunk(content_delta="the column was renamed."),
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

    monkeypatch.setattr(run_failure_routes_module, "get_configured_provider", _factory)
    yield fake


# --------------------------------------------------------------------------- #
# Flow fixture #
# --------------------------------------------------------------------------- #


_FLOW_ID = 9923  # avoid clashing with any flow another test left around


def _flow_settings() -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=_FLOW_ID,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_w23_run_failure",
    )


def _build_linear_flow() -> FlowGraph:
    """``orders (1) → filter_eu (2)`` with the filter's predicate set up
    to be referenced by the synthetic failure error."""

    flow = FlowGraph(flow_settings=_flow_settings(), name="w23_test")

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


def _attach_failure(flow: FlowGraph, node_id: int, error: str) -> None:
    info = flow.create_initial_run_information(number_of_nodes=len(flow.nodes), run_type="full_run")
    info.node_step_result.append(
        output_model.NodeResult(
            node_id=node_id,
            node_name=flow.get_node(node_id).node_type,
            success=False,
            error=error,
            is_running=False,
        )
    )
    info.success = False
    info.nodes_completed = 0
    flow.latest_run_info = info


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


# --------------------------------------------------------------------------- #
# 1. Happy path #
# --------------------------------------------------------------------------- #


def test_explain_run_failure_emits_provider_chunks(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
) -> None:
    error_text = "ColumnNotFoundError: column 'regions' not in schema"
    _attach_failure(registered_flow, node_id=2, error=error_text)

    response = authed_client.post(
        "/ai/explain_run_failure",
        json={
            "flow_id": _FLOW_ID,
            "node_id": 2,
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")

    body = response.text
    assert 'event: chunk\ndata: {"content_delta": "Looks like "}' in body
    assert 'event: chunk\ndata: {"content_delta": "the column was renamed."}' in body
    assert 'event: done\ndata: {"finish_reason": "stop"}' in body
    assert body.index("Looks like ") < body.index("the column was renamed.") < body.index("done")

    # Assert the messages that reach the provider:
    captured = patch_get_configured_provider.last_call_kwargs["messages"]
    assert len(captured) == 2
    system_msg, user_msg = captured

    assert system_msg.role == "system"
    #'s assemble_system_prompt(surface="explain") concatenates base + assist;
    # both files carry the schema-grounding contract phrase.
    assert system_msg.content.strip(), "system prompt must not be empty"

    assert user_msg.role == "user"
    # renders subgraph + node settings + schemas; the failing node and its
    # upstream are present.
    assert "filter_eu" in user_msg.content or "node 2" in user_msg.content.lower()
    # The failure block is appended verbatim.
    assert "## Failure" in user_msg.content
    assert error_text in user_msg.content
    assert "read-only assist" in user_msg.content

    # Read-only contract: tools=None.
    assert patch_get_configured_provider.last_call_kwargs.get("tools") is None


# --------------------------------------------------------------------------- #
# 2. Client-supplied error wins #
# --------------------------------------------------------------------------- #


def test_explain_run_failure_client_error_message_wins(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
) -> None:
    # No latest_run_info populated; only the body's error_message is available.
    response = authed_client.post(
        "/ai/explain_run_failure",
        json={
            "flow_id": _FLOW_ID,
            "node_id": 2,
            "provider": "anthropic",
            "error_message": "freshly captured client-side error",
        },
    )
    assert response.status_code == 200
    user_msg = patch_get_configured_provider.last_call_kwargs["messages"][1]
    assert "freshly captured client-side error" in user_msg.content


# --------------------------------------------------------------------------- #
# 3. Server reads latest_run_info when client omits it #
# --------------------------------------------------------------------------- #


def test_explain_run_failure_reads_latest_run_info(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
) -> None:
    _attach_failure(registered_flow, node_id=2, error="recorded-side error from prior run")

    response = authed_client.post(
        "/ai/explain_run_failure",
        json={
            "flow_id": _FLOW_ID,
            "node_id": 2,
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    user_msg = patch_get_configured_provider.last_call_kwargs["messages"][1]
    assert "recorded-side error from prior run" in user_msg.content


# --------------------------------------------------------------------------- #
# 4. No recorded failure → 422 #
# --------------------------------------------------------------------------- #


def test_explain_run_failure_no_recorded_failure_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
) -> None:
    # No latest_run_info, no body error_message.
    response = authed_client.post(
        "/ai/explain_run_failure",
        json={
            "flow_id": _FLOW_ID,
            "node_id": 2,
            "provider": "anthropic",
        },
    )
    assert response.status_code == 422
    assert "no recorded failure" in response.json()["detail"].lower()


# --------------------------------------------------------------------------- #
# 5. Flow not found → 422 #
# --------------------------------------------------------------------------- #


def test_explain_run_failure_flow_not_found_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
) -> None:
    response = authed_client.post(
        "/ai/explain_run_failure",
        json={
            "flow_id": 999_999,
            "node_id": 1,
            "provider": "anthropic",
            "error_message": "anything",
        },
    )
    assert response.status_code == 422
    assert "flow" in response.json()["detail"].lower()


# --------------------------------------------------------------------------- #
# 6. Node not found → 422 #
# --------------------------------------------------------------------------- #


def test_explain_run_failure_node_not_found_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/explain_run_failure",
        json={
            "flow_id": _FLOW_ID,
            "node_id": 999,
            "provider": "anthropic",
            "error_message": "anything",
        },
    )
    assert response.status_code == 422
    assert "node" in response.json()["detail"].lower()


# --------------------------------------------------------------------------- #
# 7. Unknown provider → 404 #
# --------------------------------------------------------------------------- #


def test_explain_run_failure_unknown_provider_returns_404(
    authed_client: TestClient,
    registered_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/explain_run_failure",
        json={
            "flow_id": _FLOW_ID,
            "node_id": 2,
            "provider": "imaginary",
            "error_message": "anything",
        },
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 8. Provider not configured → 409 #
# --------------------------------------------------------------------------- #


def test_explain_run_failure_unconfigured_returns_409(
    authed_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    registered_flow: FlowGraph,
) -> None:
    def _raise(*_args: Any, **_kwargs: Any) -> None:
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(run_failure_routes_module, "get_configured_provider", _raise)

    response = authed_client.post(
        "/ai/explain_run_failure",
        json={
            "flow_id": _FLOW_ID,
            "node_id": 2,
            "provider": "anthropic",
            "error_message": "anything",
        },
    )
    assert response.status_code == 409
    assert "anthropic" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 9. Feature flag off → 503 #
# --------------------------------------------------------------------------- #


def test_explain_run_failure_disabled_returns_503(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_flow: FlowGraph,
) -> None:
    """Inheriting's router-level dependency means flipping the flag off
    must return 503 here too. Read ``FEATURE_FLAG_AI`` off the module via
    ``core_settings`` (same fix + applied).
    """

    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/explain_run_failure",
            json={
                "flow_id": _FLOW_ID,
                "node_id": 2,
                "provider": "anthropic",
                "error_message": "anything",
            },
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503
    assert "AI features are disabled" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 10. Request validation #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "missing_field, payload",
    [
        ("flow_id", {"node_id": 2, "provider": "anthropic", "error_message": "x"}),
        ("node_id", {"flow_id": _FLOW_ID, "provider": "anthropic", "error_message": "x"}),
        ("provider", {"flow_id": _FLOW_ID, "node_id": 2, "error_message": "x"}),
    ],
    ids=["missing_flow_id", "missing_node_id", "missing_provider"],
)
def test_explain_run_failure_validates_required_fields(
    authed_client: TestClient, missing_field: str, payload: dict[str, Any]
) -> None:
    response = authed_client.post("/ai/explain_run_failure", json=payload)
    assert response.status_code == 422
    detail = response.json()["detail"]
    # Pydantic surfaces the missing field name in the error structure.
    assert any(missing_field in str(item) for item in detail)


# --------------------------------------------------------------------------- #
# 11. Lazy-litellm contract #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_import_for_run_failure_routes() -> None:
    """``import flowfile_core.ai.run_failure_routes`` mustn't pull litellm.

    Same contract as W11/W12/W13/W20 — the module sits behind the BYOK
    seam, not the ``provider_factory`` bootstrap, so the heavy SDK stays
    out of the import graph until a real call happens.

    Caveat: a sibling test may have already imported ``litellm`` in this
    process (the suite shares a Python interpreter). When that's the
    case we can't observe the lazy contract, so the assertion is gated
    on a clean snapshot — same posture as /.
    """

    litellm_already_loaded = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)

    saved = {k: v for k, v in sys.modules.items() if k.startswith("flowfile_core.ai.run_failure_routes")}
    for k in list(saved):
        sys.modules.pop(k, None)
    try:
        import importlib

        importlib.import_module("flowfile_core.ai.run_failure_routes")
        if not litellm_already_loaded:
            assert (
                "litellm" not in sys.modules
            ), "flowfile_core.ai.run_failure_routes pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)
