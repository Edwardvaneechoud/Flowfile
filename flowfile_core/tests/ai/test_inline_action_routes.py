"""W21 — inline ✨ menu endpoint tests.

Cases:

* ``test_inline_action_emits_provider_chunks`` — happy path: builds a
  tiny linear flow with a filter node, POSTs ``action="explain"``, and
  verifies the SSE response carries ``event: chunk`` blocks +
  ``event: done``. Inspects the captured ``messages`` argument to
  ``provider.stream()`` to assert the W22 system prompt + the
  ``## Action: explain`` block + ``tools=None`` all reach the LLM.
* ``test_inline_action_per_action_user_message_distinct`` — parametrised
  per action (``explain`` / ``add_description``); each carries a
  distinct instruction phrase verbatim in the user message.
* ``test_regenerate_code_requires_code_bearing_node`` — ``regenerate_code``
  on a filter node returns 422 with "code-bearing" in the detail.
* ``test_regenerate_code_on_polars_code_node_returns_200`` — same
  action on a ``polars_code`` node returns 200 and emits the
  regenerate-code prompt block.
* ``test_inline_action_flow_not_found_returns_422`` — bogus ``flow_id``
  → 422.
* ``test_inline_action_node_not_found_returns_422`` — bogus ``node_id``
  → 422.
* ``test_inline_action_unknown_provider_returns_404`` — bogus
  ``provider`` → 404 with the supported-list detail (matches W20 / W23).
* ``test_inline_action_unconfigured_returns_409`` — no BYOK row + no
  env var → 409 with the ``ProviderNotConfiguredError`` message.
* ``test_inline_action_disabled_returns_503`` — flipping
  ``FEATURE_FLAG_AI`` off short-circuits at W17's gate.
* ``test_inline_action_validates_required_fields`` — request validation
  on missing ``flow_id`` / ``node_id`` / ``provider`` / ``action`` and
  invalid ``action`` literal.
* ``test_inline_action_samples_mode_forwarded`` — ``samples_mode="regex"``
  is forwarded to ``render_prompt_context`` (spied via monkeypatch).
* ``test_inline_action_tools_none_invariant`` — read-only by
  construction across every action variant.
* ``test_lazy_litellm_import_for_inline_action_routes`` — importing
  ``flowfile_core.ai.inline_action_routes`` does not pull ``litellm``
  into ``sys.modules``.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import inline_action_routes as inline_action_routes_module
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.providers.base import StreamChunk
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema

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
    + the W21 ``## Action`` block).
    """

    def __init__(self, chunks: list[StreamChunk] | None = None) -> None:
        self.chunks = chunks or [
            StreamChunk(content_delta="This node "),
            StreamChunk(content_delta="filters EU rows."),
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

    monkeypatch.setattr(inline_action_routes_module, "get_configured_provider", _factory)
    yield fake


# --------------------------------------------------------------------------- #
# Flow fixtures                                                                #
# --------------------------------------------------------------------------- #


_FILTER_FLOW_ID = 9921  # avoid clashing with sibling test fixtures
_POLARS_FLOW_ID = 9922


def _flow_settings(flow_id: int, suffix: str) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path=f"/tmp/test_w21_{suffix}",
    )


def _build_filter_flow() -> FlowGraph:
    """``orders (1) → filter_eu (2)`` — non-code-bearing filter node.

    Used to exercise the per-action variants (Explain / Optimise / etc.)
    plus the ``regenerate_code`` rejection on a non-code-bearing node.
    """

    flow = FlowGraph(flow_settings=_flow_settings(_FILTER_FLOW_ID, "filter"), name="w21_filter")

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


def _build_polars_code_flow() -> FlowGraph:
    """``orders (1) → polars_compute (2)`` — code-bearing polars_code node.

    Used to confirm the ``regenerate_code`` happy path on a code-bearing
    node and to inspect the regenerate-code instruction block.
    """

    flow = FlowGraph(flow_settings=_flow_settings(_POLARS_FLOW_ID, "polars"), name="w21_polars")

    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=1,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="order_id", data_type="Integer"),
                input_schema.MinimalFieldInfo(name="amount", data_type="Float"),
            ],
            data=[[1, 2], [10.0, 20.0]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(1).name = "orders"

    polars_node = input_schema.NodePolarsCode(
        flow_id=flow.flow_id,
        node_id=2,
        depending_on_ids=[1],
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code='input_df.with_columns(pl.col("amount") * 2)',
        ),
    )
    flow.add_polars_code(polars_node)
    add_connection(
        flow,
        node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2),
    )
    flow.get_node(2).name = "polars_compute"

    for node in flow.nodes:
        node.get_predicted_schema()
    return flow


@pytest.fixture
def registered_filter_flow() -> Iterator[FlowGraph]:
    flow = _build_filter_flow()
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


@pytest.fixture
def registered_polars_flow() -> Iterator[FlowGraph]:
    flow = _build_polars_code_flow()
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


# --------------------------------------------------------------------------- #
# 1. Happy path                                                                #
# --------------------------------------------------------------------------- #


def test_inline_action_emits_provider_chunks(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_filter_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _FILTER_FLOW_ID,
            "node_id": 2,
            "action": "explain",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")

    body = response.text
    assert 'event: chunk\ndata: {"content_delta": "This node "}' in body
    assert 'event: chunk\ndata: {"content_delta": "filters EU rows."}' in body
    assert 'event: done\ndata: {"finish_reason": "stop"}' in body

    captured = patch_get_configured_provider.last_call_kwargs["messages"]
    assert len(captured) == 2
    system_msg, user_msg = captured

    assert system_msg.role == "system"
    assert system_msg.content.strip(), "system prompt must not be empty"

    assert user_msg.role == "user"
    # W22 puts the failing node and its upstream in the deterministic body.
    assert "filter_eu" in user_msg.content or "node 2" in user_msg.content.lower()
    # The W21 action block is appended verbatim with the action name.
    assert "## Action: explain" in user_msg.content
    assert "id `2`" in user_msg.content
    assert "type `filter`" in user_msg.content
    # The explain instruction phrase is present.
    assert "Explain in plain language" in user_msg.content

    # Read-only contract: tools=None across every variant.
    assert patch_get_configured_provider.last_call_kwargs.get("tools") is None


# --------------------------------------------------------------------------- #
# 2. Per-action user-message distinct                                          #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "action, expected_phrase",
    [
        ("explain", "Explain in plain language"),
        ("add_description", "Write a single sentence"),
    ],
    ids=["explain", "add_description"],
)
def test_inline_action_per_action_user_message_distinct(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_filter_flow: FlowGraph,
    action: str,
    expected_phrase: str,
) -> None:
    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _FILTER_FLOW_ID,
            "node_id": 2,
            "action": action,
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200, response.text
    user_msg = patch_get_configured_provider.last_call_kwargs["messages"][1]
    assert f"## Action: {action}" in user_msg.content
    assert expected_phrase in user_msg.content


# --------------------------------------------------------------------------- #
# 3. regenerate_code requires code-bearing node                                #
# --------------------------------------------------------------------------- #


def test_regenerate_code_requires_code_bearing_node(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_filter_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _FILTER_FLOW_ID,
            "node_id": 2,
            "action": "regenerate_code",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert "code-bearing" in detail
    assert "polars_code" in detail
    assert "filter" in detail


# --------------------------------------------------------------------------- #
# 4. regenerate_code on polars_code node → 200                                 #
# --------------------------------------------------------------------------- #


def test_regenerate_code_on_polars_code_node_returns_200(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_polars_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _POLARS_FLOW_ID,
            "node_id": 2,
            "action": "regenerate_code",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200, response.text
    user_msg = patch_get_configured_provider.last_call_kwargs["messages"][1]
    assert "## Action: regenerate_code" in user_msg.content
    assert "Rewrite the code in this node" in user_msg.content
    assert "type `polars_code`" in user_msg.content


# --------------------------------------------------------------------------- #
# 5. Flow not found → 422                                                      #
# --------------------------------------------------------------------------- #


def test_inline_action_flow_not_found_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
) -> None:
    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": 999_998,
            "node_id": 1,
            "action": "explain",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 422
    assert "flow" in response.json()["detail"].lower()


# --------------------------------------------------------------------------- #
# 6. Node not found → 422                                                      #
# --------------------------------------------------------------------------- #


def test_inline_action_node_not_found_returns_422(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_filter_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _FILTER_FLOW_ID,
            "node_id": 999,
            "action": "explain",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 422
    assert "node" in response.json()["detail"].lower()


# --------------------------------------------------------------------------- #
# 7. Unknown provider → 404                                                    #
# --------------------------------------------------------------------------- #


def test_inline_action_unknown_provider_returns_404(
    authed_client: TestClient,
    registered_filter_flow: FlowGraph,
) -> None:
    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _FILTER_FLOW_ID,
            "node_id": 2,
            "action": "explain",
            "provider": "imaginary",
        },
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 8. Provider not configured → 409                                             #
# --------------------------------------------------------------------------- #


def test_inline_action_unconfigured_returns_409(
    authed_client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    registered_filter_flow: FlowGraph,
) -> None:
    def _raise(*_args: Any, **_kwargs: Any) -> None:
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(inline_action_routes_module, "get_configured_provider", _raise)

    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _FILTER_FLOW_ID,
            "node_id": 2,
            "action": "explain",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 409
    assert "anthropic" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 9. Feature flag off → 503                                                    #
# --------------------------------------------------------------------------- #


def test_inline_action_disabled_returns_503(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_filter_flow: FlowGraph,
) -> None:
    """Inheriting W17's router-level dependency means flipping the flag off
    must return 503 here too. Read ``FEATURE_FLAG_AI`` off the module via
    ``core_settings`` (same fix W17 + W20 + W23 + W50 applied).
    """

    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/inline_action",
            json={
                "flow_id": _FILTER_FLOW_ID,
                "node_id": 2,
                "action": "explain",
                "provider": "anthropic",
            },
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503
    assert "AI features are disabled" in response.json()["detail"]


# --------------------------------------------------------------------------- #
# 10. Request validation                                                       #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "missing_field, payload",
    [
        ("flow_id", {"node_id": 2, "action": "explain", "provider": "anthropic"}),
        ("node_id", {"flow_id": _FILTER_FLOW_ID, "action": "explain", "provider": "anthropic"}),
        ("action", {"flow_id": _FILTER_FLOW_ID, "node_id": 2, "provider": "anthropic"}),
        ("provider", {"flow_id": _FILTER_FLOW_ID, "node_id": 2, "action": "explain"}),
    ],
    ids=["missing_flow_id", "missing_node_id", "missing_action", "missing_provider"],
)
def test_inline_action_validates_required_fields(
    authed_client: TestClient, missing_field: str, payload: dict[str, Any]
) -> None:
    response = authed_client.post("/ai/inline_action", json=payload)
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any(missing_field in str(item) for item in detail)


def test_inline_action_rejects_invalid_action_literal(
    authed_client: TestClient, registered_filter_flow: FlowGraph
) -> None:
    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _FILTER_FLOW_ID,
            "node_id": 2,
            "action": "make_coffee",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 422
    detail = response.json()["detail"]
    # Pydantic surfaces the field name + the offending value.
    assert any("action" in str(item) for item in detail)


# --------------------------------------------------------------------------- #
# 11. samples_mode forwarded to W22                                            #
# --------------------------------------------------------------------------- #


def test_inline_action_samples_mode_forwarded(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    monkeypatch: pytest.MonkeyPatch,
    registered_filter_flow: FlowGraph,
) -> None:
    captured: dict[str, Any] = {}
    real_render = inline_action_routes_module.render_prompt_context

    def _spy(flow: Any, pinned: list[int], **kwargs: Any) -> Any:
        captured["pinned"] = pinned
        captured["surface"] = kwargs.get("surface")
        captured["samples_mode"] = kwargs.get("samples_mode")
        return real_render(flow, pinned, **kwargs)

    monkeypatch.setattr(inline_action_routes_module, "render_prompt_context", _spy)

    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _FILTER_FLOW_ID,
            "node_id": 2,
            "action": "explain",
            "provider": "anthropic",
            "samples_mode": "regex",
        },
    )
    assert response.status_code == 200, response.text
    assert captured["pinned"] == [2]
    assert captured["surface"] == "explain"
    assert captured["samples_mode"] == "regex"


# --------------------------------------------------------------------------- #
# 12. Tools=None invariant across actions                                      #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "action",
    ["explain", "add_description"],
)
def test_inline_action_tools_none_invariant(
    authed_client: TestClient,
    patch_get_configured_provider: FakeProvider,
    registered_filter_flow: FlowGraph,
    action: str,
) -> None:
    response = authed_client.post(
        "/ai/inline_action",
        json={
            "flow_id": _FILTER_FLOW_ID,
            "node_id": 2,
            "action": action,
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200, response.text
    assert patch_get_configured_provider.last_call_kwargs.get("tools") is None


# --------------------------------------------------------------------------- #
# 13. Lazy-litellm contract                                                    #
# --------------------------------------------------------------------------- #


def test_lazy_litellm_import_for_inline_action_routes() -> None:
    """``import flowfile_core.ai.inline_action_routes`` mustn't pull litellm.

    Same contract as W11/W12/W13/W20/W23/W50 — the module sits behind
    the BYOK seam, not the ``provider_factory`` bootstrap, so the heavy
    SDK stays out of the import graph until a real call happens.

    Caveat: a sibling test may have already imported ``litellm`` in this
    process (the suite shares a Python interpreter). When that's the
    case we can't observe the lazy contract, so the assertion is gated
    on a clean snapshot — same posture as W12 / W20 / W23 / W50.
    """

    litellm_already_loaded = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)

    saved = {k: v for k, v in sys.modules.items() if k.startswith("flowfile_core.ai.inline_action_routes")}
    for k in list(saved):
        sys.modules.pop(k, None)
    try:
        import importlib

        importlib.import_module("flowfile_core.ai.inline_action_routes")
        if not litellm_already_loaded:
            assert (
                "litellm" not in sys.modules
            ), "flowfile_core.ai.inline_action_routes pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)
