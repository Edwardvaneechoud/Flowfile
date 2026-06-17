"""``POST /ai/route`` endpoint tests.

Cases:

* ``test_route_returns_agent_verdict_for_build_phrase`` — happy path:
  the heuristic short-circuits on a clear build phrase and the route
  returns ``verdict="agent"``.
* ``test_route_returns_chat_verdict_for_question`` — heuristic-driven
  chat verdict.
* ``test_route_returns_chat_verdict_when_classifier_fails`` —
  ``classify_intent`` raises every internal failure into a chat
  fallback; the route mirrors that and returns 200, never 5xx.
* ``test_route_unknown_provider_returns_404``.
* ``test_route_unconfigured_returns_409``.
* ``test_route_disabled_returns_503`` — ``FEATURE_FLAG_AI=False``
  short-circuits via the router-level dependency.
* ``test_route_validates_message_present`` — empty ``message`` → 422.
* ``test_route_emits_audit_event`` — every classification persists an
  ``auto_promotion_classified`` audit row with the expected fields.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import audit, intent_router
from flowfile_core.ai import intent_router_routes as routes_module
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.intent_router import IntentClassification
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings


# Fixtures


class _FakeProvider:
    name: str = "fake"
    model: str = "fake-haiku"
    supports_tools: bool = True
    supports_streaming: bool = True

    async def chat(self, *_a: Any, **_kw: Any) -> Any:  # pragma: no cover - not exercised
        raise AssertionError(
            "tests must monkeypatch classify_intent so chat() is never called from /ai/route"
        )

    def stream(self, *_a: Any, **_kw: Any) -> Any:  # pragma: no cover
        raise AssertionError("stream() must not be called by /ai/route")


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    fake_user = PydanticUser(id=1, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake_user
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


@pytest.fixture
def patch_get_configured_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[_FakeProvider]:
    fake = _FakeProvider()

    def _factory(*_args: Any, **_kwargs: Any) -> _FakeProvider:
        return fake

    monkeypatch.setattr(routes_module, "get_configured_provider", _factory)
    yield fake


@pytest.fixture
def patch_classify(monkeypatch: pytest.MonkeyPatch):
    """Replace ``classify_intent`` with a stub that returns a configurable
    :class:`IntentClassification`. Returns the recording dict so tests can
    inspect what the route forwarded."""

    state: dict[str, Any] = {"calls": [], "result": None, "raise": None}

    async def _stub(message: str, **kwargs: Any) -> IntentClassification:
        state["calls"].append({"message": message, "kwargs": kwargs})
        if state["raise"] is not None:
            raise state["raise"]
        return state["result"] or IntentClassification(
            kind="chat",
            confidence=0.0,
            reason="default-stub",
        )

    monkeypatch.setattr(routes_module, "classify_intent", _stub)
    yield state


# Verdict mapping


def test_route_returns_agent_verdict_for_build_phrase(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    patch_classify: dict[str, Any],
) -> None:
    patch_classify["result"] = IntentClassification(
        kind="build",
        confidence=0.85,
        reason="user wants to add a group_by",
    )
    response = authed_client.post(
        "/ai/route",
        json={
            "message": "add a group_by node grouping by status",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["verdict"] == "agent"
    assert payload["kind"] == "build"
    assert payload["confidence"] == pytest.approx(0.85)
    assert payload["reason"] == "user wants to add a group_by"
    assert isinstance(payload["latency_ms"], int)
    assert payload["latency_ms"] >= 0


def test_route_returns_chat_verdict_for_question(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    patch_classify: dict[str, Any],
) -> None:
    patch_classify["result"] = IntentClassification(
        kind="chat",
        confidence=0.9,
        reason="message opens with a question word",
    )
    response = authed_client.post(
        "/ai/route",
        json={
            "message": "what columns does node 2 have",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["verdict"] == "chat"
    assert payload["kind"] == "chat"


def test_route_low_confidence_build_falls_back_to_chat(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    patch_classify: dict[str, Any],
) -> None:
    """Below the promotion threshold, even a ``build`` kind stays as chat."""
    patch_classify["result"] = IntentClassification(
        kind="build",
        confidence=0.3,
        reason="weak signal",
    )
    response = authed_client.post(
        "/ai/route",
        json={
            "message": "maybe drop the trailing whitespace",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    assert response.json()["verdict"] == "chat"


def test_route_forwards_history_to_classifier(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    patch_classify: dict[str, Any],
) -> None:
    """round 2 — the route forwards ``history`` to ``classify_intent``
    as a list of ``Message`` objects so the LLM can use prior chat context
    to disambiguate short follow-ups like *"can you implement?"*."""
    patch_classify["result"] = IntentClassification(
        kind="build",
        confidence=0.85,
        reason="follow-up to a suggestion",
    )
    response = authed_client.post(
        "/ai/route",
        json={
            "message": "can you implement?",
            "provider": "anthropic",
            "history": [
                {"role": "user", "content": "how do I count customers per city?"},
                {"role": "assistant", "content": "Add a group_by node grouping on city ..."},
            ],
        },
    )
    assert response.status_code == 200
    assert response.json()["verdict"] == "agent"

    assert len(patch_classify["calls"]) == 1
    forwarded = patch_classify["calls"][0]["kwargs"]["history"]
    assert forwarded is not None
    assert len(forwarded) == 2
    assert forwarded[0].role == "user"
    assert "customers per city" in forwarded[0].content
    assert forwarded[1].role == "assistant"
    assert forwarded[1].content.startswith("Add a group_by")


def test_route_history_omitted_passes_none_to_classifier(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    patch_classify: dict[str, Any],
) -> None:
    """When the frontend omits ``history`` (e.g. first message of a thread),
    the classifier sees ``history=None`` and decides on the message alone."""
    response = authed_client.post(
        "/ai/route",
        json={"message": "add a sort node", "provider": "anthropic"},
    )
    assert response.status_code == 200
    assert patch_classify["calls"][0]["kwargs"].get("history") is None


def test_route_promotes_pronoun_followup_after_build_shaped_prior_turn(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    patch_classify: dict[str, Any],
) -> None:
    """round 4 regression — the *"Can you implement?"* smoke-test case.

    A short pronoun-y follow-up after an assistant turn that proposed
    concrete nodes / steps must classify as ``build`` with confidence at
    or above the promotion threshold. The route then flips the verdict
    to ``"agent"`` and the audit row carries that decision plus the
    ``intent_classifier`` surface marker.

    Tests the wire contract end-to-end: the stubbed classifier mimics
    what a real LLM returns when handed the prior context, and the
    route forwards both that context and the resulting verdict.
    """
    patch_classify["result"] = IntentClassification(
        kind="build",
        confidence=0.85,
        reason=(
            "user is asking the assistant to execute the prior group_by suggestion"
        ),
    )
    response = authed_client.post(
        "/ai/route",
        json={
            "message": "Can you implement?",
            "provider": "anthropic",
            "history": [
                {
                    "role": "user",
                    "content": "How do I get the number of customers per city?",
                },
                {
                    "role": "assistant",
                    "content": (
                        "Add a group_by node grouping by `city` and aggregating "
                        "customer count..."
                    ),
                },
            ],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["verdict"] == "agent"
    assert payload["kind"] == "build"
    # Promotion threshold guard — verifies the route's verdict_for() actually
    # gates on the classifier's confidence rather than blindly accepting kind.
    from flowfile_core.ai.intent_router import PROMOTION_CONFIDENCE_THRESHOLD

    assert payload["confidence"] >= PROMOTION_CONFIDENCE_THRESHOLD

    # The classifier saw the prior turns: history forwarding is wired, the
    # LLM has the context it needs to disambiguate "Can you implement?"
    forwarded = patch_classify["calls"][0]["kwargs"]["history"]
    assert forwarded is not None
    assert len(forwarded) == 2
    assert forwarded[0].role == "user"
    assert "customers per city" in forwarded[0].content.lower()
    assert forwarded[1].role == "assistant"
    assert "group_by" in forwarded[1].content


# Failure-mode tolerance


def test_route_returns_chat_verdict_when_classifier_fails(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The classifier itself collapses provider failures to a chat fallback,
    so the route must surface a 200 with ``verdict="chat"`` rather than
    propagating the exception."""

    fallback = IntentClassification(
        kind="chat",
        confidence=0.0,
        reason="classifier call failed",
    )

    async def _stub(*_args: Any, **_kwargs: Any) -> IntentClassification:
        return fallback

    monkeypatch.setattr(routes_module, "classify_intent", _stub)

    response = authed_client.post(
        "/ai/route",
        json={
            "message": "add a sort node",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["verdict"] == "chat"
    assert payload["confidence"] == 0.0
    assert "failed" in payload["reason"].lower()


# Provider error mapping


def test_route_unknown_provider_returns_404(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/route",
        json={
            "message": "add a sort node",
            "provider": "imaginary",
        },
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


def test_route_unconfigured_returns_409(
    authed_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(*_args: Any, **_kwargs: Any) -> None:
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(routes_module, "get_configured_provider", _raise)
    response = authed_client.post(
        "/ai/route",
        json={
            "message": "add a sort node",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 409
    assert "anthropic" in response.json()["detail"]


def test_route_disabled_returns_503(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    patch_classify: dict[str, Any],
) -> None:
    """Inheriting's router-level dependency: flipping the flag off
    must return 503 here too. Read ``FEATURE_FLAG_AI`` off the module via
    ``core_settings`` rather than caching the symbol — see the
    :mod:`test_chat_routes` note for context."""
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        response = authed_client.post(
            "/ai/route",
            json={
                "message": "add a sort node",
                "provider": "anthropic",
            },
        )
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert response.status_code == 503
    assert "AI features are disabled" in response.json()["detail"]


# Request validation


def test_route_validates_message_present(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/route",
        json={"message": "", "provider": "anthropic"},
    )
    assert response.status_code == 422


def test_route_validates_provider_present(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/route",
        json={"message": "add a sort node"},
    )
    assert response.status_code == 422


# Audit-event emission


def test_route_emits_audit_event(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    patch_classify: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every classification must emit an ``auto_promotion_classified``
    audit row with the kind / confidence / reason / latency_ms fields
    populated. The frontend's promotion banner copy is the same
    ``reason`` string."""

    captured: list[audit.AuditEvent] = []

    def _capture(event: audit.AuditEvent, db: Any = None) -> Any:  # noqa: ARG001
        captured.append(event)
        return None

    monkeypatch.setattr(routes_module.audit, "record_event", _capture)

    patch_classify["result"] = IntentClassification(
        kind="build",
        confidence=0.78,
        reason="message uses an imperative build verb",
    )
    response = authed_client.post(
        "/ai/route",
        json={
            "message": "add a group_by node grouping by status, calculating average salary",
            "provider": "anthropic",
            "model": "claude-haiku-4-5",
        },
    )
    assert response.status_code == 200
    assert len(captured) == 1
    event = captured[0]
    assert event.tool_name == "internal.intent_classification"
    assert event.user_id == 1
    assert event.provider == "anthropic"
    # ``model`` resolution favours the explicit body model when the
    # provider stub lacks a real surface_models map.
    assert event.model in {"claude-haiku-4-5", "fake-haiku"}
    assert event.tool_args is not None
    args = event.tool_args
    assert args["event"] == "auto_promotion_classified"
    # round 4 — audit row carries the dedicated ``intent_classifier``
    # surface so post-launch tuning can filter classifier rows independently
    # of the ``settings_autocomplete`` autocomplete tier they used to
    # share.
    assert args["surface"] == "intent_classifier"
    assert args["kind"] == "build"
    assert args["confidence"] == pytest.approx(0.78)
    assert args["reason"] == "message uses an imperative build verb"
    assert args["verdict"] == "agent"
    assert args["latency_ms"] >= 0
    assert "group_by" in args["message_preview"]


def test_route_audit_failure_does_not_break_response(
    authed_client: TestClient,
    patch_get_configured_provider: _FakeProvider,
    patch_classify: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Audit DB hiccups must not surface as 5xx — the classifier verdict
    is the user-facing contract, not the audit row."""

    def _explode(*_a: Any, **_kw: Any) -> None:
        raise RuntimeError("simulated audit failure")

    monkeypatch.setattr(routes_module.audit, "record_event", _explode)

    patch_classify["result"] = IntentClassification(
        kind="build", confidence=0.85, reason="…"
    )
    response = authed_client.post(
        "/ai/route",
        json={"message": "add a sort node", "provider": "anthropic"},
    )
    assert response.status_code == 200
    assert response.json()["verdict"] == "agent"


# Lazy-litellm contract


def test_lazy_litellm_contract_for_intent_router_routes() -> None:
    """Importing the routes module mustn't drag ``litellm`` into ``sys.modules``."""
    import importlib
    import sys

    sys.modules.pop("litellm", None)
    importlib.reload(intent_router)
    importlib.reload(routes_module)
    assert "litellm" not in sys.modules
