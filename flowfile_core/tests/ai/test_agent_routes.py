"""W40 — :mod:`flowfile_core.ai.agent_routes` tests.

Cases:

* ``test_start_streams_planner_events`` — happy path; SSE body carries
  ``event: complete`` with a ``diff_id`` payload.
* ``test_start_404_unknown_provider`` — bogus provider name → 404.
* ``test_start_404_missing_flow`` — flow_id with no registered flow → 404.
* ``test_start_409_unconfigured`` — provider raises ``ProviderNotConfiguredError`` → 409.
* ``test_start_422_provider_no_tools`` — patched provider with
  ``supports_tools=False`` → 422.
* ``test_start_422_session_id_collision`` — session_id reused → 422.
* ``test_resume_continue_streams`` — paused-drift session resume → SSE.
* ``test_resume_discard_returns_json`` — pop session, return JSON.
* ``test_resume_409_not_paused`` — running session can't resume → 409.
* ``test_resume_404_unknown_session`` — bogus id → 404.
* ``test_resume_404_cross_user`` — owned by user 2, called by user 1 → 404
  (no leak).
* ``test_abort_idempotent_on_completed`` — abort on completed session
  returns 200 with the recorded final state.
* ``test_abort_404_unknown`` — bogus session_id → 404.
* ``test_get_state_404_unknown`` — bogus session_id → 404.
* ``test_get_state_returns_snapshot`` — start a session, GET → status
  snapshot, no internal messages list.
* ``test_503_when_feature_flag_off`` — toggle FF off → 503 across all
  routes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import agent_routes as agent_routes_module
from flowfile_core.ai import sessions
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.providers.base import ChatResponse, ToolCall, Usage
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs.settings import FEATURE_FLAG_AI
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas

# --------------------------------------------------------------------------- #
# Test helpers                                                                 #
# --------------------------------------------------------------------------- #


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_w40_routes",
    )


def _add_orders(flow: FlowGraph, node_id: int = 1) -> None:
    raw = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData(
            columns=[
                input_schema.MinimalFieldInfo(name="region", data_type="String"),
            ],
            data=[["EU", "US"]],
        ),
    )
    flow.add_manual_input(raw)
    flow.get_node(node_id).get_predicted_schema()


def _flow_with_orders(flow_id: int = 1) -> FlowGraph:
    flow = FlowGraph(flow_settings=_flow_settings(flow_id), name="w40_routes_test")
    _add_orders(flow)
    return flow


class _FakeProvider:
    """Scripted-response provider matching the planner contract."""

    name = "anthropic"
    model = "fake"
    supports_tools = True
    supports_streaming = True

    def __init__(self, tool_calls_per_step: list[list[ToolCall]] | None = None) -> None:
        self._steps = list(tool_calls_per_step or [])
        # Append a final "stop" turn so the loop terminates.
        self._steps.append([])

    async def chat(self, **_kw: Any) -> ChatResponse:
        if not self._steps:
            return ChatResponse(model=self.model, content="done", tool_calls=[], finish_reason="stop", usage=Usage())
        tool_calls = self._steps.pop(0)
        return ChatResponse(
            model=self.model,
            content=None,
            tool_calls=tool_calls,
            finish_reason="tool_calls" if tool_calls else "stop",
            usage=Usage(),
        )

    def stream(self, *_a: Any, **_kw: Any) -> AsyncIterator:
        raise AssertionError("planner uses chat() only")


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    fake = PydanticUser(id=1, username="local_user")
    main.app.dependency_overrides[get_current_active_user] = lambda: fake
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


@pytest.fixture
def registered_flow() -> Iterator[FlowGraph]:
    flow = _flow_with_orders()
    flow_file_handler._flows[flow.flow_id] = flow
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    sessions.clear_for_tests()
    yield
    sessions.clear_for_tests()


@pytest.fixture
def patch_provider(monkeypatch: pytest.MonkeyPatch) -> Iterator[_FakeProvider]:
    """Patch ``agent_routes.get_configured_provider`` with a fake."""
    fake = _FakeProvider()

    def _factory(*_a: Any, **_kw: Any) -> _FakeProvider:
        return fake

    monkeypatch.setattr(agent_routes_module, "get_configured_provider", _factory)
    yield fake


def _filter_args() -> dict[str, Any]:
    return {
        "filter_input": {
            "filter_type": "advanced",
            "advanced_filter": "[region]=='EU'",
        },
    }


# --------------------------------------------------------------------------- #
# Start                                                                        #
# --------------------------------------------------------------------------- #


def test_start_streams_planner_events(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeProvider(
        tool_calls_per_step=[[ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())]]
    )
    monkeypatch.setattr(agent_routes_module, "get_configured_provider", lambda *_a, **_kw: fake)

    response = authed_client.post(
        "/ai/agent/start",
        json={
            "flow_id": 1,
            "prompt": "filter to EU",
            "surface": "agent_complex",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    body = response.text
    assert "event: tool_call_proposed" in body
    assert "event: tool_call_staged" in body
    assert "event: complete" in body


def test_start_404_unknown_provider(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    response = authed_client.post(
        "/ai/agent/start",
        json={
            "flow_id": 1,
            "prompt": "filter",
            "provider": "imaginary",
        },
    )
    assert response.status_code == 404
    assert "imaginary" in response.json()["detail"]


def test_start_404_missing_flow(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/agent/start",
        json={
            "flow_id": 9999,
            "prompt": "filter",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 404


def test_start_409_unconfigured(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise(*_a: Any, **_kw: Any) -> None:
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(agent_routes_module, "get_configured_provider", _raise)
    response = authed_client.post(
        "/ai/agent/start",
        json={
            "flow_id": 1,
            "prompt": "filter",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 409


def test_start_422_provider_no_tools(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeProvider()
    fake.supports_tools = False  # type: ignore[assignment]
    monkeypatch.setattr(agent_routes_module, "get_configured_provider", lambda *_a, **_kw: fake)

    response = authed_client.post(
        "/ai/agent/start",
        json={
            "flow_id": 1,
            "prompt": "filter",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 422
    assert "tool-calling" in response.json()["detail"].lower()


def test_start_422_session_id_collision(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    patch_provider: _FakeProvider,
) -> None:
    # Pre-register a session with a known id.
    snap = sessions.capture_graph_snapshot(registered_flow)
    sessions.register_session(
        sessions.AgentSession(
            session_id="taken-id",
            flow_id=1,
            user_id=1,
            user_prompt="prior",
            provider_name="anthropic",
            snapshot=snap,
        )
    )
    response = authed_client.post(
        "/ai/agent/start",
        json={
            "flow_id": 1,
            "prompt": "filter",
            "provider": "anthropic",
            "session_id": "taken-id",
        },
    )
    assert response.status_code == 422


# --------------------------------------------------------------------------- #
# Resume                                                                       #
# --------------------------------------------------------------------------- #


def test_resume_discard_returns_json(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    patch_provider: _FakeProvider,
) -> None:
    snap = sessions.capture_graph_snapshot(registered_flow)
    sess = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snap,
        status="paused_drift",
    )
    sessions.register_session(sess)

    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/resume",
        json={"action": "discard"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "discarded"
    assert body["session_id"] == sess.session_id
    # Popped from store.
    assert sessions.get_session(sess.session_id) is None


def test_resume_continue_streams(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeProvider(tool_calls_per_step=[])  # just an empty/stop turn
    monkeypatch.setattr(agent_routes_module, "get_configured_provider", lambda *_a, **_kw: fake)

    snap = sessions.capture_graph_snapshot(registered_flow)
    sess = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snap,
        status="paused_drift",
    )
    sessions.register_session(sess)

    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/resume",
        json={"action": "continue"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    body = response.text
    assert "event: info" in body  # the resume re-snapshot info event
    assert "event: complete" in body


def test_resume_409_not_paused(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    patch_provider: _FakeProvider,
) -> None:
    snap = sessions.capture_graph_snapshot(registered_flow)
    sess = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snap,
        status="running",
    )
    sessions.register_session(sess)
    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/resume",
        json={"action": "discard"},
    )
    assert response.status_code == 409


def test_resume_404_unknown_session(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/agent/no-such-id/resume",
        json={"action": "discard"},
    )
    assert response.status_code == 404


def test_resume_404_cross_user(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    patch_provider: _FakeProvider,
) -> None:
    # Session owned by user 2; the authed client is user 1.
    snap = sessions.capture_graph_snapshot(registered_flow)
    sess = sessions.AgentSession(
        flow_id=1,
        user_id=2,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snap,
        status="paused_drift",
    )
    sessions.register_session(sess)
    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/resume",
        json={"action": "discard"},
    )
    assert response.status_code == 404


# --------------------------------------------------------------------------- #
# Abort                                                                        #
# --------------------------------------------------------------------------- #


def test_abort_idempotent_on_completed(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    snap = sessions.capture_graph_snapshot(registered_flow)
    sess = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snap,
        status="completed",
        diff_id="d-123",
    )
    sessions.register_session(sess)

    response = authed_client.post(f"/ai/agent/{sess.session_id}/abort")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "aborted"
    assert body["partial_diff_id"] == "d-123"
    # Status is preserved as completed (not flipped to aborted).
    assert sessions.get_session(sess.session_id).status == "completed"


def test_abort_404_unknown(authed_client: TestClient) -> None:
    response = authed_client.post("/ai/agent/no-such-id/abort")
    assert response.status_code == 404


def test_abort_flips_running_session(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    snap = sessions.capture_graph_snapshot(registered_flow)
    sess = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snap,
        status="running",
    )
    sessions.register_session(sess)
    response = authed_client.post(f"/ai/agent/{sess.session_id}/abort")
    assert response.status_code == 200
    assert sessions.get_session(sess.session_id).status == "aborted"


# --------------------------------------------------------------------------- #
# Get state                                                                    #
# --------------------------------------------------------------------------- #


def test_get_state_404_unknown(authed_client: TestClient) -> None:
    response = authed_client.get("/ai/agent/no-such-id")
    assert response.status_code == 404


def test_get_state_returns_snapshot(authed_client: TestClient, registered_flow: FlowGraph) -> None:
    snap = sessions.capture_graph_snapshot(registered_flow)
    sess = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snap,
        status="running",
        step_count=2,
    )
    sessions.register_session(sess)
    response = authed_client.get(f"/ai/agent/{sess.session_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["session_id"] == sess.session_id
    assert body["status"] == "running"
    assert body["step_count"] == 2
    assert body["staged_count"] == 0
    # Internal fields not exposed.
    assert "messages" not in body
    assert "snapshot" not in body


# --------------------------------------------------------------------------- #
# Feature flag                                                                 #
# --------------------------------------------------------------------------- #


def test_503_when_feature_flag_off(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    patch_provider: _FakeProvider,
) -> None:
    original = FEATURE_FLAG_AI.value
    FEATURE_FLAG_AI.set(False)
    try:
        # Start
        r1 = authed_client.post(
            "/ai/agent/start",
            json={"flow_id": 1, "prompt": "x", "provider": "anthropic"},
        )
        assert r1.status_code == 503

        # Abort
        r2 = authed_client.post("/ai/agent/anything/abort")
        assert r2.status_code == 503

        # Get
        r3 = authed_client.get("/ai/agent/anything")
        assert r3.status_code == 503

        # Resume
        r4 = authed_client.post("/ai/agent/anything/resume", json={"action": "discard"})
        assert r4.status_code == 503

        # Followup (W49)
        r5 = authed_client.post(
            "/ai/agent/anything/followup",
            json={"action": "user_message", "message": "x"},
        )
        assert r5.status_code == 503
    finally:
        FEATURE_FLAG_AI.set(original)


# --------------------------------------------------------------------------- #
# W49 — Followup endpoint                                                      #
# --------------------------------------------------------------------------- #


def _completed_session(flow_id: int = 1, *, status: str = "completed") -> sessions.AgentSession:
    snap = sessions.GraphSnapshot(flow_id=flow_id, node_ids=(1,), node_types={1: "manual_input"})
    return sessions.AgentSession(
        flow_id=flow_id,
        user_id=1,
        user_prompt="prior",
        provider_name="anthropic",
        snapshot=snap,
        status=status,  # type: ignore[arg-type]
    )


def test_w49_followup_404_unknown_session(authed_client: TestClient) -> None:
    response = authed_client.post(
        "/ai/agent/no-such-id/followup",
        json={"action": "user_message", "message": "hi"},
    )
    assert response.status_code == 404


def test_w49_followup_409_when_session_running(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    patch_provider: _FakeProvider,
) -> None:
    sess = _completed_session(status="running")
    sessions.register_session(sess)
    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/followup",
        json={"action": "user_message", "message": "go"},
    )
    assert response.status_code == 409
    assert "followup-resumable" in response.json()["detail"]


def test_w49_followup_user_message_streams_sse(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mode 3 — completed-with-no-ops session + user_message → SSE re-entry."""
    fake = _FakeProvider(tool_calls_per_step=[])  # just emit a stop
    monkeypatch.setattr(agent_routes_module, "get_configured_provider", lambda *_a, **_kw: fake)

    sess = _completed_session(status="completed")
    sessions.register_session(sess)

    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/followup",
        json={"action": "user_message", "message": "please continue"},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    body = response.text
    assert "event: info" in body  # the followup-resume preamble info event
    assert "event: complete" in body or "event: awaiting_user_input" in body

    # Conversation history now carries the synthetic user message.
    refreshed = sessions.get_session(sess.session_id, user_id=1)
    assert refreshed is not None
    user_msgs = [m for m in refreshed.messages if m.role == "user"]
    assert any("please continue" in (m.content or "") for m in user_msgs)


def test_w49_followup_rejected_diff_injects_synthetic_user_turn(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mode 1 — rejected diff with a user note → synthetic ``user`` turn
    in the conversation carrying the note + diff_id reference."""
    fake = _FakeProvider(tool_calls_per_step=[])
    monkeypatch.setattr(agent_routes_module, "get_configured_provider", lambda *_a, **_kw: fake)

    sess = _completed_session()
    sess.diff_id = "diff-rejected"
    sessions.register_session(sess)

    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/followup",
        json={
            "action": "rejected_diff",
            "message": "please use the read node directly",
            "rejected_diff_id": "diff-rejected",
        },
    )
    assert response.status_code == 200

    refreshed = sessions.get_session(sess.session_id, user_id=1)
    assert refreshed is not None
    user_msgs = [m for m in refreshed.messages if m.role == "user"]
    rejection_msgs = [m for m in user_msgs if "rejected" in (m.content or "").lower()]
    assert rejection_msgs, "expected a synthetic rejection user message"
    content = rejection_msgs[-1].content
    assert "please use the read node directly" in content
    assert "diff-rejected" in content


def test_w49_followup_rejected_diff_generic_fallback_when_no_note(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Acceptance test 2 — empty rejection note → generic fallback text."""
    fake = _FakeProvider(tool_calls_per_step=[])
    monkeypatch.setattr(agent_routes_module, "get_configured_provider", lambda *_a, **_kw: fake)

    sess = _completed_session()
    sess.diff_id = "diff-rejected"
    sessions.register_session(sess)

    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/followup",
        json={"action": "rejected_diff", "rejected_diff_id": "diff-rejected"},
    )
    assert response.status_code == 200

    refreshed = sessions.get_session(sess.session_id, user_id=1)
    assert refreshed is not None
    rejection_msgs = [m for m in refreshed.messages if m.role == "user" and "rejected" in (m.content or "").lower()]
    assert rejection_msgs
    # Generic placeholder is included.
    assert "no specific reason" in rejection_msgs[-1].content.lower()


def test_w49_followup_user_message_empty_returns_422(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    patch_provider: _FakeProvider,
) -> None:
    """``user_message`` action with whitespace-only message → 422."""
    sess = _completed_session()
    sessions.register_session(sess)
    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/followup",
        json={"action": "user_message", "message": "   "},
    )
    assert response.status_code == 422


def test_w49_followup_404_cross_user(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    patch_provider: _FakeProvider,
) -> None:
    """Session owned by user 2 → user 1's followup returns 404 (no leak)."""
    snap = sessions.capture_graph_snapshot(registered_flow)
    sess = sessions.AgentSession(
        flow_id=1,
        user_id=2,  # different user
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snap,
        status="completed",
    )
    sessions.register_session(sess)
    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/followup",
        json={"action": "user_message", "message": "hi"},
    )
    assert response.status_code == 404


def test_w49_followup_accepts_awaiting_user_input(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mode 2 — session in ``awaiting_user_input`` accepts followup."""
    fake = _FakeProvider(tool_calls_per_step=[])
    monkeypatch.setattr(agent_routes_module, "get_configured_provider", lambda *_a, **_kw: fake)

    sess = _completed_session(status="awaiting_user_input")
    sessions.register_session(sess)

    response = authed_client.post(
        f"/ai/agent/{sess.session_id}/followup",
        json={"action": "user_message", "message": "filter on region"},
    )
    assert response.status_code == 200
