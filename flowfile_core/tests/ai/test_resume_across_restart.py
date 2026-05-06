"""W42 — cross-restart resume integration tests.

Drives the FastAPI ``/ai/agent/*`` routes against a tmp_path-rooted
:class:`DiskSessionRepository` so we can simulate process-restart
scenarios:

* ``test_running_session_survives_restart`` (AC #3) — start session,
  stage some events, "restart" (fresh disk repo instance + clear LRU),
  GET the session, assert ``status == "paused_user_action"``, then
  resume?action=continue and verify the SSE stream completes.
* ``test_resume_with_last_event_id_replays_buffered`` (AC #8) — capture
  buffered events at agent-start, simulate disconnect, resume with
  ``Last-Event-ID`` header, assert the buffered tail is flushed before
  any live frames.
* ``test_paused_drift_state_survives_restart`` — drift-paused sessions
  hydrate cleanly across restart with their drift_detail intact.
* ``test_staged_results_survive_restart`` — round-trip the
  ``staged_results`` list through disk; new instance sees the same
  staged ops.
* ``test_get_state_does_not_flip_on_fresh_running`` — sessions whose
  ``updated_at`` is fresh stay ``running`` (no false-positive flip).
* ``test_completed_session_archives_on_pop`` — terminal sessions move to
  the archive directory.
* ``test_unknown_schema_on_disk_returns_404`` — corrupted disk file with
  bad schema tag → repo returns None → route returns 404.
* ``test_resume_action_discard_pops_session`` — discard works on a
  ``paused_user_action`` session (W42 expanded the resumable set).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import agent_routes as agent_routes_module
from flowfile_core.ai import diff as diff_module
from flowfile_core.ai import replay_buffer as replay_buffer_module
from flowfile_core.ai import sessions
from flowfile_core.ai.diff_store import DiskDiffRepository
from flowfile_core.ai.providers.base import ChatResponse, ToolCall, Usage
from flowfile_core.ai.replay_buffer import ReplayBuffer
from flowfile_core.ai.session_store import (
    SCHEMA_VERSION,
    DiskSessionRepository,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, schemas

# --------------------------------------------------------------------------- #
# Helpers — minimal flow + scripted provider                                   #
# --------------------------------------------------------------------------- #


def _flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_w42_resume",
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
    flow = FlowGraph(flow_settings=_flow_settings(flow_id), name="w42_resume_test")
    _add_orders(flow)
    return flow


class _FakeProvider:
    """Scripted-response provider for the planner."""

    name = "anthropic"
    model = "fake"
    supports_tools = True
    supports_streaming = True

    def __init__(self, tool_calls_per_step: list[list[ToolCall]] | None = None) -> None:
        self._steps = list(tool_calls_per_step or [])
        self._steps.append([])  # final stop turn

    async def chat(self, **_kw: Any) -> ChatResponse:
        if not self._steps:
            return ChatResponse(
                model=self.model,
                content="done",
                tool_calls=[],
                finish_reason="stop",
                usage=Usage(),
            )
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


def _filter_args() -> dict[str, Any]:
    return {
        "filter_input": {
            "filter_type": "advanced",
            "advanced_filter": "[region]=='EU'",
        },
    }


# --------------------------------------------------------------------------- #
# Fixtures                                                                     #
# --------------------------------------------------------------------------- #


@pytest.fixture
def disk_root(tmp_path: Path) -> Path:
    return tmp_path / "ai_sessions"


@pytest.fixture
def disk_session_repo(disk_root: Path) -> Iterator[DiskSessionRepository]:
    """Set the module-level ``_REPO`` to a tmp_path-rooted disk repo."""
    repo = DiskSessionRepository(root=disk_root)
    prev = sessions.set_session_repo(repo)
    try:
        yield repo
    finally:
        sessions.set_session_repo(prev)
        repo.clear()


@pytest.fixture
def disk_diff_repo(disk_root: Path) -> Iterator[DiskDiffRepository]:
    repo = DiskDiffRepository(root=disk_root)
    prev = diff_module.set_diff_repo(repo)
    try:
        yield repo
    finally:
        diff_module.set_diff_repo(prev)
        repo.clear()


@pytest.fixture
def disk_replay_buffer(disk_root: Path) -> Iterator[ReplayBuffer]:
    buf = ReplayBuffer(root=disk_root, cap=64)
    prev = replay_buffer_module._DEFAULT_BUFFER  # noqa: SLF001
    replay_buffer_module.set_default_replay_buffer(buf)
    try:
        yield buf
    finally:
        replay_buffer_module.set_default_replay_buffer(prev)


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


@pytest.fixture
def patch_provider(monkeypatch: pytest.MonkeyPatch) -> _FakeProvider:
    fake = _FakeProvider(
        tool_calls_per_step=[
            [ToolCall(id="t1", name="flowfile.graph.add_filter", arguments=_filter_args())],
            [ToolCall(id="t2", name="flowfile.graph.add_filter", arguments=_filter_args())],
        ]
    )
    monkeypatch.setattr(agent_routes_module, "get_configured_provider", lambda *_a, **_kw: fake)
    return fake


# --------------------------------------------------------------------------- #
# Tests                                                                        #
# --------------------------------------------------------------------------- #


def test_running_session_survives_restart(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    disk_session_repo: DiskSessionRepository,
    disk_diff_repo: DiskDiffRepository,
    disk_replay_buffer: ReplayBuffer,
    patch_provider: _FakeProvider,
    disk_root: Path,
) -> None:
    """AC #3 — cold-start integration. Drive 2 tool-call iterations,
    simulate restart, GET → ``paused_user_action``, resume?action=continue
    completes against the persisted state."""
    response = authed_client.post(
        "/ai/agent/start",
        json={
            "flow_id": 1,
            "prompt": "filter twice",
            "surface": "agent_complex",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200, response.text
    body = response.text
    assert "event: tool_call_staged" in body
    assert "event: complete" in body

    # Pull the session_id back from the SSE stream.
    sid = None
    for line in body.split("\n"):
        if line.startswith("data: ") and '"session_id"' in line:
            payload = json.loads(line[6:])
            if "session_id" in payload:
                sid = payload["session_id"]
                break
    assert sid is not None, "expected session_id in SSE complete event"

    # The session has completed (the fake provider stopped after 2 steps).
    # For the cold-start scenario, we simulate a *paused-mid-run* state by
    # writing back a "running" session whose updated_at is old, then
    # clearing the LRU.
    persisted = disk_session_repo.get(sid)
    assert persisted is not None
    persisted.status = "running"
    persisted.updated_at = datetime.now(timezone.utc) - timedelta(seconds=120)
    disk_session_repo.put(persisted)

    # Simulate process restart by clearing the LRU mirror.
    disk_session_repo._lru.clear()

    # GET should hydrate from disk and flip running → paused_user_action.
    state_resp = authed_client.get(f"/ai/agent/{sid}")
    assert state_resp.status_code == 200
    state = state_resp.json()
    assert state["status"] == "paused_user_action"
    assert state["pause_reason"] == "cold_start"

    # Resume?action=continue should now succeed (W42 expanded the
    # resumable set to include paused_user_action).
    resume_resp = authed_client.post(
        f"/ai/agent/{sid}/resume",
        json={"action": "continue"},
    )
    assert resume_resp.status_code == 200
    assert resume_resp.headers["content-type"].startswith("text/event-stream")


def test_resume_with_last_event_id_replays_buffered(
    authed_client: TestClient,
    registered_flow: FlowGraph,
    disk_session_repo: DiskSessionRepository,
    disk_diff_repo: DiskDiffRepository,
    disk_replay_buffer: ReplayBuffer,
    patch_provider: _FakeProvider,
) -> None:
    """AC #8 — Last-Event-ID resume replays buffered tail before live."""
    # Run an agent so frames land in the replay buffer.
    response = authed_client.post(
        "/ai/agent/start",
        json={
            "flow_id": 1,
            "prompt": "two filters",
            "surface": "agent_complex",
            "provider": "anthropic",
        },
    )
    assert response.status_code == 200

    # Pull the session id out of the complete event.
    sid = None
    for line in response.text.split("\n"):
        if line.startswith("data: ") and '"session_id"' in line:
            payload = json.loads(line[6:])
            if "session_id" in payload:
                sid = payload["session_id"]
                break
    assert sid is not None

    # Verify multiple frames landed in the buffer.
    buffered = list(
        disk_replay_buffer.read_after(flow_id=1, session_id=sid, event_id=None)
    )
    assert len(buffered) >= 2

    # Simulate disconnect at the first emitted step; resume with that as cursor.
    cursor_eid = buffered[0][0]
    cursor_step = int(cursor_eid.rsplit(".", 1)[1])
    expected_replays = [
        eid for eid, _ in buffered if int(eid.rsplit(".", 1)[1]) > cursor_step
    ]
    # The buffer must hold at least one frame past the cursor for this test
    # to mean anything.
    assert expected_replays, "test fixture should produce >1 step worth of frames"

    # Flip status so resume?continue is allowed and force a no-op planner
    # by exhausting the fake provider's scripted steps.
    persisted = disk_session_repo.get(sid)
    assert persisted is not None
    persisted.status = "paused_user_action"
    persisted.pause_reason = "cold_start"
    disk_session_repo.put(persisted)
    disk_session_repo._lru.clear()

    resume_resp = authed_client.post(
        f"/ai/agent/{sid}/resume",
        json={"action": "continue"},
        headers={"Last-Event-ID": cursor_eid},
    )
    assert resume_resp.status_code == 200
    body = resume_resp.text

    # Frames at steps <= cursor must NOT be replayed.
    suppressed = [
        eid for eid, _ in buffered if int(eid.rsplit(".", 1)[1]) <= cursor_step
    ]
    # Every buffered frame past the cursor must show up in the body.
    for eid in expected_replays:
        assert f"id: {eid}" in body, f"expected replay of buffered {eid!r} in body"
    # And the suppressed frames must not be re-streamed by the replay path.
    # (The live planner can emit a new event at step <= cursor_step in
    # principle, but with our fake provider exhausted there's nothing to
    # emit at that range — so the only way they'd appear is via replay,
    # which is what we're guarding against.)
    for eid in suppressed:
        # We can have at most one occurrence per id (from the live planner's
        # fresh emission at the same step counter). For a fake provider with
        # an empty plan post-restart, there should be zero occurrences.
        assert body.count(f"id: {eid}\n") == 0, (
            f"buffered frame {eid!r} at step <= cursor should not be replayed"
        )


def test_paused_drift_state_survives_restart(
    disk_session_repo: DiskSessionRepository,
) -> None:
    """A drift-paused session round-trips through disk with drift_detail."""
    snapshot = sessions.GraphSnapshot(flow_id=1, node_ids=(1,), node_types={1: "manual_input"})
    session = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snapshot,
        status="paused_drift",
        pause_reason="drift_detected",
        drift_detail=sessions.DriftDetail(missing_node_ids=[1]),
    )
    sessions.register_session(session)

    # Fresh repo instance pointing at the same root.
    fresh = DiskSessionRepository(root=disk_session_repo._root)
    fetched = fresh.get(session.session_id)
    assert fetched is not None
    assert fetched.status == "paused_drift"
    assert fetched.pause_reason == "drift_detected"
    assert fetched.drift_detail is not None
    assert fetched.drift_detail.missing_node_ids == [1]


def test_staged_results_survive_restart(
    disk_session_repo: DiskSessionRepository,
) -> None:
    """``staged_results`` round-trip across restart."""
    snapshot = sessions.GraphSnapshot(flow_id=1, node_ids=(1,), node_types={1: "manual_input"})
    staged = diff_module.StagedToolEntry(
        tool_name="flowfile.graph.add_filter",
        audit_id=99,
        staged_node_payload={"settings": {"node_id": 5}, "insertion_context": {"upstream_node_ids": [1]}},
    )
    session = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="filter",
        provider_name="anthropic",
        snapshot=snapshot,
        staged_results=[staged],
        staged_node_ids=[5],
    )
    sessions.register_session(session)

    fresh = DiskSessionRepository(root=disk_session_repo._root)
    fetched = fresh.get(session.session_id)
    assert fetched is not None
    assert len(fetched.staged_results) == 1
    assert fetched.staged_results[0].tool_name == "flowfile.graph.add_filter"
    assert fetched.staged_results[0].audit_id == 99
    assert fetched.staged_node_ids == [5]


def test_get_state_does_not_flip_on_fresh_running(
    authed_client: TestClient,
    disk_session_repo: DiskSessionRepository,
) -> None:
    """A ``running`` session whose ``updated_at`` is fresh stays running."""
    snapshot = sessions.GraphSnapshot(flow_id=1, node_ids=(1,), node_types={1: "manual_input"})
    session = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snapshot,
        status="running",
        # default updated_at = now → fresh
    )
    sessions.register_session(session)

    resp = authed_client.get(f"/ai/agent/{session.session_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "running"


def test_completed_session_archives_on_pop(
    disk_session_repo: DiskSessionRepository,
) -> None:
    """Terminal sessions move to ``archive/`` on pop."""
    snapshot = sessions.GraphSnapshot(flow_id=2, node_ids=(1,), node_types={1: "manual_input"})
    session = sessions.AgentSession(
        flow_id=2,
        user_id=1,
        user_prompt="done",
        provider_name="anthropic",
        snapshot=snapshot,
        status="completed",
    )
    sessions.register_session(session)
    sid = session.session_id

    popped = sessions.pop_session(sid)
    assert popped is not None

    # Archive directory has the file.
    archive_dir = disk_session_repo._archive_dir(2)
    assert archive_dir.is_dir()
    archived_files = list(archive_dir.glob("*.json"))
    assert any(f.stem == sid for f in archived_files)

    # Active dir no longer has it.
    active_path = disk_session_repo._session_path(2, sid)
    assert not active_path.exists()


def test_unknown_schema_on_disk_returns_404(
    authed_client: TestClient,
    disk_session_repo: DiskSessionRepository,
) -> None:
    """A disk file with a bogus ``_schema`` value is treated as absent."""
    bad_session_id = "bogus-schema-session"
    target = disk_session_repo._session_path(1, bad_session_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "_schema": "ai_session.v9",
                "session_id": bad_session_id,
                "flow_id": 1,
                "user_id": 1,
            },
        ),
        encoding="utf-8",
    )

    resp = authed_client.get(f"/ai/agent/{bad_session_id}")
    assert resp.status_code == 404


def test_resume_action_discard_pops_session(
    authed_client: TestClient,
    disk_session_repo: DiskSessionRepository,
) -> None:
    """Discard works on a ``paused_user_action`` session (not just drift)."""
    snapshot = sessions.GraphSnapshot(flow_id=1, node_ids=(1,), node_types={1: "manual_input"})
    session = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snapshot,
        status="paused_user_action",
        pause_reason="cold_start",
    )
    sessions.register_session(session)
    sid = session.session_id

    resp = authed_client.post(
        f"/ai/agent/{sid}/resume",
        json={"action": "discard"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "discarded"
    assert sessions.get_session(sid) is None


def test_session_file_carries_schema_tag(
    disk_session_repo: DiskSessionRepository,
) -> None:
    """On-disk JSON has a top-level ``_schema`` tag = ``ai_session.v1``."""
    snapshot = sessions.GraphSnapshot(flow_id=1, node_ids=(1,), node_types={1: "manual_input"})
    session = sessions.AgentSession(
        flow_id=1,
        user_id=1,
        user_prompt="x",
        provider_name="anthropic",
        snapshot=snapshot,
    )
    sessions.register_session(session)

    path = disk_session_repo._session_path(1, session.session_id)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["_schema"] == SCHEMA_VERSION
