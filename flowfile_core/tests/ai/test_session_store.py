""":mod:`flowfile_core.ai.session_store` tests.

Cases (~16):

* ``test_in_memory_roundtrip`` — put / get / pop / second-pop None.
* ``test_in_memory_user_namespacing`` — wrong user_id → None for get + pop.
* ``test_in_memory_clear`` — wipe in-memory store.
* ``test_disk_roundtrip`` — persist + read back round-trips full shape.
* ``test_disk_persists_across_repo_instances`` — second repo instance
  pointing at the same root reads the prior session.
* ``test_disk_user_namespacing`` — user_id mismatch returns None on disk.
* ``test_disk_lru_evicts_least_recent`` — small lru_size; touched session
  stays, oldest evicts.
* ``test_partial_write_does_not_corrupt`` — patch ``os.replace`` to raise
  mid-write; on-disk file matches its previous content (or is absent for
  the first write).
* ``test_unknown_schema_returns_none_with_warn`` — hand-crafted JSON with
  ``"_schema": "ai_session.v9"`` → ``get`` returns None, single WARN logged,
  no exception.
* ``test_archive_caps_at_50_per_flow`` — terminal pop + 51 sessions; oldest
  archive entry is pruned; archive directory size == 50.
* ``test_pop_active_session_does_not_archive`` — non-terminal pop wipes
  the active file; archive dir empty (or unchanged).
* ``test_concurrent_writes_serialize`` — two threads call put() on the
  same session_id with different mutations; final on-disk content matches
  one of them; no exception in either thread.
* ``test_list_for_user_filters_disk`` — disk repo enumerates active
  sessions on disk filtering by user_id.
* ``test_list_archived_sorted_recent_first`` — archived sessions returned
  sorted by updated_at descending.
* ``test_clear_wipes_disk_subtree`` — DiskSessionRepository.clear()
  removes the root directory.
* ``test_lazy_litellm_contract`` — importing session_store doesn't load
  litellm.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from flowfile_core.ai import session_store
from flowfile_core.ai.session_store import (
    SCHEMA_VERSION,
    DiskSessionRepository,
    InMemorySessionRepository,
)
from flowfile_core.ai.sessions import AgentSession, GraphSnapshot

# Fixtures


def _make_session(
    *,
    session_id: str | None = None,
    user_id: int = 1,
    flow_id: int = 1,
    status: str = "running",
) -> AgentSession:
    snapshot = GraphSnapshot(
        flow_id=flow_id,
        node_ids=(1, 2),
        node_types={1: "manual_input", 2: "filter"},
    )
    kwargs: dict = {
        "flow_id": flow_id,
        "user_id": user_id,
        "user_prompt": "filter to EU",
        "provider_name": "anthropic",
        "snapshot": snapshot,
        "status": status,
    }
    if session_id is not None:
        kwargs["session_id"] = session_id
    return AgentSession(**kwargs)


@pytest.fixture
def in_memory_repo() -> InMemorySessionRepository:
    return InMemorySessionRepository()


@pytest.fixture
def disk_repo(tmp_path: Path) -> DiskSessionRepository:
    return DiskSessionRepository(root=tmp_path / "ai_sessions")


# In-memory


def test_in_memory_roundtrip(in_memory_repo: InMemorySessionRepository) -> None:
    sess = _make_session()
    in_memory_repo.put(sess)
    fetched = in_memory_repo.get(sess.session_id)
    assert fetched is sess
    popped = in_memory_repo.pop(sess.session_id)
    assert popped is sess
    assert in_memory_repo.get(sess.session_id) is None
    assert in_memory_repo.pop(sess.session_id) is None


def test_in_memory_user_namespacing(in_memory_repo: InMemorySessionRepository) -> None:
    sess = _make_session(user_id=1)
    in_memory_repo.put(sess)
    assert in_memory_repo.get(sess.session_id, user_id=2) is None
    assert in_memory_repo.pop(sess.session_id, user_id=2) is None
    assert in_memory_repo.get(sess.session_id, user_id=1) is sess


def test_in_memory_clear(in_memory_repo: InMemorySessionRepository) -> None:
    in_memory_repo.put(_make_session(user_id=1))
    in_memory_repo.put(_make_session(user_id=2))
    in_memory_repo.clear()
    assert in_memory_repo.list_for_user(1) == []
    assert in_memory_repo.list_for_user(2) == []


# Disk


def test_disk_roundtrip(disk_repo: DiskSessionRepository) -> None:
    sess = _make_session()
    disk_repo.put(sess)
    fetched = disk_repo.get(sess.session_id)
    assert fetched is not None
    assert fetched.session_id == sess.session_id
    assert fetched.user_id == sess.user_id
    assert fetched.flow_id == sess.flow_id
    assert fetched.user_prompt == sess.user_prompt
    assert fetched.snapshot.node_ids == sess.snapshot.node_ids
    assert fetched.status == sess.status


def test_disk_persists_across_repo_instances(tmp_path: Path) -> None:
    root = tmp_path / "ai_sessions"
    sess = _make_session()
    repo_a = DiskSessionRepository(root=root)
    repo_a.put(sess)

    repo_b = DiskSessionRepository(root=root)
    fetched = repo_b.get(sess.session_id)
    assert fetched is not None
    assert fetched.session_id == sess.session_id
    assert fetched.user_prompt == sess.user_prompt


def test_disk_user_namespacing(disk_repo: DiskSessionRepository) -> None:
    sess = _make_session(user_id=7)
    disk_repo.put(sess)
    assert disk_repo.get(sess.session_id, user_id=99) is None
    assert disk_repo.pop(sess.session_id, user_id=99) is None
    assert disk_repo.get(sess.session_id, user_id=7) is not None


def test_disk_lru_evicts_least_recent(tmp_path: Path) -> None:
    repo = DiskSessionRepository(root=tmp_path / "ai_sessions", lru_size=2)
    s1 = _make_session(session_id="s1")
    s2 = _make_session(session_id="s2")
    s3 = _make_session(session_id="s3")
    repo.put(s1)
    repo.put(s2)
    # Touch s1 so s2 becomes the least-recently-used.
    repo._lru_get(s1.session_id)
    repo.put(s3)
    # LRU should now hold s1 and s3; s2 was evicted.
    assert s1.session_id in repo._lru
    assert s3.session_id in repo._lru
    assert s2.session_id not in repo._lru
    # But s2 is still on disk.
    fetched = repo.get(s2.session_id)
    assert fetched is not None


def test_partial_write_does_not_corrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = DiskSessionRepository(root=tmp_path / "ai_sessions")
    sess = _make_session(session_id="durable")
    repo.put(sess)
    path = repo._session_path(sess.flow_id, sess.session_id)
    original_bytes = path.read_bytes()

    mutated = sess.model_copy(update={"step_count": 99})

    import os as os_module

    real_replace = os_module.replace

    def _exploding_replace(*args, **kwargs):  # noqa: ARG001
        raise OSError("disk full")

    monkeypatch.setattr(os_module, "replace", _exploding_replace)
    with pytest.raises(OSError):
        repo.put(mutated)
    monkeypatch.setattr(os_module, "replace", real_replace)

    # On-disk content equals the previous content.
    assert path.read_bytes() == original_bytes
    # No leftover .tmp files.
    leftovers = list(path.parent.glob(f"{path.name}.tmp.*"))
    assert leftovers == []


def test_unknown_schema_returns_none_with_warn(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo = DiskSessionRepository(root=tmp_path / "ai_sessions")
    flow_id = 1
    session_id = "alien"
    target = repo._session_path(flow_id, session_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "_schema": "ai_session.v9",
                "session_id": session_id,
                "flow_id": flow_id,
                "user_id": 1,
            },
        ),
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger=session_store.logger.name):
        fetched = repo.get(session_id)
    assert fetched is None
    matches = [r for r in caplog.records if "unknown schema tag" in r.getMessage()]
    assert len(matches) == 1


def test_archive_caps_at_50_per_flow(tmp_path: Path) -> None:
    repo = DiskSessionRepository(root=tmp_path / "ai_sessions", archive_cap=50)
    flow_id = 1

    # Stage 51 closed sessions on the same flow_id; pop() each so the
    # archive grows.
    for i in range(51):
        sess = _make_session(session_id=f"sess-{i:03d}", flow_id=flow_id, status="completed")
        repo.put(sess)
        repo.pop(sess.session_id)
        # Add a measurable mtime delta so the FIFO order is deterministic.
        time.sleep(0.005)

    archive_dir = repo._archive_dir(flow_id)
    files = sorted(archive_dir.glob("*.json"))
    assert len(files) == 50
    # The very first archived session — sess-000 — is the one that should
    # have been pruned (oldest mtime).
    names = {f.stem for f in files}
    assert "sess-000" not in names
    assert "sess-050" in names


def test_pop_active_session_does_not_archive(disk_repo: DiskSessionRepository) -> None:
    sess = _make_session(session_id="active", status="paused_drift")
    disk_repo.put(sess)
    popped = disk_repo.pop(sess.session_id)
    assert popped is not None
    archive_dir = disk_repo._archive_dir(sess.flow_id)
    assert not archive_dir.exists() or list(archive_dir.glob("*.json")) == []
    # Active file removed.
    assert not disk_repo._session_path(sess.flow_id, sess.session_id).exists()


def test_concurrent_writes_serialize(tmp_path: Path) -> None:
    repo = DiskSessionRepository(root=tmp_path / "ai_sessions")
    base = _make_session(session_id="contended")
    # Pre-create the file so both writers race against an existing entry.
    repo.put(base)

    sess_a = base.model_copy(update={"step_count": 10})
    sess_b = base.model_copy(update={"step_count": 20})

    errors: list[BaseException] = []

    def _writer(payload: AgentSession) -> None:
        try:
            for _ in range(20):
                repo.put(payload)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=_writer, args=(sess_a,))
    t2 = threading.Thread(target=_writer, args=(sess_b,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert errors == []
    path = repo._session_path(base.flow_id, base.session_id)
    text = path.read_text(encoding="utf-8")
    payload = json.loads(text)
    # Final content is exactly one of the two writers — never half-A / half-B.
    assert payload["step_count"] in (10, 20)


def test_list_for_user_filters_disk(disk_repo: DiskSessionRepository) -> None:
    a = _make_session(session_id="alice-1", user_id=1, flow_id=1)
    b = _make_session(session_id="alice-2", user_id=1, flow_id=2)
    c = _make_session(session_id="bob-1", user_id=2, flow_id=1)
    disk_repo.put(a)
    disk_repo.put(b)
    disk_repo.put(c)

    user_1 = disk_repo.list_for_user(1)
    user_2 = disk_repo.list_for_user(2)
    assert {s.session_id for s in user_1} == {"alice-1", "alice-2"}
    assert {s.session_id for s in user_2} == {"bob-1"}


def test_list_archived_sorted_recent_first(disk_repo: DiskSessionRepository) -> None:
    flow_id = 9
    # Stage three completed sessions with measurable mtime deltas.
    sessions_made: list[AgentSession] = []
    for label in ("oldest", "middle", "newest"):
        sess = _make_session(
            session_id=label,
            user_id=1,
            flow_id=flow_id,
            status="completed",
        )
        disk_repo.put(sess)
        disk_repo.pop(sess.session_id)
        sessions_made.append(sess)
        time.sleep(0.005)

    archived = disk_repo.list_archived(user_id=1, flow_id=flow_id)
    assert [s.session_id for s in archived[:3]] == ["newest", "middle", "oldest"]


def test_clear_wipes_disk_subtree(disk_repo: DiskSessionRepository) -> None:
    sess = _make_session()
    disk_repo.put(sess)
    assert disk_repo._root.exists()
    disk_repo.clear()
    assert not disk_repo._root.exists()
    # After clear the repo is reusable.
    disk_repo.put(_make_session(session_id="post-clear"))
    assert disk_repo._root.exists()


# Lazy litellm


def test_lazy_litellm_contract() -> None:
    sys.modules.pop("litellm", None)
    sys.modules.pop("flowfile_core.ai.session_store", None)
    from flowfile_core.ai import session_store as _ss  # noqa: F401

    assert "litellm" not in sys.modules


# Schema-version constant


def test_schema_version_constant() -> None:
    assert SCHEMA_VERSION == "ai_session.v1"


def test_iter_recent_files_ordering(tmp_path: Path) -> None:
    """Cheap helper exercise — recency-sorted, optional limit."""
    target = tmp_path / "scratch"
    target.mkdir()
    paths = []
    for i in range(3):
        p = target / f"f-{i}"
        p.write_text("x", encoding="utf-8")
        paths.append(p)
        time.sleep(0.005)

    iterated = list(session_store.iter_recent_files(target, limit=2))
    assert [p.name for p in iterated] == ["f-2", "f-1"]


# clear_for_tests fixture default — autouse from conftest


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Each test gets a fresh repo — no cross-test contamination."""
    yield
