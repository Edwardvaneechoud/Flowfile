""":mod:`flowfile_core.ai.diff_store` tests.

Cases (~8):

* ``test_in_memory_roundtrip`` — put / get / pop / second-pop None.
* ``test_in_memory_clear`` — wipe.
* ``test_disk_roundtrip`` — persist + read back.
* ``test_disk_persists_across_repo_instances`` — second instance reads the
  prior diff after rebuilding its index.
* ``test_partial_write_does_not_corrupt`` — mid-write os.replace failure;
  on-disk file unchanged.
* ``test_unknown_schema_returns_none_with_warn`` — bogus ``_schema`` →
  None + WARN; no raise.
* ``test_concurrent_writes_serialize`` — two threads put() the same
  diff_id; final on-disk content matches one of them.
* ``test_clear_wipes_disk_subtree`` — clear() removes the root.
"""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

import pytest

from flowfile_core.ai import diff_store
from flowfile_core.ai.diff import GraphDiff, StagedAddition, StagedInsertionContext
from flowfile_core.ai.diff_store import (
    SCHEMA_VERSION,
    DiskDiffRepository,
    InMemoryDiffRepository,
)


def _make_diff(
    *,
    diff_id: str | None = None,
    session_id: str = "sess-1",
    flow_id: int = 1,
    rationale: str | None = "filter to EU",
) -> GraphDiff:
    addition = StagedAddition(
        node_type="filter",
        settings={"node_id": 5, "depending_on_id": 1, "filter_input": {}, "node_label": "filter"},
        insertion_context=StagedInsertionContext(upstream_node_ids=[1]),
        predicted_output_schema=None,
        audit_id=None,
    )
    kwargs: dict = {
        "session_id": session_id,
        "flow_id": flow_id,
        "additions": [addition],
        "rationale": rationale,
    }
    if diff_id is not None:
        kwargs["diff_id"] = diff_id
    return GraphDiff(**kwargs)


# --------------------------------------------------------------------------- #
# In-memory #
# --------------------------------------------------------------------------- #


def test_in_memory_roundtrip() -> None:
    repo = InMemoryDiffRepository()
    diff = _make_diff()
    repo.put(diff)
    assert repo.get(diff.diff_id) is diff
    popped = repo.pop(diff.diff_id)
    assert popped is diff
    assert repo.get(diff.diff_id) is None
    assert repo.pop(diff.diff_id) is None


def test_in_memory_clear() -> None:
    repo = InMemoryDiffRepository()
    repo.put(_make_diff(diff_id="a"))
    repo.put(_make_diff(diff_id="b"))
    repo.clear()
    assert repo.get("a") is None
    assert repo.get("b") is None


# --------------------------------------------------------------------------- #
# Disk #
# --------------------------------------------------------------------------- #


def test_disk_roundtrip(tmp_path: Path) -> None:
    repo = DiskDiffRepository(root=tmp_path / "ai_sessions")
    diff = _make_diff()
    repo.put(diff)
    fetched = repo.get(diff.diff_id)
    assert fetched is not None
    assert fetched.diff_id == diff.diff_id
    assert fetched.flow_id == diff.flow_id
    assert fetched.session_id == diff.session_id
    assert fetched.rationale == diff.rationale
    assert len(fetched.additions) == 1
    assert fetched.additions[0].node_type == "filter"


def test_disk_persists_across_repo_instances(tmp_path: Path) -> None:
    root = tmp_path / "ai_sessions"
    repo_a = DiskDiffRepository(root=root)
    diff = _make_diff()
    repo_a.put(diff)

    repo_b = DiskDiffRepository(root=root)
    fetched = repo_b.get(diff.diff_id)
    assert fetched is not None
    assert fetched.diff_id == diff.diff_id


def test_partial_write_does_not_corrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = DiskDiffRepository(root=tmp_path / "ai_sessions")
    diff = _make_diff(diff_id="durable")
    repo.put(diff)
    path = repo._diff_path(diff.flow_id, diff.session_id)
    original_bytes = path.read_bytes()

    mutated = diff.model_copy(update={"rationale": "mutated"})

    import os as os_module

    real_replace = os_module.replace

    def _exploding_replace(*args, **kwargs):  # noqa: ARG001
        raise OSError("disk full")

    monkeypatch.setattr(os_module, "replace", _exploding_replace)
    with pytest.raises(OSError):
        repo.put(mutated)
    monkeypatch.setattr(os_module, "replace", real_replace)

    assert path.read_bytes() == original_bytes
    leftovers = list(path.parent.glob(f"{path.name}.tmp.*"))
    assert leftovers == []


def test_unknown_schema_returns_none_with_warn(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    repo = DiskDiffRepository(root=tmp_path / "ai_sessions")
    flow_id = 1
    session_id = "alien"
    target = repo._diff_path(flow_id, session_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(
            {
                "_schema": "ai_diff.v9",
                "diff_id": "alien-diff",
                "session_id": session_id,
                "flow_id": flow_id,
                "additions": [],
                "connections_added": [],
                "deletions": [],
                "connections_removed": [],
            },
        ),
        encoding="utf-8",
    )

    with caplog.at_level(logging.WARNING, logger=diff_store.logger.name):
        fetched = repo.get("alien-diff")
    assert fetched is None
    matches = [r for r in caplog.records if "unknown schema tag" in r.getMessage()]
    assert len(matches) == 1


def test_concurrent_writes_serialize(tmp_path: Path) -> None:
    repo = DiskDiffRepository(root=tmp_path / "ai_sessions")
    base = _make_diff(diff_id="contended")
    repo.put(base)

    diff_a = base.model_copy(update={"rationale": "A"})
    diff_b = base.model_copy(update={"rationale": "B"})

    errors: list[BaseException] = []

    def _writer(payload: GraphDiff) -> None:
        try:
            for _ in range(20):
                repo.put(payload)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=_writer, args=(diff_a,))
    t2 = threading.Thread(target=_writer, args=(diff_b,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert errors == []
    path = repo._diff_path(base.flow_id, base.session_id)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["rationale"] in ("A", "B")


def test_clear_wipes_disk_subtree(tmp_path: Path) -> None:
    repo = DiskDiffRepository(root=tmp_path / "ai_sessions")
    repo.put(_make_diff())
    assert repo._root.exists()
    repo.clear()
    assert not repo._root.exists()


def test_schema_version_constant() -> None:
    assert SCHEMA_VERSION == "ai_diff.v1"
