"""Unit tests for the embedded git backend (commit / log / diff / restore)."""

from __future__ import annotations

import pytest

from flowfile_core.workspace.git_backend import GitBackend

pytestmark = pytest.mark.skipif(not GitBackend.available(), reason="git binary not installed")


def test_commit_log_diff_restore(tmp_path):
    root = tmp_path / "proj"
    root.mkdir()
    (root / "a.txt").write_text("v1\n", encoding="utf-8")

    git = GitBackend(root)
    assert not git.is_repo()

    sha1 = git.commit("first snapshot")
    assert sha1 is not None
    assert git.is_repo()
    assert len(git.log()) == 1

    # A second snapshot: modify a file and add a new one.
    (root / "a.txt").write_text("v2\n", encoding="utf-8")
    (root / "b.txt").write_text("new file\n", encoding="utf-8")
    sha2 = git.commit("second snapshot")
    assert sha2 is not None
    assert len(git.log()) == 2

    # Nothing changed -> nothing to commit.
    assert git.commit("noop") is None

    # The second commit's patch reflects the change.
    patch = git.commit_patch(sha2)
    assert "v2" in patch or "b.txt" in patch

    # Restore to the first snapshot: a.txt reverts and the later-added b.txt is removed.
    git.restore(sha1)
    assert (root / "a.txt").read_text(encoding="utf-8") == "v1\n"
    assert not (root / "b.txt").exists()

    # Recording the restore leaves a clean tree.
    git.commit("Restore to first")
    assert git.status()["dirty"] is False


def test_history_empty_before_first_commit(tmp_path):
    git = GitBackend(tmp_path / "fresh")
    assert git.log() == []
    assert git.current_branch() is None
