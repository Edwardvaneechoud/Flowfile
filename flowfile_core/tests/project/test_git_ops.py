"""git_ops defense + degradation tests: sha validation, external-change detection,
and graceful no-op when GitPython is unavailable."""

import pytest

from flowfile_core.project import git_ops


@pytest.mark.parametrize(
    "bad",
    ["-x", "--output=/tmp/x", "", "  ", "xyz123z", "g" * 7, "0" * 6, "0" * 41, "deadbeef ", "-0000000"],
)
def test_require_hex_sha_rejects_non_hex_and_flags(bad):
    with pytest.raises(ValueError):
        git_ops._require_hex_sha(bad)


@pytest.mark.parametrize("good", ["0" * 7, "deadbeef", "a" * 40, "ABCdef0", "1234567890abcdef"])
def test_require_hex_sha_accepts_valid_hex(good):
    assert git_ops._require_hex_sha(good) == good


@pytest.mark.skipif(not git_ops.git_available(), reason="git not available")
def test_has_external_changes_detects_out_of_band_commit(tmp_path):
    from git import Repo

    git_ops.init(tmp_path)
    (tmp_path / "a.txt").write_text("one", encoding="utf-8")
    first = git_ops.commit_all(tmp_path, "first") or git_ops.head_sha(tmp_path)
    assert first

    repo = Repo(str(tmp_path))
    (tmp_path / "a.txt").write_text("two", encoding="utf-8")
    repo.git.add(A=True)
    repo.index.commit("out-of-band")

    new_head = git_ops.head_sha(tmp_path)
    assert new_head and new_head != first


@pytest.mark.skipif(not git_ops.git_available(), reason="git not available")
def test_service_has_external_changes_true_after_out_of_band_commit(tmp_path):
    from git import Repo

    from flowfile_core.project import project_sync

    owner = 1
    project_sync.close_project(owner)
    root = tmp_path / "proj"
    try:
        project_sync.init_project(str(root), "Ext Test", owner)
        assert project_sync.has_external_changes(owner) is False

        repo = Repo(str(root))
        (root / "README.md").write_text("out of band", encoding="utf-8")
        repo.git.add(A=True)
        repo.index.commit("out-of-band")

        assert project_sync.has_external_changes(owner) is True
    finally:
        project_sync.close_project(owner)


def test_git_unavailable_degrades_to_noop(tmp_path, monkeypatch):
    monkeypatch.setattr(git_ops, "_GIT_AVAILABLE", False)
    assert git_ops.git_available() is False
    assert git_ops.is_repo(tmp_path) is False
    git_ops.init(tmp_path)  # no-op, must not raise
    assert git_ops.head_sha(tmp_path) is None
    assert git_ops.commit_all(tmp_path, "msg") is None
    assert git_ops.log(tmp_path) == []
    assert git_ops.diff_name_status(tmp_path, "HEAD", "deadbeef") == []
    assert git_ops.uncommitted_changes(tmp_path) == []
    assert git_ops.is_dirty(tmp_path) is False
    git_ops.restore(tmp_path, "deadbeef")  # no-op, must not raise
