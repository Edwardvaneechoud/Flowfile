"""save_version self-heals a project folder that has no git repo.

A project created while the git binary was unavailable (docker image without git) gets a manifest +
DB row but no .git, so versioning silently no-ops. Once git is available, the next save_version must
initialize the repo and produce a commit rather than staying a no-op.
"""

import shutil

import pytest

from flowfile_core.project import git_ops, project_sync

OWNER = 1


def test_save_version_heals_repo_missing_git(tmp_path):
    if not git_ops.git_available():
        pytest.skip("git binary not available in this environment")

    project_sync.close_project(OWNER)
    root = tmp_path / "project"
    try:
        project_sync.init_project(str(root), "Heal Test", OWNER)
        # Simulate a project created while git was unavailable: drop the repo entirely.
        shutil.rmtree(root / ".git")
        assert not git_ops.is_repo(root)
        assert git_ops.log(root) == []

        sha = project_sync.save_version(OWNER, "first save")

        assert sha is not None
        assert git_ops.is_repo(root)
        assert len(git_ops.log(root)) >= 1
    finally:
        project_sync.close_project(OWNER)
