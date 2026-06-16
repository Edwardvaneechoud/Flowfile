"""Thin git wrapper (GitPython). Degrades to a no-op when git is unavailable;
the deterministic folder layout is still usable with bare git in that case."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    from git import Repo

    _GIT_AVAILABLE = True
except Exception:  # pragma: no cover - import guard
    _GIT_AVAILABLE = False


def git_available() -> bool:
    return _GIT_AVAILABLE


def is_repo(root: Path) -> bool:
    if not _GIT_AVAILABLE:
        return False
    try:
        Repo(str(root))
        return True
    except Exception:
        return False


def init(root: Path) -> None:
    if _GIT_AVAILABLE and not is_repo(root):
        Repo.init(str(root))


def commit_all(root: Path, message: str) -> str | None:
    """Stage everything and commit. Returns the sha, or None if nothing changed."""
    if not is_repo(root):
        return None
    repo = Repo(str(root))
    repo.git.add(A=True)
    has_head = repo.head.is_valid()
    if has_head and not repo.index.diff("HEAD"):
        return None
    return repo.index.commit(message).hexsha


def head_sha(root: Path) -> str | None:
    if not is_repo(root):
        return None
    repo = Repo(str(root))
    try:
        return repo.head.commit.hexsha
    except Exception:
        return None


def log(root: Path, limit: int = 50) -> list[dict]:
    if not is_repo(root):
        return []
    repo = Repo(str(root))
    try:
        return [
            {
                "sha": c.hexsha,
                "message": c.message.strip(),
                "committed_at": c.committed_datetime.isoformat(),
            }
            for c in repo.iter_commits(max_count=limit)
        ]
    except Exception:
        return []


def restore(root: Path, sha: str) -> None:
    """Check the project files at ``sha`` back into the working tree."""
    if is_repo(root):
        Repo(str(root)).git.checkout(sha, "--", ".")
