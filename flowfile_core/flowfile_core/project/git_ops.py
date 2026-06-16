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
    """Reset the index + working tree to exactly match ``sha`` (including deletions); HEAD unchanged.

    Unlike ``checkout <sha> -- .`` this removes files that were added after ``sha``, so a restore
    truly reverts to that version's content.
    """
    if is_repo(root):
        Repo(str(root)).git.read_tree("-u", "--reset", sha)


def _parse_name_status(out: str) -> list[dict]:
    changes: list[dict] = []
    for line in out.splitlines():
        parts = line.strip().split("\t")
        if len(parts) >= 2 and parts[0]:
            changes.append({"status": parts[0], "path": parts[-1]})
    return changes


def diff_name_status(root: Path, a: str, b: str) -> list[dict]:
    """``git diff --name-status a b`` → ``[{"status", "path"}]``. Empty on any error."""
    if not is_repo(root):
        return []
    try:
        return _parse_name_status(Repo(str(root)).git.diff("--name-status", a, b))
    except Exception:
        return []


def changes_in(root: Path, sha: str) -> list[dict]:
    """What commit ``sha`` changed vs its parent. ``--root`` makes the parentless initial
    commit list all its files (added) instead of showing an empty diff."""
    if not is_repo(root):
        return []
    try:
        return _parse_name_status(Repo(str(root)).git.diff_tree("--no-commit-id", "--name-status", "-r", "--root", sha))
    except Exception:
        return []


def uncommitted_changes(root: Path) -> list[dict]:
    """Working-tree changes vs the last version (incl. untracked new files), as name-status rows."""
    if not is_repo(root):
        return []
    try:
        out = Repo(str(root)).git.status("--porcelain")
    except Exception:
        return []
    changes: list[dict] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        code, path = line[:2], line[3:].strip()
        if "->" in path:  # rename: report the new path
            path = path.split("->")[-1].strip()
        if code == "??" or "A" in code:
            status = "A"
        elif "D" in code:
            status = "D"
        else:
            status = "M"
        changes.append({"status": status, "path": path})
    return changes


def is_dirty(root: Path) -> bool:
    """True when the working tree has changes (incl. untracked) to save as a version."""
    if not is_repo(root):
        return False
    try:
        return Repo(str(root)).is_dirty(untracked_files=True)
    except Exception:
        return False
