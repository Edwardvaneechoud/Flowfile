"""Thin git wrapper (GitPython). Degrades to a no-op when git is unavailable;
the deterministic folder layout is still usable with bare git in that case."""

from __future__ import annotations

import logging
import re
import threading
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    from git import Repo

    _GIT_AVAILABLE = True
except Exception:  # pragma: no cover - import guard
    _GIT_AVAILABLE = False

# Per-repo lock serializing git mutations on the same repo.

_repo_locks: dict[str, threading.RLock] = {}
_repo_locks_guard = threading.Lock()  # guards the dict itself, NOT held during git ops


def _lock_for(root: Path) -> threading.RLock:
    key = str(Path(root).resolve())
    with _repo_locks_guard:
        lock = _repo_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            _repo_locks[key] = lock
        return lock


@contextmanager
def repo_lock(root: Path):
    """Serialize all git/working-tree ops on one repo root. Re-entrant per thread.
    Hold across projection+commit as a SINGLE critical section, and across read_tree/restore/reload."""
    lock = _lock_for(root)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


# sha validation
_SHA_RE = re.compile(r"^[0-9a-fA-F]{7,40}$")


def _require_hex_sha(sha: str) -> str:
    """Reject any value that is not a 7-40 hex-char commit sha.

    A leading '-' would be parsed by git as an option flag; a non-hex string can
    trigger unexpected git behaviour (e.g. '--output=<path>' file overwrite via
    diff --output).  Returns the validated sha unchanged.
    """
    if not _SHA_RE.match(sha):
        raise ValueError(f"Invalid commit sha: {sha!r}")
    return sha


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


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


_MANAGED_PATHS = [
    "project.yaml",
    ".gitignore",
    "secrets.yaml",
    "namespaces.yaml",
    "tables.yaml",
    "models.yaml",
    "kernels.yaml",
    "visualizations.yaml",
    "dashboards.yaml",
    "flows",
    "connections",
    "schedules",
]


def _is_managed(path: str) -> bool:
    """True when ``path`` (repo-relative, forward-slash) belongs to the managed set.

    Root-level entries match exactly; directory entries match the prefix so that
    any file under ``flows/`` is considered managed."""
    for mp in _MANAGED_PATHS:
        if path == mp or path.startswith(mp + "/") or path.startswith(mp + "\\"):
            return True
    return False


def commit_all(root: Path, message: str) -> str | None:
    """Stage only the known managed manifest paths and commit.

    Avoids ``git add -A``: staging everything under the root would sweep
    unrelated tenant data and secret-bearing files into history.

    Strategy: stage ALL changes (so deletions of tracked files are captured), then
    unstage any file that falls outside the managed set before committing."""
    if not is_repo(root):
        return None
    with repo_lock(root):
        repo = Repo(str(root))
        repo.git.add(A=True)
        # Unstage anything that is not in the managed set.
        if repo.head.is_valid():
            staged = repo.index.diff("HEAD")
            to_reset: list[str] = [
                diff.b_path or diff.a_path
                for diff in staged
                if not _is_managed(diff.b_path or diff.a_path or "")
            ]
            if to_reset:
                repo.index.reset(paths=to_reset)
        else:
            # No HEAD yet (first/init commit): diff(None) is index-vs-worktree which is empty
            # immediately after add -A.  Enumerate staged blobs directly and remove unmanaged ones.
            # index.entries keys are (path, stage_number) tuples; path is repo-relative with '/' separators.
            # index.reset() requires a HEAD ref so use index.remove() with working_tree=False instead.
            to_remove = [path for path, _stage in repo.index.entries.keys() if not _is_managed(path)]
            if to_remove:
                repo.index.remove(to_remove, working_tree=False)
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


def tree_blob_sizes(root: Path, sha: str) -> dict[str, int]:
    """``{relative path: size in bytes}`` for every blob in ``sha``'s tree, without touching the
    working tree — so a restore target can be cap-checked before its files are written to disk."""
    _require_hex_sha(sha)
    if not is_repo(root):
        return {}
    tree = Repo(str(root)).commit(sha).tree
    return {b.path: b.size for b in tree.traverse() if b.type == "blob"}


def read_blob(root: Path, sha: str, rel_path: str) -> str | None:
    """The UTF-8 text of one blob in ``sha``'s tree, or ``None`` if it is absent."""
    _require_hex_sha(sha)
    if not is_repo(root):
        return None
    try:
        blob = Repo(str(root)).commit(sha).tree / rel_path
    except KeyError:
        return None
    return blob.data_stream.read().decode("utf-8")


def restore(root: Path, sha: str) -> None:
    """Reset the index + working tree to exactly match ``sha`` (including deletions); HEAD unchanged.

    Unlike ``checkout <sha> -- .`` this removes files that were added after ``sha``, so a restore
    truly reverts to that version's content.
    """
    _require_hex_sha(sha)
    if is_repo(root):
        with repo_lock(root):
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
    _require_hex_sha(b)
    if not is_repo(root):
        return []
    try:
        return _parse_name_status(Repo(str(root)).git.diff("--name-status", a, b))
    except Exception as exc:
        logger.warning("diff_name_status failed (a=%r b=%r): %s", a, b, exc)
        return []


def changes_in(root: Path, sha: str) -> list[dict]:
    """What commit ``sha`` changed vs its parent. ``--root`` makes the parentless initial
    commit list all its files (added) instead of showing an empty diff."""
    _require_hex_sha(sha)
    if not is_repo(root):
        return []
    try:
        return _parse_name_status(Repo(str(root)).git.diff_tree("--no-commit-id", "--name-status", "-r", "--root", sha))
    except Exception as exc:
        logger.warning("changes_in failed (sha=%r): %s", sha, exc)
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
