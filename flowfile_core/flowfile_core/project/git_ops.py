"""Thin git wrapper (GitPython). Degrades to a no-op when git is unavailable;
the deterministic folder layout is still usable with bare git in that case."""

from __future__ import annotations

import logging
import re
import threading
from contextlib import contextmanager
from pathlib import Path

from filelock import FileLock

from flowfile_core.project import kinds, manifest

logger = logging.getLogger(__name__)

try:
    from git import Commit, Repo

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


_file_locks: dict[str, FileLock] = {}


def _file_lock_for(root: Path) -> FileLock | None:
    git_dir = Path(root).resolve() / ".git"
    if not git_dir.is_dir():
        return None
    key = str(git_dir)
    with _repo_locks_guard:
        lock = _file_locks.get(key)
        if lock is None:
            lock = FileLock(str(git_dir / "flowfile.lock"))
            _file_locks[key] = lock
        return lock


@contextmanager
def project_lock(root: Path):
    """``repo_lock`` plus a file lock at ``.git/flowfile.lock``, so a save, open, restore or reload in
    another process waits for this one instead of losing a race on ``index.lock`` or a file.

    The file lock is one object per root and counts re-entries, so nesting in one thread is safe. A
    folder with no ``.git`` (git unavailable) gets the thread lock only."""
    with repo_lock(root):
        file_lock = _file_lock_for(root)
        if file_lock is None:
            yield
            return
        with file_lock:
            yield


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


def _managed_pathspecs() -> list[str]:
    """Git pathspecs for the files Flowfile writes: the project manifest, ``.gitignore`` and every path
    pattern a project kind owns (``project.kinds``). Anything else in the folder (a README,
    ``.DS_Store``, ``flows/notes.yaml``) is never counted as unsaved and never staged."""
    patterns = kinds.managed_patterns()
    roots = [manifest.MANIFEST_NAME, ".gitignore"] + [p for p in patterns if "*" not in p]
    return [f":(literal){p}" for p in roots] + [f":(glob){p}" for p in patterns if "*" in p]


def _managed_status(repo) -> list[tuple[str, str]]:
    """``(XY code, path)`` for each managed file that differs from HEAD, untracked ones included.

    ``git status`` accepts pathspecs that match nothing, unlike ``git add``."""
    out = repo.git.status("--porcelain", "-z", "--untracked-files=all", "--", *_managed_pathspecs())
    entries = out.split("\0")
    rows: list[tuple[str, str]] = []
    i = 0
    while i < len(entries):
        entry = entries[i]
        i += 1
        if len(entry) < 4:
            continue
        code, path = entry[:2], entry[3:]
        if code[0] in "RC":
            i += 1  # -z puts a rename's source path in the next field
        rows.append((code, path))
    return rows


def commit_all(root: Path, message: str) -> str | None:
    """Stage the managed files that changed and commit them.

    Only the managed pathspecs are staged, so files Flowfile did not write never reach the index
    or the object store. Each path changed in the working tree is staged by name, since ``git add``
    refuses a pathspec that matches nothing (a first commit with no ``flows/``, or a deletion a
    restore's ``read-tree`` already staged)."""
    if not is_repo(root):
        return None
    with repo_lock(root):
        repo = Repo(str(root))
        paths = [path for code, path in _managed_status(repo) if code[1] != " "]
        if paths:
            repo.git.add("-A", "--", *(f":(literal){p}" for p in paths))
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


def _folder_git(repo, folder: Path, *args: str) -> str:
    """Run git against ``folder`` as the work tree with an index of its own beside it
    (``.<name>.index``), so the project's real index and working tree are never touched."""
    folder = Path(folder)
    index = folder.parent / f".{folder.name}.index"
    return repo.git.execute(
        ["git", f"--work-tree={folder}", *args],
        env={"GIT_INDEX_FILE": str(index)},
        strip_newline_in_stdout=False,
    )


def seed_folder(root: Path, folder: Path, sha: str = "HEAD") -> bool:
    """Write ``sha``'s files into ``folder`` (created if missing) through its own index.

    Returns ``False`` when there is no repo or no such commit, leaving ``folder`` empty."""
    Path(folder).mkdir(parents=True, exist_ok=True)
    if not is_repo(root):
        return False
    repo = Repo(str(root))
    if sha == "HEAD":
        if not repo.head.is_valid():
            return False
    else:
        _require_hex_sha(sha)
    _folder_git(repo, folder, "read-tree", "-u", "--reset", sha)
    return True


def diff_against(root: Path, folder: Path, sha: str = "HEAD") -> list[dict]:
    """Name-status rows of the managed files in a seeded ``folder`` against ``sha``'s tree.

    Every file in the folder is staged into its own index first, so new files count as added and
    removed ones as deleted; only the managed pathspecs are reported."""
    if sha != "HEAD":
        _require_hex_sha(sha)
    repo = Repo(str(root))
    _folder_git(repo, folder, "add", "-A", "--", ".")
    out = _folder_git(
        repo, folder, "diff", "--cached", "--no-renames", "--name-status", "-z", sha, "--", *_managed_pathspecs()
    )
    fields = [f for f in out.split("\0") if f]
    return [{"status": fields[i], "path": fields[i + 1]} for i in range(0, len(fields) - 1, 2)]


def commit_folder(root: Path, folder: Path, message: str, ref: str) -> str | None:
    """Commit a seeded ``folder``'s files on top of HEAD and point ``ref`` at the commit.

    ``ref`` is ``HEAD`` (the branch moves, so the commit is part of the history) or a ref of its own
    such as ``refs/flowfile/recovery/<stamp>`` (the branch stays where it is). The project's index and
    working tree are left as they were. ``None`` when the folder holds exactly HEAD's tree."""
    repo = Repo(str(root))
    _folder_git(repo, folder, "add", "-A", "--", ".")
    tree_sha = _folder_git(repo, folder, "write-tree").strip()
    if tree_sha == repo.head.commit.tree.hexsha:
        return None
    commit = Commit.create_from_tree(repo, repo.tree(tree_sha), message, parent_commits=[repo.head.commit], head=False)
    repo.git.update_ref(ref, commit.hexsha)
    return commit.hexsha


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
    """Managed files changed since the last version (incl. untracked new ones), as name-status rows."""
    if not is_repo(root):
        return []
    try:
        rows = _managed_status(Repo(str(root)))
    except Exception:
        return []
    changes: list[dict] = []
    for code, path in rows:
        if code == "??" or "A" in code:
            status = "A"
        elif "D" in code:
            status = "D"
        else:
            status = "M"
        changes.append({"status": status, "path": path})
    return changes


def is_dirty(root: Path) -> bool:
    """True when a managed file has changes (incl. untracked) to save as a version."""
    if not is_repo(root):
        return False
    try:
        return bool(_managed_status(Repo(str(root))))
    except Exception:
        return False
