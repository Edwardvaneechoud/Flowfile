"""Thin git wrapper (GitPython). Degrades to a no-op when git is unavailable;
the deterministic folder layout is still usable with bare git in that case."""

from __future__ import annotations

import functools
import logging
import os
import re
import tempfile
import threading
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

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
    """Git pathspecs for the files Flowfile writes: the project manifest, ``.gitignore``, ``.gitattributes`` and
    every path pattern a project kind owns (``project.kinds``). Anything else in the folder (a README,
    ``.DS_Store``, ``flows/notes.yaml``) is never counted as unsaved and never staged."""
    patterns = kinds.managed_patterns()
    roots = [manifest.MANIFEST_NAME, ".gitignore", manifest.GITATTRIBUTES_NAME] + [p for p in patterns if "*" not in p]
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


def _is_managed(path: str) -> bool:
    own = (manifest.MANIFEST_NAME, ".gitignore", manifest.GITATTRIBUTES_NAME)
    return path in own or kinds.kind_for_path(path) is not None


def _unstage_unmanaged(repo) -> None:
    """Take every staged path outside the managed set back out of the index, so a file someone staged by
    hand is not committed with a version."""
    staged = repo.git.diff("--cached", "--name-only", "--no-renames", "-z").split("\0")
    stray = [f":(literal){p}" for p in staged if p and not _is_managed(p)]
    if not stray:
        return
    if repo.head.is_valid():
        repo.git.reset("-q", "--", *stray)
    else:
        repo.git.rm("--cached", "-q", "--", *stray)


def commit_all(root: Path, message: str) -> str | None:
    """Stage the managed files that changed and commit them, and nothing else.

    Only the managed pathspecs are staged, and any other path staged by hand is unstaged first, so files
    Flowfile did not write never reach a version. Each path changed in the working tree is staged by name,
    since ``git add`` refuses a pathspec that matches nothing (a first commit with no ``flows/``, or a
    deletion a restore's ``read-tree`` already staged)."""
    if not is_repo(root):
        return None
    with repo_lock(root):
        repo = Repo(str(root))
        paths = [path for code, path in _managed_status(repo) if code[1] != " "]
        if paths:
            repo.git.add("-A", "--", *(f":(literal){p}" for p in paths))
        _unstage_unmanaged(repo)
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


@functools.lru_cache(maxsize=8)
def _managed_tree(root: str, hexsha: str) -> dict[str, str]:
    tree = Repo(root).commit(hexsha).tree
    return {
        b.path: b.data_stream.read().decode("utf-8", errors="replace")
        for b in tree.traverse()
        if b.type == "blob" and (b.path == manifest.MANIFEST_NAME or kinds.kind_for_path(b.path) is not None)
    }


def read_tree_at(root: Path, sha: str) -> dict[str, str]:
    """``{path: text}`` of the managed files in a commit's tree (``"HEAD"`` or a hex sha), ``project.yaml``
    included; ``{}`` with no repo or no commit. Cached per resolved commit, whose tree never changes."""
    if sha != "HEAD":
        _require_hex_sha(sha)
    if not is_repo(root):
        return {}
    repo = Repo(str(root))
    if sha == "HEAD" and not repo.head.is_valid():
        return {}
    return _managed_tree(str(Path(root).resolve()), repo.commit(sha).hexsha)


def parent_of(root: Path, sha: str) -> str | None:
    """The first parent of commit ``sha``, ``None`` for the first commit or without a repo."""
    _require_hex_sha(sha)
    if not is_repo(root):
        return None
    parents = Repo(str(root)).commit(sha).parents
    return parents[0].hexsha if parents else None


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


REMOTE_NAME = "origin"
_BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$")


class GitRemoteError(ValueError):
    """A branch, remote, fetch or push git refused, worded for the person; never carries a token."""


def check_branch_name(name: str) -> str:
    """``name`` when git takes it as a branch and it cannot be read as an option or a revision expression."""
    if not _BRANCH_RE.match(name) or ".." in name or "//" in name or name.endswith((".", "/", ".lock")):
        raise GitRemoteError(f"Not a branch name Flowfile accepts: {name!r}")
    return name


def current_branch(root: Path) -> str | None:
    """The checked-out branch, ``None`` on a detached HEAD or without a repo."""
    if not is_repo(root):
        return None
    try:
        return Repo(str(root)).active_branch.name
    except TypeError:
        return None


def branch_sha(root: Path, ref: str) -> str | None:
    """The commit ``ref`` names (a branch, ``refs/remotes/origin/<b>``), ``None`` when it does not exist."""
    if not is_repo(root):
        return None
    try:
        return Repo(str(root)).git.rev_parse("--verify", "--quiet", f"{ref}^{{commit}}").strip() or None
    except Exception:
        return None


def ahead_behind(root: Path, ours: str, theirs: str) -> tuple[int, int]:
    """``(commits on ours not on theirs, commits on theirs not on ours)``."""
    out = Repo(str(root)).git.rev_list("--left-right", "--count", f"{ours}...{theirs}")
    left, right = out.split()
    return int(left), int(right)


def is_ancestor(root: Path, older: str, newer: str) -> bool:
    try:
        Repo(str(root)).git.merge_base("--is-ancestor", older, newer)
        return True
    except Exception:
        return False


def branches(root: Path) -> list[dict]:
    """Each local branch: ``name``, ``sha``, ``current``, and against its copy on the remote ``ahead`` and
    ``behind`` (``None`` when the remote has no such branch, as last fetched)."""
    if not is_repo(root):
        return []
    repo = Repo(str(root))
    current = current_branch(root)
    rows = []
    for head in repo.heads:
        remote_sha = branch_sha(root, f"refs/remotes/{REMOTE_NAME}/{head.name}")
        ahead = behind = None
        if remote_sha is not None:
            ahead, behind = ahead_behind(root, head.commit.hexsha, remote_sha)
        rows.append(
            {
                "name": head.name,
                "sha": head.commit.hexsha,
                "current": head.name == current,
                "ahead": ahead,
                "behind": behind,
            }
        )
    return sorted(rows, key=lambda r: (not r["current"], r["name"]))


def create_branch(root: Path, name: str, start: str = "HEAD") -> str:
    """A new branch ``name`` at ``start``; the checkout is left alone. Answers its sha."""
    check_branch_name(name)
    with repo_lock(root):
        repo = Repo(str(root))
        if branch_sha(root, f"refs/heads/{name}") is not None:
            raise GitRemoteError(f"A branch named {name!r} already exists.")
        start_sha = branch_sha(root, start)
        if start_sha is None:
            raise GitRemoteError("Save a version first: a branch starts from a version.")
        repo.git.branch("--", name, start_sha)
        return start_sha


def checkout(root: Path, name: str) -> None:
    """Check out branch ``name``, discarding the managed files' working-tree changes, which the caller kept."""
    check_branch_name(name)
    with repo_lock(root):
        Repo(str(root)).git.checkout("-f", name, "--")


def move_branch(root: Path, sha: str) -> None:
    """Point the checked-out branch at ``sha`` and reset the index and working tree to it."""
    _require_hex_sha(sha)
    with repo_lock(root):
        Repo(str(root)).git.reset("--hard", sha)


def commit_worktree(root: Path, message: str, ref: str) -> str | None:
    """Commit the managed files as the working tree holds them on top of HEAD under ``ref``, through an index of
    its own, so the project's index, working tree and branch stay as they are. ``None`` when they equal HEAD."""
    repo = Repo(str(root))
    with tempfile.TemporaryDirectory(prefix="flowfile-index-") as tmp:
        env = {"GIT_INDEX_FILE": str(Path(tmp) / "index")}
        repo.git.execute(["git", "read-tree", "HEAD"], env=env)
        repo.git.execute(["git", "add", "-A", "--", *_managed_pathspecs()], env=env)
        tree_sha = repo.git.execute(["git", "write-tree"], env=env).strip()
    if tree_sha == repo.head.commit.tree.hexsha:
        return None
    commit = Commit.create_from_tree(repo, repo.tree(tree_sha), message, parent_commits=[repo.head.commit], head=False)
    repo.git.update_ref(ref, commit.hexsha)
    return commit.hexsha


def merge_base(root: Path, a: str, b: str) -> str | None:
    try:
        return Repo(str(root)).git.merge_base(_require_hex_sha(a), _require_hex_sha(b)).strip() or None
    except Exception:
        return None


def commit_tree_changes(
    root: Path, start: str, changes: dict[str, str | None], message: str, parents: list[str]
) -> str:
    """A commit of ``start``'s tree with ``changes`` made (``None`` removes the file) and ``parents``, built in a
    temporary folder; no branch, index or working tree of the project moves."""
    repo = Repo(str(root))
    with tempfile.TemporaryDirectory(prefix="flowfile-merge-") as tmp:
        folder = Path(tmp) / "tree"
        seed_folder(root, folder, _require_hex_sha(start))
        for rel, text in changes.items():
            if rel.startswith("/") or ".." in Path(rel).parts:
                raise ValueError(f"Not a project path: {rel!r}")
            target = folder / rel
            if text is None:
                target.unlink(missing_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(text, encoding="utf-8")
        _folder_git(repo, folder, "add", "-A", "--", ".")
        tree_sha = _folder_git(repo, folder, "write-tree").strip()
    commit = Commit.create_from_tree(
        repo,
        repo.tree(tree_sha),
        message,
        parent_commits=[repo.commit(_require_hex_sha(p)) for p in parents],
        head=False,
    )
    return commit.hexsha


def get_config(root: Path, key: str) -> str | None:
    out = Repo(str(root)).git.config("--get", key, with_exceptions=False).strip()
    return out or None


def set_config(root: Path, key: str, value: str | None) -> None:
    repo = Repo(str(root))
    if value is None:
        repo.git.config("--unset-all", key, with_exceptions=False)
    else:
        repo.git.config(key, value)


def attach_head(root: Path, branch: str) -> None:
    """Make ``branch`` the checked-out branch without touching the index or working tree (same commit)."""
    check_branch_name(branch)
    Repo(str(root)).git.symbolic_ref("HEAD", f"refs/heads/{branch}")


def _check_remote_url(url: str, local_allowed: bool) -> str:
    """``url`` when it is an ``https``, ``http`` or ``ssh`` address with no password in it, or, where
    ``local_allowed``, a folder on this machine; never a transport that runs a command (``ext::``)."""
    url = url.strip()
    if not url or url.startswith("-") or "::" in url or any(c in url for c in "\r\n\0"):
        raise GitRemoteError("Not a remote address Flowfile accepts.")
    parsed = urlsplit(url)
    if parsed.scheme in ("https", "http", "ssh"):
        if parsed.password is not None:
            raise GitRemoteError("Leave the token out of the address: give it as the remote's token instead.")
        if not parsed.hostname:
            raise GitRemoteError("The remote address names no host.")
        return url
    if parsed.scheme == "file" or (not parsed.scheme and Path(url).is_absolute()):
        if not local_allowed:
            raise GitRemoteError("A folder on the server cannot be a remote here; use an https or ssh address.")
        return url
    if re.match(r"^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+:[^:]", url):
        return url
    raise GitRemoteError("Use an https or ssh address for the remote.")


def remote_url(root: Path) -> str | None:
    if not is_repo(root):
        return None
    try:
        return Repo(str(root)).remote(REMOTE_NAME).url
    except ValueError:
        return None


def set_remote(root: Path, url: str, *, local_allowed: bool) -> str:
    """Point ``origin`` at ``url`` (added when missing). The address holds no secret; the token goes per call."""
    url = _check_remote_url(url, local_allowed)
    with repo_lock(root):
        repo = Repo(str(root))
        if remote_url(root) is None:
            repo.git.remote("add", REMOTE_NAME, url)
        else:
            repo.git.remote("set-url", REMOTE_NAME, url)
    return url


_ASKPASS = (
    "#!/bin/sh\n"
    'case "$1" in\n'
    "  Username*) printf '%s\\n' \"$FLOWFILE_GIT_USERNAME\" ;;\n"
    "  *) printf '%s\\n' \"$FLOWFILE_GIT_TOKEN\" ;;\n"
    "esac\n"
)


@contextmanager
def _remote_env(token: str | None, username: str | None):
    """The environment of one fetch or push: git asks ``GIT_ASKPASS``, a script outside the project that prints
    the token from this process's environment, so the token is in no file, address or argument, and with no
    credential helper and no terminal prompt nothing stores it or waits for a person."""
    env = {"GIT_TERMINAL_PROMPT": "0", "GIT_ASKPASS": "", "SSH_ASKPASS": ""}
    if not token:
        yield env
        return
    with tempfile.TemporaryDirectory(prefix="flowfile-askpass-") as tmp:
        script = Path(tmp) / "askpass.sh"
        script.write_text(_ASKPASS, encoding="utf-8")
        script.chmod(0o700)
        env.update(
            GIT_ASKPASS=str(script),
            FLOWFILE_GIT_TOKEN=token,
            FLOWFILE_GIT_USERNAME=username or "x-access-token",
        )
        yield env


_SAFE_CONFIG = ("-c", "credential.helper=", "-c", "protocol.ext.allow=never", "-c", "core.askPass=")


def _remote_git(root: Path, token: str | None, username: str | None, *args: str) -> None:
    """Run ``git <args>`` against the remote, its failure as a ``GitRemoteError`` without git's own text, which
    can repeat the address it reached."""
    repo = Repo(str(root))
    with _remote_env(token, username) as env:
        try:
            repo.git.execute(["git", *_SAFE_CONFIG, *args], env={**os.environ, **env})
        except Exception as exc:
            stderr = getattr(exc, "stderr", "") or ""
            raise GitRemoteError(_remote_failure(stderr)) from None


def _remote_failure(stderr: str) -> str:
    text = stderr.lower()
    if "authentication failed" in text or "could not read username" in text or "403" in text or "401" in text:
        return "The remote refused the sign-in: check the remote's token."
    if "non-fast-forward" in text or "fetch first" in text or "rejected" in text:
        return "The remote has versions this branch does not: pull first."
    if "could not resolve host" in text or "connection refused" in text or "timed out" in text:
        return "The remote could not be reached."
    if "does not appear to be a git repository" in text or "not found" in text:
        return "No repository at the remote's address."
    return "The remote refused the request."


def fetch(root: Path, token: str | None = None, username: str | None = None) -> None:
    if remote_url(root) is None:
        raise GitRemoteError("Set a remote first.")
    _remote_git(root, token, username, "fetch", "--prune", REMOTE_NAME)


def push(root: Path, branch: str, token: str | None = None, username: str | None = None) -> None:
    """Push ``branch`` to the same name on the remote and track it; never forced."""
    check_branch_name(branch)
    if remote_url(root) is None:
        raise GitRemoteError("Set a remote first.")
    _remote_git(
        root, token, username, "push", "--set-upstream", REMOTE_NAME, f"refs/heads/{branch}:refs/heads/{branch}"
    )
