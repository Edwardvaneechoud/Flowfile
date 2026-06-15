"""Embedded git backend for the workspace (Phase 2).

A thin wrapper over the ``git`` CLI — no extra Python dependency, and it degrades
gracefully when the ``git`` binary is absent (history features simply report
unavailable). Git only ever runs on an explicit user action (commit / restore),
never automatically.

The project tree is its own repository: ``is_repo`` checks for ``.git`` directly
under the root so operations can never accidentally target a parent repo the
project happens to live inside.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

# Config applied to app-made commits. A fallback identity means a commit never
# fails on a machine with no global user.name / user.email (CI, fresh
# containers), and gpgsign=false keeps our commits from blocking on (or failing
# under) a user's commit-signing setup — the app commits on the user's behalf.
_COMMIT_IDENTITY = [
    "-c",
    "user.name=Flowfile",
    "-c",
    "user.email=flowfile@local",
    "-c",
    "commit.gpgsign=false",
]


class GitError(RuntimeError):
    """A git command exited non-zero."""


class GitBackend:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    @staticmethod
    def available() -> bool:
        return shutil.which("git") is not None

    def _run(self, *args: str, check: bool = True) -> str:
        proc = subprocess.run(
            ["git", "-C", str(self.root), *args],
            capture_output=True,
            text=True,
        )
        if check and proc.returncode != 0:
            raise GitError(proc.stderr.strip() or f"git {' '.join(args)} failed")
        return proc.stdout

    def is_repo(self) -> bool:
        return (self.root / ".git").is_dir()

    def init(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self._run("init")

    def current_branch(self) -> str | None:
        if not self.is_repo():
            return None
        return self._run("rev-parse", "--abbrev-ref", "HEAD", check=False).strip() or None

    def status(self) -> dict:
        porcelain = self._run("status", "--porcelain", check=False).splitlines()
        uncommitted = sorted(line[3:].strip() for line in porcelain if line.strip())
        return {"branch": self.current_branch(), "dirty": bool(uncommitted), "uncommitted": uncommitted}

    def log(self, limit: int = 50) -> list[dict]:
        if not self.is_repo():
            return []
        # Unit-separator-delimited fields keep parsing robust against any char in
        # the subject/author.
        fmt = "%H%x1f%h%x1f%an%x1f%ae%x1f%aI%x1f%s"
        out = self._run("log", f"--max-count={limit}", f"--pretty=format:{fmt}", check=False)
        commits: list[dict] = []
        for line in out.splitlines():
            parts = line.split("\x1f")
            if len(parts) != 6:
                continue
            sha, short, author, email, date, subject = parts
            commits.append(
                {"sha": sha, "short_sha": short, "author": author, "email": email, "date": date, "subject": subject}
            )
        return commits

    def commit(self, message: str) -> str | None:
        """Stage everything and commit. Returns the new sha, or None if clean."""
        if not self.is_repo():
            self.init()
        self._run("add", "-A")
        if not self._run("status", "--porcelain", check=False).strip():
            return None  # nothing staged → nothing to commit
        self._run(*_COMMIT_IDENTITY, "commit", "-m", message or "Update Flowfile project")
        return self._run("rev-parse", "HEAD").strip()

    def commit_patch(self, sha: str) -> str:
        """The unified diff introduced by a single commit."""
        if not self.is_repo():
            return ""
        return self._run("show", sha, "--patch", "--format=", check=False).strip("\n")

    def worktree_diff(self) -> str:
        """Tracked, uncommitted changes vs HEAD."""
        if not self.is_repo():
            return ""
        return self._run("diff", "HEAD", check=False)

    def restore(self, sha: str) -> None:
        """Make the working tree match ``sha`` (without moving the branch).

        Restores tracked files to the commit's content and deletes files that
        were added after it, so the tree matches the snapshot. The caller commits
        the result, so a restore is itself an undoable step forward in history.
        """
        if not self.is_repo():
            raise GitError("Not a git repository")
        self._run("checkout", sha, "--", ".")
        added = self._run("diff", "--name-only", "--diff-filter=A", sha, "HEAD", check=False).splitlines()
        for rel in added:
            rel = rel.strip()
            if rel:
                (self.root / rel).unlink(missing_ok=True)
