"""Project tree layout: directory names, file naming by stable key, slugging.

All paths are resolved relative to a project root (a directory that maps 1:1 to
a git repo). This module knows *where* each artifact lives; it does not read or
write -- that is the exporters/importers' job.
"""

from __future__ import annotations

import re
from pathlib import Path

# Top-level directories inside a project root.
MANIFEST_FILE = "flowfile.project.yaml"
GITIGNORE_FILE = ".gitignore"
FLOWS_DIR = "flows"
CONNECTIONS_DIR = "connections"
SECRETS_DIR = "secrets"
SCHEDULES_DIR = "schedules"
SYNC_STATE_DIR = ".flowfile"
SYNC_STATE_FILE = "sync-state.json"

SECRETS_MANIFEST_FILE = "secrets.manifest.yaml"

# Connection sub-directories, keyed by the ``kind`` discriminator.
CONNECTION_KINDS = {
    "database_connection": "database",
    "cloud_connection": "cloud",
    "ga_connection": "google_analytics",
    "kafka_connection": "kafka",
}

FLOW_SUFFIX = ".flow.yaml"
LAYOUT_SUFFIX = ".layout.yaml"
SCHEDULES_SUFFIX = ".schedules.yaml"

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    """Stable, filesystem-safe slug for a human name.

    Lowercase, non-alphanumerics collapsed to single underscores, trimmed. Empty
    input yields ``"unnamed"`` so a file is always produced.
    """
    slug = _SLUG_RE.sub("_", (name or "").strip().lower()).strip("_")
    return slug or "unnamed"


class ProjectLayout:
    """Resolves absolute paths within one project root."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    # -- top level ---------------------------------------------------------
    @property
    def manifest_path(self) -> Path:
        return self.root / MANIFEST_FILE

    @property
    def gitignore_path(self) -> Path:
        return self.root / GITIGNORE_FILE

    @property
    def flows_dir(self) -> Path:
        return self.root / FLOWS_DIR

    @property
    def connections_dir(self) -> Path:
        return self.root / CONNECTIONS_DIR

    @property
    def secrets_dir(self) -> Path:
        return self.root / SECRETS_DIR

    @property
    def schedules_dir(self) -> Path:
        return self.root / SCHEDULES_DIR

    @property
    def secrets_manifest_path(self) -> Path:
        return self.secrets_dir / SECRETS_MANIFEST_FILE

    @property
    def sync_state_path(self) -> Path:
        return self.root / SYNC_STATE_DIR / SYNC_STATE_FILE

    # -- per-artifact ------------------------------------------------------
    def flow_path(self, slug: str) -> Path:
        return self.flows_dir / f"{slug}{FLOW_SUFFIX}"

    def flow_layout_path(self, slug: str) -> Path:
        return self.flows_dir / f"{slug}{LAYOUT_SUFFIX}"

    def connection_path(self, kind: str, connection_name: str) -> Path:
        subdir = CONNECTION_KINDS.get(kind, kind)
        return self.connections_dir / subdir / f"{slugify(connection_name)}.yaml"

    def schedules_path(self, flow_slug: str) -> Path:
        return self.schedules_dir / f"{flow_slug}{SCHEDULES_SUFFIX}"

    # -- helpers -----------------------------------------------------------
    def rel(self, path: Path) -> str:
        """Project-relative POSIX path string (the stable artifact key)."""
        return path.relative_to(self.root).as_posix()

    def iter_connection_files(self) -> list[Path]:
        if not self.connections_dir.exists():
            return []
        return sorted(self.connections_dir.glob("*/*.yaml"))

    def iter_flow_files(self) -> list[Path]:
        if not self.flows_dir.exists():
            return []
        return sorted(self.flows_dir.glob(f"*{FLOW_SUFFIX}"))

    def iter_schedule_files(self) -> list[Path]:
        if not self.schedules_dir.exists():
            return []
        return sorted(self.schedules_dir.glob(f"*{SCHEDULES_SUFFIX}"))
