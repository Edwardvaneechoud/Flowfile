"""Project manifest (project.yaml), folder layout, and generated .gitignore."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from flowfile_core.project.normalize import write_yaml

PROJECT_FORMAT = 1
MANIFEST_NAME = "project.yaml"

# Single source of truth for the project's file layout. Each root manifest maps its filename
# stem to the friendly label used in the restore-change summary; the managed-path set, the
# projection mirror dirs, and git's staged-path filter are all derived from these.
ROOT_MANIFESTS: dict[str, str] = {
    "secrets": "secrets",
    "namespaces": "namespaces",
    "tables": "catalog tables",
    "models": "models",
    "kernels": "kernels",
    "visualizations": "visualizations",
    "dashboards": "dashboards",
}
# Dirs whose .yaml files mirror DB rows one-for-one (pruned to the written set on full projection).
MIRRORED_DIRS = ("flows", "connections/database", "connections/cloud", "schedules")
# All repo-relative entries git is allowed to stage: the project manifest, .gitignore, every root
# manifest, plus the top-level dirs holding flows/connections/schedules.
MANAGED_PATHS: list[str] = (
    [MANIFEST_NAME, ".gitignore"]
    + [f"{stem}.yaml" for stem in ROOT_MANIFESTS]
    + ["flows", "connections", "schedules"]
)

_GITIGNORE = """\
# Flowfile project — never commit secrets, local state, or materialized data
.env
*.secret
*.db
*.db-journal
*.db-wal
.flowfile/
data/
catalog_tables/
master_key.txt
*.key
*.pem
.secret_key
*.json.enc
"""

# Lines that MUST appear in any project's .gitignore. A hand-crafted
# project opened with a weaker file gets these appended automatically.
_REQUIRED_IGNORE_LINES = {
    ".env",
    "*.secret",
    "*.db",
    ".flowfile/",
    "data/",
    "catalog_tables/",
    "master_key.txt",
    "*.key",
    "*.pem",
    ".secret_key",
    "*.json.enc",
}


@dataclass
class ProjectManifest:
    name: str
    project_id: str
    created_with_version: str
    project_format: int = PROJECT_FORMAT
    # All-or-nothing toggle: when False, catalog tables (tables.yaml) and global artifacts
    # (models.yaml) are kept out of the project — not projected, committed, imported, or pruned.
    track_data_artifacts: bool = True

    def to_dict(self) -> dict:
        return {
            "project_format": self.project_format,
            "name": self.name,
            "project_id": self.project_id,
            "created_with_version": self.created_with_version,
            "track_data_artifacts": self.track_data_artifacts,
        }


def manifest_path(root: Path) -> Path:
    return root / MANIFEST_NAME


def write_manifest(root: Path, manifest: ProjectManifest) -> None:
    write_yaml(manifest_path(root), manifest.to_dict())


def read_manifest(root: Path) -> ProjectManifest | None:
    p = manifest_path(root)
    if not p.exists():
        return None
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return ProjectManifest(
        name=data.get("name", root.name),
        project_id=data.get("project_id", ""),
        created_with_version=data.get("created_with_version", ""),
        project_format=data.get("project_format", PROJECT_FORMAT),
        track_data_artifacts=data.get("track_data_artifacts", True),
    )


def write_gitignore(root: Path) -> None:
    """Write or harden .gitignore.

    If absent, write the full template. If present (e.g. a hand-crafted project
    opened via ``open_project``), append any required lines that are missing so a
    weak .gitignore can never commit key files or secret-bearing patterns."""
    gi = root / ".gitignore"
    if not gi.exists():
        gi.write_text(_GITIGNORE, encoding="utf-8")
        return
    existing_text = gi.read_text(encoding="utf-8")
    existing_lines = {ln.strip() for ln in existing_text.splitlines()}
    missing = [ln for ln in sorted(_REQUIRED_IGNORE_LINES) if ln not in existing_lines]
    if missing:
        addition = "\n# Added by Flowfile — required secret/data guards\n" + "\n".join(missing) + "\n"
        gi.write_text(existing_text.rstrip("\n") + "\n" + addition, encoding="utf-8")


def flows_dir(root: Path) -> Path:
    return root / "flows"


def connections_dir(root: Path, kind: str) -> Path:
    return root / "connections" / kind


def schedules_dir(root: Path) -> Path:
    return root / "schedules"


def secrets_manifest_path(root: Path) -> Path:
    return root / "secrets.yaml"


def namespaces_manifest_path(root: Path) -> Path:
    return root / "namespaces.yaml"


def tables_manifest_path(root: Path) -> Path:
    return root / "tables.yaml"


def models_manifest_path(root: Path) -> Path:
    return root / "models.yaml"


def kernels_manifest_path(root: Path) -> Path:
    return root / "kernels.yaml"


def visualizations_manifest_path(root: Path) -> Path:
    return root / "visualizations.yaml"


def dashboards_manifest_path(root: Path) -> Path:
    return root / "dashboards.yaml"
