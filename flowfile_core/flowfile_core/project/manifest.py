"""Project manifest (project.yaml), folder layout, and generated .gitignore."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from flowfile_core.project.normalize import write_yaml

PROJECT_FORMAT = 1
MANIFEST_NAME = "project.yaml"

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
"""


@dataclass
class ProjectManifest:
    name: str
    project_id: str
    created_with_version: str
    project_format: int = PROJECT_FORMAT

    def to_dict(self) -> dict:
        return {
            "project_format": self.project_format,
            "name": self.name,
            "project_id": self.project_id,
            "created_with_version": self.created_with_version,
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
    )


def write_gitignore(root: Path) -> None:
    gi = root / ".gitignore"
    if not gi.exists():
        gi.write_text(_GITIGNORE, encoding="utf-8")


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
