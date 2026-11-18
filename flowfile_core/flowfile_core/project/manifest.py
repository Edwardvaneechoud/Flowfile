"""Project manifest (project.yaml), folder layout, and generated .gitignore."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from flowfile_core.project.normalize import write_yaml

PROJECT_FORMAT = 2
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
    """``project.yaml``. ``kinds`` lists the kinds the folder describes: an import applies and prunes only those."""

    name: str
    project_id: str
    created_with_version: str
    project_format: int = PROJECT_FORMAT
    kinds: list[str] = field(default_factory=lambda: _registered_names(track_data_artifacts=True))

    @property
    def track_data_artifacts(self) -> bool:
        """Whether the data kinds (catalog tables, models) are listed; setting it adds or removes them."""
        return all(name in self.kinds for name in _data_kind_names())

    @track_data_artifacts.setter
    def track_data_artifacts(self, value: bool) -> None:
        data = _data_kind_names()
        self.kinds = [name for name in self.kinds if name not in data]
        if value:
            self.kinds += data

    def to_dict(self) -> dict:
        return {
            "project_format": self.project_format,
            "name": self.name,
            "project_id": self.project_id,
            "created_with_version": self.created_with_version,
            "kinds": list(self.kinds),
        }


def _data_kind_names() -> list[str]:
    from flowfile_core.project import kinds

    return [k.name for k in kinds.ordered() if k.tracked_by_data_flag]


def _registered_names(*, track_data_artifacts: bool) -> list[str]:
    from flowfile_core.project import kinds

    return [k.name for k in kinds.ordered() if track_data_artifacts or not k.tracked_by_data_flag]


def manifest_path(root: Path) -> Path:
    return root / MANIFEST_NAME


def write_manifest(root: Path, manifest: ProjectManifest) -> None:
    write_yaml(manifest_path(root), manifest.to_dict())


def parse_manifest(data: dict, default_name: str) -> ProjectManifest:
    """``project.yaml``'s mapping as a manifest. A newer ``project_format`` or a ``kinds`` that is not a list of
    names is a ``ValueError``; a file without ``kinds`` (format 1) lists every registered kind, less the data kinds
    when its ``track_data_artifacts`` is false."""
    found = data.get("project_format", 1)
    if isinstance(found, int) and found > PROJECT_FORMAT:
        raise ValueError(
            f"{MANIFEST_NAME} was written by a newer Flowfile (project format {found}; this one reads up to "
            f"{PROJECT_FORMAT}). Nothing was changed."
        )
    listed = data.get("kinds")
    if listed is None:
        listed = _registered_names(track_data_artifacts=data.get("track_data_artifacts", True) is not False)
    elif not (isinstance(listed, list) and all(isinstance(n, str) and n for n in listed)):
        raise ValueError(f"{MANIFEST_NAME} must list its 'kinds' as names. Nothing was changed.")
    return ProjectManifest(
        name=data.get("name", default_name),
        project_id=data.get("project_id", ""),
        created_with_version=data.get("created_with_version", ""),
        project_format=found,
        kinds=list(listed),
    )


def read_manifest(root: Path) -> ProjectManifest | None:
    """The folder's manifest, ``None`` without one; a file that cannot be read is a ``ValueError``."""
    p = manifest_path(root)
    if not p.exists():
        return None
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ValueError(f"{MANIFEST_NAME} is not valid YAML. Nothing was changed.") from e
    return parse_manifest(data or {}, root.name)


def kinds_to_save(m: ProjectManifest) -> list[str]:
    """The kinds a save lists: the registered ones in order (the data kinds only when tracked), then the listed
    names no kind is registered for, in their listed order, so their files stay described."""
    from flowfile_core.project import kinds

    return _registered_names(track_data_artifacts=m.track_data_artifacts) + kinds.unknown(m.kinds)


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


def notebooks_dir(root: Path) -> Path:
    return root / "notebooks"


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
