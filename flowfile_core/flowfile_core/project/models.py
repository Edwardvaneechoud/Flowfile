"""Lightweight value objects shared across the project package."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ActiveProject:
    id: int
    name: str
    root: Path
    owner_id: int
    track_data_artifacts: bool = True


class NoActiveProjectError(RuntimeError):
    """Raised by read/write actions when the caller has no active project (router → 404)."""


@dataclass
class KeptResources:
    """The resources still present in the project files after an import — everything else of the
    owner's is pruned. A ``None`` set means the files say nothing about that category (the project
    does not track it, or the imported tree has no file of that kind: no root manifest, or no file
    in its folder), so it is left alone entirely (never pruned). An empty set still prunes the whole
    category."""

    flow_uuids: set[str] | None
    db_connections: set[str] | None
    cloud_connections: set[str] | None
    namespace_ids: set[int] | None
    table_ids: set[int] | None
    artifact_ids: set[int] | None
    kernel_ids: set[str] | None
    viz_uuids: set[str] | None
    dashboard_uuids: set[str] | None
    notebook_uuids: set[str] | None


@dataclass
class SetupResult:
    imported_flows: int = 0
    imported_connections: int = 0
    imported_schedules: int = 0
    placeholder_secrets: list[str] = field(default_factory=list)
    prune_errors: list[str] = field(default_factory=list)
    recovery_sha: str | None = None

    def to_dict(self) -> dict:
        return {
            "imported": {
                "flows": self.imported_flows,
                "connections": self.imported_connections,
                "schedules": self.imported_schedules,
            },
            "placeholder_secrets": sorted(set(self.placeholder_secrets)),
            "prune_errors": list(self.prune_errors),
            "recovery_sha": self.recovery_sha,
        }
