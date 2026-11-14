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
class SetupResult:
    imported_flows: int = 0
    imported_connections: int = 0
    imported_schedules: int = 0
    by_kind: dict[str, int] = field(default_factory=dict)
    unknown_kinds: list[str] = field(default_factory=list)
    not_applied: list[str] = field(default_factory=list)
    placeholder_secrets: list[str] = field(default_factory=list)
    recovery_sha: str | None = None

    def to_dict(self) -> dict:
        return {
            "imported": {
                "flows": self.imported_flows,
                "connections": self.imported_connections,
                "schedules": self.imported_schedules,
                "by_kind": dict(self.by_kind),
            },
            "unknown_kinds": sorted(self.unknown_kinds),
            "not_applied": sorted(self.not_applied),
            "placeholder_secrets": sorted(set(self.placeholder_secrets)),
            "recovery_sha": self.recovery_sha,
        }
