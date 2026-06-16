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


@dataclass
class SetupResult:
    imported_flows: int = 0
    imported_connections: int = 0
    imported_schedules: int = 0
    placeholder_secrets: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "imported": {
                "flows": self.imported_flows,
                "connections": self.imported_connections,
                "schedules": self.imported_schedules,
            },
            "placeholder_secrets": sorted(set(self.placeholder_secrets)),
        }
