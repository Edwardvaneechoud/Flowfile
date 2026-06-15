"""Git-enabled Flowfile project workspace (Phase 1).

This package projects the runtime SQLite catalog into a deterministic,
secret-free directory tree that maps 1:1 to a git repo, and reconstructs the
runtime state from that tree on a fresh clone.

The SQLite DB stays the source of truth for runtime (scheduler/executor/catalog
read it). The project tree is an export/import + history layer:

* ``export`` (DB -> files) writes the deterministic tree.
* ``apply``  (files -> DB) is an explicit, opt-in rebuild (clone bootstrap,
  branch switch, rollback). It is never auto-imported on file change.

MVP scope: flows, connections (with ``${secret:NAME}`` placeholders), the
secret manifest (names only -- never values) and schedules. Catalog namespaces,
table-schema definitions, visualizations and embedded git live in later phases.
"""

from flowfile_core.workspace.models import (
    DriftReport,
    ProjectManifest,
    SecretRequirement,
    WorkspaceApplyResult,
    WorkspaceExportResult,
    WorkspaceStatus,
)
from flowfile_core.workspace.sync import WorkspaceSync

__all__ = [
    "DriftReport",
    "ProjectManifest",
    "SecretRequirement",
    "WorkspaceApplyResult",
    "WorkspaceExportResult",
    "WorkspaceStatus",
    "WorkspaceSync",
]
