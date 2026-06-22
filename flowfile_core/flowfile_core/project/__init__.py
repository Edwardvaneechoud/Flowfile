"""Git-friendly Flowfile project: a deterministic, secret-free folder that mirrors
this install's flows / connections / schedules for versioning and rebuild.

The DB stays the runtime source of truth; projection (DB→files) is automatic and
invisible, import (files→DB) happens only at explicit boundaries (open / restore).
"""

from flowfile_core.project.service import ProjectSyncService, project_sync

__all__ = ["ProjectSyncService", "project_sync"]
