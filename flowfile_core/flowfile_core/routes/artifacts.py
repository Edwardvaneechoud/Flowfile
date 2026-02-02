"""REST API endpoints for artifact persistence and recovery."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from flowfile_core.artifacts.models import (
    ArtifactInfo,
    CleanupResult,
    PersistenceStats,
    RecoveryMode,
    RecoveryStatus,
)
from flowfile_core.artifacts.store import artifact_manager
from flowfile_core.auth.jwt import get_current_active_user

router = APIRouter(
    prefix="/artifacts",
    tags=["artifacts"],
    dependencies=[Depends(get_current_active_user)],
)


# ------------------------------------------------------------------
# List & inspect
# ------------------------------------------------------------------


@router.get("/{flow_id}", response_model=list[ArtifactInfo])
def list_artifacts(flow_id: int) -> list[ArtifactInfo]:
    """List all artifacts for a flow (persisted and in-memory)."""
    store = artifact_manager.get_store(flow_id)
    return store.list_artifacts()


@router.get("/{flow_id}/persistence", response_model=PersistenceStats)
def get_persistence_info(flow_id: int) -> PersistenceStats:
    """Get aggregate persistence statistics for a flow's artifacts."""
    store = artifact_manager.get_store(flow_id)
    return store.get_persistence_stats()


@router.get("/{flow_id}/{name}", response_model=ArtifactInfo)
def get_artifact_metadata(flow_id: int, name: str) -> ArtifactInfo:
    """Get metadata for a single artifact (does not return the object itself)."""
    store = artifact_manager.get_store(flow_id)
    artifacts = store.list_artifacts()
    for art in artifacts:
        if art.name == name:
            return art
    raise HTTPException(status_code=404, detail=f"Artifact '{name}' not found")


# ------------------------------------------------------------------
# Recovery
# ------------------------------------------------------------------


@router.post("/{flow_id}/recover", response_model=RecoveryStatus)
def trigger_recovery(flow_id: int, mode: RecoveryMode | None = None) -> RecoveryStatus:
    """Trigger artifact recovery for a flow.

    Query parameter ``mode`` overrides the store's default recovery mode.
    Accepted values: ``lazy``, ``eager``, ``none``.
    """
    store = artifact_manager.get_store(flow_id)
    return store.recover(mode)


@router.get("/{flow_id}/recovery-status", response_model=RecoveryStatus)
def get_recovery_status(flow_id: int) -> RecoveryStatus:
    """Check which artifacts are available for recovery (non-destructive scan)."""
    store = artifact_manager.get_store(flow_id)
    return store._scan_recoverable()


# ------------------------------------------------------------------
# Cleanup & deletion
# ------------------------------------------------------------------


@router.post("/{flow_id}/cleanup", response_model=CleanupResult)
def cleanup_artifacts(flow_id: int, max_age_hours: float | None = None) -> CleanupResult:
    """Clean up old artifacts for a flow.

    If ``max_age_hours`` is provided, only artifacts older than that are
    removed.  Otherwise **all** artifacts are deleted.
    """
    store = artifact_manager.get_store(flow_id)
    return store.cleanup(max_age_hours=max_age_hours)


@router.delete("/{flow_id}/{name}")
def delete_artifact(flow_id: int, name: str) -> dict:
    """Delete a single artifact by name."""
    store = artifact_manager.get_store(flow_id)
    deleted = store.delete(name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Artifact '{name}' not found")
    return {"deleted": name}


@router.delete("/{flow_id}", response_model=CleanupResult)
def clear_all_artifacts(flow_id: int) -> CleanupResult:
    """Delete all artifacts for a flow (memory + disk)."""
    store = artifact_manager.get_store(flow_id)
    return store.clear()


# ------------------------------------------------------------------
# Discovery
# ------------------------------------------------------------------


@router.get("/", response_model=list[int])
def list_flow_ids_with_artifacts() -> list[int]:
    """Return flow IDs that have persisted artifacts on disk."""
    return artifact_manager.list_flow_ids()
