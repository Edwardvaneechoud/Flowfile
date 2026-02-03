"""API routes for the Global Artifacts system.

Provides endpoints for:
- Uploading artifacts (prepare + finalize two-step workflow)
- Retrieving artifacts by name or ID
- Listing and searching artifacts
- Deleting artifacts

This module is a thin HTTP adapter: it delegates all business logic to
``ArtifactService`` and translates domain exceptions into HTTP responses.

IMPORTANT: Core API never handles blob data. All binary data flows directly
between kernel and storage backend. Core only manages metadata.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from flowfile_core.artifacts.exceptions import (
    ArtifactNotActiveError,
    ArtifactNotFoundError,
    ArtifactUploadError,
    NamespaceNotFoundError,
)
from flowfile_core.artifacts.service import ArtifactService
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db
from flowfile_core.schemas.artifact_schema import (
    ArtifactDeleteResponse,
    ArtifactListItem,
    ArtifactOut,
    ArtifactWithVersions,
    FinalizeUploadRequest,
    FinalizeUploadResponse,
    PrepareUploadRequest,
    PrepareUploadResponse,
)

router = APIRouter(
    prefix="/artifacts",
    tags=["artifacts"],
)


# ---------------------------------------------------------------------------
# Dependency injection
# ---------------------------------------------------------------------------


def get_artifact_service(db: Session = Depends(get_db)) -> ArtifactService:
    """FastAPI dependency that provides a configured ``ArtifactService``."""
    from flowfile_core.artifacts import get_storage_backend

    storage = get_storage_backend()
    return ArtifactService(db, storage)


# ---------------------------------------------------------------------------
# Upload workflow
# ---------------------------------------------------------------------------


@router.post(
    "/prepare-upload",
    response_model=PrepareUploadResponse,
    status_code=201,
    summary="Prepare artifact upload",
    description=(
        "Step 1 of upload: Create pending artifact record and return upload target. "
        "Kernel writes blob directly to storage, then calls /finalize."
    ),
)
def prepare_upload(
    body: PrepareUploadRequest,
    current_user=Depends(get_current_active_user),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Initiate an artifact upload."""
    try:
        return service.prepare_upload(body, owner_id=current_user.id)
    except NamespaceNotFoundError:
        raise HTTPException(404, "Namespace not found")


@router.post(
    "/finalize",
    response_model=FinalizeUploadResponse,
    summary="Finalize artifact upload",
    description=(
        "Step 2 of upload: Verify blob exists and SHA-256 matches, "
        "then activate the artifact."
    ),
)
def finalize_upload(
    body: FinalizeUploadRequest,
    service: ArtifactService = Depends(get_artifact_service),
):
    """Finalize an artifact upload after blob is written."""
    try:
        return service.finalize_upload(
            artifact_id=body.artifact_id,
            storage_key=body.storage_key,
            sha256=body.sha256,
            size_bytes=body.size_bytes,
        )
    except ArtifactNotFoundError:
        raise HTTPException(404, "Artifact not found")
    except ArtifactNotActiveError as e:
        raise HTTPException(400, f"Artifact not in pending state: {e.status}")
    except ArtifactUploadError as e:
        raise HTTPException(400, str(e))


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------


@router.get(
    "/by-name/{name}",
    response_model=ArtifactOut,
    summary="Get artifact by name",
    description=(
        "Lookup artifact by name. Returns latest version unless specified. "
        "Includes download_source for kernel to fetch blob directly."
    ),
)
def get_artifact_by_name(
    name: str,
    version: int | None = Query(None, description="Specific version to retrieve"),
    namespace_id: int | None = Query(None, description="Namespace filter"),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Get artifact by name with optional version."""
    try:
        return service.get_artifact_by_name(
            name=name,
            namespace_id=namespace_id,
            version=version,
        )
    except ArtifactNotFoundError as e:
        raise HTTPException(404, str(e))


@router.get(
    "/by-name/{name}/versions",
    response_model=ArtifactWithVersions,
    summary="Get artifact with all versions",
    description="Get artifact metadata and list of all available versions.",
)
def get_artifact_versions(
    name: str,
    namespace_id: int | None = Query(None, description="Namespace filter"),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Get artifact with all available versions."""
    try:
        return service.get_artifact_with_versions(
            name=name,
            namespace_id=namespace_id,
        )
    except ArtifactNotFoundError as e:
        raise HTTPException(404, str(e))


@router.get(
    "/{artifact_id}",
    response_model=ArtifactOut,
    summary="Get artifact by ID",
    description="Lookup artifact by database ID.",
)
def get_artifact_by_id(
    artifact_id: int,
    service: ArtifactService = Depends(get_artifact_service),
):
    """Get artifact by ID."""
    try:
        return service.get_artifact_by_id(artifact_id)
    except ArtifactNotFoundError:
        raise HTTPException(404, "Artifact not found")


# ---------------------------------------------------------------------------
# Listing
# ---------------------------------------------------------------------------


@router.get(
    "/",
    response_model=list[ArtifactListItem],
    summary="List artifacts",
    description="List artifacts with optional filtering by namespace, tags, name, or type.",
)
def list_artifacts(
    namespace_id: int | None = Query(None, description="Filter by namespace"),
    tags: list[str] | None = Query(None, description="Filter by tags (AND logic)"),
    name_contains: str | None = Query(None, description="Filter by name substring"),
    python_type_contains: str | None = Query(
        None, description="Filter by Python type substring"
    ),
    limit: int = Query(100, ge=1, le=500, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    service: ArtifactService = Depends(get_artifact_service),
):
    """List artifacts with optional filtering."""
    return service.list_artifacts(
        namespace_id=namespace_id,
        tags=tags,
        name_contains=name_contains,
        python_type_contains=python_type_contains,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/names",
    response_model=list[str],
    summary="List artifact names",
    description="List unique artifact names in a namespace.",
)
def list_artifact_names(
    namespace_id: int | None = Query(None, description="Filter by namespace"),
    service: ArtifactService = Depends(get_artifact_service),
):
    """List unique artifact names."""
    return service.list_artifact_names(namespace_id=namespace_id)


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------


@router.delete(
    "/{artifact_id}",
    response_model=ArtifactDeleteResponse,
    summary="Delete artifact",
    description="Delete a specific artifact version (soft delete in DB, hard delete blob).",
)
def delete_artifact(
    artifact_id: int,
    current_user=Depends(get_current_active_user),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Delete a specific artifact version."""
    try:
        service.delete_artifact(artifact_id)
        return ArtifactDeleteResponse(
            status="deleted",
            artifact_id=artifact_id,
            versions_deleted=1,
        )
    except ArtifactNotFoundError:
        raise HTTPException(404, "Artifact not found")


@router.delete(
    "/by-name/{name}",
    response_model=ArtifactDeleteResponse,
    summary="Delete all versions of artifact",
    description="Delete all versions of an artifact by name.",
)
def delete_artifact_by_name(
    name: str,
    namespace_id: int | None = Query(None, description="Namespace filter"),
    current_user=Depends(get_current_active_user),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Delete all versions of an artifact."""
    try:
        versions_deleted = service.delete_all_versions(
            name=name,
            namespace_id=namespace_id,
        )
        return ArtifactDeleteResponse(
            status="deleted",
            artifact_id=0,  # Multiple versions deleted
            versions_deleted=versions_deleted,
        )
    except ArtifactNotFoundError as e:
        raise HTTPException(404, str(e))
