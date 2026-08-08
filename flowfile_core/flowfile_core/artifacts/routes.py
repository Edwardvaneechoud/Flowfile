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

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from sqlalchemy.orm import Session

from flowfile_core.artifacts.exceptions import (
    AmbiguousArtifactError,
    AmbiguousNamespaceError,
    ArtifactError,
    ArtifactNotActiveError,
    ArtifactNotFoundError,
    ArtifactUploadError,
    CannotDeleteLastVersionError,
    CannotDeleteLatestVersionError,
    InvalidArtifactNameError,
    NamespaceNotFoundError,
)
from flowfile_core.artifacts.service import ArtifactService
from flowfile_core.auth.jwt import get_user_or_internal_service, verify_internal_token
from flowfile_core.catalog.access import AccessResolver
from flowfile_core.catalog.exceptions import FlowNotFoundError, NotAuthorizedError
from flowfile_core.database.connection import get_db
from flowfile_core.schemas.artifact_schema import (
    ArtifactDeleteResponse,
    ArtifactListItem,
    ArtifactOut,
    ArtifactPruneResponse,
    ArtifactWithVersions,
    FinalizeUploadRequest,
    FinalizeUploadResponse,
    PrepareUploadRequest,
    PrepareUploadResponse,
)

router = APIRouter(
    prefix="/artifacts",
    tags=["artifacts"],
    # Every artifact endpoint authenticates; handlers that need the principal
    # itself (owner id, internal-service detection) declare it explicitly.
    dependencies=[Depends(get_user_or_internal_service)],
)


# Dependency injection


def get_artifact_service(
    db: Session = Depends(get_db),
    current_user=Depends(get_user_or_internal_service),
) -> ArtifactService:
    """FastAPI dependency that provides a configured ``ArtifactService``.

    Every artifact endpoint authenticates (``download_source`` hands out a real
    path / presigned URL). The ``AccessResolver`` additionally makes reads
    private-by-default in multi-user mode; it is a no-op for admins, the kernel
    internal-service principal, and electron.
    """
    from flowfile_core.artifacts import get_storage_backend

    storage = get_storage_backend()
    return ArtifactService(db, storage, access=AccessResolver(db, current_user))


def is_internal_service_call(
    x_internal_token: str | None = Header(None, alias="X-Internal-Token"),
) -> bool:
    """True for kernel calls, which bypass the version-safety delete guards.

    Released kernel images call ``delete_global_artifact(name, version=latest)``
    and must keep working unchanged.
    """
    if not x_internal_token:
        return False
    try:
        return verify_internal_token(x_internal_token)
    except ValueError:
        return False


def _conflict(exc: ArtifactError) -> HTTPException:
    """409 carrying a machine-readable error code alongside the message."""
    return HTTPException(409, {"error_code": getattr(exc, "code", "CONFLICT"), "message": str(exc)})


# Upload workflow


@router.post(
    "/prepare-upload",
    response_model=PrepareUploadResponse,
    status_code=201,
    summary="Prepare artifact upload",
    description=(
        "Step 1 of upload: Create pending artifact record and return upload target. "
        "Kernel writes blob directly to storage, then calls /finalize. "
        "Accepts either JWT auth or X-Internal-Token header for kernel calls."
    ),
)
async def prepare_upload(
    body: PrepareUploadRequest,
    current_user=Depends(get_user_or_internal_service),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Initiate an artifact upload."""
    try:
        result = service.prepare_upload(body, owner_id=current_user.id)
        return result
    except FlowNotFoundError:
        raise HTTPException(404, "Source registration not found") from None
    except NamespaceNotFoundError:
        raise HTTPException(404, "Namespace not found") from None
    except InvalidArtifactNameError as e:
        raise HTTPException(422, str(e)) from e


@router.post(
    "/finalize",
    response_model=FinalizeUploadResponse,
    summary="Finalize artifact upload",
    description=("Step 2 of upload: Verify blob exists and SHA-256 matches, " "then activate the artifact."),
)
def finalize_upload(
    body: FinalizeUploadRequest,
    service: ArtifactService = Depends(get_artifact_service),
):
    """Finalize an artifact upload after blob is written."""
    try:
        result = service.finalize_upload(
            artifact_id=body.artifact_id,
            storage_key=body.storage_key,
            sha256=body.sha256,
            size_bytes=body.size_bytes,
        )
        return result
    except ArtifactNotFoundError:
        raise HTTPException(404, "Artifact not found") from None
    except ArtifactNotActiveError as e:
        raise HTTPException(400, f"Artifact not in pending state: {e.status}") from e
    except ArtifactUploadError as e:
        raise HTTPException(400, str(e)) from e


# Listing (placed before parameterized routes to avoid conflicts)


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
    python_type_contains: str | None = Query(None, description="Filter by Python type substring"),
    limit: int = Query(100, ge=1, le=500, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    latest_only: bool = Query(False, description="One row per (name, namespace): the newest active version"),
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
        latest_only=latest_only,
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


# Retrieval


@router.get(
    "/by-name/{name}",
    response_model=ArtifactOut,
    summary="Get artifact by name",
    description=(
        "Lookup artifact by name or qualified 'catalog.schema::name' reference. "
        "Returns latest version unless specified. "
        "Includes download_source for kernel to fetch blob directly."
    ),
)
def get_artifact_by_name(
    name: str,
    version: int | None = Query(None, description="Specific version to retrieve"),
    namespace_id: int | None = Query(None, description="Namespace filter"),
    namespace: str | None = Query(None, description="Namespace name (resolved to id; alternative to namespace_id)"),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Get artifact by name with optional version."""
    try:
        if namespace_id is None and namespace is not None:
            namespace_id = service.resolve_namespace_id(namespace)
        return service.get_artifact_by_name(
            name=name,
            namespace_id=namespace_id,
            version=version,
        )
    except (AmbiguousArtifactError, AmbiguousNamespaceError) as e:
        raise _conflict(e) from e
    except (ArtifactNotFoundError, NamespaceNotFoundError) as e:
        raise HTTPException(404, str(e)) from e


@router.get(
    "/by-name/{name}/versions",
    response_model=ArtifactWithVersions,
    summary="Get artifact with all versions",
    description=(
        "Get artifact metadata and its versions (newest first). Accepts a qualified "
        "'catalog.schema::name' reference. Without a limit every version is returned."
    ),
)
def get_artifact_versions(
    name: str,
    namespace_id: int | None = Query(None, description="Namespace filter"),
    namespace: str | None = Query(None, description="Namespace name (resolved to id; alternative to namespace_id)"),
    limit: int | None = Query(None, ge=1, le=500, description="Maximum versions to return"),
    offset: int = Query(0, ge=0, description="Offset into the version list"),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Get artifact with its available versions."""
    try:
        if namespace_id is None and namespace is not None:
            namespace_id = service.resolve_namespace_id(namespace)
        return service.get_artifact_with_versions(
            name=name,
            namespace_id=namespace_id,
            limit=limit,
            offset=offset,
        )
    except (AmbiguousArtifactError, AmbiguousNamespaceError) as e:
        raise _conflict(e) from e
    except (ArtifactNotFoundError, NamespaceNotFoundError) as e:
        raise HTTPException(404, str(e)) from e


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
        raise HTTPException(404, "Artifact not found") from None


# Version management


@router.post(
    "/{artifact_id}/promote",
    response_model=ArtifactOut,
    summary="Promote an artifact version",
    description=(
        "Copy an older version forward as a new highest version, so it becomes what "
        "'latest' resolves to. History stays append-only; the blob is copied by the "
        "storage backend."
    ),
)
def promote_artifact_version(
    artifact_id: int,
    service: ArtifactService = Depends(get_artifact_service),
):
    """Promote a version to the new latest."""
    try:
        return service.promote_version(artifact_id)
    except NotAuthorizedError as e:
        raise HTTPException(403, str(e)) from e
    except ArtifactNotFoundError:
        raise HTTPException(404, "Artifact version not found or its data is missing") from None
    except ArtifactUploadError as e:
        raise _conflict(e) from e


@router.post(
    "/by-name/{name}/prune",
    response_model=ArtifactPruneResponse,
    summary="Prune old artifact versions",
    description=(
        "Delete all but the newest `keep` active versions. The current latest is always "
        "kept. With dry_run=true nothing is deleted."
    ),
)
def prune_artifact_versions(
    name: str,
    keep: int = Query(5, ge=1, description="How many newest versions to keep"),
    dry_run: bool = Query(True, description="Report what would be deleted without deleting"),
    namespace_id: int | None = Query(None, description="Namespace filter"),
    namespace: str | None = Query(None, description="Namespace name (resolved to id; alternative to namespace_id)"),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Prune older versions of an artifact."""
    try:
        if namespace_id is None and namespace is not None:
            namespace_id = service.resolve_namespace_id(namespace)
        return service.prune_versions(
            name=name,
            namespace_id=namespace_id,
            keep=keep,
            dry_run=dry_run,
        )
    except NotAuthorizedError as e:
        raise HTTPException(403, str(e)) from e
    except (AmbiguousArtifactError, AmbiguousNamespaceError) as e:
        raise _conflict(e) from e
    except (ArtifactNotFoundError, NamespaceNotFoundError) as e:
        raise HTTPException(404, str(e)) from e


# Deletion


@router.delete(
    "/{artifact_id}",
    response_model=ArtifactDeleteResponse,
    summary="Delete artifact",
    description=(
        "Delete a specific artifact version (soft delete in DB, hard delete blob). "
        "Refuses the current latest version (409 CANNOT_DELETE_LATEST) unless force=true, "
        "and the last remaining version (delete the whole artifact by name instead). "
        "Accepts either JWT auth or X-Internal-Token header for kernel calls."
    ),
)
async def delete_artifact(
    artifact_id: int,
    force: bool = Query(False, description="Allow deleting the current latest version"),
    internal_service: bool = Depends(is_internal_service_call),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Delete a specific artifact version."""
    try:
        service.delete_artifact(artifact_id, force=force, bypass_guards=internal_service)
        return ArtifactDeleteResponse(
            status="deleted",
            artifact_id=artifact_id,
            versions_deleted=1,
        )
    except NotAuthorizedError as e:
        raise HTTPException(403, str(e)) from e
    except (CannotDeleteLatestVersionError, CannotDeleteLastVersionError) as e:
        raise _conflict(e) from e
    except ArtifactNotFoundError:
        raise HTTPException(404, "Artifact not found") from None


@router.delete(
    "/by-name/{name}",
    response_model=ArtifactDeleteResponse,
    summary="Delete all versions of artifact",
    description=(
        "Delete all versions of an artifact by name. "
        "Accepts either JWT auth or X-Internal-Token header for kernel calls."
    ),
)
async def delete_artifact_by_name(
    name: str,
    namespace_id: int | None = Query(None, description="Namespace filter"),
    namespace: str | None = Query(None, description="Namespace name (resolved to id; alternative to namespace_id)"),
    service: ArtifactService = Depends(get_artifact_service),
):
    """Delete all versions of an artifact."""
    try:
        if namespace_id is None and namespace is not None:
            namespace_id = service.resolve_namespace_id(namespace)
        versions_deleted = service.delete_all_versions(
            name=name,
            namespace_id=namespace_id,
        )
        return ArtifactDeleteResponse(
            status="deleted",
            artifact_id=0,  # Multiple versions deleted
            versions_deleted=versions_deleted,
        )
    except NotAuthorizedError as e:
        raise HTTPException(403, str(e)) from e
    except AmbiguousNamespaceError as e:
        raise _conflict(e) from e
    except (ArtifactNotFoundError, NamespaceNotFoundError) as e:
        raise HTTPException(404, str(e)) from e
