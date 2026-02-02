"""API routes for the Global Artifacts system.

Provides endpoints for:
- Prepare/finalize two-phase upload (Core never handles blob data)
- Artifact lookup by name with optional version
- Listing and filtering artifacts
- Deletion (soft delete in DB, hard delete in storage)
"""

import json

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from flowfile_core.artifacts import get_storage_backend
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import GlobalArtifact
from flowfile_core.schemas.artifact_schema import (
    ArtifactListItem,
    ArtifactOut,
    FinalizeUploadRequest,
    FinalizeUploadResponse,
    PrepareUploadRequest,
    PrepareUploadResponse,
)

router = APIRouter(
    prefix="/artifacts",
    tags=["artifacts"],
    dependencies=[Depends(get_current_active_user)],
)

# File extension mapping for serialization formats
_FORMAT_EXTENSIONS = {
    "parquet": ".parquet",
    "joblib": ".joblib",
    "pickle": ".pkl",
}


@router.post("/prepare-upload", response_model=PrepareUploadResponse)
def prepare_upload(
    body: PrepareUploadRequest,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Step 1 of upload: Create pending artifact and return upload target.

    Kernel will write blob directly to storage, then call /finalize.
    """
    # Determine next version
    latest = (
        db.query(GlobalArtifact)
        .filter_by(name=body.name, namespace_id=body.namespace_id)
        .filter(GlobalArtifact.status.in_(["active", "pending"]))
        .order_by(GlobalArtifact.version.desc())
        .first()
    )
    next_version = (latest.version + 1) if latest else 1

    # Create pending record
    artifact = GlobalArtifact(
        name=body.name,
        namespace_id=body.namespace_id,
        version=next_version,
        status="pending",
        owner_id=current_user.id,
        source_flow_id=body.source_flow_id,
        source_node_id=body.source_node_id,
        source_kernel_id=body.source_kernel_id,
        python_type=body.python_type,
        python_module=body.python_module,
        serialization_format=body.serialization_format,
        description=body.description,
        tags=json.dumps(body.tags),
    )
    db.add(artifact)
    db.commit()
    db.refresh(artifact)

    # Get upload target from storage backend
    ext = _FORMAT_EXTENSIONS.get(body.serialization_format, ".bin")
    filename = f"{body.name}{ext}"
    storage_backend = get_storage_backend()
    target = storage_backend.prepare_upload(artifact.id, filename)

    return PrepareUploadResponse(
        artifact_id=artifact.id,
        version=next_version,
        method=target.method,
        path=target.path,
        storage_key=target.storage_key,
    )


@router.post("/finalize", response_model=FinalizeUploadResponse)
def finalize_upload(
    body: FinalizeUploadRequest,
    db: Session = Depends(get_db),
):
    """Step 2 of upload: Verify blob and activate artifact.

    Called by kernel after writing blob to storage.
    """
    artifact = db.get(GlobalArtifact, body.artifact_id)
    if not artifact:
        raise HTTPException(404, "Artifact not found")
    if artifact.status != "pending":
        raise HTTPException(400, "Artifact not in pending state")

    storage_backend = get_storage_backend()

    try:
        verified_size = storage_backend.finalize_upload(
            body.storage_key,
            body.sha256,
        )
    except FileNotFoundError:
        raise HTTPException(400, "Blob not found in storage")
    except ValueError as e:
        artifact.status = "failed"
        db.commit()
        raise HTTPException(400, str(e))

    # Activate artifact
    artifact.status = "active"
    artifact.storage_key = body.storage_key
    artifact.sha256 = body.sha256
    artifact.size_bytes = verified_size
    db.commit()

    return FinalizeUploadResponse(
        artifact_id=artifact.id,
        version=artifact.version,
    )


@router.get("/by-name/{name}", response_model=ArtifactOut)
def get_artifact_by_name(
    name: str,
    version: int | None = None,
    namespace_id: int | None = None,
    db: Session = Depends(get_db),
):
    """Lookup artifact by name. Returns latest version unless specified.

    Includes download_source for kernel to fetch blob directly.
    """
    query = db.query(GlobalArtifact).filter_by(
        name=name,
        namespace_id=namespace_id,
        status="active",
    )

    if version is not None:
        artifact = query.filter_by(version=version).first()
    else:
        artifact = query.order_by(GlobalArtifact.version.desc()).first()

    if not artifact:
        raise HTTPException(404, f"Artifact '{name}' not found")

    # Add download source
    storage_backend = get_storage_backend()
    download = storage_backend.prepare_download(artifact.storage_key)

    tags = json.loads(artifact.tags) if artifact.tags else []

    return ArtifactOut(
        id=artifact.id,
        name=artifact.name,
        namespace_id=artifact.namespace_id,
        version=artifact.version,
        status=artifact.status,
        owner_id=artifact.owner_id,
        source_flow_id=artifact.source_flow_id,
        source_node_id=artifact.source_node_id,
        source_kernel_id=artifact.source_kernel_id,
        python_type=artifact.python_type,
        python_module=artifact.python_module,
        serialization_format=artifact.serialization_format,
        storage_key=artifact.storage_key,
        size_bytes=artifact.size_bytes,
        sha256=artifact.sha256,
        description=artifact.description,
        tags=tags,
        created_at=artifact.created_at,
        updated_at=artifact.updated_at,
        download_source={"method": download.method, "path": download.path},
    )


@router.get("/", response_model=list[ArtifactListItem])
def list_artifacts(
    namespace_id: int | None = None,
    tags: list[str] | None = Query(default=None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List artifacts with optional filtering."""
    query = db.query(GlobalArtifact).filter_by(status="active")

    if namespace_id is not None:
        query = query.filter_by(namespace_id=namespace_id)

    # Tag filtering (JSON contains)
    if tags:
        for tag in tags:
            query = query.filter(GlobalArtifact.tags.contains(f'"{tag}"'))

    artifacts = (
        query.order_by(
            GlobalArtifact.name,
            GlobalArtifact.version.desc(),
        )
        .offset(offset)
        .limit(limit)
        .all()
    )

    return [
        ArtifactListItem(
            id=a.id,
            name=a.name,
            version=a.version,
            status=a.status,
            python_type=a.python_type,
            serialization_format=a.serialization_format,
            size_bytes=a.size_bytes,
            description=a.description,
            created_at=a.created_at,
            tags=json.loads(a.tags) if a.tags else [],
        )
        for a in artifacts
    ]


@router.delete("/{artifact_id}")
def delete_artifact(
    artifact_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Delete an artifact (soft delete in DB, hard delete blob)."""
    artifact = db.get(GlobalArtifact, artifact_id)
    if not artifact:
        raise HTTPException(404, "Artifact not found")

    # Delete blob from storage
    if artifact.storage_key:
        storage_backend = get_storage_backend()
        storage_backend.delete(artifact.storage_key)

    # Soft delete in DB
    artifact.status = "deleted"
    db.commit()

    return {"status": "deleted", "artifact_id": artifact_id}
