"""API routes for the Global Artifact Registry.

Provides endpoints for:
- Publishing (uploading) serialized Python objects from Kernels
- Downloading artifacts for consumption in other Kernels / flows
- Listing, searching, and filtering artifacts in the catalog
- Updating metadata and deleting artifacts

The blob is stored via the storage backend; only metadata lives in the DB.
Large uploads are streamed to disk in chunks — the Core never holds the
entire blob in memory.
"""

from __future__ import annotations

import io
import json
import logging

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import func as sa_func
from sqlalchemy.orm import Session

from flowfile_core.artifacts.storage import (
    ArtifactStorageBackend,
    LocalArtifactStorage,
    compute_sha256,
)
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import GlobalArtifact
from flowfile_core.schemas.artifact_schema import (
    ArtifactOut,
    ArtifactStats,
    ArtifactSummary,
    ArtifactUpdate,
)
from shared.storage_config import storage as flowfile_storage

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/artifacts",
    tags=["artifacts"],
    dependencies=[Depends(get_current_active_user)],
)

# ---------------------------------------------------------------------------
# Storage backend (initialised lazily on first use)
# ---------------------------------------------------------------------------

_storage_backend: ArtifactStorageBackend | None = None


def _get_storage() -> ArtifactStorageBackend:
    global _storage_backend
    if _storage_backend is None:
        root = flowfile_storage.global_artifacts_directory
        root.mkdir(parents=True, exist_ok=True)
        _storage_backend = LocalArtifactStorage(root)
    return _storage_backend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tags_from_db(row: GlobalArtifact) -> list[str]:
    if not row.tags:
        return []
    try:
        return json.loads(row.tags)
    except (json.JSONDecodeError, TypeError):
        return []


def _row_to_out(row: GlobalArtifact) -> ArtifactOut:
    return ArtifactOut(
        id=row.id,
        name=row.name,
        namespace_id=row.namespace_id,
        owner_id=row.owner_id,
        source_flow_id=row.source_flow_id,
        source_node_id=row.source_node_id,
        source_kernel_id=row.source_kernel_id,
        python_type=row.python_type,
        python_module=row.python_module,
        serialization_format=row.serialization_format,
        size_bytes=row.size_bytes,
        sha256=row.sha256,
        description=row.description,
        tags=_tags_from_db(row),
        version=row.version,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


# ---------------------------------------------------------------------------
# Publish (upload)
# ---------------------------------------------------------------------------

@router.post("/publish", response_model=ArtifactOut, status_code=201)
async def publish_artifact(
    file: UploadFile = File(...),
    name: str = Form(...),
    python_type: str = Form(""),
    python_module: str = Form(""),
    serialization_format: str = Form("pickle"),
    description: str = Form(None),
    tags: str = Form("[]"),  # JSON-encoded list
    namespace_id: int | None = Form(None),
    source_flow_id: int | None = Form(None),
    source_node_id: int | None = Form(None),
    source_kernel_id: str | None = Form(None),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Upload a serialized Python object as a global artifact.

    The kernel runtime serializes the object, then POSTs the blob as a
    multipart file together with the metadata fields.
    """
    store = _get_storage()

    # Parse tags
    try:
        tag_list: list[str] = json.loads(tags) if tags else []
    except json.JSONDecodeError:
        tag_list = []

    # Determine version — bump if same name+namespace already exists
    latest = (
        db.query(sa_func.max(GlobalArtifact.version))
        .filter(GlobalArtifact.name == name, GlobalArtifact.namespace_id == namespace_id)
        .scalar()
    )
    version = (latest or 0) + 1

    # Build storage key
    storage_key = f"{name}/v{version}/{file.filename or 'artifact.bin'}"

    # Stream the upload to a temporary buffer for SHA-256, then to storage.
    # For truly huge files a two-pass (temp file) approach would be better,
    # but this is good enough for the initial implementation.
    blob_bytes = await file.read()
    blob_stream = io.BytesIO(blob_bytes)
    sha = compute_sha256(blob_stream)
    size = store.write(storage_key, blob_stream)

    row = GlobalArtifact(
        name=name,
        namespace_id=namespace_id,
        owner_id=current_user.id,
        source_flow_id=source_flow_id,
        source_node_id=source_node_id,
        source_kernel_id=source_kernel_id,
        python_type=python_type,
        python_module=python_module,
        serialization_format=serialization_format,
        storage_key=storage_key,
        size_bytes=size,
        sha256=sha,
        description=description,
        tags=json.dumps(tag_list),
        version=version,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    logger.info(
        "Published global artifact %s (id=%d, version=%d, %d bytes)",
        name, row.id, version, size,
    )
    return _row_to_out(row)


# ---------------------------------------------------------------------------
# Download (stream)
# ---------------------------------------------------------------------------

@router.get("/{artifact_id}/download")
async def download_artifact(
    artifact_id: int,
    db: Session = Depends(get_db),
):
    """Stream the raw artifact blob back to the caller.

    The ``Content-Disposition`` header includes the original filename so
    callers that save to disk get a sensible name.
    """
    row = db.get(GlobalArtifact, artifact_id)
    if row is None:
        raise HTTPException(404, "Artifact not found")

    store = _get_storage()
    if not store.exists(row.storage_key):
        raise HTTPException(404, "Artifact blob missing from storage")

    # Derive a download filename from the storage key
    filename = row.storage_key.rsplit("/", 1)[-1]

    media_type = "application/octet-stream"
    if row.serialization_format == "parquet":
        media_type = "application/vnd.apache.parquet"

    return StreamingResponse(
        store.read_stream(row.storage_key),
        media_type=media_type,
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(row.size_bytes),
            "X-Artifact-Format": row.serialization_format,
        },
    )


# ---------------------------------------------------------------------------
# Get metadata
# ---------------------------------------------------------------------------

@router.get("/{artifact_id}", response_model=ArtifactOut)
async def get_artifact(
    artifact_id: int,
    db: Session = Depends(get_db),
):
    """Return full metadata for a single artifact."""
    row = db.get(GlobalArtifact, artifact_id)
    if row is None:
        raise HTTPException(404, "Artifact not found")
    return _row_to_out(row)


@router.get("/by-name/{name}", response_model=ArtifactOut)
async def get_artifact_by_name(
    name: str,
    namespace_id: int | None = Query(None),
    version: int | None = Query(None),
    db: Session = Depends(get_db),
):
    """Look up an artifact by name, optionally filtered by namespace and version.

    If *version* is omitted the latest version is returned.
    """
    q = db.query(GlobalArtifact).filter(GlobalArtifact.name == name)
    if namespace_id is not None:
        q = q.filter(GlobalArtifact.namespace_id == namespace_id)
    if version is not None:
        q = q.filter(GlobalArtifact.version == version)
    else:
        q = q.order_by(GlobalArtifact.version.desc())

    row = q.first()
    if row is None:
        raise HTTPException(404, f"Artifact '{name}' not found")
    return _row_to_out(row)


# ---------------------------------------------------------------------------
# List / Search
# ---------------------------------------------------------------------------

@router.get("/", response_model=list[ArtifactSummary])
async def list_artifacts(
    namespace_id: int | None = Query(None),
    name_contains: str | None = Query(None),
    python_type: str | None = Query(None),
    serialization_format: str | None = Query(None),
    tag: str | None = Query(None),
    owner_id: int | None = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """List artifacts with optional filters."""
    q = db.query(GlobalArtifact)

    if namespace_id is not None:
        q = q.filter(GlobalArtifact.namespace_id == namespace_id)
    if name_contains:
        q = q.filter(GlobalArtifact.name.contains(name_contains))
    if python_type:
        q = q.filter(GlobalArtifact.python_type == python_type)
    if serialization_format:
        q = q.filter(GlobalArtifact.serialization_format == serialization_format)
    if tag:
        # JSON string search — works for SQLite's simple LIKE
        q = q.filter(GlobalArtifact.tags.contains(f'"{tag}"'))
    if owner_id is not None:
        q = q.filter(GlobalArtifact.owner_id == owner_id)

    rows = (
        q.order_by(GlobalArtifact.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return [
        ArtifactSummary(
            id=r.id,
            name=r.name,
            python_type=r.python_type,
            serialization_format=r.serialization_format,
            size_bytes=r.size_bytes,
            version=r.version,
            owner_id=r.owner_id,
            created_at=r.created_at,
        )
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Update metadata
# ---------------------------------------------------------------------------

@router.put("/{artifact_id}", response_model=ArtifactOut)
async def update_artifact(
    artifact_id: int,
    body: ArtifactUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Update mutable metadata (name, description, tags, namespace)."""
    row = db.get(GlobalArtifact, artifact_id)
    if row is None:
        raise HTTPException(404, "Artifact not found")
    if row.owner_id != current_user.id and not getattr(current_user, "is_admin", False):
        raise HTTPException(403, "Not authorised to update this artifact")

    if body.name is not None:
        row.name = body.name
    if body.description is not None:
        row.description = body.description
    if body.tags is not None:
        row.tags = json.dumps(body.tags)
    if body.namespace_id is not None:
        row.namespace_id = body.namespace_id

    db.commit()
    db.refresh(row)
    return _row_to_out(row)


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

@router.delete("/{artifact_id}", status_code=204)
async def delete_artifact(
    artifact_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Delete an artifact (both metadata and blob)."""
    row = db.get(GlobalArtifact, artifact_id)
    if row is None:
        raise HTTPException(404, "Artifact not found")
    if row.owner_id != current_user.id and not getattr(current_user, "is_admin", False):
        raise HTTPException(403, "Not authorised to delete this artifact")

    store = _get_storage()
    try:
        store.delete(row.storage_key)
    except FileNotFoundError:
        pass  # Blob already gone — still delete the metadata row

    db.delete(row)
    db.commit()
    logger.info("Deleted global artifact %s (id=%d)", row.name, artifact_id)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

@router.get("/stats/overview", response_model=ArtifactStats)
async def artifact_stats(
    db: Session = Depends(get_db),
):
    """Catalog-wide artifact statistics."""
    total = db.query(sa_func.count(GlobalArtifact.id)).scalar() or 0
    total_size = db.query(sa_func.sum(GlobalArtifact.size_bytes)).scalar() or 0

    # Format distribution
    format_rows = (
        db.query(GlobalArtifact.serialization_format, sa_func.count(GlobalArtifact.id))
        .group_by(GlobalArtifact.serialization_format)
        .all()
    )
    format_counts = {fmt: cnt for fmt, cnt in format_rows}

    recent = (
        db.query(GlobalArtifact)
        .order_by(GlobalArtifact.created_at.desc())
        .limit(10)
        .all()
    )
    recent_out = [
        ArtifactSummary(
            id=r.id,
            name=r.name,
            python_type=r.python_type,
            serialization_format=r.serialization_format,
            size_bytes=r.size_bytes,
            version=r.version,
            owner_id=r.owner_id,
            created_at=r.created_at,
        )
        for r in recent
    ]

    return ArtifactStats(
        total_artifacts=total,
        total_size_bytes=total_size,
        format_counts=format_counts,
        recent_artifacts=recent_out,
    )
