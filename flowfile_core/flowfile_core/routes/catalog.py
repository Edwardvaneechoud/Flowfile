"""API routes for the Flow Catalog system.

Provides endpoints for:
- Namespace management (Unity Catalog-style catalog / schema hierarchy)
- Flow registration (persistent flow metadata)
- Run history with versioned snapshots
- Favorites and follows

This module is a thin HTTP adapter: it delegates all business logic to
``CatalogService`` and translates domain exceptions into HTTP responses.
"""

import json
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from shared.storage_config import storage
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.catalog import (
    CatalogService,
    FavoriteNotFoundError,
    FlowHasArtifactsError,
    FlowNotFoundError,
    FollowNotFoundError,
    NamespaceExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    NestingLimitError,
    NoSnapshotError,
    RunNotFoundError,
    SQLAlchemyCatalogRepository,
)
from flowfile_core.database.connection import get_db
from flowfile_core.schemas.catalog_schema import (
    CatalogStats,
    FavoriteOut,
    FlowRegistrationCreate,
    FlowRegistrationOut,
    FlowRegistrationUpdate,
    FlowRunDetail,
    FlowRunOut,
    FollowOut,
    NamespaceCreate,
    NamespaceOut,
    NamespaceTree,
    NamespaceUpdate,
)

router = APIRouter(
    prefix="/catalog",
    tags=["catalog"],
    dependencies=[Depends(get_current_active_user)],
)


# ---------------------------------------------------------------------------
# Dependency injection
# ---------------------------------------------------------------------------


def get_catalog_service(db: Session = Depends(get_db)) -> CatalogService:
    """FastAPI dependency that provides a configured ``CatalogService``."""
    repo = SQLAlchemyCatalogRepository(db)
    return CatalogService(repo)


# ---------------------------------------------------------------------------
# Namespace CRUD
# ---------------------------------------------------------------------------


@router.get("/namespaces", response_model=list[NamespaceOut])
def list_namespaces(
    parent_id: int | None = None,
    service: CatalogService = Depends(get_catalog_service),
):
    """List namespaces, optionally filtered by parent."""
    return service.list_namespaces(parent_id)


@router.post("/namespaces", response_model=NamespaceOut, status_code=201)
def create_namespace(
    body: NamespaceCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Create a catalog (level 0) or schema (level 1) namespace."""
    try:
        return service.create_namespace(
            name=body.name,
            owner_id=current_user.id,
            parent_id=body.parent_id,
            description=body.description,
        )
    except NamespaceNotFoundError:
        raise HTTPException(404, "Parent namespace not found")
    except NamespaceExistsError:
        raise HTTPException(409, "Namespace with this name already exists at this level")
    except NestingLimitError:
        raise HTTPException(422, "Cannot nest deeper than catalog -> schema")


@router.put("/namespaces/{namespace_id}", response_model=NamespaceOut)
def update_namespace(
    namespace_id: int,
    body: NamespaceUpdate,
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.update_namespace(
            namespace_id=namespace_id,
            name=body.name,
            description=body.description,
        )
    except NamespaceNotFoundError:
        raise HTTPException(404, "Namespace not found")


@router.delete("/namespaces/{namespace_id}", status_code=204)
def delete_namespace(
    namespace_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.delete_namespace(namespace_id)
    except NamespaceNotFoundError:
        raise HTTPException(404, "Namespace not found")
    except NamespaceNotEmptyError:
        raise HTTPException(422, "Cannot delete namespace with children or flows")


@router.get("/namespaces/tree", response_model=list[NamespaceTree])
def get_namespace_tree(
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Return the full catalog tree with flows nested under schemas."""
    return service.get_namespace_tree(user_id=current_user.id)


# ---------------------------------------------------------------------------
# Default namespace helper
# ---------------------------------------------------------------------------


@router.get("/default-namespace-id")
def get_default_namespace_id(
    service: CatalogService = Depends(get_catalog_service),
):
    """Return the ID of the default 'user_flows' schema under 'General'."""
    return service.get_default_namespace_id()


# ---------------------------------------------------------------------------
# Flow Registration CRUD
# ---------------------------------------------------------------------------


@router.get("/flows", response_model=list[FlowRegistrationOut])
def list_flows(
    namespace_id: int | None = None,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.list_flows(user_id=current_user.id, namespace_id=namespace_id)


@router.post("/flows", response_model=FlowRegistrationOut, status_code=201)
def register_flow(
    body: FlowRegistrationCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.register_flow(
            name=body.name,
            flow_path=body.flow_path,
            owner_id=current_user.id,
            namespace_id=body.namespace_id,
            description=body.description,
        )
    except NamespaceNotFoundError:
        raise HTTPException(404, "Namespace not found")


@router.get("/flows/{flow_id}", response_model=FlowRegistrationOut)
def get_flow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.get_flow(registration_id=flow_id, user_id=current_user.id)
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found")


@router.put("/flows/{flow_id}", response_model=FlowRegistrationOut)
def update_flow(
    flow_id: int,
    body: FlowRegistrationUpdate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.update_flow(
            registration_id=flow_id,
            requesting_user_id=current_user.id,
            name=body.name,
            description=body.description,
            namespace_id=body.namespace_id,
        )
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found")


@router.delete("/flows/{flow_id}", status_code=204)
def delete_flow(
    flow_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.delete_flow(registration_id=flow_id)
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found")
    except FlowHasArtifactsError as e:
        raise HTTPException(409, str(e))


# ---------------------------------------------------------------------------
# Run History
# ---------------------------------------------------------------------------


@router.get("/runs", response_model=list[FlowRunOut])
def list_runs(
    registration_id: int | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.list_runs(
        registration_id=registration_id, limit=limit, offset=offset
    )


@router.get("/runs/{run_id}", response_model=FlowRunDetail)
def get_run_detail(
    run_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    """Get a single run including the YAML snapshot of the flow version that ran."""
    try:
        return service.get_run_detail(run_id)
    except RunNotFoundError:
        raise HTTPException(404, "Run not found")


# ---------------------------------------------------------------------------
# Open Run Snapshot in Designer
# ---------------------------------------------------------------------------


@router.post("/runs/{run_id}/open")
def open_run_snapshot(
    run_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Write the run's flow snapshot to a temp file and import it into the designer."""
    try:
        snapshot_data = service.get_run_snapshot(run_id)
    except RunNotFoundError:
        raise HTTPException(404, "Run not found")
    except NoSnapshotError:
        raise HTTPException(422, "No flow snapshot available for this run")

    # Determine file extension based on content
    try:
        json.loads(snapshot_data)
        suffix = ".json"
    except (json.JSONDecodeError, TypeError):
        suffix = ".yaml"

    # Write to the flows temp directory (safe location for import)
    temp_dir = storage.temp_directory_for_flows
    temp_dir.mkdir(parents=True, exist_ok=True)
    snapshot_filename = f"run_{run_id}_snapshot{suffix}"
    snapshot_path = temp_dir / snapshot_filename

    snapshot_path.write_text(snapshot_data, encoding="utf-8")

    user_id = current_user.id if current_user else None
    flow_id = flow_file_handler.import_flow(Path(snapshot_path), user_id=user_id)
    return {"flow_id": flow_id}


# ---------------------------------------------------------------------------
# Favorites
# ---------------------------------------------------------------------------


@router.get("/favorites", response_model=list[FlowRegistrationOut])
def list_favorites(
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.list_favorites(user_id=current_user.id)


@router.post("/flows/{flow_id}/favorite", response_model=FavoriteOut, status_code=201)
def add_favorite(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.add_favorite(
            user_id=current_user.id, registration_id=flow_id
        )
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found")


@router.delete("/flows/{flow_id}/favorite", status_code=204)
def remove_favorite(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.remove_favorite(user_id=current_user.id, registration_id=flow_id)
    except FavoriteNotFoundError:
        raise HTTPException(404, "Favorite not found")


# ---------------------------------------------------------------------------
# Follows
# ---------------------------------------------------------------------------


@router.get("/following", response_model=list[FlowRegistrationOut])
def list_following(
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.list_following(user_id=current_user.id)


@router.post("/flows/{flow_id}/follow", response_model=FollowOut, status_code=201)
def add_follow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.add_follow(
            user_id=current_user.id, registration_id=flow_id
        )
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found")


@router.delete("/flows/{flow_id}/follow", status_code=204)
def remove_follow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.remove_follow(user_id=current_user.id, registration_id=flow_id)
    except FollowNotFoundError:
        raise HTTPException(404, "Follow not found")


# ---------------------------------------------------------------------------
# Dashboard / Stats
# ---------------------------------------------------------------------------


@router.get("/stats", response_model=CatalogStats)
def get_catalog_stats(
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.get_catalog_stats(user_id=current_user.id)
