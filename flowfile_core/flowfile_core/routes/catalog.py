"""API routes for the Catalog system.

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
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.catalog import (
    CatalogService,
    FavoriteNotFoundError,
    FlowAlreadyRunningError,
    FlowHasArtifactsError,
    FlowNotFoundError,
    FollowNotFoundError,
    NamespaceExistsError,
    NamespaceNotEmptyError,
    NamespaceNotFoundError,
    NestingLimitError,
    NoSnapshotError,
    RunNotFoundError,
    ScheduleNotFoundError,
    SQLAlchemyCatalogRepository,
    TableExistsError,
    TableFavoriteNotFoundError,
    TableNotFoundError,
)
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import SchedulerLock
from flowfile_core.fileExplorer import validate_path_under_cwd
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.scheduler import FlowScheduler, get_scheduler, set_scheduler
from flowfile_core.schemas.catalog_schema import (
    ActiveFlowRun,
    CatalogStats,
    CatalogTableCreate,
    CatalogTableOut,
    CatalogTablePreview,
    CatalogTableUpdate,
    FavoriteOut,
    FlowRegistrationCreate,
    FlowRegistrationOut,
    FlowRegistrationUpdate,
    FlowRunDetail,
    FlowRunOut,
    FlowScheduleCreate,
    FlowScheduleOut,
    FlowScheduleUpdate,
    FollowOut,
    GlobalArtifactOut,
    NamespaceCreate,
    NamespaceOut,
    NamespaceTree,
    NamespaceUpdate,
    PaginatedFlowRuns,
    SchedulerStatusOut,
    TableFavoriteOut,
)
from shared.storage_config import storage

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
        raise HTTPException(404, "Parent namespace not found") from None
    except NamespaceExistsError:
        raise HTTPException(409, "Namespace with this name already exists at this level") from None
    except NestingLimitError:
        raise HTTPException(422, "Cannot nest deeper than catalog -> schema") from None


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
        raise HTTPException(404, "Namespace not found") from None


@router.delete("/namespaces/{namespace_id}", status_code=204)
def delete_namespace(
    namespace_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.delete_namespace(namespace_id)
    except NamespaceNotFoundError:
        raise HTTPException(404, "Namespace not found") from None
    except NamespaceNotEmptyError:
        raise HTTPException(422, "Cannot delete namespace with children or flows") from None


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
    """Return the ID of the default 'default' schema under 'General'."""
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
        raise HTTPException(404, "Namespace not found") from None


@router.get("/flows/{flow_id}", response_model=FlowRegistrationOut)
def get_flow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.get_flow(registration_id=flow_id, user_id=current_user.id)
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found") from None


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
        raise HTTPException(404, "Flow not found") from None


@router.delete("/flows/{flow_id}", status_code=204)
def delete_flow(
    flow_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.delete_flow(registration_id=flow_id)
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found") from None
    except FlowHasArtifactsError as e:
        raise HTTPException(409, str(e)) from e


@router.get(
    "/flows/{flow_id}/artifacts",
    response_model=list[GlobalArtifactOut],
)
def list_flow_artifacts(
    flow_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    """List all active artifacts produced by a registered flow."""
    try:
        return service.list_artifacts_for_flow(flow_id)
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found") from None


# ---------------------------------------------------------------------------
# Run History
# ---------------------------------------------------------------------------


@router.get("/runs", response_model=PaginatedFlowRuns)
def list_runs(
    registration_id: int | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.list_runs(registration_id=registration_id, limit=limit, offset=offset)


@router.get("/runs/{run_id}", response_model=FlowRunDetail)
def get_run_detail(
    run_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    """Get a single run including the YAML snapshot of the flow version that ran."""
    try:
        return service.get_run_detail(run_id)
    except RunNotFoundError:
        raise HTTPException(404, "Run not found") from None


# ---------------------------------------------------------------------------
# Run Logs
# ---------------------------------------------------------------------------


@router.get("/runs/{run_id}/log")
def get_run_log(
    run_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    """Return the log content for a scheduled run."""
    from pathlib import Path

    try:
        run = service.get_run_detail(run_id)
    except RunNotFoundError:
        raise HTTPException(404, "Run not found") from None

    if run.run_type != "scheduled":
        raise HTTPException(404, "Logs are only available for scheduled runs")

    log_file = Path.home() / ".flowfile" / "logs" / f"scheduled_run_{run_id}.log"
    if not log_file.exists():
        raise HTTPException(404, "Log file not found")

    return {"log": log_file.read_text(errors="replace")}


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
        raise HTTPException(404, "Run not found") from None
    except NoSnapshotError:
        raise HTTPException(422, "No flow snapshot available for this run") from None

    # Parse snapshot and assign a new unique flow_id so the imported
    # snapshot opens as a separate tab instead of overwriting an
    # already-open flow that shares the same original ID.
    try:
        parsed = json.loads(snapshot_data)
        suffix = ".json"
    except (json.JSONDecodeError, TypeError):
        import yaml

        parsed = yaml.safe_load(snapshot_data)
        suffix = ".yaml"

    parsed["flowfile_id"] = create_unique_id()

    if suffix == ".json":
        snapshot_data = json.dumps(parsed)
    else:
        import yaml

        snapshot_data = yaml.dump(parsed)

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
        return service.add_favorite(user_id=current_user.id, registration_id=flow_id)
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found") from None


@router.delete("/flows/{flow_id}/favorite", status_code=204)
def remove_favorite(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.remove_favorite(user_id=current_user.id, registration_id=flow_id)
    except FavoriteNotFoundError:
        raise HTTPException(404, "Favorite not found") from None


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
        return service.add_follow(user_id=current_user.id, registration_id=flow_id)
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found") from None


@router.delete("/flows/{flow_id}/follow", status_code=204)
def remove_follow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.remove_follow(user_id=current_user.id, registration_id=flow_id)
    except FollowNotFoundError:
        raise HTTPException(404, "Follow not found") from None


# ---------------------------------------------------------------------------
# Catalog Tables
# ---------------------------------------------------------------------------


@router.get("/tables", response_model=list[CatalogTableOut])
def list_tables(
    namespace_id: int | None = None,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """List catalog tables, optionally filtered by namespace."""
    return service.list_tables(namespace_id=namespace_id, user_id=current_user.id)


@router.post("/tables", response_model=CatalogTableOut, status_code=201)
def register_table(
    body: CatalogTableCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Register a new table by materializing a source file as Parquet."""
    try:
        validated_path = validate_path_under_cwd(body.file_path)
        return service.register_table(
            name=body.name,
            file_path=validated_path,
            owner_id=current_user.id,
            namespace_id=body.namespace_id,
            description=body.description,
        )
    except NamespaceNotFoundError:
        raise HTTPException(404, "Namespace not found") from None
    except TableExistsError:
        raise HTTPException(409, "A table with this name already exists in this namespace") from None
    except ValueError as e:
        raise HTTPException(422, str(e)) from e


@router.get("/tables/{table_id}", response_model=CatalogTableOut)
def get_table(
    table_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.get_table(table_id, user_id=current_user.id)
    except TableNotFoundError:
        raise HTTPException(404, "Catalog table not found") from None


@router.put("/tables/{table_id}", response_model=CatalogTableOut)
def update_table(
    table_id: int,
    body: CatalogTableUpdate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.update_table(
            table_id=table_id,
            name=body.name,
            description=body.description,
            namespace_id=body.namespace_id,
        )
    except TableNotFoundError:
        raise HTTPException(404, "Catalog table not found") from None


@router.delete("/tables/{table_id}", status_code=204)
def delete_table(
    table_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.delete_table(table_id)
    except TableNotFoundError:
        raise HTTPException(404, "Catalog table not found") from None


@router.get("/tables/{table_id}/preview", response_model=CatalogTablePreview)
def get_table_preview(
    table_id: int,
    limit: int = Query(100, ge=1, le=10000),
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Preview the first N rows of a catalog table."""
    try:
        return service.get_table_preview(table_id, limit=limit)
    except TableNotFoundError:
        raise HTTPException(404, "Catalog table not found") from None


# ---------------------------------------------------------------------------
# Table Favorites
# ---------------------------------------------------------------------------


@router.get("/table-favorites", response_model=list[CatalogTableOut])
def list_table_favorites(
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.list_table_favorites(user_id=current_user.id)


@router.post("/tables/{table_id}/favorite", response_model=TableFavoriteOut, status_code=201)
def add_table_favorite(
    table_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.add_table_favorite(user_id=current_user.id, table_id=table_id)
    except TableNotFoundError:
        raise HTTPException(404, "Catalog table not found") from None


@router.delete("/tables/{table_id}/favorite", status_code=204)
def remove_table_favorite(
    table_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.remove_table_favorite(user_id=current_user.id, table_id=table_id)
    except TableFavoriteNotFoundError:
        raise HTTPException(404, "Table favorite not found") from None


# ---------------------------------------------------------------------------
# Dashboard / Stats
# ---------------------------------------------------------------------------


@router.get("/stats", response_model=CatalogStats)
def get_catalog_stats(
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.get_catalog_stats(user_id=current_user.id)


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------


@router.get("/schedules", response_model=list[FlowScheduleOut])
def list_schedules(
    registration_id: int | None = None,
    service: CatalogService = Depends(get_catalog_service),
):
    """List schedules, optionally filtered by flow registration."""
    return service.list_schedules(registration_id=registration_id)


@router.post("/schedules", response_model=FlowScheduleOut, status_code=201)
def create_schedule(
    body: FlowScheduleCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.create_schedule(
            registration_id=body.registration_id,
            owner_id=current_user.id,
            schedule_type=body.schedule_type,
            interval_seconds=body.interval_seconds,
            trigger_table_id=body.trigger_table_id,
            trigger_table_ids=body.trigger_table_ids,
            enabled=body.enabled,
            description=body.description,
        )
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found") from None
    except TableNotFoundError:
        raise HTTPException(404, "Trigger table not found") from None
    except ValueError as e:
        raise HTTPException(422, str(e)) from e


@router.get("/schedules/{schedule_id}", response_model=FlowScheduleOut)
def get_schedule(
    schedule_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.get_schedule(schedule_id)
    except ScheduleNotFoundError:
        raise HTTPException(404, "Schedule not found") from None


@router.put("/schedules/{schedule_id}", response_model=FlowScheduleOut)
def update_schedule(
    schedule_id: int,
    body: FlowScheduleUpdate,
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        return service.update_schedule(
            schedule_id=schedule_id,
            enabled=body.enabled,
            interval_seconds=body.interval_seconds,
            description=body.description,
        )
    except ScheduleNotFoundError:
        raise HTTPException(404, "Schedule not found") from None
    except ValueError as e:
        raise HTTPException(422, str(e)) from e


@router.delete("/schedules/{schedule_id}", status_code=204)
def delete_schedule(
    schedule_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    try:
        service.delete_schedule(schedule_id)
    except ScheduleNotFoundError:
        raise HTTPException(404, "Schedule not found") from None


@router.post("/flows/{flow_id}/run", response_model=FlowRunOut)
def run_flow_now(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Trigger a registered flow immediately without needing a schedule."""
    try:
        return service.run_flow_now(registration_id=flow_id, user_id=current_user.id)
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found") from None
    except FlowAlreadyRunningError:
        raise HTTPException(409, "Flow already has an active run") from None


@router.post("/schedules/{schedule_id}/run-now", response_model=FlowRunOut)
def trigger_schedule_now(
    schedule_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Manually trigger a scheduled flow immediately."""
    try:
        return service.trigger_schedule_now(schedule_id=schedule_id, user_id=current_user.id)
    except ScheduleNotFoundError:
        raise HTTPException(404, "Schedule not found") from None
    except FlowNotFoundError:
        raise HTTPException(404, "Flow not found") from None
    except FlowAlreadyRunningError:
        raise HTTPException(409, "Flow already has an active run") from None


# ---------------------------------------------------------------------------
# Active Runs
# ---------------------------------------------------------------------------


@router.get("/active-runs", response_model=list[ActiveFlowRun])
def list_active_runs(
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """List all currently running flows."""
    return service.list_active_runs()


@router.post("/runs/{run_id}/cancel", status_code=204)
def cancel_run(
    run_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    """Cancel a running flow by marking it as failed."""
    try:
        service.cancel_run(run_id)
    except RunNotFoundError:
        raise HTTPException(404, "Run not found") from None


# ---------------------------------------------------------------------------
# Scheduler management
# ---------------------------------------------------------------------------


@router.get("/scheduler/status", response_model=SchedulerStatusOut)
def scheduler_status(
    db=Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Return the current scheduler lock status."""
    lock = db.get(SchedulerLock, 1)
    if lock is None:
        return SchedulerStatusOut(active=False)

    embedded = get_scheduler()
    is_embedded = embedded is not None and getattr(embedded, "_holder_id", None) == lock.holder_id

    return SchedulerStatusOut(
        active=True,
        holder_id=lock.holder_id,
        started_at=lock.started_at,
        heartbeat_at=lock.heartbeat_at,
        is_embedded=is_embedded,
    )


@router.post("/scheduler/start", status_code=200)
async def scheduler_start(current_user=Depends(get_current_active_user)):
    """Start the embedded scheduler. No-op if already running."""
    scheduler = get_scheduler()
    if scheduler is not None:
        return {"message": "Scheduler already running"}

    scheduler = FlowScheduler()
    await scheduler.start()
    set_scheduler(scheduler)
    return {"message": "Scheduler started"}


@router.post("/scheduler/stop", status_code=200)
async def scheduler_stop(current_user=Depends(get_current_active_user)):
    """Stop the embedded scheduler. No-op if not running."""
    scheduler = get_scheduler()
    if scheduler is None:
        return {"message": "Scheduler not running"}

    await scheduler.stop()
    set_scheduler(None)
    return {"message": "Scheduler stopped"}
