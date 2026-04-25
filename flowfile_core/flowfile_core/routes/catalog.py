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
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

import yaml
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.catalog import (
    AmbiguousTableError,
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
    VisualizationComputeError,
    VisualizationExistsError,
    VisualizationNotFoundError,
)
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import RunType, SchedulerLock
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
    DeltaTableHistory,
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
    QueryVirtualTableCreate,
    QueryVirtualTableUpdate,
    ResolveTableResult,
    SaveQueryAsFlowRequest,
    SchedulerStatusOut,
    SqlQueryRequest,
    SqlQueryResult,
    TableFavoriteOut,
    VirtualFlowTableCreate,
    VirtualFlowTableUpdate,
    VisualizationAdHocComputeRequest,
    VisualizationComputeRequest,
    VisualizationComputeResponse,
    VisualizationCreate,
    VisualizationFieldsRequest,
    VisualizationFieldsResponse,
    VisualizationOut,
    VisualizationUpdate,
)
from flowfile_scheduler.engine import STALE_THRESHOLD
from shared.storage_config import storage

router = APIRouter(
    prefix="/catalog",
    tags=["catalog"],
    dependencies=[Depends(get_current_active_user)],
)


def get_catalog_service(db: Session = Depends(get_db)) -> CatalogService:
    """FastAPI dependency that provides a configured ``CatalogService``."""
    repo = SQLAlchemyCatalogRepository(db)
    return CatalogService(repo)


# ---------------------------------------------------------------------------
# Exception → HTTP mapping
# ---------------------------------------------------------------------------

_CATALOG_EXCEPTION_MAP: dict[type[Exception], tuple[int, str | None]] = {
    NamespaceNotFoundError: (404, "Namespace not found"),
    NamespaceExistsError: (409, "Namespace with this name already exists at this level"),
    NestingLimitError: (422, "Cannot nest deeper than catalog -> schema"),
    NamespaceNotEmptyError: (422, "Cannot delete namespace with children or flows"),
    FlowNotFoundError: (404, "Flow not found"),
    FlowHasArtifactsError: (409, None),
    FlowAlreadyRunningError: (409, "Flow already has an active run"),
    TableNotFoundError: (404, "Catalog table not found"),
    TableExistsError: (409, "A table with this name already exists in this namespace"),
    AmbiguousTableError: (409, None),
    RunNotFoundError: (404, "Run not found"),
    ScheduleNotFoundError: (404, "Schedule not found"),
    FavoriteNotFoundError: (404, "Favorite not found"),
    FollowNotFoundError: (404, "Follow not found"),
    TableFavoriteNotFoundError: (404, "Table favorite not found"),
    NoSnapshotError: (422, "No flow snapshot available for this run"),
    VisualizationNotFoundError: (404, None),
    VisualizationExistsError: (409, None),
    VisualizationComputeError: (502, None),
    ValueError: (422, None),
}

_HANDLED_EXCEPTIONS = tuple(_CATALOG_EXCEPTION_MAP.keys())


def handle_catalog_exceptions(**overrides: str):
    """Decorator that maps domain exceptions to ``HTTPException`` responses.

    Default messages come from ``_CATALOG_EXCEPTION_MAP``.  Pass keyword
    arguments of the form ``ExceptionClassName="custom message"`` to
    override the default for a specific exception within one endpoint.
    """

    # Pre-build a name→type lookup for the overrides
    override_map: dict[type[Exception], str] = {}
    if overrides:
        name_to_type = {cls.__name__: cls for cls in _CATALOG_EXCEPTION_MAP}
        for name, msg in overrides.items():
            if name in name_to_type:
                override_map[name_to_type[name]] = msg

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except _HANDLED_EXCEPTIONS as exc:
                for exc_type in type(exc).__mro__:
                    if exc_type in _CATALOG_EXCEPTION_MAP:
                        status_code, default_msg = _CATALOG_EXCEPTION_MAP[exc_type]
                        msg = override_map.get(exc_type, default_msg)
                        if msg is None:
                            msg = str(exc)
                        if isinstance(exc, AmbiguousTableError):
                            raise HTTPException(
                                status_code,
                                detail={"message": msg, "name": exc.name, "candidates": exc.candidates},
                            ) from None
                        raise HTTPException(status_code, msg) from None
                raise  # unreachable but keeps linters happy

        return wrapper

    return decorator


@router.get("/namespaces", response_model=list[NamespaceOut])
def list_namespaces(
    parent_id: int | None = None,
    service: CatalogService = Depends(get_catalog_service),
):
    """List namespaces, optionally filtered by parent."""
    return service.list_namespaces(parent_id)


@router.post("/namespaces", response_model=NamespaceOut, status_code=201)
@handle_catalog_exceptions(NamespaceNotFoundError="Parent namespace not found")
def create_namespace(
    body: NamespaceCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Create a catalog (level 0) or schema (level 1) namespace."""
    return service.create_namespace(
        name=body.name,
        owner_id=current_user.id,
        parent_id=body.parent_id,
        description=body.description,
    )


@router.put("/namespaces/{namespace_id}", response_model=NamespaceOut)
@handle_catalog_exceptions()
def update_namespace(
    namespace_id: int,
    body: NamespaceUpdate,
    service: CatalogService = Depends(get_catalog_service),
):
    return service.update_namespace(
        namespace_id=namespace_id,
        name=body.name,
        description=body.description,
    )


@router.delete("/namespaces/{namespace_id}", status_code=204)
@handle_catalog_exceptions()
def delete_namespace(
    namespace_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    service.delete_namespace(namespace_id)


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


@router.get("/flows", response_model=list[FlowRegistrationOut])
def list_flows(
    namespace_id: int | None = None,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.list_flows(user_id=current_user.id, namespace_id=namespace_id)


@router.post("/flows", response_model=FlowRegistrationOut, status_code=201)
@handle_catalog_exceptions()
def register_flow(
    body: FlowRegistrationCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.register_flow(
        name=body.name,
        flow_path=body.flow_path,
        owner_id=current_user.id,
        namespace_id=body.namespace_id,
        description=body.description,
    )


@router.get("/flows/{flow_id}", response_model=FlowRegistrationOut)
@handle_catalog_exceptions()
def get_flow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.get_flow(registration_id=flow_id, user_id=current_user.id)


@router.put("/flows/{flow_id}", response_model=FlowRegistrationOut)
@handle_catalog_exceptions()
def update_flow(
    flow_id: int,
    body: FlowRegistrationUpdate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.update_flow(
        registration_id=flow_id,
        requesting_user_id=current_user.id,
        name=body.name,
        description=body.description,
        namespace_id=body.namespace_id,
    )


@router.delete("/flows/{flow_id}", status_code=204)
@handle_catalog_exceptions()
def delete_flow(
    flow_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    service.delete_flow(registration_id=flow_id)


@router.get(
    "/flows/{flow_id}/artifacts",
    response_model=list[GlobalArtifactOut],
)
@handle_catalog_exceptions()
def list_flow_artifacts(
    flow_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    """List all active artifacts produced by a registered flow."""
    return service.list_artifacts_for_flow(flow_id)


@router.get("/runs", response_model=PaginatedFlowRuns)
def list_runs(
    registration_id: int | None = None,
    schedule_id: int | None = None,
    run_type: RunType | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.list_runs(
        registration_id=registration_id, schedule_id=schedule_id, run_type=run_type, limit=limit, offset=offset
    )


@router.get("/runs/{run_id}", response_model=FlowRunDetail)
@handle_catalog_exceptions()
def get_run_detail(
    run_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    """Get a single run including the YAML snapshot of the flow version that ran."""
    return service.get_run_detail(run_id)


# ---------------------------------------------------------------------------
# Run Logs
# ---------------------------------------------------------------------------


@router.get("/runs/{run_id}/log")
@handle_catalog_exceptions()
def get_run_log(
    run_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    """Return the log content for a scheduled run."""
    run = service.get_run_detail(run_id)

    if run.run_type not in ("scheduled", "manual"):
        raise HTTPException(404, "Logs are only available for scheduled/manual runs")

    log_file = Path.home() / ".flowfile" / "logs" / f"scheduled_run_{run_id}.log"
    if not log_file.exists():
        raise HTTPException(404, "Log file not found")

    return {"log": log_file.read_text(errors="replace")}


# ---------------------------------------------------------------------------
# Open Run Snapshot in Designer
# ---------------------------------------------------------------------------


@router.post("/runs/{run_id}/open")
@handle_catalog_exceptions()
def open_run_snapshot(
    run_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Write the run's flow snapshot to a temp file and import it into the designer."""
    snapshot_data = service.get_run_snapshot(run_id)

    # Parse snapshot and assign a new unique flow_id so the imported
    # snapshot opens as a separate tab instead of overwriting an
    # already-open flow that shares the same original ID.
    try:
        parsed = json.loads(snapshot_data)
        suffix = ".json"
    except (json.JSONDecodeError, TypeError):
        parsed = yaml.safe_load(snapshot_data)
        suffix = ".yaml"

    parsed["flowfile_id"] = create_unique_id()

    if suffix == ".json":
        snapshot_data = json.dumps(parsed)
    else:
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
@handle_catalog_exceptions()
def add_favorite(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.add_favorite(user_id=current_user.id, registration_id=flow_id)


@router.delete("/flows/{flow_id}/favorite", status_code=204)
@handle_catalog_exceptions()
def remove_favorite(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    service.remove_favorite(user_id=current_user.id, registration_id=flow_id)


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
@handle_catalog_exceptions()
def add_follow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.add_follow(user_id=current_user.id, registration_id=flow_id)


@router.delete("/flows/{flow_id}/follow", status_code=204)
@handle_catalog_exceptions()
def remove_follow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    service.remove_follow(user_id=current_user.id, registration_id=flow_id)


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
@handle_catalog_exceptions()
def register_table(
    body: CatalogTableCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Register a new table by materializing a source file as Parquet."""
    validated_path = validate_path_under_cwd(body.file_path)
    return service.register_table(
        name=body.name,
        file_path=validated_path,
        owner_id=current_user.id,
        namespace_id=body.namespace_id,
        description=body.description,
    )


@router.get("/tables/resolve", response_model=ResolveTableResult)
@handle_catalog_exceptions()
def resolve_table(
    q: str,
    namespace_id: int | None = None,
    strict: bool = False,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Resolve a catalog table by ``"namespace.table"`` or bare ``"table"`` reference.

    Strict mode raises ``AmbiguousTableError`` (→ 409) when a bare name matches >1 row.
    Default mode soft-picks deterministically and reports alternatives in ``warnings``.
    """
    table_out, warnings = service.resolve_table_out(
        q,
        default_namespace_id=namespace_id,
        strict=strict,
        user_id=current_user.id,
    )
    return ResolveTableResult(table=table_out, warnings=warnings)


@router.get("/tables/{table_id}", response_model=CatalogTableOut)
@handle_catalog_exceptions()
def get_table(
    table_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.get_table(table_id, user_id=current_user.id)


@router.put("/tables/{table_id}", response_model=CatalogTableOut)
@handle_catalog_exceptions()
def update_table(
    table_id: int,
    body: CatalogTableUpdate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.update_table(
        table_id=table_id,
        name=body.name,
        description=body.description,
        namespace_id=body.namespace_id,
    )


@router.delete("/tables/{table_id}", status_code=204)
@handle_catalog_exceptions()
def delete_table(
    table_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    service.delete_table(table_id)


@router.get("/tables/{table_id}/preview", response_model=CatalogTablePreview)
@handle_catalog_exceptions()
def get_table_preview(
    table_id: int,
    limit: int = Query(100, ge=1, le=10000),
    version: int | None = Query(None),
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Preview the first N rows of a catalog table.

    When ``version`` is provided and the table is a Delta table, returns
    data from that specific historical version.
    """
    return service.get_table_preview(table_id, limit=limit, version=version, user_id=current_user.id)


@router.get("/tables/{table_id}/history", response_model=DeltaTableHistory)
@handle_catalog_exceptions()
def get_table_history(
    table_id: int,
    limit: int | None = Query(None),
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Return version history for a Delta catalog table."""
    return service.get_table_history(table_id, limit=limit)


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
@handle_catalog_exceptions()
def add_table_favorite(
    table_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.add_table_favorite(user_id=current_user.id, table_id=table_id)


@router.delete("/tables/{table_id}/favorite", status_code=204)
@handle_catalog_exceptions()
def remove_table_favorite(
    table_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    service.remove_table_favorite(user_id=current_user.id, table_id=table_id)


# ---------------------------------------------------------------------------
# Catalog Visualizations
# ---------------------------------------------------------------------------


@router.get("/tables/{table_id}/visualizations", response_model=list[VisualizationOut])
@handle_catalog_exceptions()
def list_visualizations(
    table_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """List saved Graphic Walker chart specs attached to a catalog table."""
    return service.list_visualizations(table_id, user_id=current_user.id)


@router.post("/tables/{table_id}/visualizations", response_model=VisualizationOut, status_code=201)
@handle_catalog_exceptions()
def create_visualization(
    table_id: int,
    body: VisualizationCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.create_visualization(table_id, body, user_id=current_user.id)


@router.put("/tables/{table_id}/visualizations/{viz_id}", response_model=VisualizationOut)
@handle_catalog_exceptions()
def update_visualization(
    table_id: int,
    viz_id: int,
    body: VisualizationUpdate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.update_visualization(table_id, viz_id, body, user_id=current_user.id)


@router.delete("/tables/{table_id}/visualizations/{viz_id}", status_code=204)
@handle_catalog_exceptions()
def delete_visualization(
    table_id: int,
    viz_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    service.delete_visualization(table_id, viz_id, user_id=current_user.id)


@router.post(
    "/tables/{table_id}/visualizations/{viz_id}/compute",
    response_model=VisualizationComputeResponse,
)
@handle_catalog_exceptions()
def compute_saved_visualization(
    table_id: int,
    viz_id: int,
    body: VisualizationComputeRequest | None = None,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Run a saved chart's query payload through the worker viz cache."""
    max_rows = body.max_rows if body is not None else None
    return service.compute_saved_visualization(table_id, viz_id, max_rows, user_id=current_user.id)


@router.post("/visualizations/compute", response_model=VisualizationComputeResponse)
@handle_catalog_exceptions()
def compute_ad_hoc_visualization(
    body: VisualizationAdHocComputeRequest,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Compute a chart from a transient source (ad-hoc SQL or a catalog table).

    Used by the live Graphic Walker editor — including the SQL Explore panel —
    to share the worker's per-source LazyFrame cache while iterating on a chart.
    """
    return service.compute_ad_hoc_visualization(
        source=body.source,
        payload=body.payload,
        max_rows=body.max_rows,
        user_id=current_user.id,
    )


@router.post("/visualizations/fields", response_model=VisualizationFieldsResponse)
@handle_catalog_exceptions()
def get_visualization_fields(
    body: VisualizationFieldsRequest,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Return the Graphic Walker field schema for a viz source."""
    return service.get_visualization_fields(body.source, user_id=current_user.id)


# ---------------------------------------------------------------------------
# Virtual Flow Tables
# ---------------------------------------------------------------------------


@router.post("/virtual-tables", response_model=CatalogTableOut, status_code=201)
@handle_catalog_exceptions(FlowNotFoundError="Producer flow not found")
def create_virtual_flow_table(
    body: VirtualFlowTableCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Create a virtual flow table (non-materialized catalog entry)."""
    table_out = service.create_virtual_flow_table(
        name=body.name,
        owner_id=current_user.id,
        producer_registration_id=body.producer_registration_id,
        namespace_id=body.namespace_id,
        description=body.description,
    )

    # Compute laziness blockers from the producer flow
    flow_reg = service.repo.get_flow(body.producer_registration_id)
    blockers = CatalogService._compute_laziness_blockers(flow_reg.flow_path if flow_reg else None)
    if blockers:
        table_out.laziness_blockers = blockers

    return table_out


@router.put("/virtual-tables/{table_id}", response_model=CatalogTableOut)
@handle_catalog_exceptions(TableNotFoundError="Virtual table not found", FlowNotFoundError="Producer flow not found")
def update_virtual_flow_table(
    table_id: int,
    body: VirtualFlowTableUpdate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Update a virtual flow table's metadata or producer."""
    return service.update_virtual_flow_table(
        table_id=table_id,
        name=body.name,
        description=body.description,
        namespace_id=body.namespace_id,
        producer_registration_id=body.producer_registration_id,
    )


@router.post("/virtual-tables/{table_id}/resolve")
@handle_catalog_exceptions(TableNotFoundError="Virtual table not found", FlowNotFoundError="Producer flow not found")
def resolve_virtual_flow_table(
    table_id: int,
    limit: int = Query(100, ge=1, le=10000),
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Resolve a virtual flow table and return a preview of the result."""
    lf = service.resolve_virtual_flow_table(table_id, user_id=current_user.id)
    df = lf.head(limit).collect()
    return {
        "columns": df.columns,
        "dtypes": [str(dt) for dt in df.dtypes],
        "rows": df.rows(),
        "total_rows": df.height,
    }


# ---------------------------------------------------------------------------
# Query-based Virtual Tables
# ---------------------------------------------------------------------------


@router.post("/query-virtual-tables", response_model=CatalogTableOut, status_code=201)
@handle_catalog_exceptions()
def create_query_virtual_table(
    body: QueryVirtualTableCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Create a query-based virtual table from a SQL expression."""
    return service.create_query_virtual_table(
        name=body.name,
        owner_id=current_user.id,
        sql_query=body.sql_query,
        namespace_id=body.namespace_id,
        description=body.description,
    )


@router.put("/query-virtual-tables/{table_id}", response_model=CatalogTableOut)
@handle_catalog_exceptions(TableNotFoundError="Query virtual table not found")
def update_query_virtual_table(
    table_id: int,
    body: QueryVirtualTableUpdate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Update a query-based virtual table."""
    return service.update_query_virtual_table(
        table_id=table_id,
        name=body.name,
        description=body.description,
        namespace_id=body.namespace_id,
        sql_query=body.sql_query,
    )


# ---------------------------------------------------------------------------

# SQL Query
# ---------------------------------------------------------------------------


@router.post("/sql/execute", response_model=SqlQueryResult)
def execute_sql_query(
    body: SqlQueryRequest,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Execute a SQL query against Delta catalog tables."""
    return service.execute_sql_query(query=body.query, max_rows=body.max_rows, user_id=current_user.id)


@router.post("/sql/save-as-flow")
def save_query_as_flow(
    body: SaveQueryAsFlowRequest,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Save a SQL query as a registered flow with catalog_reader + sql_query nodes."""
    try:
        flow_id = service.save_sql_query_as_flow(
            query=body.query,
            name=body.name,
            owner_id=current_user.id,
            namespace_id=body.namespace_id,
            description=body.description,
            used_tables=body.used_tables,
        )
        return {"flow_id": flow_id}
    except Exception as e:
        raise HTTPException(500, str(e)) from e


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
@handle_catalog_exceptions(TableNotFoundError="Trigger table not found")
def create_schedule(
    body: FlowScheduleCreate,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    return service.create_schedule(
        registration_id=body.registration_id,
        owner_id=current_user.id,
        schedule_type=body.schedule_type,
        interval_seconds=body.interval_seconds,
        trigger_table_id=body.trigger_table_id,
        trigger_table_ids=body.trigger_table_ids,
        enabled=body.enabled,
        name=body.name,
        description=body.description,
    )


@router.get("/schedules/{schedule_id}", response_model=FlowScheduleOut)
@handle_catalog_exceptions()
def get_schedule(
    schedule_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    return service.get_schedule(schedule_id)


@router.put("/schedules/{schedule_id}", response_model=FlowScheduleOut)
@handle_catalog_exceptions()
def update_schedule(
    schedule_id: int,
    body: FlowScheduleUpdate,
    service: CatalogService = Depends(get_catalog_service),
):
    return service.update_schedule(
        schedule_id=schedule_id,
        enabled=body.enabled,
        interval_seconds=body.interval_seconds,
        name=body.name,
        description=body.description,
    )


@router.delete("/schedules/{schedule_id}", status_code=204)
@handle_catalog_exceptions()
def delete_schedule(
    schedule_id: int,
    service: CatalogService = Depends(get_catalog_service),
):
    service.delete_schedule(schedule_id)


@router.post("/flows/{flow_id}/run", response_model=FlowRunOut)
@handle_catalog_exceptions()
def run_flow_now(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Trigger a registered flow immediately without needing a schedule."""
    return service.run_flow_now(registration_id=flow_id, user_id=current_user.id)


@router.post("/schedules/{schedule_id}/run-now", response_model=FlowRunOut)
@handle_catalog_exceptions()
def trigger_schedule_now(
    schedule_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Manually trigger a scheduled flow immediately."""
    return service.trigger_schedule_now(schedule_id=schedule_id, user_id=current_user.id)


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
@handle_catalog_exceptions()
def cancel_run(
    run_id: int,
    current_user=Depends(get_current_active_user),
    service: CatalogService = Depends(get_catalog_service),
):
    """Cancel a running flow by marking it as failed."""
    service.cancel_run(run_id)


# ---------------------------------------------------------------------------
# Scheduler management
# ---------------------------------------------------------------------------


@router.get("/scheduler/status", response_model=SchedulerStatusOut)
def scheduler_status(
    db=Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Return the current scheduler lock status."""
    import logging

    logger = logging.getLogger("flowfile.scheduler.status")

    lock = db.get(SchedulerLock, 1)
    if lock is None:
        logger.debug("No scheduler lock row found — reporting inactive")
        return SchedulerStatusOut(active=False)

    embedded = get_scheduler()
    is_embedded = embedded is not None and getattr(embedded, "_holder_id", None) == lock.holder_id

    # Detect stale heartbeat — scheduler may have died without releasing the lock
    active = True
    if lock.heartbeat_at is not None:
        heartbeat = lock.heartbeat_at
        if heartbeat.tzinfo is None:
            heartbeat = heartbeat.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - heartbeat).total_seconds()
        if elapsed > STALE_THRESHOLD:
            active = False
            logger.warning(
                "Scheduler heartbeat is STALE: last heartbeat %.1fs ago (threshold=%ds), "
                "holder_id=%s, heartbeat_at=%s — marking inactive",
                elapsed,
                STALE_THRESHOLD,
                lock.holder_id,
                lock.heartbeat_at,
            )
        else:
            logger.debug(
                "Scheduler heartbeat OK: %.1fs ago (threshold=%ds), holder_id=%s",
                elapsed,
                STALE_THRESHOLD,
                lock.holder_id,
            )
    else:
        logger.warning("Scheduler lock exists but heartbeat_at is None — holder_id=%s", lock.holder_id)

    logger.debug(
        "Scheduler status response: active=%s, holder_id=%s, is_embedded=%s, started_at=%s, heartbeat_at=%s",
        active,
        lock.holder_id,
        is_embedded,
        lock.started_at,
        lock.heartbeat_at,
    )

    return SchedulerStatusOut(
        active=active,
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
