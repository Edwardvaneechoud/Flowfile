"""API routes for the Flow Catalog system.

Provides endpoints for:
- Namespace management (Unity Catalog-style catalog / schema hierarchy)
- Flow registration (persistent flow metadata)
- Run history with versioned snapshots
- Favorites and follows
"""

import json
import os
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query
from shared.storage_config import storage
from sqlalchemy.orm import Session

from flowfile_core import flow_file_handler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.database.connection import get_db
from flowfile_core.database.models import (
    CatalogNamespace,
    FlowFavorite,
    FlowFollow,
    FlowRegistration,
    FlowRun,
)
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
# Helpers
# ---------------------------------------------------------------------------

def _enrich_flow(
    flow: FlowRegistration,
    db: Session,
    user_id: int,
) -> FlowRegistrationOut:
    """Attach favourite/follow flags and run stats to a FlowRegistration row."""
    is_fav = db.query(FlowFavorite).filter_by(
        user_id=user_id, registration_id=flow.id
    ).first() is not None
    is_follow = db.query(FlowFollow).filter_by(
        user_id=user_id, registration_id=flow.id
    ).first() is not None
    run_count = db.query(FlowRun).filter_by(registration_id=flow.id).count()
    last_run = (
        db.query(FlowRun)
        .filter_by(registration_id=flow.id)
        .order_by(FlowRun.started_at.desc())
        .first()
    )
    return FlowRegistrationOut(
        id=flow.id,
        name=flow.name,
        description=flow.description,
        flow_path=flow.flow_path,
        namespace_id=flow.namespace_id,
        owner_id=flow.owner_id,
        created_at=flow.created_at,
        updated_at=flow.updated_at,
        is_favorite=is_fav,
        is_following=is_follow,
        run_count=run_count,
        last_run_at=last_run.started_at if last_run else None,
        last_run_success=last_run.success if last_run else None,
        file_exists=os.path.exists(flow.flow_path) if flow.flow_path else False,
    )


# ---------------------------------------------------------------------------
# Namespace CRUD
# ---------------------------------------------------------------------------


@router.get("/namespaces", response_model=list[NamespaceOut])
def list_namespaces(
    parent_id: int | None = None,
    db: Session = Depends(get_db),
):
    """List namespaces, optionally filtered by parent."""
    q = db.query(CatalogNamespace)
    if parent_id is not None:
        q = q.filter(CatalogNamespace.parent_id == parent_id)
    else:
        q = q.filter(CatalogNamespace.parent_id.is_(None))
    return q.order_by(CatalogNamespace.name).all()


@router.post("/namespaces", response_model=NamespaceOut, status_code=201)
def create_namespace(
    body: NamespaceCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Create a catalog (level 0) or schema (level 1) namespace."""
    level = 0
    if body.parent_id is not None:
        parent = db.get(CatalogNamespace, body.parent_id)
        if parent is None:
            raise HTTPException(404, "Parent namespace not found")
        if parent.level >= 1:
            raise HTTPException(422, "Cannot nest deeper than catalog -> schema")
        level = parent.level + 1

    existing = (
        db.query(CatalogNamespace)
        .filter_by(name=body.name, parent_id=body.parent_id)
        .first()
    )
    if existing:
        raise HTTPException(409, "Namespace with this name already exists at this level")

    ns = CatalogNamespace(
        name=body.name,
        parent_id=body.parent_id,
        level=level,
        description=body.description,
        owner_id=current_user.id,
    )
    db.add(ns)
    db.commit()
    db.refresh(ns)
    return ns


@router.put("/namespaces/{namespace_id}", response_model=NamespaceOut)
def update_namespace(
    namespace_id: int,
    body: NamespaceUpdate,
    db: Session = Depends(get_db),
):
    ns = db.get(CatalogNamespace, namespace_id)
    if ns is None:
        raise HTTPException(404, "Namespace not found")
    if body.name is not None:
        ns.name = body.name
    if body.description is not None:
        ns.description = body.description
    db.commit()
    db.refresh(ns)
    return ns


@router.delete("/namespaces/{namespace_id}", status_code=204)
def delete_namespace(
    namespace_id: int,
    db: Session = Depends(get_db),
):
    ns = db.get(CatalogNamespace, namespace_id)
    if ns is None:
        raise HTTPException(404, "Namespace not found")
    # Prevent deletion if children or flows exist
    children = db.query(CatalogNamespace).filter_by(parent_id=namespace_id).count()
    flows = db.query(FlowRegistration).filter_by(namespace_id=namespace_id).count()
    if children > 0 or flows > 0:
        raise HTTPException(422, "Cannot delete namespace with children or flows")
    db.delete(ns)
    db.commit()


@router.get("/namespaces/tree", response_model=list[NamespaceTree])
def get_namespace_tree(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Return the full catalog tree with flows nested under schemas."""
    catalogs = (
        db.query(CatalogNamespace)
        .filter(CatalogNamespace.parent_id.is_(None))
        .order_by(CatalogNamespace.name)
        .all()
    )
    result = []
    for cat in catalogs:
        schemas_db = (
            db.query(CatalogNamespace)
            .filter_by(parent_id=cat.id)
            .order_by(CatalogNamespace.name)
            .all()
        )
        children = []
        for schema in schemas_db:
            flows_db = (
                db.query(FlowRegistration)
                .filter_by(namespace_id=schema.id)
                .order_by(FlowRegistration.name)
                .all()
            )
            flow_outs = [_enrich_flow(f, db, current_user.id) for f in flows_db]
            children.append(
                NamespaceTree(
                    id=schema.id,
                    name=schema.name,
                    parent_id=schema.parent_id,
                    level=schema.level,
                    description=schema.description,
                    owner_id=schema.owner_id,
                    created_at=schema.created_at,
                    updated_at=schema.updated_at,
                    children=[],
                    flows=flow_outs,
                )
            )
        # Also include flows directly under catalog (unschema'd)
        root_flows_db = (
            db.query(FlowRegistration)
            .filter_by(namespace_id=cat.id)
            .order_by(FlowRegistration.name)
            .all()
        )
        root_flows = [_enrich_flow(f, db, current_user.id) for f in root_flows_db]
        result.append(
            NamespaceTree(
                id=cat.id,
                name=cat.name,
                parent_id=cat.parent_id,
                level=cat.level,
                description=cat.description,
                owner_id=cat.owner_id,
                created_at=cat.created_at,
                updated_at=cat.updated_at,
                children=children,
                flows=root_flows,
            )
        )
    return result


# ---------------------------------------------------------------------------
# Default namespace helper
# ---------------------------------------------------------------------------


@router.get("/default-namespace-id")
def get_default_namespace_id(
    db: Session = Depends(get_db),
):
    """Return the ID of the default 'user_flows' schema under 'General'."""
    general = db.query(CatalogNamespace).filter_by(name="General", parent_id=None).first()
    if general is None:
        return None
    user_flows = db.query(CatalogNamespace).filter_by(
        name="user_flows", parent_id=general.id
    ).first()
    if user_flows is None:
        return None
    return user_flows.id


# ---------------------------------------------------------------------------
# Flow Registration CRUD
# ---------------------------------------------------------------------------


@router.get("/flows", response_model=list[FlowRegistrationOut])
def list_flows(
    namespace_id: int | None = None,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    q = db.query(FlowRegistration)
    if namespace_id is not None:
        q = q.filter_by(namespace_id=namespace_id)
    flows = q.order_by(FlowRegistration.name).all()
    return [_enrich_flow(f, db, current_user.id) for f in flows]


@router.post("/flows", response_model=FlowRegistrationOut, status_code=201)
def register_flow(
    body: FlowRegistrationCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    if body.namespace_id is not None:
        ns = db.get(CatalogNamespace, body.namespace_id)
        if ns is None:
            raise HTTPException(404, "Namespace not found")
    flow = FlowRegistration(
        name=body.name,
        description=body.description,
        flow_path=body.flow_path,
        namespace_id=body.namespace_id,
        owner_id=current_user.id,
    )
    db.add(flow)
    db.commit()
    db.refresh(flow)
    return _enrich_flow(flow, db, current_user.id)


@router.get("/flows/{flow_id}", response_model=FlowRegistrationOut)
def get_flow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    flow = db.get(FlowRegistration, flow_id)
    if flow is None:
        raise HTTPException(404, "Flow not found")
    return _enrich_flow(flow, db, current_user.id)


@router.put("/flows/{flow_id}", response_model=FlowRegistrationOut)
def update_flow(
    flow_id: int,
    body: FlowRegistrationUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    flow = db.get(FlowRegistration, flow_id)
    if flow is None:
        raise HTTPException(404, "Flow not found")
    if body.name is not None:
        flow.name = body.name
    if body.description is not None:
        flow.description = body.description
    if body.namespace_id is not None:
        flow.namespace_id = body.namespace_id
    db.commit()
    db.refresh(flow)
    return _enrich_flow(flow, db, current_user.id)


@router.delete("/flows/{flow_id}", status_code=204)
def delete_flow(
    flow_id: int,
    db: Session = Depends(get_db),
):
    flow = db.get(FlowRegistration, flow_id)
    if flow is None:
        raise HTTPException(404, "Flow not found")
    # Clean up related records
    db.query(FlowFavorite).filter_by(registration_id=flow_id).delete()
    db.query(FlowFollow).filter_by(registration_id=flow_id).delete()
    db.delete(flow)
    db.commit()


# ---------------------------------------------------------------------------
# Run History
# ---------------------------------------------------------------------------


@router.get("/runs", response_model=list[FlowRunOut])
def list_runs(
    registration_id: int | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    q = db.query(FlowRun)
    if registration_id is not None:
        q = q.filter_by(registration_id=registration_id)
    runs = (
        q.order_by(FlowRun.started_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return [
        FlowRunOut(
            id=r.id,
            registration_id=r.registration_id,
            flow_name=r.flow_name,
            flow_path=r.flow_path,
            user_id=r.user_id,
            started_at=r.started_at,
            ended_at=r.ended_at,
            success=r.success,
            nodes_completed=r.nodes_completed,
            number_of_nodes=r.number_of_nodes,
            duration_seconds=r.duration_seconds,
            run_type=r.run_type,
            has_snapshot=r.flow_snapshot is not None,
        )
        for r in runs
    ]


@router.get("/runs/{run_id}", response_model=FlowRunDetail)
def get_run_detail(
    run_id: int,
    db: Session = Depends(get_db),
):
    """Get a single run including the YAML snapshot of the flow version that ran."""
    run = db.get(FlowRun, run_id)
    if run is None:
        raise HTTPException(404, "Run not found")
    return FlowRunDetail(
        id=run.id,
        registration_id=run.registration_id,
        flow_name=run.flow_name,
        flow_path=run.flow_path,
        user_id=run.user_id,
        started_at=run.started_at,
        ended_at=run.ended_at,
        success=run.success,
        nodes_completed=run.nodes_completed,
        number_of_nodes=run.number_of_nodes,
        duration_seconds=run.duration_seconds,
        run_type=run.run_type,
        has_snapshot=run.flow_snapshot is not None,
        flow_snapshot=run.flow_snapshot,
        node_results_json=run.node_results_json,
    )


# ---------------------------------------------------------------------------
# Open Run Snapshot in Designer
# ---------------------------------------------------------------------------


@router.post("/runs/{run_id}/open")
def open_run_snapshot(
    run_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Write the run's flow snapshot to a temp file and import it into the designer."""
    run = db.get(FlowRun, run_id)
    if run is None:
        raise HTTPException(404, "Run not found")
    if not run.flow_snapshot:
        raise HTTPException(422, "No flow snapshot available for this run")

    # Determine file extension based on content
    snapshot_data = run.flow_snapshot
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
    db: Session = Depends(get_db),
):
    favs = (
        db.query(FlowFavorite)
        .filter_by(user_id=current_user.id)
        .order_by(FlowFavorite.created_at.desc())
        .all()
    )
    result = []
    for fav in favs:
        flow = db.get(FlowRegistration, fav.registration_id)
        if flow:
            result.append(_enrich_flow(flow, db, current_user.id))
    return result


@router.post("/flows/{flow_id}/favorite", response_model=FavoriteOut, status_code=201)
def add_favorite(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    flow = db.get(FlowRegistration, flow_id)
    if flow is None:
        raise HTTPException(404, "Flow not found")
    existing = db.query(FlowFavorite).filter_by(
        user_id=current_user.id, registration_id=flow_id
    ).first()
    if existing:
        return existing
    fav = FlowFavorite(user_id=current_user.id, registration_id=flow_id)
    db.add(fav)
    db.commit()
    db.refresh(fav)
    return fav


@router.delete("/flows/{flow_id}/favorite", status_code=204)
def remove_favorite(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    fav = db.query(FlowFavorite).filter_by(
        user_id=current_user.id, registration_id=flow_id
    ).first()
    if fav is None:
        raise HTTPException(404, "Favorite not found")
    db.delete(fav)
    db.commit()


# ---------------------------------------------------------------------------
# Follows
# ---------------------------------------------------------------------------


@router.get("/following", response_model=list[FlowRegistrationOut])
def list_following(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    follows = (
        db.query(FlowFollow)
        .filter_by(user_id=current_user.id)
        .order_by(FlowFollow.created_at.desc())
        .all()
    )
    result = []
    for follow in follows:
        flow = db.get(FlowRegistration, follow.registration_id)
        if flow:
            result.append(_enrich_flow(flow, db, current_user.id))
    return result


@router.post("/flows/{flow_id}/follow", response_model=FollowOut, status_code=201)
def add_follow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    flow = db.get(FlowRegistration, flow_id)
    if flow is None:
        raise HTTPException(404, "Flow not found")
    existing = db.query(FlowFollow).filter_by(
        user_id=current_user.id, registration_id=flow_id
    ).first()
    if existing:
        return existing
    follow = FlowFollow(user_id=current_user.id, registration_id=flow_id)
    db.add(follow)
    db.commit()
    db.refresh(follow)
    return follow


@router.delete("/flows/{flow_id}/follow", status_code=204)
def remove_follow(
    flow_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    follow = db.query(FlowFollow).filter_by(
        user_id=current_user.id, registration_id=flow_id
    ).first()
    if follow is None:
        raise HTTPException(404, "Follow not found")
    db.delete(follow)
    db.commit()


# ---------------------------------------------------------------------------
# Dashboard / Stats
# ---------------------------------------------------------------------------


@router.get("/stats", response_model=CatalogStats)
def get_catalog_stats(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    total_ns = db.query(CatalogNamespace).filter_by(level=0).count()
    total_flows = db.query(FlowRegistration).count()
    total_runs = db.query(FlowRun).count()
    total_favs = db.query(FlowFavorite).filter_by(user_id=current_user.id).count()
    recent = (
        db.query(FlowRun)
        .order_by(FlowRun.started_at.desc())
        .limit(10)
        .all()
    )
    recent_out = [
        FlowRunOut(
            id=r.id,
            registration_id=r.registration_id,
            flow_name=r.flow_name,
            flow_path=r.flow_path,
            user_id=r.user_id,
            started_at=r.started_at,
            ended_at=r.ended_at,
            success=r.success,
            nodes_completed=r.nodes_completed,
            number_of_nodes=r.number_of_nodes,
            duration_seconds=r.duration_seconds,
            run_type=r.run_type,
            has_snapshot=r.flow_snapshot is not None,
        )
        for r in recent
    ]
    fav_ids = [
        f.registration_id
        for f in db.query(FlowFavorite).filter_by(user_id=current_user.id).all()
    ]
    fav_flows = []
    for fid in fav_ids:
        flow = db.get(FlowRegistration, fid)
        if flow:
            fav_flows.append(_enrich_flow(flow, db, current_user.id))
    return CatalogStats(
        total_namespaces=total_ns,
        total_flows=total_flows,
        total_runs=total_runs,
        total_favorites=total_favs,
        recent_runs=recent_out,
        favorite_flows=fav_flows,
    )
