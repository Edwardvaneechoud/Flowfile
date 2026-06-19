"""DB → files writers. Deterministic, secret-free, atomic.

Reads the catalog DB and renders the project tree. Secrets are emitted as
``${secret:NAME}`` placeholders — never ciphertext (which is user-keyed and
useless on another machine).
"""

from __future__ import annotations

import copy
import json
import logging
from pathlib import Path

import yaml
from sqlalchemy.orm import Session

from flowfile_core.catalog.services.tables import _is_managed_table_path
from flowfile_core.database.models import (
    CatalogDashboard,
    CatalogNamespace,
    CatalogTable,
    CatalogVisualization,
    CloudStorageConnection,
    DatabaseConnection,
    FlowRegistration,
    FlowSchedule,
    GlobalArtifact,
    Kernel,
    Secret,
)
from flowfile_core.project import manifest
from flowfile_core.project.normalize import normalize_flow_data, safe_stem, unique_stem, write_yaml
from flowfile_core.project.secrets_resolver import make_placeholder

logger = logging.getLogger(__name__)

# (file field, model FK column) for cloud secrets that round-trip through the store fns.
_CLOUD_SECRETS = [
    ("aws_secret_access_key", "aws_secret_access_key_id"),
    ("azure_account_key", "azure_account_key_id"),
    ("azure_client_secret", "azure_client_secret_id"),
    ("azure_sas_token", "azure_sas_token_id"),
    ("gcs_service_account_key", "gcs_service_account_key_id"),
]
_CLOUD_SECRET_FK_COLUMNS = [fk for _, fk in _CLOUD_SECRETS] + ["aws_session_token_id"]
_PROJECTABLE_SCHEDULE_TYPES = ("interval", "cron")


def _secret_name(db: Session, secret_id: int | None) -> str | None:
    if not secret_id:
        return None
    row = db.query(Secret).filter(Secret.id == secret_id).first()
    return row.name if row else None


# --- flows -------------------------------------------------------------------


def _load_flow_data(flow_path: str) -> dict | None:
    p = Path(flow_path)
    if not p.exists() or p.suffix.lower() not in (".yaml", ".yml", ".json"):
        return None
    text = p.read_text(encoding="utf-8")
    return json.loads(text) if p.suffix.lower() == ".json" else yaml.safe_load(text)


def _name_stem(name: str | None) -> str:
    """Clean file stem for a flow name. Strips a trailing ``.flow`` so a flow saved to
    ``x.flow.yaml`` (name becomes ``x.flow``) doesn't project to ``x.flow.flow.yaml``."""
    name = name or ""
    if name.endswith(".flow"):
        name = name[: -len(".flow")]
    return safe_stem(name)


def _flow_stem(root: Path, reg: FlowRegistration) -> str:
    """The stem of the flow's actual projected file (matched by ``flow_uuid``), so a schedule lines up
    with a disambiguated ``<stem>_<uuid8>.flow.yaml`` instead of overwriting a same-named flow's file.
    Falls back to the intrinsic ``flowfile_name`` stem when the flow file isn't on disk yet."""
    for p in manifest.flows_dir(root).glob("*.flow.yaml"):
        try:
            data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        if data.get("flow_uuid") == reg.flow_uuid:
            return p.name[: -len(".flow.yaml")]
    data = _load_flow_data(reg.flow_path) or {}
    return _name_stem(data.get("flowfile_name") or reg.name)


def _namespace_path(db: Session, namespace_id: int | None) -> dict | None:
    """Resolve a flow's namespace_id to portable ``{"catalog", "schema"}`` names.

    Flows live under a schema (level 1); returns the parent catalog + schema name. A level-0
    row (or a missing parent) yields ``schema: None``. ``None`` when the flow has no namespace.
    """
    if namespace_id is None:
        return None
    ns = db.get(CatalogNamespace, namespace_id)
    if ns is None:
        return None
    if ns.parent_id is None:
        return {"catalog": ns.name, "schema": None}
    parent = db.get(CatalogNamespace, ns.parent_id)
    return {"catalog": parent.name if parent else ns.name, "schema": ns.name}


def project_flow(root: Path, reg: FlowRegistration, namespace: dict | None = None) -> Path | None:
    data = _load_flow_data(reg.flow_path)
    if data is None:
        return None
    stem = _name_stem(data.get("flowfile_name") or reg.name)
    target = manifest.flows_dir(root) / f"{stem}.flow.yaml"
    if target.exists():
        existing = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
        if existing.get("flow_uuid") not in (None, reg.flow_uuid):
            target = manifest.flows_dir(root) / f"{stem}_{reg.flow_uuid[:8]}.flow.yaml"
    write_yaml(target, normalize_flow_data(data, reg.flow_uuid, reg.name, namespace))
    return target


def remove_flow(root: Path, flow_uuid: str | None = None, name: str | None = None) -> None:
    """Remove a flow's projected file and its schedule file.

    Matches by ``flow_uuid`` (filename-independent — the projected name is derived from the flow's
    intrinsic ``flowfile_name``, which the caller may no longer have after a delete); falls back to a
    name-stem glob when only a name is known.
    """
    flows_dir = manifest.flows_dir(root)
    if flow_uuid:
        for p in flows_dir.glob("*.flow.yaml"):
            try:
                data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
            except Exception:
                continue
            if data.get("flow_uuid") == flow_uuid:
                p.unlink(missing_ok=True)
                manifest.schedules_dir(root).joinpath(f"{p.name[: -len('.flow.yaml')]}.yaml").unlink(missing_ok=True)
    elif name:
        stem = _name_stem(name)
        for p in flows_dir.glob(f"{stem}*.flow.yaml"):
            p.unlink(missing_ok=True)
            manifest.schedules_dir(root).joinpath(f"{p.name[: -len('.flow.yaml')]}.yaml").unlink(missing_ok=True)
        manifest.schedules_dir(root).joinpath(f"{stem}.yaml").unlink(missing_ok=True)


def remove_stale_flow_files(root: Path, flow_uuid: str, keep: Path) -> None:
    """After (re)projecting a flow, drop any other file carrying the same ``flow_uuid`` — e.g. an
    old name left behind by a rename — plus its now-orphaned schedule file."""
    for p in manifest.flows_dir(root).glob("*.flow.yaml"):
        if p == keep:
            continue
        try:
            data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        if data.get("flow_uuid") == flow_uuid:
            p.unlink(missing_ok=True)
            manifest.schedules_dir(root).joinpath(f"{p.name[: -len('.flow.yaml')]}.yaml").unlink(missing_ok=True)


# --- connections -------------------------------------------------------------


def _db_connection_dict(db: Session, conn: DatabaseConnection) -> dict:
    secret_name = _secret_name(db, conn.password_id)
    return {
        "kind": "database_connection",
        "connection_name": conn.connection_name,
        "database_type": conn.database_type,
        "host": conn.host,
        "port": conn.port,
        "database": conn.database,
        "username": conn.username,
        "ssl_enabled": conn.ssl_enabled,
        "password": make_placeholder(secret_name) if secret_name else None,
    }


def _cloud_connection_dict(db: Session, conn: CloudStorageConnection) -> dict:
    d = {
        "kind": "cloud_connection",
        "connection_name": conn.connection_name,
        "storage_type": conn.storage_type,
        "auth_method": conn.auth_method,
        "aws_region": conn.aws_region,
        "aws_access_key_id": conn.aws_access_key_id,
        "aws_role_arn": conn.aws_role_arn,
        "aws_allow_unsafe_html": conn.aws_allow_unsafe_html,
        "azure_account_name": conn.azure_account_name,
        "azure_tenant_id": conn.azure_tenant_id,
        "azure_client_id": conn.azure_client_id,
        "gcs_project_id": conn.gcs_project_id,
        "endpoint_url": conn.endpoint_url,
        "verify_ssl": conn.verify_ssl,
    }
    for field, fk in _CLOUD_SECRETS:
        name = _secret_name(db, getattr(conn, fk))
        d[field] = make_placeholder(name) if name else None
    return d


def project_database_connection(db: Session, root: Path, name: str, owner_id: int) -> Path | None:
    conn = (
        db.query(DatabaseConnection)
        .filter(DatabaseConnection.connection_name == name, DatabaseConnection.user_id == owner_id)
        .first()
    )
    if conn is None:
        return None
    target = manifest.connections_dir(root, "database") / f"{unique_stem(name)}.yaml"
    write_yaml(target, _db_connection_dict(db, conn))
    return target


def project_cloud_connection(db: Session, root: Path, name: str, owner_id: int) -> Path | None:
    conn = (
        db.query(CloudStorageConnection)
        .filter(CloudStorageConnection.connection_name == name, CloudStorageConnection.user_id == owner_id)
        .first()
    )
    if conn is None:
        return None
    target = manifest.connections_dir(root, "cloud") / f"{unique_stem(name)}.yaml"
    write_yaml(target, _cloud_connection_dict(db, conn))
    return target


def remove_connection(root: Path, kind: str, name: str) -> None:
    manifest.connections_dir(root, kind).joinpath(f"{unique_stem(name)}.yaml").unlink(missing_ok=True)


# --- schedules ---------------------------------------------------------------


def _schedule_dict(s: FlowSchedule) -> dict:
    return {
        "name": s.name,
        "enabled": s.enabled,
        "schedule_type": s.schedule_type,
        "interval_seconds": s.interval_seconds,
        "cron_expression": s.cron_expression,
        "cron_timezone": s.cron_timezone,
        "description": s.description,
    }


def project_schedules_for_registration(
    db: Session, root: Path, reg: FlowRegistration, stem: str | None = None
) -> Path | None:
    schedules = (
        db.query(FlowSchedule)
        .filter(
            FlowSchedule.registration_id == reg.id,
            FlowSchedule.schedule_type.in_(_PROJECTABLE_SCHEDULE_TYPES),
        )
        .order_by(FlowSchedule.id.asc())
        .all()
    )
    # Share the flow's file stem so a flow and its schedule always line up (caller passes the
    # resolved stem in full projection; the single-schedule hook resolves it from the projected file).
    target = manifest.schedules_dir(root) / f"{stem or _flow_stem(root, reg)}.yaml"
    if not schedules:
        target.unlink(missing_ok=True)
        return None
    write_yaml(target, {"flow_uuid": reg.flow_uuid, "schedules": [_schedule_dict(s) for s in schedules]})
    return target


# --- secret manifest (standalone secrets only) -------------------------------


def regenerate_secret_manifest(db: Session, root: Path, owner_id: int) -> None:
    """secrets.yaml lists only standalone secrets; connection secrets are implied by
    the connection files (and recreated by their store functions on import)."""
    linked: set[int] = set()
    for conn in db.query(DatabaseConnection).filter(DatabaseConnection.user_id == owner_id):
        if conn.password_id:
            linked.add(conn.password_id)
    for conn in db.query(CloudStorageConnection).filter(CloudStorageConnection.user_id == owner_id):
        for fk in _CLOUD_SECRET_FK_COLUMNS:
            sid = getattr(conn, fk)
            if sid:
                linked.add(sid)
    standalone = sorted({s.name for s in db.query(Secret).filter(Secret.user_id == owner_id) if s.id not in linked})
    write_yaml(manifest.secrets_manifest_path(root), {"required_secrets": standalone})


# --- namespace manifest (custom, non-public catalogs/schemas) ----------------


def _namespace_entry(name_key: str, name: str, description: str | None, is_public: bool) -> dict:
    """A manifest row: ``name_key`` ("catalog" or "name"), then optional description/is_public so
    custom namespaces stay clutter-free (those keys only appear when meaningful)."""
    entry = {name_key: name}
    if description is not None:
        entry["description"] = description
    if is_public:
        entry["is_public"] = True
    return entry


def regenerate_namespace_manifest(db: Session, root: Path, owner_id: int) -> None:
    """namespaces.yaml mirrors the owner's catalog tree by name — seeded system namespaces
    (General + default/Unnamed Flows/Local Flows/...) and custom catalogs/schemas alike — as a
    nested ``catalog -> schemas`` structure so the hierarchy is visible and versioned.

    ``is_public`` marks the seeder-managed system namespaces: the import path resolves those to
    existing rows (never recreating them as private duplicates) and the prune path never deletes
    them. Custom (non-public) namespaces are the ones import recreates and restore/reload prune."""
    rows = db.query(CatalogNamespace).filter(CatalogNamespace.owner_id == owner_id).all()
    catalogs: dict[str, dict] = {}
    schemas: dict[str, list[dict]] = {}
    for ns in rows:
        if ns.parent_id is None:
            catalogs[ns.name] = _namespace_entry("catalog", ns.name, ns.description, bool(ns.is_public))
    for ns in rows:
        if ns.parent_id is not None:
            parent = db.get(CatalogNamespace, ns.parent_id)
            catalog_name = parent.name if parent else ns.name
            if catalog_name not in catalogs:  # parent owned by another user (e.g. shared public General)
                # Emit only the name for cross-owner parents to avoid writing another user's
                # description into the project's git history.
                catalogs[catalog_name] = _namespace_entry(
                    "catalog", catalog_name, None, bool(parent and parent.is_public)
                )
            schemas.setdefault(catalog_name, []).append(
                _namespace_entry("name", ns.name, ns.description, bool(ns.is_public))
            )
    out: list[dict] = []
    for catalog_name in sorted(catalogs):
        entry = dict(catalogs[catalog_name])
        entry["schemas"] = sorted(schemas.get(catalog_name, []), key=lambda s: s["name"])
        out.append(entry)
    write_yaml(manifest.namespaces_manifest_path(root), {"namespaces": out})


# --- catalog tables ----------------------------------------------------------


def _table_pointer(table: CatalogTable) -> dict:
    """Portable pointer to a physical table's data. Managed tables store only the dir basename
    (rebuilt under catalog_tables_directory on import); external paths are stored verbatim and
    round-trip on the same machine only. The data itself is never committed (catalog_tables/ is
    gitignored)."""
    if table.file_path and _is_managed_table_path(table.file_path):
        return {"type": "managed", "name": Path(table.file_path).name}
    if table.file_path:
        return {"type": "external", "path": table.file_path}
    return {"type": "none"}


def _table_entry(db: Session, table: CatalogTable) -> dict:
    entry: dict = {
        "name": table.name,
        "namespace": _namespace_path(db, table.namespace_id),
        "table_type": table.table_type,
        "storage_format": table.storage_format,
    }
    if table.description is not None:
        entry["description"] = table.description
    schema = json.loads(table.schema_json) if table.schema_json else []
    entry["schema"] = [{"name": c["name"], "dtype": c["dtype"]} for c in schema]
    if table.partition_columns:
        entry["partition_columns"] = json.loads(table.partition_columns)
    if table.table_type == "virtual":
        entry["sql_query"] = table.sql_query
    else:
        entry["pointer"] = _table_pointer(table)
    if table.source_registration_id:
        reg = db.get(FlowRegistration, table.source_registration_id)
        if reg is not None:
            entry["source_flow_uuid"] = reg.flow_uuid
    return entry


def regenerate_tables_manifest(db: Session, root: Path, owner_id: int) -> None:
    """tables.yaml mirrors the owner's physical tables and SQL-view virtual tables as
    definition + schema + a portable data pointer. Flow-produced virtual tables are skipped
    (their serialized plan isn't portable; re-running the producer flow rebuilds them). Volatile
    data stats (row/size counts, run id, timestamps) are stripped so a pure data write doesn't
    churn the file."""
    rows = db.query(CatalogTable).filter(CatalogTable.owner_id == owner_id).all()
    entries = [_table_entry(db, t) for t in rows if not (t.table_type == "virtual" and not t.sql_query)]
    entries.sort(key=lambda e: (e["name"], json.dumps(e.get("namespace"), sort_keys=True)))
    write_yaml(manifest.tables_manifest_path(root), {"tables": entries})


# --- models (global artifacts) -----------------------------------------------


def _artifact_entry(db: Session, a: GlobalArtifact) -> dict:
    entry: dict = {
        "name": a.name,
        "namespace": _namespace_path(db, a.namespace_id),
        "version": a.version,
        "serialization_format": a.serialization_format,
    }
    reg = db.get(FlowRegistration, a.source_registration_id)
    if reg is not None:
        entry["source_flow_uuid"] = reg.flow_uuid
    for key in ("python_type", "python_module", "description"):
        value = getattr(a, key)
        if value is not None:
            entry[key] = value
    tags = json.loads(a.tags) if a.tags else []
    if tags:
        entry["tags"] = tags
    return entry


def regenerate_models_manifest(db: Session, root: Path, owner_id: int) -> None:
    """models.yaml mirrors the owner's active global artifacts as definition + lineage. The blob
    lives outside the project (global_artifacts_directory) and is never committed; on a fresh clone
    the row reappears and the blob refills when its producing flow re-runs."""
    rows = db.query(GlobalArtifact).filter(GlobalArtifact.owner_id == owner_id, GlobalArtifact.status == "active").all()
    entries = [_artifact_entry(db, a) for a in rows]
    entries.sort(key=lambda e: (e["name"], json.dumps(e.get("namespace"), sort_keys=True), e["version"]))
    write_yaml(manifest.models_manifest_path(root), {"models": entries})


# --- kernels -----------------------------------------------------------------


def _kernel_entry(kernel: Kernel) -> dict:
    """A kernel's portable definition: stable ``id`` (the key, round-trips verbatim), display name,
    image flavour, requested packages, and resource limits. Bake-time details (resolved_packages,
    the resolved image tag) are intentionally omitted — they are host-specific and would churn the
    file on a rebake."""
    entry: dict = {
        "id": kernel.id,
        "name": kernel.name,
        "image_flavour": kernel.image_flavour,
        "packages": sorted(json.loads(kernel.packages) if kernel.packages else []),
        "cpu_cores": kernel.cpu_cores,
        "memory_gb": kernel.memory_gb,
        "gpu": kernel.gpu,
    }
    if kernel.custom_image:
        entry["custom_image"] = kernel.custom_image
    return entry


def regenerate_kernels_manifest(db: Session, root: Path, owner_id: int) -> None:
    """kernels.yaml mirrors the owner's kernel definitions so the compute setup round-trips with the
    project. Only the definition is recorded — import recreates the config row; the container is
    still started on demand (no auto-bake, and Docker is never required to project or import)."""
    rows = db.query(Kernel).filter(Kernel.user_id == owner_id).all()
    entries = [_kernel_entry(k) for k in rows]
    entries.sort(key=lambda e: (e["name"], e["id"]))
    write_yaml(manifest.kernels_manifest_path(root), {"kernels": entries})


# --- visualizations & dashboards ---------------------------------------------


def _table_ref(db: Session, table_id: int | None) -> dict | None:
    """Portable ``{catalog, schema, name}`` reference to a catalog table (``None`` when absent)."""
    if table_id is None:
        return None
    table = db.get(CatalogTable, table_id)
    if table is None:
        return None
    ns = _namespace_path(db, table.namespace_id) or {"catalog": None, "schema": None}
    return {**ns, "name": table.name}


def _viz_uuid_for_id(db: Session, viz_id: int | None) -> str | None:
    if viz_id is None:
        return None
    viz = db.get(CatalogVisualization, viz_id)
    return viz.viz_uuid if viz is not None else None


def _visualization_entry(db: Session, viz: CatalogVisualization) -> dict:
    entry: dict = {
        "viz_uuid": viz.viz_uuid,
        "name": viz.name,
        "namespace": _namespace_path(db, viz.namespace_id),
        "source_type": viz.source_type or "table",
        "spec": json.loads(viz.spec_json) if viz.spec_json else [],
    }
    if (viz.source_type or "table") == "sql":
        entry["sql_query"] = viz.sql_query
    else:
        entry["source_table"] = _table_ref(db, viz.catalog_table_id)
    for key in ("description", "chart_type", "spec_gw_version"):
        value = getattr(viz, key)
        if value is not None:
            entry[key] = value
    return entry


def regenerate_visualizations_manifest(db: Session, root: Path, owner_id: int) -> None:
    """visualizations.yaml mirrors the owner's saved charts keyed by stable ``viz_uuid``: the
    GraphicWalker spec plus a portable source (``{catalog, schema}`` namespace + source table by
    name, or inline SQL). The client-side PNG thumbnail is never committed — it re-exports on the
    next view, like table data and model blobs are never committed."""
    rows = db.query(CatalogVisualization).filter(CatalogVisualization.created_by == owner_id).all()
    entries = [_visualization_entry(db, v) for v in rows]
    entries.sort(key=lambda e: (e["name"], e["viz_uuid"]))
    write_yaml(manifest.visualizations_manifest_path(root), {"visualizations": entries})


def _portable_layout(db: Session, layout: dict) -> dict:
    """Rewrite a dashboard layout's machine-local references to portable handles so the canvas
    round-trips: each tile's ``viz_id`` → ``viz_uuid``, each filter's ``datasource_id`` →
    ``{catalog, schema, name}``. A reference whose target is gone resolves to ``None`` (the tile
    surfaces a placeholder at view time — the same decoupled behaviour as a deleted viz)."""
    out = copy.deepcopy(layout)
    for tile in out.get("tiles") or []:
        if "viz_id" in tile:
            tile["viz_uuid"] = _viz_uuid_for_id(db, tile.pop("viz_id"))
    for flt in out.get("filters") or []:
        if "datasource_id" in flt:
            flt["datasource"] = _table_ref(db, flt.pop("datasource_id"))
    return out


def _dashboard_entry(db: Session, dashboard: CatalogDashboard) -> dict:
    layout = json.loads(dashboard.layout_json) if dashboard.layout_json else {}
    entry: dict = {
        "dashboard_uuid": dashboard.dashboard_uuid,
        "name": dashboard.name,
        "namespace": _namespace_path(db, dashboard.namespace_id),
        "layout": _portable_layout(db, layout),
    }
    if dashboard.description is not None:
        entry["description"] = dashboard.description
    return entry


def regenerate_dashboards_manifest(db: Session, root: Path, owner_id: int) -> None:
    """dashboards.yaml mirrors the owner's dashboards keyed by stable ``dashboard_uuid``. Tiles
    reference their visualization by portable ``viz_uuid`` and filters their datasource by portable
    table name, so the canvas re-links to local ids on another machine."""
    rows = db.query(CatalogDashboard).filter(CatalogDashboard.created_by == owner_id).all()
    entries = [_dashboard_entry(db, d) for d in rows]
    entries.sort(key=lambda e: (e["name"], e["dashboard_uuid"]))
    write_yaml(manifest.dashboards_manifest_path(root), {"dashboards": entries})


# --- full projection ---------------------------------------------------------

# Project dirs that mirror the DB; everything in them is pruned to the written set on full projection.
_MIRRORED_DIRS = ("flows", "connections/database", "connections/cloud", "schedules")


def _prune_orphan_files(root: Path, written: set[Path]) -> None:
    """Delete project files with no matching DB resource so the folder is an exact mirror.

    Only touches ``.yaml`` files under the mirrored dirs; ``project.yaml``, ``.gitignore`` and
    ``secrets.yaml`` live at the root and are never swept.
    """
    for rel in _MIRRORED_DIRS:
        d = root / rel
        if not d.exists():
            continue
        for p in d.glob("*.yaml"):
            if p not in written:
                p.unlink(missing_ok=True)


def project_all(db: Session, root: Path, owner_id: int) -> None:
    written: set[Path] = set()
    # Order by flow_uuid so the bare-vs-suffixed filename for same-named flows is stable across runs.
    for reg in (
        db.query(FlowRegistration)
        .filter(FlowRegistration.owner_id == owner_id)
        .order_by(FlowRegistration.flow_uuid.asc())
    ):
        flow_path = project_flow(root, reg, _namespace_path(db, reg.namespace_id))
        if flow_path is not None:
            written.add(flow_path)
        stem = flow_path.name[: -len(".flow.yaml")] if flow_path is not None else None
        sched_path = project_schedules_for_registration(db, root, reg, stem)
        if sched_path is not None:
            written.add(sched_path)
    for conn in db.query(DatabaseConnection).filter(DatabaseConnection.user_id == owner_id):
        written.add(project_database_connection(db, root, conn.connection_name, owner_id))
    for conn in db.query(CloudStorageConnection).filter(CloudStorageConnection.user_id == owner_id):
        written.add(project_cloud_connection(db, root, conn.connection_name, owner_id))
    regenerate_secret_manifest(db, root, owner_id)
    regenerate_namespace_manifest(db, root, owner_id)
    # Catalog tables + global artifacts are opt-out per project (manifest.track_data_artifacts).
    # When off, drop the root manifests (they aren't swept by _prune_orphan_files).
    m = manifest.read_manifest(root)
    if m is None or m.track_data_artifacts:
        regenerate_tables_manifest(db, root, owner_id)
        regenerate_models_manifest(db, root, owner_id)
    else:
        manifest.tables_manifest_path(root).unlink(missing_ok=True)
        manifest.models_manifest_path(root).unlink(missing_ok=True)
    regenerate_kernels_manifest(db, root, owner_id)
    regenerate_visualizations_manifest(db, root, owner_id)
    regenerate_dashboards_manifest(db, root, owner_id)
    _prune_orphan_files(root, written - {None})
