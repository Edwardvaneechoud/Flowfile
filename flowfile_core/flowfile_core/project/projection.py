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
from sqlalchemy import select
from sqlalchemy.orm import Session

from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.services import notebook_store
from flowfile_core.catalog.services.tables import _is_managed_table_path
from flowfile_core.database.models import (
    CatalogDashboard,
    CatalogNamespace,
    CatalogNotebook,
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
from flowfile_core.project.normalize import (
    dump_yaml,
    normalize_flow_data,
    safe_stem,
    unique_stem,
)
from flowfile_core.project.secrets_resolver import make_placeholder
from shared.storage_config import may_touch_flow_file

logger = logging.getLogger(__name__)

# (file field, model FK column) for cloud secrets that round-trip through the store fns.
_CLOUD_SECRETS = [
    ("aws_secret_access_key", "aws_secret_access_key_id"),
    ("aws_session_token", "aws_session_token_id"),
    ("azure_account_key", "azure_account_key_id"),
    ("azure_client_secret", "azure_client_secret_id"),
    ("azure_sas_token", "azure_sas_token_id"),
    ("gcs_service_account_key", "gcs_service_account_key_id"),
]
_PROJECTABLE_SCHEDULE_TYPES = ("interval", "cron")

# Non-secret cloud-connection fields that round-trip verbatim (projection reads them off the ORM
# row, the importer passes them straight into FullCloudStorageConnection).
_CLOUD_PLAIN_FIELDS = (
    "aws_region",
    "aws_access_key_id",
    "aws_role_arn",
    "aws_allow_unsafe_html",
    "aws_profile",
    "azure_account_name",
    "azure_tenant_id",
    "azure_client_id",
    "gcs_project_id",
    "endpoint_url",
    "verify_ssl",
)
# Non-secret database-connection fields that round-trip verbatim.
_DB_PLAIN_FIELDS = ("database_type", "host", "port", "database", "username", "ssl_enabled")


def _secret_name(db: Session, secret_id: int | None) -> str | None:
    if not secret_id:
        return None
    row = db.query(Secret).filter(Secret.id == secret_id).first()
    return row.name if row else None


# --- flows -------------------------------------------------------------------


def exportable(flow_path: str | None) -> bool:
    """Whether a registration's flow file can be projected into the project.

    The export and the importer's prune share this one condition: a registration the export skips
    (no file on disk, a ``.flowfile`` pickle, a kernel scratch flow's ``<kernel:id>`` pseudo-path, or
    on a multi-user server a file outside the server's flow folders, which the project never reads
    from) is never in the files, so a restore or reload must leave it alone rather than read it as
    removed."""
    if not flow_path:
        return False
    p = Path(flow_path)
    return p.suffix.lower() in (".yaml", ".yml", ".json") and may_touch_flow_file(p) and p.exists()


YAML_LOADER = getattr(yaml, "CSafeLoader", yaml.SafeLoader)


def _load_flow_data(flow_path: str) -> dict | None:
    if not exportable(flow_path):
        return None
    p = Path(flow_path)
    text = p.read_text(encoding="utf-8")
    return json.loads(text) if p.suffix.lower() == ".json" else yaml.load(text, Loader=YAML_LOADER)


def _name_stem(name: str | None) -> str:
    """Clean file stem for a flow name. Strips a trailing ``.flow`` so a flow saved to
    ``x.flow.yaml`` (name becomes ``x.flow``) doesn't project to ``x.flow.flow.yaml``."""
    name = name or ""
    if name.endswith(".flow"):
        name = name[: -len(".flow")]
    return safe_stem(name)


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


def _reference_lookup(db: Session):
    """The names a flow file's node references resolve to on this install, for ``normalize_flow_data``.

    A table resolves as the catalog reader resolves it at run time (its id, else its name in its namespace) and is
    named ``catalog.schema.table``, which resolves without ambiguity; a namespace as the writer and models resolve
    it (a stored full name that resolves, else the id's); a flow by its ``flow_uuid``, namespace and name, unless
    the reference already holds a uuid, which the run time reads before the id. ``None`` when nothing resolves.
    """
    repo = SQLAlchemyCatalogRepository(db)
    svc = CatalogService(repo)

    def lookup(kind: str, settings: dict) -> dict | None:
        if kind == "namespace":
            stored = settings.get("namespace_full_name")
            if stored and svc.resolve_namespace_id_by_full_name(stored) is not None:
                return {"namespace_full_name": stored}
            full_name = svc.resolve_namespace_full_name(settings.get("namespace_id"))
            return {"namespace_full_name": full_name} if full_name else None
        if kind == "flow":
            if settings.get("flow_uuid"):
                return None
            reg = repo.get_flow(settings["registration_id"])
            if reg is None:
                return None
            return {
                "flow_uuid": reg.flow_uuid,
                "namespace": svc.resolve_namespace_full_name(reg.namespace_id),
                "name": reg.name,
            }
        table = repo.get_table(settings["catalog_table_id"]) if settings.get("catalog_table_id") else None
        reference = settings.get("catalog_full_table_name") or settings.get("catalog_table_name")
        if table is None and not settings.get("catalog_table_id") and reference:
            try:
                table = svc.resolve_table(reference, default_namespace_id=settings.get("catalog_namespace_id"))
            except Exception:
                table = None
        if table is None:
            return None
        namespace = svc.resolve_namespace_full_name(table.namespace_id)
        return {"catalog_full_table_name": f"{namespace}.{table.name}" if namespace else table.name}

    return lookup


# flow_path -> (owner_id, stamp, [(kind, settings, answer)], (stem, text))
_flow_texts: dict[str, tuple] = {}


def flow_file(db: Session, reg: FlowRegistration, namespace: dict | None = None) -> tuple[str, str] | None:
    """The stem a flow's file is named after and the file's text, or ``None`` when it is not exportable.

    The result is cached per flow path, keyed on the file's mtime and size, the registration's uuid, name and
    namespace, and the answers the reference lookup gave while rendering. A hit asks those lookups again (database
    reads only), so a renamed table or namespace re-renders the flow while an unchanged flow is never parsed."""
    if not exportable(reg.flow_path):
        return None
    st = Path(reg.flow_path).stat()
    stamp = (st.st_mtime_ns, st.st_size, reg.flow_uuid, reg.name, namespace)
    lookup = _reference_lookup(db)
    cached = _flow_texts.get(reg.flow_path)
    if cached is not None and cached[1] == stamp and all(lookup(k, s) == a for k, s, a in cached[2]):
        return cached[3]
    asked: list[tuple] = []

    def recording(kind: str, settings: dict) -> dict | None:
        answer = lookup(kind, settings)
        asked.append((kind, copy.deepcopy(settings), answer))
        return answer

    data = _load_flow_data(reg.flow_path)
    if data is None:
        return None
    stem = _name_stem(data.get("flowfile_name") or reg.name)
    result = stem, dump_yaml(normalize_flow_data(data, reg.flow_uuid, reg.name, namespace, lookup=recording))
    _flow_texts[reg.flow_path] = (reg.owner_id, stamp, asked, result)
    return result


def _owner_flows(db: Session, owner_id: int):
    return (
        db.query(FlowRegistration)
        .filter(FlowRegistration.owner_id == owner_id)
        .order_by(FlowRegistration.flow_uuid.asc())
    )


def export_flows(db: Session, owner_id: int) -> dict[str, tuple[str, str]]:
    """``{flow_uuid: (file stem, text)}`` of the owner's exportable flows, the stems assigned in memory.

    Flows are taken in ``flow_uuid`` order and a stem an earlier flow holds, compared without letter case
    (a case-insensitive filesystem holds one of the two), gets the flow's ``_<uuid8>`` suffix."""
    out: dict[str, tuple[str, str]] = {}
    taken: set[str] = set()
    seen: set[str] = set()
    namespaces: dict[int | None, dict | None] = {}
    for reg in _owner_flows(db, owner_id):
        if reg.namespace_id not in namespaces:
            namespaces[reg.namespace_id] = _namespace_path(db, reg.namespace_id)
        exported = flow_file(db, reg, namespaces[reg.namespace_id])
        if exported is None:
            continue
        stem, text = exported
        if stem.casefold() in taken:
            stem = f"{stem}_{reg.flow_uuid[:8]}"
        taken.add(stem.casefold())
        out[reg.flow_uuid] = (stem, text)
        seen.add(reg.flow_path)
    for path, cached in list(_flow_texts.items()):
        if cached[0] == owner_id and path not in seen:
            _flow_texts.pop(path, None)
    return out


# --- connections -------------------------------------------------------------


def _db_connection_dict(db: Session, conn: DatabaseConnection) -> dict:
    secret_name = _secret_name(db, conn.password_id)
    d = {"kind": "database_connection", "connection_name": conn.connection_name}
    for f in _DB_PLAIN_FIELDS:
        d[f] = getattr(conn, f)
    d["password"] = make_placeholder(secret_name) if secret_name else None
    return d


def _cloud_connection_dict(db: Session, conn: CloudStorageConnection) -> dict:
    d = {
        "kind": "cloud_connection",
        "connection_name": conn.connection_name,
        "storage_type": conn.storage_type,
        "auth_method": conn.auth_method,
    }
    for f in _CLOUD_PLAIN_FIELDS:
        d[f] = getattr(conn, f)
    for field, fk in _CLOUD_SECRETS:
        name = _secret_name(db, getattr(conn, fk))
        d[field] = make_placeholder(name) if name else None
    return d


def export_connections(db: Session, owner_id: int, kind: str) -> dict[str, str]:
    """``{path: text}`` of the owner's database (``kind="database"``) or cloud connections."""
    if kind == "database":
        rows = db.query(DatabaseConnection).filter(DatabaseConnection.user_id == owner_id)
        render = _db_connection_dict
    else:
        rows = db.query(CloudStorageConnection).filter(CloudStorageConnection.user_id == owner_id)
        render = _cloud_connection_dict
    return {
        f"connections/{kind}/{unique_stem(conn.connection_name)}.yaml": dump_yaml(render(db, conn)) for conn in rows
    }


# --- schedules ---------------------------------------------------------------


def _schedule_dict(s: FlowSchedule) -> dict:
    """A schedule as its project file holds it; ``backend`` only when one is set, so older files stay unchanged."""
    out = {
        "id": s.schedule_uuid,
        "name": s.name,
        "enabled": s.enabled,
        "schedule_type": s.schedule_type,
        "interval_seconds": s.interval_seconds,
        "cron_expression": s.cron_expression,
        "cron_timezone": s.cron_timezone,
        "description": s.description,
    }
    if s.backend:
        out["backend"] = s.backend
    return out


def _schedule_file(reg: FlowRegistration, schedules: list[FlowSchedule]) -> dict:
    return {"flow_uuid": reg.flow_uuid, "schedules": [_schedule_dict(s) for s in schedules]}


def export_schedules(db: Session, owner_id: int, stems: dict[str, str]) -> dict[str, str]:
    """``{path: text}`` of the owner's schedule files, each named after its flow's stem in ``stems``
    (a flow with no file after its name). Flows are taken in ``flow_uuid`` order and a flow with no
    schedule drops a file an earlier one wrote under the same stem, as a write over the folder did."""
    tree: dict[str, str] = {}
    by_flow: dict[int, list[FlowSchedule]] = {}
    for s in (
        db.query(FlowSchedule)
        .join(FlowRegistration, FlowSchedule.registration_id == FlowRegistration.id)
        .filter(FlowRegistration.owner_id == owner_id, FlowSchedule.schedule_type.in_(_PROJECTABLE_SCHEDULE_TYPES))
        .order_by(FlowSchedule.id.asc())
    ):
        by_flow.setdefault(s.registration_id, []).append(s)
    for reg in _owner_flows(db, owner_id):
        rel = f"schedules/{stems.get(reg.flow_uuid) or _name_stem(reg.name)}.yaml"
        schedules = by_flow.get(reg.id)
        if schedules:
            tree[rel] = dump_yaml(_schedule_file(reg, schedules))
        else:
            tree.pop(rel, None)
    return tree


# --- secret manifest (standalone secrets only) -------------------------------


def secret_manifest_data(db: Session, owner_id: int) -> dict:
    """secrets.yaml lists the owner's standalone secrets: those no row refers to through a foreign key
    to ``secrets.id`` and that no feature keeps under a name of its own. A connection's secrets are
    implied by its file (and recreated by its store function on import); a Kafka, Google Analytics or
    AI-provider key, OAuth settings and the GitHub publish token belong to features with no file."""
    from flowfile_core.configs.app_settings import _OAUTH_KEYS
    from flowfile_core.database.models import Base
    from flowfile_core.routes.community_github import GITHUB_PUBLISH_LOGIN_KEY, GITHUB_PUBLISH_TOKEN_KEY

    linked: set[int] = set()
    for table in Base.metadata.tables.values():
        for column in table.columns:
            if any(fk.column.table.name == Secret.__tablename__ for fk in column.foreign_keys):
                linked.update(sid for (sid,) in db.execute(select(column).where(column.isnot(None))))
    feature_names = {*_OAUTH_KEYS, GITHUB_PUBLISH_TOKEN_KEY, GITHUB_PUBLISH_LOGIN_KEY}
    q = db.query(Secret.name).filter(Secret.user_id == owner_id)
    if linked:
        q = q.filter(~Secret.id.in_(linked))
    return {"required_secrets": sorted({name for (name,) in q} - feature_names)}


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


def namespace_manifest_data(db: Session, owner_id: int) -> dict:
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
    return {"namespaces": out}


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
        # A producer outside the project (another owner's, a run snapshot) is never named by the files.
        if reg is not None and reg.owner_id == table.owner_id and exportable(reg.flow_path):
            entry["source_flow_uuid"] = reg.flow_uuid
    return entry


def tables_manifest_data(db: Session, owner_id: int) -> dict:
    """tables.yaml mirrors the owner's physical tables and SQL-view virtual tables as
    definition + schema + a portable data pointer. Flow-produced virtual tables are skipped
    (their serialized plan isn't portable; re-running the producer flow rebuilds them). Volatile
    data stats (row/size counts, run id, timestamps) are stripped so a pure data write doesn't
    churn the file."""
    rows = db.query(CatalogTable).filter(CatalogTable.owner_id == owner_id).all()
    entries = [_table_entry(db, t) for t in rows if not (t.table_type == "virtual" and not t.sql_query)]
    entries.sort(key=lambda e: (e["name"], json.dumps(e.get("namespace"), sort_keys=True)))
    return {"tables": entries}


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


def models_manifest_data(db: Session, owner_id: int) -> dict:
    """models.yaml mirrors the owner's active global artifacts as definition + lineage. The blob
    lives outside the project (global_artifacts_directory) and is never committed; on a fresh clone
    the row reappears and the blob refills when its producing flow re-runs."""
    rows = db.query(GlobalArtifact).filter(GlobalArtifact.owner_id == owner_id, GlobalArtifact.status == "active").all()
    entries = [_artifact_entry(db, a) for a in rows]
    entries.sort(key=lambda e: (e["name"], json.dumps(e.get("namespace"), sort_keys=True), e["version"]))
    return {"models": entries}


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


def kernels_manifest_data(db: Session, owner_id: int) -> dict:
    """kernels.yaml mirrors the owner's kernel definitions so the compute setup round-trips with the
    project. Only the definition is recorded — import recreates the config row; the container is
    still started on demand (no auto-bake, and Docker is never required to project or import)."""
    rows = db.query(Kernel).filter(Kernel.user_id == owner_id).all()
    entries = [_kernel_entry(k) for k in rows]
    entries.sort(key=lambda e: (e["name"], e["id"]))
    return {"kernels": entries}


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


def visualizations_manifest_data(db: Session, owner_id: int) -> dict:
    """visualizations.yaml mirrors the owner's saved charts keyed by stable ``viz_uuid``: the
    GraphicWalker spec plus a portable source (``{catalog, schema}`` namespace + source table by
    name, or inline SQL). The client-side PNG thumbnail is never committed — it re-exports on the
    next view, like table data and model blobs are never committed."""
    rows = db.query(CatalogVisualization).filter(CatalogVisualization.created_by == owner_id).all()
    entries = [_visualization_entry(db, v) for v in rows]
    entries.sort(key=lambda e: (e["name"], e["viz_uuid"]))
    return {"visualizations": entries}


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


def dashboards_manifest_data(db: Session, owner_id: int) -> dict:
    """dashboards.yaml mirrors the owner's dashboards keyed by stable ``dashboard_uuid``. Tiles
    reference their visualization by portable ``viz_uuid`` and filters their datasource by portable
    table name, so the canvas re-links to local ids on another machine."""
    rows = db.query(CatalogDashboard).filter(CatalogDashboard.created_by == owner_id).all()
    entries = [_dashboard_entry(db, d) for d in rows]
    entries.sort(key=lambda e: (e["name"], e["dashboard_uuid"]))
    return {"dashboards": entries}


# --- notebooks ---------------------------------------------------------------


def notebook_file_text(db: Session, nb: CatalogNotebook, owner_id: int) -> str:
    return notebook_store.serialize_notebook(
        notebook_uuid=nb.notebook_uuid,
        name=nb.name,
        description=nb.description,
        namespace=_namespace_path(db, nb.namespace_id),
        default_kernel_id=nb.default_kernel_id,
        cells=notebook_store.read_notebook_cells(owner_id, nb.notebook_uuid),
    )


def _owner_notebooks(db: Session, owner_id: int):
    return (
        db.query(CatalogNotebook)
        .filter(CatalogNotebook.owner_id == owner_id)
        .order_by(CatalogNotebook.notebook_uuid.asc())
    )


def export_notebooks(db: Session, owner_id: int) -> dict[str, str]:
    """``{path: text}`` of the owner's notebooks, named in ``notebook_uuid`` order as flows are."""
    tree: dict[str, str] = {}
    taken: set[str] = set()
    for nb in _owner_notebooks(db, owner_id):
        stem = safe_stem(nb.name)
        if stem.casefold() in taken:
            stem = f"{stem}_{nb.notebook_uuid[:8]}"
        taken.add(stem.casefold())
        tree[f"notebooks/{stem}.notebook.yaml"] = notebook_file_text(db, nb, owner_id)
    return tree
