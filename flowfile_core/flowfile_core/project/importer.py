"""files → DB. The single, idempotent import path.

Reused by project setup/open, restore, and external-change reload. Upserts by
resource name; missing secret values become empty placeholders so setup never
dead-ends (the user fills them in later).
"""

from __future__ import annotations

import copy
import json
import logging
import uuid
from pathlib import Path

import yaml
from pydantic import SecretStr

from flowfile_core import flow_file_handler
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.delta_utils import table_exists
from flowfile_core.catalog.exceptions import NamespaceNotEmptyError
from flowfile_core.database.connection import get_db_context
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
)
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    _get_own_cloud_connection,
    _get_own_database_connection,
    delete_cloud_connection,
    delete_database_connection,
    store_cloud_connection,
    store_database_connection,
    update_cloud_connection,
    update_database_connection,
)
from flowfile_core.project import manifest, repository
from flowfile_core.project.models import SetupResult
from flowfile_core.project.normalize import safe_stem, write_yaml
from flowfile_core.project.projection import _CLOUD_SECRETS, _PROJECTABLE_SCHEDULE_TYPES
from flowfile_core.project.secrets_resolver import load_dotenv, placeholder_name, resolve
from flowfile_core.schemas.catalog_schema import DashboardLayout
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_core.schemas.input_schema import FullDatabaseConnection
from flowfile_core.secret_manager.secret_manager import (
    SecretInput,
    decrypt_secret,
    get_encrypted_secret,
    store_secret,
)
from shared.storage_config import storage

logger = logging.getLogger(__name__)


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _resolve_secret_value(name: str, owner_id: int, dotenv: dict, result: SetupResult) -> str:
    """env/.env → existing stored secret → empty placeholder (recorded for the refill UI)."""
    value = resolve(name, dotenv)
    if value is not None:
        return value
    encrypted = get_encrypted_secret(owner_id, name)
    if encrypted is not None:
        return decrypt_secret(encrypted).get_secret_value()
    result.placeholder_secrets.append(name)
    return ""


def _import_standalone_secrets(root: Path, owner_id: int, dotenv: dict, result: SetupResult) -> None:
    for name in _read_yaml(manifest.secrets_manifest_path(root)).get("required_secrets", []) or []:
        if get_encrypted_secret(owner_id, name) is not None:
            continue
        value = resolve(name, dotenv)
        if value is None:
            result.placeholder_secrets.append(name)
            value = ""
        with get_db_context() as db:
            store_secret(db, SecretInput(name=name, value=SecretStr(value)), owner_id)


def _import_db_connection(data: dict, owner_id: int, dotenv: dict, result: SetupResult) -> None:
    name = data["connection_name"]
    secret_name = placeholder_name(data.get("password")) or name
    value = _resolve_secret_value(secret_name, owner_id, dotenv, result)
    conn = FullDatabaseConnection(
        connection_name=name,
        database_type=data.get("database_type", "postgresql"),
        username=data.get("username", ""),
        password=SecretStr(value),
        host=data.get("host"),
        port=data.get("port"),
        database=data.get("database"),
        ssl_enabled=data.get("ssl_enabled", False),
    )
    with get_db_context() as db:
        if _get_own_database_connection(db, name, owner_id):
            update_database_connection(db, conn, owner_id)
        else:
            store_database_connection(db, conn, owner_id)
    result.imported_connections += 1


def _import_cloud_connection(data: dict, owner_id: int, dotenv: dict, result: SetupResult) -> None:
    name = data["connection_name"]
    kwargs: dict = {
        "storage_type": data["storage_type"],
        "auth_method": data["auth_method"],
        "connection_name": name,
        "aws_region": data.get("aws_region"),
        "aws_access_key_id": data.get("aws_access_key_id"),
        "aws_role_arn": data.get("aws_role_arn"),
        "aws_allow_unsafe_html": data.get("aws_allow_unsafe_html"),
        "azure_account_name": data.get("azure_account_name"),
        "azure_tenant_id": data.get("azure_tenant_id"),
        "azure_client_id": data.get("azure_client_id"),
        "gcs_project_id": data.get("gcs_project_id"),
        "endpoint_url": data.get("endpoint_url"),
        "verify_ssl": data.get("verify_ssl", True),
    }
    for field_name, _ in _CLOUD_SECRETS:
        ph = placeholder_name(data.get(field_name))
        kwargs[field_name] = SecretStr(_resolve_secret_value(ph, owner_id, dotenv, result)) if ph else None
    conn = FullCloudStorageConnection(**kwargs)
    with get_db_context() as db:
        if _get_own_cloud_connection(db, name, owner_id):
            update_cloud_connection(db, conn, owner_id)
        else:
            store_cloud_connection(db, conn, owner_id)
    result.imported_connections += 1


def _create_namespace(
    service: CatalogService,
    name: str,
    owner_id: int,
    parent_id: int | None,
    description: str | None,
    is_public: bool,
) -> CatalogNamespace:
    """Create a namespace, preserving its ``is_public`` flag (so a recreated public namespace stays
    public and the YAML round-trip is byte-identical)."""
    ns = service.create_namespace(name=name, owner_id=owner_id, parent_id=parent_id, description=description)
    if is_public and not ns.is_public:
        ns.is_public = True
        service.repo.update_namespace(ns)
    return ns


def _resolve_namespace(
    catalog_name: str | None,
    schema_name: str | None,
    owner_id: int,
    description: str | None = None,
    create: bool = True,
    is_public: bool = False,
) -> int | None:
    """Resolve the catalog (level 0) then schema (level 1) by name; return the schema id (or the
    catalog id when no schema). Reuses existing rows by name (idempotent get-or-create), so the
    always-seeded system namespaces resolve in place — never duplicated. With ``create`` a missing
    row is created (preserving ``is_public``); without it (resolve-only callers like flows, which run
    after namespaces are imported) a missing row yields None."""
    if not catalog_name:
        return None
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        catalog = service.repo.get_namespace_by_name(catalog_name, None)
        if catalog is None:
            if not create:
                return None
            catalog = _create_namespace(
                service,
                catalog_name,
                owner_id,
                None,
                None if schema_name else description,
                is_public and not schema_name,
            )
        if not schema_name:
            return catalog.id
        schema = service.repo.get_namespace_by_name(schema_name, catalog.id)
        if schema is None:
            if not create:
                return None
            schema = _create_namespace(service, schema_name, owner_id, catalog.id, description, is_public)
        return schema.id


def _import_namespaces(root: Path, owner_id: int) -> set[int]:
    """Recreate the owner's namespaces from namespaces.yaml (nested ``catalog -> schemas``); return
    the set of ids that must survive a prune. Every namespace is get-or-created: the always-seeded
    system namespaces resolve in place, while project-specific ones (custom catalogs/schemas and
    demo-seeded ``is_public`` ones the seeder didn't create on this machine) are recreated, preserving
    ``is_public`` so flows/tables can attach to their real schema."""
    catalogs = _read_yaml(manifest.namespaces_manifest_path(root)).get("namespaces", []) or []
    kept: set[int] = set()
    for catalog in sorted(catalogs, key=lambda c: c.get("catalog") or ""):
        catalog_name = catalog.get("catalog")
        if not catalog_name:
            continue
        cat_id = _resolve_namespace(
            catalog_name, None, owner_id, catalog.get("description"), is_public=bool(catalog.get("is_public"))
        )
        if cat_id is not None:
            kept.add(cat_id)
        for schema in catalog.get("schemas") or []:
            schema_name = schema.get("name")
            if not schema_name:
                continue
            sch_id = _resolve_namespace(
                catalog_name, schema_name, owner_id, schema.get("description"), is_public=bool(schema.get("is_public"))
            )
            if sch_id is not None:
                kept.add(sch_id)
    return kept


def _resolve_entry_namespace(ns: dict, owner_id: int) -> int | None:
    """Resolve a tables/models manifest entry's portable ``{catalog, schema}`` to a namespace id.
    Resolve-only: namespaces are imported (and created) before tables/models run."""
    catalog = ns.get("catalog")
    return _resolve_namespace(catalog, ns.get("schema"), owner_id, create=False) if catalog else None


def _resolve_flow_uuid_to_reg_id(flow_uuid: str | None) -> int | None:
    """Resolve a portable source-flow uuid to this install's registration id (None when absent)."""
    if not flow_uuid:
        return None
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter(FlowRegistration.flow_uuid == flow_uuid).first()
        return reg.id if reg else None


def _resolve_table_path(pointer: dict) -> str | None:
    """Rebuild a physical table's data path from its portable pointer. A managed table re-points to
    its dir under catalog_tables_directory (present ⇒ real, absent ⇒ stub); external paths are used
    verbatim. ``None`` when there is no pointer."""
    if pointer.get("type") == "managed" and pointer.get("name"):
        return str(storage.catalog_tables_directory / pointer["name"])
    if pointer.get("type") == "external":
        return pointer.get("path")
    return None


def _materialize_empty_delta(path: str, schema: list[dict]) -> None:
    """Write a 0-row Delta table matching the stored schema, so a recovered table is real and
    queryable instead of a dangling stub. Reuses the canonical field→polars mapping
    (MinimalFieldInfo → FlowfileColumn → create_from_schema) and the shared LazyFrame Delta writer."""
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
    from flowfile_core.schemas.input_schema import MinimalFieldInfo
    from shared.delta_utils import write_delta

    columns = [
        FlowfileColumn.create_from_minimal_field_info(MinimalFieldInfo(name=c["name"], data_type=c["dtype"]))
        for c in schema
    ]
    empty = FlowDataEngine.create_from_schema(columns)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    write_delta(empty.data_frame, path, mode="overwrite")


def _import_tables(root: Path, owner_id: int) -> set[int]:
    """Recreate the owner's physical + SQL-view tables from tables.yaml; return the kept ids.

    A physical table re-points to its data when present on this machine; when the managed data is
    absent it is rebuilt as a real 0-row Delta table from the stored schema (so it is queryable and
    repopulates when its flow runs). Existing data is never overwritten. A SQL view is rebuilt from
    its stored query + schema without executing it."""
    kept: set[int] = set()
    for entry in _read_yaml(manifest.tables_manifest_path(root)).get("tables", []) or []:
        name = entry.get("name")
        if not name:
            continue
        ns_id = _resolve_entry_namespace(entry.get("namespace") or {}, owner_id)
        schema = entry.get("schema") or []
        pcs = entry.get("partition_columns") or []
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            existing = repo.get_table_by_name(name, ns_id)
            table = existing or CatalogTable(name=name, namespace_id=ns_id, owner_id=owner_id)
            table.namespace_id = ns_id
            table.description = entry.get("description")
            table.storage_format = entry.get("storage_format", "delta")
            table.schema_json = json.dumps(schema)
            table.column_count = len(schema)
            table.partition_columns = json.dumps(pcs) if pcs else None
            if entry.get("table_type") == "virtual":
                table.table_type = "virtual"
                table.file_path = None
                table.sql_query = entry.get("sql_query")
            else:
                table.table_type = "physical"
                pointer = entry.get("pointer") or {}
                resolved = _resolve_table_path(pointer)
                # Rebuild a real, empty Delta table from the stored schema when the managed data is
                # absent on this machine (never overwrite existing data; never write external paths).
                if resolved and pointer.get("type") == "managed" and schema and not table_exists(resolved):
                    try:
                        _materialize_empty_delta(resolved, schema)
                        table.row_count = 0
                        table.size_bytes = 0
                    except Exception:
                        logger.warning("Could not rebuild empty table %s from schema", name, exc_info=True)
                table.file_path = resolved
                table.source_registration_id = _resolve_flow_uuid_to_reg_id(entry.get("source_flow_uuid"))
            if existing is None:
                repo.create_table(table)
            else:
                repo.update_table(table)
            kept.add(table.id)
    return kept


def _import_artifacts(root: Path, owner_id: int) -> set[int]:
    """Recreate the owner's models (global artifacts) from models.yaml; return the kept ids.

    Upserts by (name, namespace, version) so reloads never bloat versions. The blob isn't restored
    (it lives outside the project) — a fresh row is ``active`` but its data refills when the producing
    flow re-runs. An artifact whose source flow isn't present is skipped (its FK can't be satisfied)."""
    kept: set[int] = set()
    for entry in _read_yaml(manifest.models_manifest_path(root)).get("models", []) or []:
        name, version = entry.get("name"), entry.get("version")
        if not name or version is None:
            continue
        src_reg_id = _resolve_flow_uuid_to_reg_id(entry.get("source_flow_uuid"))
        if src_reg_id is None:
            logger.info(
                "Project import: skipping model %s (source flow %s absent)", name, entry.get("source_flow_uuid")
            )
            continue
        ns_id = _resolve_entry_namespace(entry.get("namespace") or {}, owner_id)
        with get_db_context() as db:
            existing = db.query(GlobalArtifact).filter_by(name=name, namespace_id=ns_id, version=version).first()
            artifact = existing or GlobalArtifact(name=name, namespace_id=ns_id, version=version, owner_id=owner_id)
            artifact.source_registration_id = src_reg_id
            artifact.serialization_format = entry.get("serialization_format", "pickle")
            artifact.status = "active"
            artifact.python_type = entry.get("python_type")
            artifact.python_module = entry.get("python_module")
            artifact.description = entry.get("description")
            artifact.tags = json.dumps(entry.get("tags") or [])
            if existing is None:
                db.add(artifact)
            db.commit()
            db.refresh(artifact)
            kept.add(artifact.id)
    return kept


def _import_kernels(root: Path, owner_id: int) -> set[str]:
    """Recreate the owner's kernel definitions from kernels.yaml; return the kept ids.

    Upserts the Kernel config row by its stable id (only the definition — flavour, requested
    packages, resource limits). The derived image is not baked and no container is started: an
    imported kernel comes up STOPPED and is launched on demand, exactly like a freshly-created one.
    ``resolved_packages`` stays empty until the kernel is next baked on this machine."""
    kept: set[str] = set()
    for entry in _read_yaml(manifest.kernels_manifest_path(root)).get("kernels", []) or []:
        name = entry.get("name")
        if not name:
            continue
        kernel_id = entry.get("id") or str(uuid.uuid4())
        with get_db_context() as db:
            existing = db.query(Kernel).filter(Kernel.id == kernel_id).first()
            kernel = existing or Kernel(id=kernel_id, user_id=owner_id, resolved_packages="[]")
            kernel.name = name
            kernel.user_id = owner_id
            kernel.packages = json.dumps(entry.get("packages") or [])
            kernel.cpu_cores = entry.get("cpu_cores", 2.0)
            kernel.memory_gb = entry.get("memory_gb", 4.0)
            kernel.gpu = bool(entry.get("gpu", False))
            kernel.image_flavour = entry.get("image_flavour") or "base"
            kernel.custom_image = entry.get("custom_image")
            if existing is None:
                db.add(kernel)
            db.commit()
            kept.add(kernel_id)
    return kept


def _resolve_table_ref(ref: dict | None, owner_id: int) -> int | None:
    """Portable ``{catalog, schema, name}`` → this install's catalog_table_id (None when absent)."""
    if not ref or not ref.get("name"):
        return None
    ns_id = _resolve_namespace(ref.get("catalog"), ref.get("schema"), owner_id, create=False)
    with get_db_context() as db:
        table = SQLAlchemyCatalogRepository(db).get_table_by_name(ref["name"], ns_id)
        return table.id if table else None


def _resolve_viz_id_by_uuid(viz_uuid: str | None) -> int | None:
    if not viz_uuid:
        return None
    with get_db_context() as db:
        viz = db.query(CatalogVisualization).filter_by(viz_uuid=viz_uuid).first()
        return viz.id if viz else None


def _import_visualizations(root: Path, owner_id: int) -> set[str]:
    """Recreate the owner's saved charts from visualizations.yaml; return the kept viz_uuids.

    Upserts by viz_uuid. The source table is re-resolved by its portable ``{catalog, schema, name}``
    (tables.yaml rebuilds physical tables first, so it is normally present); a table-source viz whose
    table is still absent keeps its definition with a null source rather than being dropped. The
    PNG thumbnail isn't restored — it re-exports on the next view."""
    kept: set[str] = set()
    for entry in _read_yaml(manifest.visualizations_manifest_path(root)).get("visualizations", []) or []:
        name = entry.get("name")
        if not name:
            continue
        viz_uuid = entry.get("viz_uuid") or str(uuid.uuid4())
        ns_id = _resolve_entry_namespace(entry.get("namespace") or {}, owner_id)
        source_type = entry.get("source_type") or "table"
        table_id = _resolve_table_ref(entry.get("source_table"), owner_id) if source_type == "table" else None
        with get_db_context() as db:
            existing = db.query(CatalogVisualization).filter_by(viz_uuid=viz_uuid).first()
            viz = existing or CatalogVisualization(viz_uuid=viz_uuid, created_by=owner_id)
            viz.name = name
            viz.description = entry.get("description")
            viz.chart_type = entry.get("chart_type")
            viz.spec_json = json.dumps(entry.get("spec") or [])
            viz.spec_gw_version = entry.get("spec_gw_version")
            viz.source_type = source_type
            viz.catalog_table_id = table_id
            viz.sql_query = entry.get("sql_query") if source_type == "sql" else None
            viz.namespace_id = ns_id
            viz.created_by = owner_id
            if existing is None:
                db.add(viz)
            db.commit()
            kept.add(viz_uuid)
    return kept


def _localize_layout(layout: dict, owner_id: int) -> dict:
    """Inverse of projection._portable_layout: each tile's ``viz_uuid`` → local ``viz_id``, each
    filter's ``datasource`` → local ``datasource_id``. Unresolvable references become ``None`` (the
    tile renders a placeholder), matching the decoupled-tile design."""
    out = copy.deepcopy(layout)
    for tile in out.get("tiles") or []:
        if "viz_uuid" in tile:
            tile["viz_id"] = _resolve_viz_id_by_uuid(tile.pop("viz_uuid"))
    for flt in out.get("filters") or []:
        if "datasource" in flt:
            flt["datasource_id"] = _resolve_table_ref(flt.pop("datasource"), owner_id)
    return out


def _import_dashboards(root: Path, owner_id: int) -> set[str]:
    """Recreate the owner's dashboards from dashboards.yaml; return the kept dashboard_uuids.

    Runs after visualizations so each tile's ``viz_uuid`` resolves to a local viz id. Upserts by
    dashboard_uuid; the localized layout is re-validated through ``DashboardLayout`` so it is stored
    in the same canonical shape the app writes (keeping the round-trip byte-identical)."""
    kept: set[str] = set()
    for entry in _read_yaml(manifest.dashboards_manifest_path(root)).get("dashboards", []) or []:
        name = entry.get("name")
        if not name:
            continue
        dashboard_uuid = entry.get("dashboard_uuid") or str(uuid.uuid4())
        ns_id = _resolve_entry_namespace(entry.get("namespace") or {}, owner_id)
        layout = DashboardLayout.model_validate(_localize_layout(entry.get("layout") or {}, owner_id))
        with get_db_context() as db:
            existing = db.query(CatalogDashboard).filter_by(dashboard_uuid=dashboard_uuid).first()
            dashboard = existing or CatalogDashboard(dashboard_uuid=dashboard_uuid, created_by=owner_id)
            dashboard.name = name
            dashboard.description = entry.get("description")
            dashboard.namespace_id = ns_id
            dashboard.layout_json = layout.model_dump_json()
            dashboard.layout_version = layout.grid.version
            dashboard.created_by = owner_id
            if existing is None:
                db.add(dashboard)
            db.commit()
            kept.add(dashboard_uuid)
    return kept


def _import_flow(data: dict, owner_id: int) -> bool:
    flow_uuid = data.get("flow_uuid")
    if not flow_uuid:
        return False
    flowfile_name = data.get("flowfile_name") or "flow"
    catalog_name = data.get("catalog_name") or flowfile_name  # friendly display label; preserved, not the key
    ns = data.get("namespace") or {}
    # Resolve-only: every namespace a flow belongs to is either seeded or already created by
    # _import_namespaces (which runs first), so we never create one here.
    target_ns_id = (
        _resolve_namespace(ns.get("catalog"), ns.get("schema"), owner_id, create=False) if ns.get("catalog") else None
    )
    with get_db_context() as db:
        existing = db.query(FlowRegistration).filter(FlowRegistration.flow_uuid == flow_uuid).first()
        runtime_path = (
            Path(existing.flow_path)
            if existing
            else storage.flows_directory / "project" / f"{safe_stem(flowfile_name)}_{flow_uuid[:8]}.flow.yaml"
        )
    write_yaml(runtime_path, data)  # flow_uuid / catalog_name / namespace keys are ignored by the FlowfileData loader
    flow_file_handler.import_flow(runtime_path, user_id=owner_id)
    if existing is None:
        auto_register_flow(str(runtime_path), catalog_name, owner_id)
    # Reconcile identity + display name + namespace placement to the file (keeps flow_uuid stable for
    # determinism, the catalog name round-tripping, and the flow landing in its real schema on import).
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter(FlowRegistration.flow_path == str(runtime_path)).first()
        if reg:
            changed = (
                reg.flow_uuid != flow_uuid
                or reg.name != catalog_name
                or (target_ns_id is not None and reg.namespace_id != target_ns_id)
            )
            reg.flow_uuid = flow_uuid
            reg.name = catalog_name
            if target_ns_id is not None:
                reg.namespace_id = target_ns_id
            if changed:
                db.commit()
    return True


def _import_schedules(data: dict, owner_id: int) -> int:
    flow_uuid = data.get("flow_uuid")
    if not flow_uuid:
        return 0
    schedules = data.get("schedules") or []
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter(FlowRegistration.flow_uuid == flow_uuid).first()
        if reg is None:
            return 0
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        # The schedule file is the source of truth for this flow's interval/cron schedules, so
        # replace them wholesale. A name-based upsert duplicated nameless schedules on every
        # open/restore/reload (they can't be matched by name).
        for s in (
            db.query(FlowSchedule)
            .filter(
                FlowSchedule.registration_id == reg.id,
                FlowSchedule.schedule_type.in_(_PROJECTABLE_SCHEDULE_TYPES),
            )
            .all()
        ):
            service.delete_schedule(s.id)
        count = 0
        seen: set[tuple] = set()
        for sd in schedules:
            if sd.get("schedule_type") not in _PROJECTABLE_SCHEDULE_TYPES:
                continue
            sig = (
                sd.get("schedule_type"),
                sd.get("interval_seconds"),
                sd.get("cron_expression"),
                sd.get("cron_timezone"),
                sd.get("name"),
            )
            if sig in seen:  # collapse already-duplicated entries so existing dupes self-heal
                continue
            seen.add(sig)
            service.create_schedule(
                registration_id=reg.id,
                owner_id=owner_id,
                schedule_type=sd["schedule_type"],
                interval_seconds=sd.get("interval_seconds"),
                cron_expression=sd.get("cron_expression"),
                cron_timezone=sd.get("cron_timezone"),
                enabled=sd.get("enabled", True),
                name=sd.get("name"),
                description=sd.get("description"),
            )
            count += 1
    return count


def _prune_removed(
    owner_id: int,
    kept_flow_uuids: set[str],
    kept_db: set[str],
    kept_cloud: set[str],
    kept_namespace_ids: set[int],
    kept_table_ids: set[int] | None,
    kept_artifact_ids: set[int] | None,
    kept_kernel_ids: set[str],
    kept_viz_uuids: set[str],
    kept_dashboard_uuids: set[str],
) -> None:
    """Delete the owner's resources that are no longer present in the project files.

    A project mirrors the owner's whole environment, so on restore/reload the files are the complete
    intended set. Order matters: models and tables are pruned before flows (a flow can't be deleted
    while it still owns artifacts), and namespaces last (so emptied custom ones become deletable).
    Pruning never removes data — table data is kept (``delete_file=False``) and a model is soft-deleted
    so its blob survives. A ``None`` kept-set means that category isn't tracked by this project, so it
    is left entirely alone (never pruned). Best-effort per resource: failures are logged and skipped.
    """
    # Kernels are independent of the catalog graph, so order is free. Delete only the config row
    # (the running container, if any, is dropped by the manager reconcile that follows the import).
    from flowfile_core.kernel.persistence import delete_kernel

    with get_db_context() as db:
        for kernel in db.query(Kernel).filter(Kernel.user_id == owner_id).all():
            if kernel.id not in kept_kernel_ids:
                try:
                    delete_kernel(db, kernel.id)
                except Exception:
                    logger.warning("Project prune: could not remove kernel %s", kernel.id, exc_info=True)
    # Dashboards then visualizations (tiles reference viz by value, not FK, so order is free). The
    # GraphicWalker spec is the whole artifact — there is no separate blob to preserve.
    with get_db_context() as db:
        for dashboard in db.query(CatalogDashboard).filter(CatalogDashboard.created_by == owner_id).all():
            if dashboard.dashboard_uuid not in kept_dashboard_uuids:
                db.query(CatalogDashboard).filter_by(id=dashboard.id).delete()
        for viz in db.query(CatalogVisualization).filter(CatalogVisualization.created_by == owner_id).all():
            if viz.viz_uuid not in kept_viz_uuids:
                db.query(CatalogVisualization).filter_by(id=viz.id).delete()
        db.commit()
    # Models first (soft-delete, keep the blob): a kept flow's pruned artifact just goes inactive; a
    # pruned flow's are hard-deleted by delete_flow below. None = not tracked here, leave them all.
    if kept_artifact_ids is not None:
        with get_db_context() as db:
            for a in (
                db.query(GlobalArtifact)
                .filter(GlobalArtifact.owner_id == owner_id, GlobalArtifact.status == "active")
                .all()
            ):
                if a.id not in kept_artifact_ids:
                    a.status = "deleted"
            db.commit()
    # Tables before flows (keep the data); skip flow-virtual tables (not projected, not ours to prune).
    if kept_table_ids is not None:
        with get_db_context() as db:
            service = CatalogService(SQLAlchemyCatalogRepository(db))
            for table in db.query(CatalogTable).filter(CatalogTable.owner_id == owner_id).all():
                if table.id in kept_table_ids or (table.table_type == "virtual" and not table.sql_query):
                    continue
                try:
                    service.delete_table(table.id, delete_file=False)
                except Exception:
                    logger.warning("Project prune: could not remove table %s", table.name, exc_info=True)
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        for reg in db.query(FlowRegistration).filter(FlowRegistration.owner_id == owner_id).all():
            if reg.flow_uuid in kept_flow_uuids:
                continue
            try:
                for s in service.list_schedules(registration_id=reg.id):
                    service.delete_schedule(s.id)
                service.delete_flow(reg.id, delete_file=True)
            except Exception:
                logger.warning("Project prune: could not remove flow %s", reg.flow_uuid, exc_info=True)
    with get_db_context() as db:
        for conn in db.query(DatabaseConnection).filter(DatabaseConnection.user_id == owner_id).all():
            if conn.connection_name not in kept_db:
                try:
                    delete_database_connection(db, conn.connection_name, owner_id)
                except Exception:
                    logger.warning("Project prune: could not remove db conn %s", conn.connection_name, exc_info=True)
    with get_db_context() as db:
        for conn in db.query(CloudStorageConnection).filter(CloudStorageConnection.user_id == owner_id).all():
            if conn.connection_name not in kept_cloud:
                try:
                    delete_cloud_connection(db, conn.connection_name, owner_id)
                except Exception:
                    logger.warning("Project prune: could not remove cloud conn %s", conn.connection_name, exc_info=True)
    # Namespaces last: flows were pruned above, so an emptied custom schema is now deletable. Delete
    # schemas (level 1) before catalogs (level 0); seeded is_public namespaces are excluded entirely.
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        owned = (
            db.query(CatalogNamespace)
            .filter(CatalogNamespace.owner_id == owner_id, CatalogNamespace.is_public.is_(False))
            .all()
        )
        for ns in sorted(owned, key=lambda n: -(n.level or 0)):
            if ns.id in kept_namespace_ids:
                continue
            try:
                service.delete_namespace(ns.id)
            except NamespaceNotEmptyError:
                logger.info("Project prune: namespace %s still has tables/flows; skipping", ns.id)
            except Exception:
                logger.warning("Project prune: could not delete namespace %s", ns.id, exc_info=True)


def _reconcile_kernel_manager() -> None:
    """Best-effort: refresh an already-running kernel manager's in-memory registry after an import.

    Kernels are cached in the ``KernelManager`` singleton (unlike tables/models, read fresh per
    request), so an imported row needs the manager told about it to surface without a restart. This
    only touches a manager that already exists — it never forces Docker construction here (a manager
    built later loads the imported rows itself via ``_restore_kernels_from_db``). Failures are
    swallowed; the DB rows are authoritative regardless and appear on the next core start."""
    try:
        import flowfile_core.kernel as kernel_pkg

        manager = getattr(kernel_pkg, "_manager", None)
        if manager is not None:
            manager.reconcile_configs_from_db()
    except Exception:
        logger.debug("Kernel manager reconcile after import skipped", exc_info=True)


def import_project(root: Path, owner_id: int, prune: bool = False) -> SetupResult:
    """Rebuild this install's environment from the project folder. Always completes.

    Projection is suppressed for the whole rebuild: importing flows/tables/etc. would otherwise fire
    the DB→file hooks and regenerate the manifests from the half-built DB, wiping the very files being
    read. With ``prune=True`` (restore / reload) resources absent from the files are removed so the DB
    matches the files exactly; ``prune=False`` (open) is additive.
    """
    from flowfile_core.project import project_sync

    with project_sync.suppress_projection():
        return _do_import_project(root, owner_id, prune)


def _do_import_project(root: Path, owner_id: int, prune: bool = False) -> SetupResult:
    dotenv = load_dotenv(root)
    result = SetupResult()

    _import_standalone_secrets(root, owner_id, dotenv, result)

    kept_db: set[str] = set()
    for f in sorted(manifest.connections_dir(root, "database").glob("*.yaml")):
        data = _read_yaml(f)
        _import_db_connection(data, owner_id, dotenv, result)
        if data.get("connection_name"):
            kept_db.add(data["connection_name"])
    kept_cloud: set[str] = set()
    for f in sorted(manifest.connections_dir(root, "cloud").glob("*.yaml")):
        data = _read_yaml(f)
        _import_cloud_connection(data, owner_id, dotenv, result)
        if data.get("connection_name"):
            kept_cloud.add(data["connection_name"])

    kept_namespace_ids = _import_namespaces(root, owner_id)

    kept_flow_uuids: set[str] = set()
    for f in sorted(manifest.flows_dir(root).glob("*.flow.yaml")):
        data = _read_yaml(f)
        if _import_flow(data, owner_id):
            result.imported_flows += 1
        if data.get("flow_uuid"):
            kept_flow_uuids.add(data["flow_uuid"])

    for f in sorted(manifest.schedules_dir(root).glob("*.yaml")):
        result.imported_schedules += _import_schedules(_read_yaml(f), owner_id)

    # Tables + models after flows so their namespace and source-flow lineage resolve. When the
    # project opts out of tracking them, skip import and signal prune to leave them alone (None).
    m = manifest.read_manifest(root)
    track_data_artifacts = m is None or m.track_data_artifacts
    kept_table_ids = _import_tables(root, owner_id) if track_data_artifacts else None
    kept_artifact_ids = _import_artifacts(root, owner_id) if track_data_artifacts else None
    kept_kernel_ids = _import_kernels(root, owner_id)
    # Visualizations after tables (source re-links by name); dashboards after visualizations
    # (tiles re-link by viz_uuid).
    kept_viz_uuids = _import_visualizations(root, owner_id)
    kept_dashboard_uuids = _import_dashboards(root, owner_id)

    if prune:
        _prune_removed(
            owner_id,
            kept_flow_uuids,
            kept_db,
            kept_cloud,
            kept_namespace_ids,
            kept_table_ids,
            kept_artifact_ids,
            kept_kernel_ids,
            kept_viz_uuids,
            kept_dashboard_uuids,
        )

    _reconcile_kernel_manager()

    from flowfile_core.project import git_ops

    with get_db_context() as db:
        proj = repository.get_by_path(db, str(root))
        if proj is not None:
            repository.set_head_sha(db, proj.id, git_ops.head_sha(root))
    return result
