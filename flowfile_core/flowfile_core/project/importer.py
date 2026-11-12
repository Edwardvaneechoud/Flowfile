"""files → DB. The single, idempotent import path.

Reused by project setup/open, restore, and external-change reload. Upserts by
resource name; missing secret values become empty placeholders so setup never
dead-ends (the user fills them in later).
"""

from __future__ import annotations

import contextvars
import copy
import json
import logging
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path

import yaml
from pydantic import SecretStr, ValidationError
from sqlalchemy.exc import IntegrityError

from flowfile_core import flow_file_handler
from flowfile_core.auth import sharing
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.delta_utils import table_exists
from flowfile_core.catalog.exceptions import NamespaceExistsError, NamespaceNotEmptyError
from flowfile_core.catalog.services import notebook_store
from flowfile_core.catalog.services.tables import _is_managed_table_path
from flowfile_core.catalog.validators import validate_schedule_create
from flowfile_core.database.connection import get_db_context
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
)
from flowfile_core.fileExplorer.funcs import _is_contained
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
from flowfile_core.project import manifest, projection, repository
from flowfile_core.project.manifest_entries import CloudConnectionEntry, DatabaseConnectionEntry, SchemaColumn
from flowfile_core.project.models import KeptResources, SetupResult
from flowfile_core.project.normalize import safe_stem, write_yaml
from flowfile_core.project.projection import _CLOUD_PLAIN_FIELDS, _CLOUD_SECRETS, _PROJECTABLE_SCHEDULE_TYPES
from flowfile_core.project.secrets_resolver import load_dotenv, placeholder_name, resolve
from flowfile_core.schemas.catalog_schema import DashboardLayout, NotebookCellModel
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


class ImportTooLargeError(ValueError):
    """A project manifest exceeds a size/entry cap (router → 413). Subclasses ``ValueError`` so
    existing ``except ValueError`` paths keep working."""


class ProjectFileError(ValueError):
    """A project file cannot be read or does not have the shape its kind needs (router → 422). Raised
    by the preflight before anything is written; the message names the file by its repo-relative path
    and never repeats its content beyond the YAML parser's own short problem description."""


# Process-wide per-flow_uuid lock. The flows directory is shared across tenants in docker, so two
# concurrent imports (different project roots -> different repo_locks) of the SAME unregistered
# flow_uuid would both miss the existing/global probe and both compute+write the identical
# flows_directory/project/<stem>_<uuid8> path. Serializing the existing-check → path-resolve →
# write → register critical section by flow_uuid forces the second importer to re-probe UNDER the
# lock, see the first's registration, and mint a fresh uuid via the existing collision branch. The
# lock is keyed by flow_uuid only and acquired solely inside _import_flow (innermost; repo_lock is
# the outer lock when held), so it cannot introduce a lock-ordering cycle. Single-user/single-thread
# imports never contend, so behavior (and the byte-identical round-trip) is unchanged.
_flow_uuid_locks: dict[str, threading.Lock] = {}
_flow_uuid_locks_guard = threading.Lock()  # guards the dict itself, NOT held during the import


@contextmanager
def _flow_uuid_lock(flow_uuid: str):
    with _flow_uuid_locks_guard:
        lock = _flow_uuid_locks.get(flow_uuid)
        if lock is None:
            lock = threading.Lock()
            _flow_uuid_locks[flow_uuid] = lock
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


# Per-import memo for resolve-only namespace lookups. Namespaces are imported first and never change
# during the rest of an import, so the same (catalog, schema, owner) read repeated per table/model/
# flow/viz hits this map instead of a fresh session+query. Only create=False results are cached.
_ns_resolve_cache: contextvars.ContextVar[dict | None] = contextvars.ContextVar("_ns_resolve_cache", default=None)

# Import-size caps. Per-manifest-file YAML size cap (bytes); 5 MB is unreasonably large for a manifest.
_MAX_YAML_BYTES = 5 * 1024 * 1024  # 5 MB
# Per-import caps on individual resource kinds.
_MAX_FLOWS = 500
_MAX_TABLES = 500
_MAX_CONNECTIONS = 200
_MAX_NAMESPACES = 200
_MAX_ARTIFACTS = 500
_MAX_KERNELS = 50
_MAX_VISUALIZATIONS = 500
_MAX_DASHBOARDS = 200
_MAX_NOTEBOOKS = 500
_MAX_SCHEDULES_PER_FLOW = 50


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    size = path.stat().st_size
    if size > _MAX_YAML_BYTES:
        raise ImportTooLargeError(
            f"Project import: manifest file {path.name!r} is {size} bytes, "
            f"which exceeds the {_MAX_YAML_BYTES}-byte cap. Import aborted."
        )
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


def _import_db_connection(data: dict, owner_id: int, dotenv: dict, result: SetupResult) -> str | None:
    try:
        entry = DatabaseConnectionEntry.model_validate(data)
    except ValidationError:
        logger.warning("Project import: skipping malformed database connection file", exc_info=True)
        return None
    name = entry.connection_name
    secret_name = placeholder_name(entry.password) or name
    value = _resolve_secret_value(secret_name, owner_id, dotenv, result)
    conn = FullDatabaseConnection(
        connection_name=name,
        database_type=entry.database_type,
        username=entry.username,
        password=SecretStr(value),
        host=entry.host,
        port=entry.port,
        database=entry.database,
        ssl_enabled=entry.ssl_enabled,
    )
    with get_db_context() as db:
        if _get_own_database_connection(db, name, owner_id):
            update_database_connection(db, conn, owner_id)
        else:
            store_database_connection(db, conn, owner_id)
    result.imported_connections += 1
    return name


def _import_cloud_connection(data: dict, owner_id: int, dotenv: dict, result: SetupResult) -> str | None:
    try:
        entry = CloudConnectionEntry.model_validate(data)
    except ValidationError:
        logger.warning("Project import: skipping malformed cloud connection file", exc_info=True)
        return None
    name = entry.connection_name
    kwargs: dict = {
        "storage_type": entry.storage_type,
        "auth_method": entry.auth_method,
        "connection_name": name,
        "aws_region": entry.aws_region,
        "aws_access_key_id": entry.aws_access_key_id,
        "aws_role_arn": entry.aws_role_arn,
        "aws_allow_unsafe_html": entry.aws_allow_unsafe_html,
        "aws_profile": entry.aws_profile,
        "azure_account_name": entry.azure_account_name,
        "azure_tenant_id": entry.azure_tenant_id,
        "azure_client_id": entry.azure_client_id,
        "gcs_project_id": entry.gcs_project_id,
        "endpoint_url": entry.endpoint_url,
        "verify_ssl": entry.verify_ssl,
    }
    for field_name, _ in _CLOUD_SECRETS:
        ph = placeholder_name(getattr(entry, field_name))
        kwargs[field_name] = SecretStr(_resolve_secret_value(ph, owner_id, dotenv, result)) if ph else None
    conn = FullCloudStorageConnection(**kwargs)
    with get_db_context() as db:
        if _get_own_cloud_connection(db, name, owner_id):
            update_cloud_connection(db, conn, owner_id)
        else:
            store_cloud_connection(db, conn, owner_id)
    result.imported_connections += 1
    return name


def _create_namespace(
    service: CatalogService,
    name: str,
    owner_id: int,
    parent_id: int | None,
    description: str | None,
    is_public: bool,
) -> CatalogNamespace | None:
    """Create a namespace, preserving its ``is_public`` flag (so a recreated public namespace stays
    public and the YAML round-trip is byte-identical).

    Returns ``None`` on a cross-owner name collision: ``create_namespace`` checks the global
    ``uq_namespace_name_parent`` and raises ``NamespaceExistsError`` when a row of that (name,
    parent) is already owned by another user. We never overwrite or re-own that foreign row, so we
    skip+log and reuse the existing row's id for READ/placement only (returning that row), never
    mutating it."""
    try:
        ns = service.create_namespace(name=name, owner_id=owner_id, parent_id=parent_id, description=description)
    except NamespaceExistsError:
        existing = service.repo.get_namespace_by_name(name, parent_id)
        logger.warning(
            "Project import: namespace %r (parent=%s) already owned by another user; reusing its id "
            "for placement, not recreating/mutating it",
            name,
            parent_id,
        )
        return existing
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
    catalog id when no schema).

    Own-first, ELSE public, read-only: resolve the caller's own row first (owner-scoped). When the
    caller owns none, fall back to an existing ``is_public=True`` row of that name/parent (the
    seeded ``General``/``default``/... system namespaces, owned by ``local_user``) for placement
    ONLY — the imported flow/table attaches to its ``namespace_id`` but the public row is never
    mutated or recreated. Only when neither an own nor a public row exists and ``create`` is set is a
    fresh row created under the caller; resolve-only callers (flows/tables, ``create=False``) get
    None when nothing matches. A public row resolved for a parent is reused as the parent_id for a
    child schema lookup, but the child stays own-first-else-public on its own."""
    if not catalog_name:
        return None
    cache = _ns_resolve_cache.get() if not create else None
    cache_key = (catalog_name, schema_name, owner_id)
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    def _resolve() -> int | None:
        with get_db_context() as db:
            service = CatalogService(SQLAlchemyCatalogRepository(db))
            catalog = service.repo.get_namespace_by_name(catalog_name, None, owner_id=owner_id, include_public=True)
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
                if catalog is None:
                    return None
            if not schema_name:
                return catalog.id
            schema = service.repo.get_namespace_by_name(schema_name, catalog.id, owner_id=owner_id, include_public=True)
            if schema is None:
                if not create:
                    return None
                # Never create a child under a public parent the caller doesn't own (that would
                # collide on uq_namespace_name_parent against another user's tree).
                if catalog.owner_id != owner_id:
                    return None
                schema = _create_namespace(service, schema_name, owner_id, catalog.id, description, is_public)
                if schema is None:
                    return None
            return schema.id

    result = _resolve()
    if cache is not None:
        cache[cache_key] = result
    return result


def _import_namespaces(root: Path, owner_id: int) -> set[int]:
    """Recreate the owner's namespaces from namespaces.yaml (nested ``catalog -> schemas``); return
    the set of ids that must survive a prune. Every namespace is get-or-created: the always-seeded
    system namespaces resolve in place, while project-specific ones (custom catalogs/schemas and
    demo-seeded ``is_public`` ones the seeder didn't create on this machine) are recreated, preserving
    ``is_public`` so flows/tables can attach to their real schema."""
    catalogs = _read_yaml(manifest.namespaces_manifest_path(root)).get("namespaces", []) or []
    _assert_within_cap(catalogs, _MAX_NAMESPACES, "namespaces")
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


def _resolve_flow_uuid_to_reg_id(flow_uuid: str | None, owner_id: int) -> int | None:
    """Resolve a portable source-flow uuid to this install's registration id, scoped to the importing
    owner (None when absent or owned by another user — lineage stays unset rather than cross-linking)."""
    if not flow_uuid:
        return None
    with get_db_context() as db:
        reg = repository.owned_or_none(db, FlowRegistration, "owner_id", owner_id, flow_uuid=flow_uuid)
        return reg.id if reg else None


def _resolve_table_path(pointer: dict, root: Path) -> str | None:
    """Rebuild a physical table's data path from its portable pointer. A managed table re-points to
    its dir under catalog_tables_directory (the pointer name is reduced to a single basename and the
    result is asserted to stay under the managed dir — a traversal/absolute name is skip+logged so a
    crafted tables.yaml can never drive a write outside the managed dir). External paths are
    accepted verbatim only when they stay under the user's data roots / the project root, else
    skip+logged. ``None`` when there is no pointer."""
    if pointer.get("type") == "managed" and pointer.get("name"):
        resolved = storage.catalog_tables_directory / Path(pointer["name"]).name
        if not _is_managed_table_path(str(resolved)):
            logger.warning("Project import: managed table pointer %r escapes the managed dir; skipping", pointer)
            return None
        return str(resolved)
    if pointer.get("type") == "external":
        path = pointer.get("path")
        if path and _external_path_allowed(path, root):
            return path
        logger.warning("Project import: external table pointer %r escapes the allowed data roots; skipping", path)
        return None
    return None


def _external_path_allowed(path: str, root: Path) -> bool:
    """An external table path is only trusted when it stays under a user data root or the project
    root (a crafted external pointer with ``../`` / an absolute path is rejected)."""
    bases = [str(storage.user_data_directory), str(storage.flows_directory), str(storage.outputs_directory), str(root)]
    return any(_is_contained(base, path) for base in bases)


def _materialize_empty_delta(path: str, schema: list[dict]) -> None:
    """Write a 0-row Delta table matching the stored schema, so a recovered table is real and
    queryable instead of a dangling stub. Reuses the canonical field→polars mapping
    (MinimalFieldInfo → FlowfileColumn → create_from_schema) and the shared LazyFrame Delta writer."""
    from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
    from flowfile_core.schemas.input_schema import MinimalFieldInfo
    from shared.delta_utils import write_delta

    cols = [SchemaColumn.model_validate(c) for c in schema]
    columns = [
        FlowfileColumn.create_from_minimal_field_info(MinimalFieldInfo(name=c.name, data_type=c.dtype)) for c in cols
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
    tables_entries = _read_yaml(manifest.tables_manifest_path(root)).get("tables", []) or []
    _assert_within_cap(tables_entries, _MAX_TABLES, "catalog tables")
    for entry in tables_entries:
        name = entry.get("name")
        if not name:
            continue
        ns_id = _resolve_entry_namespace(entry.get("namespace") or {}, owner_id)
        schema = entry.get("schema") or []
        pcs = entry.get("partition_columns") or []
        with get_db_context() as db:
            repo = SQLAlchemyCatalogRepository(db)
            existing = repo.get_table_by_name(name, ns_id, owner_id=owner_id)
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
                resolved = _resolve_table_path(pointer, root)
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
                table.source_registration_id = _resolve_flow_uuid_to_reg_id(entry.get("source_flow_uuid"), owner_id)
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
    flow re-runs. An artifact whose source flow isn't present is skipped (its FK can't be satisfied), and a
    version deleted locally is left deleted — import must never resurrect a deletion."""
    kept: set[int] = set()
    artifact_entries = _read_yaml(manifest.models_manifest_path(root)).get("models", []) or []
    _assert_within_cap(artifact_entries, _MAX_ARTIFACTS, "models")
    for entry in artifact_entries:
        name, version = entry.get("name"), entry.get("version")
        if not name or version is None:
            continue
        src_reg_id = _resolve_flow_uuid_to_reg_id(entry.get("source_flow_uuid"), owner_id)
        if src_reg_id is None:
            logger.info(
                "Project import: skipping model %s (source flow %s absent)", name, entry.get("source_flow_uuid")
            )
            continue
        ns_id = _resolve_entry_namespace(entry.get("namespace") or {}, owner_id)
        with get_db_context() as db:
            existing = repository.owned_or_none(
                db, GlobalArtifact, "owner_id", owner_id, name=name, namespace_id=ns_id, version=version
            )
            if existing is not None and existing.status == "deleted":
                # A version deleted locally stays deleted; import must not resurrect it.
                continue
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
    kernel_entries = _read_yaml(manifest.kernels_manifest_path(root)).get("kernels", []) or []
    _assert_within_cap(kernel_entries, _MAX_KERNELS, "kernels")
    for entry in kernel_entries:
        name = entry.get("name")
        if not name:
            continue
        kernel_id = entry.get("id") or str(uuid.uuid4())
        with get_db_context() as db:
            existing = repository.owned_or_none(db, Kernel, "user_id", owner_id, id=kernel_id)
            if existing is None and db.query(Kernel).filter(Kernel.id == kernel_id).first() is not None:
                logger.warning("Project import: kernel id %s owned by another user; minting a fresh id", kernel_id)
                kernel_id = str(uuid.uuid4())
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
        table = SQLAlchemyCatalogRepository(db).get_table_by_name(ref["name"], ns_id, owner_id=owner_id)
        return table.id if table else None


def _resolve_viz_id_by_uuid(viz_uuid: str | None, owner_id: int) -> int | None:
    if not viz_uuid:
        return None
    with get_db_context() as db:
        viz = repository.owned_or_none(db, CatalogVisualization, "created_by", owner_id, viz_uuid=viz_uuid)
        return viz.id if viz else None


def _import_visualizations(root: Path, owner_id: int) -> set[str]:
    """Recreate the owner's saved charts from visualizations.yaml; return the kept viz_uuids.

    Upserts by viz_uuid. The source table is re-resolved by its portable ``{catalog, schema, name}``
    (tables.yaml rebuilds physical tables first, so it is normally present); a table-source viz whose
    table is still absent keeps its definition with a null source rather than being dropped. The
    PNG thumbnail isn't restored — it re-exports on the next view."""
    kept: set[str] = set()
    viz_entries = _read_yaml(manifest.visualizations_manifest_path(root)).get("visualizations", []) or []
    _assert_within_cap(viz_entries, _MAX_VISUALIZATIONS, "visualizations")
    for entry in viz_entries:
        name = entry.get("name")
        if not name:
            continue
        viz_uuid = entry.get("viz_uuid") or str(uuid.uuid4())
        ns_id = _resolve_entry_namespace(entry.get("namespace") or {}, owner_id)
        source_type = entry.get("source_type") or "table"
        table_id = _resolve_table_ref(entry.get("source_table"), owner_id) if source_type == "table" else None
        with get_db_context() as db:
            existing = repository.owned_or_none(db, CatalogVisualization, "created_by", owner_id, viz_uuid=viz_uuid)
            if existing is None and db.query(CatalogVisualization).filter_by(viz_uuid=viz_uuid).first() is not None:
                logger.warning("Project import: viz_uuid %s owned by another user; minting a fresh uuid", viz_uuid)
                viz_uuid = str(uuid.uuid4())
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
            tile["viz_id"] = _resolve_viz_id_by_uuid(tile.pop("viz_uuid"), owner_id)
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
    dashboard_entries = _read_yaml(manifest.dashboards_manifest_path(root)).get("dashboards", []) or []
    _assert_within_cap(dashboard_entries, _MAX_DASHBOARDS, "dashboards")
    for entry in dashboard_entries:
        name = entry.get("name")
        if not name:
            continue
        dashboard_uuid = entry.get("dashboard_uuid") or str(uuid.uuid4())
        ns_id = _resolve_entry_namespace(entry.get("namespace") or {}, owner_id)
        layout = DashboardLayout.model_validate(_localize_layout(entry.get("layout") or {}, owner_id))
        with get_db_context() as db:
            existing = repository.owned_or_none(
                db, CatalogDashboard, "created_by", owner_id, dashboard_uuid=dashboard_uuid
            )
            if existing is None and db.query(CatalogDashboard).filter_by(dashboard_uuid=dashboard_uuid).first():
                logger.warning(
                    "Project import: dashboard_uuid %s owned by another user; minting a fresh uuid", dashboard_uuid
                )
                dashboard_uuid = str(uuid.uuid4())
            dashboard = existing or CatalogDashboard(dashboard_uuid=dashboard_uuid, created_by=owner_id)
            dashboard.name = name
            dashboard.description = entry.get("description")
            dashboard.namespace_id = ns_id
            dashboard.layout_json = layout.model_dump_json()
            # layout_version is a server-owned write counter (CAS token), unrelated to the
            # layout's grid.version: keep it monotonic on upsert, start fresh rows at 1.
            if existing is not None:
                dashboard.layout_version = (existing.layout_version or 1) + 1
            else:
                dashboard.layout_version = 1
            dashboard.created_by = owner_id
            if existing is None:
                db.add(dashboard)
            db.commit()
            kept.add(dashboard_uuid)
    return kept


def _import_notebooks(root: Path, owner_id: int) -> set[str]:
    """Recreate the owner's notebooks from notebooks/*.notebook.yaml; return the kept notebook_uuids.

    Upserts by notebook_uuid (mint a fresh one on a cross-owner collision, like visualizations). The
    portable ``{catalog, schema}`` namespace re-resolves to a local id (namespaces import first) and
    the cells are written back to the runtime content file so the notebook opens with its document
    intact. A row that would collide on the global (name, namespace) uniqueness is skipped+logged so
    import always completes."""
    kept: set[str] = set()
    files = sorted(manifest.notebooks_dir(root).glob("*.notebook.yaml"))
    _assert_within_cap(files, _MAX_NOTEBOOKS, "notebooks")
    for f in files:
        data = _read_yaml(f)
        name = data.get("name")
        if not name:
            continue
        notebook_uuid = data.get("notebook_uuid") or str(uuid.uuid4())
        # Mint a fresh uuid for a malformed value: notebook_store derives the content-file path via
        # uuid.UUID(), so a non-UUID string (hand-edit / merge corruption / crafted file) would raise
        # there and abort the whole import. Validate up front to keep import always-completing.
        try:
            uuid.UUID(str(notebook_uuid))
        except (TypeError, ValueError):
            logger.warning("Project import: notebook file %s has a malformed notebook_uuid; minting fresh", f.name)
            notebook_uuid = str(uuid.uuid4())
        ns = data.get("namespace") or {}
        ns_id = (
            _resolve_namespace(ns.get("catalog"), ns.get("schema"), owner_id, create=False)
            if ns.get("catalog")
            else None
        )
        cells: list[NotebookCellModel] = []
        for item in data.get("cells") or []:
            try:
                cells.append(NotebookCellModel.model_validate(item))
            except (TypeError, ValueError):
                logger.warning("Project import: skipping invalid notebook cell in %s", f.name)
        with get_db_context() as db:
            existing = repository.owned_or_none(db, CatalogNotebook, "owner_id", owner_id, notebook_uuid=notebook_uuid)
            taken = db.query(CatalogNotebook).filter_by(notebook_uuid=notebook_uuid).first()
            if existing is None and taken is not None:
                logger.warning(
                    "Project import: notebook_uuid %s owned by another user; minting a fresh uuid", notebook_uuid
                )
                notebook_uuid = str(uuid.uuid4())
            nb = existing or CatalogNotebook(notebook_uuid=notebook_uuid, owner_id=owner_id)
            nb.name = name
            nb.description = data.get("description")
            nb.namespace_id = ns_id
            nb.default_kernel_id = data.get("default_kernel_id")
            nb.owner_id = owner_id
            if existing is None:
                db.add(nb)
            try:
                db.commit()
            except IntegrityError:
                db.rollback()
                logger.warning("Project import: notebook %r collides on (name, namespace) uniqueness; skipping", name)
                continue
            ns_name = db.get(CatalogNamespace, ns_id).name if ns_id is not None else None
        notebook_store.write_notebook_file(
            owner_id,
            notebook_uuid,
            name=name,
            description=data.get("description"),
            namespace_name=ns_name,
            default_kernel_id=data.get("default_kernel_id"),
            cells=cells,
        )
        kept.add(notebook_uuid)
    return kept


def _import_flow(data: dict, owner_id: int) -> str | None:
    """Import one flow; returns the effective flow_uuid (the minted one on a cross-owner collision) or
    None when there is nothing to import."""
    flow_uuid = data.get("flow_uuid")
    if not flow_uuid:
        return None
    flowfile_name = data.get("flowfile_name") or "flow"
    catalog_name = data.get("catalog_name") or flowfile_name  # friendly display label; preserved, not the key
    ns = data.get("namespace") or {}
    # Resolve-only: every namespace a flow belongs to is either seeded or already created by
    # _import_namespaces (which runs first), so we never create one here.
    target_ns_id = (
        _resolve_namespace(ns.get("catalog"), ns.get("schema"), owner_id, create=False) if ns.get("catalog") else None
    )
    # Serialize the probe→resolve→write→register section by the incoming flow_uuid (the contested
    # shared-dir resource) so a concurrent importer of the same unregistered uuid re-probes under the
    # lock and mints a fresh one instead of colliding on the identical runtime_path.
    with _flow_uuid_lock(flow_uuid):
        with get_db_context() as db:
            existing = repository.owned_or_none(db, FlowRegistration, "owner_id", owner_id, flow_uuid=flow_uuid)
            # A flow_uuid owned by another user (or just registered by a concurrent importer) must never
            # resolve to that on-disk file/registration: mint a fresh uuid and write a brand-new flow
            # under the caller's own dir instead of overwriting the existing file.
            if existing is None and db.query(FlowRegistration).filter(FlowRegistration.flow_uuid == flow_uuid).first():
                logger.warning("Project import: flow_uuid %s already registered; minting a fresh uuid", flow_uuid)
                flow_uuid = str(uuid.uuid4())
                data = {**data, "flow_uuid": flow_uuid}
            runtime_path = (
                Path(existing.flow_path)
                if existing
                else storage.flows_directory / "project" / f"{safe_stem(flowfile_name)}_{flow_uuid[:8]}.flow.yaml"
            )
        write_yaml(runtime_path, data)  # flow_uuid / catalog_name / namespace ignored by the FlowfileData loader
        # Load (with owner_id so cloud connections resolve) but don't open an editor session: project
        # import must not auto-open flows on the canvas. The user opens a flow explicitly via /import_flow/.
        flow_file_handler.import_flow(runtime_path, user_id=owner_id, register_session=False)
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
    return flow_uuid


def _import_schedules(data: dict, owner_id: int) -> int:
    flow_uuid = data.get("flow_uuid")
    if not flow_uuid:
        return 0
    schedules = data.get("schedules") or []
    with get_db_context() as db:
        reg = repository.owned_or_none(db, FlowRegistration, "owner_id", owner_id, flow_uuid=flow_uuid)
        if reg is None:
            return 0
        if len(schedules) > _MAX_SCHEDULES_PER_FLOW:
            logger.warning(
                "Project import: schedule file has %d entries for flow %s; capped at %d",
                len(schedules),
                flow_uuid,
                _MAX_SCHEDULES_PER_FLOW,
            )
            raise ImportTooLargeError(
                f"Project import: schedule file for flow {flow_uuid!r} has {len(schedules)} entries, "
                f"exceeding the {_MAX_SCHEDULES_PER_FLOW}-entry cap. Import aborted."
            )
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        rows = (
            db.query(FlowSchedule)
            .filter(
                FlowSchedule.registration_id == reg.id,
                FlowSchedule.schedule_type.in_(_PROJECTABLE_SCHEDULE_TYPES),
            )
            .order_by(FlowSchedule.id.asc())
            .all()
        )
        by_uuid = {r.schedule_uuid: r for r in rows}
        entries = [sd for sd in schedules if sd.get("schedule_type") in _PROJECTABLE_SCHEDULE_TYPES]
        matched: dict[int, FlowSchedule] = {}
        seen_ids: set[str] = set()
        with_ids: list[dict] = []
        for sd in entries:
            sid = sd.get("id")
            if sid is None or sid in seen_ids:
                continue
            seen_ids.add(sid)
            with_ids.append(sd)
            if sid in by_uuid:
                matched[id(sd)] = by_uuid[sid]
        claimed = {r.id for r in matched.values()}
        seen_sigs: set[tuple] = set()
        to_apply: list[dict] = list(with_ids)
        for sd in entries:
            if sd.get("id") is not None:
                continue
            sig = _schedule_signature(sd)
            if sig in seen_sigs:  # collapse already-duplicated entries so existing dupes self-heal
                continue
            seen_sigs.add(sig)
            to_apply.append(sd)
            row = next((r for r in rows if r.id not in claimed and _schedule_signature(r) == sig), None)
            if row is not None:
                matched[id(sd)] = row
                claimed.add(row.id)
        for row in rows:
            if row.id not in claimed:
                service.delete_schedule(row.id)
        count = 0
        for sd in to_apply:
            row = matched.get(id(sd))
            if row is not None:
                _update_schedule_in_place(db, row, sd)
            else:
                created = service.create_schedule(
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
                sid = sd.get("id")
                if sid is not None and db.query(FlowSchedule).filter(FlowSchedule.schedule_uuid == sid).first() is None:
                    db.query(FlowSchedule).filter(FlowSchedule.id == created.id).update({"schedule_uuid": sid})
                    db.commit()
                elif sid is not None:
                    logger.warning("Project import: schedule id %s held by another schedule; minting a fresh id", sid)
            count += 1
    return count


def _schedule_signature(s) -> tuple:
    """What an entry without an ``id`` (a file written before schedules had one) is matched on."""
    get = s.get if isinstance(s, dict) else lambda k: getattr(s, k)
    return (
        get("schedule_type"),
        get("interval_seconds"),
        get("cron_expression"),
        get("cron_timezone"),
        get("name"),
    )


def _update_schedule_in_place(db, row: FlowSchedule, sd: dict) -> None:
    """Set a schedule's definition from its file entry, keeping the row, its run state and the
    notification rules and trigger rows that point at it."""
    validate_schedule_create(
        schedule_type=sd["schedule_type"],
        interval_seconds=sd.get("interval_seconds"),
        trigger_table_id=None,
        trigger_table_ids=None,
        table_exists=lambda _table_id: False,
        cron_expression=sd.get("cron_expression"),
        cron_timezone=sd.get("cron_timezone"),
    )
    row.schedule_type = sd["schedule_type"]
    row.interval_seconds = sd.get("interval_seconds")
    row.cron_expression = sd.get("cron_expression")
    row.cron_timezone = sd.get("cron_timezone")
    row.enabled = sd.get("enabled", True)
    row.name = sd.get("name")
    row.description = sd.get("description")
    db.commit()


def _prune_owned(db, column, owner_filter, kept: set | None, delete_fn, label: str) -> None:
    """Run ``delete_fn(db, value)`` for every value of ``column`` matching ``owner_filter`` that is
    not in ``kept`` (best-effort per row; failures are logged and skipped). ``None`` prunes nothing."""
    if kept is None:
        return
    q = db.query(column).filter(owner_filter)
    if kept:
        q = q.filter(~column.in_(kept))
    for (value,) in q.all():
        try:
            delete_fn(db, value)
        except Exception:
            logger.warning("Project prune: could not remove %s %s", label, value, exc_info=True)


def _prune_removed(owner_id: int, kept: KeptResources, result: SetupResult) -> None:
    """Delete the owner's resources that are no longer present in the project files.

    A project mirrors the owner's whole environment, so on restore/reload the files are the complete
    intended set. Order matters: models and tables are pruned before flows (a flow can't be deleted
    while it still owns artifacts), and namespaces last (so emptied custom ones become deletable).
    Pruning never removes data — table data is kept (``delete_file=False``) and a model is soft-deleted
    so its blob survives. A ``None`` kept-set means the files say nothing about that category (not
    tracked, or no file of that kind in the tree), so it is left entirely alone (never pruned); a flow
    registration the export cannot write is never pruned either. Best-effort per resource: failures
    are logged, recorded on ``result.prune_errors`` (so the endpoint reports an incomplete prune
    instead of a silent success), and skipped.
    """
    # Kernels are independent of the catalog graph, so order is free. Delete only the config row
    # (the running container, if any, is dropped by the manager reconcile that follows the import).
    from flowfile_core.kernel.persistence import delete_kernel

    with get_db_context() as db:
        _prune_owned(db, Kernel.id, Kernel.user_id == owner_id, kept.kernel_ids, delete_kernel, "kernel")
    # Dashboards then visualizations (tiles reference viz by value, not FK, so order is free). The
    # GraphicWalker spec is the whole artifact — there is no separate blob to preserve.
    # Grants must be cleaned explicitly: bulk Query.delete() bypasses the ORM after_delete backstop
    # (see auth/sharing.py:373-374), so orphaned resource_grants rows would survive and could
    # re-attach to a future resource that reuses the freed rowid.
    with get_db_context() as db:
        if kept.dashboard_uuids is not None:
            dq = db.query(CatalogDashboard.id).filter(CatalogDashboard.created_by == owner_id)
            if kept.dashboard_uuids:
                dq = dq.filter(~CatalogDashboard.dashboard_uuid.in_(kept.dashboard_uuids))
            for (dash_id,) in dq.all():
                sharing.delete_grants_for_resource(db, "dashboard", dash_id)
                db.query(CatalogDashboard).filter_by(id=dash_id).delete()
        if kept.viz_uuids is not None:
            vq = db.query(CatalogVisualization.id).filter(CatalogVisualization.created_by == owner_id)
            if kept.viz_uuids:
                vq = vq.filter(~CatalogVisualization.viz_uuid.in_(kept.viz_uuids))
            for (viz_id,) in vq.all():
                sharing.delete_grants_for_resource(db, "visualization", viz_id)
                db.query(CatalogVisualization).filter_by(id=viz_id).delete()
        db.commit()
    # Notebooks (independent of the catalog graph). Clean grants explicitly (bulk delete bypasses the
    # ORM after_delete backstop) and drop the on-disk content file so a pruned notebook leaves nothing.
    if kept.notebook_uuids is not None:
        with get_db_context() as db:
            nq = db.query(CatalogNotebook.id, CatalogNotebook.notebook_uuid).filter(
                CatalogNotebook.owner_id == owner_id
            )
            if kept.notebook_uuids:
                nq = nq.filter(~CatalogNotebook.notebook_uuid.in_(kept.notebook_uuids))
            for nb_id, nb_uuid in nq.all():
                sharing.delete_grants_for_resource(db, "catalog_notebook", nb_id)
                db.query(CatalogNotebook).filter_by(id=nb_id).delete()
                notebook_store.delete_notebook_file(owner_id, nb_uuid)
            db.commit()
    # Models first (soft-delete, keep the blob): a kept flow's pruned artifact just goes inactive.
    # A pruned flow's still-active artifacts are NOT touched here (delete_flow can't drop them — it
    # raises FlowHasArtifactsError); they are soft-deleted in the flow-prune branch below regardless
    # of this toggle, so the flow delete succeeds. None = not tracked here, leave them all.
    if kept.artifact_ids is not None:
        with get_db_context() as db:
            q = db.query(GlobalArtifact).filter(
                GlobalArtifact.owner_id == owner_id, GlobalArtifact.status == "active"
            )
            if kept.artifact_ids:
                q = q.filter(~GlobalArtifact.id.in_(kept.artifact_ids))
            q.update({"status": "deleted"}, synchronize_session=False)
            db.commit()
    # Tables before flows (keep the data); skip flow-virtual tables (not projected, not ours to prune).
    if kept.table_ids is not None:
        with get_db_context() as db:
            service = CatalogService(SQLAlchemyCatalogRepository(db))
            q = db.query(CatalogTable).filter(CatalogTable.owner_id == owner_id)
            if kept.table_ids:
                q = q.filter(~CatalogTable.id.in_(kept.table_ids))
            for table in q.all():
                if table.table_type == "virtual" and not table.sql_query:
                    continue
                try:
                    service.delete_table(table.id, delete_file=False)
                except Exception:
                    logger.warning("Project prune: could not remove table %s", table.name, exc_info=True)
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        fq = db.query(FlowRegistration).filter(FlowRegistration.owner_id == owner_id)
        if kept.flow_uuids:
            fq = fq.filter(~FlowRegistration.flow_uuid.in_(kept.flow_uuids))
        regs = [] if kept.flow_uuids is None else fq.all()
        for reg in regs:
            # A registration the export never writes (no file, .flowfile, kernel scratch) isn't in the files.
            if not projection.exportable(reg.flow_path):
                continue
            # Read attributes into locals before delete_flow expires/detaches the ORM row.
            reg_flow_path = reg.flow_path
            reg_flow_uuid = reg.flow_uuid
            # A pruned flow must be fully removed even when this project doesn't track artifacts
            # (kept_artifact_ids is None, so the soft-delete pass above was skipped). Release the
            # flow's still-active artifacts here regardless of the toggle, otherwise delete_flow
            # raises FlowHasArtifactsError and the flow survives in DB+disk as a resurrectable ghost.
            db.query(GlobalArtifact).filter(
                GlobalArtifact.source_registration_id == reg.id,
                GlobalArtifact.status == "active",
            ).update({"status": "deleted"}, synchronize_session=False)
            db.commit()
            try:
                for s in service.list_schedules(registration_id=reg.id):
                    service.delete_schedule(s.id)
                service.delete_flow(reg.id, delete_file=True)
                # Evict from the in-memory handler too (catalog delete only touches DB+disk), so a
                # flow open before a restore can't ghost in _flows / a user session after pruning.
                flow_file_handler.evict_flow_by_path(reg_flow_path)
            except Exception:
                logger.warning("Project prune: could not remove flow %s", reg_flow_uuid, exc_info=True)
                result.prune_errors.append(f"flow:{reg_flow_uuid}")
    with get_db_context() as db:
        _prune_owned(
            db,
            DatabaseConnection.connection_name,
            DatabaseConnection.user_id == owner_id,
            kept.db_connections,
            lambda d, name: delete_database_connection(d, name, owner_id),
            "db conn",
        )
    with get_db_context() as db:
        _prune_owned(
            db,
            CloudStorageConnection.connection_name,
            CloudStorageConnection.user_id == owner_id,
            kept.cloud_connections,
            lambda d, name: delete_cloud_connection(d, name, owner_id),
            "cloud conn",
        )
    # Namespaces last: flows were pruned above, so an emptied custom schema is now deletable. Delete
    # schemas (level 1) before catalogs (level 0); seeded is_public namespaces are excluded entirely.
    if kept.namespace_ids is None:
        return
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        nq = db.query(CatalogNamespace.id, CatalogNamespace.level).filter(
            CatalogNamespace.owner_id == owner_id, CatalogNamespace.is_public.is_(False)
        )
        if kept.namespace_ids:
            nq = nq.filter(~CatalogNamespace.id.in_(kept.namespace_ids))
        for ns_id, _level in sorted(nq.all(), key=lambda r: -(r[1] or 0)):
            try:
                service.delete_namespace(ns_id)
            except NamespaceNotEmptyError:
                logger.info("Project prune: namespace %s still has tables/flows; skipping", ns_id)
            except Exception:
                logger.warning("Project prune: could not delete namespace %s", ns_id, exc_info=True)


def _reconcile_kernel_manager(owner_id: int) -> None:
    """Best-effort: refresh the kernel manager's in-memory registry for ``owner_id`` after an import.

    Scoped to the importing owner so a concurrent import by another user can't cause
    ``_cleanup_container`` to kill a third party's kernel. The manager's ``_kernels_lock``
    is taken inside ``reconcile_configs_from_db`` for the full mutation. Failures are swallowed;
    the DB rows are authoritative and appear on the next core start."""
    try:
        import flowfile_core.kernel as kernel_pkg

        manager = getattr(kernel_pkg, "_manager", None)
        if manager is not None:
            manager.reconcile_configs_from_db(owner_id=owner_id)
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

    with project_sync.suppress_projection(owner_id):
        return _do_import_project(root, owner_id, prune)


def _assert_within_cap(items: list, cap: int, kind: str) -> None:
    """Raise if ``items`` exceeds ``cap``, bounding per-import resource counts."""
    if len(items) <= cap:
        return
    raise ImportTooLargeError(
        f"Project import: {kind} manifest exceeds the {cap}-entry cap "
        f"({len(items)} entries). Import aborted to prevent resource exhaustion."
    )


_ROOT_MANIFEST_CAPS = [
    ("namespaces.yaml", "namespaces", _MAX_NAMESPACES, "namespaces"),
    ("tables.yaml", "tables", _MAX_TABLES, "catalog tables"),
    ("models.yaml", "models", _MAX_ARTIFACTS, "models"),
    ("kernels.yaml", "kernels", _MAX_KERNELS, "kernels"),
    ("visualizations.yaml", "visualizations", _MAX_VISUALIZATIONS, "visualizations"),
    ("dashboards.yaml", "dashboards", _MAX_DASHBOARDS, "dashboards"),
]


def preflight_import_caps(root: Path, sha: str) -> None:
    """Validate the size/entry caps over the tree at ``sha`` BEFORE its files are written to disk,
    so a cap breach aborts a restore cleanly (files unchanged, DB untouched) instead of tearing the
    DB/file state mid-rebuild. Raises the same ``ValueError`` the inline caps do."""
    from flowfile_core.project import git_ops

    sizes = git_ops.tree_blob_sizes(root, sha)
    for path, size in sizes.items():
        if path.endswith(".yaml") and size > _MAX_YAML_BYTES:
            raise ImportTooLargeError(
                f"Project import: manifest file {path.split('/')[-1]!r} is {size} bytes, "
                f"which exceeds the {_MAX_YAML_BYTES}-byte cap. Import aborted."
            )

    def _under(prefix: str, suffix: str) -> list[str]:
        return [p for p in sizes if p.startswith(prefix) and p.endswith(suffix)]

    _assert_within_cap(_under("flows/", ".flow.yaml"), _MAX_FLOWS, "flows")
    _assert_within_cap(_under("schedules/", ".yaml"), _MAX_FLOWS, "schedules")
    _assert_within_cap(_under("connections/database/", ".yaml"), _MAX_CONNECTIONS, "database connections")
    _assert_within_cap(_under("connections/cloud/", ".yaml"), _MAX_CONNECTIONS, "cloud connections")
    _assert_within_cap(_under("notebooks/", ".notebook.yaml"), _MAX_NOTEBOOKS, "notebooks")

    for path, key, cap, kind in _ROOT_MANIFEST_CAPS:
        text = git_ops.read_blob(root, sha, path)
        if not text:
            continue
        _assert_within_cap((yaml.safe_load(text) or {}).get(key, []) or [], cap, kind)

    for path in _under("schedules/", ".yaml"):
        text = git_ops.read_blob(root, sha, path)
        schedules = ((yaml.safe_load(text) or {}) if text else {}).get("schedules", []) or []
        if len(schedules) > _MAX_SCHEDULES_PER_FLOW:
            raise ImportTooLargeError(
                f"Project import: schedule file {path.split('/')[-1]!r} has {len(schedules)} entries, "
                f"exceeding the {_MAX_SCHEDULES_PER_FLOW}-entry cap. Import aborted."
            )


def preflight_import_caps_worktree(root: Path) -> None:
    """Validate size/entry caps over the on-disk working tree before a rebuild that mutates the DB,
    so a cap breach aborts cleanly (DB untouched) instead of mid-rebuild. Used by reload, which (unlike
    restore) imports the working tree directly. Raises the same ``ValueError`` the inline caps do."""
    for path in root.rglob("*.yaml"):
        if path.is_file() and path.stat().st_size > _MAX_YAML_BYTES:
            raise ImportTooLargeError(
                f"Project import: manifest file {path.name!r} is {path.stat().st_size} bytes, "
                f"which exceeds the {_MAX_YAML_BYTES}-byte cap. Import aborted."
            )
    _assert_within_cap(list(manifest.flows_dir(root).glob("*.flow.yaml")), _MAX_FLOWS, "flows")
    sched_files = list(manifest.schedules_dir(root).glob("*.yaml"))
    _assert_within_cap(sched_files, _MAX_FLOWS, "schedules")
    db_files = list(manifest.connections_dir(root, "database").glob("*.yaml"))
    cloud_files = list(manifest.connections_dir(root, "cloud").glob("*.yaml"))
    _assert_within_cap(db_files, _MAX_CONNECTIONS, "database connections")
    _assert_within_cap(cloud_files, _MAX_CONNECTIONS, "cloud connections")
    _assert_within_cap(list(manifest.notebooks_dir(root).glob("*.notebook.yaml")), _MAX_NOTEBOOKS, "notebooks")
    for path_fn, key, cap, kind in (
        (manifest.namespaces_manifest_path, "namespaces", _MAX_NAMESPACES, "namespaces"),
        (manifest.tables_manifest_path, "tables", _MAX_TABLES, "catalog tables"),
        (manifest.models_manifest_path, "models", _MAX_ARTIFACTS, "models"),
        (manifest.kernels_manifest_path, "kernels", _MAX_KERNELS, "kernels"),
        (manifest.visualizations_manifest_path, "visualizations", _MAX_VISUALIZATIONS, "visualizations"),
        (manifest.dashboards_manifest_path, "dashboards", _MAX_DASHBOARDS, "dashboards"),
    ):
        _assert_within_cap(_read_yaml(path_fn(root)).get(key, []) or [], cap, kind)
    for f in sched_files:
        schedules = _read_yaml(f).get("schedules", []) or []
        if len(schedules) > _MAX_SCHEDULES_PER_FLOW:
            raise ImportTooLargeError(
                f"Project import: schedule file {f.name!r} has {len(schedules)} entries, "
                f"exceeding the {_MAX_SCHEDULES_PER_FLOW}-entry cap. Import aborted."
            )


_LIST_MANIFESTS = {
    "namespaces": ("namespaces", "catalog"),
    "tables": ("tables", "name"),
    "models": ("models", "name"),
    "kernels": ("kernels", "name"),
    "visualizations": ("visualizations", "name"),
    "dashboards": ("dashboards", "name"),
}


def _file_kind(rel: str) -> str | None:
    """The kind of a repo-relative path the importer reads, or ``None`` for a file it never reads
    (the same globs ``_do_import_project_inner`` uses: one level deep, by suffix)."""
    parts = rel.split("/")
    if len(parts) == 1:
        if rel == manifest.MANIFEST_NAME:
            return "project"
        stem = rel[: -len(".yaml")] if rel.endswith(".yaml") else None
        return stem if stem in manifest.ROOT_MANIFESTS else None
    if len(parts) == 2:
        folder, name = parts
        if folder == "flows" and name.endswith(".flow.yaml"):
            return "flow"
        if folder == "schedules" and name.endswith(".yaml"):
            return "schedule"
        if folder == "notebooks" and name.endswith(".notebook.yaml"):
            return "notebook"
    if (
        len(parts) == 3
        and parts[0] == "connections"
        and parts[1] in ("database", "cloud")
        and parts[2].endswith(".yaml")
    ):
        return f"{parts[1]}_connection"
    return None


def _yaml_problem(e: yaml.YAMLError) -> str:
    problem = getattr(e, "problem", None) or "the file could not be parsed"
    mark = getattr(e, "problem_mark", None)
    return f"{problem} (line {mark.line + 1})" if mark is not None else problem


def _invalid_fields(e: ValidationError) -> str:
    """The failing field locations of a validation error, never its input values (which may hold a
    host, a user name or a secret placeholder)."""
    locs = sorted({".".join(str(p) for p in err["loc"]) or "<root>" for err in e.errors()})
    return ", ".join(locs)


def _is_mapping_list(value) -> bool:
    return value is None or (isinstance(value, list) and all(isinstance(v, dict) for v in value))


def _is_text(value) -> bool:
    return isinstance(value, str) and bool(value)


def _optional(value, kind: type) -> bool:
    return value is None or isinstance(value, kind)


def _check_file(rel: str, ok: bool, problem: str) -> None:
    if not ok:
        raise ProjectFileError(f"Project file {rel!r} {problem}. Nothing was changed.")


def _parse_project_file(rel: str, text: str) -> dict:
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as e:
        raise ProjectFileError(
            f"Project file {rel!r} is not valid YAML: {_yaml_problem(e)}. Nothing was changed."
        ) from None
    if data is None:
        return {}
    _check_file(rel, isinstance(data, dict), f"must hold a mapping at the top level, not a {type(data).__name__}")
    return data


def _dashboard_layout_shape(layout: dict) -> dict:
    """A layout with its portable references replaced by unresolved local ids, the shape
    ``_localize_layout`` hands to ``DashboardLayout`` (a reference that does not resolve is ``None``)."""
    out = copy.deepcopy(layout)
    for key, portable, local in (("tiles", "viz_uuid", "viz_id"), ("filters", "datasource", "datasource_id")):
        items = out.get(key)
        for item in items if isinstance(items, list) else []:
            if isinstance(item, dict) and portable in item:
                item.pop(portable)
                item[local] = None
    return out


def _validate_manifest_entries(rel: str, stem: str, data: dict) -> None:
    key, name_key = _LIST_MANIFESTS[stem]
    entries = data.get(key)
    _check_file(rel, _is_mapping_list(entries), f"must list its {key} as mappings under {key!r}")
    for i, entry in enumerate(entries or []):
        where = f"entry {i + 1} of {key!r}"
        _check_file(rel, _is_text(entry.get(name_key)), f"{where} has no {name_key!r}")
        _check_file(rel, _optional(entry.get("namespace"), dict), f"{where} has a 'namespace' that is not a mapping")
        if stem == "namespaces":
            schemas = entry.get("schemas")
            _check_file(rel, _is_mapping_list(schemas), f"{where} must list its 'schemas' as mappings")
            for schema in schemas or []:
                _check_file(rel, _is_text(schema.get("name")), f"{where} has a schema with no 'name'")
        elif stem == "tables":
            _check_file(
                rel, _is_mapping_list(entry.get("schema")), f"{where} must list its 'schema' columns as mappings"
            )
            _check_file(rel, _optional(entry.get("pointer"), dict), f"{where} has a 'pointer' that is not a mapping")
            _check_file(
                rel,
                _optional(entry.get("partition_columns"), list),
                f"{where} has 'partition_columns' that is not a list",
            )
        elif stem == "models":
            _check_file(rel, isinstance(entry.get("version"), int), f"{where} has no integer 'version'")
            _check_file(rel, _optional(entry.get("tags"), list), f"{where} has 'tags' that is not a list")
        elif stem == "kernels":
            _check_file(rel, _optional(entry.get("packages"), list), f"{where} has 'packages' that is not a list")
        elif stem == "visualizations":
            _check_file(
                rel, _optional(entry.get("source_table"), dict), f"{where} has a 'source_table' that is not a mapping"
            )
        elif stem == "dashboards":
            layout = entry.get("layout")
            _check_file(rel, _optional(layout, dict), f"{where} has a 'layout' that is not a mapping")
            try:
                DashboardLayout.model_validate(_dashboard_layout_shape(layout or {}))
            except ValidationError as e:
                _check_file(rel, False, f"{where} has an invalid layout ({_invalid_fields(e)})")


def _validate_project_file(rel: str, kind: str, text: str) -> None:
    """Raise ``ProjectFileError`` when one file's content would make the import skip an entry as if it
    were absent, or raise half-way through."""
    data = _parse_project_file(rel, text)
    if kind == "secrets":
        names = data.get("required_secrets")
        ok = names is None or (isinstance(names, list) and all(_is_text(n) for n in names))
        _check_file(rel, ok, "must list 'required_secrets' as names")
    elif kind in _LIST_MANIFESTS:
        _validate_manifest_entries(rel, kind, data)
    elif kind == "flow":
        _check_file(rel, _is_text(data.get("flow_uuid")), "has no string 'flow_uuid'")
        _check_file(rel, _optional(data.get("namespace"), dict), "has a 'namespace' that is not a mapping")
        for field_name in ("flowfile_name", "catalog_name"):
            _check_file(rel, _optional(data.get(field_name), str), f"has a {field_name!r} that is not text")
    elif kind == "schedule":
        _check_file(rel, _is_text(data.get("flow_uuid")), "has no string 'flow_uuid'")
        _check_file(rel, _is_mapping_list(data.get("schedules")), "must list its 'schedules' as mappings")
        for i, entry in enumerate(data.get("schedules") or []):
            _check_file(rel, _optional(entry.get("id"), str), f"has a schedule {i + 1} whose 'id' is not text")
    elif kind == "notebook":
        _check_file(rel, _is_text(data.get("name")), "has no 'name'")
        _check_file(rel, _optional(data.get("namespace"), dict), "has a 'namespace' that is not a mapping")
        cells = data.get("cells")
        _check_file(rel, _is_mapping_list(cells), "must list its 'cells' as mappings")
        for i, cell in enumerate(cells or []):
            try:
                NotebookCellModel.model_validate(cell)
            except ValidationError as e:
                _check_file(rel, False, f"has an invalid cell {i + 1} ({_invalid_fields(e)})")
    elif kind in ("database_connection", "cloud_connection"):
        try:
            if kind == "database_connection":
                DatabaseConnectionEntry.model_validate(data)
            else:
                entry = CloudConnectionEntry.model_validate(data)
                FullCloudStorageConnection(
                    connection_name=entry.connection_name,
                    storage_type=entry.storage_type,
                    auth_method=entry.auth_method,
                    **{f: getattr(entry, f) for f in _CLOUD_PLAIN_FIELDS},
                )
        except ValidationError as e:
            _check_file(rel, False, f"is not a valid connection ({_invalid_fields(e)})")


def _validate_project_files(read_text, rels: list[str]) -> None:
    """Validate every file the import reads, ``read_text(rel)`` giving its text. The tables and models
    manifests are skipped when the tree's own project.yaml opts out of tracking them, as the import does."""
    kinds = {rel: kind for rel in rels if (kind := _file_kind(rel)) is not None}
    texts: dict[str, str] = {}
    for rel in sorted(kinds):
        try:
            texts[rel] = read_text(rel)
        except UnicodeDecodeError:
            raise ProjectFileError(f"Project file {rel!r} is not UTF-8 text. Nothing was changed.") from None
    project = (
        _parse_project_file(manifest.MANIFEST_NAME, texts[manifest.MANIFEST_NAME])
        if manifest.MANIFEST_NAME in texts
        else {}
    )
    tracks_data = project.get("track_data_artifacts", True)
    for rel in sorted(kinds):
        if kinds[rel] in ("tables", "models") and not tracks_data:
            continue
        _validate_project_file(rel, kinds[rel], texts[rel])


def preflight_import_files(root: Path, sha: str) -> None:
    """Read and validate every managed file of the tree at ``sha`` before a restore writes anything,
    so an unreadable or malformed file aborts it with the folder and the database untouched."""
    from flowfile_core.project import git_ops

    rels = list(git_ops.tree_blob_sizes(root, sha))
    _validate_project_files(lambda rel: git_ops.read_blob(root, sha, rel) or "", rels)


def preflight_import_files_worktree(root: Path) -> None:
    """Read and validate every managed file of the working tree before open or reload touches the
    database, so an unreadable or malformed file aborts it with nothing changed."""
    paths = [manifest.manifest_path(root)] + [root / f"{stem}.yaml" for stem in manifest.ROOT_MANIFESTS]
    paths += manifest.flows_dir(root).glob("*.flow.yaml")
    paths += manifest.schedules_dir(root).glob("*.yaml")
    paths += manifest.connections_dir(root, "database").glob("*.yaml")
    paths += manifest.connections_dir(root, "cloud").glob("*.yaml")
    paths += manifest.notebooks_dir(root).glob("*.notebook.yaml")
    rels = [p.relative_to(root).as_posix() for p in paths if p.is_file()]
    _validate_project_files(lambda rel: (root / rel).read_text(encoding="utf-8"), rels)


def _do_import_project(root: Path, owner_id: int, prune: bool = False) -> SetupResult:
    token = _ns_resolve_cache.set({})
    try:
        return _do_import_project_inner(root, owner_id, prune)
    finally:
        _ns_resolve_cache.reset(token)


def _do_import_project_inner(root: Path, owner_id: int, prune: bool = False) -> SetupResult:
    dotenv = load_dotenv(root)
    result = SetupResult()

    _import_standalone_secrets(root, owner_id, dotenv, result)

    db_files = sorted(manifest.connections_dir(root, "database").glob("*.yaml"))
    _assert_within_cap(db_files, _MAX_CONNECTIONS, "database connections")
    kept_db: set[str] = set()
    for f in db_files:
        name = _import_db_connection(_read_yaml(f), owner_id, dotenv, result)
        if name:
            kept_db.add(name)

    cloud_files = sorted(manifest.connections_dir(root, "cloud").glob("*.yaml"))
    _assert_within_cap(cloud_files, _MAX_CONNECTIONS, "cloud connections")
    kept_cloud: set[str] = set()
    for f in cloud_files:
        name = _import_cloud_connection(_read_yaml(f), owner_id, dotenv, result)
        if name:
            kept_cloud.add(name)

    kept_namespace_ids = _import_namespaces(root, owner_id)
    if not manifest.namespaces_manifest_path(root).exists():
        kept_namespace_ids = None

    flow_files = sorted(manifest.flows_dir(root).glob("*.flow.yaml"))
    _assert_within_cap(flow_files, _MAX_FLOWS, "flows")
    kept_flow_uuids: set[str] = set()
    for f in flow_files:
        data = _read_yaml(f)
        effective_uuid = _import_flow(data, owner_id)
        if effective_uuid is not None:
            result.imported_flows += 1
            kept_flow_uuids.add(effective_uuid)

    schedule_files = sorted(manifest.schedules_dir(root).glob("*.yaml"))
    _assert_within_cap(schedule_files, _MAX_FLOWS, "schedules")
    for f in schedule_files:
        result.imported_schedules += _import_schedules(_read_yaml(f), owner_id)

    # Tables + models after flows so their namespace and source-flow lineage resolve. When the
    # project opts out of tracking them, skip import and signal prune to leave them alone (None).
    m = manifest.read_manifest(root)
    track_data_artifacts = m is None or m.track_data_artifacts
    # A root manifest absent from the tree says nothing about its kind: None, so prune leaves it alone.
    kept_table_ids = (
        _import_tables(root, owner_id)
        if track_data_artifacts and manifest.tables_manifest_path(root).exists()
        else None
    )
    kept_artifact_ids = (
        _import_artifacts(root, owner_id)
        if track_data_artifacts and manifest.models_manifest_path(root).exists()
        else None
    )
    kept_kernel_ids = _import_kernels(root, owner_id) if manifest.kernels_manifest_path(root).exists() else None
    # Visualizations after tables (source re-links by name); dashboards after visualizations
    # (tiles re-link by viz_uuid).
    kept_viz_uuids = (
        _import_visualizations(root, owner_id) if manifest.visualizations_manifest_path(root).exists() else None
    )
    kept_dashboard_uuids = (
        _import_dashboards(root, owner_id) if manifest.dashboards_manifest_path(root).exists() else None
    )
    kept_notebook_uuids = _import_notebooks(root, owner_id)
    if not any(manifest.notebooks_dir(root).glob("*.notebook.yaml")):
        kept_notebook_uuids = None

    if prune:
        _prune_removed(
            owner_id,
            # A folder with no file of its kind says nothing about it, so that kind is not pruned.
            KeptResources(
                flow_uuids=kept_flow_uuids if flow_files else None,
                db_connections=kept_db if db_files else None,
                cloud_connections=kept_cloud if cloud_files else None,
                namespace_ids=kept_namespace_ids,
                table_ids=kept_table_ids,
                artifact_ids=kept_artifact_ids,
                kernel_ids=kept_kernel_ids,
                viz_uuids=kept_viz_uuids,
                dashboard_uuids=kept_dashboard_uuids,
                notebook_uuids=kept_notebook_uuids,
            ),
            result,
        )

    _reconcile_kernel_manager(owner_id)

    from flowfile_core.project import git_ops

    with get_db_context() as db:
        proj = repository.get_by_path(db, str(root))
        if proj is not None:
            repository.set_head_sha(db, proj.id, git_ops.head_sha(root))
    return result
