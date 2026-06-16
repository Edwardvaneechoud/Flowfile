"""files → DB. The single, idempotent import path.

Reused by project setup/open, restore, and external-change reload. Upserts by
resource name; missing secret values become empty placeholders so setup never
dead-ends (the user fills them in later).
"""

from __future__ import annotations

import logging
from pathlib import Path

import yaml
from pydantic import SecretStr

from flowfile_core import flow_file_handler
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.catalog.exceptions import NamespaceNotEmptyError
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import (
    CatalogNamespace,
    CloudStorageConnection,
    DatabaseConnection,
    FlowRegistration,
    FlowSchedule,
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


def _resolve_namespace(
    catalog_name: str | None,
    schema_name: str | None,
    owner_id: int,
    description: str | None = None,
    create: bool = True,
) -> int | None:
    """Resolve the catalog (level 0) then schema (level 1) by name; return the schema id (or the
    catalog id when no schema). Reuses seeded rows (name lookups ignore owner); idempotent.
    With ``create`` (custom namespaces) a missing row is created with ``description``; without it
    (seeder-managed system namespaces) a missing row yields None instead of a private duplicate."""
    if not catalog_name:
        return None
    with get_db_context() as db:
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        catalog = service.repo.get_namespace_by_name(catalog_name, None)
        if catalog is None:
            if not create:
                return None
            catalog = service.create_namespace(
                name=catalog_name, owner_id=owner_id, description=None if schema_name else description
            )
        if not schema_name:
            return catalog.id
        schema = service.repo.get_namespace_by_name(schema_name, catalog.id)
        if schema is None:
            if not create:
                return None
            schema = service.create_namespace(
                name=schema_name, owner_id=owner_id, parent_id=catalog.id, description=description
            )
        return schema.id


def _import_namespaces(root: Path, owner_id: int) -> set[int]:
    """Recreate the owner's namespaces from namespaces.yaml (nested ``catalog -> schemas``); return
    the set of ids that must survive a prune. Seeded ``is_public`` entries resolve-only (the seeder
    owns them); custom entries are get-or-created."""
    catalogs = _read_yaml(manifest.namespaces_manifest_path(root)).get("namespaces", []) or []
    kept: set[int] = set()
    for catalog in sorted(catalogs, key=lambda c: c.get("catalog") or ""):
        catalog_name = catalog.get("catalog")
        if not catalog_name:
            continue
        cat_id = _resolve_namespace(
            catalog_name, None, owner_id, catalog.get("description"), create=not catalog.get("is_public")
        )
        if cat_id is not None:
            kept.add(cat_id)
        for schema in catalog.get("schemas") or []:
            schema_name = schema.get("name")
            if not schema_name:
                continue
            sch_id = _resolve_namespace(
                catalog_name, schema_name, owner_id, schema.get("description"), create=not schema.get("is_public")
            )
            if sch_id is not None:
                kept.add(sch_id)
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
) -> None:
    """Delete the owner's flows/connections/namespaces that are no longer present in the project files.

    A project mirrors the owner's whole environment, so on restore/reload the files are the complete
    intended set. Best-effort per resource: a failure (e.g. a flow with artifacts) is logged and skipped.
    """
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


def import_project(root: Path, owner_id: int, prune: bool = False) -> SetupResult:
    """Rebuild this install's environment from the project folder. Always completes.

    With ``prune=True`` (restore / reload) the owner's flows and connections that are absent from the
    files are deleted, so the DB ends up matching the files exactly. ``prune=False`` (open) is additive.
    """
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

    if prune:
        _prune_removed(owner_id, kept_flow_uuids, kept_db, kept_cloud, kept_namespace_ids)

    from flowfile_core.project import git_ops

    with get_db_context() as db:
        proj = repository.get_by_path(db, str(root))
        if proj is not None:
            repository.set_head_sha(db, proj.id, git_ops.head_sha(root))
    return result
