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
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    _get_own_cloud_connection,
    _get_own_database_connection,
    store_cloud_connection,
    store_database_connection,
    update_cloud_connection,
    update_database_connection,
)
from flowfile_core.project import manifest, repository
from flowfile_core.project.models import SetupResult
from flowfile_core.project.normalize import safe_stem, write_yaml
from flowfile_core.project.projection import _CLOUD_SECRETS
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


def _import_flow(data: dict, owner_id: int) -> bool:
    flow_uuid = data.get("flow_uuid")
    if not flow_uuid:
        return False
    name = data.get("flowfile_name") or "flow"
    with get_db_context() as db:
        existing = db.query(FlowRegistration).filter(FlowRegistration.flow_uuid == flow_uuid).first()
        runtime_path = (
            Path(existing.flow_path)
            if existing
            else storage.flows_directory / "project" / f"{safe_stem(name)}_{flow_uuid[:8]}.flow.yaml"
        )
    write_yaml(runtime_path, data)  # flow_uuid key is ignored by the FlowfileData loader
    flow_file_handler.import_flow(runtime_path, user_id=owner_id)
    if existing is None:
        auto_register_flow(str(runtime_path), name, owner_id)
        with get_db_context() as db:
            reg = db.query(FlowRegistration).filter(FlowRegistration.flow_path == str(runtime_path)).first()
            if reg and reg.flow_uuid != flow_uuid:
                reg.flow_uuid = flow_uuid
                db.commit()
    return True


def _import_schedules(data: dict, owner_id: int) -> int:
    flow_uuid = data.get("flow_uuid")
    schedules = data.get("schedules") or []
    if not flow_uuid or not schedules:
        return 0
    count = 0
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter(FlowRegistration.flow_uuid == flow_uuid).first()
        if reg is None:
            return 0
        service = CatalogService(SQLAlchemyCatalogRepository(db))
        existing = {s.name: s for s in service.list_schedules(registration_id=reg.id) if s.name}
        for sd in schedules:
            sd_name = sd.get("name")
            if sd_name and sd_name in existing:
                service.update_schedule(
                    existing[sd_name].id,
                    enabled=sd.get("enabled"),
                    interval_seconds=sd.get("interval_seconds"),
                    cron_expression=sd.get("cron_expression"),
                    cron_timezone=sd.get("cron_timezone"),
                    description=sd.get("description"),
                )
            else:
                service.create_schedule(
                    registration_id=reg.id,
                    owner_id=owner_id,
                    schedule_type=sd["schedule_type"],
                    interval_seconds=sd.get("interval_seconds"),
                    cron_expression=sd.get("cron_expression"),
                    cron_timezone=sd.get("cron_timezone"),
                    enabled=sd.get("enabled", True),
                    name=sd_name,
                    description=sd.get("description"),
                )
            count += 1
    return count


def import_project(root: Path, owner_id: int) -> SetupResult:
    """Rebuild this install's environment from the project folder. Always completes."""
    dotenv = load_dotenv(root)
    result = SetupResult()

    _import_standalone_secrets(root, owner_id, dotenv, result)

    for f in sorted(manifest.connections_dir(root, "database").glob("*.yaml")):
        _import_db_connection(_read_yaml(f), owner_id, dotenv, result)
    for f in sorted(manifest.connections_dir(root, "cloud").glob("*.yaml")):
        _import_cloud_connection(_read_yaml(f), owner_id, dotenv, result)

    for f in sorted(manifest.flows_dir(root).glob("*.flow.yaml")):
        if _import_flow(_read_yaml(f), owner_id):
            result.imported_flows += 1

    for f in sorted(manifest.schedules_dir(root).glob("*.yaml")):
        result.imported_schedules += _import_schedules(_read_yaml(f), owner_id)

    from flowfile_core.project import git_ops

    with get_db_context() as db:
        proj = repository.get_by_path(db, str(root))
        if proj is not None:
            repository.set_head_sha(db, proj.id, git_ops.head_sha(root))
    return result
