"""DB → files writers. Deterministic, secret-free, atomic.

Reads the catalog DB and renders the project tree. Secrets are emitted as
``${secret:NAME}`` placeholders — never ciphertext (which is user-keyed and
useless on another machine).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import yaml
from sqlalchemy.orm import Session

from flowfile_core.database.models import (
    CloudStorageConnection,
    DatabaseConnection,
    FlowRegistration,
    FlowSchedule,
    Secret,
)
from flowfile_core.project import manifest
from flowfile_core.project.normalize import normalize_flow_data, safe_stem, write_yaml
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


def project_flow(root: Path, reg: FlowRegistration) -> None:
    data = _load_flow_data(reg.flow_path)
    if data is None:
        return
    target = manifest.flows_dir(root) / f"{safe_stem(reg.name)}.flow.yaml"
    if target.exists():
        existing = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
        if existing.get("flow_uuid") not in (None, reg.flow_uuid):
            target = manifest.flows_dir(root) / f"{safe_stem(reg.name)}_{reg.flow_uuid[:8]}.flow.yaml"
    write_yaml(target, normalize_flow_data(data, reg.flow_uuid))


def remove_flow(root: Path, name: str) -> None:
    for p in manifest.flows_dir(root).glob(f"{safe_stem(name)}*.flow.yaml"):
        p.unlink(missing_ok=True)


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


def project_database_connection(db: Session, root: Path, name: str, owner_id: int) -> None:
    conn = (
        db.query(DatabaseConnection)
        .filter(DatabaseConnection.connection_name == name, DatabaseConnection.user_id == owner_id)
        .first()
    )
    if conn is None:
        return
    write_yaml(manifest.connections_dir(root, "database") / f"{safe_stem(name)}.yaml", _db_connection_dict(db, conn))


def project_cloud_connection(db: Session, root: Path, name: str, owner_id: int) -> None:
    conn = (
        db.query(CloudStorageConnection)
        .filter(CloudStorageConnection.connection_name == name, CloudStorageConnection.user_id == owner_id)
        .first()
    )
    if conn is None:
        return
    write_yaml(manifest.connections_dir(root, "cloud") / f"{safe_stem(name)}.yaml", _cloud_connection_dict(db, conn))


def remove_connection(root: Path, kind: str, name: str) -> None:
    manifest.connections_dir(root, kind).joinpath(f"{safe_stem(name)}.yaml").unlink(missing_ok=True)


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


def project_schedules_for_registration(db: Session, root: Path, reg: FlowRegistration) -> None:
    schedules = (
        db.query(FlowSchedule)
        .filter(
            FlowSchedule.registration_id == reg.id,
            FlowSchedule.schedule_type.in_(_PROJECTABLE_SCHEDULE_TYPES),
        )
        .order_by(FlowSchedule.id.asc())
        .all()
    )
    target = manifest.schedules_dir(root) / f"{safe_stem(reg.name)}.yaml"
    if not schedules:
        target.unlink(missing_ok=True)
        return
    write_yaml(target, {"flow_uuid": reg.flow_uuid, "schedules": [_schedule_dict(s) for s in schedules]})


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


# --- full projection ---------------------------------------------------------


def project_all(db: Session, root: Path, owner_id: int) -> None:
    for reg in (
        db.query(FlowRegistration).filter(FlowRegistration.owner_id == owner_id).order_by(FlowRegistration.name.asc())
    ):
        project_flow(root, reg)
        project_schedules_for_registration(db, root, reg)
    for conn in db.query(DatabaseConnection).filter(DatabaseConnection.user_id == owner_id):
        project_database_connection(db, root, conn.connection_name, owner_id)
    for conn in db.query(CloudStorageConnection).filter(CloudStorageConnection.user_id == owner_id):
        project_cloud_connection(db, root, conn.connection_name, owner_id)
    regenerate_secret_manifest(db, root, owner_id)
