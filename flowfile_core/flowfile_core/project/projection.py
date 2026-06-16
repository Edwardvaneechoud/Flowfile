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
    CatalogNamespace,
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


def _name_stem(name: str | None) -> str:
    """Clean file stem for a flow name. Strips a trailing ``.flow`` so a flow saved to
    ``x.flow.yaml`` (name becomes ``x.flow``) doesn't project to ``x.flow.flow.yaml``."""
    name = name or ""
    if name.endswith(".flow"):
        name = name[: -len(".flow")]
    return safe_stem(name)


def _flow_stem(reg: FlowRegistration) -> str:
    """The flow's file stem: its intrinsic ``flowfile_name`` (stable across the DB↔files round-trip)."""
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
                stem = _name_stem(data.get("flowfile_name") or p.name)
                manifest.schedules_dir(root).joinpath(f"{stem}.yaml").unlink(missing_ok=True)
    elif name:
        stem = _name_stem(name)
        for p in flows_dir.glob(f"{stem}*.flow.yaml"):
            p.unlink(missing_ok=True)
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
            stem = _name_stem(data.get("flowfile_name") or p.name)
            manifest.schedules_dir(root).joinpath(f"{stem}.yaml").unlink(missing_ok=True)


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
    target = manifest.connections_dir(root, "database") / f"{safe_stem(name)}.yaml"
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
    target = manifest.connections_dir(root, "cloud") / f"{safe_stem(name)}.yaml"
    write_yaml(target, _cloud_connection_dict(db, conn))
    return target


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
    # resolved stem in full projection; the single-schedule hook recomputes it).
    target = manifest.schedules_dir(root) / f"{stem or _flow_stem(reg)}.yaml"
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
                catalogs[catalog_name] = _namespace_entry(
                    "catalog", catalog_name, parent.description if parent else None, bool(parent and parent.is_public)
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
    _prune_orphan_files(root, written - {None})
