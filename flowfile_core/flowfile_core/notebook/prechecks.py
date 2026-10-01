"""Path and connection checks a notebook cell's source or writer passes before it is placed.

The canvas applies these rules when a node runs; a sync applies them when a cell places the node, so a
cell that names a server path in multi-user mode, a connection the user cannot use, or no connection
where server credentials are refused fails on its line before any node exists. The checks are the
canvas's own predicates, without what they would open: row lookups (own first, then group-granted)
and ``sharing`` rules read live, never a connection, a decrypted secret or a file. Settings still
holding a ``${name}`` reference are checked when the node runs instead.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from flowfile_core.auth import sharing
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager import db_connections
from flowfile_core.kafka.connection_manager import get_kafka_connection, get_kafka_connection_by_name
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageSettings
from flowfile_core.secret_manager.secret_manager import get_encrypted_secret
from shared.cloud_storage.utils import validate_cloud_resource_path
from shared.db_dialects import get_dialect_or_generic


def placement_refusal(settings: Any, user_id: int | None) -> str | None:
    """Why ``settings`` may not be placed for ``user_id``, in the canvas's words; ``None`` when it may."""
    if isinstance(settings, input_schema.NodeCloudStorageReader):
        return _cloud_refusal(settings.cloud_storage_settings, "reader", user_id)
    if isinstance(settings, input_schema.NodeCloudStorageWriter):
        return _cloud_refusal(settings.cloud_storage_settings, "writer", user_id)
    if isinstance(settings, input_schema.NodeDatabaseReader):
        return _database_refusal(settings.database_settings, user_id)
    if isinstance(settings, input_schema.NodeDatabaseWriter):
        return _database_refusal(settings.database_write_settings, user_id)
    if isinstance(settings, input_schema.NodeKafkaSource):
        return _kafka_refusal(settings.kafka_settings, user_id)
    return None


def _unresolved(*values: str | None) -> bool:
    return any("${" in (value or "") for value in values)


def _cloud_refusal(settings: CloudStorageSettings, role: str, user_id: int | None) -> str | None:
    """``validate_cloud_resource_path`` and the connection rules of ``get_cloud_connection_settings``, undecrypted."""
    from flowfile_core.flowfile.flow_graph import get_cloud_connection_settings

    if _unresolved(settings.resource_path, settings.connection_name):
        return None
    try:
        validate_cloud_resource_path(
            settings.resource_path, role=role, allow_local_paths=sharing.ambient_credentials_allowed()
        )
    except ValueError as exc:
        return str(exc)
    name = settings.connection_name
    if not name:
        try:
            get_cloud_connection_settings(None, user_id, settings.auth_mode, resource_path=settings.resource_path)
        except (ValueError, HTTPException) as exc:
            return str(exc.detail) if isinstance(exc, HTTPException) else str(exc)
        return None
    with get_db_context() as db:
        row = db_connections.get_cloud_connection(db, name, user_id)
        if row is None:
            return "Cloud connection settings not found"
        if sharing.uses_server_identity(row.auth_method) and not sharing.is_admin_user(db, row.user_id):
            return f"Cloud connection '{name}' ({row.auth_method}) {sharing.SERVER_IDENTITY_REFUSED_MESSAGE}"
    return None


def _database_refusal(settings: Any, user_id: int | None) -> str | None:
    """The connection or password secret a database node names must exist for the user; nothing is read from it.

    A file-based inline connection (SQLite, DuckDB) uses no password, as on the canvas.
    """
    if settings.connection_mode == "reference":
        name = settings.database_connection_name
        if _unresolved(name):
            return None
        with get_db_context() as db:
            if db_connections.get_database_connection(db, name, user_id) is None:
                return f"Database connection '{name}' not found or not accessible for this user"
        return None
    connection = settings.database_connection
    if connection is None or get_dialect_or_generic(connection.database_type).file_based:
        return None
    password_ref = getattr(connection, "password_ref", None)
    if password_ref and not _unresolved(password_ref) and get_encrypted_secret(user_id, password_ref) is None:
        return "Password not found"
    return None


def _kafka_refusal(settings: input_schema.KafkaSourceSettings, user_id: int | None) -> str | None:
    if _unresolved(settings.kafka_connection_name):
        return None
    with get_db_context() as db:
        row = None
        if settings.kafka_connection_id is not None:
            row = get_kafka_connection(db, settings.kafka_connection_id, user_id)
        if row is None and settings.kafka_connection_name:
            row = get_kafka_connection_by_name(db, settings.kafka_connection_name, user_id)
    return "Kafka connection not found" if row is None else None
