"""Portable engine and local-file resolution for the catalog database."""

import threading
from pathlib import Path

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine, make_url

from shared.storage_config import get_database_url

SQLITE_POOL_ARGS = {"pool_size": 20, "max_overflow": 30, "pool_timeout": 5}

_engines: dict[str, Engine] = {}
_engines_lock = threading.Lock()


def _enable_wal(dbapi_connection, _record) -> None:
    dbapi_connection.execute("PRAGMA journal_mode=WAL")


def create_catalog_engine(url: str | None = None) -> Engine:
    """Engine factory: file SQLite gets ``SQLITE_POOL_ARGS`` (memory SQLite rejects them); never alters the file."""
    parsed = make_url(url or get_database_url())
    is_sqlite = parsed.get_backend_name() == "sqlite"
    connect_args = {"check_same_thread": False} if is_sqlite else {}
    pool_args = SQLITE_POOL_ARGS if is_sqlite and sqlite_database_path(parsed) is not None else {}
    return create_engine(parsed, connect_args=connect_args, pool_pre_ping=not is_sqlite, **pool_args)


def get_catalog_engine(url: str | None = None) -> Engine:
    """Process-wide engine per URL, WAL-enabled for file SQLite.

    Ad-hoc callers share this instead of leaking a throwaway engine per call.
    """
    key = make_url(url or get_database_url()).render_as_string(hide_password=False)
    with _engines_lock:
        engine = _engines.get(key)
        if engine is None:
            engine = _engines[key] = create_catalog_engine(key)
            if sqlite_database_path(key) is not None:
                event.listen(engine, "connect", _enable_wal)
        return engine


def sqlite_database_path(url: str | None = None) -> Path | None:
    """Return a file path only for an on-disk SQLite catalog."""
    parsed = make_url(url or get_database_url())
    if parsed.get_backend_name() != "sqlite" or parsed.database in (None, "", ":memory:"):
        return None
    if parsed.query.get("mode") == "memory":
        return None
    return Path(parsed.database)


_CLOUD_CONNECTION_FIELDS = (
    "connection_name",
    "storage_type",
    "auth_method",
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
_CLOUD_CONNECTION_SECRETS = (
    "aws_secret_access_key",
    "aws_session_token",
    "azure_account_key",
    "azure_client_secret",
    "azure_sas_token",
    "gcs_service_account_key",
)


def cloud_connection_record(connection_id: int) -> tuple[dict, dict[str, str | None]] | None:
    """A stored cloud connection by id: its plain fields and its secrets as stored ciphertext; None when absent.

    Read from the catalog database this process is configured for, by id only, with no access check: who may use
    the connection was decided where the reference to it was made.
    """
    secrets = ", ".join(
        f"(SELECT encrypted_value FROM secrets WHERE secrets.id = c.{name}_id) AS {name}"
        for name in _CLOUD_CONNECTION_SECRETS
    )
    query = text(
        f"SELECT {', '.join('c.' + name for name in _CLOUD_CONNECTION_FIELDS)}, {secrets} "
        "FROM cloud_storage_connections c WHERE c.id = :id"
    )
    with get_catalog_engine().connect() as connection:
        row = connection.execute(query, {"id": connection_id}).mappings().first()
    if row is None:
        return None
    return (
        {name: row[name] for name in _CLOUD_CONNECTION_FIELDS},
        {name: row[name] for name in _CLOUD_CONNECTION_SECRETS},
    )
