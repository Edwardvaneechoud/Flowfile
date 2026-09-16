"""Portable engine and local-file resolution for the catalog database."""

from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, make_url

from shared.storage_config import get_database_url


def create_catalog_engine(url: str | None = None) -> Engine:
    """Create a catalog engine with driver-specific connection options."""
    parsed = make_url(url or get_database_url())
    connect_args = {"check_same_thread": False} if parsed.get_backend_name() == "sqlite" else {}
    return create_engine(parsed, connect_args=connect_args)


def sqlite_database_path(url: str | None = None) -> Path | None:
    """Return a file path only for an on-disk SQLite catalog."""
    parsed = make_url(url or get_database_url())
    if parsed.get_backend_name() != "sqlite" or parsed.database in (None, "", ":memory:"):
        return None
    if parsed.query.get("mode") == "memory":
        return None
    return Path(parsed.database)
