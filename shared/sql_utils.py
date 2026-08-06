"""
Shared SQL URI construction and conversion utilities.

Used by both flowfile_core and flowfile_worker to avoid duplicating
URI-building logic across services. Per-dialect behavior lives in
``shared.db_dialects``; this module is the stable entry point.
"""

from __future__ import annotations

import logging

from shared.db_dialects import POSTGRES_FAMILY, get_dialect_or_generic, iter_dialects

logger = logging.getLogger(__name__)

__all__ = ["POSTGRES_FAMILY", "SQLALCHEMY_DRIVER_MAP", "construct_sql_uri", "get_sqlalchemy_uri"]


def construct_sql_uri(
    database_type: str = "postgresql",
    host: str | None = None,
    port: int | None = None,
    username: str | None = None,
    password: str | None = None,
    database: str | None = None,
    url: str | None = None,
    ssl_enabled: bool = False,
    connect_timeout: int | None = None,
    auth_method: str | None = None,
    private_key: str | None = None,
    private_key_passphrase: str | None = None,
    oauth_token: str | None = None,
    **kwargs,
) -> str:
    """
    Constructs a SQL URI string from the provided parameters.

    Args:
        database_type: Database type (postgresql, mysql, sqlite, etc.)
        host: Database host address
        port: Database port number
        username: Database username
        password: Database password as a plain string (caller handles decryption)
        database: Database name
        url: Complete database URL (overrides other parameters if provided)
        ssl_enabled: Adds sslmode=require for postgres-family databases
        connect_timeout: Connection timeout in seconds (postgres-family only)
        auth_method: Authentication method; must be in the dialect's auth_methods
            ("password" or None everywhere, "key_pair" where supported)
        private_key: Private key PEM text as a plain string (caller handles decryption)
        private_key_passphrase: Optional passphrase for an encrypted private key
        oauth_token: OAuth access token as a plain string (caller handles refresh/decryption)
        **kwargs: Additional connection parameters appended as query string

    Returns:
        str: Formatted database URI

    Raises:
        ValueError: If insufficient information is provided
    """
    if url:
        return url

    return get_dialect_or_generic(database_type).build_uri(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        ssl_enabled=ssl_enabled,
        connect_timeout=connect_timeout,
        auth_method=auth_method,
        private_key=private_key,
        private_key_passphrase=private_key_passphrase,
        oauth_token=oauth_token,
        **kwargs,
    )


# Mapping from base database URI schemes to SQLAlchemy-compatible schemes with driver suffixes.
# connectorx uses base schemes (e.g. mysql://) while SQLAlchemy needs driver-specific schemes.
# Built from the dialect registry; kept as a module constant for backward compatibility.
SQLALCHEMY_DRIVER_MAP = {d.name: d.sqlalchemy_driver for d in iter_dialects() if d.sqlalchemy_driver}


def get_sqlalchemy_uri(uri: str) -> str:
    """Convert a base database URI to SQLAlchemy-compatible format with driver suffix.

    connectorx (used by pl.read_database_uri) accepts base URI schemes like mysql://,
    but SQLAlchemy requires driver-specific schemes like mysql+pymysql://.
    This function converts base URIs to the SQLAlchemy-compatible format.

    URIs that don't need conversion (e.g. postgresql://, sqlite:///) are returned unchanged.

    Args:
        uri: A database URI string (e.g. "mysql://user:pass@host:3306/db")

    Returns:
        The URI with the appropriate SQLAlchemy driver suffix applied.
    """
    for base_scheme, sa_scheme in SQLALCHEMY_DRIVER_MAP.items():
        if uri.startswith(f"{base_scheme}://"):
            return uri.replace(f"{base_scheme}://", f"{sa_scheme}://", 1)
    return uri
