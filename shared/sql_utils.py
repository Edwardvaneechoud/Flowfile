"""
Shared SQL URI construction and conversion utilities.

Used by both flowfile_core and flowfile_worker to avoid duplicating
URI-building logic across services.
"""

from __future__ import annotations

from urllib.parse import quote_plus

# Database types speaking the postgres wire protocol, where libpq-style
# sslmode/connect_timeout query params are valid (pymysql rejects unknown params).
POSTGRES_FAMILY = {"postgresql", "postgres", "redshift"}


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
        **kwargs: Additional connection parameters appended as query string

    Returns:
        str: Formatted database URI

    Raises:
        ValueError: If insufficient information is provided
    """
    if url:
        return url

    if database_type.lower() == "sqlite":
        path = database or host or "./database.db"
        # Strip sqlite:/// prefix if the full URI was passed as the path
        if path.startswith("sqlite:///"):
            path = path[len("sqlite:///"):]
        return f"sqlite:///{path}"

    if not host:
        raise ValueError("Host is required to create a URI")

    credentials = ""
    if username:
        credentials = username
        if password:
            encoded_password = quote_plus(password)
            credentials += f":{encoded_password}"
        credentials += "@"

    port_section = f":{port}" if port else ""

    if database:
        base_uri = f"{database_type}://{credentials}{host}{port_section}/{database}"
    else:
        base_uri = f"{database_type}://{credentials}{host}{port_section}"

    query_params: dict[str, str] = {}
    if database_type.lower() in POSTGRES_FAMILY:
        if ssl_enabled:
            query_params["sslmode"] = "require"
        if connect_timeout is not None:
            query_params["connect_timeout"] = str(connect_timeout)
    query_params.update(kwargs)

    if query_params:
        sep = "&" if "?" in base_uri else "?"
        params = "&".join(f"{key}={quote_plus(str(value))}" for key, value in query_params.items())
        base_uri += f"{sep}{params}"

    return base_uri


# Mapping from base database URI schemes to SQLAlchemy-compatible schemes with driver suffixes.
# connectorx uses base schemes (e.g. mysql://) while SQLAlchemy needs driver-specific schemes.
SQLALCHEMY_DRIVER_MAP = {
    "mysql": "mysql+pymysql",
}


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
