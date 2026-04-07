"""
Shared SQL URI construction and conversion utilities.

Used by both flowfile_core and flowfile_worker to avoid duplicating
URI-building logic across services.
"""

from __future__ import annotations

from urllib.parse import quote_plus


def construct_sql_uri(
    database_type: str = "postgresql",
    host: str | None = None,
    port: int | None = None,
    username: str | None = None,
    password: str | None = None,
    database: str | None = None,
    url: str | None = None,
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
        **kwargs: Additional connection parameters appended as query string

    Returns:
        str: Formatted database URI

    Raises:
        ValueError: If insufficient information is provided
    """
    # If URL is explicitly provided, return it directly
    if url:
        return url

    # For SQLite, we handle differently since it uses a file path
    if database_type.lower() == "sqlite":
        # For SQLite, database is the path to the file
        path = database or host or "./database.db"
        # Strip sqlite:/// prefix if the full URI was passed as the path
        if path.startswith("sqlite:///"):
            path = path[len("sqlite:///"):]
        return f"sqlite:///{path}"

    # Validate that minimum required fields are present for other databases
    if not host:
        raise ValueError("Host is required to create a URI")

    # Create credential part if username is provided
    credentials = ""
    if username:
        credentials = username
        if password:
            encoded_password = quote_plus(password)
            credentials += f":{encoded_password}"
        credentials += "@"

    # Add port if specified
    port_section = f":{port}" if port else ""

    # Create base URI
    if database:
        base_uri = f"{database_type}://{credentials}{host}{port_section}/{database}"
    else:
        base_uri = f"{database_type}://{credentials}{host}{port_section}"

    # Add any additional connection parameters
    if kwargs:
        params = "&".join(f"{key}={quote_plus(str(value))}" for key, value in kwargs.items())
        base_uri += f"?{params}"

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
