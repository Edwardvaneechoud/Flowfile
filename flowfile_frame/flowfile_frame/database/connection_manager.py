"""Database connection management for flowfile_frame.

This module provides functions for managing database connections,
similar to how cloud_storage/secret_manager.py handles cloud storage connections.
"""

from pydantic import SecretStr

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_database_connection,
    store_database_connection,
)
from flowfile_core.schemas.input_schema import (
    FullDatabaseConnection,
    FullDatabaseConnectionInterface,
)
from flowfile_frame import _metadata
from flowfile_frame._identity import current_user_id
from flowfile_frame.notebook import refuse
from shared.db_dialects import KNOWN_DIALECT_NAMES, get_dialect_or_generic
from shared.snowflake_auth import AUTH_ACCESS_TOKEN, AUTH_KEY_PAIR, AUTH_OAUTH

SNOWFLAKE = "snowflake"


def get_current_user_id() -> int:
    """Get the current user ID for database operations.

    Returns:
        int: The current user ID; see ``_identity.current_user_id``.
    """
    return current_user_id()


def _checked_auth_method(
    database_type: str,
    auth_method: str | None,
    role: str | None,
    warehouse: str | None,
    private_key_passphrase: str | None,
) -> str | None:
    """The sign-in method to store: Snowflake's (an access token when none is given), ``None`` for every other type.

    Raises ``ValueError`` for browser sign-in, which needs the app, and for a Snowflake setting on another type.
    """
    if database_type.lower() != SNOWFLAKE:
        given = [
            name
            for name, value in (
                ("auth_method", auth_method),
                ("role", role),
                ("warehouse", warehouse),
                ("private_key_passphrase", private_key_passphrase),
            )
            if value is not None
        ]
        if given:
            raise ValueError(f"{', '.join(given)} only applies to Snowflake connections.")
        return None
    method = auth_method or AUTH_ACCESS_TOKEN
    if method == AUTH_OAUTH:
        raise ValueError(
            "Browser sign-in to Snowflake is only available in the app: create the connection there "
            "(Settings > Connections > Database), or use auth_method='access_token' or 'key_pair'."
        )
    if method not in (AUTH_ACCESS_TOKEN, AUTH_KEY_PAIR):
        raise ValueError(f"Unknown Snowflake auth_method {method!r}; use 'access_token' or 'key_pair'.")
    return method


def create_database_connection(
    connection_name: str,
    *,
    database_type: str = "postgresql",
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    username: str | None = None,
    password: str | SecretStr | None = None,
    ssl_enabled: bool = False,
    url: str | None = None,
    auth_method: str | None = None,
    role: str | None = None,
    warehouse: str | None = None,
    private_key_passphrase: str | None = None,
) -> FullDatabaseConnection:
    """Create and store a new database connection.

    Args:
        connection_name: Unique name for this connection.
        database_type: Type of database (one of shared.db_dialects.KNOWN_DIALECT_NAMES,
            e.g. postgresql, mysql, sqlite, duckdb, mssql, denodo, snowflake).
        host: Database server hostname.
        port: Database server port.
        database: Database name.
        username: Database username (not needed for file-based types like sqlite/duckdb).
        password: Database password (not needed for file-based types like sqlite/duckdb).
        ssl_enabled: Whether to use SSL for the connection.
        url: Full database URL (overrides other connection parameters).
        auth_method: Snowflake only: ``"access_token"`` (the default) or ``"key_pair"``. Browser sign-in
            (``"oauth"``) is only available from the app's connection form.
        role: Snowflake only: the role the session uses; the user's default role when omitted.
        warehouse: Snowflake only: the warehouse queries run on; the user's default when omitted.
        private_key_passphrase: Snowflake key-pair only: opens an encrypted private key once; it is not stored.

    For Snowflake, ``host`` is the account (an account identifier such as ``myorg-myaccount``, its
    ``.snowflakecomputing.com`` host, or a pasted Snowsight link), ``username`` the Snowflake user and
    ``password`` the access token or, with ``auth_method="key_pair"``, the private key file's text; port and
    SSL are not used::

        ff.create_database_connection(
            "snowflake_sales",
            database_type="snowflake",
            host="myorg-myaccount",
            username="ETL_SERVICE",
            password=Path("rsa_key.p8").read_text(),
            auth_method="key_pair",
            warehouse="COMPUTE_WH",
            database="SALES",
        )

    Returns:
        FullDatabaseConnection: The created connection object.

    Raises:
        ValueError: If a connection with this name already exists, the database_type is not a
            supported dialect, or the Snowflake settings cannot be used (the message says why).
    """
    refuse("ff.create_database_connection")
    if database_type.lower() not in KNOWN_DIALECT_NAMES:
        raise ValueError(
            f"Unsupported database type '{database_type}'. Supported types: {', '.join(KNOWN_DIALECT_NAMES)}"
        )
    auth_method = _checked_auth_method(database_type, auth_method, role, warehouse, private_key_passphrase)
    user_id = get_current_user_id()

    if isinstance(password, str):
        password = SecretStr(password)

    if get_dialect_or_generic(database_type).file_based:
        # No credentials for file-based databases; the stored model requires strings
        username = username or ""
        password = password if password is not None else SecretStr("")

    connection = FullDatabaseConnection(
        connection_name=connection_name,
        database_type=database_type,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        ssl_enabled=ssl_enabled,
        url=url,
        auth_method=auth_method,
        role=role,
        warehouse=warehouse,
    )

    with get_db_context() as db:
        store_database_connection(db, connection, user_id, private_key_passphrase=private_key_passphrase)

    return connection


def create_database_connection_if_not_exists(
    connection_name: str,
    *,
    database_type: str = "postgresql",
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    username: str | None = None,
    password: str | SecretStr | None = None,
    ssl_enabled: bool = False,
    url: str | None = None,
    auth_method: str | None = None,
    role: str | None = None,
    warehouse: str | None = None,
    private_key_passphrase: str | None = None,
) -> FullDatabaseConnection:
    """Create a database connection if it doesn't already exist.

    Args:
        connection_name: Unique name for this connection.
        database_type: Type of database (one of shared.db_dialects.KNOWN_DIALECT_NAMES,
            e.g. postgresql, mysql, sqlite, duckdb, mssql, denodo, snowflake).
        host: Database server hostname.
        port: Database server port.
        database: Database name.
        username: Database username (not needed for file-based types like sqlite/duckdb).
        password: Database password (not needed for file-based types like sqlite/duckdb).
        ssl_enabled: Whether to use SSL for the connection.
        url: Full database URL (overrides other connection parameters).
        auth_method: Snowflake only: ``"access_token"`` (the default) or ``"key_pair"``. Browser sign-in
            (``"oauth"``) is only available from the app's connection form.
        role: Snowflake only: the role the session uses; the user's default role when omitted.
        warehouse: Snowflake only: the warehouse queries run on; the user's default when omitted.
        private_key_passphrase: Snowflake key-pair only: opens an encrypted private key once; it is not stored.

    Returns:
        FullDatabaseConnection: The existing or newly created connection.
    """
    refuse("ff.create_database_connection_if_not_exists")
    get_current_user_id()

    existing = get_database_connection_by_name(connection_name)
    if existing:
        return existing

    return create_database_connection(
        connection_name,
        database_type=database_type,
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        ssl_enabled=ssl_enabled,
        url=url,
        auth_method=auth_method,
        role=role,
        warehouse=warehouse,
        private_key_passphrase=private_key_passphrase,
    )


def get_database_connection_by_name(connection_name: str) -> FullDatabaseConnection | None:
    """Get a database connection by its name.

    In a notebook kernel session the connection comes from the app and its ``password`` is empty: no
    secret reaches the kernel. In a script it is the stored (encrypted) value.

    Args:
        connection_name: The name of the connection to retrieve.

    Returns:
        FullDatabaseConnection if found, None otherwise.
    """
    return _metadata.database_connection(connection_name)


def get_all_available_database_connections() -> list[FullDatabaseConnectionInterface]:
    """Get all available database connections for the current user.

    Returns:
        List of database connection interfaces (without passwords).
    """
    return _metadata.database_connections()


def del_database_connection(connection_name: str) -> bool:
    """Delete a database connection by its name.

    Args:
        connection_name: The name of the connection to delete.

    Returns:
        True if the connection was deleted, False if it didn't exist.
    """
    refuse("ff.del_database_connection")
    from flowfile_core.database.models import Secret

    user_id = get_current_user_id()
    with get_db_context() as db:
        connection = get_database_connection(db, connection_name, user_id)
        if connection:
            # Delete the associated password secret
            if connection.password_id:
                secret = db.query(Secret).filter(Secret.id == connection.password_id).first()
                if secret:
                    db.delete(secret)

            db.delete(connection)
            db.commit()
            return True
        return False
