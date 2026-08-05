"""Database connection management for flowfile_frame.

This module provides functions for managing database connections,
similar to how cloud_storage/secret_manager.py handles cloud storage connections.
"""

from pydantic import SecretStr

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    get_database_connection,
    get_database_connection_schema,
    store_database_connection,
)
from flowfile_core.schemas.input_schema import (
    FullDatabaseConnection,
    FullDatabaseConnectionInterface,
)
from shared.db_dialects import KNOWN_DIALECT_NAMES, get_dialect_or_generic


def get_current_user_id() -> int:
    """Get the current user ID for database operations.

    Returns:
        int: The current user ID (defaults to 1 for single-user mode).
    """
    # In single-file mode, we use user_id = 1
    return 1


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
    extra_params: dict[str, str] | None = None,
    auth_method: str | None = None,
    private_key: str | SecretStr | None = None,
    private_key_passphrase: str | SecretStr | None = None,
) -> FullDatabaseConnection:
    """Create and store a new database connection.

    Args:
        connection_name: Unique name for this connection.
        database_type: Type of database (one of shared.db_dialects.KNOWN_DIALECT_NAMES,
            e.g. postgresql, mysql, sqlite, duckdb, mssql, snowflake).
        host: Database server hostname.
        port: Database server port.
        database: Database name.
        username: Database username (not needed for file-based types like sqlite/duckdb).
        password: Database password (not needed for file-based types like sqlite/duckdb).
        ssl_enabled: Whether to use SSL for the connection.
        url: Full database URL (overrides other connection parameters).
        extra_params: Dialect-specific connection parameters, e.g. for Snowflake
            ``{"account": "myorg-myaccount", "warehouse": "COMPUTE_WH", "role": "ANALYST"}``.
            Keys that could override credentials (user, password, host, ...) are rejected.
        auth_method: Authentication method; must be supported by the dialect
            (``"password"`` everywhere, ``"key_pair"`` for Snowflake JWT auth).
        private_key: Private key PEM *text* (never a path) for key-pair auth, e.g.
            ``open("rsa_key.p8").read()``. Stored as an encrypted secret.
        private_key_passphrase: Optional passphrase when the PEM is encrypted.

    Returns:
        FullDatabaseConnection: The created connection object.

    Raises:
        ValueError: If a connection with this name already exists, the
            database_type is not a supported dialect, or key-pair auth is
            requested without a private key.
    """
    if database_type.lower() not in KNOWN_DIALECT_NAMES:
        raise ValueError(
            f"Unsupported database type '{database_type}'. Supported types: {', '.join(KNOWN_DIALECT_NAMES)}"
        )
    user_id = get_current_user_id()

    if isinstance(password, str):
        password = SecretStr(password)
    if isinstance(private_key, str):
        private_key = SecretStr(private_key)
    if isinstance(private_key_passphrase, str):
        private_key_passphrase = SecretStr(private_key_passphrase)

    if get_dialect_or_generic(database_type).file_based:
        # No credentials for file-based databases; the stored model requires strings
        username = username or ""
        password = password if password is not None else SecretStr("")
    if auth_method == "key_pair" and password is None:
        password = SecretStr("")

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
        extra_params=extra_params,
        auth_method=auth_method,
        private_key=private_key,
        private_key_passphrase=private_key_passphrase,
    )

    with get_db_context() as db:
        store_database_connection(db, connection, user_id)

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
    extra_params: dict[str, str] | None = None,
    auth_method: str | None = None,
    private_key: str | SecretStr | None = None,
    private_key_passphrase: str | SecretStr | None = None,
) -> FullDatabaseConnection:
    """Create a database connection if it doesn't already exist.

    Args:
        connection_name: Unique name for this connection.
        database_type: Type of database (one of shared.db_dialects.KNOWN_DIALECT_NAMES,
            e.g. postgresql, mysql, sqlite, duckdb, mssql, snowflake).
        host: Database server hostname.
        port: Database server port.
        database: Database name.
        username: Database username (not needed for file-based types like sqlite/duckdb).
        password: Database password (not needed for file-based types like sqlite/duckdb).
        ssl_enabled: Whether to use SSL for the connection.
        url: Full database URL (overrides other connection parameters).
        extra_params: Dialect-specific connection parameters (see create_database_connection).
        auth_method: Authentication method (see create_database_connection).
        private_key: Private key PEM text for key-pair auth (see create_database_connection).
        private_key_passphrase: Optional passphrase when the PEM is encrypted.

    Returns:
        FullDatabaseConnection: The existing or newly created connection.
    """
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
        extra_params=extra_params,
        auth_method=auth_method,
        private_key=private_key,
        private_key_passphrase=private_key_passphrase,
    )


def get_database_connection_by_name(connection_name: str) -> FullDatabaseConnection | None:
    """Get a database connection by its name.

    Args:
        connection_name: The name of the connection to retrieve.

    Returns:
        FullDatabaseConnection if found, None otherwise.
    """
    user_id = get_current_user_id()
    with get_db_context() as db:
        return get_database_connection_schema(db, connection_name, user_id)


def get_all_available_database_connections() -> list[FullDatabaseConnectionInterface]:
    """Get all available database connections for the current user.

    Returns:
        List of database connection interfaces (without passwords).
    """
    from flowfile_core.database.models import DatabaseConnection as DBConnectionModel
    from flowfile_core.flowfile.database_connection_manager.db_connections import parse_extra_params

    user_id = get_current_user_id()
    with get_db_context() as db:
        connections = db.query(DBConnectionModel).filter(DBConnectionModel.user_id == user_id).all()

        return [
            FullDatabaseConnectionInterface(
                connection_name=conn.connection_name,
                database_type=conn.database_type,
                username=conn.username,
                host=conn.host,
                port=conn.port,
                database=conn.database,
                ssl_enabled=conn.ssl_enabled,
                extra_params=parse_extra_params(conn.extra_params),
                auth_method=conn.auth_method,
            )
            for conn in connections
        ]


def del_database_connection(connection_name: str) -> bool:
    """Delete a database connection by its name.

    Args:
        connection_name: The name of the connection to delete.

    Returns:
        True if the connection was deleted, False if it didn't exist.
    """
    from flowfile_core.database.models import Secret

    user_id = get_current_user_id()
    with get_db_context() as db:
        connection = get_database_connection(db, connection_name, user_id)
        if connection:
            # Delete every associated secret (password + key-pair material)
            all_secret_ids = (
                connection.password_id,
                connection.private_key_id,
                connection.private_key_passphrase_id,
            )
            secret_ids = [secret_id for secret_id in all_secret_ids if secret_id is not None]
            if secret_ids:
                db.query(Secret).filter(Secret.id.in_(secret_ids)).delete(synchronize_session=False)

            db.delete(connection)
            db.commit()
            return True
        return False
