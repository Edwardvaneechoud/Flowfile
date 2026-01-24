"""Database connection management for flowfile_frame.

This module provides functions for managing database connections,
similar to how cloud_storage/secret_manager.py handles cloud storage connections.
"""

from typing import Literal

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
    database_type: Literal["postgresql", "mysql", "sqlite", "mssql", "oracle"] = "postgresql",
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    username: str | None = None,
    password: str | SecretStr | None = None,
    ssl_enabled: bool = False,
    url: str | None = None,
) -> FullDatabaseConnection:
    """Create and store a new database connection.

    Args:
        connection_name: Unique name for this connection.
        database_type: Type of database (postgresql, mysql, sqlite, mssql, oracle).
        host: Database server hostname.
        port: Database server port.
        database: Database name.
        username: Database username.
        password: Database password.
        ssl_enabled: Whether to use SSL for the connection.
        url: Full database URL (overrides other connection parameters).

    Returns:
        FullDatabaseConnection: The created connection object.

    Raises:
        ValueError: If a connection with this name already exists.
    """
    user_id = get_current_user_id()

    # Convert password to SecretStr if it's a plain string
    if isinstance(password, str):
        password = SecretStr(password)

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
    )

    with get_db_context() as db:
        store_database_connection(db, connection, user_id)

    return connection


def create_database_connection_if_not_exists(
    connection_name: str,
    *,
    database_type: Literal["postgresql", "mysql", "sqlite", "mssql", "oracle"] = "postgresql",
    host: str | None = None,
    port: int | None = None,
    database: str | None = None,
    username: str | None = None,
    password: str | SecretStr | None = None,
    ssl_enabled: bool = False,
    url: str | None = None,
) -> FullDatabaseConnection:
    """Create a database connection if it doesn't already exist.

    Args:
        connection_name: Unique name for this connection.
        database_type: Type of database (postgresql, mysql, sqlite, mssql, oracle).
        host: Database server hostname.
        port: Database server port.
        database: Database name.
        username: Database username.
        password: Database password.
        ssl_enabled: Whether to use SSL for the connection.
        url: Full database URL (overrides other connection parameters).

    Returns:
        FullDatabaseConnection: The existing or newly created connection.
    """
    user_id = get_current_user_id()

    # Check if connection already exists
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

    user_id = get_current_user_id()
    with get_db_context() as db:
        connections = (
            db.query(DBConnectionModel)
            .filter(DBConnectionModel.user_id == user_id)
            .all()
        )

        return [
            FullDatabaseConnectionInterface(
                connection_name=conn.connection_name,
                database_type=conn.database_type,
                username=conn.username,
                host=conn.host,
                port=conn.port,
                database=conn.database,
                ssl_enabled=conn.ssl_enabled,
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
            # Delete the associated password secret
            if connection.password_id:
                secret = db.query(Secret).filter(Secret.id == connection.password_id).first()
                if secret:
                    db.delete(secret)

            db.delete(connection)
            db.commit()
            return True
        return False
