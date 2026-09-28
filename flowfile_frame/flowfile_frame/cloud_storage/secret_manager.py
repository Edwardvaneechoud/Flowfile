from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    get_all_cloud_connections_interface,
    store_cloud_connection,
)
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface
from flowfile_frame._identity import current_user_id
from flowfile_frame.notebook import refuse


def get_current_user_id() -> int | None:
    """The user the connection helpers act as (``_identity.current_user_id``)."""
    return current_user_id()


def create_cloud_storage_connection(connection: FullCloudStorageConnection) -> None:
    """
    Create a cloud storage connection using the provided connection details.

    Args:
        connection (FullCloudStorageConnection): The connection details for cloud storage.

    Returns:
        None
    """
    refuse("fl.create_cloud_storage_connection")
    with get_db_context() as db:
        store_cloud_connection(db, connection, current_user_id())


def create_cloud_storage_connection_if_not_exists(connection: FullCloudStorageConnection) -> None:
    """
    Create a cloud storage connection if it does not already exist.

    Args:
        connection (FullCloudStorageConnection): The connection details for cloud storage.

    Returns:
        None
    """
    refuse("fl.create_cloud_storage_connection_if_not_exists")
    all_connections = get_all_available_cloud_storage_connections()
    if not any(conn.connection_name == connection.connection_name for conn in all_connections):
        create_cloud_storage_connection(connection)


def get_all_available_cloud_storage_connections() -> list[FullCloudStorageConnectionInterface]:
    with get_db_context() as db:
        all_connections = get_all_cloud_connections_interface(db, current_user_id())
    return all_connections


def del_cloud_storage_connection(connection_name: str) -> None:
    refuse("fl.del_cloud_storage_connection")
    with get_db_context() as db:
        user_id = current_user_id()
        delete_cloud_connection(db, connection_name, user_id)
