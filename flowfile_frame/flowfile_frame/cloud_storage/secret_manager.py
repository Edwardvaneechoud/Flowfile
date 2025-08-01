from typing import List

from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface
from flowfile_core.flowfile.database_connection_manager.db_connections import (store_cloud_connection,
                                                                               get_all_cloud_connections_interface,
                                                                               delete_cloud_connection)
from flowfile_core.database.connection import get_db_context
from flowfile_core.auth.jwt import  get_current_user_sync, create_access_token
from asyncio import run


def get_current_user_id() -> int | None:
    access_token = create_access_token(data={"sub": "local_user"})
    with get_db_context() as db:
        current_user_id = get_current_user_sync(
            access_token,
            db
        ).id
    return current_user_id


def create_cloud_storage_connection(connection: FullCloudStorageConnection) -> None:
    """
    Create a cloud storage connection using the provided connection details.

    Args:
        connection (FullCloudStorageConnection): The connection details for cloud storage.

    Returns:
        None
    """
    access_token = create_access_token(data={"sub": "local_user"})

    with get_db_context() as db:
        current_user_id = get_current_user_sync(
            access_token,
            db
        ).id
        store_cloud_connection(
            db,
            connection,
            current_user_id
        )


def create_cloud_storage_connection_if_not_exists(connection: FullCloudStorageConnection) -> None:
    """
    Create a cloud storage connection if it does not already exist.

    Args:
        connection (FullCloudStorageConnection): The connection details for cloud storage.

    Returns:
        None
    """
    all_connections = get_all_available_cloud_storage_connections()
    if not any(conn.connection_name == connection.connection_name for conn in all_connections):
        create_cloud_storage_connection(connection)


def get_all_available_cloud_storage_connections() -> List[FullCloudStorageConnectionInterface]:
    with get_db_context() as db:
        all_connections = get_all_cloud_connections_interface(
            db,
            get_current_user_id()
        )
    return all_connections


def del_cloud_storage_connection(connection_name: str) -> None:
    with get_db_context() as db:
        user_id = get_current_user_id()
        delete_cloud_connection(db, connection_name, user_id)
