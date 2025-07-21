from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface
from pydantic import SecretStr

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    store_cloud_connection,
    delete_cloud_connection,
    get_all_cloud_connections_interface,
    cloud_connection_interface_from_db_connection)
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection


def cloud_connection():
    """Reusable AWS CLI connection configuration."""
    minio_connection = FullCloudStorageConnection(
        connection_name="minio-test",
        storage_type="s3",  # Use s3, not a separate minio type
        auth_method="access_key",
        aws_access_key_id="minioadmin",
        aws_secret_access_key=SecretStr("minioadmin"),
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
    )
    return minio_connection


def ensure_cloud_storage_connection_is_available_and_get_connection() -> FullCloudStorageConnectionInterface:
    user_id = 1
    with get_db_context() as db:
        all_cloud_connections = get_all_cloud_connections_interface(db, user_id)
        for cs in all_cloud_connections:
            delete_cloud_connection(db, cs.connection_name, user_id)

    with get_db_context() as db:
        return cloud_connection_interface_from_db_connection(
            store_cloud_connection(db, cloud_connection(), user_id=user_id)
        )
