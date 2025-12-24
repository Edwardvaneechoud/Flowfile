from pydantic import SecretStr

from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    cloud_connection_interface_from_db_connection,
    delete_cloud_connection,
    get_all_cloud_connections_interface,
    store_cloud_connection,
)
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection, FullCloudStorageConnectionInterface


def get_cloud_connection():
    """Reusable AWS CLI connection configuration."""
    minio_connection = FullCloudStorageConnection(
        connection_name="minio-test",
        storage_type="s3",  # Use s3, not a separate minio type
        auth_method="access_key",
        aws_access_key_id="minioadmin",
        aws_secret_access_key=SecretStr("minioadmin"),
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
        aws_allow_unsafe_html=True,
    )
    return minio_connection


def ensure_no_cloud_storage_connection_is_available(user_id: int) -> None:
    with get_db_context() as db:
        all_cloud_connections = get_all_cloud_connections_interface(db, user_id)
        for cs in all_cloud_connections:
            delete_cloud_connection(db, cs.connection_name, user_id)


def ensure_cloud_storage_connection_is_available_and_get_connection(user_id: int = 1) -> FullCloudStorageConnectionInterface:
    ensure_no_cloud_storage_connection_is_available(user_id)
    with get_db_context() as db:
        return cloud_connection_interface_from_db_connection(
            store_cloud_connection(db, get_cloud_connection(), user_id=user_id)
        )
