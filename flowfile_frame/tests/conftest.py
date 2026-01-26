import os

os.environ['TESTING'] = 'True'
# Disable worker offloading in tests - these are unit tests that don't need the worker
os.environ['FLOWFILE_OFFLOAD_TO_WORKER'] = '0'

from pydantic import SecretStr

from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_frame.cloud_storage.secret_manager import (
    create_cloud_storage_connection,
    del_cloud_storage_connection,
    get_all_available_cloud_storage_connections,
)


def create_cloud_connection():
    all_cloud_connections = get_all_available_cloud_storage_connections()
    if "minio-flowframe-test" in [connection.connection_name for connection in all_cloud_connections]:
        del_cloud_storage_connection("minio-flowframe-test")
    minio_connection = FullCloudStorageConnection(
        connection_name="minio-flowframe-test",
        storage_type="s3",
        auth_method="access_key",
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
        aws_allow_unsafe_html=True,
        aws_access_key_id="minioadmin",
        aws_secret_access_key=SecretStr("minioadmin")
    )
    create_cloud_storage_connection(minio_connection)


create_cloud_connection()
