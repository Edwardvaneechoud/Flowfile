import atexit
import os
import shutil
import tempfile
from pathlib import Path

os.environ['TESTING'] = 'True'

# The storage singleton caches its base directory at import; keep registered flows out of ~/.flowfile.
if 'FLOWFILE_STORAGE_DIR' not in os.environ:
    os.environ['FLOWFILE_STORAGE_DIR'] = tempfile.mkdtemp(prefix='flowfile_frame_tests_')
    atexit.register(shutil.rmtree, os.environ['FLOWFILE_STORAGE_DIR'], ignore_errors=True)

# Keep jwt_secret / master_key / internal_token out of the developer's real
# ~/.config/flowfile store (flowfile_frame imports flowfile_core in-process).
os.environ.setdefault(
    'FLOWFILE_SECURE_STORAGE_PATH',
    str(Path(tempfile.gettempdir()) / 'flowfile_test_secure_storage'),
)

import pytest
from pydantic import SecretStr

from flowfile_core.configs import node_store
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from flowfile_frame.cloud_storage.secret_manager import (
    create_cloud_storage_connection,
    del_cloud_storage_connection,
    get_all_available_cloud_storage_connections,
)

pytest.register_assert_rewrite(f"{__package__}.native_helpers")


@pytest.fixture
def store_snapshot():
    """Placing a class outside notebook mode registers it process-wide; restore the store after the test."""
    saved_overrides = dict(node_store.CUSTOM_NODE_STORE._overrides)
    saved_dict = dict(node_store.node_dict)
    saved_list = list(node_store.nodes_list)
    yield
    node_store.CUSTOM_NODE_STORE.clear()
    node_store.CUSTOM_NODE_STORE.update(saved_overrides)
    node_store.node_dict.clear()
    node_store.node_dict.update(saved_dict)
    node_store.nodes_list[:] = saved_list


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
