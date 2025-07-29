
import platform
import subprocess

import pytest
from pydantic import SecretStr

from flowfile_worker.external_sources.s3_source.models import (FullCloudStorageConnection,
                                                               )
from flowfile_worker.secrets import encrypt_secret


def is_docker_available():
    """Check if Docker is running."""
    if platform.system() == "Windows":
        return False
    try:
        subprocess.run(["docker", "info"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


@pytest.fixture
def cloud_storage_connection_settings() -> FullCloudStorageConnection:
    """Create a cloud storage connection settings object."""

    aws_access_secret = encrypt_secret('minioadmin')
    minio_connection = FullCloudStorageConnection(
        connection_name="minio-test",
        storage_type="s3",  # Use s3, not a separate minio type
        auth_method="access_key",
        aws_access_key_id="minioadmin",
        aws_secret_access_key=SecretStr(aws_access_secret),
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000",
    )
    return minio_connection