
import platform
import subprocess
from pathlib import Path

import pytest
from pydantic import SecretStr

from flowfile_worker.external_sources.s3_source.models import (
    FullCloudStorageConnection,
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

def find_parent_directory(target_dir_name, start_path=None):
    """Navigate up directories until finding the target directory"""
    current_path = Path(start_path) if start_path else Path.cwd()

    while current_path != current_path.parent:
        if current_path.name == target_dir_name:
            return current_path
        if current_path.name == target_dir_name:
            return current_path
        current_path = current_path.parent

    raise FileNotFoundError(f"Directory '{target_dir_name}' not found")


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
