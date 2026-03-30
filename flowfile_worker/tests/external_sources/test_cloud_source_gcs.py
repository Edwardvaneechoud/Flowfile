from dataclasses import dataclass
from logging import getLogger

import polars as pl
import pytest

from flowfile_worker.external_sources.s3_source.main import write_df_to_cloud
from flowfile_worker.external_sources.s3_source.models import (
    CloudStorageWriteSettings,
    FullCloudStorageConnection,
    WriteSettings,
)

logger = getLogger(__name__)

try:
    from tests.utils import is_docker_available
except ModuleNotFoundError:
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("flowfile_worker/tests/utils.py")))
    from utils import is_docker_available

from test_utils.gcs.fixtures import GCS_ENDPOINT_URL, get_gcs_client


@pytest.fixture
def gcs_connection_settings() -> FullCloudStorageConnection:
    """Create a GCS connection settings object for fake-gcs-server."""
    return FullCloudStorageConnection(
        connection_name="fake-gcs-worker-test",
        storage_type="gcs",
        auth_method="env_vars",
        gcs_project_id="test-project",
        endpoint_url=GCS_ENDPOINT_URL,
    )


@dataclass
class GCSWorkerWriteCase:
    """Test case for GCS worker writing."""

    id: str
    write_settings: WriteSettings
    object_name: str


GCS_WRITE_TEST_CASES = [
    GCSWorkerWriteCase(
        id="write_parquet_file",
        write_settings=WriteSettings(
            resource_path="gs://worker-test-bucket/write_test.parquet",
            file_format="parquet",
            write_mode="overwrite",
            parquet_compression="snappy",
        ),
        object_name="write_test.parquet",
    ),
    GCSWorkerWriteCase(
        id="write_csv_file",
        write_settings=WriteSettings(
            resource_path="gs://worker-test-bucket/write_test.csv",
            file_format="csv",
            write_mode="overwrite",
            csv_delimiter="|",
        ),
        object_name="write_test.csv",
    ),
    GCSWorkerWriteCase(
        id="write_json_file",
        write_settings=WriteSettings(
            resource_path="gs://worker-test-bucket/write_test.json",
            file_format="json",
            write_mode="overwrite",
        ),
        object_name="write_test.json",
    ),
]


@pytest.mark.skipif(
    not is_docker_available(),
    reason="Docker is not available or not running",
)
@pytest.mark.parametrize("test_case", GCS_WRITE_TEST_CASES, ids=lambda tc: tc.id)
def test_write_df_to_gcs(test_case: GCSWorkerWriteCase, gcs_connection_settings):
    df = pl.LazyFrame(
        {
            "id": range(100),
            "title": [f"Movie_{i}" for i in range(100)],
            "genre": [f"Genre_{i % 5}" for i in range(100)],
        }
    )

    cloud_storage_write_settings = CloudStorageWriteSettings(
        write_settings=test_case.write_settings,
        connection=gcs_connection_settings,
    )

    try:
        write_df_to_cloud(df, cloud_storage_write_settings, logger)

        # Verify the object exists in fake-gcs-server
        client = get_gcs_client()
        bucket = client.bucket("worker-test-bucket")
        blob = bucket.blob(test_case.object_name)
        assert blob.exists(), f"Object '{test_case.object_name}' should exist"
        logger.info(f"Verification successful: Object '{test_case.object_name}' found in fake-gcs-server.")
    except Exception as e:
        logger.error(f"Verification failed: {e!s}")
        raise
