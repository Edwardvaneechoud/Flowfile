import polars as pl
from logging import getLogger
import pytest
from dataclasses import dataclass
from typing import Optional

from flowfile_worker.secrets import encrypt_secret

from flowfile_worker.external_sources.s3_source.models import (CloudStorageWriteSettings,
                                                               CloudStorageType,
                                                               FullCloudStorageConnection,
                                                               WriteSettings,
                                                               )
from flowfile_worker.external_sources.s3_source.main import write_df_to_cloud
from pydantic import SecretStr

logger = getLogger(__name__)


try:
    # noinspection PyUnresolvedReferences
    from tests.utils import is_docker_available
    from test_utils.s3.fixtures import get_minio_client
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_worker/tests/utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    # noinspection PyUnresolvedReferences
    from utils import (is_docker_available)
    from test_utils.s3.fixtures import get_minio_client


@dataclass
class S3TestWriteCase:
    """Test case for S3 reading functionality."""
    id: str
    write_settings: WriteSettings
    file_name: str


S3_WRITE_TEST_CASES = [
    S3TestWriteCase(
        id="write_parquet_file",
        write_settings=WriteSettings(
            resource_path="s3://worker-test-bucket/write_test.parquet",
            file_format="parquet",
            write_mode="overwrite",
            parquet_compression="snappy",
        ),
        file_name="write_test.parquet"
    ),
    S3TestWriteCase(
        id="write_csv_file",
        write_settings=WriteSettings(
            resource_path="s3://worker-test-bucket/write_test.csv",
            file_format="csv",
            write_mode="overwrite",
            csv_delimiter="|",
        ),
        file_name="write_test.csv"
    ),
    S3TestWriteCase(
        id="write_json_file",
        write_settings=WriteSettings(
            resource_path="s3://worker-test-bucket/write_test.json",
            file_format="json",
            write_mode="overwrite",
        ),
        file_name="write_test.json"
    ),
    S3TestWriteCase(
        id="overwrite_delta",
        write_settings=WriteSettings(
            resource_path="s3://worker-test-bucket/write_test_delta",
            file_format="delta",
            write_mode="overwrite",
        ),
        file_name="write_test_delta/_delta_log/00000000000000000000.json"
    ),
    S3TestWriteCase(
        id="append_delta",
        write_settings=WriteSettings(
            resource_path="s3://worker-test-bucket/write_test_delta",
            file_format="delta",
            write_mode="append",
        ),
        file_name="write_test_delta/_delta_log/00000000000000000001.json"
    ),
]
@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database connection cannot be established")
@pytest.mark.parametrize("test_case", S3_WRITE_TEST_CASES, ids=lambda tc: tc.id)
def test_write_df_to_cloud_storage(test_case: S3TestWriteCase,
                                   cloud_storage_connection_settings):
    df = pl.LazyFrame({
        'id': range(100),
        'title': [f'Movie_{i}' for i in range(100)],
        'genre': [f'Genre_{i % 5}' for i in range(100)]
    })
    if test_case.write_settings.file_format == "delta":
        cloud_storage_connection_settings.aws_allow_unsafe_html = True

    cloud_storage_write_settings = CloudStorageWriteSettings(
        write_settings=test_case.write_settings,
        connection=cloud_storage_connection_settings,
    )
    s3_client = get_minio_client()
    try:
        write_df_to_cloud(df, cloud_storage_write_settings, logger)
        response = s3_client.head_object(Bucket="worker-test-bucket", Key=test_case.file_name)
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        assert int(response['ContentLength']) > 0  # Ensure the file is not empty
        logger.info(f"✅ Verification successful: Object '{test_case.write_settings.resource_path}' found in bucket worker-test-bucket.")
    except Exception as e:
        logger.error(f"❌ Verification failed: {str(e)}")
        raise e
