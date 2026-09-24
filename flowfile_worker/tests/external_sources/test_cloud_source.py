import os
import uuid
from dataclasses import dataclass
from logging import getLogger

import polars as pl
import pytest
from deltalake import DeltaTable
from pydantic import ValidationError

from flowfile_worker.external_sources.s3_source.main import write_df_to_cloud
from flowfile_worker.external_sources.s3_source.models import (
    CloudStorageWriteSettings,
    FullCloudStorageConnection,
    WriteSettings,
)

logger = getLogger(__name__)


try:
    # noinspection PyUnresolvedReferences
    from test_utils.s3.fixtures import get_minio_client
    from tests.utils import cloud_storage_connection_settings, is_docker_available
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_worker/tests/utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    # noinspection PyUnresolvedReferences
    from utils import is_docker_available

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
        assert int(response['ContentLength']) > 0
        logger.info(f"✅ Verification successful: Object '{test_case.write_settings.resource_path}' found in bucket worker-test-bucket.")
    except Exception as e:
        logger.error(f"❌ Verification failed: {str(e)}")
        raise e


@pytest.mark.parametrize("path", ["", "   ", "output_folder/table"])
def test_write_settings_reject_paths_that_resolve_to_the_worker_cwd(path):
    with pytest.raises(ValidationError, match="Cloud storage writer has no target path|is not a URI"):
        WriteSettings(resource_path=path, file_format="delta", write_mode="append")


@pytest.mark.parametrize("path", ["s3://bucket/table", "az://container/table", "/abs/local/table"])
def test_write_settings_accept_cloud_uris_and_absolute_paths(path):
    assert WriteSettings(resource_path=path).resource_path == path


@pytest.fixture
def minio_aws_cli_profile(monkeypatch, tmp_path):
    """Static-key default profile for MinIO in temp AWS files, every ambient AWS_* variable removed.

    The node-level "No connection" carries no endpoint of its own, so MinIO is reached through AWS_ENDPOINT_URL.
    """
    for key in list(os.environ):
        if key.startswith("AWS_"):
            monkeypatch.delenv(key)
    (tmp_path / "credentials").write_text(
        "[default]\naws_access_key_id = minioadmin\naws_secret_access_key = minioadmin\n"
    )
    (tmp_path / "config").write_text("[default]\nregion = us-east-1\n")
    (tmp_path / "boto.cfg").write_text("")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    monkeypatch.setenv("BOTO_CONFIG", str(tmp_path / "boto.cfg"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.setenv("AWS_ALLOW_HTTP", "true")


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available so MinIO cannot be reached")
def test_write_partitioned_delta_with_aws_cli_connection(minio_aws_cli_profile):
    """The UI's "No connection" writer: aws-cli auth from a static-key profile, delta append partitioned.

    Runs in-process because the spawned worker's environment is fixed at session start.
    """
    bucket, prefix = "worker-test-bucket", f"aws_cli_partitioned_{uuid.uuid4().hex[:8]}"
    s3_client = get_minio_client()
    try:
        s3_client.create_bucket(Bucket=bucket)
    except Exception:
        pass
    settings = CloudStorageWriteSettings(
        write_settings=WriteSettings(
            resource_path=f"s3://{bucket}/{prefix}",
            file_format="delta",
            write_mode="append",
            partition_by=["output_field"],
        ),
        connection=FullCloudStorageConnection(storage_type="s3", auth_method="aws-cli"),
    )
    df = pl.LazyFrame({"id": [1, 2, 3], "category": ["a", None, "na"], "output_field": ["test", "test", "other"]})
    storage_options = {
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "aws_region": "us-east-1",
        "endpoint_url": "http://localhost:9000",
        "aws_allow_http": "true",
    }
    try:
        write_df_to_cloud(df, settings, logger)
        write_df_to_cloud(df, settings, logger)
        table = DeltaTable(f"s3://{bucket}/{prefix}", storage_options=storage_options)
        assert table.version() == 1
        assert table.metadata().partition_columns == ["output_field"]
        assert pl.scan_delta(f"s3://{bucket}/{prefix}", storage_options=storage_options).collect().height == 6
    finally:
        listed = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/")
        keys = [{"Key": obj["Key"]} for obj in listed.get("Contents", [])]
        if keys:
            s3_client.delete_objects(Bucket=bucket, Delete={"Objects": keys})

