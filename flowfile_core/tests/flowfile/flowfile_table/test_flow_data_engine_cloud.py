from dataclasses import dataclass
from datetime import datetime
from logging import getLogger

import pytest
from pydantic import SecretStr

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageReadSettings,
    CloudStorageReadSettingsInternal,
    CloudStorageWriteSettings,
    CloudStorageWriteSettingsInternal,
    FullCloudStorageConnection,
)
from flowfile_core.schemas.transform_schema import UniqueInput

logger = getLogger(__name__)


try:
    from tests.flowfile_core_test_utils import ensure_password_is_available, is_docker_available
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    # noinspection PyUnresolvedReferences
    from flowfile_core_test_utils import is_docker_available


import os


@pytest.fixture
def s3_env_vars():
    """A pytest fixture to set S3 environment variables for a test."""
    original_vars = {
        key: os.environ.get(key) for key in [
            "AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
            "AWS_REGION", "AWS_ALLOW_HTTP"
        ]
    }

    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["AWS_ALLOW_HTTP"] = "true"
    os.environ["AWS_SESSION_TOKEN"] = ""  # Overwrite so that it is not using AWS-cli credentials
    yield

    for key, value in original_vars.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


@dataclass
class S3TestReadCase:
    """Test case for S3 reading functionality."""
    id: str
    read_settings: CloudStorageReadSettings
    expected_columns: int | None = None
    expected_lazy_records: int = -1
    expected_actual_records: int | None = None
    expected_sample_size: int = 5
    should_fail_on_create: bool = False
    should_fail_on_collect: bool = False
    expected_error: type = Exception


@dataclass
class S3TestWriteCase:
    """Test case for S3 reading functionality."""
    id: str
    write_settings: CloudStorageWriteSettings
    expected_columns: int | None = None
    expected_lazy_records: int = -1


@pytest.fixture(scope="module")
def source_flow_data_engine():
    """
    Provides a source FlowDataEngine with a sample DataFrame.
    This is created once per module to be used as the source for all write tests.
    """
    df = FlowDataEngine({
        "id": [1, 2, 3, 4, 5],
        "name": ["alpha", "beta", "gamma", "delta", "epsilon"],
        "value": [10.1, 20.2, 30.3, 40.4, 50.5],
        "is_active": [True, False, True, False, True]
    })
    return df


@pytest.fixture
def aws_access_key_connection():
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


# Bundle test cases with CloudStorageReadSettings
S3_READ_TEST_CASES = [
    S3TestReadCase(
        id="single_parquet_file",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://test-bucket/single-file-parquet/data.parquet",
            file_format="parquet",
            scan_mode="single_file"
        ),
    ),
    S3TestReadCase(
        id="directory_parquet_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://test-bucket/multi-file-parquet",
            file_format="parquet",
            scan_mode="directory"
        ),
    ),
    S3TestReadCase(
        id="nested_directory_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://test-bucket/multi-file-parquet/**/*.parquet",
            file_format="parquet",
            scan_mode="directory"
        ),
    ),
    S3TestReadCase(
        id="csv_single_file",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://test-bucket/single-file-csv/data.csv",
            file_format="csv",
            scan_mode="single_file",
            csv_has_header=True,
            csv_delimiter=",",
            csv_encoding="utf8"
        ),
    ),
    S3TestReadCase(
        id="csv_directory_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://test-bucket/multi-file-csv",
            file_format="csv",
            scan_mode="directory",
            csv_has_header=True,
            csv_delimiter=",",
            csv_encoding="utf8"
        )
    ),
    S3TestReadCase(
        id="single_json_file",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://test-bucket/single-file-json/data.json",
            file_format="json",
            scan_mode="single_file",
        ),
        expected_columns=None,
        expected_lazy_records=-1,
        expected_actual_records=None,
        expected_sample_size=10,
    ),
    S3TestReadCase(
        id="multi_json_file",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://test-bucket/multi-file-json",
            file_format="json",
            scan_mode="directory",
        ),
        expected_columns=None,
        expected_lazy_records=-1,
        expected_actual_records=None,
        expected_sample_size=10,
    ),

    S3TestReadCase(
        id="delta_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://test-bucket/delta-lake-table",
            file_format="delta",
        ),
        expected_columns=None,
        expected_lazy_records=-1,
        expected_actual_records=None,
        expected_sample_size=10,
    ),
]

S3_WRITE_TEST_CASES = [
    S3TestWriteCase(
        id="write_parquet_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="s3://flowfile-test/write_test.parquet",
            file_format="parquet",
            write_mode="overwrite",
            parquet_compression="snappy",
            auth_mode="aws-cli"
        ),
        expected_columns=4,
    ),
    S3TestWriteCase(
        id="write_csv_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="s3://flowfile-test/write_test.csv",
            file_format="csv",
            write_mode="overwrite",
            csv_delimiter="|",
            auth_mode="aws-cli"
        ),
        expected_columns=5,
    ),
    S3TestWriteCase(
        id="write_json_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="s3://flowfile-test/write_test.json",
            file_format="json",
            write_mode="overwrite",
            auth_mode="aws-cli"
        ),
        expected_columns=5,
    ),
    S3TestWriteCase(
        id="overwrite_delta",
        write_settings=CloudStorageWriteSettings(
            resource_path="s3://flowfile-test/write_test_delta",
            file_format="delta",
            write_mode="overwrite",
            auth_mode="aws-cli"
        ),
        expected_columns=5,
    ),
    S3TestWriteCase(
        id="append_delta_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="s3://flowfile-test/write_test_append",
            file_format="delta",
            write_mode="append",
            auth_mode="aws-cli"
        ),
        expected_columns=5,
    ),

]

@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database connection cannot be established")
@pytest.mark.parametrize("test_case", S3_READ_TEST_CASES, ids=lambda tc: tc.id)
def test_read_from_s3_with_aws_keys(test_case: S3TestReadCase, aws_access_key_connection):
    """Test reading various file formats and configurations from S3."""
    if test_case.read_settings.file_format == "delta":
        aws_access_key_connection.aws_allow_unsafe_html = True
    # Create settings with the bundled read_settings
    settings = CloudStorageReadSettingsInternal(
        connection=aws_access_key_connection,
        read_settings=test_case.read_settings
    )
    # Log test details
    logger.info(f"Testing scenario: {test_case.id}")
    logger.info(f"Resource path: {test_case.read_settings.resource_path}")
    logger.info(f"File format: {test_case.read_settings.file_format}")
    # Create FlowDataEngine

    flow_data_engine = FlowDataEngine.from_cloud_storage_obj(settings)
    # Basic assertions
    assert flow_data_engine is not None
    assert flow_data_engine.lazy is True
    assert flow_data_engine.schema is not None
    assert len(flow_data_engine.columns) > 0, "Should have at least one column"
    assert flow_data_engine.get_number_of_records(force_calculate=True) != 6_666_666


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
@pytest.mark.parametrize("test_case", S3_WRITE_TEST_CASES, ids=lambda tc: tc.id)
def test_write_to_s3_with_aws_keys(
        test_case: S3TestWriteCase,
        source_flow_data_engine: FlowDataEngine,
        aws_access_key_connection: FullCloudStorageConnection
):
    """
    Tests writing data to S3 and verifies the output by reading it back.
    """
    logger.info(f"--- Running S3 Write Test: {test_case.id} ---")
    logger.info(f"Writing to: {test_case.write_settings.resource_path}")
    added_values: list[str] = []
    if test_case.write_settings.file_format == "delta":
        aws_access_key_connection.aws_allow_unsafe_html = True
    for i in range(5 if test_case.write_settings.write_mode == 'append' else 1):
        now = str(datetime.now())
        added_values.append(now)
        output_file = source_flow_data_engine.apply_flowfile_formula(f'"{now}"', "ref_col")
        write_settings_internal = CloudStorageWriteSettingsInternal(
            connection=aws_access_key_connection,
            write_settings=test_case.write_settings
        )
        output_file.to_cloud_storage_obj(write_settings_internal)
    read_settings = CloudStorageReadSettingsInternal(
        connection=aws_access_key_connection,
        read_settings=CloudStorageReadSettings.model_validate(test_case.write_settings.model_dump()))
    now_vals = (FlowDataEngine.from_cloud_storage_obj(read_settings).select_columns(["ref_col"])
                .make_unique(UniqueInput(columns=["ref_col"]))).to_raw_data().data[0]
    for now_value in added_values:
        assert now_value in now_vals, "Data did not update"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
@pytest.mark.parametrize("test_case", S3_WRITE_TEST_CASES, ids=lambda tc: tc.id)
def test_write_to_s3_with_aws_env_vars(
        test_case: S3TestWriteCase,
        source_flow_data_engine: FlowDataEngine,
        s3_env_vars: dict[str, str],
):
    """
    Tests writing data to S3 and verifies the output by reading it back.
    """
    logger.info(f"--- Running S3 Write Test: {test_case.id} ---")
    logger.info(f"Writing to: {test_case.write_settings.resource_path}")
    added_values: list[str] = []
    connection = FullCloudStorageConnection(
        connection_name="minio-test-env-vars",
        storage_type="s3",
        auth_method="env_vars",
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000"
    )
    test_case.write_settings.auth_mode = "env_vars"
    if test_case.write_settings.file_format == "delta":
        connection.aws_allow_unsafe_html = True
    for i in range(5 if test_case.write_settings.write_mode == 'append' else 1):
        now = str(datetime.now())
        added_values.append(now)
        output_file = source_flow_data_engine.apply_flowfile_formula(f'"{now}"', "ref_col")
        write_settings_internal = CloudStorageWriteSettingsInternal(
            connection=connection,
            write_settings=test_case.write_settings
        )
        output_file.to_cloud_storage_obj(write_settings_internal)
    read_settings = CloudStorageReadSettingsInternal(
        connection=connection,
        read_settings=CloudStorageReadSettings.model_validate(test_case.write_settings.model_dump()))
    now_vals = (FlowDataEngine.from_cloud_storage_obj(read_settings).select_columns(["ref_col"])
                .make_unique(UniqueInput(columns=["ref_col"]))).to_raw_data().data[0]
    for now_value in added_values:
        assert now_value in now_vals, "Data did not update"


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
@pytest.mark.parametrize("test_case", S3_READ_TEST_CASES, ids=lambda tc: tc.id)
def test_read_from_s3_with_env_vars(test_case: S3TestReadCase, s3_env_vars):
    """
    Tests reading various file formats from S3 using environment variables for authentication.
    """
    # Create a connection object that relies on environment variables
    connection = FullCloudStorageConnection(
        connection_name="minio-test-env-vars",
        storage_type="s3",
        auth_method="env_vars",
        aws_region="us-east-1",
        endpoint_url="http://localhost:9000"
    )

    if test_case.read_settings.file_format == "delta":
        connection.aws_allow_unsafe_html = True
    test_case.read_settings.auth_mode = "env_vars"
    settings = CloudStorageReadSettingsInternal(
        connection=connection,
        read_settings=test_case.read_settings
    )

    logger.info(f"Testing scenario with env vars: {test_case.id}")
    logger.info(f"Resource path: {test_case.read_settings.resource_path}")

    flow_data_engine = FlowDataEngine.from_cloud_storage_obj(settings)
    assert flow_data_engine is not None
    assert flow_data_engine.lazy is True
    assert flow_data_engine.schema is not None
    assert len(flow_data_engine.columns) > 0, "Should have at least one column"
    # Note: You may want to adjust this assertion to be more specific if possible
    assert flow_data_engine.get_number_of_records(force_calculate=True) != 6_666_666


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running")
def test_read_parquet_single():
    """Test reading a Parquet file from S3 using AWS CLI credentials."""
    # Create settings using AWS CLI authentication
    # No access keys needed - will use ~/.aws/credentials
    settings = CloudStorageReadSettingsInternal(
        connection=FullCloudStorageConnection(
            connection_name="minio-test",
            storage_type="s3",
            auth_method="access_key",
            aws_access_key_id="minioadmin",
            aws_secret_access_key=SecretStr("minioadmin"),
            aws_region="us-east-1",
            endpoint_url="http://localhost:9000",
        ),
        read_settings=CloudStorageReadSettings(
            resource_path="s3://test-bucket/single-file-parquet/data.parquet",
            file_format="parquet",
            scan_mode="single_file"
        )
    )
    flow_data_engine = FlowDataEngine.from_cloud_storage_obj(settings)
    assert flow_data_engine.schema is not None
    assert len(flow_data_engine.columns) == 4
    assert flow_data_engine.number_of_records == 6_666_666, "The number of columns should be a fictive number"
    assert flow_data_engine.get_number_of_records(force_calculate=True) == 100000

    sample_data = flow_data_engine.get_sample(5)
    assert sample_data.lazy
    assert "Parquet SCAN" in sample_data.data_frame.explain(), "Should still have predicate pushdown to Remote scan"
    sample_data.lazy = False
    assert sample_data.get_number_of_records() == 5, "Should have the correct number of records after materialization"
