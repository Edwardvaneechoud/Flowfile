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
    from tests.flowfile_core_test_utils import is_docker_available
except ModuleNotFoundError:
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    from flowfile_core_test_utils import is_docker_available

import os

from test_utils.gcs.fixtures import GCS_HOST, GCS_PORT, GCS_ENDPOINT_URL


@pytest.fixture(autouse=True)
def gcs_emulator_env():
    """Set STORAGE_EMULATOR_HOST so Polars' object_store uses the emulator with anonymous auth."""
    original = os.environ.get("STORAGE_EMULATOR_HOST")
    os.environ["STORAGE_EMULATOR_HOST"] = f"{GCS_HOST}:{GCS_PORT}"
    yield
    if original is None:
        os.environ.pop("STORAGE_EMULATOR_HOST", None)
    else:
        os.environ["STORAGE_EMULATOR_HOST"] = original


@pytest.fixture
def gcs_connection():
    """Reusable GCS connection using fake-gcs-server."""
    return FullCloudStorageConnection(
        connection_name="fake-gcs-test",
        storage_type="gcs",
        auth_method="env_vars",
        gcs_project_id="test-project",
        endpoint_url=GCS_ENDPOINT_URL,
    )


@pytest.fixture(scope="module")
def source_flow_data_engine():
    """Provides a source FlowDataEngine with a sample DataFrame."""
    df = FlowDataEngine(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["alpha", "beta", "gamma", "delta", "epsilon"],
            "value": [10.1, 20.2, 30.3, 40.4, 50.5],
            "is_active": [True, False, True, False, True],
        }
    )
    return df


@dataclass
class GCSTestReadCase:
    """Test case for GCS reading functionality."""

    id: str
    read_settings: CloudStorageReadSettings
    expected_columns: int | None = None
    expected_sample_size: int = 5


@dataclass
class GCSTestWriteCase:
    """Test case for GCS writing functionality."""

    id: str
    write_settings: CloudStorageWriteSettings
    expected_columns: int | None = None


GCS_READ_TEST_CASES = [
    GCSTestReadCase(
        id="single_parquet_file",
        read_settings=CloudStorageReadSettings(
            resource_path="gs://test-bucket/single-file-parquet/data.parquet",
            file_format="parquet",
            scan_mode="single_file",
        ),
    ),
    GCSTestReadCase(
        id="directory_parquet_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="gs://test-bucket/multi-file-parquet",
            file_format="parquet",
            scan_mode="directory",
        ),
    ),
    GCSTestReadCase(
        id="csv_single_file",
        read_settings=CloudStorageReadSettings(
            resource_path="gs://test-bucket/single-file-csv/data.csv",
            file_format="csv",
            scan_mode="single_file",
            csv_has_header=True,
            csv_delimiter=",",
            csv_encoding="utf8",
        ),
    ),
    GCSTestReadCase(
        id="csv_directory_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="gs://test-bucket/multi-file-csv",
            file_format="csv",
            scan_mode="directory",
            csv_has_header=True,
            csv_delimiter=",",
            csv_encoding="utf8",
        ),
    ),
    GCSTestReadCase(
        id="single_json_file",
        read_settings=CloudStorageReadSettings(
            resource_path="gs://test-bucket/single-file-json/data.json",
            file_format="json",
            scan_mode="single_file",
        ),
        expected_sample_size=10,
    ),
    GCSTestReadCase(
        id="multi_json_file",
        read_settings=CloudStorageReadSettings(
            resource_path="gs://test-bucket/multi-file-json",
            file_format="json",
            scan_mode="directory",
        ),
        expected_sample_size=10,
    ),
]

GCS_WRITE_TEST_CASES = [
    GCSTestWriteCase(
        id="write_parquet_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="gs://flowfile-test/write_test.parquet",
            file_format="parquet",
            write_mode="overwrite",
            parquet_compression="snappy",
            auth_mode="env_vars",
        ),
        expected_columns=4,
    ),
    GCSTestWriteCase(
        id="write_csv_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="gs://flowfile-test/write_test.csv",
            file_format="csv",
            write_mode="overwrite",
            csv_delimiter="|",
            auth_mode="env_vars",
        ),
        expected_columns=5,
    ),
    GCSTestWriteCase(
        id="write_json_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="gs://flowfile-test/write_test.json",
            file_format="json",
            write_mode="overwrite",
            auth_mode="env_vars",
        ),
        expected_columns=5,
    ),
]


@pytest.mark.skipif(
    not is_docker_available(),
    reason="Docker is not available or not running",
)
@pytest.mark.parametrize("test_case", GCS_READ_TEST_CASES, ids=lambda tc: tc.id)
def test_read_from_gcs(test_case: GCSTestReadCase, gcs_connection):
    """Test reading various file formats from GCS (fake-gcs-server)."""
    settings = CloudStorageReadSettingsInternal(
        connection=gcs_connection,
        read_settings=test_case.read_settings,
    )
    logger.info(f"Testing GCS read scenario: {test_case.id}")
    logger.info(f"Resource path: {test_case.read_settings.resource_path}")

    flow_data_engine = FlowDataEngine.from_cloud_storage_obj(settings)

    assert flow_data_engine is not None
    assert flow_data_engine.lazy is True
    assert flow_data_engine.schema is not None
    assert len(flow_data_engine.columns) > 0, "Should have at least one column"
    assert flow_data_engine.get_number_of_records(force_calculate=True) != 6_666_666


@pytest.mark.skipif(
    not is_docker_available(),
    reason="Docker is not available or not running",
)
@pytest.mark.parametrize("test_case", GCS_WRITE_TEST_CASES, ids=lambda tc: tc.id)
def test_write_to_gcs(
    test_case: GCSTestWriteCase,
    source_flow_data_engine: FlowDataEngine,
    gcs_connection: FullCloudStorageConnection,
):
    """Test writing data to GCS (fake-gcs-server) and verifying by reading back."""
    logger.info(f"--- Running GCS Write Test: {test_case.id} ---")
    logger.info(f"Writing to: {test_case.write_settings.resource_path}")

    now = str(datetime.now())
    output_file = source_flow_data_engine.apply_flowfile_formula(f'"{now}"', "ref_col")
    write_settings_internal = CloudStorageWriteSettingsInternal(
        connection=gcs_connection,
        write_settings=test_case.write_settings,
    )
    output_file.to_cloud_storage_obj(write_settings_internal)

    read_settings = CloudStorageReadSettingsInternal(
        connection=gcs_connection,
        read_settings=CloudStorageReadSettings.model_validate(test_case.write_settings.model_dump()),
    )
    now_vals = (
        FlowDataEngine.from_cloud_storage_obj(read_settings)
        .select_columns(["ref_col"])
        .make_unique(UniqueInput(columns=["ref_col"]))
    ).to_raw_data().data[0]
    assert now in now_vals, "Data did not update"
