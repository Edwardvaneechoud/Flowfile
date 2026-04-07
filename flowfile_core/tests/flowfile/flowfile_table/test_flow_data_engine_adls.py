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

from test_utils.azurite.fixtures import (
    AZURITE_ACCOUNT_KEY,
    AZURITE_ACCOUNT_NAME,
    AZURITE_BLOB_PORT,
    AZURITE_HOST,
    is_azurite_available,
)


@pytest.fixture
def adls_access_key_connection():
    """Reusable ADLS connection using Azurite emulator with access key auth."""
    return FullCloudStorageConnection(
        connection_name="azurite-test",
        storage_type="adls",
        auth_method="access_key",
        azure_account_name=AZURITE_ACCOUNT_NAME,
        azure_account_key=SecretStr(AZURITE_ACCOUNT_KEY),
        endpoint_url=f"http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}",
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
class ADLSTestReadCase:
    """Test case for ADLS reading functionality."""

    id: str
    read_settings: CloudStorageReadSettings
    expected_columns: int | None = None
    expected_lazy_records: int = -1
    expected_actual_records: int | None = None
    expected_sample_size: int = 5


@dataclass
class ADLSTestWriteCase:
    """Test case for ADLS writing functionality."""

    id: str
    write_settings: CloudStorageWriteSettings
    expected_columns: int | None = None


ADLS_READ_TEST_CASES = [
    ADLSTestReadCase(
        id="single_parquet_file",
        read_settings=CloudStorageReadSettings(
            resource_path="az://test-container/single-file-parquet/data.parquet",
            file_format="parquet",
            scan_mode="single_file",
        ),
    ),
    ADLSTestReadCase(
        id="directory_parquet_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="az://test-container/multi-file-parquet",
            file_format="parquet",
            scan_mode="directory",
        ),
    ),
    ADLSTestReadCase(
        id="csv_single_file",
        read_settings=CloudStorageReadSettings(
            resource_path="az://test-container/single-file-csv/data.csv",
            file_format="csv",
            scan_mode="single_file",
            csv_has_header=True,
            csv_delimiter=",",
            csv_encoding="utf8",
        ),
    ),
    ADLSTestReadCase(
        id="csv_directory_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="az://test-container/multi-file-csv",
            file_format="csv",
            scan_mode="directory",
            csv_has_header=True,
            csv_delimiter=",",
            csv_encoding="utf8",
        ),
    ),
    ADLSTestReadCase(
        id="single_json_file",
        read_settings=CloudStorageReadSettings(
            resource_path="az://test-container/single-file-json/data.json",
            file_format="json",
            scan_mode="single_file",
        ),
        expected_sample_size=10,
    ),
    ADLSTestReadCase(
        id="multi_json_file",
        read_settings=CloudStorageReadSettings(
            resource_path="az://test-container/multi-file-json",
            file_format="json",
            scan_mode="directory",
        ),
        expected_sample_size=10,
    ),
    ADLSTestReadCase(
        id="delta_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="az://test-container/delta-lake-table",
            file_format="delta",
        ),
        expected_sample_size=10,
    ),
]

ADLS_WRITE_TEST_CASES = [
    ADLSTestWriteCase(
        id="write_parquet_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="az://flowfile-test/write_test.parquet",
            file_format="parquet",
            write_mode="overwrite",
            parquet_compression="snappy",
            auth_mode="access_key",
        ),
        expected_columns=4,
    ),
    ADLSTestWriteCase(
        id="write_csv_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="az://flowfile-test/write_test.csv",
            file_format="csv",
            write_mode="overwrite",
            csv_delimiter="|",
            auth_mode="access_key",
        ),
        expected_columns=5,
    ),
    ADLSTestWriteCase(
        id="write_json_file",
        write_settings=CloudStorageWriteSettings(
            resource_path="az://flowfile-test/write_test.json",
            file_format="json",
            write_mode="overwrite",
            auth_mode="access_key",
        ),
        expected_columns=5,
    ),
    ADLSTestWriteCase(
        id="overwrite_delta",
        write_settings=CloudStorageWriteSettings(
            resource_path="az://flowfile-test/write_test_delta",
            file_format="delta",
            write_mode="overwrite",
            auth_mode="access_key",
        ),
        expected_columns=5,
    ),
]


@pytest.mark.skipif(
    not is_azurite_available(),
    reason="Azurite emulator is not running or has no test data",
)
@pytest.mark.parametrize("test_case", ADLS_READ_TEST_CASES, ids=lambda tc: tc.id)
def test_read_from_adls_with_access_key(test_case: ADLSTestReadCase, adls_access_key_connection):
    """Test reading various file formats from ADLS (Azurite) using access key auth."""
    settings = CloudStorageReadSettingsInternal(
        connection=adls_access_key_connection,
        read_settings=test_case.read_settings,
    )
    logger.info(f"Testing ADLS read scenario: {test_case.id}")
    logger.info(f"Resource path: {test_case.read_settings.resource_path}")
    flow_data_engine = FlowDataEngine.from_cloud_storage_obj(settings)

    assert flow_data_engine is not None
    assert flow_data_engine.lazy is True
    assert flow_data_engine.schema is not None
    assert len(flow_data_engine.columns) > 0, "Should have at least one column"
    assert flow_data_engine.get_number_of_records(force_calculate=True) != 6_666_666


@pytest.mark.skipif(
    not is_azurite_available(),
    reason="Azurite emulator is not running or has no test data",
)
@pytest.mark.parametrize("test_case", ADLS_WRITE_TEST_CASES, ids=lambda tc: tc.id)
def test_write_to_adls_with_access_key(
    test_case: ADLSTestWriteCase,
    source_flow_data_engine: FlowDataEngine,
    adls_access_key_connection: FullCloudStorageConnection,
):
    """Test writing data to ADLS (Azurite) and verifying by reading back."""
    logger.info(f"--- Running ADLS Write Test: {test_case.id} ---")
    logger.info(f"Writing to: {test_case.write_settings.resource_path}")
    now = str(datetime.now())
    output_file = source_flow_data_engine.apply_flowfile_formula(f'"{now}"', "ref_col")
    write_settings_internal = CloudStorageWriteSettingsInternal(
        connection=adls_access_key_connection,
        write_settings=test_case.write_settings,
    )
    output_file.to_cloud_storage_obj(write_settings_internal)

    read_settings = CloudStorageReadSettingsInternal(
        connection=adls_access_key_connection,
        read_settings=CloudStorageReadSettings.model_validate(test_case.write_settings.model_dump()),
    )
    now_vals = (
        FlowDataEngine.from_cloud_storage_obj(read_settings)
        .select_columns(["ref_col"])
        .make_unique(UniqueInput(columns=["ref_col"]))
    ).to_raw_data().data[0]
    assert now in now_vals, "Data did not update"
