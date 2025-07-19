
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine, execute_polars_code
from flowfile_core.schemas.cloud_storage_schemas import (CloudStorageReadSettings,
                                                         CloudStorageReadSettingsInternal,
                                                         FullCloudStorageConnection)
import polars as pl
import pytest
from typing import Dict, Any, Optional
from dataclasses import dataclass
from logging import getLogger

logger = getLogger(__name__)


@dataclass
class S3TestCase:
    """Test case for S3 reading functionality."""
    id: str
    read_settings: CloudStorageReadSettings
    expected_columns: Optional[int] = None
    expected_lazy_records: int = -1
    expected_actual_records: Optional[int] = None
    expected_sample_size: int = 5
    should_fail_on_create: bool = False
    should_fail_on_collect: bool = False
    expected_error: type = Exception


@pytest.fixture
def aws_cli_connection():
    """Reusable AWS CLI connection configuration."""
    return FullCloudStorageConnection(
        connection_name="aws-cli-connection",
        storage_type="s3",
        auth_method="aws-cli",
        aws_region="eu-north-1"
    )


# Bundle test cases with CloudStorageReadSettings
S3_READ_TEST_CASES = [
    S3TestCase(
        id="single_parquet_file",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://eu-north-1-rs-small-data-demo/gold/balance_sheet_data",
            file_format="parquet",
            scan_mode="single_file"
        ),
        expected_columns=9,
        expected_lazy_records=-1,
        expected_actual_records=1534248,
        expected_sample_size=5,
    ),
    S3TestCase(
        id="directory_parquet_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://eu-north-1-rs-small-data-demo/silver/realtime_stock_data/",
            file_format="parquet",
            scan_mode="directory"
        ),
        expected_columns=None,
        expected_lazy_records=-1,
        expected_actual_records=None,
        expected_sample_size=10,
    ),
    S3TestCase(
        id="nested_directory_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://eu-north-1-rs-small-data-demo/raw/interval_stockprices/**/*.parquet",
            file_format="parquet",
            scan_mode="directory"
        ),
    ),
    S3TestCase(
        id="csv_single_file",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://eu-north-1-rs-small-data-demo/landing/nasdaq_screener/nasdaq_screener_1723980768900.csv",
            file_format="csv",
            scan_mode="single_file",
            csv_has_header=True,
            csv_delimiter=",",
            csv_encoding="utf8"
        ),
    ),
    S3TestCase(
        id="csv_directory_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://eu-north-1-rs-small-data-demo/landing/nasdaq_screener/*.csv",
            file_format="csv",
            scan_mode="directory",
            csv_has_header=True,
            csv_delimiter=",",
            csv_encoding="utf8"
        )
    ),
    S3TestCase(
        id="delta_scan",
        read_settings=CloudStorageReadSettings(
            resource_path="s3://eu-north-1-rs-small-data-demo/raw/realtime_stock_prices/",
            file_format="delta",
        ),
        expected_columns=None,
        expected_lazy_records=-1,
        expected_actual_records=None,
        expected_sample_size=10,
    ),
]


@pytest.mark.parametrize("test_case", S3_READ_TEST_CASES, ids=lambda tc: tc.id)
def test_read_from_s3_with_aws_cli(test_case: S3TestCase, aws_cli_connection):
    """Test reading various file formats and configurations from S3."""
    breakpoint()
    # Create settings with the bundled read_settings
    settings = CloudStorageReadSettingsInternal(
        connection=aws_cli_connection,
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


def test_read_parquet_single(scenario: Dict[str, Any]):
    """Test reading a Parquet file from S3 using AWS CLI credentials."""
    breakpoint()
    # Create settings using AWS CLI authentication
    # No access keys needed - will use ~/.aws/credentials
    settings = CloudStorageReadSettingsInternal(
        connection=FullCloudStorageConnection(
            connection_name="aws-cli-connection",
            storage_type="s3",
            auth_method="aws-cli",  # This should use your CLI credentials
            aws_region="eu-north-1"  # Adjust to your region
        ),
        read_settings=CloudStorageReadSettings(
            resource_path="s3://eu-north-1-rs-small-data-demo/gold/balance_sheet_data",  # Adjust path
            file_format="parquet",
            scan_mode="single_file"
        )
    )
    flow_data_engine = FlowDataEngine.from_cloud_storage_obj(settings)
    assert flow_data_engine.schema is not None
    assert len(flow_data_engine.columns) == 9
    assert flow_data_engine.number_of_records == 6_666_666, "The number of columns should be a fictive number"
    assert flow_data_engine.get_number_of_records(force_calculate=True) == 1534248

    sample_data = flow_data_engine.get_sample(5)
    assert sample_data.lazy
    assert "Parquet SCAN" in sample_data.data_frame.explain(), "Should still have predicate pushdown to Remote scan"
    sample_data.lazy = False
    assert sample_data.get_number_of_records() == 5, "Should have the correct number of records after materialization"

