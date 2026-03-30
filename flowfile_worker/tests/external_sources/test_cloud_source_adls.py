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
from flowfile_worker.secrets import encrypt_secret

logger = getLogger(__name__)

try:
    from tests.utils import is_docker_available
except ModuleNotFoundError:
    import os
    import sys

    sys.path.append(os.path.dirname(os.path.abspath("flowfile_worker/tests/utils.py")))
    from utils import is_docker_available

from test_utils.azurite.fixtures import (
    AZURITE_ACCOUNT_KEY,
    AZURITE_ACCOUNT_NAME,
    AZURITE_BLOB_PORT,
    AZURITE_HOST,
    get_blob_service_client,
)


@pytest.fixture
def adls_connection_settings() -> FullCloudStorageConnection:
    """Create an ADLS connection settings object for Azurite."""
    encrypted_key = encrypt_secret(AZURITE_ACCOUNT_KEY)
    from pydantic import SecretStr

    return FullCloudStorageConnection(
        connection_name="azurite-worker-test",
        storage_type="adls",
        auth_method="access_key",
        azure_account_name=AZURITE_ACCOUNT_NAME,
        azure_account_key=SecretStr(encrypted_key),
        endpoint_url=f"http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}",
    )


@dataclass
class ADLSWorkerWriteCase:
    """Test case for ADLS worker writing."""

    id: str
    write_settings: WriteSettings
    blob_name: str


ADLS_WRITE_TEST_CASES = [
    ADLSWorkerWriteCase(
        id="write_parquet_file",
        write_settings=WriteSettings(
            resource_path="az://worker-test-container/write_test.parquet",
            file_format="parquet",
            write_mode="overwrite",
            parquet_compression="snappy",
        ),
        blob_name="write_test.parquet",
    ),
    ADLSWorkerWriteCase(
        id="write_csv_file",
        write_settings=WriteSettings(
            resource_path="az://worker-test-container/write_test.csv",
            file_format="csv",
            write_mode="overwrite",
            csv_delimiter="|",
        ),
        blob_name="write_test.csv",
    ),
    ADLSWorkerWriteCase(
        id="write_json_file",
        write_settings=WriteSettings(
            resource_path="az://worker-test-container/write_test.json",
            file_format="json",
            write_mode="overwrite",
        ),
        blob_name="write_test.json",
    ),
]


@pytest.mark.skipif(
    not is_docker_available(),
    reason="Docker is not available or not running",
)
@pytest.mark.parametrize("test_case", ADLS_WRITE_TEST_CASES, ids=lambda tc: tc.id)
def test_write_df_to_adls(test_case: ADLSWorkerWriteCase, adls_connection_settings):
    df = pl.LazyFrame(
        {
            "id": range(100),
            "title": [f"Movie_{i}" for i in range(100)],
            "genre": [f"Genre_{i % 5}" for i in range(100)],
        }
    )

    cloud_storage_write_settings = CloudStorageWriteSettings(
        write_settings=test_case.write_settings,
        connection=adls_connection_settings,
    )

    try:
        write_df_to_cloud(df, cloud_storage_write_settings, logger)

        # Verify the blob exists in Azurite
        blob_service = get_blob_service_client()
        blob_client = blob_service.get_blob_client(
            container="worker-test-container", blob=test_case.blob_name
        )
        properties = blob_client.get_blob_properties()
        assert properties.size > 0, "Blob should not be empty"
        logger.info(f"Verification successful: Blob '{test_case.blob_name}' found in Azurite.")
    except Exception as e:
        logger.error(f"Verification failed: {e!s}")
        raise
