import io
import logging

import polars as pl
import pyarrow as pa
from azure.storage.blob import BlobServiceClient
from deltalake import write_deltalake

from test_utils.azurite.fixtures import (
    AZURITE_ACCOUNT_KEY,
    AZURITE_ACCOUNT_NAME,
    AZURITE_BLOB_PORT,
    AZURITE_CONNECTION_STRING,
    AZURITE_HOST,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _upload_blob(client: BlobServiceClient, container: str, blob_name: str, data: bytes):
    blob_client = client.get_blob_client(container=container, blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)


def _create_single_csv_file(client: BlobServiceClient, df: pl.DataFrame, container: str):
    logger.info("Writing single-file CSV...")
    buf = io.BytesIO()
    df.write_csv(buf)
    _upload_blob(client, container, "single-file-csv/data.csv", buf.getvalue())


def _create_multi_file_csv(client: BlobServiceClient, df: pl.DataFrame, container: str, num_files: int = 10):
    logger.info(f"Writing {num_files} CSV files...")
    rows_per_file = len(df) // num_files
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        buf = io.BytesIO()
        sub_df.write_csv(buf)
        _upload_blob(client, container, f"multi-file-csv/part_{i:02d}.csv", buf.getvalue())


def _create_single_file_json(client: BlobServiceClient, df: pl.DataFrame, container: str):
    logger.info("Writing single-file JSON...")
    buf = io.BytesIO()
    df.write_ndjson(buf)
    _upload_blob(client, container, "single-file-json/data.json", buf.getvalue())


def _create_multi_file_json(client: BlobServiceClient, df: pl.DataFrame, container: str, num_files: int = 10):
    logger.info(f"Writing {num_files} JSON files...")
    rows_per_file = len(df) // num_files
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        buf = io.BytesIO()
        sub_df.write_ndjson(buf)
        _upload_blob(client, container, f"multi-file-json/part_{i:02d}.json", buf.getvalue())


def _create_single_parquet_file(client: BlobServiceClient, df: pl.DataFrame, container: str):
    logger.info("Writing single-file Parquet...")
    buf = io.BytesIO()
    df.write_parquet(buf)
    _upload_blob(client, container, "single-file-parquet/data.parquet", buf.getvalue())


def _create_multi_parquet_file(client: BlobServiceClient, df: pl.DataFrame, container: str, num_files: int = 10):
    logger.info(f"Writing {num_files} Parquet files...")
    rows_per_file = len(df) // num_files
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        buf = io.BytesIO()
        sub_df.write_parquet(buf)
        _upload_blob(client, container, f"multi-file-parquet/part_{i:02d}.parquet", buf.getvalue())


def _create_delta_lake_table(arrow_table: pa.Table, container: str):
    logger.info("Writing Delta Lake table...")
    storage_options = {
        "AZURE_STORAGE_ACCOUNT_NAME": AZURITE_ACCOUNT_NAME,
        "AZURE_STORAGE_ACCOUNT_KEY": AZURITE_ACCOUNT_KEY,
        "AZURE_STORAGE_USE_EMULATOR": "true",
        "AZURE_STORAGE_ENDPOINT": f"http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}",
        "AZURE_STORAGE_ALLOW_HTTP": "true",
    }
    delta_path = f"az://{container}/delta-lake-table"
    write_deltalake(delta_path, arrow_table, mode="overwrite", storage_options=storage_options)


def populate_test_data(container: str = "test-container"):
    """Populate an Azurite container with test data in various formats."""
    logger.info("Starting Azurite data population...")

    client = BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)

    data_size = 100_000
    df = pl.DataFrame(
        {
            "id": range(1, data_size + 1),
            "name": [f"user_{i}" for i in range(1, data_size + 1)],
            "value": [i * 10.5 for i in range(1, data_size + 1)],
            "category": ["A", "B", "C", "D", "E"] * (data_size // 5),
        }
    )
    logger.info(f"Generated a Polars DataFrame with {data_size} rows.")

    _create_single_csv_file(client, df, container)
    _create_multi_file_csv(client, df, container)
    _create_single_file_json(client, df, container)
    _create_multi_file_json(client, df, container)
    _create_single_parquet_file(client, df, container)
    _create_multi_parquet_file(client, df, container)

    arrow_table = df.to_arrow()
    _create_delta_lake_table(arrow_table, container)

    logger.info("All Azurite test data populated successfully.")
