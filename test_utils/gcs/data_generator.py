import io
import logging

import polars as pl
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

from test_utils.gcs.fixtures import GCS_ENDPOINT_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _get_gcs_client():
    client = storage.Client(credentials=AnonymousCredentials(), project="test-project")
    client._connection.API_BASE_URL = GCS_ENDPOINT_URL
    return client


def _upload_blob(client, bucket_name: str, blob_name: str, data: bytes):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)


def _create_single_csv_file(client, df: pl.DataFrame, bucket_name: str):
    logger.info("Writing single-file CSV...")
    buf = io.BytesIO()
    df.write_csv(buf)
    _upload_blob(client, bucket_name, "single-file-csv/data.csv", buf.getvalue())


def _create_multi_file_csv(client, df: pl.DataFrame, bucket_name: str, num_files: int = 10):
    logger.info(f"Writing {num_files} CSV files...")
    rows_per_file = len(df) // num_files
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        buf = io.BytesIO()
        sub_df.write_csv(buf)
        _upload_blob(client, bucket_name, f"multi-file-csv/part_{i:02d}.csv", buf.getvalue())


def _create_single_file_json(client, df: pl.DataFrame, bucket_name: str):
    logger.info("Writing single-file JSON...")
    buf = io.BytesIO()
    df.write_ndjson(buf)
    _upload_blob(client, bucket_name, "single-file-json/data.json", buf.getvalue())


def _create_multi_file_json(client, df: pl.DataFrame, bucket_name: str, num_files: int = 10):
    logger.info(f"Writing {num_files} JSON files...")
    rows_per_file = len(df) // num_files
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        buf = io.BytesIO()
        sub_df.write_ndjson(buf)
        _upload_blob(client, bucket_name, f"multi-file-json/part_{i:02d}.json", buf.getvalue())


def _create_single_parquet_file(client, df: pl.DataFrame, bucket_name: str):
    logger.info("Writing single-file Parquet...")
    buf = io.BytesIO()
    df.write_parquet(buf)
    _upload_blob(client, bucket_name, "single-file-parquet/data.parquet", buf.getvalue())


def _create_multi_parquet_file(client, df: pl.DataFrame, bucket_name: str, num_files: int = 10):
    logger.info(f"Writing {num_files} Parquet files...")
    rows_per_file = len(df) // num_files
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        buf = io.BytesIO()
        sub_df.write_parquet(buf)
        _upload_blob(client, bucket_name, f"multi-file-parquet/part_{i:02d}.parquet", buf.getvalue())


def populate_test_data(bucket_name: str = "test-bucket"):
    """Populate a fake-gcs-server bucket with test data in various formats."""
    logger.info("Starting GCS data population...")

    client = _get_gcs_client()

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

    _create_single_csv_file(client, df, bucket_name)
    _create_multi_file_csv(client, df, bucket_name)
    _create_single_file_json(client, df, bucket_name)
    _create_multi_file_json(client, df, bucket_name)
    _create_single_parquet_file(client, df, bucket_name)
    _create_multi_parquet_file(client, df, bucket_name)

    logger.info("All GCS test data populated successfully.")
