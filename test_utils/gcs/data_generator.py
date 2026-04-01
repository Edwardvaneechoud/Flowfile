import io
import logging

import polars as pl
import pyarrow as pa
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


def _create_delta_lake_table(arrow_table: pa.Table, bucket_name: str):
    """Write a Delta Lake table to GCS via local write + gcsfs upload.

    delta-rs Rust GCS backend doesn't work reliably with fake-gcs-server,
    so we write locally and upload the result.
    """
    logger.info("Writing Delta Lake table...")
    import os
    import tempfile

    import gcsfs
    from deltalake import write_deltalake

    fs = gcsfs.GCSFileSystem(token="anon", endpoint_url=GCS_ENDPOINT_URL)
    gcs_path = f"{bucket_name}/delta-lake-table"

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, "delta-lake-table")
        write_deltalake(table_or_uri=local_path, data=arrow_table, mode="overwrite")
        fs.put(local_path + "/", gcs_path, recursive=True)


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

    arrow_table = df.to_arrow()
    try:
        _create_delta_lake_table(arrow_table, bucket_name)
    except Exception as e:
        logger.error(f"Failed to create Delta Lake table (non-fatal): {e}")

    # Verify: list all blobs in the bucket and log them
    _log_bucket_contents(client, bucket_name)
    logger.info("All GCS test data populated successfully.")


def _log_bucket_contents(client, bucket_name: str):
    """List and log all blobs in a bucket for verification."""
    logger.info(f"--- Contents of gs://{bucket_name} ---")
    blobs = list(client.list_blobs(bucket_name))
    if not blobs:
        logger.warning(f"  EMPTY - no blobs found in gs://{bucket_name}")
        return
    total_size = 0
    for blob in blobs:
        size = blob.size or 0
        total_size += size
        logger.info(f"  gs://{bucket_name}/{blob.name}  ({size:,} bytes)")
    logger.info(f"--- Total: {len(blobs)} blobs, {total_size:,} bytes ---")
