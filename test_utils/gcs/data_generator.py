
import logging
import io
import os

# Third-party libraries
from google.cloud import storage
import polars as pl
import pyarrow as pa
from deltalake import write_deltalake
from pyiceberg.catalog import load_catalog
import mimetypes
from typing import Dict
from google.auth.credentials import AnonymousCredentials
import requests
import google.auth.transport.requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

GCS_HOST = os.environ.get("TEST_GCS_HOST", "localhost")
GCS_PORT = int(os.environ.get("TEST_GCS_PORT", 4443))
GCS_BUCKET_NAME = os.environ.get("TEST_GCS_CONTAINER", "test-gcs")
GCS_ENDPOINT_URL = f"http://{GCS_HOST}:{GCS_PORT}"
FAKE_GCS_SERVER_NAME = os.environ.get("TEST_MINIO_CONTAINER", "test-gcs-server")

def _create_single_csv_file(gcs_client, df: pl.DataFrame, bucket_name: str):
    """Creates a single CSV file from a DataFrame and uploads it to GCS."""
    logger.info("Writing single-file CSV...")
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    csv_buffer.seek(0)
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob('single-file-csv/data.csv')
    blob.upload_from_file(csv_buffer, content_type='text/csv')


def _create_multi_file_csv(gcs_client, df: pl.DataFrame, bucket_name: str, num_files: int = 10):
    """Creates multiple CSV files from a DataFrame and uploads them to S3."""
    logger.info(f"Writing {num_files} CSV files...")
    data_size = len(df)
    rows_per_file = data_size // num_files
    bucket = gcs_client.bucket(bucket_name)
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        csv_buffer = io.BytesIO()
        sub_df.write_csv(csv_buffer)
        csv_buffer.seek(0)
        blob = bucket.blob(f'multi-file-csv/part_{i:02d}.csv')
        blob.upload_from_file(csv_buffer, content_type='text/csv')



def _create_single_file_json(gcs_client, df: pl.DataFrame, bucket_name: str):
    """Creates a single JSON file from a DataFrame and uploads it to S3."""
    logger.info("Writing single-file JSON...")
    json_buffer = io.BytesIO()
    df.write_ndjson(json_buffer)
    json_buffer.seek(0)
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob('single-file-json/data.json')
    blob.upload_from_file(json_buffer, content_type='application/json')


def _create_multi_file_json(gcs_client, df: pl.DataFrame, bucket_name: str, num_files: int = 10):
    """Creates multiple JSON files from a DataFrame and uploads them to S3."""
    logger.info(f"Writing {num_files} JSON files...")
    data_size = len(df)
    rows_per_file = data_size // num_files
    bucket = gcs_client.bucket(bucket_name)
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        json_buffer = io.BytesIO()
        sub_df.write_ndjson(json_buffer)
        json_buffer.seek(0)
        blob = bucket.blob(f'multi-file-json/part_{i:02d}.json')
        blob.upload_from_file(json_buffer, content_type='application/json')


def _create_single_parquet_file(gcs_client, df: pl.DataFrame, bucket_name: str):
    """Creates a single Parquet file from a DataFrame and uploads it to S3."""
    logger.info("Writing single-file Parquet...")
    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob('single-file-parquet/data.parquet')
    blob.upload_from_file(parquet_buffer, content_type='application/vnd.apache.parquet')


def _create_multi_parquet_file(gcs_client, df: pl.DataFrame, bucket_name: str, num_files: int = 10):
    """Creates multiple Parquet files from a DataFrame and uploads them to S3."""
    logger.info(f"Writing {num_files} Parquet files...")
    data_size = len(df)
    rows_per_file = data_size // num_files
    bucket = gcs_client.bucket(bucket_name)
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        parquet_buffer = io.BytesIO()
        sub_df.write_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        blob = bucket.blob(f'multi-file-parquet/part_{i:02d}.parquet')
        blob.upload_from_file(parquet_buffer, content_type='application/vnd.apache.parquet')

def guess_content_type(filename):
    """ Created since mimetypes doesn't know about parquet by default."""
    if filename.endswith(".parquet"):
        return "application/vnd.apache.parquet"
    if filename.endswith(".avro"):
        return "application/avro"
    return mimetypes.guess_type(filename)[0] or "application/octet-stream"



def _create_delta_lake_table(gcs_client, arrow_table: pa.Table, bucket_name: str):
    """Creates a Delta Lake table from a PyArrow table in S3."""
    logger.info("Writing Delta Lake table...")
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(f"delta-lake-table")
    for root, _, files in os.walk(table_path):
        for f in files:
            local_path = os.path.join(root, f)
            rel_path = os.path.relpath(local_path, arrow_table)
            blob = bucket.blob(f"delta_table/{rel_path}")
            blob.upload_from_filename(local_path, content_type=guess_content_type(local_path))
            print(f"Uploaded {rel_path} with content_type={blob.content_type}")



def _create_iceberg_table(df: pl.DataFrame, bucket_name: str, endpoint_url: str, app_credentials: Dict,
                          gcs_client):
    """Creates an Apache Iceberg table and FORCES sane metadata pointers."""
    logger.info("Writing Apache Iceberg table with SANE metadata access...")
    # Configure the catalog properties for S3 access
    catalog_props = {
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "gcs.endpoint": endpoint_url,
        "gcs.app-credentials": app_credentials
    }

    # Creating a bucket instance to upload files
    bucket = gcs_client.bucket(bucket_name)

    # Use the SQL catalog with an in-memory SQLite database for storing metadata pointers
    catalog = load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": "sqlite:///:memory:",  # Use an in-memory SQL DB for the catalog
            "warehouse": f"https://storage.googleapis.com/{bucket_name}/iceberg_warehouse",
            **catalog_props,
        }
    )
    table_identifier = ("default_db", "iceberg_table")
    # Create a namespace (like a schema or database) for the table
    try:
        catalog.drop_namespace("default_db")
    except Exception:
        pass  # Ignore if namespace doesn't exist
    catalog.create_namespace("default_db")
    try:
        catalog.load_table(table_identifier)
        catalog.drop_table(table_identifier)
    except:
        pass

    # Create the table schema and object first
    schema = df.to_arrow().schema
    table = catalog.create_table(identifier=table_identifier, schema=schema)

    # Use the simplified write_iceberg method from Polars
    df.write_iceberg(table, mode='overwrite')

    # NOW CREATE WHAT SHOULD EXIST BY DEFAULT - SANE METADATA POINTERS
    # Get the current metadata location from the table
    current_metadata = table.metadata_location
    logger.info(f"Original metadata location: {current_metadata}")

    # Extract just the path part
    if current_metadata.startswith("s3a://"):
        current_metadata_key = current_metadata.replace(f"https://storage.googleapis.com/{bucket_name}/", "")
    else:
        current_metadata_key = current_metadata.replace(f"https://storage.googleapis.com/{bucket_name}/", "")

    # Read the current metadata
    response = gcs_client.get_object(Bucket=bucket_name, Key=current_metadata_key)
    metadata_content = response['Body'].read()

    # Get the metadata directory
    metadata_dir = "/".join(current_metadata_key.split("/")[:-1])

    # Write it to standardized locations
    # 1. metadata.json in the metadata folder (this is what pl.scan_iceberg expects)
    blob = bucket.blob(f"{metadata_dir}")
    blob.upload_from_filename("metadata.json", content_type=guess_content_type(metadata_content))

    logger.info(f"Created stable metadata.json at: https://storage.googleapis.com/{bucket_name}/{metadata_dir}/metadata.json")

    # 2. current.json as an additional pointer
    blob = bucket.blob(f"{metadata_dir}")
    blob.upload_from_filename("current.json", content_type=guess_content_type(metadata_content))

    # 3. VERSION file that contains the current metadata filename
    current_metadata_filename = current_metadata_key.split("/")[-1]
    blob = bucket.bloc(f"{metadata_dir}/VERSION")
    blob.upload_from_filename(current_metadata_filename.encode(), content_type=guess_content_type(metadata_content))

    # 4. version-hint.text (some Iceberg readers look for this)
    blob = bucket.bloc(f"{metadata_dir}/version-hint.text")
    blob.upload_from_filename(current_metadata_filename.encode(), content_type=guess_content_type(metadata_content))

    table_base = "iceberg_warehouse/default_db.db/my_iceberg_table"
    logger.info(f"""
✅ Iceberg table created with SANE access patterns:
   - Versioned metadata: https://storage.googleapis.com/{bucket_name}/{current_metadata_key}
   - Latest metadata: https://storage.googleapis.com/{bucket_name}/{table_base}/metadata/metadata.json
   - Current pointer: https://storage.googleapis.com/{bucket_name}/{table_base}/metadata/current.json
   - Version hint: https://storage.googleapis.com/{bucket_name}/{table_base}/metadata/version-hint.text

   Read with: pl.scan_iceberg('s3://{bucket_name}/{table_base}/metadata/metadata.json').collect()
""")


def populate_test_data(endpoint_url: str, bucket_name: str):
    """
    Populates a MinIO bucket with a variety of large-scale test data formats.

    Args:
        endpoint_url (str): The S3 endpoint URL for the MinIO instance.
        access_key (str): The access key for MinIO.
        secret_key (str): The secret key for MinIO.
        bucket_name (str): The name of the bucket to populate.
    """
    logger.info("🚀 Starting data population...")

    # ---- Custom transport that skips SSL verification ----
    session = requests.Session()
    session.verify = False  # disable SSL cert verification

    transport = google.auth.transport.requests.AuthorizedSession(
        AnonymousCredentials()
    )
    transport.session = session  # inject custom session
    
    # --- S3 Client and Storage Options ---
    gcs_client = storage.Client(
        project="test-project",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": f"{endpoint_url}"},
        _http=transport
        )
   
    # --- Data Generation ---
    data_size = 100_000
    df = pl.DataFrame({
        "id": range(1, data_size + 1),
        "name": [f"user_{i}" for i in range(1, data_size + 1)],
        "value": [i * 10.5 for i in range(1, data_size + 1)],
        "category": ["A", "B", "C", "D", "E"] * (data_size // 5)
    })
    logger.info(f"Generated a Polars DataFrame with {data_size} rows.")
    #
    # # --- Execute Data Population Scenarios ---
    _create_single_csv_file(gcs_client, df, bucket_name)
    _create_multi_file_csv(gcs_client, df, bucket_name)
    _create_single_file_json(gcs_client, df, bucket_name)
    _create_multi_file_json(gcs_client, df, bucket_name)
    _create_single_parquet_file(gcs_client, df, bucket_name)
    _create_multi_parquet_file(gcs_client, df, bucket_name)

    # Convert to PyArrow table once for Delta and Iceberg
    arrow_table = df.to_arrow()

    _create_delta_lake_table(arrow_table, bucket_name)
    _create_iceberg_table(df, bucket_name, endpoint_url, gcs_client)

    logger.info("✅ All test data populated successfully.")


if __name__ == '__main__':
    populate_test_data(endpoint_url=GCS_ENDPOINT_URL,
                       bucket_name=GCS_BUCKET_NAME)