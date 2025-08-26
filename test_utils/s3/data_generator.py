
import logging
import io
import os

# Third-party libraries
import boto3
from botocore.client import Config
import polars as pl
import pyarrow as pa
from deltalake import write_deltalake
from pyiceberg.catalog import load_catalog


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MINIO_HOST = os.environ.get("TEST_MINIO_HOST", "localhost")
MINIO_PORT = int(os.environ.get("TEST_MINIO_PORT", 9000))
MINIO_CONSOLE_PORT = int(os.environ.get("TEST_MINIO_CONSOLE_PORT", 9001))
MINIO_ACCESS_KEY = os.environ.get("TEST_MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("TEST_MINIO_SECRET_KEY", "minioadmin")
MINIO_CONTAINER_NAME = os.environ.get("TEST_MINIO_CONTAINER", "test-minio-s3")
MINIO_ENDPOINT_URL = f"http://{MINIO_HOST}:{MINIO_PORT}"


def _create_single_csv_file(s3_client, df: pl.DataFrame, bucket_name: str):
    """Creates a single CSV file from a DataFrame and uploads it to S3."""
    logger.info("Writing single-file CSV...")
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    csv_buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket_name,
        Key='single-file-csv/data.csv',
        Body=csv_buffer.getvalue()
    )


def _create_multi_file_csv(s3_client, df: pl.DataFrame, bucket_name: str, num_files: int = 10):
    """Creates multiple CSV files from a DataFrame and uploads them to S3."""
    logger.info(f"Writing {num_files} CSV files...")
    data_size = len(df)
    rows_per_file = data_size // num_files
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        csv_buffer = io.BytesIO()
        sub_df.write_csv(csv_buffer)
        csv_buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f'multi-file-csv/part_{i:02d}.csv',
            Body=csv_buffer.getvalue()
        )


def _create_single_file_json(s3_client, df: pl.DataFrame, bucket_name: str):
    """Creates a single JSON file from a DataFrame and uploads it to S3."""
    logger.info("Writing single-file JSON...")
    json_buffer = io.BytesIO()
    df.write_ndjson(json_buffer)
    json_buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket_name,
        Key='single-file-json/data.json',
        Body=json_buffer.getvalue()
    )


def _create_multi_file_json(s3_client, df: pl.DataFrame, bucket_name: str, num_files: int = 10):
    """Creates multiple JSON files from a DataFrame and uploads them to S3."""
    logger.info(f"Writing {num_files} JSON files...")
    data_size = len(df)
    rows_per_file = data_size // num_files
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        json_buffer = io.BytesIO()
        sub_df.write_ndjson(json_buffer)
        json_buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f'multi-file-json/part_{i:02d}.json',
            Body=json_buffer.getvalue()
        )


def _create_single_parquet_file(s3_client, df: pl.DataFrame, bucket_name: str):
    """Creates a single Parquet file from a DataFrame and uploads it to S3."""
    logger.info("Writing single-file Parquet...")
    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket_name,
        Key='single-file-parquet/data.parquet',
        Body=parquet_buffer.getvalue()
    )


def _create_multi_parquet_file(s3_client, df: pl.DataFrame, bucket_name: str, num_files: int = 10):
    """Creates multiple Parquet files from a DataFrame and uploads them to S3."""
    logger.info(f"Writing {num_files} Parquet files...")
    data_size = len(df)
    rows_per_file = data_size // num_files
    for i in range(num_files):
        sub_df = df.slice(i * rows_per_file, rows_per_file)
        parquet_buffer = io.BytesIO()
        sub_df.write_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f'multi-file-parquet/part_{i:02d}.parquet',
            Body=parquet_buffer.getvalue()
        )


def _create_delta_lake_table(arrow_table: pa.Table, bucket_name: str, storage_options: dict):
    """Creates a Delta Lake table from a PyArrow table in S3."""
    logger.info("Writing Delta Lake table...")
    delta_table_path = f"s3://{bucket_name}/delta-lake-table"
    write_deltalake(
        delta_table_path,
        arrow_table,
        mode='overwrite',
        storage_options=storage_options
    )


def _create_iceberg_table(df: pl.DataFrame, bucket_name: str, endpoint_url: str, access_key: str, secret_key: str,
                          s3_client):
    """Creates an Apache Iceberg table and FORCES sane metadata pointers."""
    logger.info("Writing Apache Iceberg table with SANE metadata access...")
    # Configure the catalog properties for S3 access
    catalog_props = {
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": endpoint_url,
        "s3.access-key-id": access_key,
        "s3.secret-access-key": secret_key,
    }
    # Use the SQL catalog with an in-memory SQLite database for storing metadata pointers
    catalog = load_catalog(
        "default",
        **{
            "type": "sql",
            "uri": "sqlite:///:memory:",  # Use an in-memory SQL DB for the catalog
            "warehouse": f"s3a://{bucket_name}/iceberg_warehouse",
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
        current_metadata_key = current_metadata.replace(f"s3a://{bucket_name}/", "")
    else:
        current_metadata_key = current_metadata.replace(f"s3://{bucket_name}/", "")

    # Read the current metadata
    response = s3_client.get_object(Bucket=bucket_name, Key=current_metadata_key)
    metadata_content = response['Body'].read()

    # Get the metadata directory
    metadata_dir = "/".join(current_metadata_key.split("/")[:-1])

    # Write it to standardized locations
    # 1. metadata.json in the metadata folder (this is what pl.scan_iceberg expects)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"{metadata_dir}/metadata.json",
        Body=metadata_content
    )
    logger.info(f"Created stable metadata.json at: s3://{bucket_name}/{metadata_dir}/metadata.json")

    # 2. current.json as an additional pointer
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"{metadata_dir}/current.json",
        Body=metadata_content
    )

    # 3. VERSION file that contains the current metadata filename
    current_metadata_filename = current_metadata_key.split("/")[-1]
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"{metadata_dir}/VERSION",
        Body=current_metadata_filename.encode()
    )

    # 4. version-hint.text (some Iceberg readers look for this)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=f"{metadata_dir}/version-hint.text",
        Body=current_metadata_filename.encode()
    )

    table_base = "iceberg_warehouse/default_db.db/my_iceberg_table"
    logger.info(f"""
✅ Iceberg table created with SANE access patterns:
   - Versioned metadata: s3://{bucket_name}/{current_metadata_key}
   - Latest metadata: s3://{bucket_name}/{table_base}/metadata/metadata.json
   - Current pointer: s3://{bucket_name}/{table_base}/metadata/current.json
   - Version hint: s3://{bucket_name}/{table_base}/metadata/version-hint.text

   Read with: pl.scan_iceberg('s3://{bucket_name}/{table_base}/metadata/metadata.json').collect()
""")


def populate_test_data(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str):
    """
    Populates a MinIO bucket with a variety of large-scale test data formats.

    Args:
        endpoint_url (str): The S3 endpoint URL for the MinIO instance.
        access_key (str): The access key for MinIO.
        secret_key (str): The secret key for MinIO.
        bucket_name (str): The name of the bucket to populate.
    """
    logger.info("🚀 Starting data population...")
    # --- S3 Client and Storage Options ---
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    storage_options = {
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

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
    _create_single_csv_file(s3_client, df, bucket_name)
    _create_multi_file_csv(s3_client, df, bucket_name)
    _create_single_file_json(s3_client, df, bucket_name)
    _create_multi_file_json(s3_client, df, bucket_name)
    _create_single_parquet_file(s3_client, df, bucket_name)
    _create_multi_parquet_file(s3_client, df, bucket_name)

    # Convert to PyArrow table once for Delta and Iceberg
    arrow_table = df.to_arrow()

    _create_delta_lake_table(arrow_table, bucket_name, storage_options)
    _create_iceberg_table(df, bucket_name, endpoint_url, access_key, secret_key, s3_client)

    logger.info("✅ All test data populated successfully.")


if __name__ == '__main__':
    populate_test_data(endpoint_url=MINIO_ENDPOINT_URL,
                       access_key=MINIO_ACCESS_KEY,
                       secret_key=MINIO_SECRET_KEY,
                       bucket_name="test-bucket")