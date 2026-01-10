"""Generate test data for Azurite ADLS testing."""
import io
import logging

import polars as pl
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger("adls_data_generator")


def populate_test_data(
    account_name: str, account_key: str, blob_endpoint: str, container_name: str = "test-container"
):
    """
    Populate Azurite with test data in various formats.

    Args:
        account_name: Azure storage account name
        account_key: Azure storage account key
        blob_endpoint: Blob storage endpoint URL
        container_name: Container to populate with data
    """
    # Create connection string
    connection_string = (
        f"DefaultEndpointsProtocol=http;"
        f"AccountName={account_name};"
        f"AccountKey={account_key};"
        f"BlobEndpoint={blob_endpoint};"
    )

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # Create sample DataFrame
    df = pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "city": ["New York", "London", "Tokyo", "Paris", "Berlin"],
            "score": [85.5, 92.3, 78.9, 88.1, 95.7],
        }
    )

    # Upload Parquet file
    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    blob_client = container_client.get_blob_client("data/test_data.parquet")
    blob_client.upload_blob(parquet_buffer, overwrite=True)
    logger.info(f"Uploaded: {container_name}/data/test_data.parquet")

    # Upload multiple Parquet files for directory testing
    for i in range(3):
        df_part = df.slice(i * 2, 2)
        parquet_buffer = io.BytesIO()
        df_part.write_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        blob_client = container_client.get_blob_client(f"data/partitioned/part_{i}.parquet")
        blob_client.upload_blob(parquet_buffer, overwrite=True)
        logger.info(f"Uploaded: {container_name}/data/partitioned/part_{i}.parquet")

    # Upload CSV file
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    csv_buffer.seek(0)
    blob_client = container_client.get_blob_client("data/test_data.csv")
    blob_client.upload_blob(csv_buffer, overwrite=True)
    logger.info(f"Uploaded: {container_name}/data/test_data.csv")

    # Upload JSON file
    json_buffer = io.BytesIO()
    df.write_ndjson(json_buffer)
    json_buffer.seek(0)
    blob_client = container_client.get_blob_client("data/test_data.json")
    blob_client.upload_blob(json_buffer, overwrite=True)
    logger.info(f"Uploaded: {container_name}/data/test_data.json")

    logger.info(f"Successfully populated {container_name} with test data")
