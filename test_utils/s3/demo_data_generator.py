import logging
import io
import os
import tempfile
import shutil
import random
from datetime import datetime, timedelta

# Third-party libraries
import boto3
from botocore.client import Config
import polars as pl
import pyarrow as pa
from pyarrow import parquet as pq

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- MinIO/S3 Configuration ---
MINIO_HOST = os.environ.get("TEST_MINIO_HOST", "localhost")
MINIO_PORT = int(os.environ.get("TEST_MINIO_PORT", 9000))
MINIO_ACCESS_KEY = os.environ.get("TEST_MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("TEST_MINIO_SECRET_KEY", "minioadmin")
MINIO_ENDPOINT_URL = f"http://{MINIO_HOST}:{MINIO_PORT}"

# --- Data Generation Functions ---

def _create_sales_data(s3_client, df: pl.DataFrame, bucket_name: str):
    """
    Creates partitioned Parquet files for the sales data based on year and month.
    s3://data-lake/sales/year=YYYY/month=MM/
    """
    logger.info("Writing partitioned sales data...")
    # Use Polars' built-in partitioning
    # A temporary local directory is needed to stage the partitioned files before uploading
    with tempfile.TemporaryDirectory() as temp_dir:
        df.write_parquet(
            temp_dir,
            use_pyarrow=True,
            pyarrow_options={"partition_cols": ["year", "month"]}
        )
        # Walk through the local directory and upload files to S3
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.endswith(".parquet"):
                    local_path = os.path.join(root, file)
                    # Construct the S3 key to match the desired structure
                    relative_path = os.path.relpath(local_path, temp_dir)
                    s3_key = f"data-lake/sales/{relative_path.replace(os.path.sep, '/')}"
                    s3_client.upload_file(local_path, bucket_name, s3_key)
    logger.info(f"Finished writing sales data to s3://{bucket_name}/data-lake/sales/")

def _create_customers_data(s3_client, df: pl.DataFrame, bucket_name: str):
    """
    Creates a Parquet file for the customers data.
    s3://data-lake/customers/
    """
    logger.info("Writing customers Parquet data...")
    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket_name,
        Key='data-lake/customers/customers.parquet',
        Body=parquet_buffer.getvalue()
    )
    logger.info(f"Finished writing customers data to s3://{bucket_name}/data-lake/customers/")


def _create_orders_data(s3_client, df: pl.DataFrame, bucket_name: str):
    """
    Creates a pipe-delimited CSV file for the orders data.
    s3://raw-data/orders/
    """
    logger.info("Writing orders CSV data...")
    csv_buffer = io.BytesIO()
    # Write with pipe delimiter and header
    df.write_csv(csv_buffer, separator="|")
    csv_buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket_name,
        Key='raw-data/orders/orders.csv',
        Body=csv_buffer.getvalue()
    )
    logger.info(f"Finished writing orders data to s3://{bucket_name}/raw-data/orders/")

def _create_products_data(df: pl.DataFrame):
    """
    Creates a local Parquet file for the products data.
    """
    logger.info("Writing local products Parquet data...")
    # Create a directory for local data if it doesn't exist
    local_data_dir = "local_data"
    os.makedirs(local_data_dir, exist_ok=True)
    file_path = os.path.join(local_data_dir, "local_products.parquet")
    df.write_parquet(file_path)
    logger.info(f"Finished writing products data to {file_path}")


def create_demo_data(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str):
    """
    Populates a MinIO bucket with test data matching the schemas from the examples.
    """
    logger.info("🚀 Starting data population for flowfile examples...")
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    # --- Generate Core DataFrames ---
    DATA_SIZE = 15_000 # Increased data size for more variety
    START_DATE = datetime(2022, 1, 1)
    END_DATE = datetime(2024, 12, 31)
    TOTAL_DAYS = (END_DATE - START_DATE).days

    # States for region mapping
    states = ["CA", "OR", "WA", "NY", "NJ", "PA", "TX", "FL", "GA", "IL", "OH", "MI"]

    # Generate base sales data across multiple years
    sales_data = {
        "order_id": range(1, DATA_SIZE + 1),
        "customer_id": [random.randint(100, 299) for _ in range(DATA_SIZE)],
        "product_id": [random.randint(1, 100) for _ in range(DATA_SIZE)],
        "order_date": [START_DATE + timedelta(days=random.randint(0, TOTAL_DAYS)) for _ in range(DATA_SIZE)],
        "quantity": [random.randint(1, 5) for _ in range(DATA_SIZE)],
        "unit_price": [round(random.uniform(10.0, 500.0), 2) for _ in range(DATA_SIZE)],
        "discount_rate": [random.choice([0.0, 0.1, 0.15, 0.2, None]) for _ in range(DATA_SIZE)],
        "status": [random.choice(["completed", "pending", "cancelled"]) for _ in range(DATA_SIZE)],
        "customer_lifetime_value": [random.uniform(500, 20000) for _ in range(DATA_SIZE)],
        "state": [random.choice(states) for _ in range(DATA_SIZE)],
    }
    sales_df = pl.from_dict(sales_data).with_columns([
        pl.col("order_date").dt.year().alias("year"),
        pl.col("order_date").dt.month().alias("month"),
        # The 'amount' column in the example seems to be the price before discount
        pl.col("unit_price").alias("amount")
    ])

    # Generate customers DataFrame
    unique_customer_ids = sales_df["customer_id"].unique().to_list()
    customers_df = pl.DataFrame({
        "customer_id": unique_customer_ids,
        "customer_segment": [random.choice(["VIP", "Regular", "New"]) for _ in unique_customer_ids]
    })

    # Generate products DataFrame
    unique_product_ids = sales_df["product_id"].unique().to_list()
    # Create a map of product_id to unit_price from the first occurrence in sales_df
    product_price_map = sales_df.group_by("product_id").agg(pl.first("unit_price")).to_dict(as_series=False)
    price_dict = dict(zip(product_price_map['product_id'], product_price_map['unit_price']))

    products_df = pl.DataFrame({
        "product_id": unique_product_ids,
        "product_category": [random.choice(["Electronics", "Books", "Clothing", "Home Goods"]) for _ in unique_product_ids],
        "unit_price": [price_dict.get(pid) for pid in unique_product_ids]
    })

    # Generate orders DataFrame for the CSV file (subset of sales)
    orders_df = sales_df.select(["customer_id", "product_id", "quantity", "discount_rate"])

    logger.info(f"Generated {len(sales_df)} sales records across {sales_df['year'].n_unique()} years, for {len(customers_df)} customers, and {len(products_df)} products.")

    # --- Write Data to S3 and Local Filesystem ---
    _create_sales_data(s3_client, sales_df, bucket_name)
    _create_customers_data(s3_client, customers_df, bucket_name)
    _create_orders_data(s3_client, orders_df, bucket_name)
    _create_products_data(products_df)

    logger.info("✅ All test data populated successfully.")


if __name__ == '__main__':
    # The bucket that will be created and populated
    BUCKET = "flowfile-demo-data"

    create_demo_data(
        endpoint_url=MINIO_ENDPOINT_URL,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket_name=BUCKET
    )
