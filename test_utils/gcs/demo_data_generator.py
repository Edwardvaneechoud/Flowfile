import logging
import io
import os
import tempfile
import shutil
import random
from datetime import datetime, timedelta

# Third-party libraries
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
import polars as pl
import pyarrow as pa
from pyarrow import parquet as pq

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- MinIO/gcs Configuration ---
GCS_HOST = os.environ.get("TEST_GCS_HOST", "localhost")
GCS_PORT = int(os.environ.get("TEST_GCS_PORT", 4443))
GCS_ENDPOINT_URL = f"http://{GCS_HOST}:{GCS_PORT}"

# --- Data Generation Functions ---

def _create_sales_data(gcs_client, df: pl.DataFrame, bucket_name: str):
    """
    Creates partitioned Parquet files for the sales data based on year and month.
    gcs://data-lake/sales/year=YYYY/month=MM/
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
        # Walk through the local directory and upload files to gcs
        for root, _, files in os.walk(temp_dir):
            for file in files:
                if file.endswith(".parquet"):
                    local_path = os.path.join(root, file)
                    # Construct the gcs key to match the desired structure
                    relative_path = os.path.relpath(local_path, temp_dir)
                    gcs_key = f"data-lake/sales/{relative_path.replace(os.path.sep, '/')}"
                    bucket = gcs_client.bucket(bucket_name)
                    blob = bucket.blob(f'{gcs_key}')
                    blob.upload_from_file(local_path, content_type='application/parquet')
    logger.info(f"Finished writing sales data to gcs://{bucket_name}/data-lake/sales/")

def _create_customers_data(gcs_client, df: pl.DataFrame, bucket_name: str):
    """
    Creates a Parquet file for the customers data.
    gcs://data-lake/customers/
    """
    logger.info("Writing customers Parquet data...")
    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob('data-lake/customers/customers.parquet')
    blob.upload_from_file(parquet_buffer.getvalue(), content_type='application/parquet')
    logger.info(f"Finished writing customers data to gcs://{bucket_name}/data-lake/customers/")


def _create_orders_data(gcs_client, df: pl.DataFrame, bucket_name: str):
    """
    Creates a pipe-delimited CSV file for the orders data.
    gcs://raw-data/orders/
    """
    logger.info("Writing orders CSV data...")
    csv_buffer = io.BytesIO()
    # Write with pipe delimiter and header
    df.write_csv(csv_buffer, separator="|")
    csv_buffer.seek(0)
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob('raw-data/orders/orders.csv')
    blob.upload_from_file(csv_buffer.getvalue(), content_type='text/csv')
    logger.info(f"Finished writing orders data to gcs://{bucket_name}/raw-data/orders/")

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


def create_demo_data(endpoint_url: str, bucket_name: str):
    """
    Populates a MinIO bucket with test data matching the schemas from the examples.
    """
    logger.info("🚀 Starting data population for flowfile examples...")

    gcs_client = storage.Client(
        project="test-project",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": f"{endpoint_url}"}
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

    # --- Write Data to gcs and Local Filesystem ---
    _create_sales_data(gcs_client, sales_df, bucket_name)
    _create_customers_data(gcs_client, customers_df, bucket_name)
    _create_orders_data(gcs_client, orders_df, bucket_name)
    _create_products_data(products_df)

    logger.info("✅ All test data populated successfully.")


if __name__ == '__main__':
    # The bucket that will be created and populated
    BUCKET = "flowfile-demo-data"

    create_demo_data(
        endpoint_url=GCS_ENDPOINT_URL,
        bucket_name=BUCKET
    )
