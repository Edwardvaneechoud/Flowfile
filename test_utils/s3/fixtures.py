import os
import time
import subprocess
import logging
from contextlib import contextmanager
from typing import Dict, Generator, Optional
import boto3
from botocore.client import Config

logger = logging.getLogger("s3_fixture")

MINIO_HOST = os.environ.get("TEST_MINIO_HOST", "localhost")
MINIO_PORT = int(os.environ.get("TEST_MINIO_PORT", 9000))
MINIO_CONSOLE_PORT = int(os.environ.get("TEST_MINIO_CONSOLE_PORT", 9001))
MINIO_ACCESS_KEY = os.environ.get("TEST_MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("TEST_MINIO_SECRET_KEY", "minioadmin")
MINIO_CONTAINER_NAME = os.environ.get("TEST_MINIO_CONTAINER", "test-minio-s3")
MINIO_ENDPOINT_URL = f"http://{MINIO_HOST}:{MINIO_PORT}"


def get_minio_client():
    """Get boto3 client for MinIO"""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )


def wait_for_minio(max_retries=30, interval=1):
    """Wait for MinIO to be ready"""
    for i in range(max_retries):
        try:
            client = get_minio_client()
            client.list_buckets()
            logger.info("MinIO is ready")
            return True
        except Exception:
            if i < max_retries - 1:
                time.sleep(interval)
            continue
    return False

def is_container_running(container_name: str) -> bool:
    """Check if MinIO container is already running"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )
        return container_name in result.stdout.strip()
    except subprocess.CalledProcessError:
        return False


def stop_minio_container() -> bool:
    """Stop MinIO container"""
    if not is_container_running(MINIO_CONTAINER_NAME):
        return True

    try:
        subprocess.run(["docker", "stop", MINIO_CONTAINER_NAME], check=True)
        subprocess.run(["docker", "rm", MINIO_CONTAINER_NAME], check=True)
        return True
    except Exception as e:
        logger.error(f"Failed to stop MinIO: {e}")
        return False


def create_test_buckets():
    """Create test buckets and populate with sample data"""
    client = get_minio_client()

    # Create test buckets
    buckets = ['test-bucket', 'flowfile-test', 'sample-data']
    for bucket in buckets:
        try:
            client.create_bucket(Bucket=bucket)
            logger.info(f"Created bucket: {bucket}")
        except client.exceptions.BucketAlreadyExists:
            logger.info(f"Bucket already exists: {bucket}")
        except client.exceptions.BucketAlreadyOwnedByYou:
            logger.info(f"Bucket already owned: {bucket}")


def populate_test_data():
    """Populate MinIO with test data"""
    import polars as pl
    import io

    client = get_minio_client()

    # Create sample DataFrame
    df = pl.DataFrame({
        "id": range(1, 1000),
        "name": [f"user_{i}" for i in range(1, 1000)],
        "value": [i * 10.5 for i in range(1, 1000)],
        "category": ["A", "B", "C", "D"] * 250
    })

    # Write as Parquet
    parquet_buffer = io.BytesIO()
    df.write_parquet(parquet_buffer)
    parquet_buffer.seek(0)
    client.put_object(
        Bucket='test-bucket',
        Key='sample_data.parquet',
        Body=parquet_buffer.getvalue()
    )

    # Write as CSV
    csv_buffer = io.BytesIO()
    df.write_csv(csv_buffer)
    csv_buffer.seek(0)
    client.put_object(
        Bucket='test-bucket',
        Key='sample_data.csv',
        Body=csv_buffer.getvalue()
    )

    # Create directory structure with multiple files
    for i in range(5):
        sub_df = df.slice(i * 20, 20)
        buffer = io.BytesIO()
        sub_df.write_parquet(buffer)
        buffer.seek(0)
        client.put_object(
            Bucket='test-bucket',
            Key=f'partitioned/part_{i}.parquet',
            Body=buffer.getvalue()
        )

    logger.info("Test data populated successfully")


def start_minio_container() -> bool:
    """Start MinIO container with initialization"""
    if is_container_running(MINIO_CONTAINER_NAME):
        logger.info(f"Container {MINIO_CONTAINER_NAME} is already running")
        return True

    try:
        # Start MinIO with volume for persistence
        subprocess.run([
            "docker", "run", "-d",
            "--name", MINIO_CONTAINER_NAME,
            "-p", f"{MINIO_PORT}:9000",
            "-p", f"{MINIO_CONSOLE_PORT}:9001",
            "-e", f"MINIO_ROOT_USER={MINIO_ACCESS_KEY}",
            "-e", f"MINIO_ROOT_PASSWORD={MINIO_SECRET_KEY}",
            "-v", f"{MINIO_CONTAINER_NAME}-data:/data",
            "minio/minio", "server", "/data", "--console-address", ":9001"
        ], check=True)

        # Wait for MinIO to be ready
        if wait_for_minio():
            create_test_buckets()
            populate_test_data()
            return True
        return False

    except Exception as e:
        logger.error(f"Failed to start MinIO: {e}")
        return False


@contextmanager
def managed_minio() -> Generator[Dict[str, any], None, None]:
    """Context manager for MinIO container with full connection info"""
    if not start_minio_container():
        yield {}
        return

    try:
        connection_info = {
            "endpoint_url": MINIO_ENDPOINT_URL,
            "access_key": MINIO_ACCESS_KEY,
            "secret_key": MINIO_SECRET_KEY,
            "host": MINIO_HOST,
            "port": MINIO_PORT,
            "console_port": MINIO_CONSOLE_PORT,
            "connection_string": f"s3://{MINIO_ACCESS_KEY}:{MINIO_SECRET_KEY}@{MINIO_HOST}:{MINIO_PORT}"
        }
        yield connection_info
    finally:
        # Optionally keep container running for debugging
        if os.environ.get("KEEP_MINIO_RUNNING", "false").lower() != "true":
            stop_minio_container()
