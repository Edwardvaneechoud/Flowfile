import os
import time
import subprocess
import logging
from contextlib import contextmanager
from typing import Dict, Generator
import shutil
import boto3
from botocore.client import Config
from test_utils.s3.data_generator import populate_test_data
from test_utils.s3.demo_data_generator import create_demo_data

logger = logging.getLogger("s3_fixture")

MINIO_HOST = os.environ.get("TEST_MINIO_HOST", "localhost")
MINIO_PORT = int(os.environ.get("TEST_MINIO_PORT", 9000))
MINIO_CONSOLE_PORT = int(os.environ.get("TEST_MINIO_CONSOLE_PORT", 9001))
MINIO_ACCESS_KEY = os.environ.get("TEST_MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("TEST_MINIO_SECRET_KEY", "minioadmin")
MINIO_CONTAINER_NAME = os.environ.get("TEST_MINIO_CONTAINER", "test-minio-s3")
MINIO_ENDPOINT_URL = f"http://{MINIO_HOST}:{MINIO_PORT}"

# Operating system detection
IS_MACOS = os.uname().sysname == 'Darwin' if hasattr(os, 'uname') else False
IS_WINDOWS = os.name == 'nt'

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
    """Stop the MinIO container and remove its data volume for a clean shutdown."""
    container_name = MINIO_CONTAINER_NAME
    volume_name = f"{container_name}-data"

    if not is_container_running(container_name):
        logger.info(f"Container '{container_name}' is not running.")
        # Attempt to remove the volume in case it was left orphaned
        try:
            subprocess.run(["docker", "volume", "rm", volume_name], check=False, capture_output=True)
        except Exception:
            pass  # Ignore errors if volume doesn't exist
        return True

    logger.info(f"Stopping and cleaning up container '{container_name}' and volume '{volume_name}'...")
    try:
        # Stop and remove the container
        subprocess.run(["docker", "stop", container_name], check=True, capture_output=True)
        subprocess.run(["docker", "rm", container_name], check=True, capture_output=True)

        # Remove the associated volume to clear all data
        subprocess.run(["docker", "volume", "rm", volume_name], check=True, capture_output=True)

        logger.info("✅ MinIO container and data volume successfully removed.")
        return True
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode()
        if "no such volume" in stderr:
            logger.info("Volume was already removed or never created.")
            return True
        logger.error(f"❌ Failed to clean up MinIO resources: {stderr}")
        return False


def create_test_buckets():
    """Create test buckets and populate with sample data"""
    client = get_minio_client()

    # Create test buckets
    buckets = ['test-bucket', 'flowfile-test', 'sample-data', 'worker-test-bucket', 'demo-bucket']
    for bucket in buckets:
        try:
            client.create_bucket(Bucket=bucket)
            logger.info(f"Created bucket: {bucket}")
        except client.exceptions.BucketAlreadyExists:
            logger.info(f"Bucket already exists: {bucket}")
        except client.exceptions.BucketAlreadyOwnedByYou:
            logger.info(f"Bucket already owned: {bucket}")


def is_docker_available() -> bool:
    """
    Check if Docker is available on the system.

    Returns:
        bool: True if Docker is available and working, False otherwise
    """
    # Skip Docker on macOS and Windows in CI
    if (IS_MACOS or IS_WINDOWS) and os.environ.get('CI', '').lower() in ('true', '1', 'yes'):
        logger.info("Skipping Docker on macOS/Windows in CI environment")
        return False

    # If docker executable is not in PATH
    if shutil.which("docker") is None:
        logger.warning("Docker executable not found in PATH")
        return False

    # Try a simple docker command
    try:
        result = subprocess.run(
            ["docker", "info"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,
            check=False  # Don't raise exception on non-zero return code
        )

        if result.returncode != 0:
            logger.warning("Docker is not operational")
            return False

        return True
    except (subprocess.SubprocessError, OSError):
        logger.warning("Error running Docker command")
        return False


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
            populate_test_data(endpoint_url=MINIO_ENDPOINT_URL,
                               access_key=MINIO_ACCESS_KEY,
                               secret_key=MINIO_SECRET_KEY,
                               bucket_name="test-bucket")
            create_demo_data(endpoint_url=MINIO_ENDPOINT_URL,
                               access_key=MINIO_ACCESS_KEY,
                               secret_key=MINIO_SECRET_KEY,
                               bucket_name="demo-bucket")
            return True
        return False

    except Exception as e:
        logger.error(f"Failed to start MinIO: {e}")
        stop_minio_container()
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
