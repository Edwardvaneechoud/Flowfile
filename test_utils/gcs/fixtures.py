import os
import time
import subprocess
import logging
from contextlib import contextmanager
from typing import Dict, Generator
import shutil
from google.cloud import storage
from google.auth.credentials import AnonymousCredentials
from test_utils.gcs.data_generator import populate_test_data
from test_utils.gcs.demo_data_generator import create_demo_data



logger = logging.getLogger("gcs_fixture")

GCS_HOST = os.environ.get("TEST_GCS_HOST", "localhost")
GCS_PORT = int(os.environ.get("TEST_GCS_PORT", 4443))
GCS_BUCKET_NAME = os.environ.get("TEST_GCS_CONTAINER", "test-gcs")
GCS_ENDPOINT_URL = f"https://{GCS_HOST}:{GCS_PORT}"
FAKE_GCS_SERVER_NAME = os.environ.get("TEST_GCS_CONTAINER", "test-gcs-server")
GCS_ROOT_USER = "GCS"
GCS_ROOT_PASSWORD = "gcsadmin"

# Operating system detection
IS_MACOS = os.uname().sysname == 'Darwin' if hasattr(os, 'uname') else False
IS_WINDOWS = os.name == 'nt'


def get_gcs_client():
    """Get google.storage client for GCS Server"""
    return storage.Client(
    client_options={"credentials": AnonymousCredentials(), "api_endpoint": GCS_ENDPOINT_URL},
)


def wait_for_gcs_server(max_retries=30, interval=1):
    """Wait for GCS Server to be ready"""
    for i in range(max_retries):
        try:
            client = get_gcs_client()
            client.list_buckets()
            logger.info("GCS Server is ready")
            return True
        except Exception:
            if i < max_retries - 1:
                time.sleep(interval)
            continue
    return False

def is_container_running(container_name: str) -> bool:
    """Check if Fake GCS Server container is already running"""
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


def stop_gcs_container() -> bool:
    """Stop the Fake GCS Server container and remove its data volume for a clean shutdown."""
    container_name = FAKE_GCS_SERVER_NAME
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

        logger.info("✅ GCS Server container and data volume successfully removed.")
        return True
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode()
        if "no such volume" in stderr:
            logger.info("Volume was already removed or never created.")
            return True
        logger.error(f"❌ Failed to clean up GCS Server resources: {stderr}")
        return False


def create_test_buckets():
    """Create test buckets and populate with sample data"""
    client = get_gcs_client()

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


def start_gcs_container() -> bool:
    """Start Fake GCS Server container with initialization"""
    if is_container_running(FAKE_GCS_SERVER_NAME):
        logger.info(f"Container {FAKE_GCS_SERVER_NAME} is already running")
        return True
    try:
        # Start GCS Server with volume for persistence
        subprocess.run([
            "docker", "run", "-d",
            "--name", FAKE_GCS_SERVER_NAME,
            "-p", f"{GCS_PORT}:4443",
            "-e", f"GCS_ROOT_USER={GCS_ROOT_USER}",
            "-e", f"GCS_ROOT_PASSWORD={GCS_ROOT_PASSWORD}",
            "-v", f"{FAKE_GCS_SERVER_NAME}-data:/data",
            "fsouza/fake-gcs-server", "server", "/data", "--console-address", ":4443", "-scheme", "https"
        ], check=True)

        # Wait for GCS Server to be ready
        if wait_for_gcs_server():
            create_test_buckets()
            populate_test_data(endpoint_url=GCS_ENDPOINT_URL,
                               bucket_name="test-bucket")
            create_demo_data(endpoint_url=GCS_ENDPOINT_URL,
                               bucket_name="demo-bucket")
            return True
        return False

    except Exception as e:
        logger.error(f"Failed to start GCS Server: {e}")
        stop_gcs_container()
        return False


@contextmanager
def managed_gcs() -> Generator[Dict[str, any], None, None]:
    """Context manager for GCS Server container with full connection info"""
    if not start_gcs_container():
        yield {}
        return

    try:
        connection_info = {
            "endpoint_url": GCS_ENDPOINT_URL,
            "host": GCS_HOST,
            "port": GCS_PORT,
            "console_port": GCS_PORT,
            "connection_string": GCS_ENDPOINT_URL,
        }
        yield connection_info
    finally:
        # Optionally keep container running for debugging
        if os.environ.get("KEEP_GCS_RUNNING", "false").lower() != "true":
            stop_gcs_container()
