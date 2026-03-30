import logging
import os
import shutil
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager

import requests

logger = logging.getLogger("gcs_fixture")

GCS_HOST = os.environ.get("TEST_GCS_HOST", "localhost")
GCS_PORT = int(os.environ.get("TEST_GCS_PORT", 4443))
GCS_CONTAINER_NAME = os.environ.get("TEST_GCS_CONTAINER", "test-fake-gcs")
GCS_ENDPOINT_URL = f"http://{GCS_HOST}:{GCS_PORT}"
GCS_EXTERNAL_URL = f"http://{GCS_HOST}:{GCS_PORT}"

# OS detection
IS_MACOS = os.uname().sysname == "Darwin" if hasattr(os, "uname") else False
IS_WINDOWS = os.name == "nt"


def get_gcs_client():
    """Get a Google Cloud Storage client pointing to fake-gcs-server."""
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    client = storage.Client(
        credentials=AnonymousCredentials(),
        project="test-project",
    )
    client._connection.API_BASE_URL = GCS_ENDPOINT_URL
    return client


def wait_for_gcs(max_retries=30, interval=1):
    """Wait for fake-gcs-server to be ready."""
    for i in range(max_retries):
        try:
            resp = requests.get(f"{GCS_ENDPOINT_URL}/storage/v1/b", timeout=2)
            if resp.status_code == 200:
                logger.info("fake-gcs-server is ready")
                return True
        except Exception:
            if i < max_retries - 1:
                time.sleep(interval)
            continue
    return False


def is_container_running(container_name: str) -> bool:
    """Check if a Docker container is already running."""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True,
        )
        return container_name in result.stdout.strip()
    except subprocess.CalledProcessError:
        return False


def is_docker_available() -> bool:
    """Check if Docker is available on the system."""
    if (IS_MACOS or IS_WINDOWS) and os.environ.get("CI", "").lower() in ("true", "1", "yes"):
        logger.info("Skipping Docker on macOS/Windows in CI environment")
        return False
    if shutil.which("docker") is None:
        logger.warning("Docker executable not found in PATH")
        return False
    try:
        result = subprocess.run(
            ["docker", "info"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5, check=False
        )
        return result.returncode == 0
    except (subprocess.SubprocessError, OSError):
        logger.warning("Error running Docker command")
        return False


def stop_fake_gcs_container() -> bool:
    """Stop the fake-gcs-server container."""
    container_name = GCS_CONTAINER_NAME
    if not is_container_running(container_name):
        logger.info(f"Container '{container_name}' is not running.")
        return True
    logger.info(f"Stopping container '{container_name}'...")
    try:
        subprocess.run(["docker", "stop", container_name], check=True, capture_output=True)
        subprocess.run(["docker", "rm", container_name], check=True, capture_output=True)
        logger.info("fake-gcs-server container successfully removed.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to clean up fake-gcs-server: {e.stderr.decode()}")
        return False


def create_test_buckets():
    """Create test buckets in fake-gcs-server."""
    client = get_gcs_client()
    buckets = ["test-bucket", "flowfile-test", "sample-data", "worker-test-bucket"]
    for bucket_name in buckets:
        try:
            client.create_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        except Exception as e:
            if "409" in str(e):
                logger.info(f"Bucket already exists: {bucket_name}")
            else:
                logger.warning(f"Failed to create bucket {bucket_name}: {e}")


def start_fake_gcs_container() -> bool:
    """Start fake-gcs-server container."""
    if is_container_running(GCS_CONTAINER_NAME):
        logger.info(f"Container {GCS_CONTAINER_NAME} is already running")
        return True

    try:
        subprocess.run(
            [
                "docker", "run", "-d",
                "--name", GCS_CONTAINER_NAME,
                "-p", f"{GCS_PORT}:4443",
                "fsouza/fake-gcs-server",
                "-scheme", "http",
                "-port", "4443",
                "-external-url", GCS_EXTERNAL_URL,
            ],
            check=True,
        )

        if wait_for_gcs():
            create_test_buckets()
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to start fake-gcs-server: {e}")
        stop_fake_gcs_container()
        return False


@contextmanager
def managed_gcs() -> Generator[dict[str, any], None, None]:
    """Context manager for fake-gcs-server container with full connection info."""
    if not start_fake_gcs_container():
        yield {}
        return

    try:
        connection_info = {
            "endpoint_url": GCS_ENDPOINT_URL,
            "host": GCS_HOST,
            "port": GCS_PORT,
            "project": "test-project",
        }
        yield connection_info
    finally:
        if os.environ.get("KEEP_GCS_RUNNING", "false").lower() != "true":
            stop_fake_gcs_container()
