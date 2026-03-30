import logging
import os
import shutil
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager

import requests

logger = logging.getLogger("azurite_fixture")

AZURITE_HOST = os.environ.get("TEST_AZURITE_HOST", "localhost")
AZURITE_BLOB_PORT = int(os.environ.get("TEST_AZURITE_BLOB_PORT", 10000))
AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
AZURITE_CONTAINER_NAME = os.environ.get("TEST_AZURITE_CONTAINER", "test-azurite")
AZURITE_BLOB_ENDPOINT = f"http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}/{AZURITE_ACCOUNT_NAME}"
AZURITE_CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=http;"
    f"AccountName={AZURITE_ACCOUNT_NAME};"
    f"AccountKey={AZURITE_ACCOUNT_KEY};"
    f"BlobEndpoint=http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}/{AZURITE_ACCOUNT_NAME}"
)

# OS detection
IS_MACOS = os.uname().sysname == "Darwin" if hasattr(os, "uname") else False
IS_WINDOWS = os.name == "nt"


def get_blob_service_client():
    """Get Azure BlobServiceClient for Azurite."""
    from azure.storage.blob import BlobServiceClient

    return BlobServiceClient.from_connection_string(AZURITE_CONNECTION_STRING)


def wait_for_azurite(max_retries=30, interval=1):
    """Wait for Azurite to be ready."""
    for i in range(max_retries):
        try:
            resp = requests.get(f"http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}/{AZURITE_ACCOUNT_NAME}?comp=list", timeout=2)
            if resp.status_code in (200, 403):
                logger.info("Azurite is ready")
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


def stop_azurite_container() -> bool:
    """Stop the Azurite container."""
    container_name = AZURITE_CONTAINER_NAME
    if not is_container_running(container_name):
        logger.info(f"Container '{container_name}' is not running.")
        return True
    logger.info(f"Stopping container '{container_name}'...")
    try:
        subprocess.run(["docker", "stop", container_name], check=True, capture_output=True)
        subprocess.run(["docker", "rm", container_name], check=True, capture_output=True)
        logger.info("Azurite container successfully removed.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to clean up Azurite: {e.stderr.decode()}")
        return False


def create_test_containers():
    """Create test blob containers in Azurite."""
    client = get_blob_service_client()
    containers = ["test-container", "flowfile-test", "sample-data", "worker-test-container"]
    for container in containers:
        try:
            client.create_container(container)
            logger.info(f"Created container: {container}")
        except Exception as e:
            if "ContainerAlreadyExists" in str(e):
                logger.info(f"Container already exists: {container}")
            else:
                logger.warning(f"Failed to create container {container}: {e}")


def start_azurite_container() -> bool:
    """Start Azurite container."""
    if is_container_running(AZURITE_CONTAINER_NAME):
        logger.info(f"Container {AZURITE_CONTAINER_NAME} is already running")
        return True

    try:
        subprocess.run(
            [
                "docker", "run", "-d",
                "--name", AZURITE_CONTAINER_NAME,
                "-p", f"{AZURITE_BLOB_PORT}:10000",
                "mcr.microsoft.com/azure-storage/azurite",
                "azurite-blob", "--blobHost", "0.0.0.0", "--blobPort", "10000",
            ],
            check=True,
        )

        if wait_for_azurite():
            create_test_containers()
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to start Azurite: {e}")
        stop_azurite_container()
        return False


@contextmanager
def managed_azurite() -> Generator[dict[str, any], None, None]:
    """Context manager for Azurite container with full connection info."""
    if not start_azurite_container():
        yield {}
        return

    try:
        connection_info = {
            "account_name": AZURITE_ACCOUNT_NAME,
            "account_key": AZURITE_ACCOUNT_KEY,
            "blob_endpoint": AZURITE_BLOB_ENDPOINT,
            "connection_string": AZURITE_CONNECTION_STRING,
            "host": AZURITE_HOST,
            "blob_port": AZURITE_BLOB_PORT,
        }
        yield connection_info
    finally:
        if os.environ.get("KEEP_AZURITE_RUNNING", "false").lower() != "true":
            stop_azurite_container()
