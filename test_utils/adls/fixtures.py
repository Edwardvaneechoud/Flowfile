import logging
import os
import shutil
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from test_utils.adls.data_generator import populate_test_data

logger = logging.getLogger("adls_fixture")

AZURITE_HOST = os.environ.get("TEST_AZURITE_HOST", "localhost")
AZURITE_BLOB_PORT = int(os.environ.get("TEST_AZURITE_BLOB_PORT", 10000))
AZURITE_QUEUE_PORT = int(os.environ.get("TEST_AZURITE_QUEUE_PORT", 10001))
AZURITE_TABLE_PORT = int(os.environ.get("TEST_AZURITE_TABLE_PORT", 10002))
AZURITE_ACCOUNT_NAME = os.environ.get("TEST_AZURITE_ACCOUNT_NAME", "devstoreaccount1")
AZURITE_ACCOUNT_KEY = os.environ.get(
    "TEST_AZURITE_ACCOUNT_KEY",
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
AZURITE_CONTAINER_NAME = os.environ.get("TEST_AZURITE_CONTAINER", "test-azurite-adls")
AZURITE_BLOB_ENDPOINT = f"http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}/{AZURITE_ACCOUNT_NAME}"

# Operating system detection
IS_MACOS = os.uname().sysname == "Darwin" if hasattr(os, "uname") else False
IS_WINDOWS = os.name == "nt"


def get_blob_service_client():
    """Get Azure Blob Service Client for Azurite"""
    connection_string = (
        f"DefaultEndpointsProtocol=http;"
        f"AccountName={AZURITE_ACCOUNT_NAME};"
        f"AccountKey={AZURITE_ACCOUNT_KEY};"
        f"BlobEndpoint=http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}/{AZURITE_ACCOUNT_NAME};"
    )
    return BlobServiceClient.from_connection_string(connection_string)


def wait_for_azurite(max_retries=30, interval=1):
    """Wait for Azurite to be ready"""
    for i in range(max_retries):
        try:
            client = get_blob_service_client()
            # Try to list containers to verify connection
            list(client.list_containers())
            logger.info("Azurite is ready")
            return True
        except Exception as e:
            if i < max_retries - 1:
                logger.debug(f"Waiting for Azurite... ({i+1}/{max_retries})")
                time.sleep(interval)
            else:
                logger.error(f"Failed to connect to Azurite after {max_retries} attempts: {e}")
            continue
    return False


def is_container_running(container_name: str) -> bool:
    """Check if Azurite container is already running"""
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


def stop_azurite_container() -> bool:
    """Stop the Azurite container and remove its data volume for a clean shutdown."""
    container_name = AZURITE_CONTAINER_NAME
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

        logger.info("✅ Azurite container and data volume successfully removed.")
        return True
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode() if e.stderr else ""
        if "no such volume" in stderr:
            logger.info("Volume was already removed or never created.")
            return True
        logger.error(f"❌ Failed to clean up Azurite resources: {stderr}")
        return False


def create_test_containers():
    """Create test containers and populate with sample data"""
    client = get_blob_service_client()

    # Create test containers
    containers = ["test-container", "flowfile-test", "sample-data", "worker-test-container", "demo-container"]
    for container in containers:
        try:
            client.create_container(container)
            logger.info(f"Created container: {container}")
        except Exception as e:
            if "ContainerAlreadyExists" in str(e):
                logger.info(f"Container already exists: {container}")
            else:
                logger.warning(f"Error creating container {container}: {e}")


def is_docker_available() -> bool:
    """
    Check if Docker is available on the system.

    Returns:
        bool: True if Docker is available and working, False otherwise
    """
    # Skip Docker on macOS and Windows in CI
    if (IS_MACOS or IS_WINDOWS) and os.environ.get("CI", "").lower() in ("true", "1", "yes"):
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
            check=False,  # Don't raise exception on non-zero return code
        )

        if result.returncode != 0:
            logger.warning("Docker is not operational")
            return False

        return True
    except (subprocess.SubprocessError, OSError):
        logger.warning("Error running Docker command")
        return False


def start_azurite_container() -> bool:
    """Start Azurite container with initialization"""
    if is_container_running(AZURITE_CONTAINER_NAME):
        logger.info(f"Container {AZURITE_CONTAINER_NAME} is already running")
        return True

    try:
        # Start Azurite with volume for persistence
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                AZURITE_CONTAINER_NAME,
                "-p",
                f"{AZURITE_BLOB_PORT}:10000",
                "-p",
                f"{AZURITE_QUEUE_PORT}:10001",
                "-p",
                f"{AZURITE_TABLE_PORT}:10002",
                "-v",
                f"{AZURITE_CONTAINER_NAME}-data:/data",
                "mcr.microsoft.com/azure-storage/azurite",
                "azurite-blob",
                "--blobHost",
                "0.0.0.0",
                "--blobPort",
                "10000",
                "-l",
                "/data",
            ],
            check=True,
        )

        # Wait for Azurite to be ready
        if wait_for_azurite():
            create_test_containers()
            populate_test_data(
                account_name=AZURITE_ACCOUNT_NAME,
                account_key=AZURITE_ACCOUNT_KEY,
                blob_endpoint=AZURITE_BLOB_ENDPOINT,
                container_name="test-container",
            )
            return True
        return False

    except Exception as e:
        logger.error(f"Failed to start Azurite: {e}")
        stop_azurite_container()
        return False


@contextmanager
def managed_azurite() -> Generator[dict[str, any], None, None]:
    """Context manager for Azurite container with full connection info"""
    if not start_azurite_container():
        yield {}
        return

    try:
        connection_info = {
            "account_name": AZURITE_ACCOUNT_NAME,
            "account_key": AZURITE_ACCOUNT_KEY,
            "blob_endpoint": AZURITE_BLOB_ENDPOINT,
            "host": AZURITE_HOST,
            "blob_port": AZURITE_BLOB_PORT,
            "queue_port": AZURITE_QUEUE_PORT,
            "table_port": AZURITE_TABLE_PORT,
        }
        yield connection_info
    finally:
        # Optionally keep container running for debugging
        if os.environ.get("KEEP_AZURITE_RUNNING", "false").lower() != "true":
            stop_azurite_container()
