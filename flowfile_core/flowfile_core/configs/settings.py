
# flowfile_core/flowfile_core/configs/settings.py
import platform
import os
import tempfile
import argparse

from databases import DatabaseURL
from passlib.context import CryptContext
from starlette.config import Config
from starlette.datastructures import Secret


# Constants for server and worker configuration
DEFAULT_SERVER_HOST = "0.0.0.0"
DEFAULT_SERVER_PORT = 63578
DEFAULT_WORKER_PORT = 63579
SINGLE_FILE_MODE: bool = os.environ.get("SINGLE_FILE_MODE", "0") == "1"


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Flowfile Backend Server")
    parser.add_argument(
        "--host", type=str, default=DEFAULT_SERVER_HOST, help="Host to bind to"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_SERVER_PORT, help="Port to bind to"
    )
    parser.add_argument(
        "--worker-port",
        type=int,
        help="Port for the worker process",
    )
    args = parser.parse_known_args()[0]

    return args


def get_temp_dir() -> str:
    """Get the appropriate temp directory path based on environment"""
    # Check for Docker environment variable first
    docker_temp = os.getenv("TEMP_DIR")
    if docker_temp:
        return docker_temp

    return tempfile.gettempdir()


def get_default_worker_url(worker_port=None):
    """
    Get the default worker URL based on environment and settings

    Args:
        worker_port: Optional port override (used when passed as command line arg)
    """
    # Check for Docker environment first
    worker_host = os.getenv("WORKER_HOST", None)

    if worker_port is None:
        worker_port = os.getenv("WORKER_PORT", DEFAULT_WORKER_PORT)

    # Convert to int if it's a string
    worker_port = int(worker_port) if isinstance(worker_port, str) else worker_port

    if worker_host:
        worker_url = f"http://{worker_host}:{worker_port}"

    elif platform.system() == "Windows":
        worker_url = f"http://127.0.0.1:{worker_port}"
    else:
        worker_url = f"http://0.0.0.0:{worker_port}"
    worker_url += "/worker" if SINGLE_FILE_MODE else ""
    return worker_url


args = parse_args()

SERVER_HOST = args.host if args.host is not None else DEFAULT_SERVER_HOST
SERVER_PORT = args.port if args.port is not None else DEFAULT_SERVER_PORT
WORKER_PORT = args.worker_port if args.worker_port is not None else int(os.getenv("WORKER_PORT", DEFAULT_WORKER_PORT))
# Worker configuration
WORKER_HOST = os.getenv("WORKER_HOST", "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1")

config = Config(".env")
DEBUG: bool = config("DEBUG", cast=bool, default=False)
FILE_LOCATION = config("FILE_LOCATION", cast=str, default=".\\files\\")
AVAILABLE_RAM = config("AVAILABLE_RAM", cast=int, default=8)
WORKER_URL = config("WORKER_URL", cast=str, default=get_default_worker_url(WORKER_PORT))
IS_RUNNING_IN_DOCKER = os.getenv('RUNNING_IN_DOCKER', 'false').lower() == 'true'
TEMP_DIR = get_temp_dir()

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 120
PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")