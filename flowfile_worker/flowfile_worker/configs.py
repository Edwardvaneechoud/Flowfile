# flowfile_worker.configs

import logging
import platform
import argparse
import os
from connectorx import __version__


# Configure logging
logging.basicConfig(format='%(asctime)s: %(message)s')
logger = logging.getLogger('FlowfileWorker')
logger.setLevel(logging.INFO)

# Constants for worker and core configuration
DEFAULT_SERVICE_HOST = "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1"
DEFAULT_SERVICE_PORT = 63579
DEFAULT_CORE_HOST = "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1"
DEFAULT_CORE_PORT = 63578
TEST_MODE = True if 'TEST_MODE' in os.environ else False


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Flowfile Worker Server")
    parser.add_argument(
        "--host", type=str, default=DEFAULT_SERVICE_HOST, help="Host to bind worker to"
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_SERVICE_PORT, help="Port to bind worker to"
    )
    parser.add_argument(
        "--core-host",
        type=str,
        default=DEFAULT_CORE_HOST,
        help="Host of the core service",
    )
    parser.add_argument(
        "--core-port",
        type=int,
        default=DEFAULT_CORE_PORT,
        help="Port of the core service",
    )

    # Use known_args to handle PyInstaller's extra args
    args = parser.parse_known_args()[0]

    # Validate arguments
    if args.port < 1 or args.port > 65535:
        raise ValueError(
            f"Invalid port number: {args.port}. Port must be between 1 and 65535."
        )

    if args.core_port < 1 or args.core_port > 65535:
        raise ValueError(
            f"Invalid core port number: {args.core_port}. Port must be between 1 and 65535."
        )

    # Check if hosts are valid (basic check)
    if not args.host:
        raise ValueError("Worker host cannot be empty")

    if not args.core_host:
        raise ValueError("Core host cannot be empty")

    return args


def get_core_url(host, port):
    """
    Get the core URL based on provided host and port

    Args:
        host: Core service host
        port: Core service port
    """
    return f"http://{host}:{port}"


# Parse arguments - defaults are already set in the argument parser
args = parse_args()

# These variables will already use defaults from argparse if not provided
SERVICE_HOST = args.host
SERVICE_PORT = args.port
CORE_HOST = args.core_host
CORE_PORT = args.core_port

# Generate the core URI
FLOWFILE_CORE_URI = get_core_url(CORE_HOST, CORE_PORT)

logger.info(f"ConnectorX version: {__version__}")
# Log configuration
logger.info(f"Worker configured at {SERVICE_HOST}:{SERVICE_PORT}")
logger.info(f"Core service configured at {FLOWFILE_CORE_URI}")