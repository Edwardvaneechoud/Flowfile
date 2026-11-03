import argparse
import logging
import multiprocessing
import os
import platform

logging.basicConfig(format="%(asctime)s: %(message)s")
logger = logging.getLogger("FlowfileWorker")
logger.setLevel(logging.INFO)

DEFAULT_SERVICE_HOST = "127.0.0.1"
JOINING_SERVICE_HOST = "0.0.0.0"
DEFAULT_SERVICE_PORT = 63579
# Check environment variable for core host (used in Docker mode)
DEFAULT_CORE_HOST = os.environ.get("CORE_HOST", "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1")
DEFAULT_CORE_PORT = int(os.environ.get("CORE_PORT", 63578))
TEST_MODE = True if "TEST_MODE" in os.environ else False


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Flowfile Worker Server")
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help=f"Host to bind worker to ({DEFAULT_SERVICE_HOST} when not given; {JOINING_SERVICE_HOST}, every interface, "
        "for a worker joining a hub)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"Port to bind worker to ({DEFAULT_SERVICE_PORT} when not given; a worker joining a hub moves to a free "
        "port when that one is in use)",
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
    parser.add_argument(
        "--join",
        type=str,
        default=os.environ.get("FLOWFILE_HUB_JOIN_TOKEN") or None,
        help="A join token from the hub's Compute page; the worker joins that hub before it serves",
    )
    parser.add_argument("--hub", type=str, default=os.environ.get("FLOWFILE_HUB_URL") or None, help="The hub to join")
    parser.add_argument(
        "--name",
        type=str,
        default=os.environ.get("FLOWFILE_NODE_NAME") or None,
        help="This worker's name on the hub; the machine's name when not given",
    )
    parser.add_argument("--url", type=str, default=None, help="This worker's address as desktops reach it")

    # Use known_args to handle PyInstaller's extra args
    args = parser.parse_known_args()[0]

    # A worker joins a hub to be reached by it and by desktops, so a join token binds every interface by default.
    if args.host is None:
        args.host = JOINING_SERVICE_HOST if args.join else DEFAULT_SERVICE_HOST

    if args.port is not None and (args.port < 1 or args.port > 65535):
        raise ValueError(f"Invalid port number: {args.port}. Port must be between 1 and 65535.")

    if args.core_port < 1 or args.core_port > 65535:
        raise ValueError(f"Invalid core port number: {args.core_port}. Port must be between 1 and 65535.")

    if not args.host:
        raise ValueError("Worker host cannot be empty")

    if not args.core_host:
        raise ValueError("Core host cannot be empty")

    if args.join and not args.hub:
        raise ValueError("--join needs --hub, the address of the hub to join")

    return args


def get_core_url(host, port):
    """
    Get the core URL based on provided host and port

    Args:
        host: Core service host
        port: Core service port
    """
    return f"http://{host}:{port}"


def _is_main_process():
    """Check if we're running in the main process (not a spawned child)"""
    return multiprocessing.current_process().name == "MainProcess"


# Only parse args in the main process; the server's run() logs what it was configured with
# Spawned child processes will use environment variables or defaults
if _is_main_process():
    args = parse_args()

    SERVICE_HOST = args.host
    SERVICE_PORT = DEFAULT_SERVICE_PORT if args.port is None else args.port
    PORT_GIVEN = args.port is not None
    CORE_HOST = args.core_host
    CORE_PORT = args.core_port
    JOIN_TOKEN = args.join
    HUB_URL = args.hub
    NODE_NAME = args.name
    NODE_URL = args.url
else:
    # In spawned processes, use defaults from environment variables
    SERVICE_HOST = DEFAULT_SERVICE_HOST
    SERVICE_PORT = DEFAULT_SERVICE_PORT
    PORT_GIVEN = False
    CORE_HOST = DEFAULT_CORE_HOST
    CORE_PORT = DEFAULT_CORE_PORT
    JOIN_TOKEN = None
    HUB_URL = None
    NODE_NAME = None
    NODE_URL = None

FLOWFILE_CORE_URI = get_core_url(CORE_HOST, CORE_PORT)
