# flowfile_core/flowfile_core/configs/settings.py
import argparse
import os
import platform
import tempfile

from passlib.context import CryptContext
from starlette.config import Config

from flowfile_core.configs.utils import MutableBool
from shared.storage_config import storage

# Constants for server and worker configuration
DEFAULT_SERVER_HOST = "0.0.0.0"
DEFAULT_SERVER_PORT = 63578
DEFAULT_WORKER_PORT = 63579

# Single file mode flag, this determines where worker requests are being send to.
SINGLE_FILE_MODE: MutableBool = MutableBool(os.environ.get("FLOWFILE_SINGLE_FILE_MODE", "0") == "1")

# Offload to worker flag, this determines if the worker should handle processing tasks.
OFFLOAD_TO_WORKER: MutableBool = MutableBool(os.environ.get("FLOWFILE_OFFLOAD_TO_WORKER", "1") == "1")

# AI subsystem master switch — gates the entire `/ai/*` router (W17).
#
# DEV DEFAULT: hardcoded True so the AI tab is always on during build-out.
# Plan §10 specifies off-by-default for Phase 0 release.
#
# To flip for going-live: comment out the `"true"` literal below, uncomment
# the env-var line, and set `FEATURE_FLAG_AI=false` in the production env.
# Accepts truthy strings case-insensitively (`true|1|yes|on`); anything else
# is False.
#
# Mutable so the W18 admin endpoint and test fixtures can flip it via
# `settings.FEATURE_FLAG_AI.set(...)` without re-importing — `is_ai_enabled()`
# reads the live value on every call.
FEATURE_FLAG_AI: MutableBool = MutableBool(
    # os.environ.get("FEATURE_FLAG_AI", "0").strip().lower() in ("true", "1", "yes", "on")
    "true"
)

# AI prompt logging — dev / debugging hatch (W59).
#
# When True, every `LiteLLMProvider.chat` / `.stream` call writes a single
# JSONL line to `{FLOWFILE_STORAGE_DIR}/ai_prompts/{YYYY-MM-DD}.jsonl`
# capturing the full system prompt, message history, tool catalog, and
# response. Off by default — production runs stay silent. Same parsing
# pattern as `FEATURE_FLAG_AI`; `MutableBool` so the W18-style admin path
# (or test fixtures) can flip without re-import.
FLOWFILE_AI_LOG_PROMPTS: MutableBool = MutableBool(
    os.environ.get("FLOWFILE_AI_LOG_PROMPTS", "0").strip().lower() in ("true", "1", "yes", "on")
)

# Optional: scrub user / tool messages through the W25 PII regex pipeline
# before writing to the log. Off by default (the whole point of the log is
# to see what the model saw). Opt in when you'd rather keep customer-shaped
# fixtures readable in transcripts that get checked in / shared. System
# prompts and assistant responses are never scrubbed.
FLOWFILE_AI_LOG_PROMPTS_SCRUB: MutableBool = MutableBool(
    os.environ.get("FLOWFILE_AI_LOG_PROMPTS_SCRUB", "0").strip().lower() in ("true", "1", "yes", "on")
)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Flowfile Backend Server")
    parser.add_argument("--host", type=str, default=DEFAULT_SERVER_HOST, help="Host to bind to")
    parser.add_argument("--port", type=int, default=DEFAULT_SERVER_PORT, help="Port to bind to")
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
        worker_port = os.getenv("FLOWFILE_WORKER_PORT", DEFAULT_WORKER_PORT)

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
WORKER_PORT = (
    args.worker_port if args.worker_port is not None else int(os.getenv("FLOWFILE_WORKER_PORT", DEFAULT_WORKER_PORT))
)
WORKER_HOST = os.getenv("WORKER_HOST", "0.0.0.0" if platform.system() != "Windows" else "127.0.0.1")

config = Config(".env")
DEBUG: bool = config("DEBUG", cast=bool, default=False)
FILE_LOCATION = config("FILE_LOCATION", cast=str, default=".\\files\\")
AVAILABLE_RAM = config("AVAILABLE_RAM", cast=int, default=8)
WORKER_URL = config("FLOWFILE_WORKER_URL", cast=str, default=get_default_worker_url(WORKER_PORT))
TEMP_DIR = storage.temp_directory

# FLOWFILE_MODE: Determines the runtime environment
# Possible values: "electron" (desktop app), "package" (Python package), "docker" (container)
FLOWFILE_MODE = os.getenv("FLOWFILE_MODE", "electron")


def is_docker_mode() -> bool:
    """Check if running in Docker container mode"""
    return FLOWFILE_MODE == "docker"


def is_electron_mode() -> bool:
    """Check if running in Electron desktop app mode"""
    return FLOWFILE_MODE == "electron"


def is_package_mode() -> bool:
    """Check if running as Python package"""
    return FLOWFILE_MODE == "package"


# Legacy compatibility - will be removed in future versions
IS_RUNNING_IN_DOCKER = is_docker_mode()

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 120
PWD_CONTEXT = CryptContext(schemes=["bcrypt"], deprecated="auto")

GOOGLE_OAUTH_CLIENT_ID = os.getenv("GOOGLE_OAUTH_CLIENT_ID", "")
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET", "")
GOOGLE_OAUTH_REDIRECT_URI = os.getenv(
    "GOOGLE_OAUTH_REDIRECT_URI",
    f"http://localhost:{SERVER_PORT}/ga_connections/oauth/callback",
)
