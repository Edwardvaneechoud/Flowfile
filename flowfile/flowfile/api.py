# flowfile/api.py

import uuid
import time
import os
import requests
import subprocess
import sys
import atexit
import logging
import webbrowser
import shutil
import platform

from pathlib import Path
from typing import Optional, Dict, Any, Union, Tuple, List
from subprocess import Popen
from flowfile_core.flowfile.flow_graph import FlowGraph
from tempfile import TemporaryDirectory

# Configuration
FLOWFILE_HOST: str = os.environ.get("FLOWFILE_HOST", "127.0.0.1")
FLOWFILE_PORT: int = int(os.environ.get("FLOWFILE_PORT", 63578))
FLOWFILE_BASE_URL: str = f"http://{FLOWFILE_HOST}:{FLOWFILE_PORT}"
DEFAULT_MODULE_NAME: str = os.environ.get("FLOWFILE_MODULE_NAME", "flowfile")
FORCE_POETRY: bool = os.environ.get("FORCE_POETRY", "").lower() in ("true", "1", "yes")
POETRY_PATH: str = os.environ.get("POETRY_PATH", "poetry")

logger: logging.Logger = logging.getLogger(__name__)

# Global variable to track the managed server process
_server_process: Optional[Popen] = None


def is_flowfile_running() -> bool:
    """Check if the Flowfile core API endpoint is responsive."""
    try:
        response: requests.Response = requests.get(f"{FLOWFILE_BASE_URL}/docs", timeout=1)
        return 200 <= response.status_code < 300
    except (requests.ConnectionError, requests.Timeout):
        return False
    except Exception as e:
        logger.error(f"Unexpected error checking Flowfile status: {e}")
        return False


def stop_flowfile_server_process() -> None:
    """Stop the Flowfile server process if it was started by this module."""
    global _server_process
    if _server_process and _server_process.poll() is None:
        logger.info(f"Stopping managed Flowfile server process (PID: {_server_process.pid})...")

        # Windows-specific handling
        if platform.system() == "Windows":
            try:
                # On Windows, use subprocess to kill the process tree
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(_server_process.pid)],
                    capture_output=True,
                    check=False
                )
                logger.info("Server process terminated (Windows).")
            except Exception as e:
                logger.error(f"Error during Windows process termination: {e}")
                # Fallback to standard terminate/kill
                try:
                    _server_process.terminate()
                    _server_process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    _server_process.kill()
                    _server_process.wait()
        else:
            # Unix-like systems
            try:
                # Try SIGTERM first
                _server_process.terminate()
                _server_process.wait(timeout=5)
                logger.info("Server process terminated gracefully.")
            except subprocess.TimeoutExpired:
                logger.warning("Server process did not terminate gracefully, killing...")
                _server_process.kill()
                _server_process.wait()
                logger.info("Server process killed.")
            except Exception as e:
                logger.error(f"Error during server process termination: {e}")

        _server_process = None

        # Wait for the port to be released
        max_wait = 10
        start_time = time.time()
        while time.time() - start_time < max_wait:
            if not is_flowfile_running():
                break
            time.sleep(0.5)

        if is_flowfile_running():
            logger.warning("Server may still be running after termination attempt")


def is_poetry_environment() -> bool:
    """
    Detect if we're running in a Poetry environment by checking:
    1. If pyproject.toml exists up the directory tree
    2. If VIRTUAL_ENV points to a poetry environment
    3. If POETRY_ACTIVE environment variable is set
    """
    # Check if explicitly set via env variable
    if FORCE_POETRY:
        return True

    # Check if POETRY_ACTIVE is set
    if os.environ.get("POETRY_ACTIVE", "").lower() in ("true", "1", "yes"):
        return True

    # Check if we're in a poetry virtual environment
    venv_path = os.environ.get("VIRTUAL_ENV", "")
    if venv_path and (
            "poetry" in venv_path.lower() or
            Path(venv_path).joinpath(".poetry-venv").exists()
    ):
        return True

    # Look for pyproject.toml with poetry section
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents]:
        pyproject = parent / "pyproject.toml"
        if pyproject.exists():
            try:
                with open(pyproject, "r") as f:
                    content = f.read()
                    if "[tool.poetry]" in content:
                        return True
            except Exception:
                pass

    return False


def is_command_available(command: str) -> bool:
    """Check if a command is available in the PATH."""
    return shutil.which(command) is not None


def build_server_command(module_name: str) -> List[str]:
    """
    Build the appropriate command to start the server based on environment detection.
    Tries Poetry first if in a Poetry environment, falls back to direct module execution.
    """
    if is_poetry_environment():
        logger.info("Poetry environment detected.")
        if is_command_available(POETRY_PATH):
            logger.info(f"Using Poetry to run {module_name}")
            command = [
                POETRY_PATH,
                "run",
                module_name,
                "run",
                "ui",
                "--no-browser",
            ]
            return command
        else:
            logger.warning(f"Poetry command not found at '{POETRY_PATH}'. Falling back to Python module.")

    # Case 2: Fallback to direct script execution
    logger.info("Falling back to direct script execution.")
    python_parent_dir = Path(sys.executable).parent
    command: List[str]
    scripts_dir = Path(sys.executable).parent

    if platform.system() == "Windows":
        exe_path = scripts_dir / f"{module_name}.exe"
        script_py_path = scripts_dir / f"{module_name}-script.py"
        plain_script_path = scripts_dir / module_name

        if exe_path.exists():
            logger.info(f"Using .exe wrapper: {exe_path}")
            command = [str(exe_path), "run", "ui", "--no-browser"]
        elif script_py_path.exists():
            logger.info(f"Using '-script.py' with interpreter: {script_py_path}")
            command = [sys.executable, str(script_py_path), "run", "ui", "--no-browser"]
        elif plain_script_path.exists():
            logger.info(f"Using plain script with interpreter: {plain_script_path}")
            command = [sys.executable, str(plain_script_path), "run", "ui", "--no-browser"]
        else:
            raise FileNotFoundError(
                f"Could not find an executable script for '{module_name}' in '{scripts_dir}'. "
                f"Checked for '{exe_path.name}', '{script_py_path.name}', and '{plain_script_path.name}'. "
                "Ensure the package is installed correctly."
            )
    else:
        # On Unix-like systems, the script in 'bin' is directly executable
        script_path = python_parent_dir / "bin" / module_name
        if not script_path.exists():
            script_path = python_parent_dir / module_name # Fallback for different venv structures

        logger.info(f"Using direct script execution path: {script_path}")
        command = [str(script_path), "run", "ui", "--no-browser"]

    logger.info(f"Built server command: {command}")
    return command


def check_if_in_single_mode() -> bool:
    try:
        response: requests.Response = requests.get(f"{FLOWFILE_BASE_URL}/single_mode", timeout=1)
        if response.ok:
            return response.json() == "1"
    except Exception:
        pass
    return False


def start_flowfile_server_process(module_name: str = DEFAULT_MODULE_NAME) -> Tuple[bool, bool]:
    """
    Start the Flowfile server as a background process if it's not already running.
    Automatically detects and uses Poetry if in a Poetry environment.

    Parameters:
    module_name: The module name to run. Defaults to the value from environment
                 variable or "flowfile".
    """
    global _server_process
    if is_flowfile_running():
        return True, check_if_in_single_mode()

    if _server_process and _server_process.poll() is None:
        logger.warning("Server process object exists but API not responding. Attempting to restart.")
        stop_flowfile_server_process()

    # Build command automatically based on environment detection
    command = build_server_command(module_name)
    logger.info(f"Starting server with command: {' '.join(command)}")
    try:
        # Windows-specific subprocess creation
        if platform.system() == "Windows":
            # Use CREATE_NEW_PROCESS_GROUP flag on Windows
            _server_process = Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
            )
        else:
            _server_process = Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )

        logger.info(f"Started server process with PID: {_server_process.pid}")

        atexit.register(stop_flowfile_server_process)

        logger.info("Waiting for server to initialize...")
        print("Starting the ui service")
        for i in range(60):
            if is_flowfile_running():
                print("Flowfile UI started.")
                logger.info("Server started successfully.")
                return True, check_if_in_single_mode()
            if i % 5 == 0 and i > 0:
                print("Waiting for Flowfile UI to start...")
            time.sleep(1)
        else:
            logger.error("Failed to start server: API did not become responsive within 60 seconds. "
                         "Try again or start service by running\n"
                         "flowfile run ui")
            if _server_process and _server_process.stderr:
                try:
                    stderr_output: str = _server_process.stderr.read().decode(errors='ignore')
                    logger.error(f"Server process stderr:\n{stderr_output[:1000]}...")
                except Exception as read_err:
                    logger.error(f"Could not read stderr from server process: {read_err}")
            stop_flowfile_server_process()
            return False, check_if_in_single_mode()

    except FileNotFoundError:
        logger.error(f"Error: Could not execute command: '{' '.join(command)}'.")
        logger.error(f"Ensure '{module_name}' is installed correctly.")
        _server_process = None
        return False, False
    except Exception as e:
        logger.error(f"An unexpected error occurred while starting the server process: {e}")
        if _server_process and _server_process.stderr:
            try:
                stderr_output = _server_process.stderr.read().decode(errors='ignore')
                logger.error(f"Server process stderr:\n{stderr_output[:1000]}...")
            except Exception as read_err:
                logger.error(f"Could not read stderr from server process: {read_err}")
        stop_flowfile_server_process()
        _server_process = None
        return False, False


def get_auth_token() -> Optional[str]:
    """Get an authentication token from the Flowfile API."""
    try:
        response: requests.Response = requests.post(
            f"{FLOWFILE_BASE_URL}/auth/token",
            json={},
            timeout=5
        )
        response.raise_for_status()
        token_data: Dict[str, Any] = response.json()
        access_token: Optional[str] = token_data.get("access_token")
        if not access_token:
            logger.error("Auth token endpoint succeeded but 'access_token' was missing in response.")
            return None
        return access_token

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get auth token: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred getting auth token: {e}")
        return None


def import_flow_to_editor(flow_path: Path, auth_token: str) -> Optional[int]:
    """Import the flow into the Flowfile editor using the API endpoint."""
    if not flow_path.is_file():
        logger.error(f"Flow file not found: {flow_path}")
        return None
    if not auth_token:
        logger.error("Cannot import flow without auth token.")
        return None

    try:
        headers: Dict[str, str] = {"Authorization": f"Bearer {auth_token}"}
        params: Dict[str, str] = {"flow_path": str(flow_path)}

        response: requests.Response = requests.get(
            f"{FLOWFILE_BASE_URL}/import_flow/",
            params=params,
            headers=headers,
            timeout=10
        )
        response.raise_for_status()

        flow_id_data: Union[int, Dict[str, Any], Any] = response.json()
        flow_id: Optional[int] = None

        if isinstance(flow_id_data, int):
            flow_id = flow_id_data
        elif isinstance(flow_id_data, dict) and "flow_id" in flow_id_data:
            flow_id = int(flow_id_data["flow_id"])
        else:
            logger.error(f"Unexpected response format from import_flow endpoint: {flow_id_data}")
            return None

        logger.info(f"Flow '{flow_path.name}' imported successfully with ID: {flow_id}")
        return flow_id

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to import flow: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Server response: {e.response.status_code} - {e.response.text[:500]}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred importing flow: {e}")
        return None


def _save_flow_to_location(
        flow_graph: FlowGraph, storage_location: Optional[str]
) -> Tuple[Optional[Path], Optional[TemporaryDirectory]]:
    """Handles graph saving, path resolution, and temporary directory creation."""
    temp_dir_obj: Optional[TemporaryDirectory] = None
    flow_file_path: Path
    try:
        if storage_location:
            flow_file_path = Path(storage_location).resolve()
            flow_file_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            temp_dir_obj = TemporaryDirectory(prefix="flowfile_graph_")
            flow_file_path = Path(temp_dir_obj.name) / f"temp_flow_{uuid.uuid4().hex[:8]}.flowfile"

        logger.info(f"Applying layout and saving flow to: {flow_file_path}")
        flow_graph.apply_layout()
        flow_graph.save_flow(str(flow_file_path))
        logger.info("Flow saved successfully.")
        return flow_file_path, temp_dir_obj
    except Exception as e:
        logger.error(f"Failed to save flow graph to location '{storage_location}': {e}", exc_info=True)
        if temp_dir_obj:
            try:
                temp_dir_obj.cleanup()
                logger.info(f"Cleaned up temp dir {temp_dir_obj.name} after save failure.")
            except Exception as cleanup_err:
                logger.error(f"Error during immediate cleanup of temp dir: {cleanup_err}")
        return None, None


def _open_flow_in_browser(flow_id: int) -> None:
    """Opens the specified flow ID in a browser tab if in unified mode."""
    if os.environ.get("FLOWFILE_MODE") == "electron":
        flow_url = f"http://{FLOWFILE_HOST}:{FLOWFILE_PORT}/ui/flow/{flow_id}"
        logger.info(f"Unified mode detected. Opening imported flow in browser: {flow_url}")
        try:
            time.sleep(0.5)
            webbrowser.open_new_tab(flow_url)
        except Exception as wb_err:
            logger.warning(f"Could not automatically open browser tab: {wb_err}")
    else:
        logger.info("Not in unified mode ('electron'), browser will not be opened automatically.")


def _cleanup_temporary_storage(temp_dir_obj: Optional[TemporaryDirectory]) -> None:
    """Safely cleans up the temporary directory if one was created."""
    if temp_dir_obj:
        try:
            temp_dir_obj.cleanup()
            logger.info(f"Cleaned up temporary directory: {temp_dir_obj.name}")
        except Exception as e:
            logger.error(f"Error cleaning up temporary directory {temp_dir_obj.name}: {e}")


def open_graph_in_editor(flow_graph: FlowGraph, storage_location: Optional[str] = None,
                         module_name: str = DEFAULT_MODULE_NAME, automatically_open_browser: bool = True) -> bool:
    """
    Save the ETL graph, ensure the Flowfile server is running (starting it
    if necessary), import the graph via API, and open it in a new browser
    tab if running in unified mode.

    Parameters:
    flow_graph: The FlowGraph object to save and open.
    storage_location: Optional path to save the .flowfile. If None,
                     a temporary file is used.
    module_name: The module name to run if server needs to be started.
                Use your Poetry package name if not using "flowfile".
    automatically_open_browser: If True, attempts to open the flow in a browser tab
    Returns:
    True if the graph was successfully imported, False otherwise.
    """
    temp_dir_obj: Optional[TemporaryDirectory] = None
    try:
        original_execution_settings = flow_graph.flow_settings.model_copy()
        flow_graph.flow_settings.execution_location = "auto"
        flow_graph.flow_settings.execution_mode = "Development"
        flow_file_path, temp_dir_obj = _save_flow_to_location(flow_graph, storage_location)
        if not flow_file_path:
            return False
        flow_graph.flow_settings = original_execution_settings
        flow_running, flow_in_single_mode = start_flowfile_server_process(module_name)
        if not flow_running:
            return False

        auth_token = get_auth_token()
        if not auth_token:
            return False

        flow_id = import_flow_to_editor(flow_file_path, auth_token)

        if flow_id is not None:
            if flow_in_single_mode and automatically_open_browser:
                _open_flow_in_browser(flow_id)
            return True
        else:
            return False

    except Exception as e:
        logger.error(f"An unexpected error occurred in open_graph_in_editor: {e}", exc_info=True)
        return False
    finally:
        _cleanup_temporary_storage(temp_dir_obj)