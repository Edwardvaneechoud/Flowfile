import uuid
import time
import os
import requests
import subprocess
from pathlib import Path
from typing import Iterable, Any, List, Optional
from flowfile_core.flowfile.FlowfileFlow import FlowGraph
from flowfile_core.schemas import schemas
from tempfile import TemporaryDirectory


def _is_iterable(obj: Any) -> bool:
    # Avoid treating strings as iterables in this context
    return isinstance(obj, Iterable) and not isinstance(obj, (str, bytes))


def _parse_inputs_as_iterable(
        inputs: tuple[Any, ...] | tuple[Iterable[Any]],
) -> List[Any]:
    if not inputs:
        return []

    # Treat elements of a single iterable as separate inputs
    if len(inputs) == 1 and _is_iterable(inputs[0]):
        return list(inputs[0])

    return list(inputs)


def _generate_id() -> int:
    """Generate a simple unique ID for nodes."""
    return int(uuid.uuid4().int % 100000)


def create_flow_graph() -> FlowGraph:
    flow_id = _generate_id()
    flow_settings = schemas.FlowSettings(
        flow_id=flow_id,
        name=f"Flow_{flow_id}",
        path=f"flow_{flow_id}"
    )
    flow_graph = FlowGraph(flow_id=flow_id, flow_settings=flow_settings)
    flow_graph.flow_settings.execution_location = 'local'  # always create a local frame so that the run time does not attempt to use the flowfile_worker process
    return flow_graph


def is_flowfile_running() -> bool:
    """Check if the Flowfile application is running by testing its API endpoint."""
    try:
        response = requests.get("http://0.0.0.0:63578/docs", timeout=2)
        return response.status_code == 200
    except (requests.ConnectionError, requests.Timeout):
        return False


def start_flowfile_application() -> bool:
    """Start the Flowfile application on macOS."""
    try:
        # Attempt to start the Flowfile application
        subprocess.Popen(['open', '-a', 'Flowfile'],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

        # Wait for the application to start up (max 10 seconds)
        start_time = time.time()
        while time.time() - start_time < 10:
            if is_flowfile_running():
                return True
            time.sleep(0.5)  # Check every half second

        # If we get here, the app didn't start in time
        return False
    except Exception as e:
        print(f"Error starting Flowfile application: {e}")
        return False


def get_auth_token() -> Optional[str]:
    """Get an authentication token from the Flowfile API."""
    try:
        response = requests.post(
            "http://0.0.0.0:63578/auth/token",
            json={},  # Empty body as specified
            timeout=5
        )

        if response.status_code == 200:
            token_data = response.json()
            return token_data.get("access_token")
        else:
            print(f"Failed to get auth token: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error getting auth token: {e}")
        return None


def import_flow_to_editor(flow_path: str, auth_token: str) -> Optional[int]:
    """Import the flow into the Flowfile editor using the API endpoint."""
    try:
        flow_path = Path(flow_path).resolve()  # Get absolute path
        if not flow_path.exists():
            print(f"Flow file not found: {flow_path}")
            return None

        # Set authorization header with the token
        headers = {"Authorization": f"Bearer {auth_token}"}

        # Make a GET request to the import endpoint
        response = requests.get(
            "http://0.0.0.0:63578/import_flow/",
            params={"flow_path": str(flow_path)},
            headers=headers,
            timeout=10
        )

        if response.status_code == 200:
            flow_id = response.json()
            print(f"Flow imported successfully with ID: {flow_id}")
            return flow_id
        else:
            print(f"Failed to import flow: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Error importing flow: {e}")
        return None


def open_graph_in_editor(flow_graph: FlowGraph, storage_location: str = None) -> bool:
    """
    Save the ETL graph and open it in the Flowfile editor.

    Parameters:
    -----------
    flow_graph : FlowGraph
        The graph to save and open
    storage_location : str, optional
        Where to save the flowfile. If None, a default name is used.

    Returns:
    --------
    bool
        True if the graph was successfully opened in the editor, False otherwise
    """
    # Create a temporary directory if needed
    temp_dir = None
    if storage_location is None:
        temp_dir = TemporaryDirectory()
        storage_location = os.path.join(temp_dir.name, 'temp_flow.flowfile')
    else:
        # Ensure path is absolute
        storage_location = os.path.abspath(storage_location)

    flow_graph.apply_layout()
    flow_graph.save_flow(storage_location)
    print(f"Flow saved to: {storage_location}")

    # Check if Flowfile is running, and start it if not
    if not is_flowfile_running():
        print("Flowfile application is not running. Starting it...")
        if not start_flowfile_application():
            print("Failed to start Flowfile application")
            if temp_dir:
                temp_dir.cleanup()
            return False
        print("Flowfile application started successfully")

    # Get authentication token
    auth_token = get_auth_token()
    if not auth_token:
        print("Failed to authenticate with Flowfile API")
        if temp_dir:
            temp_dir.cleanup()
        return False

    # Import the flow into the editor
    flow_id = import_flow_to_editor(storage_location, auth_token)

    # Clean up temporary directory if we created one
    if temp_dir:
        temp_dir.cleanup()

    return flow_id is not None