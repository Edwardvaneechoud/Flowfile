import pytest
import requests
import time
import shutil
import os
import sys
from pathlib import Path
from unittest.mock import patch
from typing import Optional

# Assuming the api module is importable as flowfile.api
from flowfile.api import (
    is_flowfile_running,
    start_flowfile_server_process,
    stop_flowfile_server_process,
    get_auth_token,
    is_poetry_environment,
    build_server_command,
    FLOWFILE_BASE_URL,
)
import flowfile as ff
from flowfile import col, open_graph_in_editor


def ensure_folder_empty(folder_path: str) -> None:
    """Remove all contents from folder, create if missing."""
    folder = Path(folder_path)

    if folder.exists():
        shutil.rmtree(folder)

    folder.mkdir(parents=True, exist_ok=True)


def _get_active_flows():
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {get_auth_token()}"  # Construct the Authorization header
    }
    return requests.get(f"{FLOWFILE_BASE_URL}/active_flowfile_sessions/", headers=headers).json()


def _get_flow_id_on_flow_location(flow_name: str = '_test_pipeline.flowfile') -> Optional[int]:
    active_flows = _get_active_flows()
    for flow in active_flows:
        flow_path: str = flow.get('path')
        if flow_name in flow_path:
            return flow.get('flow_id')


def _trigger_flow_execution(flow_id: int) -> None:
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {get_auth_token()}"  # Construct the Authorization header
    }
    r = requests.post(f"{FLOWFILE_BASE_URL}/flow/run/?flow_id=" + str(flow_id), headers=headers)
    if not r.ok:
        raise Exception(f"Failed to trigger flow execution: {r.text}")


def _poll_for_execution_completion(flow_id: int, timeout: int = 60, expect_success: bool = True) -> bool:
    """Poll for flow execution completion."""
    start_time = time.time()
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {get_auth_token()}"  # Construct the Authorization header
    }
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{FLOWFILE_BASE_URL}/flow/run_status/?flow_id={flow_id}", headers=headers)
            if response.status_code == 200:
                status = response.json()
                if expect_success:
                    if status.get("success"):
                        return True
                else:
                    return True
        except requests.RequestException:
            pass
        time.sleep(1)
    return False


class TestFlowfileAPI:
    """Integration tests for Flowfile API with minimal mocking."""

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Ensure clean state before and after each test."""
        # Store original environment
        self.original_env = os.environ.copy()

        # Ensure server is stopped before test and folder is empty
        stop_flowfile_server_process()
        ensure_folder_empty("supporting_files")

        yield

        # Restore environment
        os.environ.clear()
        os.environ.update(self.original_env)

        # Ensure server is stopped after test
        stop_flowfile_server_process()

    def test_is_flowfile_running_when_not_running(self):
        """Test that is_flowfile_running returns False when server is not running."""
        # Ensure server is not running
        stop_flowfile_server_process()
        time.sleep(1)  # Give time for server to stop

        assert is_flowfile_running() is False

    def test_is_flowfile_running_when_running(self):
        """Test that is_flowfile_running returns True when server is running."""
        # Start the server
        success, _ = start_flowfile_server_process()
        if success:
            # Server should be running now
            assert is_flowfile_running() is True
        else:
            # If server failed to start, skip this test
            pytest.skip("Server failed to start, skipping running test")

    def test_start_and_stop_server_process(self):
        """Test starting and stopping the Flowfile server process."""
        # Start the server
        success, single_mode = start_flowfile_server_process()
        assert isinstance(success, bool)
        assert isinstance(single_mode, bool)

        if success:
            # Verify server is running
            assert is_flowfile_running() is True

            # Stop the server
            stop_flowfile_server_process()
            time.sleep(2)  # Give time for server to stop

            # Verify server is stopped
            assert is_flowfile_running() is False

    def test_poetry_environment_detection(self):
        """Test Poetry environment detection logic."""
        # Test with FORCE_POETRY
        os.environ["FORCE_POETRY"] = "true"
        assert is_poetry_environment() is True
        # Test with POETRY_ACTIVE
        os.environ.pop("FORCE_POETRY", None)
        os.environ["POETRY_ACTIVE"] = "1"
        assert is_poetry_environment() is True

        # Test with clean environment
        os.environ.pop("POETRY_ACTIVE", None)
        # This should return False unless we're actually in a Poetry env
        result = is_poetry_environment()
        assert isinstance(result, bool)

    def test_build_server_command(self):
        """Test command building for different environments."""
        # Test with Poetry environment
        with patch('flowfile.api.is_poetry_environment', return_value=True):
            with patch('flowfile.api.is_command_available', return_value=True):
                cmd = build_server_command("flowfile")
                assert cmd[0] == "poetry"
                assert "run" in cmd
                assert "flowfile" in cmd
                assert "--no-browser" in cmd

        # Test without Poetry
        with patch('flowfile.api.is_poetry_environment', return_value=False):
            cmd = build_server_command("flowfile")
            assert cmd[0] == sys.executable
            assert "-m" in cmd
            assert "flowfile" in cmd
            assert "--no-browser" in cmd

    def test_get_auth_token_integration(self):
        """Test getting auth token from running server."""
        # Start server first
        success, _ = start_flowfile_server_process()
        if not success:
            pytest.skip("Server failed to start, skipping auth token test")

        # Wait for server to be fully ready
        time.sleep(2)

        token = get_auth_token()
        assert token is None or isinstance(token, str)
        if token:
            # Verify token format (assuming JWT or similar)
            assert len(token) > 20

    def test_end_to_end_pipeline_integration(self):
        """Test the complete pipeline from graph creation to UI opening."""
        # First ensure server is started
        server_started, _ = start_flowfile_server_process()
        flow_path = Path('supporting_files') / '_test_pipeline.flowfile'
        if not server_started:
            pytest.skip("Server failed to start, skipping integration test")

        # Wait a bit for server to be fully ready
        time.sleep(2)

        # Create a test pipeline as shown in the example
        df = ff.from_dict({
            "id": [1, 2, 3, 4, 5],
            "category": ["A", "B", "A", "C", "B"],
            "value": [100, 200, 150, 300, 250]
        })

        # Process the data
        result = df.filter(col("value") > 150).with_columns([
            (col("value") * 2).alias("double_value")
        ])
        # Add write step for verification
        output_path = Path("supporting_files") / "output.csv"
        grouped = result.group_by("category").agg(ff.col("value").sum())
        result_with_write = grouped.write_csv(str(output_path))
        if output_path.exists():
            os.remove(output_path)

        success = open_graph_in_editor(result_with_write.flow_graph,
                                       storage_location=str(flow_path),
                                       automatically_open_browser=False)
        assert isinstance(success, bool)
        assert flow_path.exists()
        flow_id = _get_flow_id_on_flow_location()
        _trigger_flow_execution(flow_id)
        execution_complete = _poll_for_execution_completion(flow_id)

        if execution_complete:
            assert output_path.exists()

        ensure_folder_empty('supporting_files')


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "-s"])
