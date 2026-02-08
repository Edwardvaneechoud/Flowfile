"""Docker integration test for kernel flow execution.

Starts flowfile services via ``docker compose``, creates a kernel,
imports a multi-node flow with python_script nodes (linear regression
+ prediction using artifacts), runs it, and validates the results.

Requirements:
    - Docker and docker compose available
    - Kernel image built: ``docker compose build flowfile-kernel``
    - Run with: ``pytest flowfile_core/tests/test_docker_kernel_integration.py -v``

The test is skipped automatically when Docker is not available.
"""

import json
import os
import subprocess
import tempfile
import time

import httpx
import pytest

CORE_URL = "http://localhost:63578"
COMPOSE_FILE = os.path.join(os.path.dirname(__file__), "..", "..", "docker-compose.yml")
KERNEL_ID = "integration-test"

# Flow definition: manual_input → python_script (train) → python_script (predict)
FLOW_JSON = {
    "flowfile_version": "0.6.2",
    "flowfile_id": 1,
    "flowfile_name": "kernel_test_flow",
    "flowfile_settings": {
        "description": None,
        "execution_mode": "Development",
        "execution_location": "remote",
        "auto_save": False,
        "show_detailed_progress": True,
        "max_parallel_workers": 4,
    },
    "nodes": [
        {
            "id": 1,
            "type": "manual_input",
            "is_start_node": True,
            "description": "3 cols, 4 rows: x1, x2, y",
            "node_reference": None,
            "x_position": 0,
            "y_position": 0,
            "left_input_id": None,
            "right_input_id": None,
            "input_ids": None,
            "outputs": [2],
            "setting_input": {
                "cache_results": False,
                "output_field_config": None,
                "raw_data_format": {
                    "columns": [
                        {"name": "x1", "data_type": "Float64"},
                        {"name": "x2", "data_type": "Float64"},
                        {"name": "y", "data_type": "Float64"},
                    ],
                    "data": [
                        [1.0, 2.0, 3.0, 4.0],
                        [2.0, 3.0, 4.0, 5.0],
                        [5.0, 8.0, 11.0, 14.0],
                    ],
                },
            },
        },
        {
            "id": 2,
            "type": "python_script",
            "is_start_node": False,
            "description": "",
            "node_reference": None,
            "x_position": 0,
            "y_position": 0,
            "left_input_id": None,
            "right_input_id": None,
            "input_ids": [1],
            "outputs": [3],
            "setting_input": {
                "cache_results": False,
                "output_field_config": None,
                "python_script_input": {
                    "code": (
                        "\nimport numpy as np\nimport polars as pl\n\n"
                        'df = flowfile.read_input().collect()\n'
                        'X = np.column_stack([df["x1"].to_numpy(), df["x2"].to_numpy(), np.ones(len(df))])\n'
                        'y_vals = df["y"].to_numpy()\n'
                        "coeffs = np.linalg.lstsq(X, y_vals, rcond=None)[0]\n"
                        'flowfile.publish_artifact("linear_model", {"coefficients": coeffs.tolist()})\n'
                        "flowfile.publish_output(df)\n"
                    ),
                    "kernel_id": KERNEL_ID,
                    "cells": None,
                },
            },
        },
        {
            "id": 3,
            "type": "python_script",
            "is_start_node": False,
            "description": "",
            "node_reference": None,
            "x_position": 0,
            "y_position": 0,
            "left_input_id": None,
            "right_input_id": None,
            "input_ids": [2],
            "outputs": [],
            "setting_input": {
                "cache_results": False,
                "output_field_config": None,
                "python_script_input": {
                    "code": (
                        "\nimport numpy as np\nimport polars as pl\n\n"
                        'df = flowfile.read_input().collect()\n'
                        'model = flowfile.read_artifact("linear_model")\n'
                        'coeffs = np.array(model["coefficients"])\n'
                        'X = np.column_stack([df["x1"].to_numpy(), df["x2"].to_numpy(), np.ones(len(df))])\n'
                        "predictions = X @ coeffs\n"
                        'result = df.with_columns(pl.Series("predicted_y", predictions))\n'
                        "flowfile.publish_output(result)\n"
                    ),
                    "kernel_id": KERNEL_ID,
                    "cells": None,
                },
            },
        },
    ],
}


def _docker_available() -> bool:
    try:
        subprocess.run(["docker", "info"], capture_output=True, check=True, timeout=10)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _compose(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _wait_for_service(url: str, path: str = "/health/status", timeout: float = 120) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(f"{url}{path}")
                if resp.status_code == 200:
                    return True
        except (httpx.HTTPError, OSError):
            pass
        time.sleep(2)
    return False


def _get_auth_token(client: httpx.Client) -> str:
    resp = client.post(f"{CORE_URL}/auth/token")
    resp.raise_for_status()
    return resp.json()["access_token"]


@pytest.fixture(scope="module")
def docker_services():
    """Start docker compose services, yield, then tear down."""
    if not _docker_available():
        pytest.skip("Docker is not available")

    # Build kernel image first (profiled service, not started by `up`)
    build = _compose("build", "flowfile-kernel", timeout=300)
    if build.returncode != 0:
        pytest.skip(f"Could not build kernel image: {build.stderr}")

    # Start core + worker
    _compose("up", "-d", "--build", "flowfile-core", "flowfile-worker")
    try:
        if not _wait_for_service(CORE_URL):
            logs = _compose("logs", "flowfile-core")
            pytest.fail(f"Core service did not become healthy.\n{logs.stdout}\n{logs.stderr}")
        if not _wait_for_service("http://localhost:63579"):
            logs = _compose("logs", "flowfile-worker")
            pytest.fail(f"Worker service did not become healthy.\n{logs.stdout}\n{logs.stderr}")
        yield
    finally:
        _compose("down", "-v", "--remove-orphans")


@pytest.fixture(scope="module")
def auth_client(docker_services) -> httpx.Client:
    """Return an authenticated httpx client pointed at the core API."""
    client = httpx.Client(timeout=30.0)
    token = _get_auth_token(client)
    client.headers["Authorization"] = f"Bearer {token}"
    return client


@pytest.fixture(scope="module")
def kernel_ready(auth_client: httpx.Client):
    """Create and start the integration-test kernel, yield its id, then stop+delete."""
    # Create kernel
    resp = auth_client.post(
        f"{CORE_URL}/kernels/",
        json={"id": KERNEL_ID, "name": "Docker Integration Test"},
    )
    resp.raise_for_status()

    # Start kernel
    resp = auth_client.post(f"{CORE_URL}/kernels/{KERNEL_ID}/start")
    resp.raise_for_status()

    # Wait for kernel to become idle
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        resp = auth_client.get(f"{CORE_URL}/kernels/{KERNEL_ID}")
        info = resp.json()
        if info.get("state") == "idle":
            break
        time.sleep(2)
    else:
        pytest.fail(f"Kernel did not become idle: {info}")

    yield KERNEL_ID

    # Cleanup
    auth_client.post(f"{CORE_URL}/kernels/{KERNEL_ID}/stop")
    auth_client.delete(f"{CORE_URL}/kernels/{KERNEL_ID}")


def _import_flow(auth_client: httpx.Client) -> int:
    """Write the flow JSON to a temp file inside the core container and import it."""
    flow_json_str = json.dumps(FLOW_JSON)

    # Write to saved_flows dir (bind-mounted in the container)
    saved_flows_dir = os.path.join(os.path.dirname(COMPOSE_FILE), "saved_flows")
    os.makedirs(saved_flows_dir, exist_ok=True)
    flow_path = os.path.join(saved_flows_dir, "kernel_test_flow.json")
    with open(flow_path, "w") as f:
        f.write(flow_json_str)

    try:
        # The core container sees this at /app/flowfile_core/saved_flows/kernel_test_flow.json
        resp = auth_client.get(
            f"{CORE_URL}/import_flow/",
            params={"flow_path": "/app/flowfile_core/saved_flows/kernel_test_flow.json"},
        )
        resp.raise_for_status()
        return resp.json()
    finally:
        os.unlink(flow_path)


class TestDockerKernelFlow:
    """End-to-end test: run a kernel-based flow inside Docker."""

    def test_kernel_flow_execution(self, auth_client: httpx.Client, kernel_ready: str):
        """Import flow, run it, verify node 3 has predicted_y column."""
        flow_id = _import_flow(auth_client)

        # Run the flow
        resp = auth_client.post(f"{CORE_URL}/flow/run/", params={"flow_id": flow_id})
        assert resp.status_code == 200, f"Failed to start flow: {resp.text}"

        # Poll until finished (200 = done, 202 = still running)
        deadline = time.monotonic() + 180
        run_info = None
        while time.monotonic() < deadline:
            resp = auth_client.get(f"{CORE_URL}/flow/run_status/", params={"flow_id": flow_id})
            run_info = resp.json()
            if resp.status_code == 200:
                break
            time.sleep(2)
        else:
            pytest.fail(f"Flow did not finish within timeout. Last status: {run_info}")

        # Verify success
        assert run_info["success"] is True, (
            f"Flow failed. Node results: {run_info.get('node_step_result')}"
        )
        assert run_info["nodes_completed"] == 3

        # Verify node 3 has the predicted_y column
        resp = auth_client.get(
            f"{CORE_URL}/flow_data/v2",
            params={"flow_id": flow_id},
        )
        resp.raise_for_status()
        vue_data = resp.json()

        # Find node 3's schema in the vue flow data
        node3 = next((n for n in vue_data.get("nodes", []) if n.get("id") == "3"), None)
        assert node3 is not None, "Node 3 not found in flow data"

        node3_columns = [
            col["name"]
            for col in node3.get("data", {}).get("schema", [])
        ]
        assert "predicted_y" in node3_columns, (
            f"predicted_y column not found in node 3 schema. Columns: {node3_columns}"
        )
