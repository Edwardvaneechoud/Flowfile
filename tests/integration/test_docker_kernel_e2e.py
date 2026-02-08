"""Docker Kernel end-to-end integration test.

Exercises the full Docker-in-Docker kernel flow from scratch:

    manual_input → python_script (train) → python_script (predict)

The test is fully self-contained — it builds images, starts services,
creates a kernel, imports a flow, runs it, and validates the results.

Requirements:
    - Docker Engine and docker compose v2
    - Run with: ``pytest -m docker_integration -v``
"""

import json
import os
import subprocess
import tempfile
import time

import httpx
import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
COMPOSE_FILE = os.path.join(REPO_ROOT, "docker-compose.yml")
CORE_URL = "http://localhost:63578"


def _dump_compose_logs(services: list[str]) -> str:
    """Capture docker compose logs for debugging on failure."""
    output_parts: list[str] = []
    for svc in services:
        result = subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "logs", "--tail=100", svc],
            capture_output=True,
            text=True,
            timeout=30,
        )
        output_parts.append(f"\n{'=' * 60}\n{svc} logs:\n{'=' * 60}\n{result.stdout}")
        if result.stderr:
            output_parts.append(result.stderr)
    return "\n".join(output_parts)


def _dump_kernel_logs(kernel_id: str) -> str:
    """Capture kernel container logs for debugging."""
    result = subprocess.run(
        ["docker", "logs", f"flowfile-kernel-{kernel_id}", "--tail=100"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return f"\n{'=' * 60}\nkernel ({kernel_id}) logs:\n{'=' * 60}\n{result.stdout}\n{result.stderr}"

pytestmark = pytest.mark.docker_integration

KERNEL_ID = "e2e-test"

# Flow definition: manual_input → python_script (train) → python_script (predict)
FLOW_JSON = {
    "flowfile_version": "0.6.3",
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


def _import_flow(client: httpx.Client) -> int:
    """Import the test flow into the running core service.

    Uses ``docker cp`` to place the flow JSON inside the core container,
    avoiding reliance on bind-mounted ``saved_flows`` directories.
    """
    flow_json_str = json.dumps(FLOW_JSON)

    # Get the core container ID (container is already healthy at this point)
    container_id = subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "ps", "-q", "flowfile-core"],
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()

    # Create a temp file locally, then docker cp it into the container
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write(flow_json_str)
        tmp_path = f.name

    try:
        dest_path = "/tmp/kernel_test_flow.json"
        cp_result = subprocess.run(
            ["docker", "cp", tmp_path, f"{container_id}:{dest_path}"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if cp_result.returncode != 0:
            pytest.fail(f"docker cp failed: {cp_result.stderr}")

        resp = client.get(
            "/import_flow/",
            params={"flow_path": dest_path},
        )
        resp.raise_for_status()
        return resp.json()
    finally:
        os.unlink(tmp_path)


class TestDockerKernelE2E:
    """End-to-end test: run a kernel-based flow inside Docker."""

    def test_flow_execution(self, auth_client: httpx.Client, kernel_ready: str):
        """Import flow, run it, verify node 3 has predicted_y column.

        Steps:
        - Import the 3-node flow (manual_input → train → predict)
        - Run the flow via the API
        - Poll until completion
        - Assert all 3 nodes completed successfully
        - Assert node 3's output contains a ``predicted_y`` column
        """
        # Step 8: import the flow
        flow_id = _import_flow(auth_client)

        # Step 9: run the flow
        resp = auth_client.post("/flow/run/", params={"flow_id": flow_id})
        assert resp.status_code == 200, f"Failed to start flow: {resp.text}"

        # Poll until finished (200 = done, 202 = still running)
        deadline = time.monotonic() + 180
        run_info = None
        while time.monotonic() < deadline:
            resp = auth_client.get("/flow/run_status/", params={"flow_id": flow_id})
            run_info = resp.json()
            if resp.status_code == 200:
                break
            time.sleep(2)
        else:
            logs = _dump_compose_logs(["flowfile-core"])
            kernel_logs = _dump_kernel_logs(kernel_ready)
            pytest.fail(
                f"Flow did not finish within timeout. Last status: {run_info}"
                f"{logs}{kernel_logs}"
            )

        # Step 10: validate results
        assert run_info["success"] is True, (
            f"Flow failed. Node results: {run_info.get('node_step_result')}"
            f"{_dump_compose_logs(['flowfile-core'])}"
            f"{_dump_kernel_logs(kernel_ready)}"
        )
        assert run_info["nodes_completed"] == 3

        # Verify node 3 has the predicted_y column
        resp = auth_client.get(
            "/flow_data/v2",
            params={"flow_id": flow_id},
        )
        resp.raise_for_status()
        vue_data = resp.json()

        # Find node 3's schema in the vue flow data
        node3 = next(
            (n for n in vue_data.get("nodes", []) if n.get("id") == "3"),
            None,
        )
        assert node3 is not None, "Node 3 not found in flow data"

        node3_columns = [
            col["name"] for col in node3.get("data", {}).get("schema", [])
        ]
        assert "predicted_y" in node3_columns, (
            f"predicted_y column not found in node 3 schema. Columns: {node3_columns}"
        )
