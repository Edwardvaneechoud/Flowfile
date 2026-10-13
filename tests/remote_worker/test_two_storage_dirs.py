"""Core and a worker that share no storage directory run a flow with previews.

Each service starts in a working directory of its own with a relative ``FLOWFILE_STORAGE_DIR`` of its own, so a
path under the worker's storage names, in core, a file that is not there, as it would on another host. The two share
the catalog database and the secure store, as a deployment's core and worker do. The worker reads the flow's CSV files
on this host, so it declares local files (``FLOWFILE_WORKER_LOCAL_FILES``). With the compute protocol flag on,
every node's preview, schema and record count comes from the worker and core's cache directory stays empty; with it
off, core opens the worker's files and the flow fails, which shows the services share no storage. Nothing here imports
flowfile_core; the services start as in ``tests/cloud_e2e``.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pytest
import requests

pytestmark = pytest.mark.skipif(os.name == "nt", reason="the services are managed as POSIX process groups")

_STRIPPED_ENV = ("TESTING", "TEST_MODE", "SKIP_WORKER_TESTS", "JWT_SECRET_KEY", "CORE_PORT", "CORE_HOST", "WORKER_HOST")
_BOOT = "from {package}.{module} import run; run()"
FLOW_ID = 7381

CUSTOMERS = [{"id": i, "name": f"n{i}", "region": "eu" if i % 2 else "us", "amount": i * 10} for i in range(1, 7)]
SEGMENTS = [{"id": i, "segment": f"s{i % 3}"} for i in range(1, 7)]
KEPT = [row for row in CUSTOMERS if row["amount"] > 10]
JOINED = [{**row, "segment": f"s{row['id'] % 3}", "id_right": row["id"]} for row in KEPT]
TOTALS = [{"region": "eu", "total": 80}, {"region": "us", "total": 120}]
CUSTOMER_COLUMNS = [("id", "Int64"), ("name", "String"), ("region", "String"), ("amount", "Int64")]
EXPECTED = {
    1: (CUSTOMER_COLUMNS, CUSTOMERS),
    2: (CUSTOMER_COLUMNS, KEPT),
    3: ([("id", "Int64"), ("segment", "String")], SEGMENTS),
    4: ([*CUSTOMER_COLUMNS, ("segment", "String"), ("id_right", "Int64")], JOINED),
    5: ([("region", "String"), ("total", "Int64")], TOTALS),
}


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_ready(url: str, proc: subprocess.Popen, log: Path, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"{url} exited with {proc.returncode}:\n{log.read_text()[-4000:]}")
        with contextlib.suppress(requests.RequestException):
            if requests.get(url, timeout=2).ok:
                return
        time.sleep(0.5)
    raise TimeoutError(f"{url} not ready after {timeout}s:\n{log.read_text()[-4000:]}")


def _stop(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGTERM)
    try:
        proc.wait(20)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(10)


@dataclass
class Services:
    """A running core and worker, an authenticated session against core, and each service's storage directory."""

    base: str
    http: requests.Session
    storage: dict[str, Path]

    def get(self, path: str, **params):
        response = self.http.get(f"{self.base}{path}", params=params)
        assert response.status_code == 200, response.text
        return response.json()

    def run(self, flow_id: int, timeout: float = 180) -> dict:
        """Start a run like the Run button and poll until it ends."""
        response = self.http.post(f"{self.base}/flow/run/", params={"flow_id": flow_id})
        assert response.status_code == 200, response.text
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self.http.get(f"{self.base}/flow/run_status/", params={"flow_id": flow_id})
            info = status.json()
            if status.status_code == 200 and info["run_type"] == "full_run" and info["success"] is not None:
                return info
            time.sleep(0.5)
        raise TimeoutError(f"flow {flow_id} did not finish within {timeout}s")


@contextlib.contextmanager
def _running(root: Path, protocol: str) -> Iterator[Services]:
    core_port, worker_port = _free_port(), _free_port()
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("AWS_", "FLOWFILE_")) and key not in _STRIPPED_ENV
    }
    env.update(
        HOME=str(root / "home"),
        FLOWFILE_MODE="electron",
        FLOWFILE_DB_PATH=str(root / "catalog.db"),
        FLOWFILE_SECURE_STORAGE_PATH=str(root / "secure"),
        FLOWFILE_TELEMETRY="0",
        FLOWFILE_KERNEL_WARMUP="0",
        FLOWFILE_KERNEL_GC="0",
        FLOWFILE_COMPUTE_PROTOCOL=protocol,
        FLOWFILE_WORKER_LOCAL_FILES="1",
        WORKER_HOST="127.0.0.1",
        CORE_HOST="127.0.0.1",
        CORE_PORT=str(core_port),
        FLOWFILE_WORKER_PORT=str(worker_port),
    )
    (root / "home").mkdir()
    commands = {
        "worker": ("flowfile_worker", "cli", ["--port", worker_port, "--core-port", core_port]),
        "core": ("flowfile_core", "main", ["--port", core_port, "--worker-port", worker_port]),
    }
    logs = {name: root / f"{name}.log" for name in commands}
    procs: dict[str, subprocess.Popen] = {}
    try:
        for name, (package, module, args) in commands.items():
            (root / name).mkdir()
            with logs[name].open("w") as log:
                procs[name] = subprocess.Popen(
                    [sys.executable, "-c", _BOOT.format(package=package, module=module), *map(str, args)],
                    cwd=root / name,
                    env={**env, "FLOWFILE_STORAGE_DIR": f"{name}_storage"},
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
        _wait_ready(f"http://127.0.0.1:{worker_port}/docs", procs["worker"], logs["worker"])
        base = f"http://127.0.0.1:{core_port}"
        _wait_ready(f"{base}/health/status", procs["core"], logs["core"])
        token = requests.post(f"{base}/auth/token", timeout=10)
        assert token.status_code == 200, token.text
        http = requests.Session()
        http.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
        yield Services(base=base, http=http, storage={name: root / name / f"{name}_storage" for name in commands})
    finally:
        for proc in reversed(procs.values()):
            _stop(proc)


def _read(node_id: int, path: Path, outputs: list[int]) -> dict:
    received_file = {
        "name": path.name,
        "path": str(path),
        "file_type": "csv",
        "table_settings": {"file_type": "csv", "delimiter": ",", "has_headers": True, "encoding": "utf-8"},
    }
    return {
        "id": node_id,
        "type": "read",
        "is_start_node": True,
        "outputs": outputs,
        "setting_input": {"received_file": received_file},
    }


def _flow_file(root: Path) -> Path:
    """Read 1 → filter 2 → join 4 with read 3 → group_by 5, in Development mode on the worker."""
    data = root / "data"
    data.mkdir()
    pl.DataFrame(CUSTOMERS).write_csv(data / "customers.csv")
    pl.DataFrame(SEGMENTS).write_csv(data / "segments.csv")
    join_input = {
        "join_mapping": [{"left_col": "id", "right_col": "id"}],
        "left_select": {"select": []},
        "right_select": {"select": []},
        "how": "inner",
    }
    aggregations = [{"old_name": "region", "agg": "groupby"}, {"old_name": "amount", "agg": "sum", "new_name": "total"}]
    flow = {
        "flowfile_version": "0.23.0",
        "flowfile_id": FLOW_ID,
        "flowfile_name": "two storage dirs",
        "flowfile_settings": {"execution_mode": "Development", "execution_location": "remote"},
        "nodes": [
            _read(1, data / "customers.csv", [2]),
            {
                "id": 2,
                "type": "filter",
                "input_ids": [1],
                "outputs": [4],
                "setting_input": {"filter_input": {"mode": "advanced", "advanced_filter": "[amount] > 10"}},
            },
            _read(3, data / "segments.csv", [4]),
            {
                "id": 4,
                "type": "join",
                "input_ids": [2],
                "right_input_id": 3,
                "outputs": [5],
                "setting_input": {"join_input": join_input},
            },
            {
                "id": 5,
                "type": "group_by",
                "input_ids": [4],
                "setting_input": {"groupby_input": {"agg_cols": aggregations}},
            },
        ],
    }
    path = root / "flow.json"
    path.write_text(json.dumps(flow))
    return path


def _failures(run: dict) -> dict[int, str]:
    return {result["node_id"]: result["error"] for result in run["node_step_result"] if not result["success"]}


def _key(row: dict) -> tuple:
    return tuple(row.values())


def test_core_takes_every_preview_schema_and_count_from_the_worker(tmp_path):
    with _running(tmp_path, protocol="1") as services:
        flow_id = services.get("/import_flow/", flow_path=str(_flow_file(tmp_path)))
        run = services.run(flow_id)

        assert run["success"] is True and not _failures(run), _failures(run)
        for node_id, (columns, rows) in EXPECTED.items():
            preview = services.get("/node/data", flow_id=flow_id, node_id=node_id)
            assert [(column["name"], column["data_type"]) for column in preview["table_schema"]] == columns, node_id
            assert sorted(preview["data"], key=_key) == sorted(rows, key=_key), node_id
            assert preview["number_of_records"] == len(rows), node_id
        assert any(path.is_file() for path in (services.storage["worker"] / "cache" / str(flow_id)).iterdir())
        assert list((services.storage["core"] / "cache").iterdir()) == []


def test_with_the_flag_off_core_opens_the_workers_files_and_the_flow_fails(tmp_path):
    """Guards the harness: the task routes need one disk, which these two services do not share."""
    with _running(tmp_path, protocol="0") as services:
        run = services.run(services.get("/import_flow/", flow_path=str(_flow_file(tmp_path))))

        assert run["success"] is False
        assert any("No such file or directory" in error for error in _failures(run).values()), _failures(run)
