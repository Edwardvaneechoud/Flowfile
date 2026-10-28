"""Real core and worker processes driven over HTTP, as ``tests/cloud_e2e`` and ``tests/remote_worker`` run them.

Nothing here imports flowfile_core: each service starts as ``python -c`` in an empty working directory of its own, on
free ports, with a private DB, secure store and HOME under the root it is given.
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
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests

_STRIPPED_ENV = ("TESTING", "TEST_MODE", "SKIP_WORKER_TESTS", "JWT_SECRET_KEY", "CORE_PORT", "CORE_HOST", "WORKER_HOST")
_BOOT = (
    "import shared, {package}; print('{label} code:', {package}.__file__, shared.__file__, flush=True); "
    "from {package}.{module} import run; run()"
)


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_ready(url: str, proc: subprocess.Popen, log: Path, timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"{url} exited with {proc.returncode}:\n{log.read_text()[-4000:]}")
        with contextlib.suppress(requests.RequestException):
            if requests.get(url, timeout=2).ok:
                return
        time.sleep(0.5)
    raise TimeoutError(f"{url} not ready after {timeout}s:\n{log.read_text()[-4000:]}")


def stop(proc: subprocess.Popen) -> None:
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


def flow_document(flow_id: int, name: str, nodes: list[dict]) -> dict:
    """A flow file as the designer saves one: Development mode, ``remote``."""
    return {
        "flowfile_version": "0.23.0",
        "flowfile_id": flow_id,
        "flowfile_name": name,
        "flowfile_settings": {"execution_mode": "Development", "execution_location": "remote"},
        "nodes": nodes,
    }


def failed_nodes(run: dict) -> dict[int, str]:
    """The error of each node that failed in a run report, by node id."""
    return {result["node_id"]: result["error"] for result in run["node_step_result"] if not result["success"]}


def _started(info: dict) -> datetime:
    return datetime.fromisoformat(info["start_time"]) if info["start_time"] else datetime.min


def run_flow(http: requests.Session, base: str, flow_id: int, timeout: float = 180, interval: float = 0.5) -> dict:
    """Start a run like the Run button and poll until a run newer than the previous one ends.

    Requiring a newer ``start_time`` guards against reading the previous run's result.
    """

    def status() -> requests.Response:
        response = http.get(f"{base}/flow/run_status/", params={"flow_id": flow_id})
        assert response.status_code in (200, 202), response.text
        return response

    previous = _started(status().json())
    response = http.post(f"{base}/flow/run/", params={"flow_id": flow_id})
    assert response.status_code == 200, response.text
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = status()
        info = current.json()
        if (
            current.status_code == 200
            and info["run_type"] == "full_run"
            and not info["is_running"]
            and info["success"] is not None
            and _started(info) > previous
        ):
            info["flow_id"] = flow_id
            info["nodes"] = {result["node_id"]: result for result in info["node_step_result"]}
            return info
        time.sleep(interval)
    raise TimeoutError(f"flow {flow_id} did not finish within {timeout}s")


@dataclass
class Services:
    """A running core and worker, a session signed in to core, and each service's working and storage directory."""

    mode: str
    base: str
    http: requests.Session
    cwds: dict[str, Path]
    storage: dict[str, Path]
    flows_dir: Path

    def get(self, path: str, **params):
        response = self.http.get(f"{self.base}{path}", params=params)
        assert response.status_code == 200, response.text
        return response.json()

    def post(self, path: str, body: dict) -> dict:
        response = self.http.post(f"{self.base}{path}", json=body)
        assert response.status_code in (200, 201), response.text
        return response.json()

    def create_minio_connection(self, name: str, **fields) -> None:
        """Create an S3 connection to MinIO exactly as the Cloud Connections page posts it (access key by default)."""
        from test_utils.s3 import fixtures as s3

        payload = {
            "storage_type": "s3",
            "auth_method": "access_key",
            "connection_name": name,
            "aws_region": "us-east-1",
            "aws_access_key_id": s3.MINIO_ACCESS_KEY,
            "aws_secret_access_key": s3.MINIO_SECRET_KEY,
            "aws_allow_unsafe_html": True,
            "endpoint_url": s3.MINIO_ENDPOINT_URL,
            "verify_ssl": True,
            **fields,
        }
        response = self.http.post(f"{self.base}/cloud_connections/cloud_connection", json=payload)
        assert response.status_code == 200, response.text

    def delete_connection(self, name: str) -> None:
        self.http.delete(f"{self.base}/cloud_connections/cloud_connection", params={"connection_name": name})

    def import_flow(self, flow: dict) -> int:
        path = self.flows_dir / f"flow_{uuid.uuid4().hex[:8]}.json"
        path.write_text(json.dumps(flow))
        return self.get("/import_flow/", flow_path=str(path))

    def run(self, flow_id: int, timeout: float = 180) -> dict:
        return run_flow(self.http, self.base, flow_id, timeout=timeout)

    def run_flow(self, flow: dict, timeout: float = 180) -> dict:
        return self.run(self.import_flow(flow), timeout=timeout)


@contextlib.contextmanager
def running_services(
    root: Path,
    mode: str,
    env: dict[str, str],
    *,
    worker_env: dict[str, str] | None = None,
    own_storage: bool = False,
    login: dict | None = None,
    label: str = "services",
) -> Iterator[Services]:
    """Core and a worker in working directories of their own under *root*, and a session signed in to core.

    The test process's ``FLOWFILE_*`` and ``AWS_*`` variables are dropped but ``FLOWFILE_COMPUTE_PROTOCOL``, so the
    services take the shell's flag unless *env* sets it. *env* goes to both services, *worker_env* to the worker alone
    after it. The two share one storage and kernel shared directory under *root*, unless *own_storage* gives each a
    relative ``FLOWFILE_STORAGE_DIR`` of its own inside its working directory, as on two hosts. *login* is the form
    posted to ``/auth/token``; *label* prefixes the code paths each service prints at boot.
    """
    core_port, worker_port = free_port(), free_port()
    base_env = {
        key: value
        for key, value in os.environ.items()
        if key == "FLOWFILE_COMPUTE_PROTOCOL" or not (key.startswith(("AWS_", "FLOWFILE_")) or key in _STRIPPED_ENV)
    }
    base_env.update(
        HOME=str(root / "home"),
        FLOWFILE_MODE=mode,
        FLOWFILE_DB_PATH=str(root / "catalog.db"),
        FLOWFILE_SECURE_STORAGE_PATH=str(root / "secure"),
        FLOWFILE_TELEMETRY="0",
        FLOWFILE_KERNEL_WARMUP="0",
        FLOWFILE_KERNEL_GC="0",
        WORKER_HOST="127.0.0.1",
        CORE_HOST="127.0.0.1",
        CORE_PORT=str(core_port),
        FLOWFILE_WORKER_PORT=str(worker_port),
    )
    if not own_storage:
        base_env.update(FLOWFILE_STORAGE_DIR=str(root / "storage"), FLOWFILE_SHARED_DIR=str(root / "shared"))
    base_env.update(env)
    cwds = {name: root / f"{name}_cwd" for name in ("core", "worker")}
    if own_storage:
        storage = {name: cwd / f"{name}_storage" for name, cwd in cwds.items()}
    else:
        storage = dict.fromkeys(cwds, root / "storage")
    flows_dir = storage["core"] / "e2e_flows"
    for path in (root / "home", flows_dir):
        path.mkdir(parents=True)
    logs = {name: root / f"{name}.log" for name in cwds}
    commands = {
        "worker": ("flowfile_worker", "cli", ["--port", worker_port, "--core-port", core_port]),
        "core": ("flowfile_core", "main", ["--port", core_port, "--worker-port", worker_port]),
    }
    procs: dict[str, subprocess.Popen] = {}
    try:
        for name, (package, module, args) in commands.items():
            cwds[name].mkdir(exist_ok=True)
            service_env = {**base_env, **({"FLOWFILE_STORAGE_DIR": f"{name}_storage"} if own_storage else {})}
            if name == "worker":
                service_env.update(worker_env or {})
            with logs[name].open("w") as log:
                procs[name] = subprocess.Popen(
                    [sys.executable, "-c", _BOOT.format(package=package, module=module, label=label), *map(str, args)],
                    cwd=cwds[name],
                    env=service_env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
        wait_ready(f"http://127.0.0.1:{worker_port}/docs", procs["worker"], logs["worker"])
        base = f"http://127.0.0.1:{core_port}"
        wait_ready(f"{base}/health/status", procs["core"], logs["core"])
        for name, log in logs.items():
            code = next((line for line in log.read_text().splitlines() if line.startswith(f"{label} code:")), "?")
            print(f"[{mode} {name}] {code}")
        token = requests.post(f"{base}/auth/token", data=login or None, timeout=10)
        assert token.status_code == 200, token.text
        http = requests.Session()
        http.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
        yield Services(mode=mode, base=base, http=http, cwds=cwds, storage=storage, flows_dir=flows_dir)
    finally:
        for proc in reversed(procs.values()):
            stop(proc)
