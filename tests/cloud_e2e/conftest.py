"""Real core + worker processes for the cloud storage end-to-end suite.

Each stack gets a private DB, storage dir, secure store, HOME, AWS profile, free ports and an empty working
directory (checked by ``no_local_writes``); nothing here imports flowfile_core. ``stack`` reaches MinIO only
through a connection's own endpoint, allow-HTTP flag and profile; ``ambient_stack`` reaches it ambiently.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pytest
import requests
from cryptography.fernet import Fernet

from test_utils.s3 import fixtures as s3
from test_utils.s3.cloud_e2e_seed import SEED_BUCKET, seed_cloud_e2e

from .helpers import MINIO_OPTIONS, NAMED_PROFILE, list_keys

FLOW_FIXTURE = Path(__file__).parent / "fixtures" / "user_flow_run1390.json"
RUN_ID = uuid.uuid4().hex[:8]
_STRIPPED_ENV = ("TESTING", "TEST_MODE", "SKIP_WORKER_TESTS", "JWT_SECRET_KEY", "CORE_PORT", "CORE_HOST", "WORKER_HOST")
_BOOT = (
    "import shared, {package}; print('cloud_e2e code:', {package}.__file__, shared.__file__, flush=True); "
    "from {package}.{module} import run; run()"
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _in_ci() -> bool:
    return os.environ.get("CI", "").lower() in ("true", "1", "yes")


@dataclass
class Stack:
    """One running core + worker pair and an authenticated session against core."""

    mode: str
    base: str
    http: requests.Session
    cwds: dict[str, Path]
    flows_dir: Path

    def create_minio_connection(self, name: str, **fields) -> None:
        """Create an S3 connection to MinIO exactly as the Cloud Connections page posts it (access key by default)."""
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
        response = self.http.get(f"{self.base}/import_flow/", params={"flow_path": str(path)})
        assert response.status_code == 200, response.text
        return response.json()

    def run(self, flow_id: int, timeout: float = 180) -> dict:
        """Start a run like the Run button and poll until a run newer than the previous one ends.

        Requiring a newer ``start_time`` guards against reading the previous run's result.
        """
        previous = _started(self._status(flow_id).json())
        response = self.http.post(f"{self.base}/flow/run/", params={"flow_id": flow_id})
        assert response.status_code == 200, response.text
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status = self._status(flow_id)
            info = status.json()
            if (
                status.status_code == 200
                and info["run_type"] == "full_run"
                and not info["is_running"]
                and info["success"] is not None
                and _started(info) > previous
            ):
                info["flow_id"] = flow_id
                info["nodes"] = {result["node_id"]: result for result in info["node_step_result"]}
                return info
            time.sleep(0.5)
        raise TimeoutError(f"flow {flow_id} did not finish within {timeout}s")

    def run_flow(self, flow: dict, timeout: float = 180) -> dict:
        return self.run(self.import_flow(flow), timeout=timeout)

    def _status(self, flow_id: int) -> requests.Response:
        response = self.http.get(f"{self.base}/flow/run_status/", params={"flow_id": flow_id})
        assert response.status_code in (200, 202), response.text
        return response


_STACKS: list[Stack] = []


def _started(info: dict) -> datetime:
    return datetime.fromisoformat(info["start_time"]) if info["start_time"] else datetime.min


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


@contextlib.contextmanager
def _running_stack(root: Path, mode: str, extra_env: dict[str, str], login: dict | None = None) -> Iterator[Stack]:
    core_port, worker_port = _free_port(), _free_port()
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("AWS_", "FLOWFILE_")) and key not in _STRIPPED_ENV
    }
    env.update(
        HOME=str(root / "home"),
        FLOWFILE_MODE=mode,
        FLOWFILE_DB_PATH=str(root / "catalog.db"),
        FLOWFILE_STORAGE_DIR=str(root / "storage"),
        FLOWFILE_SECURE_STORAGE_PATH=str(root / "secure"),
        FLOWFILE_SHARED_DIR=str(root / "shared"),
        FLOWFILE_TELEMETRY="0",
        FLOWFILE_KERNEL_WARMUP="0",
        FLOWFILE_KERNEL_GC="0",
        WORKER_HOST="127.0.0.1",
        CORE_HOST="127.0.0.1",
        CORE_PORT=str(core_port),
        FLOWFILE_WORKER_PORT=str(worker_port),
    )
    env.update(extra_env)
    flows_dir = root / "storage" / "e2e_flows"
    for path in (root / "home", flows_dir):
        path.mkdir(parents=True)
    cwds = {name: root / f"{name}_cwd" for name in ("core", "worker")}
    logs = {name: root / f"{name}.log" for name in ("core", "worker")}
    commands = {
        "worker": ("flowfile_worker", "cli", ["--port", worker_port, "--core-port", core_port]),
        "core": ("flowfile_core", "main", ["--port", core_port, "--worker-port", worker_port]),
    }
    procs: dict[str, subprocess.Popen] = {}
    try:
        for name, (package, module, args) in commands.items():
            cwds[name].mkdir()
            with logs[name].open("w") as log:
                procs[name] = subprocess.Popen(
                    [sys.executable, "-c", _BOOT.format(package=package, module=module), *map(str, args)],
                    cwd=cwds[name],
                    env=env,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
        _wait_ready(f"http://127.0.0.1:{worker_port}/docs", procs["worker"], logs["worker"])
        base = f"http://127.0.0.1:{core_port}"
        _wait_ready(f"{base}/health/status", procs["core"], logs["core"])
        for name, log in logs.items():
            code = next((line for line in log.read_text().splitlines() if line.startswith("cloud_e2e code:")), "?")
            print(f"[{mode} {name}] {code}")
        token = requests.post(f"{base}/auth/token", data=login or None, timeout=10)
        assert token.status_code == 200, token.text
        http = requests.Session()
        http.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
        stack = Stack(mode=mode, base=base, http=http, cwds=cwds, flows_dir=flows_dir)
        _STACKS.append(stack)
        yield stack
    finally:
        for proc in reversed(procs.values()):
            _stop(proc)


@pytest.fixture(scope="session", autouse=True)
def hermetic_aws_env() -> Iterator[None]:
    """Keep the test process's own reads and writes off any ambient AWS credentials."""
    with pytest.MonkeyPatch.context() as mp:
        for key in [key for key in os.environ if key.startswith("AWS_")]:
            mp.delenv(key)
        mp.setenv("AWS_EC2_METADATA_DISABLED", "true")
        yield


@pytest.fixture(scope="session")
def minio() -> dict[str, str]:
    """MinIO options for the test process; skips locally (fails in CI) when MinIO is unavailable."""
    if os.name == "nt":
        pytest.skip("the cloud_e2e stacks are managed as POSIX process groups")
    available = s3.is_docker_available() and (
        s3.is_container_running(s3.MINIO_CONTAINER_NAME) or s3.start_minio_container()
    )
    if not (available and s3.wait_for_minio(max_retries=10)):
        if _in_ci():
            pytest.fail("MinIO is required for the cloud_e2e suite in CI; run 'poetry run start_minio' first.")
        pytest.skip("MinIO (Docker) is not available")
    return MINIO_OPTIONS


@pytest.fixture(scope="session")
def run_prefix(minio) -> Iterator[str]:
    """A bucket prefix owned by this session; everything under it is deleted afterwards."""
    prefix = f"cloud-e2e-{RUN_ID}"
    yield prefix
    client = s3.get_minio_client()
    keys = list_keys(f"s3://{SEED_BUCKET}/{prefix}/")
    for start in range(0, len(keys), 1000):
        objects = [{"Key": key} for key in keys[start : start + 1000]]
        client.delete_objects(Bucket=SEED_BUCKET, Delete={"Objects": objects})


@pytest.fixture(scope="session")
def source_path(run_prefix) -> str:
    return seed_cloud_e2e(run_prefix)


@pytest.fixture
def new_target(run_prefix, request) -> Callable[[str], str]:
    """Unique ``s3://`` target paths for the current test."""
    test_id = re.sub(r"[^A-Za-z0-9]+", "-", request.node.name.removeprefix("test_")).strip("-")

    def _target(name: str) -> str:
        return f"s3://{SEED_BUCKET}/{run_prefix}/{test_id}-{uuid.uuid4().hex[:6]}/{name}"

    return _target


def _aws_files(root: Path, default_keys: tuple[str, str]) -> dict[str, str]:
    """A default profile with *default_keys* and a ``minio`` profile with the real MinIO keys; no session token."""
    (root / "credentials").write_text(
        f"[default]\naws_access_key_id = {default_keys[0]}\naws_secret_access_key = {default_keys[1]}\n"
        f"[{NAMED_PROFILE}]\naws_access_key_id = {s3.MINIO_ACCESS_KEY}\n"
        f"aws_secret_access_key = {s3.MINIO_SECRET_KEY}\n"
    )
    (root / "config").write_text(f"[default]\nregion = us-east-1\n[profile {NAMED_PROFILE}]\nregion = us-east-1\n")
    return {
        "AWS_SHARED_CREDENTIALS_FILE": str(root / "credentials"),
        "AWS_CONFIG_FILE": str(root / "config"),
        "AWS_EC2_METADATA_DISABLED": "true",
    }


@pytest.fixture(scope="session")
def aws_profile(tmp_path_factory) -> dict[str, str]:
    """Ambient MinIO: static MinIO keys in the default profile plus the MinIO endpoint and plain HTTP."""
    files = _aws_files(tmp_path_factory.mktemp("aws"), (s3.MINIO_ACCESS_KEY, s3.MINIO_SECRET_KEY))
    return {**files, "AWS_ENDPOINT_URL": s3.MINIO_ENDPOINT_URL, "AWS_ALLOW_HTTP": "true"}


@pytest.fixture(scope="session")
def connection_only_aws(tmp_path_factory) -> dict[str, str]:
    """No ambient route to MinIO: default keys it rejects, a dead endpoint and no plain HTTP."""
    files = _aws_files(tmp_path_factory.mktemp("aws_connection_only"), ("AKIANOTMINIO", "not-the-minio-secret"))
    return {**files, "AWS_ENDPOINT_URL": "http://127.0.0.1:9"}


@pytest.fixture(scope="session")
def stack(tmp_path_factory, minio, connection_only_aws) -> Iterator[Stack]:
    """Desktop-mode (electron) stack where only a saved connection's own settings reach MinIO."""
    with _running_stack(tmp_path_factory.mktemp("electron_stack"), "electron", connection_only_aws) as running:
        yield running


@pytest.fixture(scope="session")
def ambient_stack(tmp_path_factory, minio, aws_profile) -> Iterator[Stack]:
    """Desktop-mode (electron) stack whose own AWS profile and endpoint reach MinIO ("No connection")."""
    with _running_stack(tmp_path_factory.mktemp("ambient_stack"), "electron", aws_profile) as running:
        yield running


@pytest.fixture(scope="session")
def docker_stack(tmp_path_factory, minio, aws_profile) -> Iterator[Stack]:
    """Multi-user (docker mode) stack whose process environment holds a working AWS profile."""
    root = tmp_path_factory.mktemp("docker_stack")
    admin = {"username": "cloud_e2e_admin", "password": uuid.uuid4().hex}
    env = {
        "JWT_SECRET_KEY": uuid.uuid4().hex,
        "FLOWFILE_MASTER_KEY": Fernet.generate_key().decode(),
        "FLOWFILE_INTERNAL_TOKEN": uuid.uuid4().hex,
        "FLOWFILE_ADMIN_USER": admin["username"],
        "FLOWFILE_ADMIN_PASSWORD": admin["password"],
        "FLOWFILE_USER_DATA_DIR": str(root / "user_data"),
        **aws_profile,
    }
    with _running_stack(root, "docker", env, login=admin) as running:
        yield running


def _connection(running: Stack) -> Iterator[str]:
    name = f"minio connection {RUN_ID}"
    running.create_minio_connection(name)
    yield name
    running.delete_connection(name)


@pytest.fixture(scope="session")
def minio_connection(stack) -> Iterator[str]:
    yield from _connection(stack)


@pytest.fixture(scope="session")
def ambient_minio_connection(ambient_stack) -> Iterator[str]:
    yield from _connection(ambient_stack)


@pytest.fixture(scope="session")
def docker_minio_connection(docker_stack) -> Iterator[str]:
    yield from _connection(docker_stack)


@pytest.fixture
def user_flow(source_path) -> Callable[..., dict]:
    """The reporter's saved flow, with only its id, connection names, paths and overrides patched in.

    A fresh id per build, since importing replaces any open flow with the same id.
    """
    template = json.loads(FLOW_FIXTURE.read_text())

    def _build(location: str, connection: str, *, reader: dict | None = None, writer: dict | None = None) -> dict:
        flow = json.loads(json.dumps(template))
        flow["flowfile_id"] = 10**9 + uuid.uuid4().int % 10**9
        flow["flowfile_settings"]["execution_location"] = location
        overrides = {
            "cloud_storage_reader": {"connection_name": connection, "resource_path": source_path, **(reader or {})},
            "cloud_storage_writer": writer or {},
        }
        for node in flow["nodes"]:
            if node["type"] in overrides:
                node["setting_input"]["cloud_storage_settings"].update(overrides[node["type"]])
        return flow

    return _build


@pytest.fixture(autouse=True)
def no_local_writes() -> Iterator[None]:
    """Core and worker must never write into their working directory (the old empty-path fallout)."""
    yield
    leaked = {}
    for running in _STACKS:
        for name, cwd in running.cwds.items():
            entries = sorted(cwd.iterdir())
            if entries:
                leaked[f"{running.mode} {name}"] = [entry.name for entry in entries]
            for entry in entries:
                if entry.is_dir():
                    shutil.rmtree(entry)
                else:
                    entry.unlink()
    assert not leaked, f"services wrote into their working directory: {leaked}"
