"""Real core + worker processes for the cloud storage end-to-end suite.

Each stack gets a private DB, storage dir, secure store, HOME, AWS profile, free ports and an empty working
directory (checked by ``no_local_writes``); nothing here imports flowfile_core. ``stack`` reaches MinIO only
through a connection's own endpoint, allow-HTTP flag and profile; ``ambient_stack`` reaches it ambiently.
``FLOWFILE_COMPUTE_PROTOCOL`` is passed on from the test process, so the suite runs with the compute protocol flag in
either state; the test process's other ``FLOWFILE_*`` and ``AWS_*`` variables are dropped.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from cryptography.fernet import Fernet

from test_utils.s3 import fixtures as s3
from test_utils.s3.aws_profiles import (
    DEAD_ENDPOINT,
    MINIO_KEYS,
    MINIO_PROFILE,
    NOT_MINIO_KEYS,
    strip_aws_env,
    write_aws_files,
)
from test_utils.s3.cloud_e2e_seed import SEED_BUCKET, seed_cloud_e2e
from test_utils.services import Services, running_services

from .helpers import MINIO_OPTIONS, list_keys

FLOW_FIXTURE = Path(__file__).parent / "fixtures" / "user_flow_run1390.json"
RUN_ID = uuid.uuid4().hex[:8]


def _in_ci() -> bool:
    return os.environ.get("CI", "").lower() in ("true", "1", "yes")


_STACKS: list[Services] = []


@contextlib.contextmanager
def _running_stack(root: Path, mode: str, extra_env: dict[str, str], login: dict | None = None) -> Iterator[Services]:
    with running_services(root, mode, extra_env, login=login, label="cloud_e2e") as stack:
        _STACKS.append(stack)
        yield stack


@pytest.fixture(scope="session", autouse=True)
def hermetic_aws_env() -> Iterator[None]:
    """Keep the test process's own reads and writes off any ambient AWS credentials."""
    with pytest.MonkeyPatch.context() as mp:
        strip_aws_env(mp)
        mp.setenv("AWS_EC2_METADATA_DISABLED", "true")
        yield


@pytest.fixture(scope="session")
def minio() -> dict[str, str]:
    """MinIO options for the test process; skips locally (fails in CI) when MinIO is unavailable."""
    if os.name == "nt":
        pytest.skip("the cloud_e2e stacks are managed as POSIX process groups")
    if not s3.minio_available():
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
    return write_aws_files(root, {"default": default_keys, MINIO_PROFILE: MINIO_KEYS})


@pytest.fixture(scope="session")
def aws_profile(tmp_path_factory) -> dict[str, str]:
    """Ambient MinIO: static MinIO keys in the default profile plus the MinIO endpoint and plain HTTP."""
    files = _aws_files(tmp_path_factory.mktemp("aws"), MINIO_KEYS)
    return {**files, "AWS_ENDPOINT_URL": s3.MINIO_ENDPOINT_URL, "AWS_ALLOW_HTTP": "true"}


@pytest.fixture(scope="session")
def connection_only_aws(tmp_path_factory) -> dict[str, str]:
    """No ambient route to MinIO: default keys it rejects, a dead endpoint and no plain HTTP."""
    files = _aws_files(tmp_path_factory.mktemp("aws_connection_only"), NOT_MINIO_KEYS)
    return {**files, "AWS_ENDPOINT_URL": DEAD_ENDPOINT}


@pytest.fixture(scope="session")
def stack(tmp_path_factory, minio, connection_only_aws) -> Iterator[Services]:
    """Desktop-mode (electron) stack where only a saved connection's own settings reach MinIO."""
    with _running_stack(tmp_path_factory.mktemp("electron_stack"), "electron", connection_only_aws) as running:
        yield running


@pytest.fixture(scope="session")
def ambient_stack(tmp_path_factory, minio, aws_profile) -> Iterator[Services]:
    """Desktop-mode (electron) stack whose own AWS profile and endpoint reach MinIO ("No connection")."""
    with _running_stack(tmp_path_factory.mktemp("ambient_stack"), "electron", aws_profile) as running:
        yield running


@pytest.fixture(scope="session")
def docker_stack(tmp_path_factory, minio, aws_profile) -> Iterator[Services]:
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


def _connection(running: Services) -> Iterator[str]:
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
