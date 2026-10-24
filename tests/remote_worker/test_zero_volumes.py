"""Core and a worker in containers that share no volume run a flow from a database read to a catalog write.

``docker-compose.remote-worker.yml`` starts core, a worker, Postgres and MinIO on one network. Core and the worker have
no volume and no bind mount: they reach each other, Postgres (the catalog database and the flow's source table) and
MinIO (an object-storage catalog namespace and the S3 artifact store) over the network only, as on two hosts, and the
worker does not declare local files. The flow reads the table, adds a column with a formula, trains a model on the
worker, scores with the model the worker holds, publishes the model to the artifact store and writes the scores to the
catalog. The artifact store reaches MinIO through an HTTPS forwarder with a certificate made per run, because the URLs
it presigns for the worker are ``https`` ones. The secrets and the certificate reach the services through the
environment only. Core is configured with an exchange store on MinIO. A second flow, one node on the worker, is posted
onto ``local`` through its settings: with the protocol flag on, core, which serves, saves it as ``auto`` and the flow
runs on the worker, and with the flag off it stays on ``local`` and runs in core; its flag-off case recreates core,
which is why it comes last. The elaborate integration flow (``complex_flow.json``, its outputs written to MinIO) runs
whole on the worker, and with its nodes alternating between the worker and ``local`` is refused before any node runs,
core refusing every node on ``local``. Data crossing between two backends needs a process that does not serve, as
``flowfile_core/tests/flowfile/execution/test_mixed_placement.py`` runs it. A scheduled run's child computes on the
worker embedded in it, inside core's container, while the worker container serves nothing.
"""

from __future__ import annotations

import io
import json
import os
import re
import secrets
import subprocess
import tempfile
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import polars as pl
import pytest
import requests
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

pytestmark = pytest.mark.docker_integration

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = REPO_ROOT / "docker-compose.remote-worker.yml"
PROJECT = os.environ.get("COMPOSE_PROJECT_NAME") or f"flowfile-remote-worker-{uuid.uuid4().hex[:8]}"
FLOW_ID = 7401
ARTIFACT_BUCKET = "flowfile-artifacts"
LAKE_BUCKET = "flowfile-lake"
TRAIN_ROWS = [(float(i), float(i % 3)) for i in range(1, 9)]

_ENV: dict[str, str] = {}


@dataclass
class Stack:
    """Core's published URL, a client for MinIO on its published port, and a session signed in to core."""

    core: str
    minio: Any
    http: requests.Session


def _compose(*args: str, check: bool = True, timeout: float = 300) -> subprocess.CompletedProcess:
    result = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "-p", PROJECT, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        env={**os.environ, **_ENV},
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"docker compose {args[0]} failed:\n{result.stdout[-3000:]}\n{result.stderr[-3000:]}")
    return result


def _port(service: str, port: int) -> int:
    return int(_compose("port", service, str(port)).stdout.strip().rsplit(":", 1)[1])


def _logs() -> str:
    """Core's and the worker's last 200 lines, for a failure message."""
    return _compose("logs", "--no-color", "--tail=200", "flowfile-core", "flowfile-worker", check=False).stdout


def _docker_compose_available() -> bool:
    try:
        for command in (["docker", "info"], ["docker", "compose", "version"]):
            subprocess.run(command, capture_output=True, check=True, timeout=30)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return False
    return True


def _certificate() -> tuple[str, str]:
    """A self-signed P-256 certificate for ``minio-tls`` and its private key, both PEM."""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "minio-tls")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(hours=2))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("minio-tls")]), critical=False)
        .sign(key, hashes.SHA256())
    )
    private_key = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode(), private_key.decode()


def _wait_until(ready: Callable[[], bool], what: str, timeout: float = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ready():
            return
        time.sleep(1)
    pytest.fail(f"{what} not ready after {timeout:.0f}s:\n{_logs()}")


def _core_ready(core: str) -> bool:
    try:
        return requests.get(f"{core}/health/status", timeout=2).ok
    except requests.RequestException:
        return False


def _worker_ready() -> bool:
    probe = ("exec", "-T", "flowfile-worker", "curl", "-fs", "http://localhost:63579/docs")
    return _compose(*probe, check=False, timeout=30).returncode == 0


def _minio_ready(minio) -> bool:
    try:
        minio.list_buckets()
    except (BotoCoreError, ClientError):
        return False
    return True


def _sign_in(core: str) -> requests.Session:
    http = requests.Session()
    form = {"username": _ENV["FLOWFILE_ADMIN_USER"], "password": _ENV["FLOWFILE_ADMIN_PASSWORD"]}
    token = http.post(f"{core}/auth/token", data=form, timeout=30)
    assert token.status_code == 200, token.text
    http.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
    return http


@pytest.fixture(scope="module")
def stack() -> Iterator[Stack]:
    """The compose file's services, built and started under this run's project, and taken down with their volumes."""
    from cryptography.fernet import Fernet

    if not _docker_compose_available():
        if os.environ.get("CI", "").lower() in ("true", "1", "yes"):
            pytest.fail("Docker with compose v2 is required for the zero-volume test in CI")
        pytest.skip("Docker with compose v2 is not available")
    cert, key = _certificate()
    _ENV.update(
        FLOWFILE_ADMIN_USER="admin",
        FLOWFILE_ADMIN_PASSWORD=secrets.token_urlsafe(16),
        JWT_SECRET_KEY=secrets.token_hex(32),
        FLOWFILE_INTERNAL_TOKEN=secrets.token_hex(32),
        FLOWFILE_MASTER_KEY=Fernet.generate_key().decode(),
        MINIO_TLS_CERT=cert,
        MINIO_TLS_KEY=key,
        FLOWFILE_COMPUTE_PROTOCOL="1",
    )
    try:
        _compose("build", "flowfile-core", "flowfile-worker", timeout=1500)
        _compose("up", "-d")
        core = f"http://127.0.0.1:{_port('flowfile-core', 63578)}"
        minio = boto3.client(
            "s3",
            endpoint_url=f"http://127.0.0.1:{_port('minio', 9000)}",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        _wait_until(lambda: _core_ready(core), "core")
        _wait_until(_worker_ready, "the worker")
        _wait_until(lambda: _minio_ready(minio), "MinIO")
        yield Stack(core=core, minio=minio, http=_sign_in(core))
    finally:
        _compose("down", "-v", "--remove-orphans", check=False)


def _flow(model_name: str) -> dict:
    """Database read 1 → formula 2 → Train Model 3 (published) → Apply Model 4 → catalog writer 5, on the worker."""
    database_settings = {
        "connection_mode": "reference",
        "database_connection_name": "source_pg",
        "schema_name": "source",
        "table_name": "training",
        "query_mode": "table",
    }
    formula = {"field": {"name": "y", "data_type": "Float64"}, "function": "[x1] * 2 + [x2] * 3 + 1"}
    train_input = {
        "target_column": "y",
        "feature_columns": ["x1", "x2"],
        "model_type": "linear_regression",
        "params": {"add_bias": True},
        "publish_to_catalog": True,
        "model_name": model_name,
    }
    apply_input = {"source": "upstream", "upstream_node_id": 3, "output_column": "pred"}
    write_settings = {"table_name": "predictions", "namespace_full_name": "lake.gate", "write_mode": "overwrite"}
    return {
        "flowfile_version": "0.23.0",
        "flowfile_id": FLOW_ID,
        "flowfile_name": "zero volumes",
        "flowfile_settings": {"execution_mode": "Development", "execution_location": "remote"},
        "nodes": [
            {
                "id": 1,
                "type": "database_reader",
                "is_start_node": True,
                "outputs": [2],
                "setting_input": {"database_settings": database_settings},
            },
            {"id": 2, "type": "formula", "input_ids": [1], "outputs": [3], "setting_input": {"functions": [formula]}},
            {
                "id": 3,
                "type": "train_model",
                "input_ids": [2],
                "outputs": [4],
                "setting_input": {"train_input": train_input},
            },
            {
                "id": 4,
                "type": "apply_model",
                "input_ids": [3],
                "outputs": [5],
                "setting_input": {"apply_input": apply_input},
            },
            {
                "id": 5,
                "type": "catalog_writer",
                "input_ids": [4],
                "setting_input": {"catalog_write_settings": write_settings},
            },
        ],
    }


def _set_up_sources(stack: Stack) -> None:
    """The source table in Postgres, the two buckets, the two connections and the catalog ``lake.gate`` on MinIO."""
    values = ", ".join(f"({x1}, {x2})" for x1, x2 in TRAIN_ROWS)
    sql = (
        "CREATE SCHEMA source; CREATE TABLE source.training (x1 double precision, x2 double precision); "
        f"INSERT INTO source.training VALUES {values};"
    )
    _compose("exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "flowfile", "-d", "flowfile", "-c", sql)
    for bucket in (ARTIFACT_BUCKET, LAKE_BUCKET):
        stack.minio.create_bucket(Bucket=bucket)
    database = {
        "connection_name": "source_pg",
        "database_type": "postgresql",
        "username": "flowfile",
        "password": "flowfile",
        "host": "postgres",
        "port": 5432,
        "database": "flowfile",
    }
    response = stack.http.post(f"{stack.core}/db_connection_lib", json=database)
    assert response.status_code == 200, response.text
    lake = {
        "storage_type": "s3",
        "auth_method": "access_key",
        "connection_name": "lake_minio",
        "aws_region": "us-east-1",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "aws_allow_unsafe_html": True,
        "endpoint_url": "http://minio:9000",
        "verify_ssl": True,
    }
    response = stack.http.post(f"{stack.core}/cloud_connections/cloud_connection", json=lake)
    assert response.status_code == 200, response.text
    catalog = {"name": "lake", "storage_uri": f"s3://{LAKE_BUCKET}/lake", "storage_connection_name": "lake_minio"}
    response = stack.http.post(f"{stack.core}/catalog/namespaces", json=catalog)
    assert response.status_code == 201, response.text
    response = stack.http.post(
        f"{stack.core}/catalog/namespaces", json={"name": "gate", "parent_id": response.json()["id"]}
    )
    assert response.status_code == 201, response.text


def _copy_to_core(flow: dict, path: str) -> None:
    """Write the flow to ``path`` in core's container, as nothing is mounted there."""
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(flow, handle)
    try:
        container = _compose("ps", "-q", "flowfile-core").stdout.strip()
        subprocess.run(["docker", "cp", handle.name, f"{container}:{path}"], capture_output=True, check=True)
    finally:
        os.unlink(handle.name)


def _import_flow(stack: Stack, flow: dict) -> int:
    """Copy the flow into core's container and import it."""
    _copy_to_core(flow, "/app/zero_volumes_flow.json")
    response = stack.http.get(f"{stack.core}/import_flow/", params={"flow_path": "/app/zero_volumes_flow.json"})
    assert response.status_code == 200, response.text
    return response.json()


def _run(stack: Stack, flow_id: int, timeout: float = 300) -> dict:
    """Start a run like the Run button and poll until it ends."""
    response = stack.http.post(f"{stack.core}/flow/run/", params={"flow_id": flow_id})
    assert response.status_code == 200, response.text
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = stack.http.get(f"{stack.core}/flow/run_status/", params={"flow_id": flow_id})
        info = status.json()
        if status.status_code == 200 and info["run_type"] == "full_run" and info["success"] is not None:
            return info
        time.sleep(1)
    pytest.fail(f"flow {flow_id} did not finish within {timeout:.0f}s:\n{_logs()}")


def _cache_files(service: str) -> list[str]:
    return _compose(
        "exec", "-T", service, "find", "/app/internal_storage/cache", "-type", "f", check=False
    ).stdout.split()


def test_a_database_read_a_model_and_a_catalog_write_run_on_a_worker_that_shares_no_volume_with_core(stack):
    _set_up_sources(stack)
    model_name = f"gate_model_{uuid.uuid4().hex[:8]}"
    flow_id = _import_flow(stack, _flow(model_name))

    run = _run(stack, flow_id)

    failures = {result["node_id"]: result["error"] for result in run["node_step_result"] if not result["success"]}
    assert run["success"] is True and not failures, f"{failures}\n{_logs()}"
    assert run["nodes_completed"] == 5
    preview = stack.http.get(f"{stack.core}/node/data", params={"flow_id": flow_id, "node_id": 4}).json()
    assert [column["name"] for column in preview["table_schema"]] == ["x1", "x2", "y", "pred"]
    assert preview["number_of_records"] == len(preview["data"]) == len(TRAIN_ROWS)
    assert [round(row["pred"], 6) for row in preview["data"]] == [round(row["y"], 6) for row in preview["data"]]
    tables = [
        table for table in stack.http.get(f"{stack.core}/catalog/tables").json() if table["name"] == "predictions"
    ]
    assert len(tables) == 1 and tables[0]["row_count"] == len(TRAIN_ROWS), tables
    assert tables[0]["file_path"].startswith(f"s3://{LAKE_BUCKET}/lake/"), tables
    table_prefix = tables[0]["file_path"].removeprefix(f"s3://{LAKE_BUCKET}/")
    assert stack.minio.list_objects_v2(Bucket=LAKE_BUCKET, Prefix=f"{table_prefix}/_delta_log/").get("Contents")
    artifacts = [listed["Key"] for listed in stack.minio.list_objects_v2(Bucket=ARTIFACT_BUCKET).get("Contents", [])]
    assert len(artifacts) == 1 and re.fullmatch(rf"global_artifacts/\d+/{model_name}\.bin", artifacts[0]), artifacts
    assert _cache_files("flowfile-core") == []
    worker_files = _cache_files("flowfile-worker")
    assert any(path.endswith(".arrow") for path in worker_files), worker_files
    assert any(path.endswith(".blob") for path in worker_files), worker_files


def test_core_and_the_worker_have_no_mount(stack):
    for service in ("flowfile-core", "flowfile-worker"):
        container = _compose("ps", "-q", service).stdout.strip()
        inspected = subprocess.run(
            ["docker", "inspect", "-f", "{{json .Mounts}}", container], capture_output=True, text=True, check=True
        )
        assert json.loads(inspected.stdout) == [], (service, inspected.stdout)


SCHEDULED_FLOW = "/app/scheduled_flow.json"


def _scheduled_flow() -> dict:
    """Manual input 1 → group_by 2, the rows counted by region, saved ``remote`` as the designer saves a flow."""
    columns = [{"name": "region", "data_type": "String"}]
    aggregations = [{"old_name": "region", "agg": "groupby"}, {"old_name": "region", "agg": "count", "new_name": "n"}]
    return {
        "flowfile_version": "0.23.0",
        "flowfile_id": 7406,
        "flowfile_name": "scheduled",
        "flowfile_settings": {"execution_mode": "Development", "execution_location": "remote"},
        "nodes": [
            {
                "id": 1,
                "type": "manual_input",
                "is_start_node": True,
                "outputs": [2],
                "setting_input": {"raw_data_format": {"columns": columns, "data": [["north", "south", "north"]]}},
            },
            {
                "id": 2,
                "type": "group_by",
                "input_ids": [1],
                "setting_input": {"groupby_input": {"agg_cols": aggregations}},
            },
        ],
    }


def _worker_requests() -> list[str]:
    """Every request the worker served so far, from its access log, but the health probes on ``/docs``: each of its
    jobs starts with one."""
    log = _compose("logs", "--no-color", "flowfile-worker").stdout
    served = [line for line in log.splitlines() if ' HTTP/1.1"' in line or '"WebSocket ' in line]
    return [line for line in served if '"GET /docs ' not in line]


def _run_schedule_now(stack: Stack, schedule_id: int, timeout: float = 300) -> dict:
    """Trigger the schedule as its tick would, a ``flowfile run flow`` child of core, and poll its run until it ends."""
    response = stack.http.post(f"{stack.core}/catalog/schedules/{schedule_id}/run-now")
    assert response.status_code == 200, response.text
    run_id = response.json()["id"]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = stack.http.get(f"{stack.core}/catalog/runs/{run_id}").json()
        if run["ended_at"] is not None:
            run["log"] = stack.http.get(f"{stack.core}/catalog/runs/{run_id}/log").json().get("log")
            return run
        time.sleep(1)
    pytest.fail(f"run {run_id} did not finish within {timeout:.0f}s:\n{_logs()}")


def test_a_scheduled_run_computes_in_core_s_container_and_leaves_the_worker_idle(stack):
    _copy_to_core(_scheduled_flow(), SCHEDULED_FLOW)
    response = stack.http.post(f"{stack.core}/catalog/flows", json={"name": "scheduled", "flow_path": SCHEDULED_FLOW})
    assert response.status_code == 201, response.text
    schedule = {"registration_id": response.json()["id"], "schedule_type": "interval", "interval_seconds": 3600}
    response = stack.http.post(f"{stack.core}/catalog/schedules", json=schedule)
    assert response.status_code == 201, response.text
    served_before = _worker_requests()

    run = _run_schedule_now(stack, response.json()["id"])

    assert run["success"] is True and run["nodes_completed"] == 2, f"{run}\n{_logs()}"
    node_results = json.loads(run["node_results_json"])
    assert [result["success"] for result in node_results] == [True, True], node_results
    assert _worker_requests() == served_before


EXCHANGE_BUCKET = "flowfile-exchange"
CUSTOMERS = {
    "id": [1, 2, 3, 4, 5, 6, 7],
    "region": ["north", "south", "north", "south", "north", "east", "east"],
    "amount": [12.5, 8.0, 30.0, 15.0, 4.5, 22.0, 9.5],
}
TOTALS = [("east", 22.0), ("north", 42.5), ("south", 15.0)]
MIXED_FLOW_IDS = {"1": 7402, "0": 7403}


@pytest.fixture(scope="module")
def exchange(stack: Stack) -> None:
    """The exchange store's bucket, versioned so it keeps a record of what a run wrote and removed, and its connection."""
    stack.minio.create_bucket(Bucket=EXCHANGE_BUCKET)
    stack.minio.put_bucket_versioning(Bucket=EXCHANGE_BUCKET, VersioningConfiguration={"Status": "Enabled"})
    connection = {
        "storage_type": "s3",
        "auth_method": "access_key",
        "connection_name": "exchange_minio",
        "aws_region": "us-east-1",
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "aws_allow_unsafe_html": True,
        "endpoint_url": "http://minio:9000",
        "verify_ssl": True,
    }
    response = stack.http.post(f"{stack.core}/cloud_connections/cloud_connection", json=connection)
    assert response.status_code == 200, response.text


def _mixed_flow(flow_id: int) -> dict:
    """Manual input 1 → filter 2 (``[amount] > 10``), set to run on the worker → group_by 3, ``total`` by region."""
    columns = [
        {"name": "id", "data_type": "Int64"},
        {"name": "region", "data_type": "String"},
        {"name": "amount", "data_type": "Float64"},
    ]
    aggregations = [
        {"old_name": "region", "agg": "groupby"},
        {"old_name": "amount", "agg": "sum", "new_name": "total"},
    ]
    return {
        "flowfile_version": "0.23.0",
        "flowfile_id": flow_id,
        "flowfile_name": "one node on the worker",
        "flowfile_settings": {"execution_mode": "Development", "execution_location": "remote"},
        "nodes": [
            {
                "id": 1,
                "type": "manual_input",
                "is_start_node": True,
                "outputs": [2],
                "setting_input": {"raw_data_format": {"columns": columns, "data": list(CUSTOMERS.values())}},
            },
            {
                "id": 2,
                "type": "filter",
                "input_ids": [1],
                "outputs": [3],
                "setting_input": {
                    "execution_location": "worker",
                    "filter_input": {"mode": "advanced", "advanced_filter": "[amount] > 10"},
                },
            },
            {
                "id": 3,
                "type": "group_by",
                "input_ids": [2],
                "setting_input": {"groupby_input": {"agg_cols": aggregations}},
            },
        ],
    }


def _core_with_protocol(stack: Stack, flag: str) -> Stack:
    """Core with ``FLOWFILE_COMPUTE_PROTOCOL`` at ``flag``, recreated in the running stack when it differs."""
    if _ENV["FLOWFILE_COMPUTE_PROTOCOL"] == flag:
        return stack
    _ENV["FLOWFILE_COMPUTE_PROTOCOL"] = flag
    _compose("up", "-d", "--no-deps", "--force-recreate", "flowfile-core")
    core = f"http://127.0.0.1:{_port('flowfile-core', 63578)}"
    _wait_until(lambda: _core_ready(core), "core")
    return Stack(core=core, minio=stack.minio, http=_sign_in(core))


REFUSED = (
    'cannot run on backend "local": this core serves; compute runs on its worker. '
    "To run it elsewhere, set its Compute backend under General Settings."
)


def _assert_refused_before_any_node(stack: Stack, run: dict, flow_id: int, refused: list[int]) -> None:
    """The run failed with exactly the nodes in ``refused`` refused on ``local`` for the serving core's reason, started
    no node, and left nothing under its flow in the exchange store or in the worker's cache."""
    errors = {result["node_id"]: result["error"] for result in run["node_step_result"]}
    assert errors == dict.fromkeys(refused, REFUSED), f"{errors}\n{_logs()}"
    assert all(result["compute_error"] == "Refusal" for result in run["node_step_result"]), run["node_step_result"]
    assert run["success"] is False and run["nodes_completed"] == 0
    versions = stack.minio.list_object_versions(Bucket=EXCHANGE_BUCKET, Prefix=f"runs/{flow_id}/")
    assert "Versions" not in versions and "DeleteMarkers" not in versions, versions
    worker_cache = f"/app/internal_storage/cache/{flow_id}/"
    assert [path for path in _cache_files("flowfile-worker") if path.startswith(worker_cache)] == []


COMPLEX_FLOW = Path(__file__).with_name("complex_flow.json")
COMPLEX_BUCKET = "flowfile-complex"
COMPLEX_ROWS = {
    "customer_sales_analysis.csv": 31,
    "fuzzy_matched_customers.parquet": 3,
    "network_components.csv": 5,
    "combined_aggregations.csv": 52,
}
COMPLEX_FLOW_IDS = {"worker": 7404, "alternating": 7405}
ALTERNATING = {
    **dict.fromkeys((1, 2, 4, 5, 12, 14, 17, 26, 27, 30, 41, 72, 73, 74, 9999), "worker"),
    **dict.fromkeys((3, 11, 13, 15, 16, 28, 29, 31, 32, 42, 43, 51, 52, 61, 71), "local"),
}


@pytest.fixture(scope="module")
def complex_outputs(stack: Stack, exchange: None) -> None:
    """The bucket the elaborate flow's writers put their outputs in."""
    stack.minio.create_bucket(Bucket=COMPLEX_BUCKET)


def _complex_flow(flow_id: int, locations: dict[int, str] | None = None) -> dict:
    """The elaborate flow from its file, its writers under ``<bucket>/<flow id>/``, each node placed per ``locations``."""
    flow = json.loads(COMPLEX_FLOW.read_text())
    flow["flowfile_id"] = flow_id
    for node in flow["nodes"]:
        if node["type"] == "cloud_storage_writer":
            storage = node["setting_input"]["cloud_storage_settings"]
            storage["resource_path"] = storage["resource_path"].replace("/out/", f"/{flow_id}/")
        if locations is not None:
            node["setting_input"]["execution_location"] = locations[node["id"]]
    return flow


def _written(stack: Stack, flow_id: int) -> dict[str, int]:
    """The rows of each output the elaborate flow wrote under ``<bucket>/<flow id>/``, by file name."""
    rows = {}
    for name in COMPLEX_ROWS:
        body = io.BytesIO(stack.minio.get_object(Bucket=COMPLEX_BUCKET, Key=f"{flow_id}/{name}")["Body"].read())
        rows[name] = (pl.read_parquet(body) if name.endswith(".parquet") else pl.read_csv(body)).height
    return rows


def test_the_elaborate_flow_runs_on_a_worker_that_shares_no_volume_with_core(stack, exchange, complex_outputs):
    stack = _core_with_protocol(stack, "1")
    flow_id = _import_flow(stack, _complex_flow(COMPLEX_FLOW_IDS["worker"]))
    assert flow_id == COMPLEX_FLOW_IDS["worker"]

    run = _run(stack, flow_id)

    failures = {result["node_id"]: result["error"] for result in run["node_step_result"] if not result["success"]}
    assert run["success"] is True and not failures, f"{failures}\n{_logs()}"
    assert run["nodes_completed"] == 30
    assert _written(stack, flow_id) == COMPLEX_ROWS
    assert run["boundaries"] == []
    versions = stack.minio.list_object_versions(Bucket=EXCHANGE_BUCKET, Prefix=f"runs/{flow_id}/")
    assert "Versions" not in versions and "DeleteMarkers" not in versions, versions
    core_cache = f"/app/internal_storage/cache/local/{flow_id}/"
    assert [path for path in _cache_files("flowfile-core") if path.startswith(core_cache)] == []


def test_the_elaborate_flow_with_its_nodes_alternating_between_worker_and_local_is_refused_before_any_node_runs(
    stack, exchange, complex_outputs
):
    stack = _core_with_protocol(stack, "1")
    flow_id = _import_flow(stack, _complex_flow(COMPLEX_FLOW_IDS["alternating"], ALTERNATING))
    assert flow_id == COMPLEX_FLOW_IDS["alternating"]

    run = _run(stack, flow_id)

    _assert_refused_before_any_node(stack, run, flow_id, [node for node, on in ALTERNATING.items() if on == "local"])
    assert stack.minio.list_objects_v2(Bucket=COMPLEX_BUCKET, Prefix=f"{flow_id}/")["KeyCount"] == 0


@pytest.mark.parametrize("protocol", ["1", "0"], ids=["flag-on", "flag-off"])
def test_a_flow_moved_onto_local_with_one_node_on_the_worker_runs_on_the_worker_unless_the_flag_is_off(
    stack, exchange, protocol
):
    stack = _core_with_protocol(stack, protocol)
    flow_id = _import_flow(stack, _mixed_flow(MIXED_FLOW_IDS[protocol]))
    assert flow_id == MIXED_FLOW_IDS[protocol]
    settings = stack.http.get(f"{stack.core}/flow_settings", params={"flow_id": flow_id}).json()
    response = stack.http.post(f"{stack.core}/flow_settings", json={**settings, "execution_location": "local"})
    assert response.status_code == 200, response.text
    saved = stack.http.get(f"{stack.core}/flow_settings", params={"flow_id": flow_id}).json()
    assert saved["execution_location"] == ("auto" if protocol == "1" else "local")

    run = _run(stack, flow_id)

    failures = {result["node_id"]: result["error"] for result in run["node_step_result"] if not result["success"]}
    assert run["success"] is True and not failures, f"{failures}\n{_logs()}"
    assert run["nodes_completed"] == 3
    filtered = stack.http.get(f"{stack.core}/node/data", params={"flow_id": flow_id, "node_id": 2}).json()
    assert len(filtered["data"]) == 4, filtered
    grouped = stack.http.get(f"{stack.core}/node/data", params={"flow_id": flow_id, "node_id": 3}).json()
    assert sorted((row["region"], row["total"]) for row in grouped["data"]) == TOTALS, grouped
    assert run["boundaries"] == []
    versions = stack.minio.list_object_versions(Bucket=EXCHANGE_BUCKET, Prefix=f"runs/{flow_id}/")
    assert "Versions" not in versions and "DeleteMarkers" not in versions, versions
    if protocol == "1":
        core_cache = f"/app/internal_storage/cache/local/{flow_id}/"
        assert [path for path in _cache_files("flowfile-core") if path.startswith(core_cache)] == []
    else:
        worker_cache = f"/app/internal_storage/cache/{flow_id}/"
        assert [path for path in _cache_files("flowfile-worker") if path.startswith(worker_cache)] == []
