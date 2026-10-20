"""Core and a worker that share no storage directory run a flow with previews.

Each service starts in a working directory of its own with a relative ``FLOWFILE_STORAGE_DIR`` of its own, so a
path under the worker's storage names, in core, a file that is not there, as it would on another host. The two share
the catalog database and the secure store, as a deployment's core and worker do. The worker reads the flow's CSV files
on this host, so it declares local files (``FLOWFILE_WORKER_LOCAL_FILES``). With the compute protocol flag on,
every node's preview, schema and record count comes from the worker and core's cache directory stays empty; with it
off, core opens the worker's files and the flow fails, which shows the services share no storage. A Train Model node
and an Apply Model node run the same way with the flag on: the model stays on the worker and Apply Model scores with it
by reference, and a model published to MinIO as the artifact store, behind a self-signed TLS forwarder, is applied from
the catalog. A catalog table on MinIO, under a connection that takes its keys from the environment, is read with
the flag on by a worker that has the keys and a core that has none, so core resolves no credential to learn its schema.
So are a cloud read with no saved connection and a cloud read of a directory under that connection, the worker's
environment being the only route to MinIO. Nothing here imports flowfile_core; the services start as in
``tests/cloud_e2e``.
"""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import json
import os
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pytest
import requests

pytestmark = pytest.mark.skipif(os.name == "nt", reason="the services are managed as POSIX process groups")

_STRIPPED_ENV = ("TESTING", "TEST_MODE", "SKIP_WORKER_TESTS", "JWT_SECRET_KEY", "CORE_PORT", "CORE_HOST", "WORKER_HOST")
_BOOT = "from {package}.{module} import run; run()"
FLOW_ID = 7381
ML_FLOW_ID = 7382
CATALOG_FLOW_ID = 7383
CLOUD_FLOW_ID = 7384
MINIO_BUCKET = "worker-test-bucket"

CUSTOMERS = [{"id": i, "name": f"n{i}", "region": "eu" if i % 2 else "us", "amount": i * 10} for i in range(1, 7)]
SEGMENTS = [{"id": i, "segment": f"s{i % 3}"} for i in range(1, 7)]
KEPT = [row for row in CUSTOMERS if row["amount"] > 10]
JOINED = [{**row, "segment": f"s{row['id'] % 3}", "id_right": row["id"]} for row in KEPT]
TOTALS = [{"region": "eu", "total": 80}, {"region": "us", "total": 120}]
TRAIN_ROWS = [{"x1": float(i), "x2": float(i % 3), "y": 2.0 * i + 3.0 * (i % 3) + 1.0} for i in range(1, 9)]
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

    def post(self, path: str, body: dict) -> dict:
        response = self.http.post(f"{self.base}{path}", json=body)
        assert response.status_code in (200, 201), response.text
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
def _running(root: Path, protocol: str, *, worker_env: dict[str, str] | None = None, **env: str) -> Iterator[Services]:
    """Core and a worker in working directories of their own under ``root``.

    ``env`` goes to both, after the rest; ``worker_env`` goes to the worker alone, after that.
    """
    core_port, worker_port = _free_port(), _free_port()
    service_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("AWS_", "FLOWFILE_")) and key not in _STRIPPED_ENV
    }
    service_env.update(
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
    service_env.update(env)
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
                    env={
                        **service_env,
                        "FLOWFILE_STORAGE_DIR": f"{name}_storage",
                        **((worker_env or {}) if name == "worker" else {}),
                    },
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


def _ml_flow_file(root: Path, flow_id: int, *, published: str | None = None) -> Path:
    """Read 1 → Train Model 2 → Apply Model 3 into ``pred``, in Development mode on the worker.

    Apply Model scores with the model Train Model holds; with ``published`` Train Model publishes it to the catalog
    under that name and Apply Model reads it from there.
    """
    data = root / "data"
    data.mkdir()
    pl.DataFrame(TRAIN_ROWS).write_csv(data / "train.csv")
    train_input = {
        "target_column": "y",
        "feature_columns": ["x1", "x2"],
        "model_type": "linear_regression",
        "params": {"add_bias": True},
    }
    apply_input = {"source": "upstream", "upstream_node_id": 2, "output_column": "pred"}
    if published is not None:
        train_input.update(publish_to_catalog=True, model_name=published)
        apply_input = {"source": "catalog", "model_name": published, "output_column": "pred"}
    flow = {
        "flowfile_version": "0.23.0",
        "flowfile_id": flow_id,
        "flowfile_name": "ml two storage dirs",
        "flowfile_settings": {"execution_mode": "Development", "execution_location": "remote"},
        "nodes": [
            _read(1, data / "train.csv", [2]),
            {
                "id": 2,
                "type": "train_model",
                "input_ids": [1],
                "outputs": [3],
                "setting_input": {"train_input": train_input},
            },
            {"id": 3, "type": "apply_model", "input_ids": [2], "setting_input": {"apply_input": apply_input}},
        ],
    }
    path = root / "flow.json"
    path.write_text(json.dumps(flow))
    return path


def _predictions(services: Services, flow_id: int) -> list[float]:
    """Apply Model's ``pred`` in its preview, rounded, in the order of ``TRAIN_ROWS``."""
    preview = services.get("/node/data", flow_id=flow_id, node_id=3)
    by_x1 = {row["x1"]: row["pred"] for row in preview["data"]}
    return [round(by_x1[row["x1"]], 6) for row in TRAIN_ROWS]


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


@pytest.mark.parametrize("protocol", ["1", "0"])
def test_a_catalog_table_on_object_storage_is_read_with_the_workers_credentials_only(tmp_path, protocol):
    """Only the worker holds MinIO's keys: with the flag off core opens the table itself and the read fails."""
    from test_utils.s3 import fixtures as s3

    available = s3.is_docker_available() and (
        s3.is_container_running(s3.MINIO_CONTAINER_NAME) or s3.start_minio_container()
    )
    if not (available and s3.wait_for_minio(max_retries=10)):
        pytest.skip("MinIO (Docker) is not available")
    prefix = f"two-dirs-catalog-{uuid.uuid4().hex}"
    storage_uri = f"s3://{MINIO_BUCKET}/{prefix}/catalog"
    table_path = f"{storage_uri}/customers"
    pl.DataFrame(CUSTOMERS).write_delta(
        table_path,
        storage_options={
            "aws_region": "us-east-1",
            "endpoint_url": s3.MINIO_ENDPOINT_URL,
            "aws_allow_http": "true",
            "aws_access_key_id": s3.MINIO_ACCESS_KEY,
            "aws_secret_access_key": s3.MINIO_SECRET_KEY,
        },
    )
    keys = {"AWS_ACCESS_KEY_ID": s3.MINIO_ACCESS_KEY, "AWS_SECRET_ACCESS_KEY": s3.MINIO_SECRET_KEY}
    minio = s3.get_minio_client()
    try:
        with _running(tmp_path, protocol=protocol, worker_env=keys, AWS_EC2_METADATA_DISABLED="true") as services:
            connection = {
                "connection_name": "minio from the environment",
                "storage_type": "s3",
                "auth_method": "env_vars",
                "aws_region": "us-east-1",
                "endpoint_url": s3.MINIO_ENDPOINT_URL,
                "aws_allow_unsafe_html": True,
            }
            services.post("/cloud_connections/cloud_connection", connection)
            catalog = services.post(
                "/catalog/namespaces",
                {
                    "name": "minio",
                    "storage_uri": storage_uri,
                    "storage_connection_name": connection["connection_name"],
                },
            )
            schema = services.post("/catalog/namespaces", {"name": "sales", "parent_id": catalog["id"]})
            table = services.post(
                "/catalog/tables/from-data",
                {
                    "name": "customers",
                    "table_path": table_path,
                    "namespace_id": schema["id"],
                    "schema_columns": [{"name": name, "dtype": dtype} for name, dtype in CUSTOMER_COLUMNS],
                    "row_count": len(CUSTOMERS),
                    "column_count": len(CUSTOMER_COLUMNS),
                    "size_bytes": 1,
                },
            )
            flow = {
                "flowfile_version": "0.23.0",
                "flowfile_id": CATALOG_FLOW_ID,
                "flowfile_name": "catalog read two storage dirs",
                "flowfile_settings": {"execution_mode": "Development", "execution_location": "remote"},
                "nodes": [
                    {
                        "id": 1,
                        "type": "catalog_reader",
                        "is_start_node": True,
                        "setting_input": {"catalog_table_id": table["id"]},
                    }
                ],
            }
            flow_path = tmp_path / "flow.json"
            flow_path.write_text(json.dumps(flow))
            flow_id = services.get("/import_flow/", flow_path=str(flow_path))
            run = services.run(flow_id)

            if protocol == "1":
                assert run["success"] is True and not _failures(run), _failures(run)
                preview = services.get("/node/data", flow_id=flow_id, node_id=1)
                assert [(column["name"], column["data_type"]) for column in preview["table_schema"]] == CUSTOMER_COLUMNS
                assert sorted(preview["data"], key=_key) == sorted(CUSTOMERS, key=_key)
                assert preview["number_of_records"] == len(CUSTOMERS)
                assert list((services.storage["core"] / "cache").iterdir()) == []
            else:
                assert run["success"] is False
                assert 1 in _failures(run), _failures(run)
    finally:
        for listed in minio.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix).get("Contents", []):
            minio.delete_object(Bucket=MINIO_BUCKET, Key=listed["Key"])


@pytest.mark.parametrize("protocol", ["1", "0"])
def test_cloud_reads_core_cannot_resolve_use_the_workers_identity_only(tmp_path, protocol):
    """A read with no saved connection and a directory read under an ``env_vars`` connection; only the worker has keys.

    With the flag off core builds both reads itself: boto3's default chain finds no credentials for the first, and the
    directory is read with none.
    """
    from test_utils.s3 import fixtures as s3

    available = s3.is_docker_available() and (
        s3.is_container_running(s3.MINIO_CONTAINER_NAME) or s3.start_minio_container()
    )
    if not (available and s3.wait_for_minio(max_retries=10)):
        pytest.skip("MinIO (Docker) is not available")
    prefix = f"two-dirs-cloud-{uuid.uuid4().hex}"
    base = f"s3://{MINIO_BUCKET}/{prefix}"
    options = {
        "aws_region": "us-east-1",
        "endpoint_url": s3.MINIO_ENDPOINT_URL,
        "aws_allow_http": "true",
        "aws_access_key_id": s3.MINIO_ACCESS_KEY,
        "aws_secret_access_key": s3.MINIO_SECRET_KEY,
    }
    customers = pl.DataFrame(CUSTOMERS)
    customers.write_parquet(f"{base}/customers.parquet", storage_options=options)
    customers.head(3).write_parquet(f"{base}/parts/part0.parquet", storage_options=options)
    customers.tail(3).write_parquet(f"{base}/parts/part1.parquet", storage_options=options)
    worker_env = {
        "AWS_ACCESS_KEY_ID": s3.MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": s3.MINIO_SECRET_KEY,
        "AWS_ENDPOINT_URL": s3.MINIO_ENDPOINT_URL,
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
    }
    minio = s3.get_minio_client()
    try:
        with _running(tmp_path, protocol=protocol, worker_env=worker_env, AWS_EC2_METADATA_DISABLED="true") as services:
            connection = {
                "connection_name": "minio from the environment",
                "storage_type": "s3",
                "auth_method": "env_vars",
                "aws_region": "us-east-1",
                "endpoint_url": s3.MINIO_ENDPOINT_URL,
                "aws_allow_unsafe_html": True,
            }
            services.post("/cloud_connections/cloud_connection", connection)
            readers = {
                1: {"resource_path": f"{base}/customers.parquet", "auth_mode": "aws-cli", "connection_name": None},
                2: {
                    "resource_path": f"{base}/parts/",
                    "scan_mode": "directory",
                    "connection_name": connection["connection_name"],
                },
            }
            flow = {
                "flowfile_version": "0.23.0",
                "flowfile_id": CLOUD_FLOW_ID,
                "flowfile_name": "cloud read two storage dirs",
                "flowfile_settings": {"execution_mode": "Development", "execution_location": "remote"},
                "nodes": [
                    {
                        "id": node_id,
                        "type": "cloud_storage_reader",
                        "is_start_node": True,
                        "setting_input": {"cloud_storage_settings": {"file_format": "parquet", **read}},
                    }
                    for node_id, read in readers.items()
                ],
            }
            flow_path = tmp_path / "flow.json"
            flow_path.write_text(json.dumps(flow))
            flow_id = services.get("/import_flow/", flow_path=str(flow_path))
            run = services.run(flow_id)

            if protocol == "1":
                assert run["success"] is True and not _failures(run), _failures(run)
                for node_id in readers:
                    preview = services.get("/node/data", flow_id=flow_id, node_id=node_id)
                    columns = [(column["name"], column["data_type"]) for column in preview["table_schema"]]
                    assert columns == CUSTOMER_COLUMNS, node_id
                    assert sorted(preview["data"], key=_key) == sorted(CUSTOMERS, key=_key), node_id
                    assert preview["number_of_records"] == len(CUSTOMERS), node_id
                assert list((services.storage["core"] / "cache").iterdir()) == []
            else:
                assert run["success"] is False
                assert set(readers) <= set(_failures(run)), _failures(run)
    finally:
        for listed in minio.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix).get("Contents", []):
            minio.delete_object(Bucket=MINIO_BUCKET, Key=listed["Key"])


def test_a_model_trained_on_the_worker_scores_the_next_node_with_no_shared_disk(tmp_path):
    with _running(tmp_path, protocol="1") as services:
        flow_id = services.get("/import_flow/", flow_path=str(_ml_flow_file(tmp_path, ML_FLOW_ID)))
        run = services.run(flow_id)

        assert run["success"] is True and not _failures(run), _failures(run)
        assert _predictions(services, flow_id) == [row["y"] for row in TRAIN_ROWS]
        assert list((services.storage["core"] / "cache").iterdir()) == []
        assert list((services.storage["worker"] / "cache").rglob("*.blob"))


@pytest.fixture(scope="module")
def minio_over_tls(tmp_path_factory):
    """MinIO behind an HTTPS forwarder with a self-signed certificate: the forwarder's URL and the certificate's path.

    MinIO is had as the cloud end-to-end suite has it, and the test skips when Docker cannot give one. A URL the S3
    artifact store presigns on the forwarder's endpoint is an ``https://`` one that MinIO accepts: the Host header passes
    through.
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    from test_utils.s3 import fixtures as s3

    available = s3.is_docker_available() and (
        s3.is_container_running(s3.MINIO_CONTAINER_NAME) or s3.start_minio_container()
    )
    if not (available and s3.wait_for_minio(max_retries=10)):
        pytest.skip("MinIO (Docker) is not available")
    folder = tmp_path_factory.mktemp("tls")
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    (folder / "cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (folder / "key.pem").write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(folder / "cert.pem", folder / "key.pem")
    upstream_host, upstream_port = s3.MINIO_ENDPOINT_URL.removeprefix("http://").split(":")

    async def pipe(reader, writer):
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    async def forward(client_reader, client_writer):
        upstream_reader, upstream_writer = await asyncio.open_connection(upstream_host, int(upstream_port))
        await asyncio.gather(pipe(client_reader, upstream_writer), pipe(upstream_reader, client_writer))

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda _loop, _context: None)
    server = loop.run_until_complete(asyncio.start_server(forward, "127.0.0.1", 0, ssl=context))
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield f"https://localhost:{server.sockets[0].getsockname()[1]}", folder / "cert.pem"
    finally:
        loop.call_soon_threadsafe(server.close)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


def test_a_model_published_to_minio_is_applied_from_the_catalog_with_no_shared_disk(tmp_path, minio_over_tls):
    """Both services trust the forwarder's certificate: core's boto3 finalises the version, the worker moves the bytes."""
    from test_utils.s3 import fixtures as s3

    endpoint, certificate = minio_over_tls
    prefix = f"two-dirs-{uuid.uuid4().hex}/"
    store = {
        "FLOWFILE_ARTIFACT_STORAGE": "s3",
        "FLOWFILE_S3_BUCKET": MINIO_BUCKET,
        "FLOWFILE_S3_PREFIX": prefix,
        "FLOWFILE_S3_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": s3.MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": s3.MINIO_SECRET_KEY,
        "AWS_CA_BUNDLE": str(certificate),
        "REQUESTS_CA_BUNDLE": str(certificate),
    }
    minio = s3.get_minio_client()
    flow_file = _ml_flow_file(tmp_path, ML_FLOW_ID, published=f"model_{uuid.uuid4().hex[:12]}")
    try:
        with _running(tmp_path, protocol="1", **store) as services:
            flow_id = services.get("/import_flow/", flow_path=str(flow_file))
            run = services.run(flow_id)

            assert run["success"] is True and not _failures(run), _failures(run)
            assert _predictions(services, flow_id) == [row["y"] for row in TRAIN_ROWS]
            assert len(minio.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix).get("Contents", [])) == 1
            assert list((services.storage["core"] / "cache").iterdir()) == []
    finally:
        for listed in minio.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix).get("Contents", []):
            minio.delete_object(Bucket=MINIO_BUCKET, Key=listed["Key"])
