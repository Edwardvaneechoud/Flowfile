"""Cloud reads never put decrypted credentials into a serialized query plan.

Every test captures each plan core serializes and asserts MinIO's root secret is absent while the data still reads.
"""

import io
import os
import subprocess
import sys
import textwrap
import uuid
from pathlib import Path

import polars as pl
import pytest
from pydantic import SecretStr

from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.catalog.storage_backend import resolve_for_namespace
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    store_cloud_connection,
)
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import CloudStorageReader
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageReadSettings,
    CloudStorageWriteSettings,
    FullCloudStorageConnection,
)
from shared.delta_utils import write_delta
from tests.flowfile.conftest import add_test_manual_input, catalog_cleanup, create_test_graph
from tests.test_catalog_cloud_virtual import _create_cloud_schema, _ensure_connection, requires_minio

try:
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client
except ModuleNotFoundError:  # pragma: no cover - import shim for ad-hoc runs
    sys.path.append(os.path.dirname(os.path.abspath("test_utils/s3/fixtures.py")))
    from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client

from test_utils.s3.aws_profiles import isolate_aws

REPO_ROOT = Path(__file__).resolve().parents[3]
_BUCKET = "flowfile-test"
SECRET = MINIO_SECRET_KEY.encode()
_ROOT_OPTIONS = {
    "aws_region": "us-east-1",
    "endpoint_url": MINIO_ENDPOINT_URL,
    "aws_allow_http": "true",
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
}
_ROWS = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]


def _connection(name: str, auth_method: str, **fields) -> FullCloudStorageConnection:
    return FullCloudStorageConnection(
        connection_name=name,
        storage_type="s3",
        auth_method=auth_method,
        aws_region="us-east-1",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_allow_unsafe_html=True,
        **fields,
    )


@pytest.fixture
def serialized_plans(monkeypatch):
    """Every plan serialized in this process during the test: what core ships to the worker."""
    captured: list[bytes] = []
    original = pl.LazyFrame.serialize

    def _spy(self, file=None, **kwargs):
        blob = original(self, **kwargs)
        captured.append(blob)
        return blob if file is None else original(self, file, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "serialize", _spy)
    return captured


@pytest.fixture
def minio_prefix():
    prefix = f"plan-secrets-{uuid.uuid4().hex[:8]}"
    yield prefix
    client = get_minio_client()
    for obj in client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix).get("Contents", []):
        client.delete_object(Bucket=_BUCKET, Key=obj["Key"])


@pytest.fixture
def stored_connection():
    """Store a MinIO connection under a unique name for user 1; returns a factory, deletes them after."""
    names: list[str] = []

    def _store(auth_method: str, **fields) -> str:
        name = f"plan secrets {auth_method} {uuid.uuid4().hex[:6]}"
        with get_db_context() as db:
            store_cloud_connection(db, _connection(name, auth_method, **fields), user_id=1)
        names.append(name)
        return name

    yield _store
    with get_db_context() as db:
        for name in names:
            delete_cloud_connection(db, name, user_id=1)


@pytest.fixture
def minio_sts(monkeypatch, tmp_path):
    """Route iam_role's STS AssumeRole to MinIO, with MinIO's root keys as core's ambient credentials.

    Only core gets these, so a remote read can succeed only through the credentials encrypted into the plan.
    """
    isolate_aws(
        monkeypatch,
        tmp_path,
        endpoint=None,
        AWS_ACCESS_KEY_ID=MINIO_ACCESS_KEY,
        AWS_SECRET_ACCESS_KEY=MINIO_SECRET_KEY,
        AWS_ENDPOINT_URL_STS=MINIO_ENDPOINT_URL,
    )


@pytest.fixture
def storage_options_spy(monkeypatch):
    """Records every options dict built from a connection (the real builder runs)."""
    built: list[dict] = []
    original = CloudStorageReader.get_storage_options

    def _spy(connection):
        options = original(connection)
        built.append(options)
        return options

    monkeypatch.setattr(CloudStorageReader, "get_storage_options", staticmethod(_spy))
    return built


def _seed_files(prefix: str) -> dict[str, str]:
    """Parquet, a two-file parquet directory, CSV, NDJSON and a change-tracked Delta table under *prefix*."""
    frame = pl.DataFrame(_ROWS)
    base = f"s3://{_BUCKET}/{prefix}"
    frame.write_parquet(f"{base}/single.parquet", storage_options=_ROOT_OPTIONS)
    frame.write_parquet(f"{base}/dir/part0.parquet", storage_options=_ROOT_OPTIONS)
    frame.write_parquet(f"{base}/dir/part1.parquet", storage_options=_ROOT_OPTIONS)
    frame.write_csv(f"{base}/data.csv", storage_options=_ROOT_OPTIONS)
    frame.lazy().sink_ndjson(f"{base}/data.json", storage_options=_ROOT_OPTIONS)
    write_delta(frame.head(2), f"{base}/delta", mode="overwrite", storage_options=_ROOT_OPTIONS, enable_cdf=True)
    write_delta(frame.tail(1), f"{base}/delta", mode="append", storage_options=_ROOT_OPTIONS)
    return {
        "single": f"{base}/single.parquet",
        "dir": f"{base}/dir/",
        "csv": f"{base}/data.csv",
        "json": f"{base}/data.json",
        "delta": f"{base}/delta",
    }


def _add_reader(graph: FlowGraph, node_id: int, connection_name: str, **settings) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="cloud_storage_reader")
    )
    graph.add_cloud_storage_reader(
        input_schema.NodeCloudStorageReader(
            flow_id=graph.flow_id,
            node_id=node_id,
            user_id=1,
            cloud_storage_settings=CloudStorageReadSettings(connection_name=connection_name, **settings),
        )
    )


def _add_record_count(graph: FlowGraph, node_id: int, depending_on_id: int) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="record_count"))
    graph.add_record_count(
        input_schema.NodeRecordCount(flow_id=graph.flow_id, node_id=node_id, depending_on_id=depending_on_id)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))


def _count(graph: FlowGraph, node_id: int) -> int:
    return graph.get_node(node_id).get_resulting_data().collect()["number_of_records"].item()


def _assert_run_succeeded(run_info) -> None:
    failures = [(step.node_id, step.error) for step in run_info.node_step_result if not step.success]
    assert run_info.success, failures


def _assert_no_secret(plans: list[bytes], *secrets: bytes) -> None:
    leaked = [i for i, plan in enumerate(plans) if any(secret and secret in plan for secret in secrets)]
    assert leaked == [], f"{len(leaked)} of {len(plans)} serialized plans carry a plaintext secret"


class TestSecureScanKwargs:
    """The helper every serialized cloud scan goes through; no I/O."""

    def test_credentials_leave_the_options_and_come_back_decrypted(self):
        sentinel = f"SENTINEL-{uuid.uuid4().hex}"
        options = {**_ROOT_OPTIONS, "aws_secret_access_key": sentinel, "aws_session_token": ""}

        kwargs = CloudStorageReader.get_secure_scan_kwargs(options, user_id=1)

        assert kwargs["storage_options"] == {
            "aws_region": "us-east-1",
            "endpoint_url": MINIO_ENDPOINT_URL,
            "aws_allow_http": "true",
        }
        assert kwargs["credential_provider"].encrypted_credentials.startswith("$ffsec$1$1$")
        assert kwargs["credential_provider"]()[0]["aws_secret_access_key"] == sentinel
        plan = pl.scan_parquet(f"s3://{_BUCKET}/never-read.parquet", **kwargs).select("x").serialize()
        assert sentinel.encode() not in plan

    def test_without_an_owner_the_legacy_master_key_format_still_round_trips(self):
        kwargs = CloudStorageReader.get_secure_scan_kwargs({"aws_secret_access_key": "s"}, user_id=None)
        assert not kwargs["credential_provider"].encrypted_credentials.startswith("$ffsec$")
        assert kwargs["credential_provider"]() == ({"aws_secret_access_key": "s"}, None)

    @pytest.mark.parametrize("options", [None, {}, {"aws_region": "us-east-1"}])
    def test_options_without_credentials_get_no_provider(self, options):
        kwargs = CloudStorageReader.get_secure_scan_kwargs(options, user_id=1)
        assert "credential_provider" not in kwargs
        assert kwargs.get("storage_options") == (options or None)


@requires_minio
def test_cloud_reader_plans_carry_no_secret(execution_location, stored_connection, minio_prefix, serialized_plans):
    paths = _seed_files(minio_prefix)
    connection_name = stored_connection(
        "access_key", aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=SecretStr(MINIO_SECRET_KEY)
    )
    graph = create_test_graph(flow_id=7401, execution_location=execution_location)
    readers = {
        1: {"resource_path": paths["single"], "file_format": "parquet"},
        3: {"resource_path": paths["dir"], "file_format": "parquet", "scan_mode": "directory"},
        5: {"resource_path": paths["csv"], "file_format": "csv"},
        7: {"resource_path": paths["json"], "file_format": "json"},
        9: {"resource_path": paths["delta"], "file_format": "delta"},
        11: {
            "resource_path": paths["delta"],
            "file_format": "delta",
            "cdc_mode": "since_version",
            "cdc_from_version": 0,
        },
    }
    for node_id, settings in readers.items():
        _add_reader(graph, node_id, connection_name, **settings)
        _add_record_count(graph, node_id + 1, node_id)

    run_info = graph.run_graph()

    _assert_run_succeeded(run_info)
    # since_version 0 reads the commits after version 0: the one-row append.
    assert {node_id + 1: _count(graph, node_id + 1) for node_id in readers} == {2: 3, 4: 6, 6: 3, 8: 3, 10: 3, 12: 1}
    reader_plans = [graph.get_node(node_id).get_resulting_data().data_frame.serialize() for node_id in readers]
    if execution_location == "remote":
        assert len(serialized_plans) > len(reader_plans), "the run shipped no plan to the worker"
    _assert_no_secret(serialized_plans, SECRET)


@requires_minio
def test_iam_role_reader_plan_carries_no_temporary_credentials(
    execution_location, stored_connection, minio_prefix, minio_sts, storage_options_spy, serialized_plans
):
    """iam_role assumes the role in core; the temporary key, secret and session token stay encrypted."""
    paths = _seed_files(minio_prefix)
    connection_name = stored_connection("iam_role", aws_role_arn="arn:aws:iam::123456789012:role/flowfile-test")
    graph = create_test_graph(flow_id=7402, execution_location=execution_location)
    _add_reader(graph, 1, connection_name, resource_path=paths["single"], file_format="parquet")
    _add_record_count(graph, 2, 1)

    run_info = graph.run_graph()

    _assert_run_succeeded(run_info)
    assert _count(graph, 2) == 3
    graph.get_node(1).get_resulting_data().data_frame.serialize()
    sts_options = [options for options in storage_options_spy if options.get("aws_session_token")]
    assert sts_options, "the iam_role connection never assumed a role"
    temporary = [
        options[key].encode()
        for options in sts_options
        for key in ("aws_access_key_id", "aws_secret_access_key", "aws_session_token")
    ]
    _assert_no_secret(serialized_plans, SECRET, *temporary)


@requires_minio
def test_iam_role_local_write_uses_the_assumed_role(stored_connection, minio_prefix, minio_sts):
    """The removed placeholder provider also shadowed the STS credentials of core-side (local) writes."""
    connection_name = stored_connection("iam_role", aws_role_arn="arn:aws:iam::123456789012:role/flowfile-test")
    target = f"s3://{_BUCKET}/{minio_prefix}/iam_out.parquet"
    graph = create_test_graph(flow_id=7403, execution_location="local")
    add_test_manual_input(graph, _ROWS, node_id=1)
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="cloud_storage_writer"))
    graph.add_cloud_storage_writer(
        input_schema.NodeCloudStorageWriter(
            flow_id=graph.flow_id,
            node_id=2,
            user_id=1,
            depending_on_id=1,
            cloud_storage_settings=CloudStorageWriteSettings(
                resource_path=target, file_format="parquet", write_mode="overwrite", connection_name=connection_name
            ),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    _assert_run_succeeded(graph.run_graph())
    assert pl.read_parquet(target, storage_options=_ROOT_OPTIONS).height == 3


@pytest.fixture
def azurite_container():
    from test_utils.azurite.fixtures import get_blob_service_client, is_azurite_reachable

    if not is_azurite_reachable():
        pytest.skip("Azurite not available")
    client = get_blob_service_client()
    name = f"plan-secrets-{uuid.uuid4().hex[:8]}"
    client.create_container(name)
    yield client.get_container_client(name)
    client.delete_container(name)


def test_adls_reader_plan_carries_no_account_key(execution_location, azurite_container, serialized_plans):
    from test_utils.azurite.fixtures import AZURITE_ACCOUNT_KEY, AZURITE_ACCOUNT_NAME, AZURITE_BLOB_PORT, AZURITE_HOST

    buffer = io.BytesIO()
    pl.DataFrame(_ROWS).write_parquet(buffer)
    azurite_container.upload_blob("data.parquet", buffer.getvalue())
    connection_name = f"plan secrets adls {uuid.uuid4().hex[:6]}"
    with get_db_context() as db:
        store_cloud_connection(
            db,
            FullCloudStorageConnection(
                connection_name=connection_name,
                storage_type="adls",
                auth_method="access_key",
                azure_account_name=AZURITE_ACCOUNT_NAME,
                azure_account_key=SecretStr(AZURITE_ACCOUNT_KEY),
                endpoint_url=f"http://{AZURITE_HOST}:{AZURITE_BLOB_PORT}",
            ),
            user_id=1,
        )
    try:
        graph = create_test_graph(flow_id=7406, execution_location=execution_location)
        path = f"az://{azurite_container.container_name}/data.parquet"
        _add_reader(graph, 1, connection_name, resource_path=path, file_format="parquet")
        _add_record_count(graph, 2, 1)

        _assert_run_succeeded(graph.run_graph())

        assert _count(graph, 2) == 3
        graph.get_node(1).get_resulting_data().data_frame.serialize()
        _assert_no_secret(serialized_plans, AZURITE_ACCOUNT_KEY.encode())
    finally:
        with get_db_context() as db:
            delete_cloud_connection(db, connection_name, user_id=1)


@requires_minio
class TestCatalogReadersOnObjectStorage:
    @pytest.fixture(autouse=True)
    def _clean_catalog(self, monkeypatch):
        monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_URI", raising=False)
        monkeypatch.delenv("FLOWFILE_CATALOG_STORAGE_CONNECTION", raising=False)
        catalog_cleanup()
        yield
        catalog_cleanup()

    def test_table_sql_and_change_reads_carry_no_secret(self, execution_location, minio_prefix, serialized_plans):
        _ensure_connection()
        schema_id = _create_cloud_schema(f"s3://{_BUCKET}/{minio_prefix}/catalog")
        target = resolve_for_namespace(schema_id)
        dest = f"s3://{_BUCKET}/{minio_prefix}/catalog/secret_probe"
        write_delta(pl.DataFrame(_ROWS), dest, mode="overwrite", storage_options=target.storage_options)
        table_name = f"secret_probe_{uuid.uuid4().hex[:6]}"
        with get_db_context() as db:
            svc = CatalogService(SQLAlchemyCatalogRepository(db))
            table_id = svc.register_table_from_data(
                name=table_name,
                table_path=dest,
                owner_id=1,
                namespace_id=schema_id,
                storage_format="delta",
                schema=[{"name": "id", "dtype": "Int64"}, {"name": "name", "dtype": "Utf8"}],
                row_count=3,
                column_count=2,
                size_bytes=100,
            ).id
            svc.enable_table_cdc(table_id)
        write_delta(pl.DataFrame(_ROWS[:1]), dest, mode="append", storage_options=target.storage_options)

        graph = create_test_graph(flow_id=7404, execution_location=execution_location)
        readers = {
            1: input_schema.NodeCatalogReader(flow_id=graph.flow_id, node_id=1, catalog_table_id=table_id),
            3: input_schema.NodeCatalogReader(
                flow_id=graph.flow_id, node_id=3, sql_query=f"SELECT * FROM {table_name} WHERE id > 1"
            ),
            5: input_schema.NodeCatalogReader(
                flow_id=graph.flow_id,
                node_id=5,
                catalog_table_id=table_id,
                cdc_mode="since_version",
                cdc_from_version=0,
            ),
        }
        for node_id, settings in readers.items():
            graph.add_node_promise(
                input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="catalog_reader")
            )
            graph.add_catalog_reader(settings)
            _add_record_count(graph, node_id + 1, node_id)

        run_info = graph.run_graph()

        _assert_run_succeeded(run_info)
        assert {node_id + 1: _count(graph, node_id + 1) for node_id in readers} == {2: 4, 4: 2, 6: 1}
        for node_id in readers:
            graph.get_node(node_id).get_resulting_data().data_frame.serialize()
        _assert_no_secret(serialized_plans, SECRET)


@requires_minio
class TestPlanExecutesWhereItIsDecrypted:
    """A core-built plan run in a fresh worker-shaped process, the way a spawned worker child runs it."""

    @pytest.fixture
    def reader_plan(self, stored_connection, minio_prefix) -> bytes:
        paths = _seed_files(minio_prefix)
        connection_name = stored_connection(
            "access_key", aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=SecretStr(MINIO_SECRET_KEY)
        )
        graph = create_test_graph(flow_id=7405, execution_location="local")
        _add_reader(graph, 1, connection_name, resource_path=paths["single"], file_format="parquet")
        _assert_run_succeeded(graph.run_graph())
        plan = graph.get_node(1).get_resulting_data().data_frame.serialize()
        assert SECRET not in plan
        return plan

    def _run_in_worker_child(self, plan: bytes, tmp_path: Path, **env_overrides) -> subprocess.CompletedProcess:
        plan_file = tmp_path / "plan.bin"
        plan_file.write_bytes(plan)
        code = textwrap.dedent(
            f"""
            import io, multiprocessing
            multiprocessing.current_process().name = "Worker-1"
            import flowfile_worker.funcs  # the spawned child's entry surface registers the decryptor
            import polars as pl
            print(pl.LazyFrame.deserialize(io.BytesIO(open({str(plan_file)!r}, "rb").read())).collect().height)
            """
        )
        env = {k: v for k, v in os.environ.items() if not k.startswith("AWS_") and k != "TEST_MODE"}
        env.update(
            AWS_EC2_METADATA_DISABLED="true",
            PYTHONPATH=os.pathsep.join([str(REPO_ROOT / "flowfile_worker"), str(REPO_ROOT)]),
        )
        env.update(env_overrides)
        return subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, env=env, cwd=tmp_path, timeout=120
        )

    def test_worker_child_decrypts_with_the_shared_master_key(self, reader_plan, tmp_path):
        ran = self._run_in_worker_child(reader_plan, tmp_path)
        assert ran.returncode == 0, ran.stderr
        assert ran.stdout.strip().splitlines()[-1] == "3"

    def test_a_worker_without_the_master_key_fails_with_a_clear_error(self, reader_plan, tmp_path):
        empty_store = tmp_path / "other_secure_store"
        empty_store.mkdir()

        ran = self._run_in_worker_child(reader_plan, tmp_path, FLOWFILE_SECURE_STORAGE_PATH=str(empty_store))

        assert ran.returncode != 0
        assert "Could not decrypt the cloud storage credentials embedded in this query plan" in ran.stderr
        assert "Master key not found" in ran.stderr
        assert MINIO_SECRET_KEY not in ran.stderr
