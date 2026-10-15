"""The exchange store: where results cross between backends during a run, and the removal of a run's prefix after it.

Over the MinIO test container: each test stores a MinIO connection for user 1 under a fresh name and points
``FLOWFILE_EXCHANGE_URI`` at a fresh ``s3://test-bucket/exchange-<id>``, emptied after. The protocol flag comes from
the environment, as for the two-backend chain test whose frames and nodes these tests reuse.
"""

import re
import uuid
from collections.abc import Iterator

import pytest
from pydantic import SecretStr

from flowfile_core import events
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_cloud_connection,
    store_cloud_connection,
)
from flowfile_core.flowfile.execution import exchange, registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.exchange import ExchangeRun, exchange_store, remove_prefix
from flowfile_core.schemas.cloud_storage_schemas import FullCloudStorageConnection
from test_utils.s3.fixtures import MINIO_ACCESS_KEY, MINIO_ENDPOINT_URL, MINIO_SECRET_KEY, get_minio_client
from tests.flowfile.conftest import add_test_manual_input, create_test_graph
from tests.flowfile.execution.test_two_backend_chain import (
    STATES,
    Held,
    assert_node_b_is_correct,
    node_a,
    node_b,
)
from tests.test_catalog_cloud_virtual import requires_minio

_BUCKET = "test-bucket"


def _no_client(*args, **kwargs):
    raise AssertionError("no S3 client is made when the run holds no prefix")


def _key(prefix: str) -> str:
    """The key prefix an ``s3://test-bucket/…`` URI names."""
    return prefix.removeprefix(f"s3://{_BUCKET}/")


def _keys(prefix: str) -> list[str]:
    """The keys MinIO lists under *prefix*, an ``s3://test-bucket/…`` URI or a key prefix."""
    pages = get_minio_client().get_paginator("list_objects_v2").paginate(Bucket=_BUCKET, Prefix=_key(prefix))
    return sorted(item["Key"] for page in pages for item in page.get("Contents", []))


@pytest.fixture(autouse=True)
def _unset(monkeypatch):
    monkeypatch.delenv("FLOWFILE_EXCHANGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_EXCHANGE_CONNECTION", raising=False)


@pytest.fixture
def local() -> LocalBackend:
    return LocalBackend()


@pytest.fixture
def worker() -> WorkerBackend:
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")
    return WorkerBackend(url=registry.backend(registry.WORKER_BACKEND_ID).url)


@pytest.fixture
def kept() -> Iterator[Held]:
    held: Held = []
    yield held
    for backend, ref in held:
        backend.release(ref)


@pytest.fixture
def connection_name() -> Iterator[str]:
    """A MinIO connection stored for user 1 under a fresh name, deleted after."""
    name = f"exchange store {uuid.uuid4().hex[:6]}"
    connection = FullCloudStorageConnection(
        connection_name=name,
        storage_type="s3",
        auth_method="access_key",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=SecretStr(MINIO_SECRET_KEY),
        aws_region="us-east-1",
        endpoint_url=MINIO_ENDPOINT_URL,
        aws_allow_unsafe_html=True,
    )
    with get_db_context() as db:
        store_cloud_connection(db, connection, user_id=1)
    yield name
    with get_db_context() as db:
        delete_cloud_connection(db, name, user_id=1)


@pytest.fixture
def root(monkeypatch, connection_name) -> Iterator[str]:
    """The exchange store configured at a fresh key prefix of the test bucket; every object under it deleted after."""
    root = f"exchange-{uuid.uuid4().hex[:8]}"
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/{root}")
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection_name)
    yield root
    client = get_minio_client()
    for key in _keys(f"{root}/"):
        client.delete_object(Bucket=_BUCKET, Key=key)


def test_no_store_is_configured_without_its_uri_and_a_run_then_calls_nothing(monkeypatch):
    monkeypatch.setattr(exchange, "build_s3_client", _no_client)
    run = ExchangeRun(flow_id=1, run_id=uuid.uuid4().hex)

    assert exchange_store(1) is None
    assert run.prefix(1) is None
    run.collect()


@pytest.mark.parametrize(
    "uri, connection, named",
    [
        ("s3://test-bucket/exchange", None, "FLOWFILE_EXCHANGE_CONNECTION"),
        ("gs://bucket/exchange", "any", "'gs'"),
        ("az://container/exchange", "any", "'az'"),
        ("file:///tmp/exchange", "any", "'file'"),
        ("s3://test-bucket/exchange", f"no connection {uuid.uuid4().hex[:6]}", "'no connection "),
    ],
    ids=["no-connection", "gs", "az", "file", "unknown-connection"],
)
def test_a_misconfigured_store_is_refused_naming_what_is_wrong(monkeypatch, uri, connection, named):
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", uri)
    if connection is not None:
        monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection)
    run = ExchangeRun(flow_id=1, run_id=uuid.uuid4().hex)

    with pytest.raises(ValueError, match=re.escape(named)):
        exchange_store(1)
    with pytest.raises(ValueError, match=re.escape(named)):
        run.prefix(1)


@requires_minio
@STATES
@pytest.mark.parametrize("direction", ["local-to-worker", "worker-to-local"])
def test_a_result_crosses_between_backends_through_the_store(root, local, worker, kept, must_materialise, direction):
    source, target = (local, worker) if direction == "local-to-worker" else (worker, local)
    prefix, credential = ExchangeRun(flow_id=-1, run_id=uuid.uuid4().hex).prefix(1)
    ref_a = node_a(source, kept, must_materialise)
    uri = f"{prefix}edge.parquet"

    assert source.export(ref_a, uri, credential) == uri

    assert _keys(prefix) == [f"{_key(prefix)}edge.parquet"]
    edge = target.import_uri(uri, ref_a.schema, source.count(ref_a), credential)
    kept.append((target, edge))
    assert (edge.backend_id, edge.state) == (target.id, "deferred")
    assert [(c.column_name, c.data_type) for c in edge.schema] == [(c.column_name, c.data_type) for c in ref_a.schema]
    ref_b = node_b(target, edge, kept)
    assert ref_b.backend_id == target.id
    assert_node_b_is_correct(target, ref_b)


@requires_minio
def test_collect_removes_its_runs_prefix_and_leaves_another_runs(root, monkeypatch):
    ours, theirs = ExchangeRun(flow_id=7, run_id=uuid.uuid4().hex), ExchangeRun(flow_id=7, run_id=uuid.uuid4().hex)
    our_prefix, their_prefix = ours.prefix(1)[0], theirs.prefix(1)[0]
    ours_written = [f"{_key(our_prefix)}edge-{i}.parquet" for i in range(3)] + [f"{_key(our_prefix)}a/b.parquet"]
    theirs_written = [f"{_key(their_prefix)}edge.parquet"]
    client = get_minio_client()
    for key in ours_written + theirs_written:
        client.put_object(Bucket=_BUCKET, Key=key, Body=b"rows")

    ours.collect()

    assert _keys(our_prefix) == []
    assert _keys(their_prefix) == theirs_written
    monkeypatch.setattr(exchange, "build_s3_client", _no_client)
    ours.collect()


@requires_minio
def test_run_graph_removes_its_runs_prefix_when_the_run_ends(root, local, kept):
    graph = create_test_graph(flow_id=51)
    add_test_manual_input(graph, [{"id": 1, "amount": 2.5}])
    seen: dict = {}

    def write_an_edge(graph, **_):
        try:
            prefix, credential = graph.exchange_run.prefix(1)
            ref = node_a(local, kept, must_materialise=False)
            local.export(ref, f"{prefix}edge.parquet", credential)
            seen.update(prefix=prefix, run_id=graph.exchange_run.run_id, listed=_keys(prefix))
        except Exception as e:
            seen["error"] = e

    events.subscribe("flow_run_started", write_an_edge)
    try:
        run_info = graph.run_graph()
    finally:
        events._handlers["flow_run_started"].remove(write_an_edge)

    assert "error" not in seen, seen.get("error")
    assert run_info.success
    assert seen["prefix"] == f"s3://{_BUCKET}/{root}/{graph.flow_id}/{seen['run_id']}/"
    assert seen["listed"] == [f"{_key(seen['prefix'])}edge.parquet"]
    assert _keys(seen["prefix"]) == []
    assert graph.exchange_run is None


class RefusingBucket:
    """An S3 client whose one page lists *keys* and which refuses every delete, recording each request."""

    def __init__(self, keys: list[str]):
        self.keys = keys
        self.deleted: list[dict] = []

    def get_paginator(self, operation: str) -> "RefusingBucket":
        return self

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict]:
        return [{"Contents": [{"Key": key} for key in self.keys]}]

    def delete_objects(self, **request) -> dict:
        self.deleted.append(request)
        objects = request["Delete"]["Objects"]
        return {"Errors": [{"Key": o["Key"], "Code": "AccessDenied", "Message": "Access Denied"} for o in objects]}


def test_a_delete_the_store_refuses_fails_the_removal_naming_how_many_and_why(monkeypatch, connection_name):
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection_name)
    prefix = f"s3://{_BUCKET}/exchange/7/{uuid.uuid4().hex}/"
    keys = [f"{_key(prefix)}1-output-0-2.parquet", f"{_key(prefix)}2-output-0-3.parquet"]
    client = RefusingBucket(keys)
    monkeypatch.setattr(exchange, "build_s3_client", lambda *args, **kwargs: client)

    with pytest.raises(OSError) as raised:
        remove_prefix(exchange_store(1), prefix)

    assert str(raised.value) == f"Could not delete 2 object(s) under {prefix}: Access Denied"
    assert client.deleted == [{"Bucket": _BUCKET, "Delete": {"Objects": [{"Key": key} for key in keys], "Quiet": True}}]
