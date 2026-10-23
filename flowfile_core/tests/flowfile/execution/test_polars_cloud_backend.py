"""The Polars backend: core's client of a Polars cluster the operator runs, against a fake cluster in this process.

No test reaches a cluster. ``FakeCluster`` stands in for both ends polars-cloud gives: its ``context`` is the factory
the backend makes its client context with, and ``LazyFrame.remote`` is patched to a ``FakeRemote`` that runs the plan
here (``show`` collects its head, a single-node sink writes the parquet under a local folder named for the URI), so
each test sees which calls reached the cluster and with which storage options. The exchange store is a fixed
``ExchangeStore`` whose connection's options hold a key, so a test shows that only the plain ones reach a plan, and
its deletions are recorded instead of sent. One test runs a flow on the backend from end to end; the unreachable
case uses a closed port and the real ``polars_cloud.ClusterContext`` replaced by a recorder, never constructed.
"""

import io
import json
import socket
import time
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import polars_cloud
import pytest

from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import exchange, registry
from flowfile_core.flowfile.execution.backends import polars_cloud as backend_module
from flowfile_core.flowfile.execution.backends.polars_cloud import PolarsCloudBackend, address_refusal
from flowfile_core.flowfile.execution.backends.worker import _flowfile_schema
from flowfile_core.flowfile.execution.errors import (
    ComputeUnavailable,
    ForeignRef,
    OperationUnsupported,
    ResultMissing,
    TaskFailed,
)
from flowfile_core.flowfile.execution.exchange import ExchangeStore
from flowfile_core.flowfile.execution.placement import SingleBackendPolicy, place
from flowfile_core.flowfile.execution.protocol import CredentialRef, OperationKind, SubmitHints
from flowfile_core.flowfile.flow_data_engine.cloud_storage_reader import NATIVE_CREDENTIALS, CloudStorageReader
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageReadSettings,
    CloudStorageReadSettingsInternal,
    FullCloudStorageConnection,
)
from shared.compute_protocol import local_capabilities
from tests.flowfile.conftest import create_test_graph
from tests.flowfile.execution.test_placement import add_cloud_reader, add_manual_input, add_promise, connect, fuzzy_flow
from tests.flowfile.execution.test_two_backend_chain import CUSTOMERS, EXPECTED

BACKEND_ID = "polars-cloud:lab"
SCHEDULER = "http://127.0.0.1:5051"
DASHBOARD = "http://127.0.0.1:3001"
STORE = ExchangeStore(uri="s3://exchange/flowfile", credential=CredentialRef(connection_id=7, user_id=1))
PLAIN = {"aws_region": "nbg1", "endpoint_url": "https://objects.test"}
DEFERRED = SubmitHints(must_materialise=False, wide=False, fanout=1, preview_rows=2)
CACHED = SubmitHints(must_materialise=True, wide=True, fanout=1, preview_rows=None)
ROWS = pl.LazyFrame({"region": ["north", "south", "north"], "amount": [12.5, 8.0, 30.0]})


class FakeRemote:
    """``plan.remote(context)`` on the fake cluster: the plan runs in this process."""

    def __init__(self, plan: pl.LazyFrame, cluster: "FakeCluster"):
        self.plan = plan
        self.cluster = cluster

    def show(self, n: int) -> pl.DataFrame:
        self.cluster.record(("show", n))
        return self.plan.head(n).collect()

    def single_node(self) -> "FakeRemote":
        return self

    def sink_parquet(self, uri: str, *, storage_options=None, credential_provider="auto"):
        assert credential_provider is None, "a sink would carry a Python credential provider to the cluster"
        self.cluster.record(("sink", uri, storage_options))
        path = self.cluster.local_path(uri)
        self.plan.sink_parquet(path)
        rows = pl.scan_parquet(path).select(pl.len()).collect().item()
        return SimpleNamespace(await_result=lambda silent=False: SimpleNamespace(n_rows_total=rows))


class FakeCluster:
    """The client context factory and the cluster behind it; ``fail`` makes every call raise it."""

    def __init__(self, disk: Path):
        self.disk = disk
        self.calls: list[tuple] = []
        self.contexts: list[tuple[str, str | None]] = []
        self.fail: Exception | None = None

    def context(self, scheduler_url: str, dashboard_url: str | None) -> "FakeCluster":
        self.contexts.append((scheduler_url, dashboard_url))
        return self

    def record(self, call: tuple) -> None:
        self.calls.append(call)
        if self.fail is not None:
            raise self.fail

    def local_path(self, uri: str) -> str:
        if not uri.startswith("s3://"):
            return uri
        path = self.disk / uri.removeprefix("s3://")
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path)


@pytest.fixture
def cluster(tmp_path, monkeypatch) -> FakeCluster:
    fake = FakeCluster(tmp_path / "cluster")

    def remote(plan, context=None, **_):
        assert context is fake, "a plan went to a context the backend did not make"
        return FakeRemote(plan, fake)

    monkeypatch.setattr(pl.LazyFrame, "remote", remote)
    return fake


@pytest.fixture
def backend(cluster) -> PolarsCloudBackend:
    return PolarsCloudBackend(
        BACKEND_ID, {"scheduler_url": SCHEDULER, "dashboard_url": DASHBOARD}, connect=cluster.context
    )


@pytest.fixture
def scheduler_port_open(monkeypatch):
    """The TCP probe of the scheduler's port succeeds without a socket."""
    monkeypatch.setattr(backend_module.socket, "create_connection", lambda *a, **k: SimpleNamespace(close=lambda: None))


@pytest.fixture
def store(monkeypatch) -> list[tuple[ExchangeStore, str]]:
    """``STORE`` as the configured exchange store, its connection's options with a key; the deletions, recorded."""
    deleted: list[tuple[ExchangeStore, str]] = []
    monkeypatch.setattr(exchange, "exchange_store", lambda: STORE)
    monkeypatch.setattr(
        backend_module,
        "credential_ref_kwargs",
        lambda connection_id, user_id: {
            "credential_provider": object(),
            "storage_options": {**PLAIN, "aws_access_key_id": "AKIA-NOT-FOR-PLANS", "aws_secret_access_key": "x"},
        },
    )
    monkeypatch.setattr(exchange, "remove_prefix", lambda store, prefix: deleted.append((store, prefix)))
    return deleted


@pytest.fixture
def no_store(monkeypatch):
    monkeypatch.setattr(exchange, "exchange_store", lambda: None)


def closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def rows_of(sample: bytes) -> pl.DataFrame:
    return pl.read_ipc_stream(io.BytesIO(sample))


def basic_chain(graph: FlowGraph) -> FlowGraph:
    """Manual input 1 (``CUSTOMERS``) → basic filter 2, ``amount > 10`` → group_by 3, ``total`` by region; no formula,
    so nothing needs the formula library the cluster has in another version."""
    add_manual_input(graph, 1, CUSTOMERS.to_dicts())
    add_promise(graph, 2, "filter")
    basic = transform_schema.BasicFilter(field="amount", operator="greater_than", value="10")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(mode="basic", basic_filter=basic),
        )
    )
    connect(graph, 1, 2)
    add_promise(graph, 3, "group_by")
    aggregations = [transform_schema.AggColl("region", "groupby"), transform_schema.AggColl("amount", "sum", "total")]
    graph.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            groupby_input=transform_schema.GroupByInput(agg_cols=aggregations),
        )
    )
    connect(graph, 2, 3)
    return graph


def test_the_backend_declares_the_image_and_no_operation_without_connecting(backend, cluster):
    declared = backend.capabilities()

    assert (declared.polars_version, declared.polars_runtime, declared.python_version, declared.platform) == (
        "1.43.2",
        "64",
        "3.11.12",
        "linux-x86_64",
    )
    assert declared.plugins == {"polars_ds": "0.13.0", "polars_grouper": "0.6.1", "polars_expr_transformer": "0.6.4"}
    assert declared.operations == frozenset()
    assert (declared.read_schemes, declared.write_schemes) == (frozenset({"s3"}), frozenset({"s3"}))
    assert declared.credential_resolution == "ambient"
    assert cluster.contexts == []
    with pytest.raises(OperationUnsupported, match="plain Polars plans only"):
        backend.run_op(OperationKind.fuzzy_join, {})


def test_a_deferred_submit_holds_the_plan_its_schema_and_sample_from_one_show(backend, cluster, scheduler_port_open):
    result = backend.submit(ROWS, [], DEFERRED, "node-hash", flow_id=42, node_id=3)

    assert cluster.contexts == [(SCHEDULER, DASHBOARD)]
    assert cluster.calls == [("show", 2)]
    assert result.ref.state == "deferred"
    assert result.ref.row_count is None
    assert [(c.column_name, c.data_type) for c in result.ref.schema] == [("region", "String"), ("amount", "Float64")]
    assert rows_of(result.sample).equals(ROWS.head(2).collect())
    assert backend.plan_for(result.ref) is ROWS
    assert backend.exists(result.ref)
    assert backend.count(result.ref) == 3
    assert rows_of(backend.sample(result.ref, 1)).equals(ROWS.head(1).collect())
    assert cluster.calls[1:] == [("show", 1), ("show", 1)]
    assert len(cluster.contexts) == 1


def test_a_submit_without_a_preview_reads_its_schema_from_show_0(backend, cluster, scheduler_port_open):
    hints = SubmitHints(must_materialise=False, wide=False, fanout=1, preview_rows=None)

    result = backend.submit(ROWS, [], hints, "node-hash", flow_id=42, node_id=3)

    assert cluster.calls == [("show", 0)]
    assert result.sample is None
    assert [c.column_name for c in result.ref.schema] == ["region", "amount"]


def test_a_cached_result_is_sunk_to_the_exchange_store_with_plain_options_and_held_as_a_scan(
    backend, cluster, scheduler_port_open, store
):
    result = backend.submit(ROWS, [], CACHED, "node-hash", flow_id=42, node_id=3)

    uri = "s3://exchange/flowfile/42/results/node-hash.parquet"
    assert cluster.calls == [("sink", uri, PLAIN), ("show", 0)]
    assert (result.ref.state, result.ref.row_count, result.sample) == ("materialised", 3, None)
    held = backend.plan_for(result.ref)
    assert held is not ROWS
    assert uri.encode() in held.serialize()
    assert b"AKIA-NOT-FOR-PLANS" not in held.serialize()
    assert b"CredentialProvider" not in held.serialize()
    assert backend.count(result.ref) == 3
    assert store == []


def test_release_deletes_a_cached_object_through_the_exchange_store(backend, cluster, scheduler_port_open, store):
    result = backend.submit(ROWS, [], CACHED, "node-hash", flow_id=42, node_id=3)

    backend.release(result.ref)

    assert store == [(STORE, "s3://exchange/flowfile/42/results/node-hash.parquet")]
    assert not backend.exists(result.ref)
    with pytest.raises(ResultMissing):
        backend.plan_for(result.ref)


def test_a_resubmit_under_the_same_id_deletes_the_object_it_replaces(backend, cluster, scheduler_port_open, store):
    backend.submit(ROWS, [], CACHED, "node-hash", flow_id=42, node_id=3)

    backend.submit(ROWS, [], DEFERRED, "node-hash", flow_id=42, node_id=3)

    assert store == [(STORE, "s3://exchange/flowfile/42/results/node-hash.parquet")]


def test_a_failed_deletion_is_left_to_the_lifecycle_rule(backend, cluster, scheduler_port_open, store, monkeypatch):
    def refuse(store, prefix):
        raise OSError("Could not delete 1 object(s)")

    result = backend.submit(ROWS, [], CACHED, "node-hash", flow_id=42, node_id=3)
    monkeypatch.setattr(exchange, "remove_prefix", refuse)

    backend.release(result.ref)

    assert not backend.exists(result.ref)


def test_a_cached_result_without_an_exchange_store_fails_before_any_call(backend, cluster, no_store):
    with pytest.raises(TaskFailed, match="none is configured"):
        backend.submit(ROWS, [], CACHED, "node-hash", flow_id=42, node_id=3)

    assert (cluster.contexts, cluster.calls) == ([], [])


def test_export_sinks_the_held_plan_with_the_credentials_plain_options(
    backend, cluster, scheduler_port_open, store, tmp_path
):
    result = backend.submit(ROWS, [], DEFERRED, "node-hash", flow_id=42, node_id=3)
    target = str(tmp_path / "out.parquet")

    written = backend.export(result.ref, target, STORE.credential)

    assert written == target
    assert cluster.calls[-1] == ("sink", target, PLAIN)
    assert pl.read_parquet(target).equals(ROWS.collect())


def test_an_import_is_held_deferred_and_read_by_a_plan_sent_to_the_cluster(
    backend, cluster, scheduler_port_open, tmp_path
):
    path = tmp_path / "crossed.parquet"
    ROWS.collect().write_parquet(path)
    schema = _flowfile_schema(ROWS.collect_schema())

    ref = backend.import_uri(str(path), schema, 3, None)

    assert (ref.backend_id, ref.state, ref.row_count) == (BACKEND_ID, "deferred", 3)
    assert ref.result_id.startswith("import_")
    assert cluster.calls == []
    plan = backend.plan_for(ref).filter(pl.col("amount") > 10)
    result = backend.submit(plan, [ref], DEFERRED, "downstream", flow_id=42, node_id=4)
    assert rows_of(result.sample)["amount"].to_list() == [12.5, 30.0]
    with pytest.raises(OperationUnsupported):
        backend.import_uri(str(path), None, None, None, kind="blob")
    with pytest.raises(ValueError, match="needs its schema"):
        backend.import_uri(str(path), None, 3, None)


def test_another_backends_reference_is_refused(backend, cluster, scheduler_port_open):
    result = backend.submit(ROWS, [], DEFERRED, "node-hash", flow_id=42, node_id=3)
    foreign = type(result.ref)(**{**result.ref.__dict__, "backend_id": "worker"})

    with pytest.raises(ForeignRef):
        backend.sample(foreign, 1)
    with pytest.raises(ForeignRef):
        backend.submit(ROWS, [foreign], DEFERRED, "other", flow_id=42, node_id=4)


def test_a_failed_query_is_task_failed_with_the_clusters_message(backend, cluster, scheduler_port_open):
    cluster.fail = RuntimeError("InvalidOperationError: conversion from `str` to `i64` failed in column 'name'")

    with pytest.raises(TaskFailed, match="conversion from `str` to `i64` failed"):
        backend.submit(ROWS, [], DEFERRED, "node-hash", flow_id=42, node_id=3)


def test_a_transport_error_is_unavailable_and_the_next_call_makes_the_context_again(
    backend, cluster, scheduler_port_open
):
    cluster.fail = RuntimeError("status: Unavailable, message: tcp connect error: Connection refused")

    with pytest.raises(ComputeUnavailable, match="Could not reach the Polars cluster at http://127.0.0.1:5051"):
        backend.submit(ROWS, [], DEFERRED, "node-hash", flow_id=42, node_id=3)
    cluster.fail = None
    backend.submit(ROWS, [], DEFERRED, "node-hash", flow_id=42, node_id=3)

    assert len(cluster.contexts) == 2


def test_a_closed_scheduler_port_is_unavailable_at_once_and_no_context_is_made(monkeypatch):
    made: list = []
    monkeypatch.setattr(polars_cloud, "ClusterContext", lambda *a, **k: made.append((a, k)))
    url = f"http://127.0.0.1:{closed_port()}"
    unreachable = PolarsCloudBackend(BACKEND_ID, {"scheduler_url": url})

    started = time.monotonic()
    with pytest.raises(ComputeUnavailable) as raised:
        unreachable.submit(ROWS, [], DEFERRED, "node-hash", flow_id=42, node_id=3)

    assert time.monotonic() - started < backend_module.PROBE_TIMEOUT
    assert made == []
    assert raised.value.backend_id == BACKEND_ID
    assert str(raised.value).startswith(f"Could not reach the Polars cluster at {url}")
    assert str(raised.value).endswith(f'Is backend "{BACKEND_ID}" running? Start it, or check the Backends page.')


def test_the_handshake_compares_the_image_and_runs_its_probes_on_the_cluster(backend, cluster, scheduler_port_open):
    core = local_capabilities()
    try:
        verdict = handshake.verify(backend)
    finally:
        handshake.reset_handshake()

    assert verdict.ok is True, verdict.error
    assert verdict.differences == handshake.capability_differences(core, backend.capabilities())
    lacking = {name for name, version in core.plugins.items() if backend.capabilities().plugins.get(name) != version}
    assert set(verdict.absent) == lacking
    assert verdict.offered.operations == frozenset()
    probes = [call for call in cluster.calls if call == ("show", 1)]
    assert len(probes) == len(cluster.calls) >= 1
    assert backend._results == {}


def test_the_handshake_of_an_unreachable_cluster_refuses_it_with_the_error(monkeypatch):
    unreachable = PolarsCloudBackend(BACKEND_ID, {"scheduler_url": f"http://127.0.0.1:{closed_port()}"})
    try:
        with pytest.raises(ComputeUnavailable):
            handshake.verify(unreachable)
        kept = handshake.last_verdict(BACKEND_ID)
    finally:
        handshake.reset_handshake()

    assert kept.ok is False
    assert kept.error.startswith("Could not reach the Polars cluster at http://127.0.0.1:")


def test_placement_refuses_what_the_cluster_cannot_run(backend, tmp_path):
    graph = fuzzy_flow(8701)
    csv = tmp_path / "local.csv"
    pl.DataFrame({"n": [1, 2]}).write_csv(csv)
    add_promise(graph, 4, "read")
    received = input_schema.ReceivedTable(path=str(csv), file_type="csv", table_settings={"encoding": "utf-8"})
    graph.add_read(input_schema.NodeRead(flow_id=graph.flow_id, node_id=4, received_file=received))
    add_cloud_reader(graph, 5, None)

    placement = place(graph, {BACKEND_ID: backend.capabilities()}, SingleBackendPolicy(BACKEND_ID))

    reasons = {refusal.node_id: refusal.reason for refusal in placement.refusals}
    assert set(reasons) == {3, 4}
    assert "it runs the fuzzy_join operation, which this backend does not offer" in reasons[3]
    assert reasons[4] == "it reads storage this backend does not read: file"
    assert set(placement.assignment) == {1, 2, 3, 4, 5}


def test_a_flow_runs_on_the_cluster_and_hands_back_its_rows(cluster, scheduler_port_open, monkeypatch):
    monkeypatch.delenv("FLOWFILE_EXCHANGE_URI", raising=False)
    backend = PolarsCloudBackend(BACKEND_ID, {"scheduler_url": SCHEDULER}, connect=cluster.context)
    registry.register_backend(BACKEND_ID, lambda: backend)
    graph = basic_chain(create_test_graph(flow_id=8702, execution_location=BACKEND_ID))
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        ref = graph.get_node(3).results.result_ref
        assert (ref.backend_id, ref.state) == (BACKEND_ID, "deferred")
        assert rows_of(backend.sample(ref, 10)).sort("region").equals(EXPECTED)
        assert cluster.contexts == [(SCHEDULER, None)]
    finally:
        graph.close_flow()
        registry.reset()
        handshake.reset_handshake()


def test_a_polars_cloud_row_builds_the_backend_from_its_settings_without_connecting(monkeypatch):
    made: list = []
    monkeypatch.setattr(polars_cloud, "ClusterContext", lambda *a, **k: made.append((a, k)))
    settings = json.dumps({"scheduler_url": SCHEDULER, "dashboard_url": DASHBOARD})
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=BACKEND_ID, kind="polars-cloud", url=SCHEDULER, settings=settings))
        db.add(ConfiguredBackend(id="polars-cloud:bare", kind="polars-cloud", url=SCHEDULER))
        db.commit()
    try:
        made_backend = registry.backend(BACKEND_ID)

        assert isinstance(made_backend, PolarsCloudBackend)
        assert (made_backend.id, made_backend.url) == (BACKEND_ID, SCHEDULER)
        assert made_backend._settings == {"scheduler_url": SCHEDULER, "dashboard_url": DASHBOARD}
        assert made_backend.capabilities().credential_resolution == "ambient"
        assert registry.backend(BACKEND_ID) is made_backend
        assert made == []
        with pytest.raises(LookupError, match="name no scheduler_url"):
            registry.backend("polars-cloud:bare")
    finally:
        with get_db_context() as db:
            for backend_id in (BACKEND_ID, "polars-cloud:bare"):
                registry.forget(backend_id)
                db.delete(db.get(ConfiguredBackend, backend_id))
            db.commit()


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:5051",
        "http://localhost:5051",
        "http://10.0.0.7:5051",
        "http://172.20.1.2:5051",
        "https://192.168.1.20:5051",
        "http://[::1]:5051",
        "https://[fd00::7]:5051",
    ],
)
def test_a_loopback_or_private_address_is_accepted(url):
    assert address_refusal(url) is None


@pytest.mark.parametrize("url", ["http://8.8.8.8:5051", "https://8.8.8.8:5051", "http://172.32.0.1:5051"])
def test_a_public_address_is_refused_tls_or_not(url):
    refusal = address_refusal(url)

    assert refusal is not None
    assert "is neither loopback nor in a private range" in refusal
    assert "no client authentication" in refusal


def test_a_cloud_read_built_for_the_cluster_carries_no_python_credential_provider(backend, monkeypatch):
    """A provider pickled under core's Python would not load on the cluster's; the cluster finds its own identity."""
    chosen: list = []
    monkeypatch.setattr(
        FlowDataEngine,
        "_read_parquet_from_cloud",
        classmethod(lambda cls, path, options, provider, *a, **k: chosen.append(provider)),
    )
    connection = FullCloudStorageConnection(
        storage_type="s3",
        auth_method="env_vars",
        connection_name="ambient",
        aws_region="nbg1",
        endpoint_url=PLAIN["endpoint_url"],
    )
    settings = CloudStorageReadSettingsInternal(
        read_settings=CloudStorageReadSettings(resource_path="s3://source/data.parquet", file_format="parquet"),
        connection=connection,
    )

    FlowDataEngine.from_cloud_storage_obj(settings, user_id=1, backend=backend)
    FlowDataEngine.from_cloud_storage_obj(settings, user_id=1, backend=None)

    assert chosen == [NATIVE_CREDENTIALS, None]
    kwargs = CloudStorageReader.get_scan_kwargs(PLAIN, NATIVE_CREDENTIALS)
    assert kwargs == {"credential_provider": None, "storage_options": PLAIN}
    assert b"CredentialProvider" not in pl.scan_parquet("s3://source/data.parquet", **kwargs).serialize()
    assert b"CredentialProvider" in pl.scan_parquet("s3://source/data.parquet", storage_options=PLAIN).serialize()
