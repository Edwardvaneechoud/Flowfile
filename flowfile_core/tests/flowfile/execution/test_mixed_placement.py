"""The nodes of one run on different backends, their data crossing between them through the exchange store.

With an exchange store configured (``FLOWFILE_EXCHANGE_URI``), a run places each node on the backend its own
``execution_location`` names, ``"auto"`` on the run's, and every output a node reads from another backend crosses
through the run's prefix in the store: exported there and imported on the node's backend, where the node that reads it
is materialised with its other outputs, so no result refers to the prefix once the run removes it. A node fetched alone
is placed with the nodes it reads from and crosses as a run's node does, through a prefix of the fetch's own; an input
it reads that has not run is composed into its plan. A formula gate's probe reads its source on the backend that holds
the source's result, and the missing-file retry reruns an input on the backend that holds its result for its current
setup, the node then crossing it again, and any other input where the node runs. A CSV read whose type inference is
widened holds its result under its hash, so it is served from its cache, priced and reread where it is held like any
other result. Without a store a node's own setting is not read. The protocol flag comes from the environment, as for
the exchange-store, two-backend chain and run-flow tests whose fixtures and frames these reuse; the cases over a
declaring backend set it on themselves, but for the formula gate's. A node releases every result it minted together:
closing the graph leaves nothing held on the worker that a run's node submitted, operated or imported there, and a node
that fails after storing part of its outputs holds nothing.
"""

import re
import uuid

import polars as pl
import pytest
import requests

from flowfile_core import events
from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.configs import settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import get_cloud_connection
from flowfile_core.flowfile.execution import exchange, registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ComputeUnavailable, PlanRejected, TaskFailed
from flowfile_core.flowfile.execution.exchange import ExchangeRun
from flowfile_core.flowfile.execution.placement import MixedPlacementPolicy, Placement, SingleBackendPolicy, place
from flowfile_core.flowfile.execution.protocol import CredentialRef, OpResult, ResultRef
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_graph import execution as graph_execution
from flowfile_core.flowfile.flow_node.executor import hints_for
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.util.execution_orderer import compute_execution_plan
from flowfile_core.schemas import input_schema, transform_schema
from shared.compute_protocol import PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER
from tests import test_run_flow_node
from tests.flowfile.conftest import create_test_graph
from tests.flowfile.execution import test_exchange_store, test_placement, test_unreadable_location
from tests.flowfile.execution.test_exchange_store import _BUCKET, _key, _keys
from tests.flowfile.execution.test_placement import (
    AUTO_REMEDY,
    BACKEND_ID,
    OWN_REMEDY,
    DeclaringBackend,
    add_manual_input,
    add_promise,
    connect,
    everything,
    make_graph,
)
from tests.flowfile.execution.test_two_backend_chain import A_ROWS, CUSTOMERS, EXPECTED
from tests.flowfile.test_yaml_io import find_parent_directory
from tests.test_catalog_cloud_virtual import requires_minio

# The fixtures these tests share with the placement, exchange-store, run-flow and unreadable-location tests, bound here.
declare = test_placement.declare
connection_name = test_exchange_store.connection_name
root = test_exchange_store.root
two_port_subflow = test_run_flow_node.two_port_subflow
split_node = test_unreadable_location.split_node

LOCAL, WORKER = registry.LOCAL_BACKEND_ID, registry.WORKER_BACKEND_ID
FAIL_ROWS = CUSTOMERS.filter(pl.col("amount") <= 10)


@pytest.fixture(autouse=True)
def _unset(monkeypatch):
    monkeypatch.delenv("FLOWFILE_EXCHANGE_URI", raising=False)
    monkeypatch.delenv("FLOWFILE_EXCHANGE_CONNECTION", raising=False)


@pytest.fixture
def protocol_on(monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)


@pytest.fixture
def worker() -> WorkerBackend:
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")
    return registry.backend(WORKER)


@pytest.fixture
def no_s3_client(monkeypatch) -> list:
    """The S3 clients the run's removal makes, recorded instead of made."""
    made: list = []
    monkeypatch.setattr(exchange, "build_s3_client", lambda *args, **kwargs: made.append(args))
    return made


def chain(graph: FlowGraph, filter_on: str, user_id: int | None = None) -> FlowGraph:
    """Manual input 1 (``CUSTOMERS``) → filter 2, ``amount > 10``, on ``filter_on`` → group_by 3, ``total`` by region.

    Nodes 1 and 3 keep ``"auto"``; every node is stamped ``user_id``.
    """
    add_manual_input(graph, 1, CUSTOMERS.to_dicts())
    graph.get_node(1).setting_input.user_id = user_id
    add_promise(graph, 2, "filter")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=user_id,
            execution_location=filter_on,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[amount] > 10"),
        )
    )
    connect(graph, 1, 2)
    add_promise(graph, 3, "group_by")
    graph.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            user_id=user_id,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl("region", "groupby"),
                    transform_schema.AggColl("amount", "sum", "total"),
                ]
            ),
        )
    )
    connect(graph, 2, 3)
    return graph


def split(graph: FlowGraph, split_on: str, user_id: int = 1) -> FlowGraph:
    """Manual input 1 (``CUSTOMERS``) → filter 2 on ``split_on`` in split mode, ``amount > 10``.

    Node 1 keeps ``"auto"``; both nodes are stamped ``user_id``.
    """
    add_manual_input(graph, 1, CUSTOMERS.to_dicts())
    graph.get_node(1).setting_input.user_id = user_id
    add_promise(graph, 2, "filter")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=user_id,
            split_mode=True,
            execution_location=split_on,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[amount] > 10"),
        )
    )
    connect(graph, 1, 2)
    return graph


def listing(root: str, seen: dict):
    """A ``flow_run_finished`` handler recording in ``seen`` the run's prefix under ``root`` and the keys under it."""

    def list_the_prefix(graph, **_):
        try:
            prefix = f"s3://{_BUCKET}/{root}/{graph.flow_id}/{graph.exchange_run.run_id}/"
            seen.update(prefix=prefix, listed=_keys(prefix))
        except Exception as e:
            seen["error"] = e

    return list_the_prefix


def sorted_rows(backend, ref, n: int = 10) -> pl.DataFrame:
    """The first ``n`` rows ``backend`` holds for ``ref``, in ``CUSTOMERS``'s columns, sorted by ``id``."""
    return pl.read_ipc_stream(backend.sample(ref, n)).select(CUSTOMERS.columns).sort("id")


def record_refs(monkeypatch, *methods: str) -> list[tuple[str, WorkerBackend, ResultRef]]:
    """Every ``(method, backend, ref)`` the ``WorkerBackend`` methods named (``submit``, ``run_op``, ``import_uri``)
    answer from here on."""
    answered: list[tuple[str, WorkerBackend, ResultRef]] = []
    for method in methods:
        real = getattr(WorkerBackend, method)

        def recording(self, *args, _method=method, _real=real, **kwargs):
            answer = _real(self, *args, **kwargs)
            if isinstance(answer, ResultRef):
                refs = [answer]
            elif isinstance(answer, OpResult):
                refs = list(answer.refs.values())
            else:
                refs = [answer.ref]
            answered.extend((_method, self, ref) for ref in refs)
            return answer

        monkeypatch.setattr(WorkerBackend, method, recording)
    return answered


def v1_head(worker: WorkerBackend, result_id: str) -> int:
    """The status the worker answers ``HEAD /v1/results/{result_id}`` with, asked as core asks."""
    headers = {"X-Flowfile-Internal": get_internal_token(), PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)}
    return requests.head(f"{worker.url}/v1/results/{result_id}", headers=headers, timeout=10).status_code


def still_held(answered: list[tuple[str, WorkerBackend, ResultRef]]) -> list[str]:
    """The ids among ``answered`` the backend that answered them still holds: by ``exists`` and, with the protocol
    flag on, by the worker's own answer to ``HEAD``, which is 404 for a result it no longer holds."""
    held = []
    for _, backend, ref in answered:
        if backend.exists(ref) or (settings.FLOWFILE_COMPUTE_PROTOCOL and v1_head(backend, ref.result_id) != 404):
            held.append(ref.result_id)
    return held


def _late_conflict_csv(path: str, n_rows: int = 120_000, conflict_at: int = 60_000) -> None:
    """A CSV whose ``val`` parses as integers for its first ``conflict_at`` rows and then turns to text, past the first
    batch that the read's one-row probe in core takes, so at a 1 000-row inference window only the full read fails."""
    with open(path, "w") as fh:
        fh.write("id,val\n")
        for i in range(n_rows):
            fh.write(f"{i},{'not_a_number' if i == conflict_at else i}\n")


def read_csv_on_worker(graph: FlowGraph, path: str) -> FlowNode:
    """Read 1 of the CSV at ``path`` on the worker, caching its result, its types inferred from the first 1 000 rows."""
    add_promise(graph, 1, "read")
    graph.add_read(
        input_schema.NodeRead(
            flow_id=graph.flow_id,
            node_id=1,
            user_id=1,
            execution_location=WORKER,
            received_file=input_schema.ReceivedTable(
                file_type="csv",
                name="late.csv",
                path=path,
                table_settings=input_schema.InputCsvTable(infer_schema_length=1000),
            ),
        )
    )
    source = graph.get_node(1)
    source.node_settings.cache_results = True
    return source


def test_the_mixed_policy_places_each_node_where_its_setting_says_and_lists_the_crossings():
    graph = chain(make_graph(8501, "local"), filter_on=WORKER)
    backends = {LOCAL: everything(), WORKER: everything()}

    assert place(graph, backends, MixedPlacementPolicy(LOCAL)) == Placement(
        assignment={1: LOCAL, 2: WORKER, 3: LOCAL}, boundaries=[(1, 2), (2, 3)], refusals=[]
    )
    assert place(graph, backends, SingleBackendPolicy(LOCAL)) == Placement(
        assignment={1: LOCAL, 2: LOCAL, 3: LOCAL}, boundaries=[], refusals=[]
    )


def test_a_node_that_reads_an_input_from_another_backend_is_materialised(tmp_path):
    graph = chain(make_graph(8502, "local"), filter_on="auto")
    add_promise(graph, 4, "output")
    graph.add_output(
        input_schema.NodeOutput(
            flow_id=graph.flow_id,
            node_id=4,
            output_settings=input_schema.OutputSettings(
                name="out.csv",
                directory=str(tmp_path),
                file_type="csv",
                write_mode="overwrite",
                table_settings=input_schema.OutputCsvTable(),
            ),
        )
    )
    connect(graph, 2, 4)
    context = graph.execution_context
    filter_node, output_node = graph.get_node(2), graph.get_node(4)

    assert hints_for(filter_node, context, performance_mode=False).must_materialise is False
    filter_node._boundary_inputs = {(1, DEFAULT_OUTPUT_HANDLE): "the edge from node 1"}
    output_node._boundary_inputs = {(2, DEFAULT_OUTPUT_HANDLE): "the edge from node 2"}

    assert hints_for(filter_node, context, performance_mode=False).must_materialise is True
    output_hints = hints_for(output_node, context, performance_mode=False)
    assert (output_hints.must_materialise, output_hints.preview_rows) == (True, None)


def test_without_a_store_a_nodes_own_setting_is_not_read(protocol_on, declare, no_s3_client):
    backend = declare(everything())
    graph = chain(make_graph(8503), filter_on=LOCAL, user_id=1)

    run_info = graph.run_graph()

    assert run_info.success is True, [result.error for result in run_info.node_step_result]
    assert "submit 2" in backend.work
    assert backend.asked == 1
    assert graph.get_node(2).results.result_ref.backend_id == backend.id
    assert run_info.boundaries == []
    assert no_s3_client == []


@pytest.mark.parametrize(
    "connection, named",
    [(None, "FLOWFILE_EXCHANGE_CONNECTION"), (f"no connection {uuid.uuid4().hex[:6]}", "'no connection ")],
    ids=["no-connection", "unknown-connection"],
)
def test_a_store_that_cannot_be_used_refuses_the_importing_node_before_any_node_runs(
    protocol_on, declare, no_s3_client, monkeypatch, connection, named
):
    backend = declare(everything())
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
    if connection is not None:
        monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection)
    graph = chain(make_graph(8504), filter_on=LOCAL, user_id=1)

    run_info = graph.run_graph()

    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert errors.keys() == {2, 3}, errors
    assert errors[2].startswith(f'cannot run on backend "{LOCAL}": '), errors[2]
    assert re.search(re.escape(named), errors[2]), errors[2]
    assert errors[2].endswith(f". {OWN_REMEDY}"), errors[2]
    assert errors[3].startswith(f'cannot run on backend "{backend.id}": '), errors[3]
    assert errors[3].endswith(f". {AUTO_REMEDY}"), errors[3]
    assert all(result.compute_error == "Refusal" for result in run_info.node_step_result)
    assert (run_info.success, run_info.nodes_completed) == (False, 0)
    assert run_info.boundaries == [(1, 2), (2, 3)]
    assert run_info.model_dump(mode="json")["boundaries"] == [[1, 2], [2, 3]]
    assert backend.work == []
    assert no_s3_client == []


def test_the_store_is_the_operators_whatever_user_the_upstream_node_is_stamped(
    protocol_on, declare, no_s3_client, connection_name, monkeypatch
):
    """Node 1, stamped user 2, crosses to node 2 on core: the edge opens the store with the one user's connection."""
    declare(everything())
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection_name)
    graph = chain(make_graph(8509), filter_on=LOCAL, user_id=1)
    graph.get_node(1).setting_input.user_id = 2
    graph.get_node(3).setting_input.execution_location = LOCAL
    flow_starts = graph._flow_starts + graph.get_implicit_starter_nodes()
    plan = compute_execution_plan(nodes=graph.nodes, flow_starts=flow_starts)

    placement, edges = graph._place_run(plan, ExchangeRun(graph.flow_id, uuid.uuid4().hex))

    with get_db_context() as db:
        connection_id = get_cloud_connection(db, connection_name, 1).id
    assert placement.refusals == []
    assert [(edge.source.node_id, edge.handle, edge.target.node_id) for edge in edges] == [
        (1, DEFAULT_OUTPUT_HANDLE, 2)
    ]
    assert edges[0].credential == CredentialRef(connection_id=connection_id, user_id=1)
    assert no_s3_client == []


DOWN = "worker:down"


@pytest.mark.parametrize("flow_location", [BACKEND_ID, LOCAL])
def test_a_node_on_a_backend_that_cannot_be_asked_fails_with_its_error_and_runs_nowhere_else(
    protocol_on, declare, no_s3_client, connection_name, monkeypatch, flow_location
):
    """The chain's filter is on a backend whose handshake cannot reach it: it fails with that error, before anything is
    sent, and its group by is skipped, whether the flow runs on the declaring backend or on ``local``."""
    up = declare(everything())
    down = declare(ComputeUnavailable("Connection refused", backend_id=DOWN), DOWN)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection_name)
    graph = chain(make_graph(8530, execution_location=flow_location), filter_on=DOWN, user_id=1)
    try:
        run_info = graph.run_graph()

        rows = {result.node_id: result for result in run_info.node_step_result}
        assert rows[2].success is False and rows[2].compute_error == "ComputeUnavailable", rows[2].error
        assert rows[2].error.startswith("Connection refused"), rows[2].error
        assert 3 not in rows or rows[3].success is not True
        assert graph.get_node(3).results.resulting_data is None
        assert run_info.success is False
        assert down.work == []
        assert "submit 2" not in up.work
        assert graph.get_node(2).results.result_ref is None
    finally:
        graph.close_flow()


def test_a_node_fetched_on_a_backend_that_cannot_be_asked_fails_with_its_error(
    protocol_on, declare, no_s3_client, connection_name, monkeypatch
):
    up = declare(everything())
    down = declare(ComputeUnavailable("Connection refused", backend_id=DOWN), DOWN)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection_name)
    graph = chain(make_graph(8531), filter_on=DOWN, user_id=1)
    try:
        run_info = graph.trigger_fetch_node(2)

        [row] = run_info.node_step_result
        assert (row.node_id, row.success, row.compute_error) == (2, False, "ComputeUnavailable"), row.error
        assert down.work == []
        assert "submit 2" not in up.work
    finally:
        graph.close_flow()


@requires_minio
def test_a_node_on_the_worker_takes_its_input_from_core_and_hands_its_rows_back_through_the_store(root, worker):
    local = registry.backend(LOCAL)
    graph = chain(create_test_graph(flow_id=8505, execution_location=LOCAL), filter_on=WORKER, user_id=1)
    seen: dict = {}
    list_the_prefix = listing(root, seen)
    events.subscribe("flow_run_finished", list_the_prefix)
    try:
        run_info = graph.run_graph()

        assert "error" not in seen, seen.get("error")
        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert run_info.nodes_completed == 3
        assert graph.exchange_run is None
        assert _keys(seen["prefix"]) == []
        ref_2, ref_3 = graph.get_node(2).results.result_ref, graph.get_node(3).results.result_ref
        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert seen["listed"] == []
            assert (ref_2.backend_id, ref_3.backend_id) == (LOCAL, LOCAL)
            assert run_info.boundaries == []
            return
        assert run_info.boundaries == [(1, 2), (2, 3)]
        assert seen["listed"] == [
            f"{_key(seen['prefix'])}1-{DEFAULT_OUTPUT_HANDLE}-2.parquet",
            f"{_key(seen['prefix'])}2-{DEFAULT_OUTPUT_HANDLE}-3.parquet",
        ]
        assert (ref_2.backend_id, ref_2.state) == (WORKER, "materialised")
        assert (ref_3.backend_id, ref_3.state) == (LOCAL, "materialised")
        assert pl.read_ipc_stream(local.sample(ref_3, 10)).sort("region").equals(EXPECTED)
        assert pl.read_ipc_stream(worker.sample(ref_2, 100)).sort("id").equals(A_ROWS)
    finally:
        events._handlers["flow_run_finished"].remove(list_the_prefix)
        graph.close_flow()


@requires_minio
def test_a_node_fetched_alone_runs_on_its_own_backend_and_reads_what_ran_elsewhere_through_the_store(
    root, worker, monkeypatch
):
    """Flow 8511, the chain with its filter on the worker, is run and then fetched at node 3 and at node 2; flow 8512,
    the same chain, is fetched at node 3 with nothing run, its filter composed into the group by's plan."""
    local = registry.backend(LOCAL)
    exports: list[tuple[str, str]] = []
    for backend in (local, worker):
        export = backend.export

        def record(ref, uri, credential, backend_id=backend.id, export=export):
            exports.append((backend_id, uri))
            return export(ref, uri, credential)

        monkeypatch.setattr(backend, "export", record)

    def fetch(graph: FlowGraph, node_id: int):
        exports.clear()
        run_info = graph.trigger_fetch_node(node_id)
        assert run_info is not None and run_info.run_type == "fetch_one"
        [row] = run_info.node_step_result
        assert (row.node_id, row.success) == (node_id, True), row.error
        return run_info, list(exports)

    graph = chain(create_test_graph(flow_id=8511, execution_location=LOCAL), filter_on=WORKER, user_id=1)
    fresh = chain(create_test_graph(flow_id=8512, execution_location=LOCAL), filter_on=WORKER, user_id=1)
    try:
        run_info = graph.run_graph()
        assert run_info.success is True, [result.error for result in run_info.node_step_result]

        fetched_3, exported_3 = fetch(graph, 3)
        ref_3 = graph.get_node(3).results.result_ref
        assert pl.read_ipc_stream(local.sample(ref_3, 10)).sort("region").equals(EXPECTED)
        fetched_2, exported_2 = fetch(graph, 2)
        ref_2 = graph.get_node(2).results.result_ref
        assert pl.read_ipc_stream(registry.backend(ref_2.backend_id).sample(ref_2, 100)).sort("id").equals(A_ROWS)
        assert graph.exchange_run is None
        fetched_fresh, exported_fresh = fetch(fresh, 3)
        ref_fresh = fresh.get_node(3).results.result_ref
        assert pl.read_ipc_stream(local.sample(ref_fresh, 10)).sort("region").equals(EXPECTED)
        assert exported_fresh == []
        assert ref_fresh.backend_id == LOCAL

        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert (fetched_3.boundaries, fetched_2.boundaries, fetched_fresh.boundaries) == ([], [], [])
            assert exported_3 == exported_2 == []
            assert (ref_2.backend_id, ref_3.backend_id) == (LOCAL, LOCAL)
            return
        prefix = rf"s3://{_BUCKET}/{re.escape(root)}/8511/[0-9a-f]{{32}}/"
        assert fetched_3.boundaries == [(2, 3)]
        [(exporter, uri)] = exported_3
        assert exporter == WORKER
        assert re.fullmatch(rf"{prefix}2-{DEFAULT_OUTPUT_HANDLE}-3\.parquet", uri), uri
        assert (ref_3.backend_id, ref_3.state) == (LOCAL, "materialised")
        assert fetched_2.boundaries == [(1, 2)]
        [(exporter, uri)] = exported_2
        assert exporter == LOCAL
        assert re.fullmatch(rf"{prefix}1-{DEFAULT_OUTPUT_HANDLE}-2\.parquet", uri), uri
        assert (ref_2.backend_id, ref_2.state) == (WORKER, "materialised")
        assert _keys(f"{root}/8511/") == []
        assert fetched_fresh.boundaries == [(2, 3)]
    finally:
        graph.close_flow()
        fresh.close_flow()


@pytest.mark.parametrize("cause", ["a-node-saved-without-a-location", "an-unreachable-backend"])
def test_a_fetch_whose_placement_raises_fails_the_nodes_row_and_runs_nothing(monkeypatch, no_s3_client, cause):
    """Flow 8513, the chain on core, is run and then fetched at node 3 while placing the fetch raises: node 3's
    settings without ``execution_location`` (a flow pickled before it, a store configured so the mixed policy reads
    it), or ``_place_run`` raising ``ComputeUnavailable``. With the flag off nothing is placed and the fetch runs."""
    local = registry.backend(LOCAL)
    exports: list[str] = []
    export = local.export

    def record(ref, uri, credential):
        exports.append(uri)
        return export(ref, uri, credential)

    monkeypatch.setattr(local, "export", record)
    graph = chain(make_graph(8513, LOCAL), filter_on=LOCAL, user_id=1)
    try:
        assert graph.run_graph().success is True
        held = graph.get_node(3).results.result_ref
        if cause == "a-node-saved-without-a-location":
            monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
            del graph.get_node(3).setting_input.__dict__["execution_location"]
            raised, compute_error = AttributeError, None
        else:
            place_run = FlowGraph._place_run

            def unreachable(self, execution_plan, exchange_run):
                if settings.FLOWFILE_COMPUTE_PROTOCOL:
                    raise ComputeUnavailable("Connection refused")
                return place_run(self, execution_plan, exchange_run)

            monkeypatch.setattr(FlowGraph, "_place_run", unreachable)
            raised, compute_error = ComputeUnavailable, "ComputeUnavailable"

        if settings.FLOWFILE_COMPUTE_PROTOCOL:
            with pytest.raises(raised) as error:
                graph.trigger_fetch_node(3)
        else:
            graph.trigger_fetch_node(3)

        run_info = graph.get_run_info()
        assert run_info.run_type == "fetch_one"
        [row] = run_info.node_step_result
        assert row.node_id == 3
        assert run_info.boundaries == []
        assert (exports, no_s3_client) == ([], [])
        assert graph.flow_settings.is_running is False
        assert graph.exchange_run is None
        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert (run_info.success, row.success, row.compute_error) == (True, True, None), row.error
            return
        assert (run_info.success, row.success) == (False, False)
        assert row.error == str(error.value)
        assert row.compute_error == compute_error
        assert graph.get_node(3).results.result_ref is held
    finally:
        graph.close_flow()


def test_a_flow_opened_from_a_legacy_pickle_runs_with_a_store_configured(monkeypatch, worker):
    """``read_csv.flowfile`` was pickled before nodes carried an ``execution_location``; a store configured, the mixed
    policy reads each node's own."""
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
    flow = open_flow(find_parent_directory("Flowfile") / "flowfile_core/tests/support_files/flows/read_csv.flowfile")
    try:
        run_info = flow.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert run_info.boundaries == []
        assert [node.setting_input.execution_location for node in flow.nodes] == ["auto"] * len(flow.nodes)
    finally:
        flow.close_flow()


@requires_minio
@pytest.mark.parametrize(
    "flow_id, split_on, other", [(8506, WORKER, LOCAL), (8507, LOCAL, WORKER)], ids=["from-the-worker", "from-core"]
)
def test_a_second_output_crosses_and_every_output_reads_its_own_backends_copy_after_the_run(
    root, worker, flow_id, split_on, other
):
    """Manual input 1 on ``other`` → split filter 2 on ``split_on``, whose fail side feeds filter 3 on ``split_on``
    and filter 4 on ``other``: node 2 imports its input, and its second output crosses to node 4."""
    graph = split(create_test_graph(flow_id=flow_id, execution_location=LOCAL), split_on)
    graph.get_node(1).setting_input.execution_location = other
    for node_id, location in ((3, split_on), (4, other)):
        add_promise(graph, node_id, "filter")
        graph.add_filter(
            input_schema.NodeFilter(
                flow_id=graph.flow_id,
                node_id=node_id,
                depending_on_id=2,
                user_id=1,
                execution_location=location,
                filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[amount] >= 0"),
            )
        )
        add_connection(
            graph, input_schema.NodeConnection.create_from_simple_input(2, node_id, output_handle="output-1")
        )
    seen: dict = {}
    list_the_prefix = listing(root, seen)
    events.subscribe("flow_run_finished", list_the_prefix)
    try:
        run_info = graph.run_graph()

        assert "error" not in seen, seen.get("error")
        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert run_info.nodes_completed == 4
        assert _keys(seen["prefix"]) == []
        ref_3, ref_4 = graph.get_node(3).results.result_ref, graph.get_node(4).results.result_ref
        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert seen["listed"] == []
            assert (ref_3.backend_id, ref_4.backend_id) == (LOCAL, LOCAL)
            return
        assert seen["listed"] == [
            f"{_key(seen['prefix'])}1-{DEFAULT_OUTPUT_HANDLE}-2.parquet",
            f"{_key(seen['prefix'])}2-output-1-4.parquet",
        ]
        assert (ref_3.backend_id, ref_4.backend_id, ref_4.state) == (split_on, other, "materialised")
        assert sorted_rows(registry.backend(split_on), ref_3).equals(FAIL_ROWS)
        assert sorted_rows(registry.backend(other), ref_4).equals(FAIL_ROWS)
        fail_side = graph.get_node(2)._stored_outputs["output-1"]
        assert (fail_side.backend_id, fail_side.state) == (split_on, "materialised")
        assert sorted_rows(registry.backend(split_on), fail_side).equals(FAIL_ROWS)
    finally:
        events._handlers["flow_run_finished"].remove(list_the_prefix)
        graph.close_flow()


@requires_minio
def test_a_node_reading_two_outputs_of_one_source_on_another_backend_takes_each_through_its_own_crossing(
    root, worker, two_port_subflow
):
    """Manual input 1 → split filter 2 on the worker, whose pass and fail sides feed the two slots of run flow 9."""
    local = registry.backend(LOCAL)
    graph = split(create_test_graph(flow_id=8508, execution_location=LOCAL), WORKER)
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=9,
            user_id=1,
            flow_reference=input_schema.SubflowReference(registration_id=two_port_subflow["registration_id"]),
            input_slots=["a", "b"],
            output_slots=["out_a", "out_b"],
        )
    )
    test_run_flow_node.connect(graph, 2, 9, "input-1", DEFAULT_OUTPUT_HANDLE)
    test_run_flow_node.connect(graph, 2, 9, "input-2", "output-1")
    seen: dict = {}
    list_the_prefix = listing(root, seen)
    events.subscribe("flow_run_finished", list_the_prefix)
    try:
        run_info = graph.run_graph()

        assert "error" not in seen, seen.get("error")
        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert run_info.nodes_completed == 3
        assert _keys(seen["prefix"]) == []
        node = graph.get_node(9)
        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert seen["listed"] == []
            assert node.results.result_ref.backend_id == LOCAL
            return
        assert seen["listed"] == [
            f"{_key(seen['prefix'])}1-{DEFAULT_OUTPUT_HANDLE}-2.parquet",
            f"{_key(seen['prefix'])}2-{DEFAULT_OUTPUT_HANDLE}-9.parquet",
            f"{_key(seen['prefix'])}2-output-1-9.parquet",
        ]
        out_a, out_b = node.results.result_ref, node._stored_outputs["output-1"]
        assert [(ref.backend_id, ref.state) for ref in (out_a, out_b)] == [(LOCAL, "materialised")] * 2
        assert sorted_rows(local, out_a).equals(A_ROWS)
        assert sorted_rows(local, out_b).equals(FAIL_ROWS)
    finally:
        events._handlers["flow_run_finished"].remove(list_the_prefix)
        graph.close_flow()


@requires_minio
def test_closing_the_graph_leaves_nothing_held_on_the_worker_that_its_nodes_minted_there(
    root, worker, split_node, monkeypatch
):
    """Manual input 1 on core → random split 2 on the worker, two groups → the two-output custom node 3 on the worker.

    With the flag on node 2 imports its input, so it stores ``output-1`` beside its result; with it off the nodes' own
    settings are not read and the flow runs on the worker as a whole, through the task routes.
    """
    answered = record_refs(monkeypatch, "submit", "run_op", "import_uri")
    graph = make_graph(8520)
    add_manual_input(graph, 1, CUSTOMERS.to_dicts())
    graph.get_node(1).setting_input.user_id = 1
    graph.get_node(1).setting_input.execution_location = LOCAL
    add_promise(graph, 2, "random_split")
    graph.add_random_split(
        input_schema.NodeRandomSplit(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=1,
            seed=7,
            execution_location=WORKER,
            splits=[
                input_schema.RandomSplitGroup(name="train", percentage=50.0),
                input_schema.RandomSplitGroup(name="test", percentage=50.0),
            ],
        )
    )
    connect(graph, 1, 2)
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=3, node_type=split_node.node_key, is_user_defined=True)
    )
    graph.add_user_defined_node(
        custom_node=split_node.node_class.from_settings({}),
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=graph.flow_id, node_id=3, settings={}, is_user_defined=True, user_id=1, execution_location=WORKER
        ),
    )
    connect(graph, 2, 3)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert run_info.nodes_completed == 3
        methods = {method for method, _, _ in answered}
        if settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert methods == {"submit", "run_op", "import_uri"}
            assert graph.get_node(2)._stored_outputs.keys() == {"output-1"}
        else:
            assert methods == {"submit", "run_op"}
        assert [worker.exists(graph.get_node(node_id).results.result_ref) for node_id in (2, 3)] == [True, True]
    finally:
        graph.close_flow()

    assert still_held(answered) == []


class RefusesTheThirdOutput(DeclaringBackend):
    """Core's own backend under the declaring id, refusing the plan a node submits for its output ``output-2``;
    ``answered`` holds every reference its ``submit`` answered but the handshake's probe jobs'."""

    def __init__(self, backend_id, declared):
        super().__init__(backend_id, declared)
        self.answered: list[ResultRef] = []

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        if result_id.endswith("_output-2"):
            raise PlanRejected("the backend refused output-2")
        result = super().submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)
        if not result_id.startswith("handshake-"):
            self.answered.append(result.ref)
        return result


@requires_minio
def test_a_second_output_that_fails_to_store_fails_the_node_and_releases_everything_it_held(protocol_on, declare, root):
    """Manual input 1 on core → random split 2 on the declaring backend, three groups: node 2 imports its input, so it
    stores ``output-1`` and then ``output-2`` beside its result, and the second of those is refused."""
    backend = RefusesTheThirdOutput(BACKEND_ID, everything())
    registry.register_backend(BACKEND_ID, lambda: backend)
    graph = make_graph(8510)
    add_manual_input(graph, 1, [{"id": i} for i in range(100)])
    graph.get_node(1).setting_input.user_id = 1
    graph.get_node(1).setting_input.execution_location = LOCAL
    add_promise(graph, 2, "random_split")
    graph.add_random_split(
        input_schema.NodeRandomSplit(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=1,
            seed=42,
            splits=[
                input_schema.RandomSplitGroup(name="a", percentage=50.0),
                input_schema.RandomSplitGroup(name="b", percentage=30.0),
                input_schema.RandomSplitGroup(name="c", percentage=20.0),
            ],
        )
    )
    connect(graph, 1, 2)
    seen: dict = {}
    list_the_prefix = listing(root, seen)
    events.subscribe("flow_run_finished", list_the_prefix)
    try:
        run_info = graph.run_graph()

        assert "error" not in seen, seen.get("error")
        errors = {result.node_id: result.error for result in run_info.node_step_result if not result.success}
        assert errors.keys() == {2}, errors
        assert "the backend refused output-2" in errors[2], errors[2]
        assert run_info.success is False
        node = graph.get_node(2)
        assert (node.results.result_ref, node._stored_outputs, node._operation_refs) == (None, {}, {})
        assert node._submit_in_flight is None
        assert node._boundary_inputs == {}
        assert seen["listed"] == [f"{_key(seen['prefix'])}1-{DEFAULT_OUTPUT_HANDLE}-2.parquet"]
        assert _keys(seen["prefix"]) == []
        assert graph.exchange_run is None
        result, stored = backend.answered
        assert (result.result_id, stored.result_id) == (node.hash, f"{node.hash}_output-1")
        assert (backend.exists(result), backend.exists(stored)) == (False, False)
    finally:
        events._handlers["flow_run_finished"].remove(list_the_prefix)
        graph.close_flow()


@requires_minio
def test_a_node_that_fails_to_store_its_second_output_on_the_worker_leaves_nothing_held_there(
    protocol_on, root, worker, monkeypatch
):
    """Manual input 1 on core → split filter 2 on the worker, which refuses the plan node 2 stores ``output-1`` from."""
    answered: list[ResultRef] = []
    real_submit = WorkerBackend.submit

    def refusing_output_1(self, plan, inputs, hints, result_id, **kwargs):
        if result_id.endswith("_output-1"):
            raise PlanRejected("the worker refused output-1")
        result = real_submit(self, plan, inputs, hints, result_id, **kwargs)
        answered.append(result.ref)
        return result

    monkeypatch.setattr(WorkerBackend, "submit", refusing_output_1)
    graph = split(make_graph(8521, LOCAL), split_on=WORKER)
    try:
        run_info = graph.run_graph()

        errors = {result.node_id: result.error for result in run_info.node_step_result if not result.success}
        assert errors.keys() == {2}, errors
        assert "the worker refused output-1" in errors[2], errors[2]
        node = graph.get_node(2)
        assert (node.results.result_ref, node._stored_outputs, node._operation_refs) == (None, {}, {})
        [result] = [ref for ref in answered if ref.result_id == node.hash]
        assert result.backend_id == WORKER
        assert worker.exists(result) is False
        assert v1_head(worker, result.result_id) == 404
    finally:
        graph.close_flow()


def test_a_run_flow_child_leaves_its_operation_outputs_held_for_the_parent_that_reads_them(
    worker, tmp_path, monkeypatch
):
    """Manual input 1 (``CUSTOMERS``) → run flow 2 over a child, flow input → random split → flow output → sort 3.

    The child runs on the parent's backend in Performance mode, so its split node holds a deferred result over the
    split's stored outputs; once the child's run ends its deferred results go and the split's outputs stay, which the
    parent's sort then reads. With the flag off the split answers deferred slices of the worker's shuffle task, which
    the node does not hold.
    """
    child = test_run_flow_node.make_graph(8523, "split_child")
    add_promise(child, 1, "flow_input")
    child.add_flow_input(input_schema.NodeFlowInput(flow_id=child.flow_id, node_id=1, input_name="rows"))
    add_promise(child, 2, "random_split")
    child.add_random_split(
        input_schema.NodeRandomSplit(
            flow_id=child.flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=1,
            seed=7,
            splits=[
                input_schema.RandomSplitGroup(name="train", percentage=50.0),
                input_schema.RandomSplitGroup(name="test", percentage=50.0),
            ],
        )
    )
    connect(child, 1, 2)
    add_promise(child, 3, "flow_output")
    child.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=child.flow_id, node_id=3, output_name="result", depending_on_id=2)
    )
    connect(child, 2, 3)
    path = tmp_path / "split_child.yaml"
    child.save_flow(str(path))
    graph = make_graph(8522)
    add_manual_input(graph, 1, CUSTOMERS.to_dicts())
    add_promise(graph, 2, "run_flow")
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=2,
            user_id=1,
            flow_reference=input_schema.SubflowReference(
                registration_id=test_run_flow_node.register_flow_file(path, "split_child")
            ),
            input_slots=["rows"],
            output_slots=["result"],
            append_run_metadata=False,
        )
    )
    test_run_flow_node.connect(graph, 1, 2, "input-1")
    add_promise(graph, 3, "sort")
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            sort_input=[transform_schema.SortByInput(column="id", how="asc")],
        )
    )
    connect(graph, 2, 3)
    answered = record_refs(monkeypatch, "submit", "run_op")
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        operated = [ref for method, _, ref in answered if method == "run_op"]
        rows = sorted_rows(worker, graph.get_node(3).results.result_ref)
        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert [ref.state for ref in operated] == ["deferred"] * 2
            assert len(rows) == operated[0].row_count
            return
        parents = {graph.get_node(node_id).hash for node_id in (1, 2, 3)}
        deferred = [ref for method, _, ref in answered if method == "submit" and ref.result_id not in parents]
        assert deferred and all(ref.state == "deferred" for ref in deferred)
        assert not any(worker.exists(ref) for ref in deferred)
        assert [ref.state for ref in operated] == ["materialised"] * 2
        assert [v1_head(worker, ref.result_id) for ref in operated] == [200, 200]
        assert rows.equals(sorted_rows(worker, operated[0]))
    finally:
        graph.close_flow()
        for _, backend, ref in answered:
            if ref.state == "materialised":
                backend.release(ref)


def test_a_formula_gate_probes_its_source_on_the_backend_holding_its_result(declare, no_s3_client, monkeypatch):
    """Flow 8514 on core: manual input 1 on the declaring backend, caching its result, → formula gate 2 on the same
    backend. Nothing crosses, and the gate's probe reads node 1's result where it is held, not on the run's backend."""
    declare(everything())
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
    probes: list[str] = []
    head_rows = graph_execution.head_rows

    def record(backend, plan, n):
        probes.append(backend.id)
        return head_rows(backend, plan, n)

    monkeypatch.setattr(graph_execution, "head_rows", record)
    graph = make_graph(8514, LOCAL)
    add_manual_input(graph, 1, CUSTOMERS.to_dicts())
    source = graph.get_node(1)
    source.setting_input.user_id = 1
    source.setting_input.execution_location = BACKEND_ID
    source.node_settings.cache_results = True
    add_promise(graph, 2, "gate")
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            execution_location=BACKEND_ID,
            gate_input=transform_schema.GateInput(condition_source="formula", formula="[amount] > 20"),
        )
    )
    connect(graph, 1, 2)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert run_info.nodes_completed == 2
        assert run_info.boundaries == []
        assert graph.last_closed_gate_handles == {}
        assert no_s3_client == []
        held = graph.get_node(1).results.result_ref
        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert probes == []
            assert held.backend_id == LOCAL
            return
        assert probes == [BACKEND_ID]
        assert (held.backend_id, held.state) == (BACKEND_ID, "materialised")
    finally:
        graph.close_flow()


@requires_minio
def test_a_retry_reruns_an_input_on_the_backend_holding_its_result_and_crosses_it_again(root, worker, monkeypatch):
    """Flow 8515, the chain on core with manual input 1 on the worker: the first export of node 1's result finds it
    gone and fails node 2 with a missing file, so the retry reruns node 1 on the worker and node 2 crosses it again."""
    local = registry.backend(LOCAL)
    exports: list[str] = []
    export = worker.export

    def drop_the_first(ref, uri, credential):
        exports.append(uri)
        if len(exports) == 1:
            worker.release(ref)
            raise TaskFailed(f"No such file or directory (os error 2): {ref.result_id}.arrow")
        return export(ref, uri, credential)

    monkeypatch.setattr(worker, "export", drop_the_first)
    graph = chain(create_test_graph(flow_id=8515, execution_location=LOCAL), filter_on=LOCAL, user_id=1)
    graph.get_node(1).setting_input.execution_location = WORKER
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        ref_1, ref_2, ref_3 = (graph.get_node(node_id).results.result_ref for node_id in (1, 2, 3))
        assert pl.read_ipc_stream(local.sample(ref_3, 10)).sort("region").equals(EXPECTED)
        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert exports == []
            assert run_info.boundaries == []
            assert {ref.backend_id for ref in (ref_1, ref_2, ref_3)} == {LOCAL}
            return
        assert run_info.boundaries == [(1, 2)]
        assert len(exports) == 2 and exports[0] == exports[1], exports
        prefix = rf"s3://{_BUCKET}/{re.escape(root)}/8515/[0-9a-f]{{32}}/"
        assert re.fullmatch(rf"{prefix}1-{DEFAULT_OUTPUT_HANDLE}-2\.parquet", exports[0]), exports[0]
        assert (ref_1.backend_id, ref_1.state) == (WORKER, "materialised")
        assert worker.exists(ref_1)
        assert (ref_2.backend_id, ref_2.state) == (LOCAL, "materialised")
        assert _keys(f"{root}/8515/") == []
        assert graph.exchange_run is None
    finally:
        graph.close_flow()


@requires_minio
def test_a_retry_after_a_deep_reset_reruns_a_cached_input_on_the_backend_holding_its_result(root, worker, monkeypatch):
    """Flow 8517, the chain on core with manual input 1 on the worker caching its result, is run; switching the flow to
    Performance and back resets every node, so node 1 no longer counts as run with its current setup while it holds its
    result under the same hash. In a second run node 1 is served from that result, the first export of it finds it gone
    and fails node 2 with a missing file, so the retry reruns node 1 on the worker and node 2 crosses it again."""
    local = registry.backend(LOCAL)
    graph = chain(create_test_graph(flow_id=8517, execution_location=LOCAL), filter_on=LOCAL, user_id=1)
    source = graph.get_node(1)
    source.setting_input.execution_location = WORKER
    source.node_settings.cache_results = True
    try:
        run_info = graph.run_graph()
        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        held = source.results.result_ref
        for mode in ("Performance", "Development"):
            graph.flow_settings = graph.flow_settings.model_copy(update={"execution_mode": mode})
        assert source.node_stats.has_run_with_current_setup is False
        assert source.held_result(held.backend_id) == held
        submits: list[tuple[str, int | str]] = []
        for backend in (local, worker):

            def record(plan, inputs, hints, result_id, *, flow_id, node_id, backend=backend, submit=backend.submit):
                submits.append((backend.id, node_id))
                return submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)

            monkeypatch.setattr(backend, "submit", record)
        exports: list[str] = []
        export = worker.export

        def drop_the_first(ref, uri, credential):
            exports.append(uri)
            if len(exports) == 1:
                worker.release(ref)
                raise TaskFailed(f"No such file or directory (os error 2): {ref.result_id}.arrow")
            return export(ref, uri, credential)

        monkeypatch.setattr(worker, "export", drop_the_first)

        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        ref_1, ref_3 = source.results.result_ref, graph.get_node(3).results.result_ref
        assert pl.read_ipc_stream(local.sample(ref_3, 10)).sort("region").equals(EXPECTED)
        if not settings.FLOWFILE_COMPUTE_PROTOCOL:
            assert exports == []
            assert run_info.boundaries == []
            assert (held.backend_id, ref_1.backend_id, ref_3.backend_id) == (LOCAL, LOCAL, LOCAL)
            return
        assert held.backend_id == WORKER
        assert run_info.boundaries == [(1, 2)]
        assert submits == [(WORKER, 1), (LOCAL, 2), (LOCAL, 3)]
        assert len(exports) == 2 and exports[0] == exports[1], exports
        assert (ref_1.backend_id, ref_1.state) == (WORKER, "materialised")
        assert worker.exists(ref_1)
        assert _keys(f"{root}/8517/") == []
        assert graph.exchange_run is None
    finally:
        graph.close_flow()


@requires_minio
def test_a_retry_in_a_fetch_reruns_an_input_changed_since_it_ran_elsewhere_where_the_fetched_node_runs(
    root, worker, monkeypatch
):
    """Flow 8516, the chain on core with its filter on the worker, is run; the filter is then moved to the flow's
    backend and its last result on the worker is gone, and node 3 is fetched with its first submit failing with a
    missing file. The filter holds no result for its current setup, so node 3 composes it and the retry reruns it on
    core, where node 3 runs, not on the worker that held its last result."""
    local = registry.backend(LOCAL)
    graph = chain(create_test_graph(flow_id=8516, execution_location=LOCAL), filter_on=WORKER, user_id=1)
    try:
        run_info = graph.run_graph()
        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        stale = graph.get_node(2).results.result_ref
        assert stale.backend_id == (WORKER if settings.FLOWFILE_COMPUTE_PROTOCOL else LOCAL)
        graph.add_filter(graph.get_node(2).setting_input.model_copy(update={"execution_location": "auto"}))
        registry.backend(stale.backend_id).release(stale)
        submits: list[tuple[str, int | str]] = []
        for backend in (local, worker):

            def fail_the_first(
                plan, inputs, hints, result_id, *, flow_id, node_id, backend=backend, submit=backend.submit
            ):
                submits.append((backend.id, node_id))
                if len(submits) == 1:
                    raise TaskFailed("No such file or directory (os error 2): gone.arrow")
                return submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)

            monkeypatch.setattr(backend, "submit", fail_the_first)

        run_info = graph.trigger_fetch_node(3)

        [row] = run_info.node_step_result
        assert (row.node_id, row.success) == (3, True), row.error
        assert submits == [(LOCAL, 3), (LOCAL, 2), (LOCAL, 3)]
        assert run_info.boundaries == []
        ref_2, ref_3 = graph.get_node(2).results.result_ref, graph.get_node(3).results.result_ref
        assert (ref_2.backend_id, ref_3.backend_id) == (LOCAL, LOCAL)
        assert pl.read_ipc_stream(local.sample(ref_3, 10)).sort("region").equals(EXPECTED)
        assert graph.exchange_run is None
    finally:
        graph.close_flow()


@requires_minio
def test_an_escalated_csv_read_holds_its_result_under_its_hash_and_is_served_from_it(
    root, worker, monkeypatch, tmp_path
):
    """Flow 8518: a CSV read on the worker, caching its result, whose type inference fails at 1 000 rows and is widened,
    → filter 2 on the flow's backend, core with the protocol on and the worker with it off. The widened result is held
    under the read's hash, so a second run serves the read from it and prices its edge with its row count; when the
    first export of it in a third run finds it gone, the retry reruns the read on the worker and node 2 crosses it
    again."""
    local = registry.backend(LOCAL)
    flag = bool(settings.FLOWFILE_COMPUTE_PROTOCOL)
    filter_on = LOCAL if flag else WORKER
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "1000000")
    path = str(tmp_path / "late.csv")
    _late_conflict_csv(path)
    graph = create_test_graph(flow_id=8518, execution_location=filter_on)
    source = read_csv_on_worker(graph, path)
    add_promise(graph, 2, "filter")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=1,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[id] > 10"),
        )
    )
    connect(graph, 1, 2)
    submits: list[tuple[str, int | str, bool]] = []
    for backend in (local, worker):

        def record(plan, inputs, hints, result_id, *, flow_id, node_id, backend=backend, submit=backend.submit):
            if node_id != -1:
                submits.append((backend.id, node_id, result_id == source.hash))
            return submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)

        monkeypatch.setattr(backend, "submit", record)
    costs: list[tuple[int, tuple[int, int] | None]] = []
    cost = graph_execution.BoundaryEdge.cost

    def record_cost(edge):
        answer = cost(edge)
        costs.append((edge.source.node_id, answer))
        return answer

    monkeypatch.setattr(graph_execution.BoundaryEdge, "cost", record_cost)
    exports: list[str] = []
    drop: list[bool] = []
    export = worker.export

    def drop_when_armed(ref, uri, credential):
        exports.append(uri)
        if drop:
            drop.clear()
            worker.release(ref)
            raise TaskFailed(f"No such file or directory (os error 2): {ref.result_id}.arrow")
        return export(ref, uri, credential)

    monkeypatch.setattr(worker, "export", drop_when_armed)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        held = source.results.result_ref
        assert held.result_id == source.hash
        assert (held.backend_id, held.state, held.row_count) == (WORKER, "materialised", 120_000)
        assert source.held_result(WORKER) == held
        assert worker.exists(held)
        assert {column.column_name: column.data_type for column in held.schema}["val"] == "String"
        ref_2 = graph.get_node(2).results.result_ref
        assert ref_2.backend_id == filter_on
        assert registry.backend(filter_on).count(ref_2) == 119_989
        assert submits == [(WORKER, 1, True), (WORKER, 1, True), (filter_on, 2, False)]
        assert run_info.boundaries == ([(1, 2)] if flag else [])
        assert costs == ([(1, None)] if flag else [])
        del submits[:]
        exports_before = len(exports)

        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert submits == []
        assert source.results.result_ref == held
        assert len(exports) == exports_before
        assert run_info.boundaries == ([(1, 2)] if flag else [])
        assert costs[-1:] == ([(1, (120_000, 2))] if flag else [])

        node_2 = graph.get_node(2)
        graph.add_filter(
            node_2.setting_input.model_copy(
                update={"filter_input": transform_schema.FilterInput(mode="advanced", advanced_filter="[id] > 20")}
            )
        )
        drop.append(True)
        del exports[:]

        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        ref_2 = graph.get_node(2).results.result_ref
        assert registry.backend(filter_on).count(ref_2) == 119_979
        if not flag:
            assert exports == []
            assert submits == [(WORKER, 2, False)]
            return
        assert submits == [(WORKER, 1, True), (WORKER, 1, True), (LOCAL, 2, False)]
        assert len(exports) == 2 and exports[0] == exports[1], exports
        prefix = rf"s3://{_BUCKET}/{re.escape(root)}/8518/[0-9a-f]{{32}}/"
        assert re.fullmatch(rf"{prefix}1-{DEFAULT_OUTPUT_HANDLE}-2\.parquet", exports[0]), exports[0]
        rerun = source.results.result_ref
        assert (rerun.result_id, rerun.backend_id, rerun.state) == (source.hash, WORKER, "materialised")
        assert worker.exists(rerun)
        assert _keys(f"{root}/8518/") == []
        assert graph.exchange_run is None
    finally:
        graph.close_flow()


def test_a_formula_gate_probes_an_escalated_csv_read_on_the_backend_holding_its_result(
    worker, no_s3_client, monkeypatch, tmp_path
):
    """Flow 8519: a CSV read on the worker, caching its result, whose type inference is widened, → formula gate 2 on
    the worker. Nothing crosses, and the read holds its widened result under its hash, so with the protocol on the
    gate's probe reads it on the worker."""
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", f"s3://{_BUCKET}/exchange")
    probes: list[str] = []
    head_rows = graph_execution.head_rows

    def record(backend, plan, n):
        probes.append(backend.id)
        return head_rows(backend, plan, n)

    monkeypatch.setattr(graph_execution, "head_rows", record)
    path = str(tmp_path / "late.csv")
    _late_conflict_csv(path)
    graph = create_test_graph(flow_id=8519, execution_location=LOCAL if settings.FLOWFILE_COMPUTE_PROTOCOL else WORKER)
    source = read_csv_on_worker(graph, path)
    add_promise(graph, 2, "gate")
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            execution_location=WORKER,
            gate_input=transform_schema.GateInput(condition_source="formula", formula="[id] > 20"),
        )
    )
    connect(graph, 1, 2)
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        assert run_info.nodes_completed == 2
        assert run_info.boundaries == []
        assert graph.last_closed_gate_handles == {}
        assert no_s3_client == []
        held = source.results.result_ref
        assert held.result_id == source.hash
        assert (held.backend_id, held.state) == (WORKER, "materialised")
        assert source.held_result(WORKER) == held
        assert probes == ([WORKER] if settings.FLOWFILE_COMPUTE_PROTOCOL else [])
    finally:
        graph.close_flow()
