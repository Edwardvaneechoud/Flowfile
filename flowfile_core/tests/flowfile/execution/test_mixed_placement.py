"""The nodes of one run on different backends, their data crossing between them through the exchange store.

With an exchange store configured (``FLOWFILE_EXCHANGE_URI``), a run places each node on the backend its own
``execution_location`` names, ``"auto"`` on the run's, and every edge between two backends crosses through the run's
prefix in the store: the upstream result exported there and imported on the other backend, whose node that reads it is
materialised, so no result refers to the prefix once the run removes it. Without a store a node's own setting is not
read. The protocol flag comes from the environment, as for the exchange-store and two-backend chain tests whose fixtures
and frames these reuse; the cases over a declaring backend set it on themselves.
"""

import re
import uuid

import polars as pl
import pytest

from flowfile_core import events
from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import exchange, registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.placement import MixedPlacementPolicy, Placement, SingleBackendPolicy, place
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.executor import hints_for
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.schemas import input_schema, transform_schema
from tests.flowfile.conftest import create_test_graph
from tests.flowfile.execution import test_exchange_store, test_placement
from tests.flowfile.execution.test_exchange_store import _BUCKET, _key, _keys
from tests.flowfile.execution.test_placement import add_manual_input, add_promise, connect, everything, make_graph
from tests.flowfile.execution.test_two_backend_chain import A_ROWS, CUSTOMERS, EXPECTED
from tests.test_catalog_cloud_virtual import requires_minio

# The fixtures these tests share with the placement and exchange-store tests, bound here so pytest finds them.
declare = test_placement.declare
connection_name = test_exchange_store.connection_name
root = test_exchange_store.root

LOCAL, WORKER = registry.LOCAL_BACKEND_ID, registry.WORKER_BACKEND_ID


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
    assert errors[3].startswith(f'cannot run on backend "{backend.id}": '), errors[3]
    assert all(result.compute_error == "Refusal" for result in run_info.node_step_result)
    assert (run_info.success, run_info.nodes_completed) == (False, 0)
    assert backend.work == []
    assert no_s3_client == []


@requires_minio
def test_a_node_on_the_worker_takes_its_input_from_core_and_hands_its_rows_back_through_the_store(root, worker):
    local = registry.backend(LOCAL)
    graph = chain(create_test_graph(flow_id=8505, execution_location=LOCAL), filter_on=WORKER, user_id=1)
    seen: dict = {}

    def list_the_prefix(graph, **_):
        try:
            prefix = f"s3://{_BUCKET}/{root}/{graph.flow_id}/{graph.exchange_run.run_id}/"
            seen.update(prefix=prefix, listed=_keys(prefix))
        except Exception as e:
            seen["error"] = e

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
            return
        assert seen["listed"] == [f"{_key(seen['prefix'])}1-2.parquet", f"{_key(seen['prefix'])}2-3.parquet"]
        assert (ref_2.backend_id, ref_2.state) == (WORKER, "materialised")
        assert (ref_3.backend_id, ref_3.state) == (LOCAL, "materialised")
        assert pl.read_ipc_stream(local.sample(ref_3, 10)).sort("region").equals(EXPECTED)
        assert pl.read_ipc_stream(worker.sample(ref_2, 100)).sort("id").equals(A_ROWS)
    finally:
        events._handlers["flow_run_finished"].remove(list_the_prefix)
        graph.close_flow()
