"""An input that would cross between backends with more rows than a limit refuses the run before any node starts.

With an exchange store configured and ``FLOWFILE_EXCHANGE_MAX_ROWS`` set, an edge whose output core knows before the run
to hold more rows than the limit refuses the node that would read it, naming the rows, the columns and the limit. Core
prices an edge from what it already holds, running nothing: the row count of the result the upstream node holds on its
backend, else the count of the engine it holds for that output, its last run's only while it has run with its current
setup, a manual input's own rows among them; an edge whose count is unknown crosses. No MinIO and no worker: the store's
connection is a row in the catalog database, and the run's removal of its prefix lists through a client over an empty
bucket.
"""

import re
import uuid
from dataclasses import replace

import pytest

from flowfile_core.configs.exchange_config import ExchangeConfig
from flowfile_core.flowfile.execution import exchange, registry
from flowfile_core.flowfile.execution.exchange import ExchangeRun
from flowfile_core.flowfile.execution.placement import Refusal
from flowfile_core.flowfile.execution.protocol import OperationKind, ResultRef, SealedCredential
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.flowfile.util.execution_orderer import compute_execution_plan
from flowfile_core.schemas import input_schema
from tests import test_run_flow_node
from tests.flowfile.execution import test_exchange_store, test_placement
from tests.flowfile.execution.graph_helpers import add_filter_on, add_manual_input, add_promise, connect, make_graph
from tests.flowfile.execution.test_mixed_placement import LOCAL, chain
from tests.flowfile.execution.test_placement import AUTO_REMEDY, BACKEND_ID, OWN_REMEDY, DeclaringBackend, everything

# The fixtures these tests share with the placement and exchange-store tests, bound here so pytest finds them.
declare = test_placement.declare
connection_name = test_exchange_store.connection_name
two_port_subflow = test_run_flow_node.two_port_subflow

STORE = "s3://test-bucket/exchange"
OUT = DEFAULT_OUTPUT_HANDLE
BOTH_EDGES = [(1, OUT, 2), (2, OUT, 3)]


STEPS = {1: "Manual input 1", 2: "Filter data 2"}
"""The sources of ``chain``'s edges as the canvas names them."""
SPLIT = "Random Split 2"


def above(source: int | str, backend_id: str, rows: int, columns: int, limit: int) -> str:
    """The refusal of an input from ``source`` (a node of ``chain`` or a step's name) above ``limit`` rows."""
    step = STEPS[source] if isinstance(source, int) else source
    return (
        f'the input from the step "{step}" would cross from backend "{backend_id}" with {rows} rows of {columns} '
        f"columns, above FLOWFILE_EXCHANGE_MAX_ROWS={limit}"
    )


@pytest.fixture(autouse=True)
def _unset(monkeypatch):
    for name in ("FLOWFILE_EXCHANGE_URI", "FLOWFILE_EXCHANGE_CONNECTION", "FLOWFILE_EXCHANGE_MAX_ROWS"):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def store(monkeypatch, connection_name):
    """The exchange store at ``STORE``, opened by a MinIO connection stored for user 1."""
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", STORE)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection_name)


class EmptyBucket:
    """An S3 client over an empty bucket, recording each prefix it lists and each delete it is asked for."""

    def __init__(self):
        self.listed: list[str] = []
        self.deleted: list[dict] = []

    def get_paginator(self, operation: str) -> "EmptyBucket":
        return self

    def paginate(self, *, Bucket: str, Prefix: str) -> list[dict]:
        self.listed.append(f"s3://{Bucket}/{Prefix}")
        return [{}]

    def delete_objects(self, **request) -> dict:
        self.deleted.append(request)
        return {}


@pytest.fixture
def empty_store(monkeypatch) -> EmptyBucket:
    """The client the run's removal of its prefix makes, over an empty bucket."""
    client = EmptyBucket()
    monkeypatch.setattr(exchange, "build_s3_client", lambda *args, **kwargs: client)
    return client


def placed(graph: FlowGraph):
    """``_place_run`` over every node of ``graph``, given a fresh run's prefix in the exchange store."""
    flow_starts = graph._flow_starts + graph.get_implicit_starter_nodes()
    plan = compute_execution_plan(nodes=graph.nodes, flow_starts=flow_starts)
    return graph._place_run(plan, ExchangeRun(graph.flow_id, uuid.uuid4().hex))


def crossings(edges) -> list[tuple]:
    return [(edge.source.node_id, edge.handle, edge.target.node_id) for edge in edges]


def test_a_run_whose_input_is_known_to_be_above_the_limit_starts_no_node(
    protocol_on, declare, store, empty_store, monkeypatch
):
    backend = declare(everything())
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "6")
    graph = chain(make_graph(8531), filter_on=LOCAL, user_id=1)

    run_info = graph.run_graph()

    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert errors == {2: f'cannot run on backend "{LOCAL}": {above(1, BACKEND_ID, 7, 4, 6)}. {OWN_REMEDY}'}
    assert all(result.compute_error == "Refusal" for result in run_info.node_step_result)
    assert (run_info.success, run_info.nodes_completed) == (False, 0)
    assert backend.work == []
    assert len(empty_store.listed) == 1, empty_store.listed
    assert re.fullmatch(rf"{STORE}/8531/[0-9a-f]{{32}}/", empty_store.listed[0]), empty_store.listed
    assert empty_store.deleted == []


@pytest.mark.parametrize(
    "limit, refusals, crossed",
    [
        (None, [], BOTH_EDGES),
        ("7", [], BOTH_EDGES),
        ("100", [], BOTH_EDGES),
        ("6", [Refusal(2, LOCAL, above(1, BACKEND_ID, 7, 4, 6), OWN_REMEDY)], [(2, OUT, 3)]),
    ],
    ids=["no-limit", "at-the-limit", "below-the-limit", "above-the-limit"],
)
def test_an_edge_is_refused_only_when_its_known_rows_are_above_the_limit(
    protocol_on, declare, store, monkeypatch, limit, refusals, crossed
):
    declare(everything())
    if limit is not None:
        monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", limit)

    placement, edges = placed(chain(make_graph(8532), filter_on=LOCAL, user_id=1))

    assert placement.refusals == refusals
    assert crossings(edges) == crossed


@pytest.mark.parametrize(
    "stale, refusals, crossed",
    [
        (False, [Refusal(3, BACKEND_ID, above(2, LOCAL, 1000, 4, 999), AUTO_REMEDY)], [(1, OUT, 2)]),
        (True, [], BOTH_EDGES),
    ],
    ids=["held", "stale-hash"],
)
def test_the_row_count_of_a_held_result_prices_the_edge_it_crosses(
    protocol_on, declare, store, monkeypatch, stale, refusals, crossed
):
    declare(everything())
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "999")
    graph = chain(make_graph(8533), filter_on=LOCAL, user_id=1)
    node = graph.get_node(2)
    node.results.result_ref = ResultRef(
        backend_id=LOCAL,
        result_id=f"{node.hash}_stale" if stale else node.hash,
        schema=node.schema,
        row_count=1000,
        state="materialised",
        location=b"",
    )

    placement, edges = placed(graph)

    assert placement.refusals == refusals
    assert crossings(edges) == crossed


class CountedSplits(DeclaringBackend):
    """Core's own backend under the declaring id, whose random split knows each split's rows, as a worker's does."""

    def run_op(self, op, payload):
        result = super().run_op(op, payload)
        if op is not OperationKind.random_split:
            return result
        counted = {name: replace(ref, row_count=self.count(ref)) for name, ref in result.refs.items()}
        return replace(result, refs=counted)


def split_100(flow_id: int) -> FlowGraph:
    """Manual input 1 of 100 ids → random split 2, seeded, 80/20, on ``CountedSplits`` under the declaring id."""
    backend = CountedSplits(BACKEND_ID, everything())
    registry.register_backend(BACKEND_ID, lambda: backend)
    graph = make_graph(flow_id)
    add_manual_input(graph, 1, [{"id": i} for i in range(100)])
    add_promise(graph, 2, "random_split")
    connect(graph, 1, 2)
    graph.add_random_split(
        input_schema.NodeRandomSplit(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=1,
            seed=42,
            splits=[
                input_schema.RandomSplitGroup(name="train", percentage=80.0),
                input_schema.RandomSplitGroup(name="test", percentage=20.0),
            ],
        )
    )
    return graph


def test_a_split_whose_input_changed_after_it_ran_is_not_priced_by_that_run(
    protocol_on, declare, connection_name, monkeypatch
):
    graph = split_100(8536)
    add_filter_on(graph, 3, 2, "[id] >= 0", user_id=1, execution_location=LOCAL)
    assert graph.run_graph().success
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", STORE)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection_name)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "50")

    ran, _ = placed(graph)
    add_manual_input(graph, 1, [{"id": i} for i in range(10)])
    changed, edges = placed(graph)

    assert ran.refusals == [Refusal(3, LOCAL, above(SPLIT, BACKEND_ID, 80, 1, 50), OWN_REMEDY)]
    assert changed.refusals == []
    assert crossings(edges) == [(2, OUT, 3)]


def test_a_node_reading_two_outputs_of_one_source_above_the_limit_is_refused_once(
    protocol_on, declare, connection_name, two_port_subflow, monkeypatch
):
    """Random split 2 (80/20 of 100 rows) on the declaring backend feeds both slots of run flow 9 on core."""
    graph = split_100(8537)
    add_promise(graph, 9, "run_flow")
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=graph.flow_id,
            node_id=9,
            user_id=1,
            execution_location=LOCAL,
            flow_reference=input_schema.SubflowReference(registration_id=two_port_subflow["registration_id"]),
            input_slots=["a", "b"],
            output_slots=["out_a", "out_b"],
        )
    )
    test_run_flow_node.connect(graph, 2, 9, "input-1", OUT)
    test_run_flow_node.connect(graph, 2, 9, "input-2", "output-1")
    assert graph.run_graph(node_ids={1, 2}).success
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", STORE)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection_name)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "10")

    placement, edges = placed(graph)

    assert placement.refusals == [Refusal(9, LOCAL, above(SPLIT, BACKEND_ID, 80, 1, 10), OWN_REMEDY)]
    assert edges == []


def test_a_limit_that_is_not_a_whole_number_refuses_each_crossing_naming_it(protocol_on, declare, store, monkeypatch):
    declare(everything())
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "ten")

    placement, edges = placed(chain(make_graph(8534), filter_on=LOCAL, user_id=1))

    assert [(refusal.node_id, refusal.backend_id) for refusal in placement.refusals] == [(2, LOCAL), (3, BACKEND_ID)]
    for refusal in placement.refusals:
        assert "FLOWFILE_EXCHANGE_MAX_ROWS" in refusal.reason and "'ten'" in refusal.reason, refusal.reason
    assert edges == []


def test_a_run_given_its_hubs_store_crosses_through_it_held_to_the_hubs_limit(
    protocol_on, declare, connection_name, monkeypatch
):
    """Nothing set on the node: the store a joined server's hub vended for the run, sealed to its nodes, places it on
    more than one backend, its edges are written under the hub's URI and the hub's row limit prices them; without it
    the crossing is refused for want of a store."""
    vending = everything().model_copy(update={"credential_resolution": "vended"})
    declare(vending)
    declare(vending, registry.LOCAL_BACKEND_ID)
    monkeypatch.setattr("flowfile_core.flowfile.execution.registry.server_name", lambda backend_id: backend_id)
    graph = chain(make_graph(8541), filter_on=LOCAL, user_id=1)
    flow_starts = graph._flow_starts + graph.get_implicit_starter_nodes()
    plan = compute_execution_plan(nodes=graph.nodes, flow_starts=flow_starts)
    vended = ExchangeConfig(STORE, connection_name, max_rows=6, vended_for=1)
    run = ExchangeRun(graph.flow_id, uuid.uuid4().hex, hub_store=vended)
    run.sealed = SealedCredential(base_uri=STORE, sealed={BACKEND_ID: "sealed-token", registry.LOCAL_BACKEND_ID: "sealed-token"})

    placement, edges = graph._place_run(plan, run)

    assert run.max_rows() == 6
    assert placement.refusals == [Refusal(2, LOCAL, above(1, BACKEND_ID, 7, 4, 6), OWN_REMEDY)]
    assert crossings(edges) == [(2, OUT, 3)]
    assert edges[0].uri == f"{STORE}/8541/{run.run_id}/2-{OUT}-3.parquet"

    placement, edges = graph._place_run(plan, ExchangeRun(graph.flow_id, uuid.uuid4().hex))

    assert edges == []
    assert [refusal.node_id for refusal in placement.refusals] == [2]
    assert "no exchange store can carry its data between backends" in placement.refusals[0].reason


def test_with_the_protocol_off_nothing_is_placed_or_priced(protocol_off, declare, store, monkeypatch):
    declare(everything())
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "1")

    assert placed(chain(make_graph(8535), filter_on=LOCAL, user_id=1)) == (None, [])
