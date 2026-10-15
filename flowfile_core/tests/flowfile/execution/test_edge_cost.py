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

from flowfile_core.flowfile.execution import exchange, registry
from flowfile_core.flowfile.execution.exchange import ExchangeRun
from flowfile_core.flowfile.execution.placement import Refusal
from flowfile_core.flowfile.execution.protocol import OperationKind, ResultRef
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.flowfile.util.execution_orderer import compute_execution_plan
from flowfile_core.schemas import input_schema, transform_schema
from tests.flowfile.execution import test_exchange_store, test_placement
from tests.flowfile.execution.test_mixed_placement import LOCAL, chain
from tests.flowfile.execution.test_placement import (
    BACKEND_ID,
    DeclaringBackend,
    add_manual_input,
    add_promise,
    connect,
    everything,
    make_graph,
)

# The fixtures these tests share with the placement and exchange-store tests, bound here so pytest finds them.
declare = test_placement.declare
protocol_on = test_placement.protocol_on
protocol_off = test_placement.protocol_off
connection_name = test_exchange_store.connection_name

STORE = "s3://test-bucket/exchange"
OUT = DEFAULT_OUTPUT_HANDLE
BOTH_EDGES = [(1, OUT, 2), (2, OUT, 3)]


def above(source_id: int, backend_id: str, rows: int, columns: int, limit: int) -> str:
    return (
        f'the input from node {source_id} would cross from backend "{backend_id}" with {rows} rows of {columns} '
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
    """``_place_run`` over every node of ``graph``, a run's prefix in the exchange store held while it places."""
    graph.exchange_run = ExchangeRun(graph.flow_id, uuid.uuid4().hex)
    try:
        flow_starts = graph._flow_starts + graph.get_implicit_starter_nodes()
        return graph._place_run(compute_execution_plan(nodes=graph.nodes, flow_starts=flow_starts))
    finally:
        graph.exchange_run = None


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
    assert errors == {2: f'cannot run on backend "{LOCAL}": {above(1, BACKEND_ID, 7, 4, 6)}'}
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
        ("6", [Refusal(2, LOCAL, above(1, BACKEND_ID, 7, 4, 6))], [(2, OUT, 3)]),
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
        (False, [Refusal(3, BACKEND_ID, above(2, LOCAL, 1000, 4, 999))], [(1, OUT, 2)]),
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


def test_a_split_whose_input_changed_after_it_ran_is_not_priced_by_that_run(
    protocol_on, declare, connection_name, monkeypatch
):
    backend = CountedSplits(BACKEND_ID, everything())
    registry.register_backend(BACKEND_ID, lambda: backend)
    graph = make_graph(8536)
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
    add_promise(graph, 3, "filter")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            user_id=1,
            execution_location=LOCAL,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[id] >= 0"),
        )
    )
    connect(graph, 2, 3)
    assert graph.run_graph().success
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", STORE)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_CONNECTION", connection_name)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "50")

    ran, _ = placed(graph)
    add_manual_input(graph, 1, [{"id": i} for i in range(10)])
    changed, edges = placed(graph)

    assert ran.refusals == [Refusal(3, LOCAL, above(2, BACKEND_ID, 80, 1, 50))]
    assert changed.refusals == []
    assert crossings(edges) == [(2, OUT, 3)]


def test_a_limit_that_is_not_a_whole_number_refuses_each_crossing_naming_it(protocol_on, declare, store, monkeypatch):
    declare(everything())
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "ten")

    placement, edges = placed(chain(make_graph(8534), filter_on=LOCAL, user_id=1))

    assert [(refusal.node_id, refusal.backend_id) for refusal in placement.refusals] == [(2, LOCAL), (3, BACKEND_ID)]
    for refusal in placement.refusals:
        assert "FLOWFILE_EXCHANGE_MAX_ROWS" in refusal.reason and "'ten'" in refusal.reason, refusal.reason
    assert edges == []


def test_with_the_protocol_off_nothing_is_placed_or_priced(protocol_off, declare, store, monkeypatch):
    declare(everything())
    monkeypatch.setenv("FLOWFILE_EXCHANGE_MAX_ROWS", "1")

    assert placed(chain(make_graph(8535), filter_on=LOCAL, user_id=1)) == (None, [])
