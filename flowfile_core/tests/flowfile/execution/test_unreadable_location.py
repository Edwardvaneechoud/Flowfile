"""A flow over results stored where core cannot read them runs, previews and predicts from their references.

An unreadable location is a stored result's file that exists only while a call of the backend that holds it runs:
core keeps the reference and composes on the ``scan_ipc`` leaf ``plan_for`` returns, as it does for a worker on another
host, but any read of that leaf from core raises ``FileNotFoundError``. A touch fails the run or the test outright
instead of being counted, so these flows pass only while core takes a stored result's schema from its reference.
They run with the protocol flag off and on: with it on, core asks the backend that holds a result whether it still
does. The last two tests check the same over the session worker's ``/v1`` routes.
"""

import dataclasses
import os
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node.executor import NodeExecutor
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.models import InvalidationReason
from flowfile_core.schemas import input_schema, schemas, transform_schema
from shared.compute_protocol import WorkerLocation

BACKEND_ID = "unreadable"


class UnreadableBackend(LocalBackend):
    """Core's own backend, except that a result it stores sits at a path that exists only while one of its calls runs.

    It stores a submit whose hints say ``must_materialise`` and every result an operation holds, with the inline sample
    for a stored submit; every other plan stays deferred, as in ``LocalBackend``. A stored result is kept under
    ``<root>/<result id>.arrow``, which is its ``location``, and ``plan_for`` returns ``scan_ipc`` of that path.
    ``submit``, ``run_op``, ``sample`` and ``count`` write every stored result to its path, so the plans composed on
    them run, and remove the files before they return.
    """

    def __init__(self, root: Path):
        super().__init__()
        self.id = BACKEND_ID
        self.root = root
        self._storing = False

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        with self._readable(storing=hints.must_materialise):
            result = super().submit(plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)
            if result.ref.state == "materialised" and hints.preview_rows is not None:
                result = dataclasses.replace(result, sample=super().sample(result.ref, hints.preview_rows))
        return result

    def run_op(self, op, payload):
        with self._readable(storing=True):
            return super().run_op(op, payload)

    def sample(self, ref, n):
        with self._readable():
            return super().sample(ref, n)

    def count(self, ref):
        with self._readable():
            return super().count(ref)

    def plan_for(self, ref):
        if ref.state == "deferred":
            return super().plan_for(ref)
        self._plan(ref)
        return pl.scan_ipc(ref.location.decode("utf-8"))

    def _hold(self, frame, row_count=None, schema=None, result_id=None):
        if not self._storing:
            return super()._hold(frame, row_count=row_count, schema=schema, result_id=result_id)
        held = super()._hold(frame.lazy().collect(), result_id=result_id)
        path = str(self.root / f"{held.result_id}.arrow")
        with self._lock:
            self._results[path] = self._results.pop(held.result_id)
        return dataclasses.replace(held, location=path.encode("utf-8"))

    @contextmanager
    def _readable(self, storing: bool = False):
        """Every stored result is on disk while the block runs; with ``storing``, ``_hold`` stores what it holds."""
        with self._lock:
            stored = {key: frame for key, frame in self._results.items() if key.startswith(str(self.root))}
        self.root.mkdir(parents=True, exist_ok=True)
        for path, frame in stored.items():
            frame.collect().write_ipc(path)
        self._storing = storing
        try:
            yield
        finally:
            self._storing = False
            for path in stored:
                os.remove(path)


@pytest.fixture(params=[False, True], ids=["flag_off", "flag_on"])
def protocol(request, monkeypatch) -> bool:
    """The protocol flag in each state, back to what it was when the test ends."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", request.param)
    return request.param


@pytest.fixture
def protocol_on(monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)


@pytest.fixture
def unreadable(monkeypatch, tmp_path) -> list[UnreadableBackend]:
    """``unreadable`` is registered and is the default backend; the list holds each instance its factory made."""
    made: list[UnreadableBackend] = []

    def make_unreadable() -> UnreadableBackend:
        made.append(UnreadableBackend(tmp_path / "unreadable"))
        return made[-1]

    registry.reset()
    registry.register_backend(BACKEND_ID, make_unreadable)
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", BACKEND_ID)
    yield made
    registry.reset()


@pytest.fixture
def remote_worker():
    from flowfile_core.schemas.schemas import is_valid_execution_location_in_current_global_settings
    from tests.conftest import is_worker_running

    if not (is_worker_running() and is_valid_execution_location_in_current_global_settings("remote")):
        pytest.skip("Remote execution not active")


def make_graph(flow_id: int, **settings_fields) -> FlowGraph:
    fields = {"execution_location": "remote", "execution_mode": "Development"}
    fields.update(settings_fields)
    return FlowGraph(schemas.FlowSettings(flow_id=flow_id, name=f"unreadable_{flow_id}", path=".", **fields))


def add_promise(graph: FlowGraph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def connect(graph: FlowGraph, from_id: int, to_id: int, input_type: str = "main") -> None:
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id, to_id, input_type=input_type))


def add_manual_input(graph: FlowGraph, node_id: int, rows: list[dict]) -> None:
    add_promise(graph, node_id, "manual_input")
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(rows)
        )
    )


def add_select_on(graph: FlowGraph, node_id: int, source_id: int, select: list[tuple[str, str]], keep_missing: bool):
    add_promise(graph, node_id, "select")
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=source_id,
            select_input=[transform_schema.SelectInput(old_name=old, new_name=new) for old, new in select],
            keep_missing=keep_missing,
        )
    )
    connect(graph, source_id, node_id)


def add_filter_on(graph: FlowGraph, node_id: int, source_id: int, expression: str) -> None:
    add_promise(graph, node_id, "filter")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=source_id,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter=expression),
        )
    )
    connect(graph, source_id, node_id)


def stored_results_flow(flow_id: int) -> FlowGraph:
    """Manual input 1 → filter 2 → select 3 → join 5 (with manual input 4) and group_by 6; random split 7 on 1.

    The join and the group_by cache their results, so the backend stores them, as it stores the random split's.
    """
    graph = make_graph(flow_id, max_parallel_workers=1)
    customers = [{"id": i, "name": f"n{i}", "region": "eu" if i % 2 else "us", "amount": i * 10} for i in range(1, 7)]
    add_manual_input(graph, 1, customers)
    add_filter_on(graph, 2, 1, "[amount] > 10")
    add_select_on(graph, 3, 2, [("id", "id"), ("name", "customer"), ("region", "region"), ("amount", "amount")], False)
    add_manual_input(graph, 4, [{"id": i, "segment": f"s{i % 3}"} for i in range(1, 7)])
    add_promise(graph, 5, "join")
    graph.add_join(
        input_schema.NodeJoin(
            flow_id=graph.flow_id,
            node_id=5,
            depending_on_ids=[3, 4],
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("id", "id")], left_select=[], right_select=[]
            ),
        )
    )
    connect(graph, 3, 5)
    connect(graph, 4, 5, input_type="right")
    add_promise(graph, 6, "group_by")
    graph.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=graph.flow_id,
            node_id=6,
            depending_on_id=3,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl("region", "groupby"),
                    transform_schema.AggColl("amount", "sum", "total"),
                ]
            ),
        )
    )
    connect(graph, 3, 6)
    add_promise(graph, 7, "random_split")
    graph.add_random_split(
        input_schema.NodeRandomSplit(
            flow_id=graph.flow_id,
            node_id=7,
            depending_on_id=1,
            seed=1,
            splits=[
                input_schema.RandomSplitGroup(name="train", percentage=50.0),
                input_schema.RandomSplitGroup(name="test", percentage=50.0),
            ],
        )
    )
    connect(graph, 1, 7)
    for node_id in (5, 6):
        graph.get_node(node_id).node_settings.cache_results = True
    return graph


def geometry_flow(flow_id: int) -> FlowGraph:
    """Manual input 1 → polars code 2, which makes ``g`` a ``geoarrow.wkb`` column → sort 3, which the worker stores."""
    graph = make_graph(flow_id)
    add_manual_input(graph, 1, [{"id": i, "g": f"x{i}"} for i in range(1, 5)])
    add_promise(graph, 2, "polars_code")
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_ids=[1],
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code='input_df.with_columns(pl.col("g").cast(pl.Binary)'
                '.cast(pl.Extension("geoarrow.wkb", pl.Binary, "{}")))'
            ),
        )
    )
    connect(graph, 1, 2)
    add_promise(graph, 3, "sort")
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            sort_input=[transform_schema.SortByInput(column="id", how="desc")],
        )
    )
    connect(graph, 2, 3)
    return graph


def columns(schema) -> list[tuple[str, str]]:
    return [(column.column_name, column.data_type) for column in schema]


def decide(node: FlowNode):
    return NodeExecutor(node)._decide_execution(
        state=node._execution_state, performance_mode=False, force_refresh=False
    )


def failures(run_info) -> list[tuple[int, str]]:
    return [(result.node_id, result.error) for result in run_info.node_step_result if not result.success]


def test_a_flow_runs_previews_and_predicts_over_stored_results_core_cannot_read(unreadable, protocol):
    graph = stored_results_flow(9341)
    try:
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        for node_id in (5, 6):
            node = graph.get_node(node_id)
            ref = node.results.result_ref
            assert ref.state == "materialised"
            assert not os.path.exists(ref.location.decode("utf-8"))
            assert columns(node.schema) == columns(ref.schema)
        assert columns(graph.get_node(5).schema) == [
            ("id", "Int64"),
            ("customer", "String"),
            ("region", "String"),
            ("amount", "Int64"),
            ("segment", "String"),
            ("id_right", "Int64"),
        ]
        assert columns(graph.get_node(6).schema) == [("region", "String"), ("total", "Int64")]
        splits = graph.get_node(7)._named_outputs
        assert {handle: len(engine.schema) for handle, engine in splits.items()} == {"output-0": 4, "output-1": 4}
        previews = {node_id: len(graph.get_node(node_id).get_table_example(True).data) for node_id in range(1, 7)}
        assert previews == {1: 6, 2: 5, 3: 5, 4: 6, 5: 5, 6: 2}

        add_select_on(graph, 8, 5, [("segment", "seg")], True)
        add_filter_on(graph, 9, 6, "[total] > 0")

        assert [column.column_name for column in graph.get_node(8).get_predicted_schema()] == [
            "seg",
            "id",
            "customer",
            "region",
            "amount",
            "id_right",
        ]
        assert [column.column_name for column in graph.get_node(9).get_predicted_schema()] == ["region", "total"]
    finally:
        graph.close_flow()


def test_a_result_its_backend_dropped_is_run_again(unreadable, protocol):
    graph = stored_results_flow(9342)
    try:
        assert graph.run_graph().success
        select = graph.get_node(3)
        assert select.results.result_ref.state == "deferred"

        unreadable[0].release(select.results.result_ref)
        decision = decide(select)

        if protocol:
            assert decision.should_run
            assert decision.reason == InvalidationReason.CACHE_MISSING
        else:
            assert not decision.should_run
        run_info = graph.run_graph()
        assert run_info.success, failures(run_info)
    finally:
        graph.close_flow()


def test_a_stored_v1_result_keeps_the_columns_the_worker_sent(remote_worker, protocol_on):
    graph = geometry_flow(9343)
    try:
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        sort = graph.get_node(3)
        assert WorkerLocation.model_validate_json(sort.results.result_ref.location).kind == "file"
        assert [(column.column_name, column.semantic_type) for column in sort.schema] == [
            ("id", None),
            ("g", "geometry"),
        ]
    finally:
        graph.close_flow()


def test_a_v1_result_the_worker_dropped_is_run_again(remote_worker, protocol_on):
    graph = geometry_flow(9344)
    try:
        assert graph.run_graph().success
        sort = graph.get_node(3)
        worker = registry.backend(sort.results.result_ref.backend_id)

        worker.release(sort.results.result_ref)
        decision = decide(sort)

        assert decision.should_run
        assert decision.reason == InvalidationReason.CACHE_MISSING
        run_info = graph.run_graph()
        assert run_info.success, failures(run_info)
        assert worker.exists(sort.results.result_ref)
    finally:
        graph.close_flow()
