"""A flow over results stored where core cannot read them runs, previews and predicts from their references.

An unreadable location is a stored result's file that exists only while a call of the backend that holds it runs:
core keeps the reference and composes on the ``scan_ipc`` leaf ``plan_for`` returns, as it does for a worker on another
host, but any read of that leaf from core raises ``FileNotFoundError``. A touch fails the run or the test outright
instead of being counted, so these flows pass only while core takes a stored result's schema from its reference.
They run with the protocol flag off and on: with it on, core asks the backend that holds a result whether it still
does, and reads rows of those results through the backend: the formula editor's preview row, a formula gate's probe,
column statistics and the CSV export; and a filter, a select, a join, a group_by, a formula, a record count, a unique,
a union, an unpivot, window functions, text to rows, a multi-field formula, a graph solver, an explode hierarchy, a
pivot, a dynamic rename, a data cleansing or a cross join over a stored result, or the REST API reader's alignment of
a read it stored, takes its input's columns from the input engine's cached schema. The tests named for ``v1`` results
check the same over the session worker's ``/v1`` routes.
"""

import dataclasses
import io
import json
import os
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from flowfile_core import flow_file_handler
from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.extensions import get_instant_func_results
from flowfile_core.flowfile.flow_data_engine.bounded import unique_values
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.formula_entries import (
    FormulaEntry,
    FormulaEntryError,
    apply_formula_entries,
)
from flowfile_core.flowfile.flow_data_engine.hierarchy import explode_hierarchy_frame
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node.executor import NodeExecutor
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.models import InvalidationReason
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.routes.routes import export_node_data
from flowfile_core.schemas import input_schema, schemas, transform_schema
from shared.compute_protocol import WorkerLocation

BACKEND_ID = "unreadable"


def detached(plan: pl.LazyFrame) -> pl.LazyFrame:
    """A copy of ``plan`` through its serialised form, sharing nothing Polars resolved on the original."""
    return pl.LazyFrame.deserialize(io.BytesIO(plan.serialize()))


def detached_payload(payload):
    """An operation payload with each of its ``pl.LazyFrame`` fields detached."""
    frames = {
        field.name: detached(getattr(payload, field.name))
        for field in dataclasses.fields(payload)
        if isinstance(getattr(payload, field.name), pl.LazyFrame)
    }
    return dataclasses.replace(payload, **frames)


class UnreadableBackend(LocalBackend):
    """Core's own backend, except that a result it stores sits at a path that exists only while one of its calls runs.

    It stores a submit whose hints say ``must_materialise`` and every result an operation holds, with the inline sample
    for a stored submit; every other plan stays deferred, as in ``LocalBackend``. A stored result is kept under
    ``<root>/<result id>.arrow``, which is its ``location``, and ``plan_for`` returns ``scan_ipc`` of that path.
    ``submit``, ``run_op``, ``sample`` and ``count`` write every stored result to its path, so the plans composed on
    them run, and remove the files before they return.

    Like a worker, it runs a copy of each plan core sends (``detached``) and hands core a copy of each deferred plan
    it holds: Polars keeps what it resolves of a scan on the plan object, so a backend that resolved core's own plans
    while their files existed would answer core's later reads of the same leaves without touching the files.
    """

    def __init__(self, root: Path):
        super().__init__()
        self.id = BACKEND_ID
        self.root = root
        self._storing = False

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        with self._readable(storing=hints.must_materialise):
            result = super().submit(detached(plan), inputs, hints, result_id, flow_id=flow_id, node_id=node_id)
            if result.ref.state == "materialised" and hints.preview_rows is not None:
                result = dataclasses.replace(result, sample=super().sample(result.ref, hints.preview_rows))
        return result

    def run_op(self, op, payload):
        with self._readable(storing=True):
            return super().run_op(op, {key: detached_payload(value) for key, value in payload.items()})

    def sample(self, ref, n):
        with self._readable():
            return super().sample(ref, n)

    def count(self, ref):
        with self._readable():
            return super().count(ref)

    def plan_for(self, ref):
        if ref.state == "deferred":
            return detached(super().plan_for(ref))
        self._plan(ref)
        return pl.scan_ipc(ref.location.decode("utf-8"))

    def _store(self, plan, *, result_id, flow_id):
        return self._hold(plan, result_id=result_id)

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


def stored_inputs_flow(flow_id: int) -> FlowGraph:
    """Manual input 1 → filter 2 → select 3 → join 5 (with manual input 4) → group_by 6; filter 7 on 1, split in two.

    1, 4 and 5 cache their results, so the backend stores them: filter 2 and the split run over a stored result, the
    select over a plan composed on one, the join over a stored right input and the group_by over a stored input.
    """
    graph = make_graph(flow_id, max_parallel_workers=1)
    customers = [{"id": i, "name": f"n{i}", "region": "eu" if i % 2 else "us", "amount": i * 10} for i in range(1, 7)]
    add_manual_input(graph, 1, customers)
    add_filter_on(graph, 2, 1, "[amount] > 10")
    add_promise(graph, 3, "select")
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            select_input=[
                transform_schema.SelectInput(old_name="id"),
                transform_schema.SelectInput(old_name="name", new_name="customer"),
                transform_schema.SelectInput(old_name="region"),
                transform_schema.SelectInput(old_name="amount", data_type="Float64", data_type_change=True),
            ],
            keep_missing=False,
        )
    )
    connect(graph, 2, 3)
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
            depending_on_id=5,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl("region", "groupby"),
                    transform_schema.AggColl("amount", "sum", "total"),
                ]
            ),
        )
    )
    connect(graph, 5, 6)
    add_promise(graph, 7, "filter")
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=7,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter='[region] = "eu"'),
            split_mode=True,
        )
    )
    connect(graph, 1, 7)
    for node_id in (1, 4, 5):
        graph.get_node(node_id).node_settings.cache_results = True
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


CUSTOMERS = [("id", "Int64"), ("name", "String"), ("region", "String"), ("amount", "Int64")]
SELECTED = [("id", "Int64"), ("customer", "String"), ("region", "String"), ("amount", "Float64")]


def test_filter_select_join_and_group_by_run_over_stored_results_core_cannot_read(unreadable, protocol_on):
    graph = stored_inputs_flow(9347)
    try:
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        for node_id in (1, 4, 5):
            ref = graph.get_node(node_id).results.result_ref
            assert ref.state == "materialised"
            assert not os.path.exists(ref.location.decode("utf-8"))
        assert {node_id: columns(graph.get_node(node_id).schema) for node_id in range(1, 7)} == {
            1: CUSTOMERS,
            2: CUSTOMERS,
            3: SELECTED,
            4: [("id", "Int64"), ("segment", "String")],
            5: SELECTED + [("segment", "String"), ("id_right", "Int64")],
            6: [("region", "String"), ("total", "Float64")],
        }
        splits = graph.get_node(7)._named_outputs
        assert {handle: columns(engine.schema) for handle, engine in splits.items()} == {
            "output-0": CUSTOMERS,
            "output-1": CUSTOMERS,
        }
        previews = {node_id: len(graph.get_node(node_id).get_table_example(True).data) for node_id in range(1, 7)}
        assert previews == {1: 6, 2: 5, 3: 5, 4: 6, 5: 5, 6: 2}

        add_promise(graph, 8, "pivot")
        connect(graph, 5, 8)
        graph.add_pivot(
            input_schema.NodePivot(
                flow_id=graph.flow_id,
                node_id=8,
                depending_on_id=5,
                pivot_input=transform_schema.PivotInput(
                    index_columns=["region"], pivot_column="segment", value_col="amount", aggregations=["sum"]
                ),
            )
        )

        assert [column.column_name for column in graph.get_node(8).get_predicted_schema()] == [
            "region",
            "s0",
            "s1",
            "s2",
        ]
    finally:
        graph.close_flow()


def test_a_formula_runs_over_a_stored_result_core_cannot_read(unreadable, protocol_on):
    graph = make_graph(9350)
    try:
        add_manual_input(graph, 1, [{"x1": i, "x2": i * 10} for i in range(1, 5)])
        graph.get_node(1).node_settings.cache_results = True
        add_promise(graph, 2, "formula")
        graph.add_formula(
            input_schema.NodeFormula(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                functions=[
                    transform_schema.FunctionInput(
                        transform_schema.FieldInput(name="y", data_type="Float64"), "[x1] * 2 + [x2]"
                    ),
                    transform_schema.FunctionInput(transform_schema.FieldInput(name="z"), "[y] + 1"),
                ],
            )
        )
        connect(graph, 1, 2)
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        ref = graph.get_node(1).results.result_ref
        assert ref.state == "materialised"
        assert not os.path.exists(ref.location.decode("utf-8"))
        assert columns(graph.get_node(2).schema) == [
            ("x1", "Int64"),
            ("x2", "Int64"),
            ("y", "Float64"),
            ("z", "Float64"),
        ]
        assert len(graph.get_node(2).get_table_example(True).data) == 4
    finally:
        graph.close_flow()


TRANSFORM_ROWS = [
    {
        "id": i,
        "region": "eu" if i % 2 else "us",
        "amount": i * 10,
        "parent": f"p{i // 3}",
        "child": f"c{i}",
        "tags": "x,y" if i % 2 else "z",
    }
    for i in range(1, 7)
]
TRANSFORM_COLUMNS = [
    ("id", "Int64"),
    ("region", "String"),
    ("amount", "Int64"),
    ("parent", "String"),
    ("child", "String"),
    ("tags", "String"),
]
UNION_ROWS = [{"id": 7, "region": "eu", "amount": 70.5}, {"id": 8, "region": "us", "amount": 80.5}]

TRANSFORMS = [
    (9351, "record_count", input_schema.NodeRecordCount, {}, [("number_of_records", "UInt32")], 1),
    (
        9352,
        "unique",
        input_schema.NodeUnique,
        {"unique_input": transform_schema.UniqueInput(columns=["region"], strategy="first")},
        TRANSFORM_COLUMNS,
        2,
    ),
    (
        9353,
        "union",
        input_schema.NodeUnion,
        {},
        [("id", "Int64"), ("region", "String"), ("amount", "Float64")] + TRANSFORM_COLUMNS[3:],
        8,
    ),
    (
        9354,
        "unpivot",
        input_schema.NodeUnpivot,
        {"unpivot_input": transform_schema.UnpivotInput(index_columns=["id"], value_columns=["region", "tags"])},
        [("id", "Int64"), ("variable", "String"), ("value", "String")],
        12,
    ),
    (
        9355,
        "window_functions",
        input_schema.NodeWindowFunctions,
        {
            "window_input": transform_schema.WindowFunctionsInput(
                partition_by=["region"],
                order_by=[transform_schema.SortByInput(column="id")],
                window_functions=[
                    transform_schema.WindowFunctionInput(column="amount", function="cum_sum", new_column_name="running")
                ],
            )
        },
        TRANSFORM_COLUMNS + [("running", "Int64")],
        6,
    ),
    (
        9356,
        "text_to_rows",
        input_schema.NodeTextToRows,
        {"text_to_rows_input": transform_schema.TextToRowsInput(column_to_split="tags", output_column_name="tag")},
        TRANSFORM_COLUMNS + [("tag", "String")],
        9,
    ),
    (
        9357,
        "multi_field_formula",
        input_schema.NodeMultiFieldFormula,
        {
            "multi_field_formula_input": transform_schema.MultiFieldFormulaInput(
                formula="[_CurrentField_] * 2",
                selection_mode="list",
                selected_columns=["amount"],
                output_mode="new",
                output_suffix="_x2",
            )
        },
        TRANSFORM_COLUMNS + [("amount_x2", "Int64")],
        6,
    ),
    (
        9358,
        "graph_solver",
        input_schema.NodeGraphSolver,
        {"graph_solver_input": transform_schema.GraphSolverInput(col_from="parent", col_to="child")},
        TRANSFORM_COLUMNS + [("graph_group", "UInt64")],
        6,
    ),
    (
        9359,
        "explode_hierarchy",
        input_schema.NodeExplodeHierarchy,
        {
            "explode_hierarchy_input": transform_schema.ExplodeHierarchyInput(
                parent_column="parent", child_column="child"
            )
        },
        [
            ("ancestor", "String"),
            ("descendant", "String"),
            ("level", "UInt32"),
            ("quantity", "Float64"),
            ("is_leaf", "Boolean"),
        ],
        6,
    ),
]


@pytest.mark.parametrize(
    "flow_id, node_type, settings_class, fields, expected_columns, rows",
    TRANSFORMS,
    ids=[case[1] for case in TRANSFORMS],
)
def test_a_transform_runs_over_a_stored_result_core_cannot_read(
    unreadable, protocol_on, flow_id, node_type, settings_class, fields, expected_columns, rows
):
    graph = make_graph(flow_id, max_parallel_workers=1)
    try:
        inputs = [1, 3] if node_type == "union" else [1]
        add_manual_input(graph, 1, TRANSFORM_ROWS)
        if node_type == "union":
            add_manual_input(graph, 3, UNION_ROWS)
        for node_id in inputs:
            graph.get_node(node_id).node_settings.cache_results = True
        add_promise(graph, 2, node_type)
        sources = {"depending_on_ids": inputs} if node_type == "union" else {"depending_on_id": 1}
        getattr(graph, f"add_{node_type}")(settings_class(flow_id=graph.flow_id, node_id=2, **sources, **fields))
        for node_id in inputs:
            connect(graph, node_id, 2)
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        for node_id in inputs:
            ref = graph.get_node(node_id).results.result_ref
            assert ref.state == "materialised"
            assert not os.path.exists(ref.location.decode("utf-8"))
        assert columns(graph.get_node(2).schema) == expected_columns
        assert len(graph.get_node(2).get_table_example(True).data) == rows
    finally:
        graph.close_flow()


CLEANSING_ROWS = [{**row, "note": None} for row in TRANSFORM_ROWS] + [dict.fromkeys([*TRANSFORM_ROWS[0], "note"])]
SEGMENT_ROWS = [{"id": 1, "segment": "s1"}, {"id": 2, "segment": "s2"}]

TRANSFORM_VARIANTS = [
    (
        9360,
        "pivot",
        "pivot",
        input_schema.NodePivot,
        {
            "pivot_input": transform_schema.PivotInput(
                index_columns=["region"], pivot_column="parent", value_col="amount", aggregations=["sum"]
            )
        },
        [("region", "String"), ("p0", "Int64"), ("p1", "Int64"), ("p2", "Int64")],
        2,
    ),
    (
        9361,
        "pivot_without_index_columns",
        "pivot",
        input_schema.NodePivot,
        {
            "pivot_input": transform_schema.PivotInput(
                index_columns=[], pivot_column="region", value_col="amount", aggregations=["sum", "count"]
            )
        },
        [("eu_sum", "Int64"), ("eu_count", "UInt32"), ("us_sum", "Int64"), ("us_count", "UInt32")],
        1,
    ),
    (
        9362,
        "dynamic_rename_prefix",
        "dynamic_rename",
        input_schema.NodeDynamicRename,
        {"dynamic_rename_input": transform_schema.DynamicRenameInput(rename_mode="prefix", prefix="p_")},
        [(f"p_{name}", dtype) for name, dtype in TRANSFORM_COLUMNS],
        6,
    ),
    (
        9363,
        "dynamic_rename_first_row",
        "dynamic_rename",
        input_schema.NodeDynamicRename,
        {
            "dynamic_rename_input": transform_schema.DynamicRenameInput(
                rename_mode="first_row", selection_mode="list", selected_columns=["region", "child"]
            )
        },
        [
            ("id", "Int64"),
            ("eu", "String"),
            ("amount", "Int64"),
            ("parent", "String"),
            ("c1", "String"),
            ("tags", "String"),
        ],
        5,
    ),
    (
        9364,
        "data_cleansing",
        "data_cleansing",
        input_schema.NodeDataCleansing,
        {
            "cleansing_input": transform_schema.DataCleansingInput(
                remove_null_rows=True, remove_null_columns=True, case_mode="uppercase"
            )
        },
        TRANSFORM_COLUMNS,
        6,
    ),
    (
        9365,
        "cross_join",
        "cross_join",
        input_schema.NodeCrossJoin,
        {"cross_join_input": transform_schema.CrossJoinInput(left_select=[], right_select=[])},
        TRANSFORM_COLUMNS + [("id_right", "Int64"), ("segment", "String")],
        12,
    ),
]


@pytest.mark.parametrize(
    "flow_id, node_type, settings_class, fields, expected_columns, rows",
    [case[:1] + case[2:] for case in TRANSFORM_VARIANTS],
    ids=[case[1] for case in TRANSFORM_VARIANTS],
)
def test_a_transform_variant_runs_over_stored_results_core_cannot_read(
    unreadable, protocol_on, flow_id, node_type, settings_class, fields, expected_columns, rows
):
    graph = make_graph(flow_id, max_parallel_workers=1)
    try:
        inputs = {1: "main", 3: "right"} if node_type == "cross_join" else {1: "main"}
        add_manual_input(graph, 1, CLEANSING_ROWS if node_type == "data_cleansing" else TRANSFORM_ROWS)
        if node_type == "cross_join":
            add_manual_input(graph, 3, SEGMENT_ROWS)
        for node_id in inputs:
            graph.get_node(node_id).node_settings.cache_results = True
        add_promise(graph, 2, node_type)
        sources = {"depending_on_ids": list(inputs)} if node_type == "cross_join" else {"depending_on_id": 1}
        getattr(graph, f"add_{node_type}")(settings_class(flow_id=graph.flow_id, node_id=2, **sources, **fields))
        for node_id, input_type in inputs.items():
            connect(graph, node_id, 2, input_type=input_type)
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        for node_id in inputs:
            ref = graph.get_node(node_id).results.result_ref
            assert ref.state == "materialised"
            assert not os.path.exists(ref.location.decode("utf-8"))
        assert columns(graph.get_node(2).schema) == expected_columns
        assert len(graph.get_node(2).get_table_example(True).data) == rows
    finally:
        graph.close_flow()


API_ROWS = [{"id": 1, "name": "a", "city": "ams"}, {"id": 2, "name": "b", "city": "rtm"}]


class _Api(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_GET(self):  # noqa: N802
        body = json.dumps(API_ROWS).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def api_url():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Api)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/items"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_a_rest_api_reader_aligns_a_read_stored_where_core_cannot_read_it(unreadable, protocol_on, api_url):
    graph = make_graph(9366, max_parallel_workers=1)
    try:
        add_promise(graph, 1, "rest_api_reader")
        graph.add_rest_api_reader(
            input_schema.NodeRestApiReader(
                flow_id=graph.flow_id,
                node_id=1,
                user_id=1,
                rest_api_settings=input_schema.RestApiSettings(url=api_url),
                fields=[
                    input_schema.MinimalFieldInfo(name="name", data_type="String"),
                    input_schema.MinimalFieldInfo(name="id", data_type="Int64"),
                    input_schema.MinimalFieldInfo(name="missing", data_type="String"),
                ],
            )
        )
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        assert columns(graph.get_node(1).schema) == [
            ("name", "String"),
            ("id", "Int64"),
            ("missing", "String"),
            ("city", "String"),
        ]
        assert len(graph.get_node(1).get_table_example(True).data) == 2
    finally:
        graph.close_flow()


def test_a_pivot_without_index_columns_never_writes_to_its_settings(unreadable, protocol):
    pivot_input = transform_schema.PivotInput(
        index_columns=[], pivot_column="region", value_col="missing", aggregations=["sum"]
    )
    with pytest.raises(pl.exceptions.ColumnNotFoundError):
        FlowDataEngine(pl.LazyFrame(TRANSFORM_ROWS)).do_pivot(pivot_input)

    assert pivot_input.index_columns == []


def test_a_stored_results_columns_are_read_from_its_cached_schema(unreadable, protocol_on):
    graph = stored_inputs_flow(9348)
    try:
        assert graph.run_graph().success
        engine = graph.get_node(1).get_resulting_data()

        assert engine._fetch_null_profile() == (6, {"id": 0, "name": 0, "region": 0, "amount": 0})
        region = unique_values(unreadable[0], engine.data_frame.select("region"), 10, column="region")
        assert region.to_list() == ["eu", "us"]
        amount = next(column for column in engine.calculate_schema() if column.column_name == "amount")
        assert (amount.min_value, amount.max_value) == ("10.0", "60.0")
    finally:
        graph.close_flow()


def test_frame_helpers_given_a_schema_never_resolve_the_frame(tmp_path):
    gone = pl.scan_ipc(tmp_path / "gone.arrow")
    schema = pl.Schema({"parent": pl.String, "child": pl.String, "amount": pl.Int64})
    hierarchy = transform_schema.ExplodeHierarchyInput(parent_column="parent", child_column="child")
    entries = [FormulaEntry(1, "doubled", "[amount] * 2"), FormulaEntry(2, "quadrupled", "[doubled] * 2")]

    assert isinstance(explode_hierarchy_frame(gone, hierarchy, schema=schema), pl.LazyFrame)
    assert isinstance(apply_formula_entries(gone, entries, schema=schema), pl.LazyFrame)
    with pytest.raises(FormulaEntryError) as raised:
        apply_formula_entries(gone, [FormulaEntry(1, "doubled", "[amont] * 2")], schema=schema)
    assert raised.value.kind == "missing_column"
    assert raised.value.suggestion.to_name == "amount"


def test_a_filter_over_a_stored_v1_result_keeps_the_columns_the_worker_sent(remote_worker, protocol_on):
    graph = geometry_flow(9349)
    try:
        add_filter_on(graph, 4, 3, "[id] > 1")
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        assert WorkerLocation.model_validate_json(graph.get_node(3).results.result_ref.location).kind == "file"
        assert [(column.column_name, column.semantic_type) for column in graph.get_node(4).schema] == [
            ("id", None),
            ("g", "geometry"),
        ]
    finally:
        graph.close_flow()


READ_SITES_USER = 1


def read_sites_flow(flow_id: int) -> FlowGraph:
    """Manual input 1 → select 2, which caches its result → formula 3; random split 4 on 1; formula gate 5 on 1,
    controlled by 4's second output. The flow is open in the handler for ``READ_SITES_USER``, so the export route
    finds it.
    """
    graph = flow_file_handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name=f"read_sites_{flow_id}",
            path=".",
            execution_location="remote",
            execution_mode="Development",
            max_parallel_workers=1,
        ),
        user_id=READ_SITES_USER,
    )
    add_manual_input(graph, 1, [{"id": i, "name": f"n{i}", "amount": i * 10} for i in range(1, 5)])
    add_select_on(graph, 2, 1, [("id", "id"), ("name", "name"), ("amount", "amount")], False)
    graph.get_node(2).node_settings.cache_results = True
    add_promise(graph, 3, "formula")
    connect(graph, 2, 3)
    add_promise(graph, 4, "random_split")
    graph.add_random_split(
        input_schema.NodeRandomSplit(
            flow_id=flow_id,
            node_id=4,
            depending_on_id=1,
            seed=1,
            splits=[
                input_schema.RandomSplitGroup(name="train", percentage=50.0),
                input_schema.RandomSplitGroup(name="test", percentage=50.0),
            ],
        )
    )
    connect(graph, 1, 4)
    add_promise(graph, 5, "gate")
    connect(graph, 1, 5)
    add_connection(
        graph, input_schema.NodeConnection.create_from_simple_input(4, 5, input_type="right", output_handle="output-1")
    )
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=flow_id,
            node_id=5,
            depending_on_id=1,
            is_setup=True,
            gate_input=transform_schema.GateInput(condition_source="formula", formula="[amount] > 0"),
        )
    )
    return graph


@pytest.fixture
def read_sites(unreadable, protocol_on):
    """The read-sites flow, run once on the unreadable backend with the protocol flag on, and closed afterwards."""
    graph = read_sites_flow(9345)
    try:
        graph.run_graph()
        yield graph
    finally:
        flow_file_handler.delete_flow(9345)


def export_lines(graph: FlowGraph, node_id: int, limit: int) -> list[str]:
    """The lines of the CSV export of ``node_id``'s result; the file is removed, as serving the response would."""
    response = export_node_data(
        flow_id=graph.flow_id,
        node_id=node_id,
        output_handle=DEFAULT_OUTPUT_HANDLE,
        file_format="csv",
        limit=limit,
        current_user=SimpleNamespace(id=READ_SITES_USER),
    )
    try:
        return Path(response.path).read_text().splitlines()
    finally:
        os.remove(response.path)


def test_the_formula_editor_reads_its_input_row_through_the_backend(read_sites):
    select = read_sites.get_node(2)
    assert not os.path.exists(select.results.result_ref.location.decode("utf-8"))

    assert get_instant_func_results(read_sites.get_node(3), "[name]").result == "n1"
    select.results.example_data_generator = None
    assert get_instant_func_results(read_sites.get_node(3), "[name]").result == "n1"
    second_split = get_instant_func_results(read_sites.get_node(5), "[amount]")
    assert second_split.success and int(second_split.result) in {10, 20, 30, 40}


def test_a_formula_gate_probes_its_control_input_through_the_backend(read_sites):
    assert read_sites.latest_run_info.success, failures(read_sites.latest_run_info)


def test_column_statistics_take_the_dtype_from_the_cached_schema(read_sites):
    stats = read_sites.get_node(2).get_column_stats("amount")

    assert (stats.number_of_filled_values, stats.min_value, stats.max_value) == (4, "10", "40")


def test_the_csv_export_reads_its_rows_through_the_backend(read_sites):
    assert export_lines(read_sites, 2, 3) == ['"id","name","amount"', '"1","n1","10"', '"2","n2","20"', '"3","n3","30"']


def test_rows_of_v1_results_come_over_the_wire(remote_worker, protocol_on, monkeypatch):
    ops = []
    run_op = WorkerBackend.run_op

    def recording(self, op, payload):
        ops.append(op.value)
        return run_op(self, op, payload)

    monkeypatch.setattr(WorkerBackend, "run_op", recording)
    graph = read_sites_flow(9346)
    try:
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        select = graph.get_node(2)
        assert WorkerLocation.model_validate_json(select.results.result_ref.location).kind == "file"
        assert get_instant_func_results(graph.get_node(3), "[name]").result == "n1"
        select.results.example_data_generator = None
        assert get_instant_func_results(graph.get_node(3), "[name]").result == "n1"
        assert select.get_column_stats("amount").max_value == "40"
        assert export_lines(graph, 2, 10_000)[1:] == [f'"{i}","n{i}","{i * 10}"' for i in range(1, 5)]
        assert ops == ["random_split"]
    finally:
        flow_file_handler.delete_flow(9346)
