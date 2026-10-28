"""A flow over results stored where core cannot read them runs, previews and predicts from their references.

An unreadable location is a stored result's file that exists only while a call of the backend that holds it runs:
core keeps the reference and composes on the ``scan_ipc`` leaf ``plan_for`` returns, as it does for a worker on another
host, but any read of that leaf from core raises ``FileNotFoundError``. A touch fails the run or the test outright
instead of being counted, so these flows pass only while core takes a stored result's schema from its reference.
They run with the protocol flag off and on: with it on, core asks the backend that holds a result whether it still
does, and reads rows of those results through the backend: the formula editor's preview row, a formula gate's probe,
column statistics, the CSV export, a Run Flow node's parameter rows and the preview of each output of a random split
or a custom node; and a filter, a select, a join, a group_by, a
formula, a record count, a unique, a union, an unpivot, window functions, text to rows, a multi-field formula, a graph
solver, an explode hierarchy, a pivot, a dynamic rename, a data cleansing or a cross join over a stored result, a
node's output field config, a Run Flow node's outputs over its child's stored results and its data input and metadata
columns over stored inputs, or the REST API reader's alignment of a read it stored, takes its input's columns from the
input engine's cached schema; a Polars code node over a stored result is the ``polars_code`` operation, its code run
once on the backend that holds the input and never in core, eager code and a side effect included, and the designer's
prediction for it is the same operation over an empty frame; a SQL query node is likewise the ``sql_query``
operation, its query translated once on the backend that holds the inputs and never in core, the designer's prediction
included; and a node that fails on a missing file in core keeps its stored input rather than rerunning it. The
elaborate integration flow runs whole over stored inputs with the flag on, and with it off fails at the nodes that read
them in core. The tests named for ``v1`` results check the same over the session worker's ``/v1`` routes, where a
multi-output node's other output previews from the result the node holds for it. Two backends hold what they store
where core cannot read it: ``UnreadableBackend``, the worker embedded in this process, whose stored files sit at a
path that exists only while one of its calls runs, and ``UnreadableWorker``, the session worker as core sees one on
another host, whose stored files exist only while one of its requests runs; a fuzzy match and a SQL query run over a
stored input on the second.
"""

import dataclasses
import io
import json
import os
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from flowfile_core import flow_file_handler
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
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
from flowfile_core.flowfile.flow_data_engine.polars_code_parser import polars_code_parser
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.flow_node.executor import NodeExecutor
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.flow_node.models import InvalidationReason
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.flowfile.user_defined.registry import registry as node_registry
from flowfile_core.routes.routes import export_node_data
from flowfile_core.schemas import input_schema, schemas, transform_schema
from shared.compute_protocol import WorkerLocation
from tests.conftest import protocol_states
from tests.flowfile.execution.conftest import serve
from tests.flowfile.execution.graph_helpers import (
    add_filter_on,
    add_manual_input,
    add_promise,
    add_select_on,
    code_flow,
    connect,
    make_graph,
)
from tests.flowfile.test_flowfile_integration import build_complex_elaborate_flow

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


class UnreadableBackend(EmbeddedWorkerBackend):
    """The worker embedded in this process, except that a result it stores sits at a path that exists only while one
    of its calls runs.

    The worker holds a stored result's file under core's cache directory; the leaf ``plan_for`` answers for it here is
    ``scan_ipc`` of ``<root>/<result id>.arrow`` instead, a path that exists only while ``submit``, ``run_op``,
    ``sample``, ``count`` or ``export`` runs: ``_readable`` links every stored result there for the call and removes
    the links before it returns, so the plans core composes on those leaves run on this backend and any read of such a
    leaf from core raises ``FileNotFoundError``. Every reference this backend mints passes through ``_ref_from_wire``,
    which notes where the worker wrote a stored result.

    Like a worker, it runs a copy of each plan core sends (``detached``) and hands core a copy of each deferred plan
    it holds: Polars keeps what it resolves of a scan on the plan object, so a backend that resolved core's own plans
    while their files existed would answer core's later reads of the same leaves without touching the files.
    """

    def __init__(self, root: Path):
        self.id = BACKEND_ID
        self.root = root
        self._stored: dict[str, str] = {}
        self._lock = threading.Lock()

    def submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        with self._readable():
            return super().submit(detached(plan), inputs, hints, result_id, flow_id=flow_id, node_id=node_id)

    def run_op(self, op, payload):
        with self._readable():
            return super().run_op(op, {key: detached_payload(value) for key, value in payload.items()})

    def sample(self, ref, n):
        with self._readable():
            return super().sample(ref, n)

    def count(self, ref):
        with self._readable():
            return super().count(ref)

    def export(self, ref, uri, credential):
        with self._readable():
            return super().export(ref, uri, credential)

    def plan_for(self, ref):
        if ref.state == "deferred":
            return detached(super().plan_for(ref))
        self._table(ref)
        return pl.scan_ipc(self.root / f"{ref.result_id}.arrow")

    def release(self, ref):
        super().release(ref)
        with self._lock:
            self._stored.pop(ref.result_id, None)

    def _ref_from_wire(self, ref):
        made = super()._ref_from_wire(ref)
        held = WorkerLocation.model_validate_json(made.location)
        if held.kind == "file":
            with self._lock:
                self._stored[made.result_id] = held.path
        return made

    @contextmanager
    def _readable(self):
        """Every stored result is at its unreadable path while the block runs, and gone from it after."""
        with self._lock:
            stored = dict(self._stored)
        self.root.mkdir(parents=True, exist_ok=True)
        linked = []
        for result_id, path in stored.items():
            link = self.root / f"{result_id}.arrow"
            if os.path.isfile(path) and not link.exists():
                os.symlink(path, link)
                linked.append(link)
        try:
            yield
        finally:
            for link in linked:
                link.unlink(missing_ok=True)


protocol = protocol_states("flag_off", "flag_on")


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
    add_filter_on(graph, 7, 1, '[region] = "eu"', split_mode=True)
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


def test_a_flow_runs_previews_and_predicts_over_stored_results_core_cannot_read(unreadable, protocol_on):
    """With the flag on only: the worker stores a previewed manual input, whose leaf the flag-off path resolves in
    core."""
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


SPLIT_NODE_SOURCE = """
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase


class UnreadableSplitNode(CustomNodeBase):
    node_name: str = "Unreadable Split Node"
    number_of_outputs: int = 2
    output_names: list[str] = ["big", "small"]

    def process(self, *inputs: pl.LazyFrame):
        lf = inputs[0]
        return {"big": lf.filter(pl.col("amount") > 20), "small": lf.filter(pl.col("amount") <= 20)}
"""


@pytest.fixture
def split_node(tmp_path):
    """A custom node with two outputs loaded from a node file, so it runs as the ``custom_node`` operation."""
    path = tmp_path / "unreadable_split_node.py"
    path.write_text(SPLIT_NODE_SOURCE)
    entry = node_registry.load_file(path)
    assert entry.error is None, entry.error
    yield entry
    node_registry.remove_file(entry.file_path)


def multi_output_flow(flow_id: int, node_type: str, split_node) -> FlowGraph:
    """Manual input 1 of six customers → node 2, a random split or ``split_node``, whose outputs the backend stores."""
    graph = make_graph(flow_id, max_parallel_workers=1)
    customers = [{"id": i, "name": f"n{i}", "region": "eu" if i % 2 else "us", "amount": i * 10} for i in range(1, 7)]
    add_manual_input(graph, 1, customers)
    if node_type == "random_split":
        add_promise(graph, 2, "random_split")
        graph.add_random_split(
            input_schema.NodeRandomSplit(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                seed=1,
                splits=[
                    input_schema.RandomSplitGroup(name="train", percentage=50.0),
                    input_schema.RandomSplitGroup(name="test", percentage=50.0),
                ],
            )
        )
    else:
        graph.add_node_promise(
            input_schema.NodePromise(
                flow_id=graph.flow_id, node_id=2, node_type=split_node.node_key, is_user_defined=True
            )
        )
        graph.add_user_defined_node(
            custom_node=split_node.node_class.from_settings({}),
            user_defined_node_settings=input_schema.UserDefinedNode(
                flow_id=graph.flow_id, node_id=2, settings={}, is_user_defined=True, user_id=1
            ),
        )
    connect(graph, 1, 2)
    return graph


@pytest.mark.parametrize(
    "flow_id, node_type", [(9367, "random_split"), (9368, "custom_node")], ids=["random_split", "custom_node"]
)
def test_a_multi_output_node_previews_each_output_stored_where_core_cannot_read(
    unreadable, protocol, split_node, flow_id, node_type
):
    graph = multi_output_flow(flow_id, node_type, split_node)
    try:
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        node = graph.get_node(2)
        handles = [DEFAULT_OUTPUT_HANDLE, "output-1"]
        if not protocol:
            for handle in handles:
                with pytest.raises(FileNotFoundError):
                    node.get_table_example(True, output_handle=handle)
        else:
            previews = {handle: node.get_table_example(True, output_handle=handle) for handle in handles}
            node.results.example_data_generator = None
            read_again = node.get_table_example(True, output_handle=DEFAULT_OUTPUT_HANDLE)

            ids = {handle: [row["id"] for row in preview.data] for handle, preview in previews.items()}
            counts = {handle: preview.number_of_records for handle, preview in previews.items()}
            if node_type == "random_split":
                assert counts == {DEFAULT_OUTPUT_HANDLE: 3, "output-1": 3}
                assert sorted(ids[DEFAULT_OUTPUT_HANDLE] + ids["output-1"]) == [1, 2, 3, 4, 5, 6]
            else:
                assert ids == {DEFAULT_OUTPUT_HANDLE: [3, 4, 5, 6], "output-1": [1, 2]}
                assert counts == {DEFAULT_OUTPUT_HANDLE: 4, "output-1": 2}
            assert read_again.data == previews[DEFAULT_OUTPUT_HANDLE].data
    finally:
        graph.close_flow()


@pytest.mark.parametrize(
    "flow_id, node_type", [(9369, "random_split"), (9370, "custom_node")], ids=["random_split", "custom_node"]
)
def test_a_v1_multi_output_node_previews_its_other_output_from_the_result_it_holds(
    remote_worker, protocol_on, split_node, monkeypatch, flow_id, node_type
):
    calls: list[tuple[str, str]] = []
    real_sample, real_submit = WorkerBackend.sample, WorkerBackend.submit

    def recording_sample(self, ref, n):
        calls.append(("sample", ref.result_id))
        return real_sample(self, ref, n)

    def recording_submit(self, plan, inputs, hints, result_id, **kwargs):
        calls.append(("submit", result_id))
        return real_submit(self, plan, inputs, hints, result_id, **kwargs)

    monkeypatch.setattr(WorkerBackend, "sample", recording_sample)
    monkeypatch.setattr(WorkerBackend, "submit", recording_submit)
    graph = multi_output_flow(flow_id, node_type, split_node)
    try:
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        node = graph.get_node(2)
        ref = node.held_output("output-1")
        assert (ref.backend_id, ref.state) == (registry.WORKER_BACKEND_ID, "materialised")
        calls.clear()

        preview = node.get_table_example(True, output_handle="output-1")

        assert calls == [("sample", ref.result_id)]
        worker = registry.backend(ref.backend_id)
        assert preview.data == pl.read_ipc_stream(worker.sample(ref, 100)).to_dicts()
        ids = [row["id"] for row in preview.data]
        if node_type == "random_split":
            assert preview.number_of_records == 3 and len(ids) == 3
        else:
            assert (ids, preview.number_of_records) == ([1, 2], 2)
    finally:
        graph.close_flow()


def test_a_result_its_backend_dropped_is_run_again(unreadable, protocol_on):
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


def test_a_missing_file_in_core_reruns_a_held_input_only_with_the_flag_off(unreadable, protocol, monkeypatch, tmp_path):
    submits, releases = [], []
    submit, release = UnreadableBackend.submit, UnreadableBackend.release

    def recording_submit(self, plan, inputs, hints, result_id, *, flow_id, node_id):
        submits.append((node_id, result_id))
        return submit(self, plan, inputs, hints, result_id, flow_id=flow_id, node_id=node_id)

    def recording_release(self, ref):
        if not ref.result_id.startswith("handshake-"):
            releases.append(ref.result_id)
        return release(self, ref)

    monkeypatch.setattr(UnreadableBackend, "submit", recording_submit)
    monkeypatch.setattr(UnreadableBackend, "release", recording_release)
    missing = tmp_path / "missing.parquet"
    graph = make_graph(9380, max_parallel_workers=1)
    add_manual_input(graph, 1, [{"id": i, "amount": i * 10} for i in range(1, 4)])
    graph.get_node(1).node_settings.cache_results = True
    add_promise(graph, 2, "polars_code")
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_ids=[1],
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code=f'input_df.join(pl.read_parquet("{missing.as_posix()}").lazy(), how="cross")'
            ),
        )
    )
    connect(graph, 1, 2)
    try:
        run_info = graph.run_graph()

        ref = graph.get_node(1).results.result_ref
        assert [node_id for node_id, _ in failures(run_info)] == [2]
        assert "(os error 2)" in failures(run_info)[0][1]
        assert unreadable[0].exists(ref)
        input_submits = [result_id for node_id, result_id in submits if node_id == 1]
        if protocol:
            assert releases == []
            assert input_submits == [ref.result_id]
        else:
            assert releases == [ref.result_id]
            assert input_submits == [ref.result_id, ref.result_id]
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
    (9351, "record_count", input_schema.NodeRecordCount, {}, [("number_of_records", "UInt64")], 1),
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
        [("eu_sum", "Int64"), ("eu_count", "UInt64"), ("us_sum", "Int64"), ("us_count", "UInt64")],
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
    (
        9370,
        "polars_code",
        "polars_code",
        input_schema.NodePolarsCode,
        {
            "polars_code_input": transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.filter(pl.col('amount') > 20).select('id', 'region', 'amount')"
            )
        },
        [("id", "Int64"), ("region", "String"), ("amount", "Int64")],
        4,
    ),
    (
        9388,
        "polars_code_eager",
        "polars_code",
        input_schema.NodePolarsCode,
        {
            "polars_code_input": transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.filter(pl.col('amount') >= input_df.collect().item(2, 'amount'))"
                ".with_columns(top=pl.lit(input_df.select(pl.col('amount').max()).collect().item()))"
                ".select('id', 'amount', 'top')"
            )
        },
        [("id", "Int64"), ("amount", "Int64"), ("top", "Int32")],
        4,
    ),
    (
        9393,
        "sql_query",
        "sql_query",
        input_schema.NodeSqlQuery,
        {
            "sql_query_input": transform_schema.SqlQueryInput(
                sql_code="SELECT id, region, amount FROM input_1 WHERE amount > 20"
            )
        },
        [("id", "Int64"), ("region", "String"), ("amount", "Int64")],
        4,
    ),
    (
        9394,
        "sql_query_aggregate",
        "sql_query",
        input_schema.NodeSqlQuery,
        {
            "sql_query_input": transform_schema.SqlQueryInput(
                sql_code="SELECT region, SUM(amount) AS total FROM input_1 GROUP BY region ORDER BY region"
            )
        },
        [("region", "String"), ("total", "Int64")],
        2,
    ),
    (
        9372,
        "output_fields_raise_on_missing",
        "filter",
        input_schema.NodeFilter,
        {
            "filter_input": transform_schema.FilterInput(mode="advanced", advanced_filter="[amount] > 20"),
            "output_field_config": input_schema.OutputFieldConfig(
                enabled=True,
                validation_mode_behavior="raise_on_missing",
                fields=[
                    input_schema.OutputFieldInfo(name="amount", data_type="Int64"),
                    input_schema.OutputFieldInfo(name="id", data_type="Int64"),
                ],
            ),
        },
        [("amount", "Int64"), ("id", "Int64")],
        4,
    ),
    (
        9373,
        "output_fields_add_missing",
        "filter",
        input_schema.NodeFilter,
        {
            "filter_input": transform_schema.FilterInput(mode="advanced", advanced_filter="[amount] > 20"),
            "output_field_config": input_schema.OutputFieldConfig(
                enabled=True,
                validation_mode_behavior="add_missing",
                fields=[
                    input_schema.OutputFieldInfo(name="id", data_type="Int64"),
                    input_schema.OutputFieldInfo(name="status", data_type="String", default_value="new"),
                ],
            ),
        },
        [("id", "Int64"), ("status", "String")],
        4,
    ),
    (
        9374,
        "output_fields_add_missing_keep_extra",
        "filter",
        input_schema.NodeFilter,
        {
            "filter_input": transform_schema.FilterInput(mode="advanced", advanced_filter="[amount] > 20"),
            "output_field_config": input_schema.OutputFieldConfig(
                enabled=True,
                validation_mode_behavior="add_missing_keep_extra",
                fields=[
                    input_schema.OutputFieldInfo(name="status", data_type="String", default_value="new"),
                    input_schema.OutputFieldInfo(name="id", data_type="Int64"),
                ],
            ),
        },
        [("status", "String"), ("id", "Int64")] + TRANSFORM_COLUMNS[1:],
        4,
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
        multi_input = issubclass(settings_class, input_schema.NodeMultiInput)
        sources = {"depending_on_ids": list(inputs)} if multi_input else {"depending_on_id": 1}
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


LAZY_POLARS_CODE = "output_df = input_df.filter(pl.col('amount') > 20).select('id', 'region', 'amount')"


def executed_in_core(*args, **kwargs):
    pytest.fail("core executed the Polars code")


def side_effect_flow(flow_id: int, written: Path) -> FlowGraph:
    """Manual input 1, cached → Polars code 2, which writes its collected input to a new CSV file in ``written``."""
    code = (
        "rows = input_df.collect()\n"
        f'rows.write_csv(f"{written.as_posix()}/{{time.time_ns()}}.csv")\n'
        "output_df = rows.lazy().select('id', 'amount')"
    )
    graph = code_flow(flow_id, "polars_code", code, TRANSFORM_ROWS, max_parallel_workers=1)
    graph.get_node(1).node_settings.cache_results = True
    return graph


SQL_QUERY = "SELECT id, region, amount FROM input_1 WHERE amount > 20"
SQL_AGGREGATE = "SELECT region, SUM(amount) AS total FROM input_1 GROUP BY region ORDER BY region"


def translated_in_core(*args, **kwargs):
    pytest.fail("core translated the SQL")


def sql_query_flow(flow_id: int, sql_code: str) -> FlowGraph:
    """Manual input 1 → SQL query 2 running ``sql_code`` over it as ``input_1``; neither has run."""
    return code_flow(flow_id, "sql_query", sql_code, TRANSFORM_ROWS, max_parallel_workers=1)


def record_run_ops(monkeypatch, backend_class) -> list[SimpleNamespace]:
    """Every ``run_op`` call on ``backend_class`` from here on, as its ``kind``, ``payload`` and ``result``."""
    calls: list[SimpleNamespace] = []
    run_op = backend_class.run_op

    def recording(self, op, payload):
        call = SimpleNamespace(kind=op.value, payload=payload.get("payload"), result=None)
        calls.append(call)
        call.result = run_op(self, op, payload)
        return call.result

    monkeypatch.setattr(backend_class, "run_op", recording)
    return calls


def written_lines(written: Path) -> list[int]:
    """The number of lines of each CSV file in ``written``."""
    return [len(path.read_text().splitlines()) for path in written.glob("*.csv")]


ON_THE_BACKEND = pytest.mark.parametrize("backend", ["unreadable", "remote_worker"], ids=["backend", "worker"])
"""A case on the ``UnreadableBackend``, then on the session worker, where core's own run of the code is refused too."""

CORE_RUNS = {
    "polars_code": (polars_code_parser, "get_executable", executed_in_core),
    "sql_query": (pl.SQLContext, "execute", translated_in_core),
}
"""What core would call to run each kind of user code itself, with the stand-in that fails the test if it does."""


def on_backend(request, monkeypatch, backend: str, kind: str) -> list[SimpleNamespace]:
    """Sets up ``backend`` and answers the ``run_op`` calls it gets; on the worker, core running ``kind`` fails."""
    request.getfixturevalue(backend)
    if backend == "unreadable":
        return record_run_ops(monkeypatch, UnreadableBackend)
    monkeypatch.setattr(*CORE_RUNS[kind])
    return record_run_ops(monkeypatch, WorkerBackend)


@ON_THE_BACKEND
def test_polars_code_with_a_side_effect_runs_it_once_on_its_backend(
    request, backend, protocol_on, monkeypatch, tmp_path
):
    calls = on_backend(request, monkeypatch, backend, "polars_code")
    written = tmp_path / "written"
    written.mkdir()
    graph = side_effect_flow(9389 if backend == "unreadable" else 9390, written)
    try:
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        if backend == "remote_worker":
            assert [call.kind for call in calls] == ["polars_code"]
        assert written_lines(written) == [7]
        assert columns(graph.get_node(2).schema) == [("id", "Int64"), ("amount", "Int64")]
        assert len(graph.get_node(2).get_table_example(True).data) == 6
    finally:
        graph.close_flow()


@ON_THE_BACKEND
@pytest.mark.parametrize(
    "kind, code, flow_id",
    [("polars_code", LAZY_POLARS_CODE, 9391), ("sql_query", SQL_QUERY, 9396)],
    ids=["polars_code", "sql_query"],
)
def test_the_designers_prediction_of_a_user_code_node_is_the_operation_over_an_empty_frame(
    request, backend, kind, code, flow_id, protocol_on, monkeypatch
):
    """On the worker the prediction reads an empty frame in memory, so the worker holds its result as a file of no
    rows."""
    calls = on_backend(request, monkeypatch, backend, kind)
    graph = code_flow(flow_id + (backend == "remote_worker"), kind, code, TRANSFORM_ROWS, max_parallel_workers=1)
    try:
        predicted = graph.get_node(2).get_predicted_schema()

        assert columns(predicted) == [("id", "Int64"), ("region", "String"), ("amount", "Int64")]
        (call,) = calls
        assert call.kind == kind
        assert call.payload.inputs[0].select(pl.len()).collect().item() == 0
        assert call.result.refs["result"].row_count == 0
        if backend == "unreadable":
            assert [graph.get_node(node_id).results.result_ref for node_id in (1, 2)] == [None, None]
        else:
            assert call.result.refs["result"].state == "materialised"
    finally:
        graph.close_flow()


@pytest.mark.parametrize(
    "flow_id, iteration_mode, rows", [(9375, "first_value", 6), (9376, "iterate", 12)], ids=["first_value", "iterate"]
)
def test_a_run_flow_reads_its_child_output_stored_where_core_cannot_read(
    unreadable, protocol_on, tmp_path, flow_id, iteration_mode, rows
):
    child = make_graph(9379)
    child.flow_settings.parameters = [schemas.FlowParameter(name="limit", default_value="10", type="integer")]
    add_promise(child, 1, "flow_input")
    child.add_flow_input(
        input_schema.NodeFlowInput(
            flow_id=child.flow_id,
            node_id=1,
            input_name="a",
            raw_data_format=input_schema.RawData.from_pylist(TRANSFORM_ROWS),
            cache_results=True,
        )
    )
    add_promise(child, 2, "flow_output")
    child.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=child.flow_id, node_id=2, output_name="out_a", depending_on_id=1)
    )
    connect(child, 1, 2)
    path = tmp_path / "child_out_a.yaml"
    child.save_flow(str(path))
    child.close_flow()
    with get_db_context() as db:
        registration = db_models.FlowRegistration(
            flow_uuid=str(uuid.uuid4()), name=path.stem, flow_path=str(path), owner_id=1
        )
        db.add(registration)
        db.commit()
        db.refresh(registration)
        registration_id = registration.id

    iterate = iteration_mode == "iterate"
    graph = make_graph(flow_id, max_parallel_workers=1)
    try:
        if iterate:
            add_manual_input(graph, 1, [{"limit": 1}, {"limit": 2}])
        add_promise(graph, 2, "run_flow")
        bindings = [input_schema.RunFlowParameterBinding(parameter_name="limit", source="column", column_name="limit")]
        graph.add_run_flow(
            input_schema.NodeRunFlow(
                flow_id=graph.flow_id,
                node_id=2,
                user_id=1,
                flow_reference=input_schema.SubflowReference(registration_id=registration_id),
                input_slots=["a"],
                output_slots=["out_a"],
                parameter_bindings=bindings if iterate else [],
                iteration_mode=iteration_mode,
                append_run_metadata=False,
            )
        )
        if iterate:
            add_connection(
                graph,
                input_schema.NodeConnection(
                    input_connection=input_schema.NodeInputConnection(node_id=2, connection_class="input-0"),
                    output_connection=input_schema.NodeOutputConnection(node_id=1, connection_class="output-0"),
                ),
            )
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        assert columns(graph.get_node(2).schema) == TRANSFORM_COLUMNS
        assert unreadable[0].count(graph.get_node(2).results.result_ref) == rows
    finally:
        graph.close_flow()


def registered_child(tmp_path: Path, flow_id: int, cached: bool) -> int:
    """The registration id of a saved child: parameter ``limit``, flow_input ``a`` of ``TRANSFORM_ROWS``, ``out_a``."""
    child = make_graph(flow_id)
    child.flow_settings.parameters = [schemas.FlowParameter(name="limit", default_value="10", type="integer")]
    add_promise(child, 1, "flow_input")
    child.add_flow_input(
        input_schema.NodeFlowInput(
            flow_id=child.flow_id,
            node_id=1,
            input_name="a",
            raw_data_format=input_schema.RawData.from_pylist(TRANSFORM_ROWS),
            cache_results=cached,
        )
    )
    add_promise(child, 2, "flow_output")
    child.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=child.flow_id, node_id=2, output_name="out_a", depending_on_id=1)
    )
    connect(child, 1, 2)
    path = tmp_path / "child_out_a.yaml"
    child.save_flow(str(path))
    child.close_flow()
    with get_db_context() as db:
        registration = db_models.FlowRegistration(
            flow_uuid=str(uuid.uuid4()), name=path.stem, flow_path=str(path), owner_id=1
        )
        db.add(registration)
        db.commit()
        db.refresh(registration)
        return registration.id


PARAMS = [{"limit": 1}, {"limit": 2}]
METADATA_COLUMNS = [("param_limit", "Int64"), ("run_index", "UInt32")]


@pytest.mark.parametrize(
    "flow_id, rows_in, input_cached, handle, child_cached, iteration_mode, metadata, expected_columns, rows",
    [
        (9384, TRANSFORM_ROWS, True, "input-1", False, "first_value", False, TRANSFORM_COLUMNS, 6),
        (9385, PARAMS, False, "input-0", True, "iterate", True, TRANSFORM_COLUMNS + METADATA_COLUMNS, 12),
        (9386, PARAMS, True, "input-0", False, "iterate", False, TRANSFORM_COLUMNS, 12),
    ],
    ids=["stored_input", "append_run_metadata", "parameter_input"],
)
def test_a_run_flow_node_runs_over_inputs_stored_where_core_cannot_read(
    unreadable,
    protocol,
    tmp_path,
    flow_id,
    rows_in,
    input_cached,
    handle,
    child_cached,
    iteration_mode,
    metadata,
    expected_columns,
    rows,
):
    registration_id = registered_child(tmp_path, 9387, child_cached)
    iterate = iteration_mode == "iterate"
    graph = make_graph(flow_id, max_parallel_workers=1)
    try:
        add_manual_input(graph, 1, rows_in)
        graph.get_node(1).node_settings.cache_results = input_cached
        add_promise(graph, 2, "run_flow")
        bindings = [input_schema.RunFlowParameterBinding(parameter_name="limit", source="column", column_name="limit")]
        graph.add_run_flow(
            input_schema.NodeRunFlow(
                flow_id=graph.flow_id,
                node_id=2,
                user_id=1,
                flow_reference=input_schema.SubflowReference(registration_id=registration_id),
                input_slots=["a"],
                output_slots=["out_a"],
                parameter_bindings=bindings if iterate else [],
                iteration_mode=iteration_mode,
                append_run_metadata=metadata,
            )
        )
        connect(graph, 1, 2, handle)

        run_info = graph.run_graph()

        if protocol:
            assert run_info.success, failures(run_info)
            assert columns(graph.get_node(2).schema) == expected_columns
            assert unreadable[0].count(graph.get_node(2).results.result_ref) == rows
        else:
            assert [node_id for node_id, _ in failures(run_info)] == [2]
            assert "(os error 2)" in failures(run_info)[0][1]
    finally:
        graph.close_flow()


ELABORATE_INPUTS = (1, 2, 3, 4, 5, 27)
ELABORATE_OUTPUTS = {
    71: "customer_sales_analysis.csv",
    72: "fuzzy_matched_customers.parquet",
    73: "network_components.csv",
    74: "combined_aggregations.csv",
}


def test_the_elaborate_integration_flow_runs_over_stored_inputs_core_cannot_read(unreadable, protocol, tmp_path):
    graph = build_complex_elaborate_flow(tmp_path)
    graph.flow_settings.execution_location = "remote"
    graph.flow_settings.max_parallel_workers = 1
    for node_id in ELABORATE_INPUTS:
        graph.get_node(node_id).node_settings.cache_results = True
    try:
        run_info = graph.run_graph()

        for node_id in ELABORATE_INPUTS:
            ref = graph.get_node(node_id).results.result_ref
            assert ref.state == "materialised"
            assert not os.path.exists(ref.location.decode("utf-8"))
        if protocol:
            assert run_info.success, failures(run_info)
            assert run_info.nodes_completed == len(graph.nodes) == 30
            rows = {}
            for node_id, name in ELABORATE_OUTPUTS.items():
                path = tmp_path / name
                frame = pl.read_parquet(path) if path.suffix == ".parquet" else pl.read_csv(path)
                assert frame.columns == [column.column_name for column in graph.get_node(node_id).schema]
                rows[name] = frame.height
            assert rows["fuzzy_matched_customers.parquet"] == 3
            assert rows["network_components.csv"] == 5
            assert rows["customer_sales_analysis.csv"] > 0 and rows["combined_aggregations.csv"] > 0
        else:
            assert not run_info.success
            assert {node_id for node_id, _ in failures(run_info)} == {11, 17, 26, 28, 30, 31}
            assert all("(os error 2)" in error for _, error in failures(run_info))
    finally:
        graph.close_flow()


UNREADABLE_WORKER_ID = "unreadable_worker"


class UnreadableWorker(WorkerBackend):
    """The session worker as core sees a worker on another host, whose stored files exist only during a request.

    The file of a result the worker stores is at its path only while one of this backend's requests to the worker
    runs, so the worker stores, reads, samples and releases as always, and a read of a stored result in core outside
    a request raises ``FileNotFoundError``. The window is the request, not the backend call as in
    ``UnreadableBackend``, because a call such as ``run_op`` prepares its payload in core before it sends it.
    """

    def __init__(self, url: str):
        super().__init__(url, backend_id=UNREADABLE_WORKER_ID)
        self.stored: list[str] = []

    def put_back(self) -> None:
        """Every stored file this backend moved aside back at its path."""
        for path in self.stored:
            if os.path.exists(path + ".away"):
                os.replace(path + ".away", path)

    def _ref_from_wire(self, ref):
        """The parent's reference; the file it names is moved aside, since the request that answered it has returned."""
        result = super()._ref_from_wire(ref)
        location = WorkerLocation.model_validate_json(result.location)
        if location.kind == "file":
            self.stored.append(location.path)
            if os.path.exists(location.path):
                os.replace(location.path, location.path + ".away")
        return result

    def _v1(self, method, path, **kwargs):
        with self._requesting():
            return super()._v1(method, path, **kwargs)

    def _v1_socket_submit(self, meta, plan, result_id):
        with self._requesting():
            return super()._v1_socket_submit(meta, plan, result_id)

    @contextmanager
    def _requesting(self):
        """Every stored file is at its path while the block runs; one the worker released stays gone."""
        self.put_back()
        try:
            yield
        finally:
            for path in self.stored:
                if os.path.exists(path):
                    os.replace(path, path + ".away")


@pytest.fixture
def unreadable_worker(remote_worker, monkeypatch) -> list[UnreadableWorker]:
    """``unreadable_worker`` on the session worker is registered and is the default; the list holds each instance made.

    Each instance puts back the files it moved aside when the test ends, so none is left aside in the worker's storage.
    """
    made: list[UnreadableWorker] = []
    registry.reset()
    url = registry.worker_url()

    def make_unreadable_worker() -> UnreadableWorker:
        made.append(UnreadableWorker(url))
        return made[-1]

    registry.register_backend(UNREADABLE_WORKER_ID, make_unreadable_worker)
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", UNREADABLE_WORKER_ID)
    yield made
    for worker in made:
        worker.put_back()
    registry.reset()


def test_a_fuzzy_match_runs_on_a_worker_over_a_stored_input_core_cannot_read(unreadable_worker, protocol_on):
    graph = make_graph(9399, max_parallel_workers=1)
    add_manual_input(graph, 1, [{"name": "eduward"}, {"name": "edward"}, {"name": "courtney"}])
    graph.get_node(1).node_settings.cache_results = True
    add_promise(graph, 2, "fuzzy_match")
    graph.add_fuzzy_match(
        input_schema.NodeFuzzyMatch(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_ids=[1, 1],
            join_input=transform_schema.FuzzyMatchInput(
                join_mapping=[
                    transform_schema.FuzzyMap(
                        left_col="name", right_col="name", threshold_score=75, fuzzy_type="levenshtein"
                    )
                ],
                left_select=[transform_schema.SelectInput("name", join_key=True)],
                right_select=[transform_schema.SelectInput("name", join_key=True)],
                how="inner",
            ),
        )
    )
    connect(graph, 1, 2)
    connect(graph, 1, 2, input_type="right")
    try:
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        ref = graph.get_node(1).results.result_ref
        assert (ref.backend_id, ref.state) == (UNREADABLE_WORKER_ID, "materialised")
        path = WorkerLocation.model_validate_json(ref.location).path
        assert path in unreadable_worker[0].stored and not os.path.exists(path)
        assert [column.column_name for column in graph.get_node(2).schema] == [
            "name",
            "name_right",
            "name_vs_name_right_levenshtein",
        ]
        rows = graph.get_node(2).get_table_example(True).data
        assert sorted((row["name"], row["name_right"]) for row in rows) == [
            ("courtney", "courtney"),
            ("eduward", "eduward"),
            ("eduward", "edward"),
            ("edward", "eduward"),
            ("edward", "edward"),
        ]
    finally:
        graph.close_flow()


def test_a_sql_query_runs_on_the_worker(unreadable_worker, protocol_on, monkeypatch):
    """The query is translated over a stored input core cannot open, on the worker that holds it, never in core."""
    calls = record_run_ops(monkeypatch, WorkerBackend)
    monkeypatch.setattr(pl.SQLContext, "execute", translated_in_core)
    graph = sql_query_flow(9395, SQL_AGGREGATE)
    graph.get_node(1).node_settings.cache_results = True
    try:
        run_info = graph.run_graph()

        assert run_info.success, failures(run_info)
        assert [call.kind for call in calls] == ["sql_query"]
        ref = graph.get_node(1).results.result_ref
        assert (ref.backend_id, ref.state) == (UNREADABLE_WORKER_ID, "materialised")
        path = WorkerLocation.model_validate_json(ref.location).path
        assert path in unreadable_worker[0].stored and not os.path.exists(path)
        assert columns(graph.get_node(2).schema) == [("region", "String"), ("total", "Int64")]
        assert len(graph.get_node(2).get_table_example(True).data) == 2
    finally:
        graph.close_flow()


API_ROWS = [{"id": 1, "name": "a", "city": "ams"}, {"id": 2, "name": "b", "city": "rtm"}]


@pytest.fixture
def api_url():
    with serve(json.dumps(API_ROWS).encode(), "application/json") as url:
        yield f"{url}/items"


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
    calls = record_run_ops(monkeypatch, WorkerBackend)
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
        written_by = "the export's file is written by the flow's backend"
        assert [call.kind for call in calls] == ["random_split", "file_write"], written_by
    finally:
        flow_file_handler.delete_flow(9346)
