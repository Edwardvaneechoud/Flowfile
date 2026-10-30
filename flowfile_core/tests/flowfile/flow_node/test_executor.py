"""Tests for the FlowNode executor module.

Tests the hints a node submits its plan with, which backend path ran in each execution
location, state management, and the decision-making logic of the refactored FlowNode.
"""

import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from polars.exceptions import ColumnNotFoundError

from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.context import ExecutionContext, use_context
from flowfile_core.flowfile.execution.errors import ComputeUnavailable
from flowfile_core.flowfile.execution.protocol import ResultRef, SubmitHints, worker_stores
from flowfile_core.flowfile.flow_data_engine.column_stats import ColumnStatsUnavailable
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.flow_node.models import (
    ExecutionDecision,
    InvalidationReason,
)
from flowfile_core.flowfile.flow_node.state import (
    NodeExecutionState,
    SourceFileInfo,
)
from flowfile_core.flowfile.flow_node.executor import (
    IN_CORE_NODE_TYPES,
    OPERATION_NODE_TYPES,
    NodeExecutor,
    hints_for,
    is_narrow_transform,
)
from flowfile_core.configs.node_store.nodes import get_all_standard_nodes
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_worker.service import _materialise
from shared import compute_protocol
from shared.compute_protocol import WorkerLocation
from shared.storage_config import storage
from tests.conftest import Call, closed_url, protocol_states, spy
from typing import Literal


def _find_parent_directory(target_dir_name: str) -> Path:
    current_path = Path(__file__)
    while current_path != current_path.parent:
        if current_path.name == target_dir_name:
            return current_path
        current_path = current_path.parent
    raise FileNotFoundError(f"Directory '{target_dir_name}' not found")


def find_parent_directory(target_dir_name, start_path=None):
    """Navigate up directories until finding the target directory"""
    current_path = Path(start_path) if start_path else Path.cwd()

    while current_path != current_path.parent:
        if current_path.name == target_dir_name:
            return current_path
        if current_path.name == target_dir_name:
            return current_path
        current_path = current_path.parent

    raise FileNotFoundError(f"Directory '{target_dir_name}' not found")


ExecutionModeLit = Literal["Development", "Performance"]
ExecutionLocationLit = Literal["remote", "local"]


protocol = protocol_states("flag_off", "flag_on")


def create_graph(execution_mode: ExecutionModeLit = 'Development', flow_id: int = 1,
                 execution_location: ExecutionLocationLit = "remote") -> FlowGraph:
    """Create a new flow graph for testing."""
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(
        flow_id=flow_id,
        name='test_flow',
        path='.',
        execution_mode=execution_mode,
        execution_location=execution_location,
    ))
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1) -> FlowGraph:
    """Add a manual input node to the graph."""
    raw_data = input_schema.RawData.from_pylist(data)
    node_promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=node_id, raw_data_format=raw_data)
    graph.add_manual_input(input_file)
    return graph


def add_node_promise(graph: FlowGraph, node_type: str, node_id: int):
    """Add a node promise to the graph."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type=node_type,
    )
    graph.add_node_promise(node_promise)


def create_graph_with_select(flow_id: int = 1,
                             execution_mode: ExecutionModeLit = 'Development',
                             execution_location: ExecutionLocationLit = "remote") -> FlowGraph:
    """Create a graph with a manual input (node 1) connected to a select node (node 2)."""
    graph = create_graph(execution_mode=execution_mode, flow_id=flow_id, execution_location=execution_location)
    add_manual_input(graph, [{"name": "Alice", "age": 30}], node_id=1)
    add_node_promise(graph, 'select', node_id=2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    select_input = transform_schema.SelectInput(old_name='name', new_name='name', keep=True)
    node_select = input_schema.NodeSelect(
        flow_id=flow_id,
        node_id=2,
        depending_on_id=1,
        select_input=[select_input],
    )
    graph.add_select(node_select)
    return graph


def create_graph_with_read_and_select(flow_id: int = 10,
                                      execution_mode: ExecutionModeLit = 'Development',
                                      execution_location: ExecutionLocationLit = "remote") -> FlowGraph:
    """Create a graph with a parquet read (node 1, 1000 rows) connected to a select node (node 2).

    Uses fake_data.parquet which has 1000 rows with columns including Name, City, Email, etc.
    The select node keeps only the Name column.
    """
    graph = create_graph(execution_mode=execution_mode, flow_id=flow_id, execution_location=execution_location)
    file_path = str(
        _find_parent_directory('Flowfile')
        / 'flowfile_core' / 'tests' / 'support_files' / 'data' / 'fake_data.parquet'
    )
    add_node_promise(graph, 'read', node_id=1)
    read_node = input_schema.NodeRead(
        flow_id=flow_id,
        node_id=1,
        received_file=input_schema.ReceivedTable(
            name='fake_data.parquet',
            path=file_path,
            file_type='parquet',
        ),
    )
    graph.add_read(read_node)

    add_node_promise(graph, 'select', node_id=2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    select_input = transform_schema.SelectInput(old_name='Name', new_name='Name', keep=True)
    node_select = input_schema.NodeSelect(
        flow_id=flow_id,
        node_id=2,
        depending_on_id=1,
        select_input=[select_input],
        keep_missing=False,
    )
    graph.add_select(node_select)
    return graph


def create_graph_with_sort(flow_id: int = 2, execution_mode: ExecutionModeLit = 'Development',
                           execution_location: ExecutionLocationLit = "remote") -> FlowGraph:
    """Create a graph with a manual input (node 1) connected to a sort node (node 2)."""
    graph = create_graph(execution_mode=execution_mode, flow_id=flow_id, execution_location=execution_location)
    add_manual_input(graph, [{"name": "Charlie", "age": 25}, {"name": "Alice", "age": 30}], node_id=1)
    add_node_promise(graph, 'sort', node_id=2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    sort_input = transform_schema.SortByInput(column='name', how='asc')
    node_sort = input_schema.NodeSort(
        flow_id=flow_id,
        node_id=2,
        depending_on_id=1,
        sort_input=[sort_input],
    )
    graph.add_sort(node_sort)
    return graph


def create_graph_with_sort_and_select(flow_id: int = 3,
                                      execution_mode: ExecutionModeLit = 'Development',
                                      execution_location: ExecutionLocationLit = "remote") -> FlowGraph:
    """Create a graph with a manual input (node 1) connected to a sort node (node 2)."""
    graph = create_graph(execution_mode=execution_mode, flow_id=flow_id, execution_location=execution_location)
    add_manual_input(graph, [{"name": "Charlie", "age": 25}, {"name": "Alice", "age": 30}], node_id=1)
    add_node_promise(graph, 'sort', node_id=2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    sort_input = transform_schema.SortByInput(column='name', how='asc')
    node_sort = input_schema.NodeSort(
        flow_id=flow_id,
        node_id=2,
        depending_on_id=1,
        sort_input=[sort_input],
    )
    graph.add_sort(node_sort)
    connection = input_schema.NodeConnection.create_from_simple_input(2, 3)
    add_node_promise(graph, 'select', node_id=3)

    add_connection(graph, connection)
    select_input = transform_schema.SelectInput(old_name='name', new_name='Name', keep=True)
    node_select = input_schema.NodeSelect(
        flow_id=flow_id,
        node_id=3,
        depending_on_id=2,
        select_input=[select_input],
        keep_missing=False,
    )
    graph.add_select(node_select)
    return graph


def add_sort(graph: FlowGraph, node_id: int, depending_on_id: int, column: str = "name") -> None:
    add_node_promise(graph, "sort", node_id=node_id)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            sort_input=[transform_schema.SortByInput(column=column, how="asc")],
        )
    )


class TestInvalidationReason:
    """Tests for the InvalidationReason enum."""

    def test_reason_values_exist(self):
        """Verify all expected invalidation reasons exist."""
        assert InvalidationReason.NEVER_RAN is not None
        assert InvalidationReason.SETTINGS_CHANGED is not None
        assert InvalidationReason.SOURCE_FILE_CHANGED is not None
        assert InvalidationReason.CACHE_MISSING is not None
        assert InvalidationReason.FORCED_REFRESH is not None
        assert InvalidationReason.OUTPUT_NODE is not None
        assert InvalidationReason.PERFORMANCE_MODE is not None

    def test_reason_values_are_distinct(self):
        """Ensure all reason values are distinct."""
        reasons = [
            InvalidationReason.NEVER_RAN,
            InvalidationReason.SETTINGS_CHANGED,
            InvalidationReason.SOURCE_FILE_CHANGED,
            InvalidationReason.CACHE_MISSING,
            InvalidationReason.FORCED_REFRESH,
            InvalidationReason.OUTPUT_NODE,
            InvalidationReason.PERFORMANCE_MODE,
        ]
        assert len(reasons) == len(set(reasons))


class TestExecutionDecision:
    """Tests for the ExecutionDecision dataclass."""

    def test_decision_creation(self):
        """Test creating an execution decision."""
        decision = ExecutionDecision(should_run=True, reason=InvalidationReason.NEVER_RAN)
        assert decision.should_run is True
        assert decision.reason == InvalidationReason.NEVER_RAN

    def test_skip_decision(self):
        """Test creating a skip decision."""
        decision = ExecutionDecision(should_run=False, reason=None)
        assert decision.should_run is False
        assert decision.reason is None


class TestNodeExecutor:
    """Tests for the NodeExecutor class."""

    def test_executor_creation_with_real_node(self):
        """Test creating a NodeExecutor with a real FlowNode."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        executor = NodeExecutor(node)
        assert executor.node is node

    def test_a_manual_input_is_submitted_wide_with_a_preview(self):
        """A source without a narrow template default is wide; Development mode keeps a preview."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        hints = hints_for(node, graph.execution_context, performance_mode=False)

        assert hints == SubmitHints(must_materialise=False, wide=True, fanout=0, preview_rows=100)

    def test_decide_execution_never_ran(self):
        """Test execution decision when node never ran."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(node)
        decision = executor._decide_execution(
            state=node._execution_state,
            performance_mode=False,
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.NEVER_RAN

    def test_decide_execution_forced_refresh(self):
        """Test execution decision with forced refresh."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        node._execution_state.has_run_with_current_setup = True

        executor = NodeExecutor(node)
        decision = executor._decide_execution(
            state=node._execution_state,
            performance_mode=False,
            force_refresh=True,
        )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.FORCED_REFRESH

    def test_decide_execution_already_ran_skips(self):
        """Already-ran nodes should skip regardless of mode."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)
        node._execution_state.has_run_with_current_setup = True

        executor = NodeExecutor(node)

        # Development mode: skip
        decision = executor._decide_execution(
            state=node._execution_state,
            performance_mode=False,
            force_refresh=False,
        )
        assert decision.should_run is False

        # Performance mode: also skip
        decision = executor._decide_execution(
            state=node._execution_state,
            performance_mode=True,
            force_refresh=False,
        )
        assert decision.should_run is True

    def test_node_has_executor_property(self):
        """Test that FlowNode has an executor property."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        executor = node.executor
        assert isinstance(executor, NodeExecutor)
        assert executor.node is node

    def test_executor_is_lazily_created(self):
        """Test that executor is created lazily."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        assert node._executor is None

        executor = node.executor
        assert node._executor is not None
        assert node._executor is executor

        # Accessing again should return same instance
        assert node.executor is executor


class TestExecutorIntegration:
    """Integration tests for executor with real nodes."""

    def test_state_lifecycle(self):
        """Test state transitions through mark_successful and reset."""
        state = NodeExecutionState()

        assert state.has_run_with_current_setup is False

        state.mark_successful()
        assert state.has_run_with_current_setup is True
        assert state.has_completed_last_run is True

        state.reset()
        assert state.has_run_with_current_setup is False
        assert state.has_completed_last_run is False

    def test_source_file_change_detection_workflow(self):
        """Test the workflow of detecting source file changes."""
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            f.write("initial")
            temp_path = f.name

        try:
            state = NodeExecutionState()
            state.source_file_info = SourceFileInfo.from_path(temp_path)
            assert state.source_file_info is not None
            assert state.source_file_info.has_changed() is False

            time.sleep(0.1)
            with open(temp_path, 'w') as f:
                f.write("modified content")

            assert state.source_file_info.has_changed() is True

            state.source_file_info = SourceFileInfo.from_path(temp_path)
            assert state.source_file_info.has_changed() is False
        finally:
            os.unlink(temp_path)

    def test_executor_with_graph_execution(self, execution_location):
        """Test that executor integrates properly with graph execution."""
        graph = create_graph(execution_location=execution_location)
        add_manual_input(graph, [{"name": "Alice"}, {"name": "Bob"}], node_id=1)

        node = graph.get_node(1)

        assert node._execution_state.has_run_with_current_setup is False

        result = graph.run_graph()
        assert result.success is True

        assert node._execution_state.has_run_with_current_setup is True


def _held_ref_state(graph: FlowGraph, node_id: int) -> str | None:
    ref = graph.get_node(node_id).results.result_ref
    return None if ref is None else ref.state


def create_graph_with_output(tmp_path: Path, flow_id: int,
                             execution_location: ExecutionLocationLit = "remote") -> FlowGraph:
    """manual_input(1) -> csv output(2)."""
    graph = create_graph(flow_id=flow_id, execution_location=execution_location)
    add_manual_input(graph, [{"name": "Alice", "age": 30}], node_id=1)
    add_node_promise(graph, "output", node_id=2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_output(
        input_schema.NodeOutput(
            flow_id=flow_id,
            node_id=2,
            output_settings=input_schema.OutputSettings(
                name="out.csv",
                directory=str(tmp_path),
                file_type="csv",
                write_mode="overwrite",
                table_settings=input_schema.OutputCsvTable(),
            ),
        )
    )
    return graph


class TestHintsByNodeType:
    """``hints_for`` describes a node's plan from its template, settings and place in the graph."""

    def test_select_node_has_node_default(self):
        """Verify that the select node has a node_default set."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        assert select_node.node_default is not None, "select node should have a node_default"

    def test_select_node_is_narrow_transform(self):
        """Verify that the select node is a narrow transform type."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        assert select_node.node_default.transform_type == "narrow"
        assert is_narrow_transform(select_node)

    def test_a_narrow_transform_with_one_consumer(self):
        """A select (narrow, read by one sort, which its preview stores) is neither wide nor materialised."""
        graph = create_graph_with_sort_and_select()
        add_sort(graph, node_id=4, depending_on_id=3, column="Name")

        hints = hints_for(graph.get_node(3), graph.execution_context, performance_mode=False)

        assert hints == SubmitHints(must_materialise=False, wide=False, fanout=1, preview_rows=100)

    def test_a_wide_transform(self):
        """A sort (wide) is submitted wide."""
        graph = create_graph_with_sort()
        sort_node = graph.get_node(2)

        assert not is_narrow_transform(sort_node)
        assert hints_for(sort_node, graph.execution_context, performance_mode=False) == SubmitHints(
            must_materialise=False, wide=True, fanout=0, preview_rows=100
        )

    def test_a_node_with_two_consumers(self):
        """fanout counts the plans that execute the result: two stored sorts are two, a narrow select nothing
        reads is none."""
        graph = create_graph_with_sort_and_select()
        add_sort(graph, node_id=4, depending_on_id=2)
        add_sort(graph, node_id=5, depending_on_id=2, column="age")

        hints = hints_for(graph.get_node(2), graph.execution_context, performance_mode=False)

        assert hints.fanout == 2
        assert hints.wide is True

    def test_a_cached_node_must_materialise_and_keeps_its_preview_in_performance_mode(self):
        """cache_results asks for the rows, and the node keeps its example data in every mode."""
        graph = create_graph_with_select(execution_mode="Performance")
        select_node = graph.get_node(2)
        select_node.node_settings.cache_results = True

        hints = hints_for(select_node, graph.execution_context, performance_mode=True)

        assert graph.execution_context.preview_rows is None
        assert hints == SubmitHints(must_materialise=True, wide=False, fanout=0, preview_rows=100)

    def test_performance_mode_asks_for_no_preview(self):
        """Performance mode drops the preview; Development keeps the context's preview size."""
        graph = create_graph_with_sort(execution_mode="Performance")
        sort_node = graph.get_node(2)

        assert hints_for(sort_node, graph.execution_context, performance_mode=True).preview_rows is None
        development = ExecutionContext(backend_id="worker", parallelism=1, preview_rows=7)
        assert hints_for(sort_node, development, performance_mode=False).preview_rows == 7

    def test_an_output_node_is_never_materialised_wide_or_previewed(self, tmp_path):
        """A sink's result is the input it wrote: nothing is computed or sampled again."""
        graph = create_graph_with_output(tmp_path, flow_id=41)
        output_node = graph.get_node(2)
        output_node.node_settings.cache_results = True

        assert output_node.node_type in IN_CORE_NODE_TYPES
        assert hints_for(output_node, graph.execution_context, performance_mode=False) == SubmitHints(
            must_materialise=False, wide=False, fanout=0, preview_rows=None
        )

    def test_hints_do_not_depend_on_the_execution_location(self):
        """The location picks the backend; the hints describe the plan the same way for both."""
        remote = create_graph_with_select(flow_id=42, execution_location="remote")
        local = create_graph_with_select(flow_id=43, execution_location="local")

        assert hints_for(remote.get_node(2), remote.execution_context, False) == hints_for(
            local.get_node(2), local.execution_context, False
        )


def create_graph_with_csv_read(tmp_path: Path, flow_id: int, execution_mode: ExecutionModeLit,
                               execution_location: ExecutionLocationLit = "local", rows: int = 250) -> FlowGraph:
    """A csv read (node 1) of ``rows`` rows ``{"id": i, "val": f"v{i}"}``: wide, as a read has no narrow default."""
    path = tmp_path / "rows.csv"
    path.write_text("id,val\n" + "".join(f"{i},v{i}\n" for i in range(rows)))
    graph = create_graph(execution_mode=execution_mode, flow_id=flow_id, execution_location=execution_location)
    add_node_promise(graph, "read", node_id=1)
    graph.add_read(
        input_schema.NodeRead(
            flow_id=flow_id,
            node_id=1,
            received_file=input_schema.ReceivedTable(name="rows.csv", path=str(path), file_type="csv"),
        )
    )
    return graph


def _submitted_preview_rows(submits: list[Call], result_id: str) -> list[int | None]:
    """The ``preview_rows`` of each recorded ``submit`` under ``result_id``."""
    return [call.args[2].preview_rows for call in submits if call.args[3] == result_id]


def _samples_of(samples: list[Call], ref: ResultRef) -> list[tuple[ResultRef, int]]:
    return [(call.args[0], call.args[1]) for call in samples if call.args[0].result_id == ref.result_id]


def add_polars_code(graph: FlowGraph, node_id: int, depending_on_id: int, code: str) -> None:
    add_node_promise(graph, "polars_code", node_id)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_ids=[depending_on_id],
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
        )
    )


class TestPreviewFetchByExecutionMode:
    """A preview fetch fills the grid in either flow mode; only a Development flow stores a wide result for it."""

    def test_a_performance_flow_keeps_the_read_deferred_and_samples_the_grid_in_the_fetch(
        self, tmp_path, monkeypatch, execution_location, protocol
    ):
        graph = create_graph_with_csv_read(tmp_path, 60, "Performance", execution_location)
        backend = registry.backend(graph.execution_context.backend_id)
        submits = spy(monkeypatch, backend, "submit")
        samples = spy(monkeypatch, backend, "sample")

        graph.trigger_fetch_node(1)

        read_node = graph.get_node(1)
        ref = read_node.results.result_ref
        assert ref.state == "deferred"
        assert _submitted_preview_rows(submits, read_node.hash) == [None]
        assert _samples_of(samples, ref) == [(ref, 100)], "the fetch samples the grid once, before it reports the node run"
        if not protocol and execution_location == "remote":
            assert read_node.results.example_data_path is not None, "the sample's file backs the first-row reads"
        example = read_node.get_table_example(include_data=True)
        assert len(example.data) == 100
        assert example.data[0] == {"id": 0, "val": "v0"}
        read_node.get_table_example(include_data=True)
        assert len(_samples_of(samples, ref)) == 1, "the sampled grid is kept"

    def test_a_plan_that_fails_on_its_rows_fails_the_fetch(self, tmp_path, execution_location, protocol):
        """The columns resolve, so only reading rows shows the failure: the fetch reads them."""
        graph = create_graph_with_csv_read(tmp_path, 66, "Performance", execution_location)
        add_polars_code(graph, 2, 1, 'output_df = input_df.with_columns(pl.col("val").cast(pl.Int64, strict=True))')

        graph.trigger_fetch_node(2)

        node = graph.get_node(2)
        assert node.results.errors is not None
        assert node.node_stats.has_completed_last_run is False
        assert node.results.example_data_generator is None
        assert node.get_table_example(include_data=True).data == []

    def test_a_fetch_that_releases_a_stored_file_drops_the_nodes_below(self, tmp_path, execution_location, protocol):
        """A run stores a read two written nodes read; its Performance fetch leaves it deferred, so the sort's plan
        over the stored file is dropped rather than left dangling, and the sort's own fetch rebuilds it."""
        graph = create_graph_with_csv_read(tmp_path, 67, "Performance", execution_location)
        add_node_promise(graph, "sort", node_id=2)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
        graph.add_sort(
            input_schema.NodeSort(
                flow_id=67, node_id=2, depending_on_id=1, sort_input=[transform_schema.SortByInput(column="id", how="desc")]
            )
        )
        add_node_promise(graph, "select", node_id=3)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 3))
        graph.add_select(
            input_schema.NodeSelect(
                flow_id=67, node_id=3, depending_on_id=1, select_input=[transform_schema.SelectInput(old_name="id")]
            )
        )
        add_csv_output(graph, 4, depending_on_id=2, directory=tmp_path)
        add_csv_output(graph, 5, depending_on_id=3, directory=tmp_path)
        assert graph.run_graph().success
        assert _held_ref_state(graph, 1) == "materialised"
        sort_node = graph.get_node(2)
        assert sort_node.node_stats.has_run_with_current_setup

        graph.trigger_fetch_node(1)

        assert _held_ref_state(graph, 1) == "deferred"
        assert sort_node.results.result_ref is None
        assert sort_node.node_stats.has_run_with_current_setup is False
        graph.trigger_fetch_node(2)
        assert sort_node.results.errors is None
        assert sort_node.get_table_example(include_data=True).data[0] == {"id": 249, "val": "v249"}

    def test_a_development_fetch_that_stores_again_keeps_the_nodes_below(self, tmp_path, execution_location, protocol):
        graph = create_graph_with_csv_read(tmp_path, 68, "Development", execution_location)
        add_node_promise(graph, "sort", node_id=2)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
        graph.add_sort(
            input_schema.NodeSort(
                flow_id=68, node_id=2, depending_on_id=1, sort_input=[transform_schema.SortByInput(column="id", how="desc")]
            )
        )
        assert graph.run_graph().success
        sort_node = graph.get_node(2)
        held = sort_node.results.result_ref

        graph.trigger_fetch_node(1)

        assert _held_ref_state(graph, 1) == "materialised"
        assert sort_node.results.result_ref == held
        assert sort_node.node_stats.has_run_with_current_setup

    def test_a_development_flow_stores_the_read_and_takes_its_sample_inline(
        self, tmp_path, monkeypatch, execution_location, protocol
    ):
        graph = create_graph_with_csv_read(tmp_path, 61, "Development", execution_location)
        backend = registry.backend(graph.execution_context.backend_id)
        submits = spy(monkeypatch, backend, "submit")

        graph.trigger_fetch_node(1)

        read_node = graph.get_node(1)
        ref = read_node.results.result_ref
        assert ref.state == "materialised"
        assert _submitted_preview_rows(submits, read_node.hash) == [100]
        samples = spy(monkeypatch, backend, "sample")
        assert len(read_node.get_table_example(include_data=True).data) == 100
        assert _samples_of(samples, ref) == [], "the common case costs no second round trip"

    def test_a_cached_read_in_a_performance_flow_is_stored_with_its_sample_inline(
        self, tmp_path, monkeypatch, execution_location, protocol
    ):
        graph = create_graph_with_csv_read(tmp_path, 62, "Performance", execution_location)
        read_node = graph.get_node(1)
        read_node.node_settings.cache_results = True
        backend = registry.backend(graph.execution_context.backend_id)
        submits = spy(monkeypatch, backend, "submit")

        graph.trigger_fetch_node(1)

        ref = read_node.results.result_ref
        assert ref.state == "materialised"
        assert _submitted_preview_rows(submits, read_node.hash) == [100]
        samples = spy(monkeypatch, backend, "sample")
        assert len(read_node.get_table_example(include_data=True).data) == 100
        assert _samples_of(samples, ref) == []

    @pytest.mark.parametrize("execution_mode", ["Development", "Performance"])
    def test_an_explore_data_fetch_installs_no_grid(self, tmp_path, execution_mode, protocol):
        graph = create_graph_with_csv_read(tmp_path, 63, execution_mode)

        graph.trigger_fetch_node(1, performance_mode=True, reset_cache=False)

        read_node = graph.get_node(1)
        assert read_node.results.result_ref.state == "deferred"
        assert read_node.results.example_data_generator is None


class TestExecuteFillsTheGridByPreview:
    """``NodeExecutor.execute``'s ``preview`` decides the grid; ``performance_mode`` alone decides the hints."""

    @staticmethod
    def _execute(graph: FlowGraph, monkeypatch, performance_mode: bool, preview: bool | None):
        node = graph.get_node(1)
        backend = registry.backend(graph.execution_context.backend_id)
        submits, samples = spy(monkeypatch, backend, "submit"), spy(monkeypatch, backend, "sample")
        with use_context(graph.execution_context):
            node.executor.execute(
                reset_cache=True,
                performance_mode=performance_mode,
                node_logger=graph.flow_logger.get_node_logger(1),
                preview=preview,
            )
        assert node.results.errors is None
        return node, _submitted_preview_rows(submits, node.hash), samples

    def test_no_preview_in_development_mode_takes_the_sample_inline(self, tmp_path, monkeypatch):
        graph = create_graph_with_csv_read(tmp_path, 64, "Development")

        node, preview_rows, samples = self._execute(graph, monkeypatch, performance_mode=False, preview=None)

        assert preview_rows == [100]
        assert node.results.result_ref.state == "materialised"
        assert len(node.results.example_data_generator().to_pylist()) == 100
        assert _samples_of(samples, node.results.result_ref) == [], "the inline sample needs no second round trip"

    def test_no_preview_in_performance_mode_installs_no_grid(self, tmp_path, monkeypatch):
        graph = create_graph_with_csv_read(tmp_path, 69, "Development")

        node, preview_rows, samples = self._execute(graph, monkeypatch, performance_mode=True, preview=None)

        assert preview_rows == [None]
        assert node.results.result_ref.state == "deferred"
        assert node.results.example_data_generator is None
        assert _samples_of(samples, node.results.result_ref) == []

    def test_a_preview_in_performance_mode_sends_no_preview_rows_and_samples_in_the_run(self, tmp_path, monkeypatch):
        graph = create_graph_with_csv_read(tmp_path, 65, "Development")

        node, preview_rows, samples = self._execute(graph, monkeypatch, performance_mode=True, preview=True)

        assert preview_rows == [None]
        ref = node.results.result_ref
        assert ref.state == "deferred"
        assert _samples_of(samples, ref) == [(ref, 100)]
        assert len(node.results.example_data_generator().to_pylist()) == 100
        assert len(_samples_of(samples, ref)) == 1


class TestDecideExecutionByNodeType:
    """Tests that _decide_execution returns the correct decision
    for different node types and scenarios."""

    def test_select_never_ran_runs(self):
        """Select node that never ran should run."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(select_node)
        decision = executor._decide_execution(
            state=select_node._execution_state,
            performance_mode=False,
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.NEVER_RAN

    def test_select_forced_refresh_runs(self):
        """Select node with forced refresh should run."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node._execution_state.has_run_with_current_setup = True

        executor = NodeExecutor(select_node)
        decision = executor._decide_execution(
            state=select_node._execution_state,
            performance_mode=False,
            force_refresh=True,
        )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.FORCED_REFRESH

    def test_select_already_ran_skips(self):
        """Select node that already ran should skip in Development and run in Performance mode."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node._execution_state.has_run_with_current_setup = True

        executor = NodeExecutor(select_node)

        decision = executor._decide_execution(
            state=select_node._execution_state,
            performance_mode=False,
            force_refresh=False,
        )
        assert decision.should_run is False

        decision = executor._decide_execution(
            state=select_node._execution_state,
            performance_mode=True,
            force_refresh=False,
        )
        assert decision.should_run
        assert decision.reason == InvalidationReason.PERFORMANCE_MODE

    def test_a_cached_node_holding_no_stored_result_is_a_cache_miss(self):
        """A cached node with nothing stored under its hash runs, whatever its run state."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node.node_settings.cache_results = True
        select_node._execution_state.has_run_with_current_setup = True

        decision = NodeExecutor(select_node)._decide_execution(
            state=select_node._execution_state,
            performance_mode=False,
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.CACHE_MISSING

    def test_a_cached_node_skips_while_its_stored_result_is_held(self, protocol):
        """The decision asks the run's backend whether it still holds the stored result."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node.node_settings.cache_results = True
        select_node.results.result_ref = _stored_ref(select_node, "worker")

        with use_context(ExecutionContext("worker", 1, 100)):
            with patch.object(WorkerBackend, "exists", return_value=True) as exists:
                hit = NodeExecutor(select_node)._decide_execution(
                    state=select_node._execution_state, performance_mode=True, force_refresh=False
                )
            with patch.object(WorkerBackend, "exists", return_value=False):
                miss = NodeExecutor(select_node)._decide_execution(
                    state=select_node._execution_state, performance_mode=True, force_refresh=False
                )

        assert exists.called
        assert hit.should_run is False
        assert miss.should_run is True and miss.reason == InvalidationReason.CACHE_MISSING

    def test_a_cached_node_holding_a_deferred_plan_follows_the_ordinary_rules(self, protocol):
        """A held plan is no cache: Development skips a node that ran, Performance reruns it."""
        graph = create_graph_with_select(execution_location="local")
        assert graph.run_graph().success
        select_node = graph.get_node(2)
        assert select_node.results.result_ref.state == "deferred"
        select_node.node_settings.cache_results = True
        executor = NodeExecutor(select_node)

        with use_context(graph.execution_context):
            development = executor._decide_execution(
                state=select_node._execution_state, performance_mode=False, force_refresh=False
            )
            performance = executor._decide_execution(
                state=select_node._execution_state, performance_mode=True, force_refresh=False
            )

        assert development.should_run is False
        assert performance.should_run is True and performance.reason == InvalidationReason.PERFORMANCE_MODE

    def test_sort_never_ran_runs(self):
        """Sort node that never ran should run."""
        graph = create_graph_with_sort()
        sort_node = graph.get_node(2)
        sort_node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(sort_node)
        decision = executor._decide_execution(
            state=sort_node._execution_state,
            performance_mode=False,
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.NEVER_RAN


def _stored_ref(node, backend_id: str) -> ResultRef:
    return ResultRef(
        backend_id=backend_id, result_id=node.hash, schema=[], row_count=1, state="materialised", location=b"/x.arrow"
    )


@contextmanager
def _worker_holds_the_cache():
    """Decide under a worker context whose backend answers that it holds every stored result."""
    with use_context(ExecutionContext("worker", 1, 100)), patch.object(WorkerBackend, "exists", return_value=True):
        yield


class TestBackendPathByLocation:
    """Which backend path ran, read from the reference each node holds after a run."""

    def test_remote_mode_stores_wide_and_cached_nodes_and_defers_the_rest(self, tmp_path, remote_worker, protocol):
        graph = create_graph_with_sort_and_select(flow_id=44, execution_location="remote")
        add_node_promise(graph, "sample", node_id=4)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(3, 4))
        graph.add_sample(input_schema.NodeSample(flow_id=graph.flow_id, node_id=4, depending_on_id=3))
        graph.get_node(4).node_settings.cache_results = True

        assert graph.run_graph().success

        assert _held_ref_state(graph, 1) == "materialised"  # manual input: no narrow default, wide
        assert _held_ref_state(graph, 2) == "materialised"  # sort: wide
        assert _held_ref_state(graph, 3) == "deferred"  # select: narrow, one consumer
        assert _held_ref_state(graph, 4) == "materialised"  # cached sample
        select_node = graph.get_node(3)
        if protocol:
            assert select_node.results.example_data_path is None, "the inline sample backs the preview"
        else:
            assert select_node.results.example_data_path is not None, "the sampler's file backs the preview"
        assert select_node.get_table_example(True).data == [{"Name": "Alice"}, {"Name": "Charlie"}]

    def test_remote_mode_defers_a_wide_node_with_no_preview(self, remote_worker):
        """Performance mode asks for no preview, so nothing is stored for a wide node."""
        graph = create_graph_with_sort(flow_id=45, execution_mode="Performance", execution_location="remote")

        assert graph.run_graph().success

        assert _held_ref_state(graph, 2) == "deferred"
        assert graph.get_node(2).results.example_data_generator is None

    def test_remote_mode_keeps_an_output_node_in_core(self, tmp_path, remote_worker):
        graph = create_graph_with_output(tmp_path, flow_id=46, execution_location="remote")

        assert graph.run_graph().success

        assert _held_ref_state(graph, 2) == "deferred"
        assert graph.get_node(2).results.example_data_path is None
        assert (tmp_path / "out.csv").exists()

    @pytest.mark.parametrize("cached", [False, True], ids=["select", "cached-select"])
    def test_local_mode_stores_what_the_workers_policy_stores(self, protocol, cached):
        """``local`` is the worker embedded in this process, so its policy holds in both flag states: a manual
        input and a sort are wide and previewed, so stored; a select is narrow and stays deferred unless it caches."""
        graph = create_graph_with_sort_and_select(flow_id=47, execution_location="local")
        graph.get_node(3).node_settings.cache_results = cached

        assert graph.run_graph().success

        for node_id in (1, 2, 3):
            assert graph.get_node(node_id).results.result_ref.backend_id == "local"
        assert _held_ref_state(graph, 1) == "materialised"
        assert _held_ref_state(graph, 2) == "materialised"
        assert _held_ref_state(graph, 3) == ("materialised" if cached else "deferred")
        select_node = graph.get_node(3)
        assert select_node.results.example_data_path is None
        assert select_node.get_table_example(True).data == [{"Name": "Alice"}, {"Name": "Charlie"}]

    def test_both_locations_give_the_same_rows(self, execution_location):
        graph = create_graph_with_sort_and_select(flow_id=48, execution_location=execution_location)

        assert graph.run_graph().success

        assert graph.get_node(3).get_resulting_data().collect().to_dicts() == [{"Name": "Alice"}, {"Name": "Charlie"}]


class TestNodesWithDefaults:
    """Verify the node_default and transform_type of the nodes with template defaults."""

    @pytest.mark.parametrize("node_type,expected_has_default", [
        ("select", True),
        ("sample", True),
        ("sort", True),
        ("union", True),
        ("record_count", True),
    ])
    def test_nodes_with_defaults_have_node_default(self, node_type, expected_has_default):
        """Nodes in nodes_with_defaults should have a node_default on the FlowNode."""
        from flowfile_core.configs import node_store
        node_default = node_store.node_defaults.get(node_type)
        assert (node_default is not None) == expected_has_default, (
            f"Expected node_default for '{node_type}' to be "
            f"{'set' if expected_has_default else 'None'}"
        )

    @pytest.mark.parametrize("node_type,expected_transform_type", [
        ("select", "narrow"),
        ("sample", "narrow"),
        ("union", "narrow"),
        ("sort", "wide"),
        ("record_count", "wide"),
    ])
    def test_nodes_with_defaults_transform_types(self, node_type, expected_transform_type):
        """Verify transform_type for each node with defaults."""
        from flowfile_core.configs import node_store
        node_default = node_store.node_defaults.get(node_type)
        assert node_default is not None
        assert node_default.transform_type == expected_transform_type

    def test_manual_input_has_no_default_settings(self):
        """manual_input has a node_default but has_default_settings is False."""
        from flowfile_core.configs import node_store
        node_default = node_store.node_defaults.get("manual_input")
        assert node_default is not None
        assert not node_default.has_default_settings


class TestPreviewAfterExecution:
    """Tests that get_table_example reads from stored sample data after
    a graph has been executed, rather than recomputing the node.

    Uses a read node (1000-row parquet) → select node pipeline running
    locally to test the full preview path."""

    def test_preview_returns_data_after_run(self, execution_location):
        """After running the graph, preview should return sample data."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        run_info = graph.run_graph()
        assert run_info.success, f"Graph should run: {run_info}"

        select_node = graph.get_node(2)
        table_example = select_node.get_table_example(include_data=True)

        assert table_example is not None
        assert len(table_example.data) > 0, "Preview should contain rows"
        assert table_example.columns == ["Name"], "Select should keep only Name"

    def test_preview_returns_at_most_100_rows(self, execution_location):
        """The sample generator caps at 100 rows even though the source has 1000."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        select_node = graph.get_node(2)
        table_example = select_node.get_table_example(include_data=True)

        assert len(table_example.data) <= 100

    def test_preview_without_run_returns_empty_data(self):
        """Before running the graph, preview should return empty data."""
        graph = create_graph_with_read_and_select()
        select_node = graph.get_node(2)

        table_example = select_node.get_table_example(include_data=True)

        assert table_example is not None
        assert table_example.data == []

    def test_preview_does_not_recompute_node(self, execution_location):
        """Calling get_table_example should read stored data, not re-execute."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        select_node = graph.get_node(2)

        # Track whether get_resulting_data re-enters the computation path.
        # It's called for metadata, but results.resulting_data should still be cached.
        original_fn = select_node.get_resulting_data
        computation_entered = False

        def tracked_get_resulting_data():
            nonlocal computation_entered
            # If resulting_data has been cleared, the function would recompute
            if select_node.results.resulting_data is None:
                computation_entered = True
            return original_fn()

        with patch.object(select_node, 'get_resulting_data', side_effect=tracked_get_resulting_data):
            table_example = select_node.get_table_example(include_data=True)

        assert not computation_entered, (
            "get_table_example should not trigger fresh computation — "
            "results.resulting_data should still be cached"
        )
        assert len(table_example.data) > 0

    def test_preview_uses_cached_sample_on_repeated_calls(self, execution_location):
        """Multiple preview calls should return the same data without recomputation."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        select_node = graph.get_node(2)

        first = select_node.get_table_example(include_data=True)
        second = select_node.get_table_example(include_data=True)

        assert first.data == second.data, "Repeated previews should return identical data"

    def test_read_node_preview_returns_data(self, execution_location):
        """The read node (1000 rows) should also have preview data after run."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        read_node = graph.get_node(1)
        table_example = read_node.get_table_example(include_data=True)

        assert table_example is not None
        assert len(table_example.data) > 0
        assert len(table_example.data) <= 100


class TestPreviewRowCount:
    """The preview row count must be real or None (unknown) — never the old
    999 sentinel — and producing it must never trigger a count collect."""

    def test_preview_row_count_is_real_not_999(self, execution_location):
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        run_info = graph.run_graph()
        assert run_info.success, f"Graph should run: {run_info}"

        table_example = graph.get_node(2).get_table_example(include_data=True)

        # The count arrives for free in both modes: remote via the worker's
        # status, local via parquet metadata propagated through the select.
        assert table_example.number_of_records != 999
        assert table_example.number_of_records == 1000

    def test_preview_row_count_exact_for_small_result(self, execution_location):
        """A sample shorter than the sample size is the whole table — exact count."""
        graph = create_graph_with_select(execution_location=execution_location)
        run_info = graph.run_graph()
        assert run_info.success, f"Graph should run: {run_info}"

        table_example = graph.get_node(2).get_table_example(include_data=True)

        assert table_example.number_of_records == 1

    def test_preview_row_count_none_before_run(self):
        """Before a run the count is unknown, not the misleading 0 it used to be."""
        graph = create_graph_with_read_and_select()

        table_example = graph.get_node(2).get_table_example(include_data=True)

        assert table_example.number_of_records is None

    def test_preview_never_triggers_record_count_collect(self, execution_location):
        """The preview must never pay for a count: get_number_of_records on a
        lazy engine collects the whole upstream plan to render a header."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        with patch.object(FlowDataEngine, "get_number_of_records") as mock_count:
            graph.get_node(2).get_table_example(include_data=True)

        mock_count.assert_not_called()


class TestNodeColumnStats:
    """get_column_stats aggregates over the cached result without re-executing."""

    def test_column_stats_after_run(self, execution_location):
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        run_info = graph.run_graph()
        assert run_info.success, f"Graph should run: {run_info}"

        stats = graph.get_node(2).get_column_stats("Name")

        assert stats.number_of_empty_values + stats.number_of_filled_values == 1000
        assert stats.number_of_unique_values is not None
        assert stats.min_value is not None

    def test_column_stats_enrich_preview_schema(self, execution_location):
        """Stats land on the result schema's FlowfileColumn — the single truth —
        so this run's later previews carry them in table_schema too."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()
        node = graph.get_node(2)

        node.get_column_stats("Name")
        preview = node.get_table_example(include_data=True)

        name_column = next(c for c in preview.table_schema if c.name == "Name")
        assert name_column.number_of_empty_values is not None
        assert name_column.number_of_unique_values is not None

    def test_column_stats_before_run_raises(self):
        graph = create_graph_with_read_and_select()

        with pytest.raises(ColumnStatsUnavailable):
            graph.get_node(2).get_column_stats("Name")

    def test_column_stats_unknown_column_raises(self, execution_location):
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        with pytest.raises(ColumnNotFoundError):
            graph.get_node(2).get_column_stats("does_not_exist")

    def test_column_stats_never_reexecutes_node(self, execution_location):
        """Stats must resolve the engine passively — get_output/get_resulting_data
        can re-run the node from an HTTP request."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()
        node = graph.get_node(2)

        with (
            patch.object(node, "get_resulting_data") as mock_resulting,
            patch.object(node, "get_output") as mock_output,
        ):
            node.get_column_stats("Name")

        mock_resulting.assert_not_called()
        mock_output.assert_not_called()

    def test_column_stats_memoizes_row_count_for_preview(self, execution_location):
        """After one stats call the preview knows the exact total for free."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()
        node = graph.get_node(2)

        node.get_column_stats("Name")
        table_example = node.get_table_example(include_data=True)

        assert table_example.number_of_records == 1000

    def test_column_stats_outside_a_run_aggregate_on_the_graph_s_backend(self, protocol, monkeypatch):
        """Outside a run the statistics of a flow on another backend than the default aggregate on the flow's own
        backend with the protocol on, and on the default backend with it off."""
        from flowfile_core.flowfile.execution import registry
        from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
        from flowfile_core.flowfile.flow_data_engine import column_stats as column_stats_module

        class TeamBackend(EmbeddedWorkerBackend):
            id = "worker:team"

        aggregated_on: list[str] = []
        real_single_row = column_stats_module.single_row

        def recording_single_row(backend, plan, empty_message):
            aggregated_on.append(backend.id)
            return real_single_row(backend, plan, empty_message=empty_message)

        registry.reset()
        registry.register_backend("worker:team", TeamBackend)
        monkeypatch.setenv(registry.BACKEND_ENV_VAR, registry.LOCAL_BACKEND_ID)
        try:
            graph = create_graph_with_read_and_select(execution_location="worker:team")
            assert graph.run_graph().success
            monkeypatch.setattr(column_stats_module, "single_row", recording_single_row)

            stats = graph.get_node(2).get_column_stats("Name")
        finally:
            registry.reset()

        assert stats.number_of_empty_values + stats.number_of_filled_values == 1000
        assert aggregated_on == (["worker:team"] if protocol else [registry.LOCAL_BACKEND_ID])


class TestColumnStatsWorkerOffload:
    """Local-mode results are full upstream plans: with the worker as the backend their stats aggregate runs
    in the worker, never in the core process, and a failure there never falls back to core."""

    def test_local_stats_offloaded_never_collect_in_process(self, monkeypatch):
        from flowfile_core.flowfile.execution import registry
        from tests.conftest import is_worker_running

        if not is_worker_running():
            pytest.skip("Worker not running")

        graph = create_graph_with_read_and_select(execution_location="local")
        run_info = graph.run_graph()
        assert run_info.success, f"Graph should run: {run_info}"
        node = graph.get_node(2)
        collects = _count_collects(monkeypatch)

        stats = node.get_column_stats("Name", backend=registry.backend(registry.WORKER_BACKEND_ID))

        assert collects == [], "the worker computed the aggregate; core read back one row"
        assert stats.number_of_empty_values + stats.number_of_filled_values == 1000
        assert stats.number_of_unique_values is not None

    def test_offload_failure_raises_instead_of_collecting_in_core(self, monkeypatch):
        """A dead worker must surface as unavailable (409), never as a silent
        in-process collect of the full upstream plan."""
        from flowfile_core.flowfile.execution.errors import ComputeUnavailable
        from flowfile_core.flowfile.flow_data_engine import column_stats as column_stats_module

        graph = create_graph_with_read_and_select(execution_location="local")
        graph.run_graph()
        node = graph.get_node(2)
        worker = WorkerBackend(url=WORKER_URL)
        collects = _count_collects(monkeypatch)

        with (
            patch.object(
                column_stats_module, "single_row", side_effect=ComputeUnavailable("connection refused")
            ) as mock_read,
            pytest.raises(ColumnStatsUnavailable, match="connection refused"),
        ):
            node.get_column_stats("Name", backend=worker)

        assert [call.args[0] for call in mock_read.call_args_list] == [worker, worker], (
            "full query, then the count-only retry, both on the worker"
        )
        assert collects == []

    def test_offload_query_failure_retries_counts_only_in_worker(self, monkeypatch):
        """A worker-side query error degrades to the count-only exprs — still
        executed in the worker, never in core."""
        from flowfile_core.flowfile.execution.errors import TaskFailed
        from flowfile_core.flowfile.flow_data_engine import column_stats as column_stats_module

        graph = create_graph_with_read_and_select(execution_location="local")
        graph.run_graph()
        node = graph.get_node(2)
        worker = WorkerBackend(url=WORKER_URL)
        collects = _count_collects(monkeypatch)

        with patch.object(
            column_stats_module,
            "single_row",
            side_effect=[TaskFailed("dtype error in worker"), {"total_rows": 1000, "null_count": 0}],
        ) as mock_read:
            stats = node.get_column_stats("Name", backend=worker)

        assert [call.args[0] for call in mock_read.call_args_list] == [worker, worker]
        assert collects == []
        assert stats.number_of_filled_values == 1000
        assert stats.number_of_unique_values is None, "degraded result carries counts only"

    @pytest.mark.parametrize("backend_id", ["local", "worker"])
    def test_stats_of_either_flow_run_on_either_backend(self, execution_location, backend_id):
        """Whichever backend aggregates, it reads the result the node holds: its plan or the worker's file."""
        from flowfile_core.flowfile.execution import registry
        from tests.conftest import is_worker_running

        if backend_id == "worker" and not is_worker_running():
            pytest.skip("Worker not running")
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        assert graph.run_graph().success

        stats = graph.get_node(2).get_column_stats("Name", backend=registry.backend(backend_id))

        assert stats.number_of_empty_values + stats.number_of_filled_values == 1000
        assert stats.number_of_unique_values is not None


def _count_collects(monkeypatch) -> list[int]:
    """One entry per ``LazyFrame.collect`` in this process from here on."""
    import polars as pl

    collects: list[int] = []
    real_collect = pl.LazyFrame.collect

    def counting_collect(self, *args, **kwargs):
        collects.append(1)
        return real_collect(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", counting_collect)
    return collects


class TestPreviewAfterUpstreamChange:
    """Tests what happens to preview data when an upstream node changes
    after execution.

    This is the scenario we don't expect to work correctly:
    1. Run the graph
    2. Preview returns data (good)
    3. Change upstream node settings
    4. Preview again — what happens?

    The reset() method sets _execution_state.has_completed_last_run = False
    but does NOT set node_stats.has_completed_last_run = False.
    get_table_example checks node_stats.has_completed_last_run, so after
    a reset it may still enter the data path with a stale
    example_data_generator."""

    def test_upstream_change_resets_execution_state(self, execution_location):
        """After changing settings, _execution_state should reflect the reset."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        select_node = graph.get_node(2)
        assert select_node._execution_state.has_run_with_current_setup is True
        assert select_node._execution_state.has_completed_last_run is True

        # Change the select configuration (keep City instead of Name)
        new_select_input = transform_schema.SelectInput(old_name='City', new_name='City', keep=True)
        node_select = input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[new_select_input],
            keep_missing=False,
        )
        graph.add_select(node_select)

        assert select_node._execution_state.has_run_with_current_setup is False

    def test_settings_change_node_stats_has_completed_last_run(self, execution_location):
        """Check whether node_stats.has_completed_last_run is properly reset
        when the node's settings change.

        get_table_example gates on node_stats.has_completed_last_run.
        If this stays True after a reset, preview will try to read stale data."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        select_node = graph.get_node(2)
        assert select_node.node_stats.has_completed_last_run is True

        new_select_input = transform_schema.SelectInput(old_name='City', new_name='City', keep=True)
        node_select = input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[new_select_input],
            keep_missing=False,
        )
        graph.add_select(node_select)

        # KNOWN ISSUE: node_stats.has_completed_last_run is NOT reset by
        # FlowNode.reset(). If this starts failing, the stale-data bug is fixed.
        assert select_node.node_stats.has_completed_last_run is True, (
            "KNOWN ISSUE: node_stats.has_completed_last_run is not reset "
            "when the node's settings change. If this assertion starts failing, "
            "the stale preview data issue has been fixed."
        )

    def test_preview_after_settings_change_returns_stale_data(self, execution_location):
        """Documents current (incorrect) behavior: preview returns stale data
        after changing settings without re-running. When this starts failing,
        the stale-data bug is fixed."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        select_node = graph.get_node(2)
        first_preview = select_node.get_table_example(include_data=True)
        assert first_preview.columns == ["Name"]
        assert len(first_preview.data) > 0

        new_select_input = transform_schema.SelectInput(old_name='City', new_name='City', keep=True)
        node_select = input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[new_select_input],
            keep_missing=False,
        )
        graph.add_select(node_select)

        # Preview without re-running: this is the stale data scenario
        second_preview = select_node.get_table_example(include_data=True)

        # Stale behavior: example_data_generator was never cleared by reset(),
        # so preview returns OLD sample data with NEW column headers.
        # If this behavior changes, update this test to reflect the fix.
        if select_node.node_stats.has_completed_last_run:
            assert second_preview.data is not None
            assert second_preview.columns == ["City"]
            if len(second_preview.data) > 0:
                first_row = second_preview.data[0]
                assert "Name" in first_row or "City" in first_row, (
                    "Data should contain either old Name or new City column data"
                )
        else:
            # Fixed path: node_stats was properly reset, preview returns empty
            assert second_preview.data == []

    def test_preview_correct_after_rerun(self, execution_location):
        """After changing settings AND re-running, preview returns fresh data."""
        graph = create_graph_with_read_and_select(execution_location=execution_location)
        graph.run_graph()

        select_node = graph.get_node(2)
        first_preview = select_node.get_table_example(include_data=True)
        assert first_preview.columns == ["Name"]

        new_select_input = transform_schema.SelectInput(old_name='City', new_name='City', keep=True)
        node_select = input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[new_select_input],
            keep_missing=False,
        )
        graph.add_select(node_select)

        run_info = graph.run_graph()
        assert run_info.success

        select_node = graph.get_node(2)
        second_preview = select_node.get_table_example(include_data=True)
        assert second_preview.columns == ["City"]
        assert len(second_preview.data) > 0
        for row in second_preview.data:
            assert "City" in row, f"Expected City column in row, got: {row.keys()}"

    def test_preview_after_upstream_settings_change_returns_stale_data(self, execution_location):
        """Documents stale-data behavior after upstream settings change without re-run.
        When this starts failing, the bug is fixed."""
        graph = create_graph_with_sort_and_select(execution_location=execution_location)
        graph.run_graph()
        select_node = graph.get_node(3)
        assert select_node.results.example_data_generator
        first_preview = select_node.get_table_example(include_data=True)
        assert first_preview.columns == ["Name"]
        assert len(first_preview.data) > 0

        node_sort = input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            sort_input=[transform_schema.SortByInput(column='name', how='desc')]
        )
        graph.add_sort(node_sort)
        # Preview without re-running: this is the stale data scenario
        assert select_node.results.example_data_generator is not None
        assert not select_node.node_stats.has_run_with_current_setup
        assert select_node.node_stats.has_completed_last_run

    def test_fuzzy_match_behaviour(self, execution_location):
        # found an issue with the fuzzy match schema predictor invoking a full run to observe the schema
        from copy import deepcopy

        graph = create_graph(execution_mode="Development", execution_location=execution_location, flow_id=1)

        received_table = input_schema.ReceivedTable(file_type='parquet', name='table.parquet',
                                                    path=str(Path(find_parent_directory(
                                                        'Flowfile')) / 'flowfile_core/tests/support_files/data/large_table.parquet'))
        add_node_promise(graph, 'read', node_id=1)
        node_read = input_schema.NodeRead(flow_id=1, node_id=1, received_file=received_table)
        graph.add_read(node_read)

        node_select = input_schema.NodeSelect(flow_id=1, node_id=2,
                                              select_input=[
                                                  transform_schema.SelectInput("Name"),
                                                  transform_schema.SelectInput("Address"),
                                                  transform_schema.SelectInput("ID"),
                                                  transform_schema.SelectInput("Work")
                                              ],
                                              keep_missing=False
                                              )
        add_node_promise(graph, 'select', node_id=2)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
        graph.add_select(node_select)

        add_node_promise(graph, "fuzzy_match", 3)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 3))
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3, "right"))
        graph.add_fuzzy_match(input_schema.NodeFuzzyMatch(
            flow_id=1,
            node_id=3,
            join_input=transform_schema.FuzzyMatchInput(
                join_mapping=[transform_schema.FuzzyMapping(
                    left_col='Name',
                    right_col='Name',
                    threshold_score=75,
                    fuzzy_type='levenshtein',
                    valid=True
                )],
                left_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='ID', new_name='ID', keep=True),
                    transform_schema.SelectInput(old_name='Name', new_name='customer_name', keep=True, join_key=True),
                ]),
                right_select=transform_schema.JoinInputs(renames=[
                    transform_schema.SelectInput(old_name='Name', new_name='customer_name_right', keep=True, join_key=True),
                    transform_schema.SelectInput(old_name='Address', new_name='address', keep=True),
                ]),
                how='inner'
            ),
            auto_keep_all=True,
            depending_on_ids=[1, 2]
        ))

        fuzzy_node = graph.get_node(3)
        result = fuzzy_node.get_table_example(True)

        assert not fuzzy_node.node_stats.has_completed_last_run
        assert not fuzzy_node.node_stats.has_run_with_current_setup

        r = fuzzy_node.get_predicted_schema()
        assert r
        run_result = graph.run_graph()
        assert fuzzy_node.node_stats.has_completed_last_run
        assert fuzzy_node.node_stats.has_run_with_current_setup

        new_select = deepcopy(node_select)

        new_select.keep_missing = True

        graph.add_select(new_select)

        assert fuzzy_node.node_stats.has_completed_last_run
        assert not fuzzy_node.node_stats.has_run_with_current_setup

        fuzzy_node.get_resulting_data = MagicMock()
        result = fuzzy_node.get_table_example(True)
        fuzzy_node.get_resulting_data.assert_not_called()
        assert fuzzy_node.node_stats.has_completed_last_run
        assert not fuzzy_node.node_stats.has_run_with_current_setup
        assert result.columns


def _rewrite_atomic(path: Path, content: str) -> None:
    """write_text truncates under polars' live mmap (SIGBUS); write-then-replace is atomic.

    The same window exists in production (the read node's schema callback mmaps on a background
    thread while an external writer truncates the file); it predates this feature and is filed
    separately.

    Windows additionally *rejects* the replace (WinError 5) while that background scan holds the
    destination open, and the prefetch is orphaned by the schema_callback setter so it cannot be
    joined — hence the retry. On POSIX the replace succeeds first time and the loop never spins.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content)
    deadline = time.monotonic() + 10
    while True:
        try:
            os.replace(tmp, path)
            return
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.02)


def _csv_pattern(directory: Path) -> str:
    return os.path.join(str(directory), "**", "*.csv")


def _write_csv_files(directory: Path, names: list[str]) -> None:
    for i, name in enumerate(names, start=1):
        (directory / f"{name}.csv").write_text(f"id,val\n{i},{name}\n")


def create_graph_with_read(path, flow_id: int = 20, scan_mode: str = "single_file") -> FlowGraph:
    """Graph with a single csv read node — a file path in single-file mode, a directory otherwise."""
    graph = create_graph(execution_location="local", flow_id=flow_id)
    add_node_promise(graph, "read", node_id=1)
    graph.add_read(
        input_schema.NodeRead(
            flow_id=flow_id,
            node_id=1,
            received_file=input_schema.ReceivedTable(
                name=os.path.basename(str(path)) if scan_mode == "single_file" else "folder",
                path=str(path),
                file_type="csv",
                scan_mode=scan_mode,
            ),
        )
    )
    return graph


class TestSourceFileInfoFromPattern:
    """SourceFileInfo.from_pattern — the aggregate fingerprint a directory-mode read is tracked by."""

    def test_from_pattern_aggregates_count_mtime_and_size(self, tmp_path):
        """One snapshot summarises the whole match set: how many files, the newest mtime, total bytes."""
        _write_csv_files(tmp_path, ["a", "bb"])
        files = [tmp_path / "a.csv", tmp_path / "bb.csv"]

        info = SourceFileInfo.from_pattern(_csv_pattern(tmp_path))

        assert info.path == _csv_pattern(tmp_path)
        assert info.file_count == 2
        assert info.size == sum(os.path.getsize(f) for f in files)
        assert info.mtime == max(os.stat(f).st_mtime for f in files)
        assert info.has_changed() is False

    def test_has_changed_when_a_file_is_added(self, tmp_path):
        """A new file changes the data without changing any node setting."""
        _write_csv_files(tmp_path, ["a"])
        info = SourceFileInfo.from_pattern(_csv_pattern(tmp_path))

        _write_csv_files(tmp_path, ["b"])

        assert info.has_changed() is True

    def test_has_changed_when_a_file_is_removed(self, tmp_path):
        """A removed file lowers the count, which a single stat could never see."""
        _write_csv_files(tmp_path, ["a", "b"])
        info = SourceFileInfo.from_pattern(_csv_pattern(tmp_path))

        (tmp_path / "b.csv").unlink()

        assert info.has_changed() is True

    def test_has_changed_when_a_file_is_edited(self, tmp_path):
        """An in-place edit shows up through the total size and the newest mtime."""
        _write_csv_files(tmp_path, ["a", "b"])
        info = SourceFileInfo.from_pattern(_csv_pattern(tmp_path))

        edited = tmp_path / "a.csv"
        _rewrite_atomic(edited, "id,val\n1,a_much_longer_value\n")
        stat = os.stat(edited)
        os.utime(edited, (stat.st_atime + 10, stat.st_mtime + 10))

        assert info.has_changed() is True

    def test_zero_matches_is_a_valid_snapshot(self, tmp_path):
        """from_pattern never returns None: an empty directory is a state worth remembering, so a
        file appearing later reads as a change."""
        info = SourceFileInfo.from_pattern(_csv_pattern(tmp_path))

        assert info.file_count == 0
        assert info.has_changed() is False

        _write_csv_files(tmp_path, ["a"])
        assert info.has_changed() is True


class TestSourceFileInfoSerialization:
    """to_dict / from_dict must survive the file_count addition in both directions."""

    def test_round_trip_preserves_file_count(self, tmp_path):
        _write_csv_files(tmp_path, ["a", "b"])
        info = SourceFileInfo.from_pattern(_csv_pattern(tmp_path))

        restored = SourceFileInfo.from_dict(info.to_dict())

        assert restored == info
        assert restored.file_count == 2
        assert restored.has_changed() is False

    def test_legacy_dict_without_file_count_loads_as_single_file(self, tmp_path):
        """Flows saved before directory mode have no file_count key; they must keep comparing by a
        single stat rather than blowing up or being read as a zero-match pattern."""
        source = tmp_path / "data.csv"
        source.write_text("id,val\n1,a\n")
        stat = os.stat(source)

        restored = SourceFileInfo.from_dict({"path": str(source), "mtime": stat.st_mtime, "size": stat.st_size})

        assert restored.file_count is None
        assert restored.has_changed() is False


def _commit_source_snapshot(node) -> None:
    """Mirror the executor's own snapshot rule from ``execute``.

    The fingerprint is taken before the scan and committed to the state only when it is not
    None, so a source that cannot be stat'ed keeps the last known-good snapshot.
    """
    pending = node.executor._pending_source_snapshot()
    if pending is not None:
        node._execution_state.source_file_info = pending


class TestSourceFileSnapshotUpdate:
    """What the executor records after a run."""

    def test_failed_stat_does_not_erase_a_good_snapshot(self, tmp_path):
        """A path that cannot be stat'ed (moved away, permissions) must leave the last known-good
        fingerprint intact — overwriting it with None silently disables change detection."""
        source = tmp_path / "data.csv"
        source.write_text("id,val\n1,a\n")
        node = create_graph_with_read(source).get_node(1)
        state = node._execution_state

        _commit_source_snapshot(node)
        good_snapshot = state.source_file_info
        assert good_snapshot is not None

        # The snapshot re-resolves abs_file_path from path, so point path itself at the gap.
        node.setting_input.received_file.path = str(tmp_path / "not_there.csv")
        node.setting_input.received_file.name = "not_there.csv"
        _commit_source_snapshot(node)

        assert state.source_file_info is good_snapshot

    def test_directory_read_snapshots_the_aggregate(self, tmp_path):
        """A directory read must be fingerprinted by its whole match set, not by stat'ing a glob."""
        _write_csv_files(tmp_path, ["a", "b", "c"])
        node = create_graph_with_read(tmp_path, flow_id=21, scan_mode="directory").get_node(1)
        state = node._execution_state

        _commit_source_snapshot(node)

        assert state.source_file_info is not None
        assert state.source_file_info.file_count == 3


class TestSourceChangeOutranksCache:
    """Decision order: a changed source outranks the worker cache.

    The node holds a stored worker result and ``WorkerBackend.exists``, an HTTP call to the worker,
    is patched to stand in for 'the worker holds a cached result' — the behaviour under test is the
    ordering, not the lookup.
    """

    def _ran_once_with_cache(self, path, flow_id: int):
        """A cache-enabled read node that already ran once and recorded its source fingerprint."""
        node = create_graph_with_read(path, flow_id=flow_id).get_node(1)
        node.node_settings.cache_results = True
        node.results.result_ref = _stored_ref(node, "worker")
        state = node._execution_state
        _commit_source_snapshot(node)
        state.mark_successful()
        return node, state

    def test_changed_source_beats_a_cache_hit(self, tmp_path):
        """The node hash does not rotate when a file on disk changes, so a cache hit here would
        serve data the user just replaced."""
        source = tmp_path / "data.csv"
        source.write_text("id,val\n1,a\n")
        node, state = self._ran_once_with_cache(source, flow_id=22)

        _rewrite_atomic(source, "id,val\n1,a_changed_and_longer\n")
        stat = os.stat(source)
        os.utime(source, (stat.st_atime + 10, stat.st_mtime + 10))

        with _worker_holds_the_cache():
            decision = node.executor._decide_execution(
                state=state, performance_mode=False, force_refresh=False
            )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.SOURCE_FILE_CHANGED

    def test_deleted_source_beats_a_cache_hit(self, tmp_path):
        """Deliberate behaviour change: a vanished source re-runs (and fails loudly) instead of
        quietly serving a cached result for a file that no longer exists."""
        source = tmp_path / "data.csv"
        source.write_text("id,val\n1,a\n")
        node, state = self._ran_once_with_cache(source, flow_id=23)

        source.unlink()

        with _worker_holds_the_cache():
            decision = node.executor._decide_execution(
                state=state, performance_mode=False, force_refresh=False
            )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.SOURCE_FILE_CHANGED

    def test_missing_snapshot_still_honours_the_cache(self, tmp_path):
        """Known limitation, documented rather than fixed: after a restart there is no snapshot to
        compare against, so the cache is trusted and an out-of-band edit is not noticed."""
        source = tmp_path / "data.csv"
        source.write_text("id,val\n1,a\n")
        node = create_graph_with_read(source, flow_id=24).get_node(1)
        node.node_settings.cache_results = True
        node.results.result_ref = _stored_ref(node, "worker")
        state = node._execution_state
        state.mark_successful()
        assert state.source_file_info is None

        with _worker_holds_the_cache():
            decision = node.executor._decide_execution(
                state=state, performance_mode=False, force_refresh=False
            )

        assert decision.should_run is False
        assert decision.reason is None


class TestDirectoryFingerprintIdentity:
    """The aggregate fingerprint also hashes which files matched, not just the totals."""

    def test_mtime_preserving_equal_size_swap_is_detected(self, tmp_path):
        """rsync -a / cp -p style refresh: count, total size and newest mtime are all preserved,
        so only the digest over the (path, mtime, size) triples can see the change."""
        _write_csv_files(tmp_path, ["a", "b"])
        replaced = tmp_path / "b.csv"
        content = replaced.read_text()
        replaced_stat = os.stat(replaced)
        info = SourceFileInfo.from_pattern(_csv_pattern(tmp_path))
        assert info.has_changed() is False

        replaced.unlink()
        substitute = tmp_path / "c.csv"
        substitute.write_text(content)
        os.utime(substitute, (replaced_stat.st_atime, replaced_stat.st_mtime))

        assert info.has_changed() is True

    def test_legacy_digestless_snapshot_still_compares_aggregates(self, tmp_path):
        """A directory snapshot serialized before the digest existed keeps working on the
        aggregate comparison instead of always reading as changed."""
        _write_csv_files(tmp_path, ["a"])
        fresh = SourceFileInfo.from_pattern(_csv_pattern(tmp_path))
        legacy = SourceFileInfo.from_dict(
            {"path": fresh.path, "mtime": fresh.mtime, "size": fresh.size, "file_count": fresh.file_count}
        )

        assert legacy.files_digest is None
        assert legacy.has_changed() is False

        _write_csv_files(tmp_path, ["b"])
        assert legacy.has_changed() is True


class TestSourceChangeInvalidatesDownstream:
    """SOURCE_FILE_CHANGED must refresh the whole chain: the settings hash does not rotate on a
    disk change, so the executor bumps the cache epoch and lets the reset cascade downstream."""

    def _directory_graph_with_select(
        self, directory: Path, flow_id: int, execution_location: ExecutionLocationLit = "local"
    ) -> FlowGraph:
        graph = create_graph(execution_location=execution_location, flow_id=flow_id)
        add_node_promise(graph, "read", node_id=1)
        graph.add_read(
            input_schema.NodeRead(
                flow_id=flow_id,
                node_id=1,
                received_file=input_schema.ReceivedTable(
                    name="folder", path=str(directory), file_type="csv", scan_mode="directory"
                ),
            )
        )
        add_node_promise(graph, "select", node_id=2)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
        graph.add_select(
            input_schema.NodeSelect(
                flow_id=flow_id,
                node_id=2,
                depending_on_id=1,
                select_input=[transform_schema.SelectInput(old_name="id", new_name="id", keep=True)],
                keep_missing=False,
            )
        )
        return graph

    def test_added_file_reaches_the_downstream_node(self, tmp_path):
        """Regression: the downstream node used to SKIP and serve the pre-change frame."""
        _write_csv_files(tmp_path, ["a"])
        graph = self._directory_graph_with_select(tmp_path, flow_id=26)
        graph.run_graph()
        select_node = graph.get_node(2)
        assert select_node.get_resulting_data().collect().height == 1

        _write_csv_files(tmp_path, ["b"])
        graph.run_graph()

        assert graph.get_node(1).get_resulting_data().collect().height == 2
        assert select_node.get_resulting_data().collect().height == 2

    def test_repeated_changes_refresh_a_cached_remote_downstream(self, tmp_path):
        """Remote mode + cache_results downstream, changed repeatedly. The hash rotation must
        happen in run_graph's freshness pass, before the stages run — rotating inside the
        executor is reverted by _execute_single_node's hash save/restore, which froze the
        cached downstream node from the second source change onward."""
        _write_csv_files(tmp_path, ["a"])
        graph = self._directory_graph_with_select(tmp_path, flow_id=27, execution_location="remote")
        select_node = graph.get_node(2)
        select_node.node_settings.cache_results = True

        graph.run_graph()
        assert select_node.get_resulting_data().collect().height == 1

        for i, name in enumerate(["b", "c", "d"], start=2):
            _write_csv_files(tmp_path, [name])
            graph.run_graph()
            assert graph.get_node(1).get_resulting_data().collect().height == i
            assert select_node.get_resulting_data().collect().height == i, (
                f"cached downstream node froze after change {i - 1}"
            )


def _graph_with_two_sorts_and_output(out_dir: Path, flow_id: int) -> FlowGraph:
    """manual_input(1) -> sort(2) -> sort(3) -> output csv(4).

    Both sorts are wide transforms, so each runs REMOTE and leaves its own
    ``<hash>.arrow`` in the flow cache dir. The output node is in the "output"
    node group, so it re-runs on every graph run and always dereferences the
    plan node 3 is holding.
    """
    graph = create_graph(flow_id=flow_id, execution_location="remote")
    add_manual_input(
        graph,
        [{"name": "Charlie", "age": 25}, {"name": "Alice", "age": 30}, {"name": "Bob", "age": 41}],
        node_id=1,
    )

    add_node_promise(graph, "sort", node_id=2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            sort_input=[transform_schema.SortByInput(column="name", how="asc")],
        )
    )

    add_node_promise(graph, "sort", node_id=3)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=flow_id,
            node_id=3,
            depending_on_id=2,
            sort_input=[transform_schema.SortByInput(column="age", how="desc")],
        )
    )

    add_node_promise(graph, "output", node_id=4)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(3, 4))
    graph.add_output(
        input_schema.NodeOutput(
            flow_id=flow_id,
            node_id=4,
            output_settings={
                "name": "out.csv",
                "directory": str(out_dir),
                "file_type": "csv",
                "fields": [],
                "write_mode": "overwrite",
                "table_settings": {"delimiter": ",", "encoding": "utf-8"},
            },
        )
    )
    return graph


def _stored_file(node, protocol: bool) -> Path:
    """The worker's file of a node's stored result: its ``/v1`` location with the flag on, else the preview's path."""
    if protocol:
        return Path(WorkerLocation.model_validate_json(node.results.result_ref.location).path)
    return Path(node.results.example_data_path)


class TestMissingWorkerArtifact:
    """A removed worker result must make the node re-resolve, never fail the run.

    A node's remote result is a lazy scan of ``<cache>/<flow_id>/<hash>.arrow``.
    Core does not own that file: the 1-hour sweep in ``storage.cleanup_directories``,
    a worker shutdown or a manual delete can take it away while core still believes
    the node ran. Skipping such a node hands downstream a dangling scan.
    """

    def test_wiped_flow_cache_reruns_instead_of_failing(self, tmp_path):
        """Regression: wiping the whole flow cache dir used to fail the next run."""
        graph = _graph_with_two_sorts_and_output(tmp_path, flow_id=30)
        assert graph.run_graph().success

        cache_dir = storage.get_flow_cache_directory(graph.flow_id)
        assert list(cache_dir.glob("*.arrow")), "the run produced no worker artifacts to remove"

        shutil.rmtree(cache_dir)
        (tmp_path / "out.csv").unlink()

        run_info = graph.run_graph()
        failures = [(n.node_id, n.error) for n in run_info.node_step_result if not n.success]
        assert run_info.success, f"run failed after the cache was removed: {failures}"
        assert list(cache_dir.glob("*.arrow")), "nodes did not re-materialise their results"
        assert (tmp_path / "out.csv").exists(), "the output node never resolved its input"

    def test_only_the_node_with_a_missing_artifact_reruns(self, tmp_path, protocol):
        """The gate must not degrade into an unconditional re-run of the graph."""
        graph = _graph_with_two_sorts_and_output(tmp_path, flow_id=31)
        assert graph.run_graph().success

        removed = _stored_file(graph.get_node(2), protocol)
        untouched = _stored_file(graph.get_node(3), protocol)
        untouched_mtime = untouched.stat().st_mtime_ns

        removed.unlink()
        assert graph.run_graph().success

        assert removed.exists(), "node 2 should have re-run and rewritten its artifact"
        assert untouched.stat().st_mtime_ns == untouched_mtime, "node 3 should still have skipped"

    def test_decide_execution_flags_a_missing_artifact(self, tmp_path, protocol):
        """The decision itself: present artifact -> SKIP, removed artifact -> CACHE_MISSING."""
        graph = _graph_with_two_sorts_and_output(tmp_path, flow_id=32)
        assert graph.run_graph().success

        node = graph.get_node(2)
        executor = NodeExecutor(node)
        decision = executor._decide_execution(
            state=node._execution_state,
            performance_mode=False,
            force_refresh=False,
        )
        assert decision.should_run is False

        _stored_file(node, protocol).unlink()
        decision = executor._decide_execution(
            state=node._execution_state,
            performance_mode=False,
            force_refresh=False,
        )
        assert decision.should_run is True
        assert decision.reason == InvalidationReason.CACHE_MISSING


@pytest.fixture
def unreachable_worker():
    """``worker:gone``, a worker backend at a port nothing listens on, registered for the test."""
    url = closed_url()
    registry.reset()
    registry.register_backend("worker:gone", lambda: WorkerBackend(url=url, backend_id="worker:gone"))
    yield "worker:gone"
    registry.reset()


class TestHeldResultsOnAnUnreachableBackend:
    """A flow moved off a worker that cannot be reached runs on its new backend with a cold cache.

    What the nodes held on the old worker is dropped when its release cannot reach it, and nothing asks that
    worker whether a result still exists.
    """

    @staticmethod
    def _held_on(backend_id: str, result_id: str, kind: Literal["table", "blob"] = "table") -> ResultRef:
        location = WorkerLocation(kind="file" if kind == "table" else "blob", path=f"/gone/{result_id}")
        return ResultRef(
            backend_id=backend_id,
            result_id=result_id,
            schema=[],
            row_count=2 if kind == "table" else None,
            state="materialised",
            location=location.model_dump_json().encode(),
            kind=kind,
        )

    @staticmethod
    def _train_and_apply(flow_id: int, execution_location: str) -> FlowGraph:
        """Manual input 1 -> Train Model 10 (linear regression on ``x1``) -> Apply Model 20 scoring into ``pred``."""
        graph = FlowGraph(
            schemas.FlowSettings(flow_id=flow_id, name="moved_flow", path=".", execution_location=execution_location)
        )
        add_manual_input(graph, [{"x1": 1.0, "x2": 2.0, "y": 5.0}, {"x1": 3.0, "x2": 4.0, "y": 13.0}], node_id=1)
        add_node_promise(graph, "train_model", node_id=10)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 10))
        graph.add_train_model(
            input_schema.NodeTrainModel(
                flow_id=flow_id,
                node_id=10,
                depending_on_id=1,
                train_input=input_schema.TrainModelSettings(
                    target_column="y",
                    feature_columns=["x1"],
                    model_type="linear_regression",
                    params={"add_bias": True},
                ),
            )
        )
        add_node_promise(graph, "apply_model", node_id=20)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(10, 20))
        graph.add_apply_model(
            input_schema.NodeApplyModel(
                flow_id=flow_id,
                node_id=20,
                depending_on_id=10,
                apply_input=input_schema.ApplyModelSettings(
                    source="upstream", upstream_node_id=10, output_column="pred"
                ),
            )
        )
        return graph

    def test_a_flow_moved_off_an_unreachable_worker_runs_on_local(self, protocol, unreachable_worker, monkeypatch):
        asked: list[tuple[str, str, type | None]] = []

        def recorded(name: str):
            original = getattr(WorkerBackend, name)

            def call(self, ref):
                if self.id != unreachable_worker:
                    return original(self, ref)
                try:
                    answer = original(self, ref)
                except Exception as e:
                    asked.append((name, ref.kind, type(e)))
                    raise
                asked.append((name, ref.kind, None))
                return answer

            return call

        for name in ("exists", "release"):
            monkeypatch.setattr(WorkerBackend, name, recorded(name))

        graph = self._train_and_apply(5180, unreachable_worker)
        try:
            for node_id in (1, 10, 20):
                node = graph.get_node(node_id)
                node.results.result_ref = self._held_on(unreachable_worker, node.hash)
                node._execution_state.has_run_with_current_setup = True
            trained = graph.get_node(10)
            trained.results.model_ref = self._held_on(unreachable_worker, f"{trained.hash}_model", kind="blob")
            # The model its own fit answered, so the node releases it as it does on a real run.
            trained._operation_refs["model"] = trained.results.model_ref

            graph.execution_location = "local"
            run_info = graph.run_graph()

            assert run_info.success, [(step.node_id, step.error) for step in run_info.node_step_result]
            assert all(step.success for step in run_info.node_step_result)
            for node_id in (1, 10, 20):
                assert graph.get_node(node_id).results.result_ref.backend_id == "local"
            assert graph.get_node(10).results.model_ref.backend_id == "local"
            scored = graph.get_node(20).get_resulting_data().collect()
            assert scored["pred"].to_list() == pytest.approx([5.0, 13.0])
            assert not [call for call in asked if call[0] == "exists"]
            assert sorted(kind for name, kind, _ in asked if name == "release") == ["blob", "table", "table", "table"]
            assert {raised for _, _, raised in asked} == {ComputeUnavailable}
        finally:
            graph.close_flow()


class TestDeferredUntilRun:
    """A node seeded with a placeholder output only executes inside a real flow run."""

    @staticmethod
    def _seeded_select(flow_id: int):
        from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

        graph = create_graph_with_select(flow_id=flow_id, execution_location="local")
        node = graph.get_node(2)
        seed = FlowDataEngine.create_from_schema([FlowfileColumn.from_input("name", "String")])
        node.results.resulting_data = seed
        node._named_schemas = {"output-1": [FlowfileColumn.from_input("stale", "Int64")]}
        node.deferred_until_run = True
        return graph, node, seed

    def test_unseeded_deferred_node_raises_instead_of_executing(self):
        from flowfile_core.flowfile.flow_node.flow_node import DeferredNodeError

        graph = create_graph_with_select(flow_id=40, execution_location="local")
        node = graph.get_node(2)
        node.results.resulting_data = None
        node.results.errors = None
        node.deferred_until_run = True

        with pytest.raises(DeferredNodeError, match="node 2 is deferred until the flow runs"):
            node.get_resulting_data()
        assert node.results.resulting_data is None, "no empty engine may be memoized for a deferred node"
        assert node.results.errors is None
        with pytest.raises(DeferredNodeError):
            node._predicted_data_getter()

    def test_seeded_node_serves_the_seed(self):
        graph, node, seed = self._seeded_select(flow_id=41)
        assert node.get_resulting_data() is seed
        assert node.deferred_until_run is True
        assert node.get_resulting_data().data_frame.collect().height == 0

    def test_run_graph_clears_the_flag_and_the_seeded_handle_schemas(self):
        graph, node, _ = self._seeded_select(flow_id=42)
        assert graph.run_graph().success

        assert node.deferred_until_run is False
        assert "output-1" not in node._named_schemas
        assert node.get_resulting_data().data_frame.collect()["name"].to_list() == ["Alice"]

    def test_reset_re_arms_a_node_placed_deferred(self):
        graph, node, _ = self._seeded_select(flow_id=45)
        node.placed_deferred = True
        assert graph.run_graph().success
        assert node.deferred_until_run is False

        node.reset(deep=True)

        assert node.deferred_until_run is True
        assert node.results.resulting_data is None
        assert graph.run_graph().success
        assert node.deferred_until_run is False
        assert node.get_resulting_data().data_frame.collect()["name"].to_list() == ["Alice"]

    def test_reset_inside_its_own_run_does_not_re_arm_the_node(self):
        graph, node, _ = self._seeded_select(flow_id=46)
        node.placed_deferred = True
        node._cache_epoch += 1  # a stale hash: the executor's own reset() drops the seed and re-arms the flag
        assert node.needs_reset()

        assert graph.run_graph().success

        assert node.deferred_until_run is False
        assert node.get_resulting_data().data_frame.collect()["name"].to_list() == ["Alice"]

    def test_reset_leaves_a_node_not_placed_deferred_alone(self):
        graph = create_graph_with_select(flow_id=47, execution_location="local")
        node = graph.get_node(2)
        assert graph.run_graph().success

        node.reset(deep=True)

        assert node.placed_deferred is False and node.deferred_until_run is False

    def test_prepare_keeps_handle_schemas_of_non_deferred_nodes(self):
        from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

        graph = create_graph_with_select(flow_id=43, execution_location="local")
        node = graph.get_node(2)
        handle_schemas = {"output-1": [FlowfileColumn.from_input("kept", "Int64")]}
        node._named_schemas = handle_schemas

        NodeExecutor(node)._prepare_for_execution(node._execution_state)

        assert node._named_schemas is handle_schemas
        assert [c.column_name for c in node.schema_for_handle("output-1")] == ["kept"]

    def test_function_derived_schema_callback_refuses_a_deferred_node(self):
        from flowfile_core.flowfile.flow_node.flow_node import DeferredNodeError

        graph = create_graph_with_select(flow_id=44, execution_location="local")
        node = graph.get_node(2)
        calls = []

        def fn():
            calls.append(1)
            return FlowDataEngine()

        callback = node.create_schema_callback_from_function(fn)
        node.deferred_until_run = True

        with pytest.raises(DeferredNodeError):
            callback()
        assert calls == []


def create_graph_with_sort_read_twice(execution_location: ExecutionLocationLit = "local") -> FlowGraph:
    """manual_input(1) -> sort(2) -> sort(3), and sort(2) -> sort(4): two consumers, each stored for its preview."""
    graph = create_graph_with_sort(flow_id=44, execution_location=execution_location)
    add_sort(graph, node_id=3, depending_on_id=2, column="age")
    add_sort(graph, node_id=4, depending_on_id=2)
    return graph


class TestFanoutCountsTheConsumersThatRun:
    """``fanout`` counts the plans among the nodes the run executes that execute a node's result, every consumer's
    without a run."""

    def test_only_the_consumers_among_the_running_nodes_count(self):
        graph = create_graph_with_sort_read_twice()
        sort_node = graph.get_node(2)
        context = graph.execution_context

        assert hints_for(sort_node, context, False).fanout == 2
        assert hints_for(sort_node, context, False, running={1, 2, 3}).fanout == 1
        assert hints_for(sort_node, context, False, running={1, 2}).fanout == 0
        assert hints_for(sort_node, context, False, running={1, 2, 3, 4, 99}).fanout == 2

    def test_a_lineage_run_and_a_single_node_fetch_count_the_consumers_they_run(self, monkeypatch):
        """A run over nodes 1 to 3 counts the select only; a fetch of the sort runs no consumer at all."""
        from flowfile_core.flowfile.flow_node import executor as executor_module

        graph = create_graph_with_sort_read_twice()
        recorded: dict[int, int] = {}
        real_hints_for = executor_module.hints_for

        def recording_hints_for(node, context, performance_mode, running=None):
            hints = real_hints_for(node, context, performance_mode, running)
            recorded[node.node_id] = hints.fanout
            return hints

        monkeypatch.setattr(executor_module, "hints_for", recording_hints_for)

        assert graph.run_graph(node_ids=[1, 2, 3]).success
        assert recorded[2] == 1
        assert recorded[1] == 1

        graph.get_node(1).invalidate_cache()  # Development mode would otherwise skip the nodes that ran
        assert graph.run_graph().success
        assert recorded[2] == 2

        graph.trigger_fetch_node(2)
        assert recorded[2] == 0


def add_group_by(graph: FlowGraph, node_id: int, depending_on_id: int) -> None:
    add_node_promise(graph, "group_by", node_id=node_id)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))
    graph.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[transform_schema.AggColl("val", "groupby"), transform_schema.AggColl("id", "sum", "id_sum")]
            ),
        )
    )


def add_csv_output(graph: FlowGraph, node_id: int, depending_on_id: int, directory: Path) -> None:
    add_node_promise(graph, "output", node_id=node_id)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))
    graph.add_output(
        input_schema.NodeOutput(
            flow_id=graph.flow_id,
            node_id=node_id,
            output_settings=input_schema.OutputSettings(
                name=f"out_{node_id}.csv",
                directory=str(directory),
                file_type="csv",
                write_mode="overwrite",
                table_settings=input_schema.OutputCsvTable(),
            ),
        )
    )


def read_into_four_group_bys(tmp_path: Path, mode: ExecutionModeLit, flow_id: int = 45) -> FlowGraph:
    """read(1) -> group_by(2..5), nothing below them."""
    graph = create_graph_with_csv_read(tmp_path, flow_id, mode)
    for node_id in range(2, 6):
        add_group_by(graph, node_id, depending_on_id=1)
    return graph


def read_select_group_by_output(tmp_path: Path, mode: ExecutionModeLit) -> FlowGraph:
    """read(1) -> select(2) -> group_by(3) -> output(4)."""
    graph = create_graph_with_csv_read(tmp_path, 46, mode)
    add_node_promise(graph, "select", node_id=2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=46, node_id=2, depending_on_id=1, select_input=[transform_schema.SelectInput("id"),
                                                                     transform_schema.SelectInput("val")]
        )
    )
    add_group_by(graph, 3, depending_on_id=2)
    add_csv_output(graph, 4, depending_on_id=3, directory=tmp_path)
    return graph


def read_two_group_bys_two_outputs(tmp_path: Path, mode: ExecutionModeLit) -> FlowGraph:
    """read(1) -> group_by(2) -> output(4), and read(1) -> group_by(3) -> output(5)."""
    graph = create_graph_with_csv_read(tmp_path, 47, mode)
    add_group_by(graph, 2, depending_on_id=1)
    add_group_by(graph, 3, depending_on_id=1)
    add_csv_output(graph, 4, depending_on_id=2, directory=tmp_path)
    add_csv_output(graph, 5, depending_on_id=3, directory=tmp_path)
    return graph


def read_self_join_two_outputs(tmp_path: Path, mode: ExecutionModeLit) -> FlowGraph:
    """read(1) feeding both sides of join(2), and join(2) -> output(3), output(4)."""
    graph = create_graph_with_csv_read(tmp_path, 48, mode)
    add_node_promise(graph, "join", node_id=2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2, "main"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2, "right"))
    graph.add_join(
        input_schema.NodeJoin(
            flow_id=48,
            node_id=2,
            depending_on_ids=[1, 1],
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("id", "id")],
                left_select=[transform_schema.SelectInput("id"), transform_schema.SelectInput("val")],
                right_select=[transform_schema.SelectInput("id"), transform_schema.SelectInput("val", keep=False)],
                how="inner",
            ),
        )
    )
    add_csv_output(graph, 3, depending_on_id=2, directory=tmp_path)
    add_csv_output(graph, 4, depending_on_id=2, directory=tmp_path)
    return graph


def _fanout(graph: FlowGraph, node_id: int) -> int:
    performance_mode = graph.flow_settings.execution_mode == "Performance"
    return hints_for(graph.get_node(node_id), graph.execution_context, performance_mode).fanout


class TestFanoutCountsThePlansThatExecuteTheResult:
    """A collecting consumer (a sink, an operation, a stored result) executes a node's plan once; a deferred one
    executes it in every plan that executes the consumer, so it passes its own fan-out on."""

    def test_a_performance_read_into_group_bys_with_no_sink_has_no_fanout(self, tmp_path):
        graph = read_into_four_group_bys(tmp_path, "Performance")
        hints = hints_for(graph.get_node(1), graph.execution_context, performance_mode=True)

        assert hints == SubmitHints(must_materialise=False, wide=True, fanout=0, preview_rows=None)
        assert not worker_stores(hints)

    def test_a_performance_chain_into_one_output_has_fanout_one(self, tmp_path):
        graph = read_select_group_by_output(tmp_path, "Performance")

        assert [_fanout(graph, node_id) for node_id in (1, 2, 3)] == [1, 1, 1]

    def test_a_performance_read_into_two_sinks_has_fanout_two(self, tmp_path):
        graph = read_two_group_bys_two_outputs(tmp_path, "Performance")
        hints = hints_for(graph.get_node(1), graph.execution_context, performance_mode=True)

        assert hints.fanout == 2
        assert worker_stores(hints)

    def test_a_stored_join_counts_as_one_plan_of_its_input(self, tmp_path):
        graph = read_self_join_two_outputs(tmp_path, "Performance")

        assert _fanout(graph, 2) == 2
        assert _fanout(graph, 1) == 1

    @pytest.mark.parametrize(
        ("build", "expected"),
        [
            (read_into_four_group_bys, {1: 4}),
            (read_select_group_by_output, {1: 1, 2: 1}),
            (read_two_group_bys_two_outputs, {1: 2}),
            (read_self_join_two_outputs, {1: 1, 2: 2}),
        ],
    )
    def test_development_mode_counts_every_wide_consumer_its_preview_stores(self, tmp_path, build, expected):
        graph = build(tmp_path, "Development")

        assert {node_id: _fanout(graph, node_id) for node_id in expected} == expected

    def test_a_performance_run_with_no_sink_leaves_the_read_deferred(self, tmp_path, monkeypatch, protocol):
        graph = read_into_four_group_bys(tmp_path, "Performance", flow_id=49)
        backend = registry.backend(graph.execution_context.backend_id)
        submits = spy(monkeypatch, backend, "submit")

        assert graph.execution_context.backend_id == "local"
        assert graph.run_graph().success

        read_node = graph.get_node(1)
        read_hints = [call.args[2] for call in submits if call.args[3] == read_node.hash]
        assert [hints.fanout for hints in read_hints] == [0]
        assert read_node.results.result_ref.state == "deferred"
        assert all(_held_ref_state(graph, node_id) == "deferred" for node_id in range(2, 6))

    def test_an_operation_consumer_whose_result_is_stored_counts_once(self, tmp_path):
        graph = create_graph_with_csv_read(tmp_path, 50, "Performance")
        add_polars_code(graph, 2, 1, "output_df = input_df")
        graph.get_node(2).node_settings.cache_results = True
        consumer = hints_for(graph.get_node(2), graph.execution_context, performance_mode=True)

        assert worker_stores(consumer)
        assert _fanout(graph, 1) == 1


COMPOSING_NODE_TYPES = frozenset(
    {
        "cross_join",
        "data_cleansing",
        "dynamic_rename",
        "evaluate_model",
        "explode_hierarchy",
        "explore_data",
        "filter",
        "formula",
        "gate",
        "graph_solver",
        "group_by",
        "join",
        "multi_field_formula",
        "pivot",
        "record_count",
        "record_id",
        "sample",
        "select",
        "sort",
        "text_to_rows",
        "union",
        "unique",
        "unpivot",
        "wait_for",
        "window_functions",
    }
)
"""Node types with inputs whose function composes its plan on its inputs' plans, so a deferred one inlines them."""


def test_every_node_type_with_inputs_is_classified_for_fanout():
    """A new node type must be placed: one that runs its inputs itself but is missing from ``OPERATION_NODE_TYPES``
    would pass its consumers' fan-out on, and a wide input several plans read would be computed by each."""
    _, templates, _ = get_all_standard_nodes()
    with_inputs = {item for item, template in templates.items() if template.input > 0}
    classified = [IN_CORE_NODE_TYPES, OPERATION_NODE_TYPES, COMPOSING_NODE_TYPES]
    placements = {item: [group for group in classified if item in group] for item in with_inputs}

    assert [item for item, groups in placements.items() if len(groups) != 1] == []
    assert set().union(*classified) - with_inputs == set()


@pytest.mark.parametrize("must_materialise", [False, True])
@pytest.mark.parametrize("wide", [False, True])
@pytest.mark.parametrize("fanout", [0, 1, 2, 3])
@pytest.mark.parametrize("preview_rows", [None, 0, 100])
def test_core_and_the_worker_store_the_same_plans(must_materialise, wide, fanout, preview_rows):
    fields = dict(must_materialise=must_materialise, wide=wide, fanout=fanout, preview_rows=preview_rows)

    assert worker_stores(SubmitHints(**fields)) == _materialise(compute_protocol.SubmitHints(**fields))


def test_a_long_chain_stays_within_the_recursion_limit():
    """``hints_for`` walks a chain deeper than the interpreter's recursion limit without recursing."""
    from types import SimpleNamespace
    import sys

    def chain_node(node_id, consumer, node_type="select"):
        return SimpleNamespace(
            node_id=node_id,
            node_type=node_type,
            leads_to_nodes=[consumer] if consumer else [],
            node_settings=SimpleNamespace(cache_results=False),
            _boundary_inputs=[],
            node_default=SimpleNamespace(transform_type="narrow"),
            setting_input=None,
        )

    node = chain_node(0, None, node_type="output")
    for node_id in range(1, sys.getrecursionlimit() * 2):
        node = chain_node(node_id, node)

    assert hints_for(node, SimpleNamespace(preview_rows=None), performance_mode=True).fanout == 1


if __name__ == "__main__":
    pytest.main([__file__])
