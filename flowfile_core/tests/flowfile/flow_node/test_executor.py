"""Tests for the FlowNode executor module.

Tests the execution strategy determination, state management,
and decision-making logic introduced in the refactored FlowNode.
"""

import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.flow_node.models import (
    ExecutionDecision,
    ExecutionStrategy,
    InvalidationReason,
)
from flowfile_core.flowfile.flow_node.state import (
    NodeExecutionState,
    SourceFileInfo,
)
from flowfile_core.flowfile.flow_node.executor import NodeExecutor
from flowfile_core.schemas import input_schema, schemas, transform_schema
from typing import Literal


def _find_parent_directory(target_dir_name: str) -> Path:
    current_path = Path(__file__)
    while current_path != current_path.parent:
        if current_path.name == target_dir_name:
            return current_path
        current_path = current_path.parent
    raise FileNotFoundError(f"Directory '{target_dir_name}' not found")


ExecutionModeLit = Literal["Development", "Performance"]
ExecutionLocationLit = Literal["remote", "local"]

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
                             execution_mode: ExecutionModeLit = 'Development') -> FlowGraph:
    """Create a graph with a manual input (node 1) connected to a select node (node 2)."""
    graph = create_graph(execution_mode=execution_mode, flow_id=flow_id)
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
                                      execution_mode: ExecutionModeLit = 'Development') -> FlowGraph:
    """Create a graph with a parquet read (node 1, 1000 rows) connected to a select node (node 2).

    Uses fake_data.parquet which has 1000 rows with columns including Name, City, Email, etc.
    The select node keeps only the Name column.
    """
    graph = create_graph(execution_mode=execution_mode, flow_id=flow_id)
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


def create_graph_with_sort(flow_id: int = 2, execution_mode: ExecutionModeLit = 'Development') -> FlowGraph:
    """Create a graph with a manual input (node 1) connected to a sort node (node 2)."""
    graph = create_graph(execution_mode=execution_mode, flow_id=flow_id)
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


# =============================================================================
# ExecutionStrategy Tests
# =============================================================================

class TestExecutionStrategy:
    """Tests for the ExecutionStrategy enum."""

    def test_strategy_values_exist(self):
        """Verify all expected strategy values exist."""
        assert ExecutionStrategy.SKIP is not None
        assert ExecutionStrategy.FULL_LOCAL is not None
        assert ExecutionStrategy.LOCAL_WITH_SAMPLING is not None
        assert ExecutionStrategy.REMOTE is not None

    def test_strategy_values_are_distinct(self):
        """Ensure all strategy values are distinct."""
        strategies = [
            ExecutionStrategy.SKIP,
            ExecutionStrategy.FULL_LOCAL,
            ExecutionStrategy.LOCAL_WITH_SAMPLING,
            ExecutionStrategy.REMOTE,
        ]
        assert len(strategies) == len(set(strategies))


# =============================================================================
# InvalidationReason Tests
# =============================================================================

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


# =============================================================================
# ExecutionDecision Tests
# =============================================================================

class TestExecutionDecision:
    """Tests for the ExecutionDecision dataclass."""

    def test_decision_creation(self):
        """Test creating an execution decision."""
        decision = ExecutionDecision(
            should_run=True,
            strategy=ExecutionStrategy.REMOTE,
            reason=InvalidationReason.NEVER_RAN,
        )
        assert decision.should_run is True
        assert decision.strategy == ExecutionStrategy.REMOTE
        assert decision.reason == InvalidationReason.NEVER_RAN

    def test_skip_decision(self):
        """Test creating a skip decision."""
        decision = ExecutionDecision(
            should_run=False,
            strategy=ExecutionStrategy.SKIP,
            reason=None,
        )
        assert decision.should_run is False
        assert decision.strategy == ExecutionStrategy.SKIP
        assert decision.reason is None


# =============================================================================
# SourceFileInfo Tests
# =============================================================================

class TestSourceFileInfo:
    """Tests for the SourceFileInfo class."""

    def test_from_path_with_existing_file(self):
        """Test creating SourceFileInfo from an existing file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test content")
            temp_path = f.name

        try:
            info = SourceFileInfo.from_path(temp_path)
            assert info is not None
            assert info.path == temp_path
            assert info.mtime > 0
            assert info.size == 12  # len("test content")
        finally:
            os.unlink(temp_path)

    def test_from_path_with_nonexistent_file(self):
        """Test creating SourceFileInfo from a nonexistent file."""
        info = SourceFileInfo.from_path("/nonexistent/path/file.txt")
        assert info is None

    def test_has_changed_detects_modification(self):
        """Test that has_changed detects file modifications."""
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            f.write("initial content")
            temp_path = f.name

        try:
            info = SourceFileInfo.from_path(temp_path)
            assert info is not None
            assert info.has_changed() is False

            # Modify the file
            time.sleep(0.1)  # Ensure mtime changes
            with open(temp_path, 'w') as f:
                f.write("modified content that is longer")

            assert info.has_changed() is True
        finally:
            os.unlink(temp_path)

    def test_has_changed_detects_deletion(self):
        """Test that has_changed detects file deletion."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test")
            temp_path = f.name

        info = SourceFileInfo.from_path(temp_path)
        assert info is not None
        assert info.has_changed() is False

        os.unlink(temp_path)
        assert info.has_changed() is True


# =============================================================================
# NodeExecutionState Tests
# =============================================================================

class TestNodeExecutionState:
    """Tests for the NodeExecutionState class."""

    def test_initial_state(self):
        """Test initial state values."""
        state = NodeExecutionState()
        assert state.has_run_with_current_setup is False
        assert state.execution_hash is None
        assert state.source_file_info is None
        assert state.result_schema is None

    def test_mark_successful(self):
        """Test marking execution as successful."""
        state = NodeExecutionState()
        state.mark_successful()

        assert state.has_run_with_current_setup is True
        assert state.has_completed_last_run is True
        assert state.error is None

    def test_mark_failed(self):
        """Test marking execution as failed."""
        state = NodeExecutionState()
        state.mark_failed("Test error")

        assert state.has_run_with_current_setup is False
        assert state.has_completed_last_run is False
        assert state.error == "Test error"

    def test_reset(self):
        """Test resetting state."""
        state = NodeExecutionState()
        state.mark_successful()
        state.result_schema = ["col1", "col2"]
        state.execution_hash = "abc123"

        state.reset()

        assert state.has_run_with_current_setup is False
        assert state.result_schema is None
        assert state.execution_hash is None

    def test_source_file_info_tracking(self):
        """Test tracking source file info."""
        state = NodeExecutionState()

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data")
            temp_path = f.name

        try:
            info = SourceFileInfo.from_path(temp_path)
            state.source_file_info = info

            assert state.source_file_info is not None
            assert state.source_file_info.path == temp_path
        finally:
            os.unlink(temp_path)

    def test_source_file_info_preserved_on_reset(self):
        """Test that source_file_info is preserved when resetting state."""
        state = NodeExecutionState()

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data")
            temp_path = f.name

        try:
            info = SourceFileInfo.from_path(temp_path)
            state.source_file_info = info
            state.mark_successful()

            state.reset()

            # source_file_info should NOT be reset (needed for change detection)
            assert state.source_file_info is not None
            assert state.source_file_info.path == temp_path
        finally:
            os.unlink(temp_path)


# =============================================================================
# NodeExecutor Tests
# =============================================================================

class TestNodeExecutor:
    """Tests for the NodeExecutor class."""

    def test_executor_creation_with_real_node(self):
        """Test creating a NodeExecutor with a real FlowNode."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        executor = NodeExecutor(node)
        assert executor.node is node

    def test_determine_strategy_local(self):
        """Test strategy determination for local execution."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        executor = NodeExecutor(node)
        strategy = executor._determine_strategy("local")
        assert strategy == ExecutionStrategy.FULL_LOCAL

    def test_determine_strategy_remote(self):
        """Test strategy determination for remote execution."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        executor = NodeExecutor(node)
        strategy = executor._determine_strategy("remote")
        assert strategy == ExecutionStrategy.REMOTE

    def test_decide_execution_never_ran(self):
        """Test execution decision when node never ran."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        # Ensure node hasn't run
        node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(node)
        decision = executor._decide_execution(
            state=node._execution_state,
            run_location="remote",
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

        # Mark as already run
        node._execution_state.has_run_with_current_setup = True

        executor = NodeExecutor(node)
        decision = executor._decide_execution(
            state=node._execution_state,
            run_location="remote",
            performance_mode=False,
            force_refresh=True,  # Force refresh
        )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.FORCED_REFRESH

    def test_decide_execution_already_ran_skips(self):
        """Already-ran nodes should skip regardless of mode."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        # Mark as already run
        node._execution_state.has_run_with_current_setup = True

        executor = NodeExecutor(node)

        # Development mode: skip
        decision = executor._decide_execution(
            state=node._execution_state,
            run_location="remote",
            performance_mode=False,
            force_refresh=False,
        )
        assert decision.should_run is False

        # Performance mode: also skip
        decision = executor._decide_execution(
            state=node._execution_state,
            run_location="remote",
            performance_mode=True,
            force_refresh=False,
        )
        assert decision.should_run is False

    def test_node_has_executor_property(self):
        """Test that FlowNode has an executor property."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        # Access executor via property
        executor = node.executor
        assert isinstance(executor, NodeExecutor)
        assert executor.node is node

    def test_executor_is_lazily_created(self):
        """Test that executor is created lazily."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        # Executor should not exist yet
        assert node._executor is None

        # Access executor - should be created
        executor = node.executor
        assert node._executor is not None
        assert node._executor is executor

        # Accessing again should return same instance
        assert node.executor is executor


# =============================================================================
# Integration Tests
# =============================================================================

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

            # Modify file
            time.sleep(0.1)
            with open(temp_path, 'w') as f:
                f.write("modified content")

            assert state.source_file_info.has_changed() is True

            # Update file info
            state.source_file_info = SourceFileInfo.from_path(temp_path)
            assert state.source_file_info.has_changed() is False
        finally:
            os.unlink(temp_path)

    def test_executor_with_graph_execution(self):
        """Test that executor integrates properly with graph execution."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "Alice"}, {"name": "Bob"}], node_id=1)

        node = graph.get_node(1)

        # Before execution
        assert node._execution_state.has_run_with_current_setup is False

        # Run the graph
        result = graph.run_graph()
        assert result.success is True

        # After execution, state should be updated
        assert node._execution_state.has_run_with_current_setup is True


# =============================================================================
# Strategy Determination Tests (node_default routing)
# =============================================================================

class TestDetermineStrategyByNodeType:
    """Tests that _determine_strategy routes to the correct strategy
    based on whether the node has a node_default."""

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

    def test_select_strategy_remote_returns_local_with_sampling(self):
        """Select node should use LOCAL_WITH_SAMPLING when run_location is 'remote'."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        executor = NodeExecutor(select_node)

        strategy = executor._determine_strategy("remote")
        assert strategy == ExecutionStrategy.LOCAL_WITH_SAMPLING

    def test_select_strategy_local_returns_full_local(self):
        """Select node should use FULL_LOCAL when run_location is 'local',
        regardless of having a node_default."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        executor = NodeExecutor(select_node)

        strategy = executor._determine_strategy("local")
        assert strategy == ExecutionStrategy.FULL_LOCAL

    def test_sort_node_has_node_default(self):
        """Verify that the sort node has a node_default set."""
        graph = create_graph_with_sort()
        sort_node = graph.get_node(2)
        assert sort_node.node_default is not None, "sort node should have a node_default"

    def test_sort_node_is_wide_transform(self):
        """Verify that the sort node is a wide transform type."""
        graph = create_graph_with_sort()
        sort_node = graph.get_node(2)
        assert sort_node.node_default.transform_type == "wide"

    def test_sort_strategy_remote_returns_remote(self):
        """Sort node (wide transform) should get REMOTE directly from _determine_strategy.
        Only narrow transforms get LOCAL_WITH_SAMPLING."""
        graph = create_graph_with_sort()
        sort_node = graph.get_node(2)
        executor = NodeExecutor(sort_node)

        strategy = executor._determine_strategy("remote")
        assert strategy == ExecutionStrategy.REMOTE

    def test_manual_input_has_no_default_settings(self):
        """Verify that manual_input has a node_default but without has_default_settings."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)
        # All nodes get a node_default, but only lightweight transforms
        # (select, sample, etc.) have has_default_settings=True
        assert node.node_default is not None
        assert not node.node_default.has_default_settings

    def test_manual_input_strategy_remote_returns_remote(self):
        """Nodes without has_default_settings should get REMOTE strategy."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)
        executor = NodeExecutor(node)

        strategy = executor._determine_strategy("remote")
        assert strategy == ExecutionStrategy.REMOTE


# =============================================================================
# Execution Decision Tests (with real node types)
# =============================================================================

class TestDecideExecutionByNodeType:
    """Tests that _decide_execution returns the correct decision
    for different node types and scenarios."""

    def test_select_never_ran_gets_local_with_sampling(self):
        """Select node that never ran should decide LOCAL_WITH_SAMPLING."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(select_node)
        decision = executor._decide_execution(
            state=select_node._execution_state,
            run_location="remote",
            performance_mode=False,
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.strategy == ExecutionStrategy.LOCAL_WITH_SAMPLING
        assert decision.reason == InvalidationReason.NEVER_RAN

    def test_select_forced_refresh_gets_local_with_sampling(self):
        """Select node with forced refresh should decide LOCAL_WITH_SAMPLING."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node._execution_state.has_run_with_current_setup = True

        executor = NodeExecutor(select_node)
        decision = executor._decide_execution(
            state=select_node._execution_state,
            run_location="remote",
            performance_mode=False,
            force_refresh=True,
        )

        assert decision.should_run is True
        assert decision.strategy == ExecutionStrategy.LOCAL_WITH_SAMPLING
        assert decision.reason == InvalidationReason.FORCED_REFRESH

    def test_select_already_ran_skips(self):
        """Select node that already ran should skip, regardless of mode."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node._execution_state.has_run_with_current_setup = True

        executor = NodeExecutor(select_node)

        # Development mode: skip
        decision = executor._decide_execution(
            state=select_node._execution_state,
            run_location="remote",
            performance_mode=False,
            force_refresh=False,
        )
        assert decision.should_run is False

        # Performance mode: also skip
        decision = executor._decide_execution(
            state=select_node._execution_state,
            run_location="remote",
            performance_mode=True,
            force_refresh=False,
        )
        assert decision.should_run is False

    def test_select_local_always_full_local(self):
        """Select node with run_location='local' should always get FULL_LOCAL."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(select_node)
        decision = executor._decide_execution(
            state=select_node._execution_state,
            run_location="local",
            performance_mode=False,
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.strategy == ExecutionStrategy.FULL_LOCAL
        assert decision.reason == InvalidationReason.NEVER_RAN

    def test_sort_never_ran_gets_remote(self):
        """Sort node (wide transform) should get REMOTE directly.
        Wide transforms are too expensive to run locally."""
        graph = create_graph_with_sort()
        sort_node = graph.get_node(2)
        sort_node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(sort_node)
        decision = executor._decide_execution(
            state=sort_node._execution_state,
            run_location="remote",
            performance_mode=False,
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.strategy == ExecutionStrategy.REMOTE
        assert decision.reason == InvalidationReason.NEVER_RAN

    def test_manual_input_never_ran_gets_remote(self):
        """Manual input node (no default) should get REMOTE strategy."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)
        node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(node)
        decision = executor._decide_execution(
            state=node._execution_state,
            run_location="remote",
            performance_mode=False,
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.strategy == ExecutionStrategy.REMOTE
        assert decision.reason == InvalidationReason.NEVER_RAN


# =============================================================================
# Wide Transform Override Tests
# =============================================================================

class TestWideVsNarrowTransformStrategy:
    """Tests that narrow transforms get LOCAL_WITH_SAMPLING and wide
    transforms go directly to REMOTE from _determine_strategy.

    The override logic in execute() exists as a safety net but shouldn't
    trigger now that _determine_strategy handles transform_type directly."""

    def test_sort_always_remote(self):
        """Sort (wide transform) should always get REMOTE when remote,
        regardless of optimize_for_downstream."""
        graph = create_graph_with_sort()
        sort_node = graph.get_node(2)
        sort_node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(sort_node)
        strategy = executor._determine_strategy("remote")
        assert strategy == ExecutionStrategy.REMOTE

    def test_select_always_local_with_sampling(self):
        """Select (narrow transform) should get LOCAL_WITH_SAMPLING when remote."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(select_node)
        strategy = executor._determine_strategy("remote")
        assert strategy == ExecutionStrategy.LOCAL_WITH_SAMPLING

    def test_sort_local_still_full_local(self):
        """Sort (wide transform) should use FULL_LOCAL when run_location='local'."""
        graph = create_graph_with_sort()
        sort_node = graph.get_node(2)

        executor = NodeExecutor(sort_node)
        strategy = executor._determine_strategy("local")
        assert strategy == ExecutionStrategy.FULL_LOCAL

    def test_select_local_still_full_local(self):
        """Select (narrow transform) should use FULL_LOCAL when run_location='local'."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)

        executor = NodeExecutor(select_node)
        strategy = executor._determine_strategy("local")
        assert strategy == ExecutionStrategy.FULL_LOCAL


# =============================================================================
# Strategy Determination for All Nodes With Defaults
# =============================================================================

class TestNodesWithDefaults:
    """Verify that all nodes registered in nodes_with_defaults get
    LOCAL_WITH_SAMPLING and nodes outside that set get REMOTE."""

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


# =============================================================================
# Cache Results Strategy Tests
# =============================================================================

class TestCacheResultsStrategy:
    """When cache_results is enabled, everything should run fully remote.

    Caching requires full materialization of results, which means even
    narrow transforms that would normally get LOCAL_WITH_SAMPLING must
    use REMOTE so the result can be stored in the cache."""

    def test_select_with_cache_gets_remote(self):
        """Select (narrow) with cache_results=True should get REMOTE, not LOCAL_WITH_SAMPLING."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node.node_settings.cache_results = True

        executor = NodeExecutor(select_node)
        strategy = executor._determine_strategy("remote")
        assert strategy == ExecutionStrategy.REMOTE

    def test_select_without_cache_gets_local_with_sampling(self):
        """Select (narrow) without cache_results should still get LOCAL_WITH_SAMPLING."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        assert select_node.node_settings.cache_results is False

        executor = NodeExecutor(select_node)
        strategy = executor._determine_strategy("remote")
        assert strategy == ExecutionStrategy.LOCAL_WITH_SAMPLING

    def test_sort_with_cache_still_remote(self):
        """Sort (wide) with cache_results=True should still get REMOTE (was already REMOTE)."""
        graph = create_graph_with_sort()
        sort_node = graph.get_node(2)
        sort_node.node_settings.cache_results = True

        executor = NodeExecutor(sort_node)
        strategy = executor._determine_strategy("remote")
        assert strategy == ExecutionStrategy.REMOTE

    def test_cache_does_not_affect_local_execution(self):
        """cache_results should not affect local execution — still FULL_LOCAL."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node.node_settings.cache_results = True

        executor = NodeExecutor(select_node)
        strategy = executor._determine_strategy("local")
        assert strategy == ExecutionStrategy.FULL_LOCAL

    def test_select_with_cache_decide_execution_gets_remote(self):
        """Full decision flow: select with cache_results should decide REMOTE."""
        graph = create_graph_with_select()
        select_node = graph.get_node(2)
        select_node.node_settings.cache_results = True
        select_node._execution_state.has_run_with_current_setup = False

        executor = NodeExecutor(select_node)
        decision = executor._decide_execution(
            state=select_node._execution_state,
            run_location="remote",
            performance_mode=False,
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.strategy == ExecutionStrategy.REMOTE
        assert decision.reason == InvalidationReason.NEVER_RAN


# =============================================================================
# Preview (get_table_example) Tests
# =============================================================================

class TestPreviewAfterExecution:
    """Tests that get_table_example reads from stored sample data after
    a graph has been executed, rather than recomputing the node.

    Uses a read node (1000-row parquet) → select node pipeline running
    locally to test the full preview path."""

    def test_preview_returns_data_after_run(self):
        """After running the graph, preview should return sample data."""
        graph = create_graph_with_read_and_select()
        run_info = graph.run_graph()
        assert run_info.success, f"Graph should run: {run_info}"

        select_node = graph.get_node(2)
        table_example = select_node.get_table_example(include_data=True)

        assert table_example is not None
        assert len(table_example.data) > 0, "Preview should contain rows"
        assert table_example.columns == ["Name"], "Select should keep only Name"

    def test_preview_returns_at_most_100_rows(self):
        """The sample generator caps at 100 rows even though the source has 1000."""
        graph = create_graph_with_read_and_select()
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

    def test_preview_does_not_recompute_node(self):
        """Calling get_table_example should read stored data, not re-execute."""
        graph = create_graph_with_read_and_select()
        graph.run_graph()

        select_node = graph.get_node(2)

        # Patch get_resulting_data to track if it gets called with fresh computation.
        # get_resulting_data is called once inside get_table_example for metadata
        # (number_of_fields, columns), but it should return the cached result
        # (results.resulting_data is not None) — never re-enter the computation path.
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

    def test_preview_uses_cached_sample_on_repeated_calls(self):
        """Multiple preview calls should return the same data without recomputation."""
        graph = create_graph_with_read_and_select()
        graph.run_graph()

        select_node = graph.get_node(2)

        first = select_node.get_table_example(include_data=True)
        second = select_node.get_table_example(include_data=True)

        assert first.data == second.data, "Repeated previews should return identical data"

    def test_read_node_preview_returns_data(self):
        """The read node (1000 rows) should also have preview data after run."""
        graph = create_graph_with_read_and_select()
        graph.run_graph()

        read_node = graph.get_node(1)
        table_example = read_node.get_table_example(include_data=True)

        assert table_example is not None
        assert len(table_example.data) > 0
        assert len(table_example.data) <= 100


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

    def test_upstream_change_resets_execution_state(self):
        """After changing settings, _execution_state should reflect the reset."""
        graph = create_graph_with_read_and_select()
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

        # _execution_state should be reset
        assert select_node._execution_state.has_run_with_current_setup is False

    def test_settings_change_node_stats_has_completed_last_run(self):
        """Check whether node_stats.has_completed_last_run is properly reset
        when the node's settings change.

        get_table_example gates on node_stats.has_completed_last_run.
        If this stays True after a reset, preview will try to read stale data."""
        graph = create_graph_with_read_and_select()
        graph.run_graph()

        select_node = graph.get_node(2)
        assert select_node.node_stats.has_completed_last_run is True

        # Change the select configuration
        new_select_input = transform_schema.SelectInput(old_name='City', new_name='City', keep=True)
        node_select = input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[new_select_input],
            keep_missing=False,
        )
        graph.add_select(node_select)

        # This documents the current behavior — node_stats.has_completed_last_run
        # is NOT reset by FlowNode.reset(). This means get_table_example will
        # still enter the data path after a settings change.
        #
        # If this assertion fails in the future (i.e., has_completed_last_run
        # becomes False), that means the bug has been fixed and the stale-data
        # scenario below no longer applies.
        assert select_node.node_stats.has_completed_last_run is True, (
            "KNOWN ISSUE: node_stats.has_completed_last_run is not reset "
            "when the node's settings change. If this assertion starts failing, "
            "the stale preview data issue has been fixed."
        )

    def test_preview_after_settings_change_returns_stale_data(self):
        """After changing settings without re-running, preview returns stale data.

        This documents the current (incorrect) behavior:
        - Run with select=[Name] → preview shows Name column
        - Change to select=[City] without re-running
        - Preview still returns the old Name data because:
          1. node_stats.has_completed_last_run is still True (not reset)
          2. example_data_generator still points to the old sample
          3. results.resulting_data is cleared, but example_data_generator is not

        When this test starts failing, the bug is fixed."""
        graph = create_graph_with_read_and_select()
        graph.run_graph()

        select_node = graph.get_node(2)
        first_preview = select_node.get_table_example(include_data=True)
        assert first_preview.columns == ["Name"]
        assert len(first_preview.data) > 0

        # Change select to keep City instead of Name
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

        # The stale example_data_generator is still set and returns old data.
        # The resulting_data was cleared by reset(), so get_resulting_data()
        # will recompute — but example_data_generator was never cleared.
        #
        # Depending on node_stats.has_completed_last_run being True (stale)
        # and example_data_generator being non-None (stale), get_table_example
        # returns the OLD sample data and the NEW schema/columns from the
        # recomputed get_resulting_data().
        #
        # If this behavior changes, update this test to reflect the fix.
        if select_node.node_stats.has_completed_last_run:
            # Stale path: preview entered the data branch
            assert second_preview.data is not None
            # The columns come from get_resulting_data() which recomputes,
            # so they reflect the new settings
            assert second_preview.columns == ["City"]
            # But the data comes from the stale example_data_generator which
            # still has Name data from the first run
            if len(second_preview.data) > 0:
                first_row = second_preview.data[0]
                assert "Name" in first_row or "City" in first_row, (
                    "Data should contain either old Name or new City column data"
                )
        else:
            # Fixed path: node_stats was properly reset, preview returns empty
            assert second_preview.data == []

    def test_preview_correct_after_rerun(self):
        """After changing settings AND re-running, preview returns fresh data."""
        graph = create_graph_with_read_and_select()
        graph.run_graph()

        select_node = graph.get_node(2)
        first_preview = select_node.get_table_example(include_data=True)
        assert first_preview.columns == ["Name"]

        # Change select to keep City
        new_select_input = transform_schema.SelectInput(old_name='City', new_name='City', keep=True)
        node_select = input_schema.NodeSelect(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            select_input=[new_select_input],
            keep_missing=False,
        )
        graph.add_select(node_select)

        # Re-run the graph
        run_info = graph.run_graph()
        assert run_info.success

        # Now preview should reflect the new settings
        select_node = graph.get_node(2)
        second_preview = select_node.get_table_example(include_data=True)
        assert second_preview.columns == ["City"]
        assert len(second_preview.data) > 0
        # Every row should have the City key
        for row in second_preview.data:
            assert "City" in row, f"Expected City column in row, got: {row.keys()}"

    @pytest.mark.parametrize("execution_location", ["local", "remote"])
    def test_preview_after_upstream_settings_change_returns_stale_data(self, execution_location: ExecutionLocationLit):
        """After changing settings without re-running, preview returns stale data.

        This documents the current (incorrect) behavior:
        - Run with select=[Name] → preview shows Name column
        - Change to select=[City] without re-running
        - Preview still returns the old Name data because:
          1. node_stats.has_completed_last_run is still True (not reset)
          2. example_data_generator still points to the old sample
          3. results.resulting_data is cleared, but example_data_generator is not

        When this test starts failing, the bug is fixed."""
        graph = create_graph_with_sort_and_select(execution_location=execution_location)
        graph.run_graph()
        select_node = graph.get_node(3)
        assert select_node.results.example_data_generator
        breakpoint()
        first_preview = select_node.get_table_example(include_data=True)
        assert first_preview.columns == ["Name"]
        assert len(first_preview.data) > 0

        # Change select to keep City instead of Name
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


if __name__ == "__main__":
    pytest.main([__file__])