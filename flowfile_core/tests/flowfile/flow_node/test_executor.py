"""Tests for the FlowNode executor module.

Tests the execution strategy determination, state management,
and decision-making logic introduced in the refactored FlowNode.
"""

import os
import tempfile
import time

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph
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
from flowfile_core.schemas import input_schema, schemas


# =============================================================================
# Test Helpers
# =============================================================================

def create_graph(execution_mode: str = 'Development') -> FlowGraph:
    """Create a new flow graph for testing."""
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(
        flow_id=1,
        name='test_flow',
        path='.',
        execution_mode=execution_mode
    ))
    return handler.get_flow(1)


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1) -> FlowGraph:
    """Add a manual input node to the graph."""
    raw_data = input_schema.RawData.from_pylist(data)
    node_promise = input_schema.NodePromise(flow_id=1, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=node_id, raw_data_format=raw_data)
    graph.add_manual_input(input_file)
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

    def test_decide_execution_performance_mode(self):
        """Test execution decision in performance mode."""
        graph = create_graph()
        add_manual_input(graph, [{"a": 1}], node_id=1)
        node = graph.get_node(1)

        # Mark as already run
        node._execution_state.has_run_with_current_setup = True

        executor = NodeExecutor(node)
        decision = executor._decide_execution(
            state=node._execution_state,
            run_location="remote",
            performance_mode=True,  # Performance mode
            force_refresh=False,
        )

        assert decision.should_run is True
        assert decision.reason == InvalidationReason.PERFORMANCE_MODE

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
