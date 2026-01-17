"""
Tests for the undo/redo history system.

This module contains comprehensive tests for the HistoryManager class
and the FlowGraph history integration.
"""

import pytest
from time import sleep

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.history_manager import HistoryManager
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.history_schema import (
    CompressedSnapshot,
    HistoryActionType,
    HistoryConfig,
    HistoryEntry,
    HistoryState,
    UndoRedoResult,
)


# ==================== Fixtures ====================


@pytest.fixture
def flow_handler():
    """Create a fresh FlowfileHandler for testing."""
    return FlowfileHandler()


@pytest.fixture
def flow_graph(flow_handler):
    """Create a fresh FlowGraph for testing with auto history capture enabled."""
    flow_handler.register_flow(
        schemas.FlowSettings(
            flow_id=1,
            name='test_flow',
            path='.',
            execution_mode='Development',
            track_history=True
        )
    )
    return flow_handler.get_flow(1)


@pytest.fixture
def flow_graph_no_auto_history(flow_handler):
    """Create a FlowGraph with auto history capture disabled (for testing manual capture)."""
    flow_handler.register_flow(
        schemas.FlowSettings(
            flow_id=2,
            name='test_flow_no_auto',
            path='.',
            execution_mode='Development',
            track_history=False
        )
    )
    flow = flow_handler.get_flow(2)
    # Re-enable the history manager but disable auto-capture from decorators
    # The track_history=False means decorators won't auto-capture
    # but we can still manually capture for testing
    flow._history_manager._config = HistoryConfig(enabled=True)
    return flow


@pytest.fixture
def history_manager():
    """Create a fresh HistoryManager for testing."""
    return HistoryManager()


@pytest.fixture
def sample_data():
    """Sample data for manual input nodes."""
    return [
        {'name': 'John', 'city': 'New York', 'age': 30},
        {'name': 'Jane', 'city': 'Los Angeles', 'age': 25},
        {'name': 'Edward', 'city': 'Chicago', 'age': 35},
    ]


# ==================== Helper Functions ====================


def add_manual_input_node(graph: FlowGraph, data: list, node_id: int = 1):
    """Add a manual input node to the graph."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type='manual_input'
    )
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(data)
    )
    graph.add_manual_input(input_file)
    return graph


def add_filter_node(graph: FlowGraph, node_id: int, input_node_id: int):
    """Add a filter node to the graph."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type='filter'
    )
    graph.add_node_promise(node_promise)
    filter_settings = input_schema.NodeFilter(
        flow_id=graph.flow_id,
        node_id=node_id,
        depending_on_id=input_node_id,
        filter_input=transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(
                field='name',
                operator='equals',
                value='John'
            )
        )
    )
    graph.add_filter(filter_settings)
    return graph


# ==================== HistoryManager Unit Tests ====================


class TestHistoryManagerInitialization:
    """Tests for HistoryManager initialization."""

    def test_default_initialization(self, history_manager):
        """Test that HistoryManager initializes with correct defaults."""
        assert history_manager.config.enabled is True
        assert history_manager.config.max_stack_size == 50
        assert len(history_manager._undo_stack) == 0
        assert len(history_manager._redo_stack) == 0

    def test_custom_config(self):
        """Test HistoryManager with custom configuration."""
        config = HistoryConfig(enabled=False, max_stack_size=100)
        manager = HistoryManager(config=config)
        assert manager.config.enabled is False
        assert manager.config.max_stack_size == 100

    def test_is_restoring_flag_initially_false(self, history_manager):
        """Test that _is_restoring flag is initially False."""
        assert history_manager.is_restoring() is False


class TestSnapshotComparison:
    """Tests for snapshot comparison functionality."""

    def test_identical_snapshots_are_equal(self, flow_graph, sample_data):
        """Test that identical snapshots are detected as equal."""
        add_manual_input_node(flow_graph, sample_data, node_id=1)
        snapshot1 = flow_graph.get_flowfile_data().model_dump()
        snapshot2 = flow_graph.get_flowfile_data().model_dump()

        # Use CompressedSnapshot for comparison
        hash1 = CompressedSnapshot._compute_hash(snapshot1)
        hash2 = CompressedSnapshot._compute_hash(snapshot2)
        assert hash1 == hash2

    def test_different_snapshots_are_not_equal(self, flow_graph, sample_data):
        """Test that different snapshots are detected as different."""
        add_manual_input_node(flow_graph, sample_data, node_id=1)
        snapshot1 = flow_graph.get_flowfile_data().model_dump()

        # Add another node to change the state
        node_promise = input_schema.NodePromise(
            flow_id=flow_graph.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph.add_node_promise(node_promise)
        snapshot2 = flow_graph.get_flowfile_data().model_dump()

        # Use CompressedSnapshot for comparison
        hash1 = CompressedSnapshot._compute_hash(snapshot1)
        hash2 = CompressedSnapshot._compute_hash(snapshot2)
        assert hash1 != hash2


class TestCompression:
    """Tests for snapshot compression functionality."""

    def test_compression_reduces_size(self, flow_graph, sample_data):
        """Test that compression significantly reduces snapshot size."""
        add_manual_input_node(flow_graph, sample_data, node_id=1)

        # Add more nodes for a larger snapshot
        for i in range(5):
            node_promise = input_schema.NodePromise(
                flow_id=flow_graph.flow_id,
                node_id=i + 10,
                node_type='filter'
            )
            flow_graph.add_node_promise(node_promise)

        snapshot = flow_graph.get_flowfile_data().model_dump()

        # Create compressed snapshot
        compressed = CompressedSnapshot(snapshot, compression_level=6)

        # Calculate uncompressed size (approximate)
        import pickle
        uncompressed_size = len(pickle.dumps(snapshot))
        compressed_size = compressed.compressed_size

        # Compression should reduce size (typically 60-80%)
        assert compressed_size < uncompressed_size
        compression_ratio = compressed_size / uncompressed_size
        assert compression_ratio < 0.8  # At least 20% reduction

    def test_decompression_restores_original(self, flow_graph, sample_data):
        """Test that decompression restores the original snapshot."""
        add_manual_input_node(flow_graph, sample_data, node_id=1)
        snapshot = flow_graph.get_flowfile_data().model_dump()

        # Compress and decompress
        compressed = CompressedSnapshot(snapshot, compression_level=6)
        restored = compressed.decompress()

        # Should be identical
        assert restored == snapshot

    def test_compressed_snapshots_equality(self, flow_graph, sample_data):
        """Test that equal compressed snapshots compare equal."""
        add_manual_input_node(flow_graph, sample_data, node_id=1)
        snapshot = flow_graph.get_flowfile_data().model_dump()

        compressed1 = CompressedSnapshot(snapshot, compression_level=6)
        compressed2 = CompressedSnapshot(snapshot, compression_level=6)

        assert compressed1 == compressed2
        assert compressed1.hash == compressed2.hash

    def test_memory_usage_tracking(self, flow_graph_no_auto_history, sample_data):
        """Test that memory usage can be tracked."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Capture some history
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Add node"
        )

        # Get memory usage
        usage = flow_graph_no_auto_history._history_manager.get_memory_usage()

        assert "undo_stack_entries" in usage
        assert "undo_stack_bytes" in usage
        assert "total_bytes" in usage
        assert usage["undo_stack_entries"] == 1
        assert usage["undo_stack_bytes"] > 0


class TestCaptureSnapshot:
    """Tests for capture_snapshot functionality."""

    def test_capture_snapshot_adds_to_undo_stack(self, flow_graph_no_auto_history, sample_data):
        """Test that capturing a snapshot adds to the undo stack."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        initial_count = len(flow_graph_no_auto_history._history_manager._undo_stack)
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Add manual_input node",
            node_id=1
        )

        assert len(flow_graph_no_auto_history._history_manager._undo_stack) == initial_count + 1

    def test_capture_snapshot_clears_redo_stack(self, flow_graph_no_auto_history, sample_data):
        """Test that capturing a snapshot clears the redo stack."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Capture initial state, then perform undo to populate redo stack
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Initial",
            node_id=1
        )

        # Add another node
        node_promise = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise)
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Add filter",
            node_id=2
        )

        # Undo to populate redo stack
        flow_graph_no_auto_history.undo()
        assert len(flow_graph_no_auto_history._history_manager._redo_stack) > 0

        # Add a DIFFERENT node (node 3) to change the state
        # This is necessary because duplicate detection would skip capture
        # if the state hasn't actually changed
        node_promise3 = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=3,
            node_type='sample'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise3)

        # Now capture new snapshot - should clear redo stack
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "New action",
            node_id=3
        )

        assert len(flow_graph_no_auto_history._history_manager._redo_stack) == 0

    def test_capture_skips_duplicate_snapshots(self, flow_graph_no_auto_history, sample_data):
        """Test that duplicate snapshots are not added."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Capture the same state twice
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "First capture",
            node_id=1
        )
        initial_count = len(flow_graph_no_auto_history._history_manager._undo_stack)

        # Capture again without any changes
        result = flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Duplicate capture",
            node_id=1
        )

        # Should not add duplicate
        assert result is False
        assert len(flow_graph_no_auto_history._history_manager._undo_stack) == initial_count

    def test_capture_disabled_when_history_disabled(self, flow_graph_no_auto_history, sample_data):
        """Test that snapshots are not captured when history is disabled."""
        flow_graph_no_auto_history._history_manager.config = HistoryConfig(enabled=False)
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        result = flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Test",
            node_id=1
        )

        assert result is False
        assert len(flow_graph_no_auto_history._history_manager._undo_stack) == 0


class TestCaptureIfChanged:
    """Tests for capture_if_changed functionality."""

    def test_capture_if_changed_when_state_changed(self, flow_graph_no_auto_history, sample_data):
        """Test that history is captured when state actually changes."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Capture pre-snapshot
        pre_snapshot = flow_graph_no_auto_history.get_flowfile_data()

        # Make a change
        node_promise = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise)

        # Capture if changed
        result = flow_graph_no_auto_history.capture_history_if_changed(
            pre_snapshot,
            HistoryActionType.ADD_NODE,
            "Add filter node",
            node_id=2
        )

        assert result is True
        assert len(flow_graph_no_auto_history._history_manager._undo_stack) == 1

    def test_capture_if_changed_when_no_change(self, flow_graph_no_auto_history, sample_data):
        """Test that history is not captured when state doesn't change."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Capture pre-snapshot
        pre_snapshot = flow_graph_no_auto_history.get_flowfile_data()

        # Don't make any changes

        # Capture if changed
        result = flow_graph_no_auto_history.capture_history_if_changed(
            pre_snapshot,
            HistoryActionType.UPDATE_SETTINGS,
            "No-op update",
            node_id=1
        )

        assert result is False
        assert len(flow_graph_no_auto_history._history_manager._undo_stack) == 0


class TestStackSizeLimits:
    """Tests for stack size limits."""

    def test_undo_stack_respects_max_size(self, flow_graph_no_auto_history, sample_data):
        """Test that undo stack respects max_stack_size."""
        flow_graph_no_auto_history._history_manager.config = HistoryConfig(max_stack_size=3)
        # Recreate deque with new maxlen
        from collections import deque
        flow_graph_no_auto_history._history_manager._undo_stack = deque(maxlen=3)

        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Add multiple snapshots
        for i in range(5):
            node_promise = input_schema.NodePromise(
                flow_id=flow_graph_no_auto_history.flow_id,
                node_id=i + 10,
                node_type='filter'
            )
            flow_graph_no_auto_history.add_node_promise(node_promise)
            flow_graph_no_auto_history.capture_history_snapshot(
                HistoryActionType.ADD_NODE,
                f"Add node {i + 10}",
                node_id=i + 10
            )

        # Stack should not exceed max size
        assert len(flow_graph_no_auto_history._history_manager._undo_stack) <= 3


# ==================== Undo/Redo Operation Tests ====================


class TestUndoOperation:
    """Tests for undo operation."""

    def test_undo_with_empty_stack(self, flow_graph_no_auto_history):
        """Test undo returns failure with empty stack."""
        result = flow_graph_no_auto_history.undo()

        assert result.success is False
        assert result.error_message == "Nothing to undo"

    def test_undo_restores_previous_state(self, flow_graph_no_auto_history, sample_data):
        """Test that undo restores the previous state."""
        # Add initial node
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)
        initial_node_count = len(flow_graph_no_auto_history.nodes)

        # Capture state before adding second node
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Before adding second node"
        )

        # Add second node
        node_promise = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise)
        assert len(flow_graph_no_auto_history.nodes) == initial_node_count + 1

        # Undo
        result = flow_graph_no_auto_history.undo()

        assert result.success is True
        assert result.action_description == "Before adding second node"
        assert len(flow_graph_no_auto_history.nodes) == initial_node_count

    def test_undo_populates_redo_stack(self, flow_graph_no_auto_history, sample_data):
        """Test that undo populates the redo stack."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Test action"
        )

        # Add another node
        node_promise = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise)

        assert len(flow_graph_no_auto_history._history_manager._redo_stack) == 0

        # Undo
        flow_graph_no_auto_history.undo()

        assert len(flow_graph_no_auto_history._history_manager._redo_stack) == 1


class TestRedoOperation:
    """Tests for redo operation."""

    def test_redo_with_empty_stack(self, flow_graph_no_auto_history):
        """Test redo returns failure with empty stack."""
        result = flow_graph_no_auto_history.redo()

        assert result.success is False
        assert result.error_message == "Nothing to redo"

    def test_redo_restores_undone_state(self, flow_graph_no_auto_history, sample_data):
        """Test that redo restores the undone state."""
        # Add initial node
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Capture state before adding second node
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Before adding second node"
        )

        # Add second node
        node_promise = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise)

        # Remember node count with second node
        node_count_with_second = len(flow_graph_no_auto_history.nodes)

        # Undo
        flow_graph_no_auto_history.undo()
        assert len(flow_graph_no_auto_history.nodes) == node_count_with_second - 1

        # Redo
        result = flow_graph_no_auto_history.redo()

        assert result.success is True
        assert len(flow_graph_no_auto_history.nodes) == node_count_with_second


class TestUndoRedoSequence:
    """Tests for sequences of undo/redo operations."""

    def test_multiple_undo_redo_operations(self, flow_graph_no_auto_history, sample_data):
        """Test multiple sequential undo/redo operations."""
        # Add initial node
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Build history with multiple actions
        for i in range(3):
            flow_graph_no_auto_history.capture_history_snapshot(
                HistoryActionType.ADD_NODE,
                f"Action {i}"
            )
            node_promise = input_schema.NodePromise(
                flow_id=flow_graph_no_auto_history.flow_id,
                node_id=i + 10,
                node_type='filter'
            )
            flow_graph_no_auto_history.add_node_promise(node_promise)

        final_node_count = len(flow_graph_no_auto_history.nodes)

        # Undo all actions
        for _ in range(3):
            result = flow_graph_no_auto_history.undo()
            assert result.success is True

        assert len(flow_graph_no_auto_history.nodes) == final_node_count - 3

        # Redo all actions
        for _ in range(3):
            result = flow_graph_no_auto_history.redo()
            assert result.success is True

        assert len(flow_graph_no_auto_history.nodes) == final_node_count


# ==================== HistoryState Tests ====================


class TestHistoryState:
    """Tests for get_history_state functionality."""

    def test_initial_history_state(self, flow_graph_no_auto_history):
        """Test initial history state."""
        state = flow_graph_no_auto_history.get_history_state()

        assert state.can_undo is False
        assert state.can_redo is False
        assert state.undo_description is None
        assert state.redo_description is None
        assert state.undo_count == 0
        assert state.redo_count == 0

    def test_history_state_after_capture(self, flow_graph_no_auto_history, sample_data):
        """Test history state after capturing a snapshot."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Add node"
        )

        state = flow_graph_no_auto_history.get_history_state()

        assert state.can_undo is True
        assert state.can_redo is False
        assert state.undo_description == "Add node"
        assert state.undo_count == 1

    def test_history_state_after_undo(self, flow_graph_no_auto_history, sample_data):
        """Test history state after undo."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Add node"
        )

        # Add another node
        node_promise = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise)

        flow_graph_no_auto_history.undo()

        state = flow_graph_no_auto_history.get_history_state()

        assert state.can_undo is False
        assert state.can_redo is True
        assert state.redo_count == 1


# ==================== Clear History Tests ====================


class TestClearHistory:
    """Tests for clear history functionality."""

    def test_clear_empties_both_stacks(self, flow_graph_no_auto_history, sample_data):
        """Test that clear empties both undo and redo stacks."""
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Build up some history
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Action 1"
        )

        node_promise = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise)

        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Action 2"
        )

        # Undo to populate redo stack
        flow_graph_no_auto_history.undo()

        assert len(flow_graph_no_auto_history._history_manager._undo_stack) > 0
        assert len(flow_graph_no_auto_history._history_manager._redo_stack) > 0

        # Clear
        flow_graph_no_auto_history._history_manager.clear()

        assert len(flow_graph_no_auto_history._history_manager._undo_stack) == 0
        assert len(flow_graph_no_auto_history._history_manager._redo_stack) == 0


# ==================== Connection Restoration Tests ====================


class TestConnectionRestoration:
    """Tests for restoring connections during undo/redo."""

    def test_restore_connections_after_undo(self, flow_graph_no_auto_history, sample_data):
        """Test that connections are properly restored after undo."""
        # Add manual input node
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Capture state before adding filter and connection
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Before filter"
        )

        # Add filter node with dependency
        add_filter_node(flow_graph_no_auto_history, node_id=2, input_node_id=1)

        # Add connection
        node_connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=1,
            to_id=2,
            input_type='main'
        )
        add_connection(flow_graph_no_auto_history, node_connection)

        # Verify connection exists
        filter_node = flow_graph_no_auto_history.get_node(2)
        assert filter_node is not None
        assert filter_node.has_input

        # Undo
        flow_graph_no_auto_history.undo()

        # Filter node should be gone
        assert flow_graph_no_auto_history.get_node(2) is None


# ==================== Restore From Snapshot Tests ====================


class TestRestoreFromSnapshot:
    """Tests for restore_from_snapshot functionality."""

    def test_restore_preserves_flow_id(self, flow_graph_no_auto_history, sample_data):
        """Test that restore_from_snapshot preserves the original flow_id."""
        original_flow_id = flow_graph_no_auto_history.flow_id

        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Get snapshot
        snapshot = flow_graph_no_auto_history.get_flowfile_data()

        # Add more nodes
        node_promise = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise)

        # Restore
        flow_graph_no_auto_history.restore_from_snapshot(snapshot)

        assert flow_graph_no_auto_history.flow_id == original_flow_id

    def test_restore_clears_and_rebuilds_nodes(self, flow_graph_no_auto_history, sample_data):
        """Test that restore properly clears and rebuilds nodes."""
        # Add two nodes
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Get snapshot with 1 node
        snapshot = flow_graph_no_auto_history.get_flowfile_data()
        one_node_count = len(flow_graph_no_auto_history.nodes)

        # Add second node
        node_promise = input_schema.NodePromise(
            flow_id=flow_graph_no_auto_history.flow_id,
            node_id=2,
            node_type='filter'
        )
        flow_graph_no_auto_history.add_node_promise(node_promise)

        two_node_count = len(flow_graph_no_auto_history.nodes)
        assert two_node_count > one_node_count

        # Restore to one-node state
        flow_graph_no_auto_history.restore_from_snapshot(snapshot)

        assert len(flow_graph_no_auto_history.nodes) == one_node_count


# ==================== Integration Tests ====================


class TestHistoryIntegration:
    """Integration tests for the history system."""

    def test_full_workflow_with_history(self, flow_graph_no_auto_history, sample_data):
        """Test a complete workflow with multiple operations and undo/redo."""
        # Step 1: Add initial node
        add_manual_input_node(flow_graph_no_auto_history, sample_data, node_id=1)

        # Step 2: Capture and add filter
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Before adding filter"
        )
        add_filter_node(flow_graph_no_auto_history, node_id=2, input_node_id=1)

        # Step 3: Capture and add connection
        flow_graph_no_auto_history.capture_history_snapshot(
            HistoryActionType.ADD_CONNECTION,
            "Before connection"
        )
        node_connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=1,
            to_id=2,
            input_type='main'
        )
        add_connection(flow_graph_no_auto_history, node_connection)

        # Verify state
        state = flow_graph_no_auto_history.get_history_state()
        assert state.undo_count == 2

        # Undo connection
        result = flow_graph_no_auto_history.undo()
        assert result.success is True
        assert result.action_description == "Before connection"

        # Undo filter
        result = flow_graph_no_auto_history.undo()
        assert result.success is True
        assert result.action_description == "Before adding filter"

        # Only manual input should remain
        assert len(flow_graph_no_auto_history.nodes) == 1

        # Redo both
        flow_graph_no_auto_history.redo()
        flow_graph_no_auto_history.redo()

        # All nodes should be back
        assert len(flow_graph_no_auto_history.nodes) == 2


class TestHistoryWithSettingsUpdates:
    """Tests for history with settings updates."""

    def test_settings_update_captured_only_when_changed(self, flow_graph, sample_data):
        """Test that settings updates are only captured when state changes."""
        add_manual_input_node(flow_graph, sample_data, node_id=1)

        # Get initial history count
        initial_count = len(flow_graph._history_manager._undo_stack)

        # Capture pre-state
        pre_snapshot = flow_graph.get_flowfile_data()

        # Update with same settings (no-op)
        node = flow_graph.get_node(1)
        current_settings = node.setting_input

        # Apply same settings
        flow_graph.add_manual_input(current_settings)

        # Check if changed - should be False
        result = flow_graph.capture_history_if_changed(
            pre_snapshot,
            HistoryActionType.UPDATE_SETTINGS,
            "Update settings",
            node_id=1
        )

        # History should not have changed
        assert result is False
        assert len(flow_graph._history_manager._undo_stack) == initial_count
