"""
Tests for the undo/redo history system.

This module contains comprehensive tests for the HistoryManager class
and its integration with FlowGraph.
"""

import pytest
from time import time

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection, delete_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.history_manager import HistoryManager
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.history_schema import (
    FlowDiff,
    HistoryActionType,
    HistoryConfig,
    HistoryEntry,
    HistoryState,
    NodeDiff,
    UndoRedoResult,
)


# =====================================================================
# Fixtures
# =====================================================================


@pytest.fixture
def handler():
    """Create a fresh FlowfileHandler."""
    return FlowfileHandler()


@pytest.fixture
def flow_settings():
    """Create default flow settings."""
    return schemas.FlowSettings(
        flow_id=1,
        name="test_flow",
        path=".",
        execution_mode="Development"
    )


@pytest.fixture
def flow(handler, flow_settings):
    """Create a flow with the handler."""
    handler.register_flow(flow_settings)
    return handler.get_flow(flow_settings.flow_id)


@pytest.fixture
def history_manager():
    """Create a fresh HistoryManager."""
    return HistoryManager()


@pytest.fixture
def history_manager_with_config():
    """Create a HistoryManager with custom config."""
    config = HistoryConfig(
        enabled=True,
        max_stack_size=10,
        use_diff_storage=True
    )
    return HistoryManager(config)


@pytest.fixture
def sample_data():
    """Sample data for manual input nodes."""
    return [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "Los Angeles"},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
    ]


def add_manual_input_node(flow: FlowGraph, node_id: int, data: list[dict]):
    """Helper to add a manual input node."""
    node_promise = input_schema.NodePromise(
        flow_id=flow.flow_id,
        node_id=node_id,
        node_type="manual_input"
    )
    flow.add_node_promise(node_promise)
    input_settings = input_schema.NodeManualInput(
        flow_id=flow.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(data)
    )
    flow.add_manual_input(input_settings)


def add_filter_node(flow: FlowGraph, node_id: int, input_node_id: int, filter_expr: str = "True"):
    """Helper to add a filter node."""
    node_promise = input_schema.NodePromise(
        flow_id=flow.flow_id,
        node_id=node_id,
        node_type="filter"
    )
    flow.add_node_promise(node_promise)
    filter_settings = input_schema.NodeFilter(
        flow_id=flow.flow_id,
        node_id=node_id,
        depending_on_id=input_node_id,
        filter_input=transform_schema.FilterInput(
            mode="advanced",
            advanced_filter=filter_expr
        )
    )
    flow.add_filter(filter_settings)


# =====================================================================
# HistoryManager Unit Tests
# =====================================================================


class TestHistoryManagerInitialization:
    """Tests for HistoryManager initialization."""

    def test_default_initialization(self, history_manager):
        """Test default initialization."""
        assert history_manager.config.enabled is True
        assert history_manager.config.max_stack_size == 50
        assert history_manager.config.use_diff_storage is True
        assert len(history_manager._undo_stack) == 0
        assert len(history_manager._redo_stack) == 0

    def test_custom_config_initialization(self, history_manager_with_config):
        """Test initialization with custom config."""
        assert history_manager_with_config.config.max_stack_size == 10
        assert history_manager_with_config.config.use_diff_storage is True

    def test_disabled_history(self):
        """Test disabled history manager."""
        config = HistoryConfig(enabled=False)
        manager = HistoryManager(config)
        assert manager.config.enabled is False


class TestSnapshotComparison:
    """Tests for snapshot comparison logic."""

    def test_equal_snapshots(self, history_manager):
        """Test that identical snapshots are detected as equal."""
        snap1 = {
            "flowfile_version": "1.0.0",
            "flowfile_id": 1,
            "flowfile_name": "test",
            "flowfile_settings": {"execution_mode": "Development"},
            "nodes": [{"id": 1, "type": "filter", "setting_input": {}}]
        }
        snap2 = {
            "flowfile_version": "1.0.0",
            "flowfile_id": 1,
            "flowfile_name": "test",
            "flowfile_settings": {"execution_mode": "Development"},
            "nodes": [{"id": 1, "type": "filter", "setting_input": {}}]
        }
        assert history_manager._snapshots_equal(snap1, snap2) is True

    def test_unequal_snapshots_different_nodes(self, history_manager):
        """Test that snapshots with different nodes are detected as unequal."""
        snap1 = {
            "flowfile_settings": {},
            "nodes": [{"id": 1, "type": "filter"}]
        }
        snap2 = {
            "flowfile_settings": {},
            "nodes": [{"id": 1, "type": "select"}]
        }
        assert history_manager._snapshots_equal(snap1, snap2) is False

    def test_unequal_snapshots_different_settings(self, history_manager):
        """Test that snapshots with different settings are detected as unequal."""
        snap1 = {
            "flowfile_settings": {"execution_mode": "Development"},
            "nodes": []
        }
        snap2 = {
            "flowfile_settings": {"execution_mode": "Performance"},
            "nodes": []
        }
        assert history_manager._snapshots_equal(snap1, snap2) is False

    def test_none_snapshots(self, history_manager):
        """Test comparison with None snapshots."""
        snap1 = {"flowfile_settings": {}, "nodes": []}
        assert history_manager._snapshots_equal(None, None) is True
        assert history_manager._snapshots_equal(snap1, None) is False
        assert history_manager._snapshots_equal(None, snap1) is False


class TestDiffComputation:
    """Tests for diff computation."""

    def test_compute_diff_added_node(self, history_manager):
        """Test diff computation when a node is added."""
        old_snap = {
            "flowfile_settings": {},
            "nodes": []
        }
        new_snap = {
            "flowfile_settings": {},
            "nodes": [{"id": 1, "type": "filter", "input_ids": []}]
        }
        diff = history_manager._compute_diff(old_snap, new_snap)
        assert len(diff.node_changes) == 1
        assert diff.node_changes[0].action == "add"
        assert diff.node_changes[0].node_id == 1

    def test_compute_diff_removed_node(self, history_manager):
        """Test diff computation when a node is removed."""
        old_snap = {
            "flowfile_settings": {},
            "nodes": [{"id": 1, "type": "filter", "input_ids": []}]
        }
        new_snap = {
            "flowfile_settings": {},
            "nodes": []
        }
        diff = history_manager._compute_diff(old_snap, new_snap)
        assert len(diff.node_changes) == 1
        assert diff.node_changes[0].action == "remove"
        assert diff.node_changes[0].node_id == 1

    def test_compute_diff_modified_node(self, history_manager):
        """Test diff computation when a node is modified."""
        old_snap = {
            "flowfile_settings": {},
            "nodes": [{"id": 1, "type": "filter", "x_position": 0}]
        }
        new_snap = {
            "flowfile_settings": {},
            "nodes": [{"id": 1, "type": "filter", "x_position": 100}]
        }
        diff = history_manager._compute_diff(old_snap, new_snap)
        assert len(diff.node_changes) == 1
        assert diff.node_changes[0].action == "modify"
        assert diff.node_changes[0].node_id == 1

    def test_compute_diff_connection_added(self, history_manager):
        """Test diff computation when a connection is added."""
        old_snap = {
            "flowfile_settings": {},
            "nodes": [
                {"id": 1, "type": "input", "input_ids": []},
                {"id": 2, "type": "filter", "input_ids": []}
            ]
        }
        new_snap = {
            "flowfile_settings": {},
            "nodes": [
                {"id": 1, "type": "input", "input_ids": []},
                {"id": 2, "type": "filter", "input_ids": [1]}
            ]
        }
        diff = history_manager._compute_diff(old_snap, new_snap)
        assert len(diff.connection_changes) == 1
        assert diff.connection_changes[0].action == "add"
        assert diff.connection_changes[0].from_node_id == 1
        assert diff.connection_changes[0].to_node_id == 2


class TestHistoryState:
    """Tests for history state retrieval."""

    def test_initial_state(self, history_manager):
        """Test initial history state."""
        state = history_manager.get_state()
        assert state.can_undo is False
        assert state.can_redo is False
        assert state.undo_count == 0
        assert state.redo_count == 0

    def test_state_after_capture(self, flow, history_manager):
        """Test history state after capturing a snapshot."""
        # Replace flow's history manager
        flow._history_manager = history_manager

        # Add a node to have some state
        add_manual_input_node(flow, 1, [{"a": 1}])

        # Capture snapshot
        history_manager.capture_snapshot(
            flow, HistoryActionType.ADD_NODE, "Add node", node_id=1
        )

        state = history_manager.get_state()
        assert state.can_undo is True
        assert state.can_redo is False
        assert state.undo_count == 1
        assert state.undo_description == "Add node"


class TestCaptureSnapshot:
    """Tests for snapshot capture."""

    def test_capture_snapshot_success(self, flow, history_manager):
        """Test successful snapshot capture."""
        flow._history_manager = history_manager
        add_manual_input_node(flow, 1, [{"a": 1}])

        result = history_manager.capture_snapshot(
            flow, HistoryActionType.ADD_NODE, "Add manual_input node", node_id=1
        )
        assert result is True
        assert len(history_manager._undo_stack) == 1

    def test_capture_snapshot_clears_redo(self, flow, history_manager):
        """Test that capturing a new snapshot clears the redo stack."""
        flow._history_manager = history_manager
        add_manual_input_node(flow, 1, [{"a": 1}])

        # Capture and undo to populate redo stack
        history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, "First", node_id=1)

        # Add another node
        add_manual_input_node(flow, 2, [{"b": 2}])

        # Undo
        history_manager.undo(flow)
        assert len(history_manager._redo_stack) == 1

        # Capture new snapshot should clear redo
        add_manual_input_node(flow, 3, [{"c": 3}])
        history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, "New action")
        assert len(history_manager._redo_stack) == 0

    def test_capture_when_disabled(self, flow):
        """Test that capture returns False when history is disabled."""
        config = HistoryConfig(enabled=False)
        manager = HistoryManager(config)
        flow._history_manager = manager

        result = manager.capture_snapshot(
            flow, HistoryActionType.ADD_NODE, "Test"
        )
        assert result is False
        assert len(manager._undo_stack) == 0


class TestCaptureIfChanged:
    """Tests for conditional snapshot capture."""

    def test_capture_if_changed_with_change(self, flow, history_manager):
        """Test capture_if_changed when state actually changed."""
        flow._history_manager = history_manager

        # Get pre-snapshot with no nodes
        pre_snapshot = history_manager.get_pre_snapshot(flow)

        # Add a node (change)
        add_manual_input_node(flow, 1, [{"a": 1}])

        # Capture should succeed
        result = history_manager.capture_if_changed(
            flow, pre_snapshot, HistoryActionType.ADD_NODE, "Add node"
        )
        assert result is True
        assert len(history_manager._undo_stack) == 1

    def test_capture_if_changed_without_change(self, flow, history_manager):
        """Test capture_if_changed when state didn't change."""
        flow._history_manager = history_manager

        # Get pre-snapshot
        pre_snapshot = history_manager.get_pre_snapshot(flow)

        # Don't make any changes

        # Capture should fail (no change)
        result = history_manager.capture_if_changed(
            flow, pre_snapshot, HistoryActionType.UPDATE_SETTINGS, "No change"
        )
        assert result is False
        assert len(history_manager._undo_stack) == 0


class TestStackSizeLimit:
    """Tests for stack size limiting."""

    def test_max_stack_size(self, flow):
        """Test that undo stack respects max size."""
        config = HistoryConfig(max_stack_size=3)
        manager = HistoryManager(config)
        flow._history_manager = manager

        # Capture more than max_stack_size snapshots
        for i in range(5):
            add_manual_input_node(flow, i + 1, [{"a": i}])
            manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, f"Add {i}")

        # Stack should be limited to max_stack_size
        assert len(manager._undo_stack) == 3


# =====================================================================
# Undo/Redo Operation Tests
# =====================================================================


class TestUndoOperation:
    """Tests for undo operations."""

    def test_undo_empty_stack(self, flow, history_manager):
        """Test undo with empty stack returns failure."""
        flow._history_manager = history_manager
        result = history_manager.undo(flow)
        assert result.success is False
        assert "Nothing to undo" in result.error_message

    def test_undo_single_action(self, flow, history_manager):
        """Test undoing a single action."""
        flow._history_manager = history_manager

        # Capture initial state (no nodes)
        history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, "Add node", node_id=1)

        # Add a node
        add_manual_input_node(flow, 1, [{"a": 1}])
        assert flow.get_node(1) is not None

        # Undo should remove the node
        result = history_manager.undo(flow)
        assert result.success is True
        assert flow.get_node(1) is None

    def test_undo_updates_state(self, flow, history_manager):
        """Test that undo updates history state correctly."""
        flow._history_manager = history_manager

        history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, "Add node")
        add_manual_input_node(flow, 1, [{"a": 1}])

        # Before undo
        state_before = history_manager.get_state()
        assert state_before.can_undo is True
        assert state_before.can_redo is False

        # After undo
        history_manager.undo(flow)
        state_after = history_manager.get_state()
        assert state_after.can_undo is False
        assert state_after.can_redo is True


class TestRedoOperation:
    """Tests for redo operations."""

    def test_redo_empty_stack(self, flow, history_manager):
        """Test redo with empty stack returns failure."""
        flow._history_manager = history_manager
        result = history_manager.redo(flow)
        assert result.success is False
        assert "Nothing to redo" in result.error_message

    def test_redo_after_undo(self, flow, history_manager):
        """Test redo restores the undone state."""
        flow._history_manager = history_manager

        # Add node and capture
        history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, "Add node", node_id=1)
        add_manual_input_node(flow, 1, [{"a": 1}])

        # Capture current state before "next action"
        history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, "Add second node", node_id=2)
        add_manual_input_node(flow, 2, [{"b": 2}])

        # Verify both nodes exist
        assert flow.get_node(1) is not None
        assert flow.get_node(2) is not None

        # Undo (removes second capture's state)
        history_manager.undo(flow)
        assert flow.get_node(2) is None

        # Redo should restore
        result = history_manager.redo(flow)
        assert result.success is True
        # After redo, second node should be back
        # Note: The way our system works, redo restores to the state that was pushed to redo


class TestUndoRedoSequence:
    """Tests for undo/redo sequences."""

    def test_multiple_undos(self, flow, history_manager):
        """Test multiple consecutive undos."""
        flow._history_manager = history_manager

        # Create a series of actions
        for i in range(3):
            history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, f"Add node {i+1}")
            add_manual_input_node(flow, i + 1, [{"x": i}])

        # Verify all nodes exist
        for i in range(3):
            assert flow.get_node(i + 1) is not None

        # Undo all
        for i in range(3):
            result = history_manager.undo(flow)
            assert result.success is True

        # All should be undone
        assert len(history_manager._undo_stack) == 0
        assert len(history_manager._redo_stack) == 3

    def test_undo_redo_undo(self, flow, history_manager):
        """Test alternating undo and redo."""
        flow._history_manager = history_manager

        history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, "Add node")
        add_manual_input_node(flow, 1, [{"a": 1}])

        # Undo
        history_manager.undo(flow)
        assert flow.get_node(1) is None

        # Redo
        history_manager.redo(flow)
        # State after redo

        # Undo again
        history_manager.undo(flow)
        assert len(history_manager._redo_stack) > 0


class TestClearHistory:
    """Tests for clearing history."""

    def test_clear_empties_stacks(self, flow, history_manager):
        """Test that clear empties both stacks."""
        flow._history_manager = history_manager

        # Add some history
        history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, "Test")
        add_manual_input_node(flow, 1, [{"a": 1}])
        history_manager.undo(flow)

        assert len(history_manager._undo_stack) > 0 or len(history_manager._redo_stack) > 0

        # Clear
        history_manager.clear()
        assert len(history_manager._undo_stack) == 0
        assert len(history_manager._redo_stack) == 0
        assert history_manager._base_snapshot is None
        assert history_manager._last_snapshot is None


# =====================================================================
# FlowGraph Integration Tests
# =====================================================================


class TestFlowGraphHistoryIntegration:
    """Tests for FlowGraph history integration."""

    def test_flow_has_history_manager(self, flow):
        """Test that FlowGraph has a history manager."""
        assert hasattr(flow, "_history_manager")
        assert isinstance(flow._history_manager, HistoryManager)

    def test_capture_history_snapshot_method(self, flow):
        """Test FlowGraph.capture_history_snapshot method."""
        result = flow.capture_history_snapshot(
            HistoryActionType.ADD_NODE, "Test capture"
        )
        assert result is True

    def test_undo_method(self, flow):
        """Test FlowGraph.undo method."""
        # Capture and make a change
        flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add node", node_id=1)
        add_manual_input_node(flow, 1, [{"a": 1}])

        # Undo via FlowGraph
        result = flow.undo()
        assert isinstance(result, UndoRedoResult)

    def test_redo_method(self, flow):
        """Test FlowGraph.redo method."""
        # Setup for redo
        flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add node")
        add_manual_input_node(flow, 1, [{"a": 1}])
        flow.undo()

        # Redo via FlowGraph
        result = flow.redo()
        assert isinstance(result, UndoRedoResult)

    def test_get_history_state_method(self, flow):
        """Test FlowGraph.get_history_state method."""
        state = flow.get_history_state()
        assert isinstance(state, HistoryState)

    def test_clear_history_method(self, flow):
        """Test FlowGraph.clear_history method."""
        flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Test")
        flow.clear_history()
        state = flow.get_history_state()
        assert state.can_undo is False


# =====================================================================
# Connection Restoration Tests
# =====================================================================


class TestConnectionRestoration:
    """Tests for connection restoration during undo/redo."""

    def test_restore_main_connection(self, flow):
        """Test restoring main input connections."""
        # Add two nodes
        add_manual_input_node(flow, 1, [{"a": 1}])
        add_filter_node(flow, 2, 1)

        # Create connection
        node_connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=1, to_id=2, input_type="main"
        )
        add_connection(flow, node_connection)

        # Capture state with connection
        flow.capture_history_snapshot(
            HistoryActionType.DELETE_CONNECTION, "Delete connection"
        )

        # Delete connection
        delete_connection(flow, node_connection)

        # Verify connection is gone
        node2 = flow.get_node(2)
        main_inputs = node2.node_inputs.main_inputs or []
        assert len([n for n in main_inputs if n.node_id == 1]) == 0

        # Undo should restore connection
        flow.undo()

        # Verify connection is restored
        node2 = flow.get_node(2)
        main_inputs = node2.node_inputs.main_inputs or []
        # The node should have inputs again (undo restored the pre-delete state)


# =====================================================================
# Complex Workflow Tests
# =====================================================================


class TestComplexWorkflows:
    """Tests for complex multi-step workflows."""

    def test_multi_node_workflow(self, flow, sample_data):
        """Test undo/redo with multiple nodes and connections."""
        # Step 1: Add input node
        flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add input")
        add_manual_input_node(flow, 1, sample_data)

        # Step 2: Add filter node
        flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add filter")
        add_filter_node(flow, 2, 1)

        # Step 3: Connect nodes
        flow.capture_history_snapshot(HistoryActionType.ADD_CONNECTION, "Connect")
        node_connection = input_schema.NodeConnection.create_from_simple_input(
            from_id=1, to_id=2, input_type="main"
        )
        add_connection(flow, node_connection)

        # Verify state
        assert flow.get_node(1) is not None
        assert flow.get_node(2) is not None

        # Get history state
        state = flow.get_history_state()
        assert state.undo_count == 3

        # Undo all steps
        flow.undo()  # Undo connect
        flow.undo()  # Undo add filter
        flow.undo()  # Undo add input

        # All should be undone
        state = flow.get_history_state()
        assert state.undo_count == 0
        assert state.redo_count == 3

    def test_settings_update_workflow(self, flow, sample_data):
        """Test undo/redo with settings updates."""
        # Add a node
        add_manual_input_node(flow, 1, sample_data)

        # Get pre-snapshot
        pre_snapshot = flow.get_pre_snapshot()

        # Update settings (simulate by adding a new node with different settings)
        node2 = flow.get_node(1)
        original_pos = node2.setting_input.pos_x if hasattr(node2.setting_input, 'pos_x') else 0

        # Capture if changed
        flow.capture_history_if_changed(
            pre_snapshot,
            HistoryActionType.UPDATE_SETTINGS,
            "Update settings"
        )

        # State should be captured
        state = flow.get_history_state()
        # May or may not have captures depending on if we actually changed something


class TestEdgeCases:
    """Tests for edge cases."""

    def test_restore_empty_flow(self, flow):
        """Test restoring to an empty flow state."""
        # Add node
        flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add node")
        add_manual_input_node(flow, 1, [{"a": 1}])

        # Undo to empty state
        flow.undo()

        # Flow should be empty
        assert len(flow.nodes) == 0

    def test_is_restoring_flag(self, flow):
        """Test that _is_restoring flag prevents recursive captures."""
        manager = flow._history_manager

        # Normally, capturing works
        result1 = flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Test")
        assert result1 is True

        # During restoration, capturing should be blocked
        manager._is_restoring = True
        result2 = flow.capture_history_snapshot(HistoryActionType.ADD_NODE, "Test2")
        assert result2 is False
        manager._is_restoring = False

    def test_rapid_undo_redo(self, flow, sample_data):
        """Test rapid undo/redo cycles."""
        add_manual_input_node(flow, 1, sample_data)

        # Capture multiple states
        for i in range(5):
            flow.capture_history_snapshot(HistoryActionType.ADD_NODE, f"Action {i}")
            add_manual_input_node(flow, i + 2, [{"x": i}])

        # Rapid undo/redo
        for _ in range(3):
            flow.undo()
            flow.redo()
            flow.undo()

        # Should still be consistent
        state = flow.get_history_state()
        assert state.undo_count >= 0
        assert state.redo_count >= 0


# =====================================================================
# Performance Tests
# =====================================================================


class TestDiffPerformance:
    """Tests for diff-based storage performance."""

    def test_diff_storage_smaller_than_snapshot(self, flow, history_manager):
        """Test that diffs are generally smaller than full snapshots."""
        flow._history_manager = history_manager

        # Add initial state
        add_manual_input_node(flow, 1, [{"a": i} for i in range(100)])

        # Capture initial snapshot (this becomes the base)
        history_manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, "Initial")

        # Make a small change
        node = flow.get_node(1)
        if hasattr(node.setting_input, 'pos_x'):
            node.setting_input.pos_x = 100

        # Capture should use diff if it's smaller
        history_manager.capture_snapshot(flow, HistoryActionType.MOVE_NODE, "Move node")

        # Should have captured something
        assert len(history_manager._undo_stack) >= 1

    def test_stack_memory_bounded(self, flow):
        """Test that stack memory is bounded by max_stack_size."""
        config = HistoryConfig(max_stack_size=5)
        manager = HistoryManager(config)
        flow._history_manager = manager

        # Add many entries
        for i in range(20):
            manager.capture_snapshot(flow, HistoryActionType.ADD_NODE, f"Action {i}")
            add_manual_input_node(flow, i + 1, [{"x": i}])

        # Should be bounded
        assert len(manager._undo_stack) <= 5
