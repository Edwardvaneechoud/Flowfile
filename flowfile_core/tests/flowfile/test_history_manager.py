"""
Tests for the undo/redo history management system.

Tests cover:
- HistoryManager class (unit tests)
- FlowGraph undo/redo integration
- Snapshot comparison and change detection
"""

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection, delete_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.history_manager import HistoryManager
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
    HistoryConfig,
    HistoryEntry,
    HistoryState,
)


# ============================================================================
# Helper Functions
# ============================================================================

def create_flowfile_handler() -> FlowfileHandler:
    """Create a fresh FlowfileHandler instance."""
    handler = FlowfileHandler()
    assert handler._flows == {}, 'Flow should be empty'
    return handler


def create_graph(flow_id: int = 1) -> FlowGraph:
    """Create a FlowGraph with the given flow_id."""
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name='test_flow', path='.'))
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1) -> FlowGraph:
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


def add_node_promise(graph: FlowGraph, node_type: str, node_id: int) -> None:
    """Add a node promise to the graph."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type=node_type
    )
    graph.add_node_promise(node_promise)


# ============================================================================
# HistoryManager Unit Tests
# ============================================================================

class TestHistoryManagerInit:
    """Tests for HistoryManager initialization."""

    def test_init_default_config(self):
        """Test HistoryManager initializes with default config."""
        manager = HistoryManager()

        assert manager.config.enabled is True
        assert manager.config.max_stack_size == 50
        assert manager.can_undo is False
        assert manager.can_redo is False
        assert manager.undo_count == 0
        assert manager.redo_count == 0

    def test_init_custom_config(self):
        """Test HistoryManager with custom configuration."""
        config = HistoryConfig(enabled=True, max_stack_size=10)
        manager = HistoryManager(config=config)

        assert manager.config.max_stack_size == 10
        assert manager.config.enabled is True

    def test_init_disabled(self):
        """Test HistoryManager can be disabled via config."""
        config = HistoryConfig(enabled=False)
        manager = HistoryManager(config=config)

        assert manager.config.enabled is False


class TestHistoryManagerState:
    """Tests for HistoryManager state management."""

    def test_get_state_empty(self):
        """Test get_state returns correct state when empty."""
        manager = HistoryManager()
        state = manager.get_state()

        assert isinstance(state, HistoryState)
        assert state.can_undo is False
        assert state.can_redo is False
        assert state.undo_description is None
        assert state.redo_description is None
        assert state.undo_count == 0
        assert state.redo_count == 0

    def test_is_restoring_default(self):
        """Test is_restoring is False by default."""
        manager = HistoryManager()
        assert manager.is_restoring() is False


class TestHistoryManagerClear:
    """Tests for clearing history."""

    def test_clear_empty(self):
        """Test clearing empty history doesn't cause errors."""
        manager = HistoryManager()
        manager.clear()

        assert manager.undo_count == 0
        assert manager.redo_count == 0


class TestSnapshotComparison:
    """Tests for snapshot comparison functionality."""

    def test_snapshots_equal_identical(self):
        """Test that identical snapshots are detected as equal."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)

        snapshot1 = graph.get_flowfile_data()
        snapshot2 = graph.get_flowfile_data()

        manager = HistoryManager()
        assert manager._snapshots_equal(snapshot1, snapshot2) is True

    def test_snapshots_equal_different_nodes(self):
        """Test that snapshots with different nodes are detected as different."""
        graph1 = create_graph(flow_id=1)
        add_manual_input(graph1, [{"name": "John"}], node_id=1)
        snapshot1 = graph1.get_flowfile_data()

        graph2 = create_graph(flow_id=2)
        add_manual_input(graph2, [{"name": "Jane"}], node_id=1)
        snapshot2 = graph2.get_flowfile_data()

        manager = HistoryManager()
        assert manager._snapshots_equal(snapshot1, snapshot2) is False

    def test_snapshots_equal_different_data(self):
        """Test that snapshots with different data are detected as different."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)
        snapshot1 = graph.get_flowfile_data()

        # Update the data by calling add_manual_input again with different data
        new_input = input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "Jane"}])
        )
        graph.add_manual_input(new_input)
        snapshot2 = graph.get_flowfile_data()

        manager = HistoryManager()
        assert manager._snapshots_equal(snapshot1, snapshot2) is False


# ============================================================================
# FlowGraph History Integration Tests
# ============================================================================

class TestFlowGraphHistoryCapture:
    """Tests for capturing history snapshots via FlowGraph."""

    def test_capture_snapshot_adds_to_undo_stack(self):
        """Test that capturing a snapshot adds to the undo stack."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)

        # Capture initial state
        graph.capture_history_snapshot(
            HistoryActionType.ADD_NODE,
            "Add manual_input node"
        )

        state = graph.get_history_state()
        assert state.can_undo is True
        assert state.undo_count == 1
        assert state.undo_description == "Add manual_input node"

    def test_capture_clears_redo_stack(self):
        """Test that capturing a new snapshot clears the redo stack."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)

        # Capture and then undo to create redo entry
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "First action")
        add_node_promise(graph, "filter", 2)
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Second action")

        # Undo to have something in redo stack
        graph.undo()
        state = graph.get_history_state()
        assert state.can_redo is True

        # New capture should clear redo
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Third action")
        state = graph.get_history_state()
        assert state.can_redo is False

    def test_capture_with_node_id(self):
        """Test capturing snapshot with specific node_id."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)

        graph.capture_history_snapshot(
            HistoryActionType.UPDATE_SETTINGS,
            "Update settings",
            node_id=1
        )

        state = graph.get_history_state()
        assert state.can_undo is True


class TestFlowGraphUndo:
    """Tests for undo functionality via FlowGraph."""

    def test_undo_restores_previous_state(self):
        """Test that undo restores the previous graph state."""
        graph = create_graph()

        # Add first node and capture
        add_manual_input(graph, [{"name": "John"}], node_id=1)
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add first node")

        # Add second node
        add_node_promise(graph, "filter", 2)
        assert len(graph.nodes) == 2

        # Undo should restore to one node
        result = graph.undo()
        assert result.success is True
        assert result.action_description == "Add first node"
        assert len(graph.nodes) == 1

    def test_undo_nothing_to_undo(self):
        """Test undo when there's nothing to undo."""
        graph = create_graph()

        result = graph.undo()
        assert result.success is False
        assert "Nothing to undo" in result.error_message

    def test_undo_moves_to_redo_stack(self):
        """Test that undo moves the entry to redo stack."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add node")

        assert graph.get_history_state().can_redo is False

        graph.undo()

        state = graph.get_history_state()
        assert state.can_redo is True
        assert state.redo_count == 1

    def test_undo_multiple_times(self):
        """Test undoing multiple actions in sequence."""
        graph = create_graph()

        # Create history with multiple actions
        # Capture BEFORE adding each node
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Before add node 1")
        add_manual_input(graph, [{"name": "John"}], node_id=1)

        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Before add node 2")
        add_node_promise(graph, "filter", 2)

        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Before add node 3")
        add_node_promise(graph, "select", 3)

        assert len(graph.nodes) == 3
        assert graph.get_history_state().undo_count == 3

        # Undo three times
        graph.undo()  # Restore to before node 3
        assert len(graph.nodes) == 2

        graph.undo()  # Restore to before node 2
        assert len(graph.nodes) == 1

        graph.undo()  # Restore to before node 1
        assert len(graph.nodes) == 0


class TestFlowGraphRedo:
    """Tests for redo functionality via FlowGraph."""

    def test_redo_restores_undone_state(self):
        """Test that redo restores a previously undone state."""
        graph = create_graph()

        add_manual_input(graph, [{"name": "John"}], node_id=1)
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add node")

        add_node_promise(graph, "filter", 2)
        assert len(graph.nodes) == 2

        # Undo and then redo
        graph.undo()
        assert len(graph.nodes) == 1

        result = graph.redo()
        assert result.success is True
        assert len(graph.nodes) == 2

    def test_redo_nothing_to_redo(self):
        """Test redo when there's nothing to redo."""
        graph = create_graph()

        result = graph.redo()
        assert result.success is False
        assert "Nothing to redo" in result.error_message

    def test_redo_moves_to_undo_stack(self):
        """Test that redo moves entry back to undo stack."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add node")

        graph.undo()
        assert graph.get_history_state().undo_count == 0

        graph.redo()
        assert graph.get_history_state().undo_count == 1


class TestCaptureIfChanged:
    """Tests for capture_if_changed functionality (change detection)."""

    def test_capture_if_changed_with_actual_change(self):
        """Test that capture_if_changed captures when state changed."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)

        # Capture pre-state
        pre_snapshot = graph.get_flowfile_data()

        # Make a change
        add_node_promise(graph, "filter", 2)

        # Should capture because state changed
        result = graph.capture_history_if_changed(
            pre_snapshot,
            HistoryActionType.ADD_NODE,
            "Add filter node"
        )

        assert result is True
        assert graph.get_history_state().can_undo is True

    def test_capture_if_changed_no_change(self):
        """Test that capture_if_changed skips when no change occurred."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)

        # Capture pre-state
        pre_snapshot = graph.get_flowfile_data()

        # Don't make any changes

        # Should skip because state didn't change
        result = graph.capture_history_if_changed(
            pre_snapshot,
            HistoryActionType.UPDATE_SETTINGS,
            "Update settings"
        )

        assert result is False
        assert graph.get_history_state().can_undo is False

    def test_capture_if_changed_settings_update(self):
        """Test capture_if_changed with actual settings update."""
        graph = create_graph()
        add_manual_input(graph, [{"name": "John"}], node_id=1)

        # Capture pre-state
        pre_snapshot = graph.get_flowfile_data()

        # Update settings by calling the proper update method
        new_input = input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"name": "Jane"}])
        )
        graph.add_manual_input(new_input)

        # Should capture because data changed
        result = graph.capture_history_if_changed(
            pre_snapshot,
            HistoryActionType.UPDATE_SETTINGS,
            "Update manual_input settings"
        )

        assert result is True


class TestHistoryWithConnections:
    """Tests for undo/redo with node connections."""

    def test_undo_restores_connections(self):
        """Test that undo correctly restores node connections."""
        graph = create_graph()

        # Add two nodes
        add_manual_input(graph, [{"name": "John"}], node_id=1)
        add_node_promise(graph, "filter", 2)

        # Capture state before connection
        graph.capture_history_snapshot(HistoryActionType.ADD_CONNECTION, "Before connection")

        # Add connection
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        assert len(graph.node_connections) == 1

        # Undo should remove connection
        graph.undo()
        assert len(graph.node_connections) == 0

    def test_redo_restores_connections(self):
        """Test that redo correctly restores node connections."""
        graph = create_graph()

        # Setup with connection
        add_manual_input(graph, [{"name": "John"}], node_id=1)
        add_node_promise(graph, "filter", 2)
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        graph.capture_history_snapshot(HistoryActionType.DELETE_CONNECTION, "Before delete")

        # Delete connection using the module-level function
        delete_connection(graph, connection)
        assert len(graph.node_connections) == 0

        # Undo brings connection back
        graph.undo()
        assert len(graph.node_connections) == 1

        # Redo removes it again
        graph.redo()
        assert len(graph.node_connections) == 0


class TestHistoryStackLimits:
    """Tests for history stack size limits."""

    def test_max_stack_size_enforced(self):
        """Test that stack size limit is enforced."""
        config = HistoryConfig(max_stack_size=3)
        graph = create_graph()
        graph._history_manager = HistoryManager(config=config)

        # Add more items than max size
        for i in range(5):
            add_node_promise(graph, "filter", i + 10)
            graph.capture_history_snapshot(HistoryActionType.ADD_NODE, f"Add node {i}")

        # Should only have max_stack_size items
        assert graph.get_history_state().undo_count == 3


class TestHistoryDisabled:
    """Tests for disabled history functionality."""

    def test_capture_when_disabled(self):
        """Test that capture does nothing when history is disabled."""
        config = HistoryConfig(enabled=False)
        graph = create_graph()
        graph._history_manager = HistoryManager(config=config)

        add_manual_input(graph, [{"name": "John"}], node_id=1)
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Add node")

        assert graph.get_history_state().can_undo is False
        assert graph.get_history_state().undo_count == 0


class TestHistoryWithComplexOperations:
    """Tests for history with complex flow operations."""

    def test_undo_redo_full_workflow(self):
        """Test complete undo/redo workflow with multiple operations."""
        graph = create_graph()

        # Step 1: Add input node (capture BEFORE)
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Before add input")
        add_manual_input(graph, [{"name": "John", "age": 30}], node_id=1)

        # Step 2: Add filter node (capture BEFORE)
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Before add filter")
        add_node_promise(graph, "filter", 2)
        filter_input = transform_schema.FilterInput(
            advanced_filter="[age] > 25",
            filter_type='advanced'
        )
        filter_settings = input_schema.NodeFilter(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=filter_input
        )
        graph.add_filter(filter_settings)

        # Step 3: Connect nodes (capture BEFORE)
        graph.capture_history_snapshot(HistoryActionType.ADD_CONNECTION, "Before connect nodes")
        connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
        add_connection(graph, connection)

        # Verify initial state
        assert len(graph.nodes) == 2
        assert len(graph.node_connections) == 1
        assert graph.get_history_state().undo_count == 3

        # Undo connection
        result = graph.undo()
        assert result.success is True
        assert len(graph.node_connections) == 0

        # Undo filter
        result = graph.undo()
        assert result.success is True
        assert len(graph.nodes) == 1

        # Redo filter
        result = graph.redo()
        assert result.success is True
        assert len(graph.nodes) == 2

        # Redo connection
        result = graph.redo()
        assert result.success is True
        assert len(graph.node_connections) == 1

    def test_history_preserves_node_count(self):
        """Test that undo/redo preserves node count correctly."""
        graph = create_graph()

        # Capture before adding node 1
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Before node 1")
        add_manual_input(graph, [{"name": "John", "age": 30}], node_id=1)

        # Capture before adding node 2
        graph.capture_history_snapshot(HistoryActionType.ADD_NODE, "Before node 2")
        add_manual_input(graph, [{"name": "Jane", "age": 25}], node_id=2)

        # Verify we have 2 nodes
        assert len(graph.nodes) == 2

        # Undo should restore to 1 node
        graph.undo()
        assert len(graph.nodes) == 1
        assert graph.get_node(1) is not None
        assert graph.get_node(2) is None

        # Redo should restore to 2 nodes
        graph.redo()
        assert len(graph.nodes) == 2
