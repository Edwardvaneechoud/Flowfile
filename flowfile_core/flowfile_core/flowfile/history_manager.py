"""
History Manager for undo/redo functionality in FlowGraph.

This module provides a HistoryManager class that tracks changes to flow graphs
and allows users to undo and redo actions efficiently using diff-based storage.
"""

from collections import deque
from time import time
from typing import TYPE_CHECKING, Any, Optional

from flowfile_core.configs import logger
from flowfile_core.schemas.history_schema import (
    ConnectionDiff,
    FlowDiff,
    HistoryActionType,
    HistoryConfig,
    HistoryEntry,
    HistoryState,
    NodeDiff,
    SettingsDiff,
    UndoRedoResult,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph
    from flowfile_core.schemas.schemas import FlowfileData


class HistoryManager:
    """Manages undo/redo history for a FlowGraph.

    This class provides efficient history tracking using diff-based storage.
    Instead of storing full snapshots for every change, it stores only the
    differences between states, reducing memory usage significantly.

    Attributes:
        config: Configuration for history behavior.
        _undo_stack: Stack of history entries for undo operations.
        _redo_stack: Stack of history entries for redo operations.
        _is_restoring: Flag to prevent recursive captures during restore.
        _base_snapshot: The base snapshot for diff computation.
    """

    def __init__(self, config: Optional[HistoryConfig] = None):
        """Initialize the HistoryManager.

        Args:
            config: Optional configuration for history behavior.
        """
        self.config = config or HistoryConfig()
        self._undo_stack: deque[HistoryEntry] = deque(maxlen=self.config.max_stack_size)
        self._redo_stack: deque[HistoryEntry] = deque(maxlen=self.config.max_stack_size)
        self._is_restoring: bool = False
        self._base_snapshot: Optional[dict[str, Any]] = None
        self._last_snapshot: Optional[dict[str, Any]] = None

    def _get_snapshot_dict(self, flow_graph: "FlowGraph") -> dict[str, Any]:
        """Get the current flow state as a dictionary for comparison.

        Args:
            flow_graph: The FlowGraph to capture.

        Returns:
            A dictionary representation of the flow state.
        """
        flowfile_data = flow_graph.get_flowfile_data()
        return flowfile_data.model_dump(mode="json")

    def _snapshots_equal(self, snap1: dict[str, Any], snap2: dict[str, Any]) -> bool:
        """Compare two snapshots for equality.

        Compares only the relevant parts of the snapshots (nodes and settings),
        ignoring metadata like version and timestamps.

        Args:
            snap1: First snapshot dictionary.
            snap2: Second snapshot dictionary.

        Returns:
            True if the snapshots are effectively equal.
        """
        if snap1 is None or snap2 is None:
            return snap1 is snap2

        # Compare nodes
        nodes1 = {n["id"]: n for n in snap1.get("nodes", [])}
        nodes2 = {n["id"]: n for n in snap2.get("nodes", [])}

        if nodes1 != nodes2:
            return False

        # Compare settings
        settings1 = snap1.get("flowfile_settings", {})
        settings2 = snap2.get("flowfile_settings", {})

        return settings1 == settings2

    def _compute_diff(
        self, old_snapshot: dict[str, Any], new_snapshot: dict[str, Any]
    ) -> FlowDiff:
        """Compute the diff between two snapshots.

        Args:
            old_snapshot: The previous state.
            new_snapshot: The current state.

        Returns:
            A FlowDiff object representing the changes.
        """
        node_changes: list[NodeDiff] = []
        connection_changes: list[ConnectionDiff] = []
        settings_changes: Optional[SettingsDiff] = None

        # Get node dictionaries
        old_nodes = {n["id"]: n for n in old_snapshot.get("nodes", [])}
        new_nodes = {n["id"]: n for n in new_snapshot.get("nodes", [])}

        # Find added nodes
        for node_id in set(new_nodes.keys()) - set(old_nodes.keys()):
            node_changes.append(
                NodeDiff(node_id=node_id, action="add", new_data=new_nodes[node_id])
            )

        # Find removed nodes
        for node_id in set(old_nodes.keys()) - set(new_nodes.keys()):
            node_changes.append(
                NodeDiff(node_id=node_id, action="remove", old_data=old_nodes[node_id])
            )

        # Find modified nodes
        for node_id in set(old_nodes.keys()) & set(new_nodes.keys()):
            if old_nodes[node_id] != new_nodes[node_id]:
                node_changes.append(
                    NodeDiff(
                        node_id=node_id,
                        action="modify",
                        old_data=old_nodes[node_id],
                        new_data=new_nodes[node_id],
                    )
                )

        # Compute connection changes from node data
        old_connections = self._extract_connections(old_snapshot)
        new_connections = self._extract_connections(new_snapshot)

        for conn in new_connections - old_connections:
            connection_changes.append(
                ConnectionDiff(
                    from_node_id=conn[0],
                    to_node_id=conn[1],
                    connection_type=conn[2],
                    action="add",
                )
            )

        for conn in old_connections - new_connections:
            connection_changes.append(
                ConnectionDiff(
                    from_node_id=conn[0],
                    to_node_id=conn[1],
                    connection_type=conn[2],
                    action="remove",
                )
            )

        # Check settings changes
        old_settings = old_snapshot.get("flowfile_settings", {})
        new_settings = new_snapshot.get("flowfile_settings", {})
        if old_settings != new_settings:
            settings_changes = SettingsDiff(old_settings=old_settings, new_settings=new_settings)

        # Check start node changes
        old_start_nodes = set()
        new_start_nodes = set()
        for node in old_snapshot.get("nodes", []):
            if node.get("is_start_node"):
                old_start_nodes.add(node["id"])
        for node in new_snapshot.get("nodes", []):
            if node.get("is_start_node"):
                new_start_nodes.add(node["id"])

        start_node_changes = None
        added_starts = list(new_start_nodes - old_start_nodes)
        removed_starts = list(old_start_nodes - new_start_nodes)
        if added_starts or removed_starts:
            start_node_changes = {"added": added_starts, "removed": removed_starts}

        return FlowDiff(
            node_changes=node_changes,
            connection_changes=connection_changes,
            settings_changes=settings_changes,
            start_node_changes=start_node_changes,
        )

    def _extract_connections(self, snapshot: dict[str, Any]) -> set[tuple[int, int, str]]:
        """Extract connections from a snapshot as a set of tuples.

        Args:
            snapshot: The snapshot to extract connections from.

        Returns:
            A set of (from_id, to_id, connection_type) tuples.
        """
        connections = set()
        for node in snapshot.get("nodes", []):
            node_id = node["id"]
            # Main inputs
            for input_id in node.get("input_ids", []) or []:
                if input_id is not None:
                    connections.add((input_id, node_id, "main"))
            # Left input
            left_id = node.get("left_input_id")
            if left_id is not None:
                connections.add((left_id, node_id, "left"))
            # Right input
            right_id = node.get("right_input_id")
            if right_id is not None:
                connections.add((right_id, node_id, "right"))

        return connections

    def _apply_diff_reverse(
        self, snapshot: dict[str, Any], diff: FlowDiff
    ) -> dict[str, Any]:
        """Apply a diff in reverse to get the previous state.

        Args:
            snapshot: The current snapshot.
            diff: The diff to reverse.

        Returns:
            The previous state snapshot.
        """
        import copy

        result = copy.deepcopy(snapshot)
        nodes = {n["id"]: n for n in result.get("nodes", [])}

        for node_change in diff.node_changes:
            if node_change.action == "add":
                # Reverse: remove the added node
                nodes.pop(node_change.node_id, None)
            elif node_change.action == "remove":
                # Reverse: add the removed node back
                if node_change.old_data:
                    nodes[node_change.node_id] = node_change.old_data
            elif node_change.action == "modify":
                # Reverse: restore the old data
                if node_change.old_data:
                    nodes[node_change.node_id] = node_change.old_data

        result["nodes"] = list(nodes.values())

        # Reverse settings changes
        if diff.settings_changes and diff.settings_changes.old_settings:
            result["flowfile_settings"] = diff.settings_changes.old_settings

        return result

    def _apply_diff_forward(
        self, snapshot: dict[str, Any], diff: FlowDiff
    ) -> dict[str, Any]:
        """Apply a diff forward to get the next state.

        Args:
            snapshot: The current snapshot.
            diff: The diff to apply.

        Returns:
            The next state snapshot.
        """
        import copy

        result = copy.deepcopy(snapshot)
        nodes = {n["id"]: n for n in result.get("nodes", [])}

        for node_change in diff.node_changes:
            if node_change.action == "add":
                # Forward: add the node
                if node_change.new_data:
                    nodes[node_change.node_id] = node_change.new_data
            elif node_change.action == "remove":
                # Forward: remove the node
                nodes.pop(node_change.node_id, None)
            elif node_change.action == "modify":
                # Forward: apply the new data
                if node_change.new_data:
                    nodes[node_change.node_id] = node_change.new_data

        result["nodes"] = list(nodes.values())

        # Apply settings changes
        if diff.settings_changes and diff.settings_changes.new_settings:
            result["flowfile_settings"] = diff.settings_changes.new_settings

        return result

    def capture_snapshot(
        self,
        flow_graph: "FlowGraph",
        action_type: HistoryActionType,
        description: str,
        node_id: Optional[int] = None,
    ) -> bool:
        """Capture the current state before a change.

        This should be called BEFORE making changes to the flow graph.
        The captured state represents the state we can return to via undo.

        Args:
            flow_graph: The FlowGraph to capture.
            action_type: The type of action being performed.
            description: A human-readable description of the action.
            node_id: Optional ID of the node involved in the action.

        Returns:
            True if the snapshot was captured, False if skipped.
        """
        if not self.config.enabled or self._is_restoring:
            return False

        current_snapshot = self._get_snapshot_dict(flow_graph)

        # Check if state has changed from last snapshot
        if self._last_snapshot is not None and self._snapshots_equal(
            current_snapshot, self._last_snapshot
        ):
            logger.debug("Skipping duplicate snapshot capture")
            return False

        # Create history entry
        entry = HistoryEntry(
            action_type=action_type,
            description=description,
            timestamp=time(),
            node_id=node_id,
            is_full_snapshot=True,
            snapshot_data=current_snapshot,
        )

        # If using diff storage and we have a base, compute diff instead
        if self.config.use_diff_storage and self._base_snapshot is not None:
            diff = self._compute_diff(self._base_snapshot, current_snapshot)
            # Only use diff if it's smaller than full snapshot
            diff_size = len(str(diff.model_dump()))
            snapshot_size = len(str(current_snapshot))
            if diff_size < snapshot_size * 0.7:  # Use diff if it's at least 30% smaller
                entry = HistoryEntry(
                    action_type=action_type,
                    description=description,
                    timestamp=time(),
                    node_id=node_id,
                    is_full_snapshot=False,
                    diff_data=diff,
                )

        self._undo_stack.append(entry)
        self._last_snapshot = current_snapshot

        # Clear redo stack when new action is performed
        self._redo_stack.clear()

        # Update base snapshot periodically (every 10 entries) for diff efficiency
        if len(self._undo_stack) % 10 == 0 or self._base_snapshot is None:
            self._base_snapshot = current_snapshot

        logger.debug(f"Captured history snapshot: {description}")
        return True

    def capture_if_changed(
        self,
        flow_graph: "FlowGraph",
        pre_snapshot: dict[str, Any],
        action_type: HistoryActionType,
        description: str,
        node_id: Optional[int] = None,
    ) -> bool:
        """Capture state only if it actually changed.

        This is useful for settings updates that might not actually change anything.
        Call this AFTER making changes, passing the pre-change snapshot.

        Args:
            flow_graph: The FlowGraph to capture.
            pre_snapshot: The snapshot from before the change was made.
            action_type: The type of action performed.
            description: A human-readable description of the action.
            node_id: Optional ID of the node involved.

        Returns:
            True if a change was detected and captured, False otherwise.
        """
        if not self.config.enabled or self._is_restoring:
            return False

        current_snapshot = self._get_snapshot_dict(flow_graph)

        # Only capture if state actually changed
        if self._snapshots_equal(pre_snapshot, current_snapshot):
            logger.debug("No change detected, skipping history capture")
            return False

        # Store the pre-change snapshot (the state we want to undo to)
        entry = HistoryEntry(
            action_type=action_type,
            description=description,
            timestamp=time(),
            node_id=node_id,
            is_full_snapshot=True,
            snapshot_data=pre_snapshot,
        )

        # If using diff storage, compute and store diff
        if self.config.use_diff_storage and self._base_snapshot is not None:
            diff = self._compute_diff(self._base_snapshot, pre_snapshot)
            diff_size = len(str(diff.model_dump()))
            snapshot_size = len(str(pre_snapshot))
            if diff_size < snapshot_size * 0.7:
                entry = HistoryEntry(
                    action_type=action_type,
                    description=description,
                    timestamp=time(),
                    node_id=node_id,
                    is_full_snapshot=False,
                    diff_data=diff,
                )

        self._undo_stack.append(entry)
        self._last_snapshot = current_snapshot

        # Clear redo stack when new action is performed
        self._redo_stack.clear()

        # Update base snapshot periodically
        if len(self._undo_stack) % 10 == 0 or self._base_snapshot is None:
            self._base_snapshot = current_snapshot

        logger.debug(f"Captured history snapshot (changed): {description}")
        return True

    def get_pre_snapshot(self, flow_graph: "FlowGraph") -> dict[str, Any]:
        """Get a snapshot for comparison purposes.

        Call this BEFORE making changes when using capture_if_changed.

        Args:
            flow_graph: The FlowGraph to capture.

        Returns:
            A dictionary snapshot of the current state.
        """
        return self._get_snapshot_dict(flow_graph)

    def _reconstruct_snapshot(self, entry: HistoryEntry) -> dict[str, Any]:
        """Reconstruct a full snapshot from a history entry.

        If the entry uses diff storage, this applies the diff to the base
        snapshot to reconstruct the full state.

        Args:
            entry: The history entry to reconstruct.

        Returns:
            The full snapshot dictionary.
        """
        if entry.is_full_snapshot and entry.snapshot_data is not None:
            return entry.snapshot_data

        if entry.diff_data is not None and self._base_snapshot is not None:
            return self._apply_diff_reverse(self._base_snapshot, entry.diff_data)

        raise ValueError("Cannot reconstruct snapshot: missing data")

    def undo(self, flow_graph: "FlowGraph") -> UndoRedoResult:
        """Undo the last action.

        Pops from undo stack, pushes current state to redo stack,
        and restores the popped snapshot.

        Args:
            flow_graph: The FlowGraph to modify.

        Returns:
            An UndoRedoResult indicating success or failure.
        """
        if not self._undo_stack:
            return UndoRedoResult(
                success=False,
                error_message="Nothing to undo",
                new_state=self.get_state(),
            )

        try:
            self._is_restoring = True

            # Get current state for redo
            current_snapshot = self._get_snapshot_dict(flow_graph)

            # Pop the last action
            entry = self._undo_stack.pop()

            # Reconstruct the snapshot to restore
            snapshot_to_restore = self._reconstruct_snapshot(entry)

            # Push current state to redo stack
            redo_entry = HistoryEntry(
                action_type=entry.action_type,
                description=entry.description,
                timestamp=time(),
                node_id=entry.node_id,
                is_full_snapshot=True,
                snapshot_data=current_snapshot,
            )
            self._redo_stack.append(redo_entry)

            # Restore the snapshot
            flow_graph.restore_from_snapshot(snapshot_to_restore)
            self._last_snapshot = snapshot_to_restore

            return UndoRedoResult(
                success=True,
                action_description=f"Undid: {entry.description}",
                new_state=self.get_state(),
            )

        except Exception as e:
            logger.error(f"Error during undo: {e}")
            return UndoRedoResult(
                success=False,
                error_message=str(e),
                new_state=self.get_state(),
            )
        finally:
            self._is_restoring = False

    def redo(self, flow_graph: "FlowGraph") -> UndoRedoResult:
        """Redo the last undone action.

        Pops from redo stack, pushes current state to undo stack,
        and restores the popped snapshot.

        Args:
            flow_graph: The FlowGraph to modify.

        Returns:
            An UndoRedoResult indicating success or failure.
        """
        if not self._redo_stack:
            return UndoRedoResult(
                success=False,
                error_message="Nothing to redo",
                new_state=self.get_state(),
            )

        try:
            self._is_restoring = True

            # Get current state for undo
            current_snapshot = self._get_snapshot_dict(flow_graph)

            # Pop the redo entry
            entry = self._redo_stack.pop()

            # Reconstruct the snapshot to restore
            snapshot_to_restore = self._reconstruct_snapshot(entry)

            # Push current state to undo stack
            undo_entry = HistoryEntry(
                action_type=entry.action_type,
                description=entry.description,
                timestamp=time(),
                node_id=entry.node_id,
                is_full_snapshot=True,
                snapshot_data=current_snapshot,
            )
            self._undo_stack.append(undo_entry)

            # Restore the snapshot
            flow_graph.restore_from_snapshot(snapshot_to_restore)
            self._last_snapshot = snapshot_to_restore

            return UndoRedoResult(
                success=True,
                action_description=f"Redid: {entry.description}",
                new_state=self.get_state(),
            )

        except Exception as e:
            logger.error(f"Error during redo: {e}")
            return UndoRedoResult(
                success=False,
                error_message=str(e),
                new_state=self.get_state(),
            )
        finally:
            self._is_restoring = False

    def get_state(self) -> HistoryState:
        """Get the current history state.

        Returns:
            A HistoryState object with current undo/redo availability.
        """
        undo_desc = None
        redo_desc = None

        if self._undo_stack:
            undo_desc = self._undo_stack[-1].description
        if self._redo_stack:
            redo_desc = self._redo_stack[-1].description

        return HistoryState(
            can_undo=len(self._undo_stack) > 0,
            can_redo=len(self._redo_stack) > 0,
            undo_description=undo_desc,
            redo_description=redo_desc,
            undo_count=len(self._undo_stack),
            redo_count=len(self._redo_stack),
        )

    def clear(self) -> None:
        """Clear all history."""
        self._undo_stack.clear()
        self._redo_stack.clear()
        self._base_snapshot = None
        self._last_snapshot = None
        logger.debug("History cleared")

    @property
    def is_restoring(self) -> bool:
        """Check if we're currently in a restore operation."""
        return self._is_restoring
