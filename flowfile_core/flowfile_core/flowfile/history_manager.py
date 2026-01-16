"""
History Manager for undo/redo functionality in FlowGraph.

This module provides a HistoryManager class that tracks changes to a flow graph
and enables undo/redo operations by maintaining snapshots of the flow state.
"""

from collections import deque
from time import time
from typing import TYPE_CHECKING, Optional

from flowfile_core.configs import logger
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
    HistoryConfig,
    HistoryEntry,
    HistoryState,
    UndoRedoResult,
)
from flowfile_core.schemas.schemas import FlowfileData

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph


class HistoryManager:
    """Manages undo/redo history for a FlowGraph instance.

    This class maintains two stacks (undo and redo) of flow state snapshots.
    When an action is performed, the current state is captured before the change.
    Undo restores the previous state and moves it to the redo stack.
    Redo re-applies the undone action.

    Attributes:
        config: Configuration settings for the history manager.
    """

    def __init__(self, config: Optional[HistoryConfig] = None):
        """Initialize the HistoryManager.

        Args:
            config: Optional configuration. Defaults to HistoryConfig().
        """
        self.config = config or HistoryConfig()
        self._undo_stack: deque[HistoryEntry] = deque(maxlen=self.config.max_stack_size)
        self._redo_stack: deque[HistoryEntry] = deque(maxlen=self.config.max_stack_size)
        self._is_restoring: bool = False

    @property
    def can_undo(self) -> bool:
        """Check if there are actions that can be undone."""
        return len(self._undo_stack) > 0

    @property
    def can_redo(self) -> bool:
        """Check if there are actions that can be redone."""
        return len(self._redo_stack) > 0

    @property
    def undo_count(self) -> int:
        """Get the number of actions in the undo stack."""
        return len(self._undo_stack)

    @property
    def redo_count(self) -> int:
        """Get the number of actions in the redo stack."""
        return len(self._redo_stack)

    def get_state(self) -> HistoryState:
        """Get the current state of the history manager.

        Returns:
            HistoryState object with current undo/redo status.
        """
        return HistoryState(
            can_undo=self.can_undo,
            can_redo=self.can_redo,
            undo_description=self._undo_stack[-1].description if self.can_undo else None,
            redo_description=self._redo_stack[-1].description if self.can_redo else None,
            undo_count=self.undo_count,
            redo_count=self.redo_count,
        )

    def _snapshots_equal(self, snapshot1: FlowfileData, snapshot2: FlowfileData) -> bool:
        """Compare two snapshots to check if they represent the same state.

        Uses Pydantic's model_dump() for deep comparison of all fields.

        Args:
            snapshot1: First snapshot to compare.
            snapshot2: Second snapshot to compare.

        Returns:
            True if snapshots are identical, False otherwise.
        """
        try:
            # Compare using model_dump for deep equality check
            # Exclude timestamp-like fields that might change between captures
            dump1 = snapshot1.model_dump()
            dump2 = snapshot2.model_dump()
            return dump1 == dump2
        except Exception as e:
            logger.warning(f"History: Error comparing snapshots: {e}")
            # If comparison fails, assume they're different to be safe
            return False

    def capture_snapshot(
        self,
        flow_graph: "FlowGraph",
        action_type: HistoryActionType,
        description: str,
        node_id: Optional[int] = None,
    ) -> bool:
        """Capture the current flow state before making changes.

        This method should be called BEFORE modifying the flow graph.
        The snapshot captures the state that can be restored via undo.
        Only captures if the state has actually changed from the last snapshot.

        Args:
            flow_graph: The FlowGraph instance to capture.
            action_type: The type of action about to be performed.
            description: Human-readable description of the action.
            node_id: Optional node ID if the action involves a specific node.

        Returns:
            True if snapshot was captured, False if skipped (no change or disabled).
        """
        logger.info(f"History: capture_snapshot called - action_type={action_type}, description='{description}', node_id={node_id}")

        if not self.config.enabled:
            logger.info("History: Skipping capture - history is disabled")
            return False

        if self._is_restoring:
            logger.info("History: Skipping capture - currently restoring")
            return False

        try:
            logger.info(f"History: Getting flowfile data for snapshot...")
            snapshot = flow_graph.get_flowfile_data()
            logger.info(f"History: Snapshot contains {len(snapshot.nodes)} nodes")

            # Check if state has actually changed from the last snapshot
            if self._undo_stack:
                last_snapshot = self._undo_stack[-1].snapshot
                if self._snapshots_equal(snapshot, last_snapshot):
                    logger.info(f"History: Skipping capture - no changes detected for '{description}'")
                    return False

            entry = HistoryEntry(
                snapshot=snapshot,
                action_type=action_type,
                description=description,
                timestamp=time(),
                node_id=node_id,
            )
            self._undo_stack.append(entry)
            # Clear redo stack when new action is performed
            self._redo_stack.clear()
            logger.info(f"History: Captured snapshot for '{description}' (undo stack size: {len(self._undo_stack)}, redo stack cleared)")
            return True
        except Exception as e:
            logger.error(f"History: Failed to capture snapshot: {e}", exc_info=True)
            return False

    def undo(self, flow_graph: "FlowGraph") -> UndoRedoResult:
        """Undo the last action by restoring the previous state.

        Args:
            flow_graph: The FlowGraph instance to restore.

        Returns:
            UndoRedoResult indicating success or failure.
        """
        logger.info(f"History: undo() called - can_undo={self.can_undo}, undo_stack_size={len(self._undo_stack)}")

        if not self.can_undo:
            logger.info("History: Nothing to undo")
            return UndoRedoResult(
                success=False,
                error_message="Nothing to undo",
            )

        if flow_graph.flow_settings.is_running:
            logger.info("History: Cannot undo - flow is running")
            return UndoRedoResult(
                success=False,
                error_message="Cannot undo while flow is running",
            )

        try:
            self._is_restoring = True

            # Get the entry to undo (this contains the state BEFORE the action)
            entry = self._undo_stack.pop()
            logger.info(f"History: Popped entry '{entry.description}' from undo stack (snapshot has {len(entry.snapshot.nodes)} nodes)")

            # Capture current state for redo BEFORE restoring
            logger.info("History: Capturing current state for redo stack...")
            current_snapshot = flow_graph.get_flowfile_data()
            redo_entry = HistoryEntry(
                snapshot=current_snapshot,
                action_type=entry.action_type,
                description=entry.description,
                timestamp=time(),
                node_id=entry.node_id,
            )
            self._redo_stack.append(redo_entry)
            logger.info(f"History: Pushed current state to redo stack (redo stack size: {len(self._redo_stack)})")

            # Restore the previous state
            logger.info(f"History: Restoring snapshot with {len(entry.snapshot.nodes)} nodes...")
            flow_graph.restore_from_snapshot(entry.snapshot)

            logger.info(f"History: Undo SUCCESS - '{entry.description}' (undo stack: {len(self._undo_stack)}, redo stack: {len(self._redo_stack)})")
            return UndoRedoResult(
                success=True,
                action_description=entry.description,
            )

        except Exception as e:
            logger.error(f"History: Undo FAILED: {e}", exc_info=True)
            return UndoRedoResult(
                success=False,
                error_message=str(e),
            )
        finally:
            self._is_restoring = False

    def redo(self, flow_graph: "FlowGraph") -> UndoRedoResult:
        """Redo the last undone action.

        Args:
            flow_graph: The FlowGraph instance to modify.

        Returns:
            UndoRedoResult indicating success or failure.
        """
        logger.info(f"History: redo() called - can_redo={self.can_redo}, redo_stack_size={len(self._redo_stack)}")

        if not self.can_redo:
            logger.info("History: Nothing to redo")
            return UndoRedoResult(
                success=False,
                error_message="Nothing to redo",
            )

        if flow_graph.flow_settings.is_running:
            logger.info("History: Cannot redo - flow is running")
            return UndoRedoResult(
                success=False,
                error_message="Cannot redo while flow is running",
            )

        try:
            self._is_restoring = True

            # Get the entry to redo (this contains the state AFTER the action)
            entry = self._redo_stack.pop()
            logger.info(f"History: Popped entry '{entry.description}' from redo stack (snapshot has {len(entry.snapshot.nodes)} nodes)")

            # Capture current state for undo BEFORE restoring
            logger.info("History: Capturing current state for undo stack...")
            current_snapshot = flow_graph.get_flowfile_data()
            undo_entry = HistoryEntry(
                snapshot=current_snapshot,
                action_type=entry.action_type,
                description=entry.description,
                timestamp=time(),
                node_id=entry.node_id,
            )
            self._undo_stack.append(undo_entry)
            logger.info(f"History: Pushed current state to undo stack (undo stack size: {len(self._undo_stack)})")

            # Restore the redo state
            logger.info(f"History: Restoring snapshot with {len(entry.snapshot.nodes)} nodes...")
            flow_graph.restore_from_snapshot(entry.snapshot)

            logger.info(f"History: Redo SUCCESS - '{entry.description}' (undo stack: {len(self._undo_stack)}, redo stack: {len(self._redo_stack)})")
            return UndoRedoResult(
                success=True,
                action_description=entry.description,
            )

        except Exception as e:
            logger.error(f"History: Redo FAILED: {e}", exc_info=True)
            return UndoRedoResult(
                success=False,
                error_message=str(e),
            )
        finally:
            self._is_restoring = False

    def capture_if_changed(
        self,
        flow_graph: "FlowGraph",
        pre_snapshot: FlowfileData,
        action_type: HistoryActionType,
        description: str,
        node_id: Optional[int] = None,
    ) -> bool:
        """Add a pre-captured snapshot to history only if the state has changed.

        This method should be called AFTER modifying the flow graph, with a
        snapshot that was captured BEFORE the modification. It compares the
        pre-change snapshot with the current state to determine if a real
        change occurred.

        Args:
            flow_graph: The FlowGraph instance (current state after change).
            pre_snapshot: Snapshot captured BEFORE the change was made.
            action_type: The type of action that was performed.
            description: Human-readable description of the action.
            node_id: Optional node ID if the action involves a specific node.

        Returns:
            True if snapshot was added to history, False if skipped (no change).
        """
        logger.info(f"History: capture_if_changed called - action_type={action_type}, description='{description}'")

        if not self.config.enabled:
            logger.info("History: Skipping capture - history is disabled")
            return False

        if self._is_restoring:
            logger.info("History: Skipping capture - currently restoring")
            return False

        try:
            # Get the current state (after the change)
            current_snapshot = flow_graph.get_flowfile_data()

            # Compare pre and post states
            if self._snapshots_equal(pre_snapshot, current_snapshot):
                logger.info(f"History: Skipping capture - no actual changes for '{description}'")
                return False

            # State changed - add pre-snapshot to history so undo restores it
            entry = HistoryEntry(
                snapshot=pre_snapshot,
                action_type=action_type,
                description=description,
                timestamp=time(),
                node_id=node_id,
            )
            self._undo_stack.append(entry)
            # Clear redo stack when new action is performed
            self._redo_stack.clear()
            logger.info(f"History: Captured snapshot for '{description}' (undo stack size: {len(self._undo_stack)}, redo stack cleared)")
            return True
        except Exception as e:
            logger.error(f"History: Failed to capture snapshot: {e}", exc_info=True)
            return False

    def clear(self) -> None:
        """Clear all history entries."""
        self._undo_stack.clear()
        self._redo_stack.clear()
        logger.info("History: Cleared all history")

    def is_restoring(self) -> bool:
        """Check if the history manager is currently performing a restore operation.

        Returns:
            True if currently restoring, False otherwise.
        """
        return self._is_restoring
