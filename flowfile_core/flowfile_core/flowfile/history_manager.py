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

    def capture_snapshot(
        self,
        flow_graph: "FlowGraph",
        action_type: HistoryActionType,
        description: str,
        node_id: Optional[int] = None,
    ) -> None:
        """Capture the current flow state before making changes.

        This method should be called BEFORE modifying the flow graph.
        The snapshot captures the state that can be restored via undo.

        Args:
            flow_graph: The FlowGraph instance to capture.
            action_type: The type of action about to be performed.
            description: Human-readable description of the action.
            node_id: Optional node ID if the action involves a specific node.
        """
        if not self.config.enabled:
            return

        if self._is_restoring:
            # Don't capture snapshots during undo/redo operations
            return

        try:
            snapshot = flow_graph.get_flowfile_data()
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
            logger.info(f"History: Captured snapshot for '{description}' (undo stack size: {len(self._undo_stack)})")
        except Exception as e:
            logger.error(f"History: Failed to capture snapshot: {e}")

    def undo(self, flow_graph: "FlowGraph") -> UndoRedoResult:
        """Undo the last action by restoring the previous state.

        Args:
            flow_graph: The FlowGraph instance to restore.

        Returns:
            UndoRedoResult indicating success or failure.
        """
        if not self.can_undo:
            return UndoRedoResult(
                success=False,
                error_message="Nothing to undo",
            )

        if flow_graph.flow_settings.is_running:
            return UndoRedoResult(
                success=False,
                error_message="Cannot undo while flow is running",
            )

        try:
            self._is_restoring = True

            # Get the entry to undo (this contains the state BEFORE the action)
            entry = self._undo_stack.pop()

            # Capture current state for redo BEFORE restoring
            current_snapshot = flow_graph.get_flowfile_data()
            redo_entry = HistoryEntry(
                snapshot=current_snapshot,
                action_type=entry.action_type,
                description=entry.description,
                timestamp=time(),
                node_id=entry.node_id,
            )
            self._redo_stack.append(redo_entry)

            # Restore the previous state
            flow_graph.restore_from_snapshot(entry.snapshot)

            logger.info(f"History: Undone '{entry.description}'")
            return UndoRedoResult(
                success=True,
                action_description=entry.description,
            )

        except Exception as e:
            logger.error(f"History: Undo failed: {e}")
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
        if not self.can_redo:
            return UndoRedoResult(
                success=False,
                error_message="Nothing to redo",
            )

        if flow_graph.flow_settings.is_running:
            return UndoRedoResult(
                success=False,
                error_message="Cannot redo while flow is running",
            )

        try:
            self._is_restoring = True

            # Get the entry to redo (this contains the state AFTER the action)
            entry = self._redo_stack.pop()

            # Capture current state for undo BEFORE restoring
            current_snapshot = flow_graph.get_flowfile_data()
            undo_entry = HistoryEntry(
                snapshot=current_snapshot,
                action_type=entry.action_type,
                description=entry.description,
                timestamp=time(),
                node_id=entry.node_id,
            )
            self._undo_stack.append(undo_entry)

            # Restore the redo state
            flow_graph.restore_from_snapshot(entry.snapshot)

            logger.info(f"History: Redone '{entry.description}'")
            return UndoRedoResult(
                success=True,
                action_description=entry.description,
            )

        except Exception as e:
            logger.error(f"History: Redo failed: {e}")
            return UndoRedoResult(
                success=False,
                error_message=str(e),
            )
        finally:
            self._is_restoring = False

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
