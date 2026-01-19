"""
History Manager for undo/redo functionality in flow graphs.

This module provides the HistoryManager class which manages undo/redo stacks
and enables users to revert or reapply changes to their flow graphs.

Optimizations:
- Compressed snapshots using zlib (60-80% memory reduction)
- Pre-computed hashes for O(1) snapshot comparison
- __slots__ for memory-efficient entry storage
"""

from collections import deque
from time import time
from typing import TYPE_CHECKING, Optional

from flowfile_core.configs import logger
from flowfile_core.schemas.history_schema import (
    CompressedSnapshot,
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
    """Manages undo/redo history for a FlowGraph.

    Uses two deques (undo_stack and redo_stack) to track state changes.
    Snapshots are captured BEFORE changes occur, so undo restores to that state.

    Memory Optimization:
    - Snapshots are compressed using zlib (typically 60-80% size reduction)
    - Hashes are pre-computed for O(1) equality checks
    - HistoryEntry uses __slots__ for reduced memory overhead
    """

    __slots__ = ('_config', '_undo_stack', '_redo_stack', '_is_restoring', '_last_snapshot_hash')

    def __init__(self, config: Optional[HistoryConfig] = None):
        """Initialize the HistoryManager.

        Args:
            config: Optional configuration for history behavior.
        """
        self._config = config or HistoryConfig()
        self._undo_stack: deque[HistoryEntry] = deque(maxlen=self._config.max_stack_size)
        self._redo_stack: deque[HistoryEntry] = deque(maxlen=self._config.max_stack_size)
        self._is_restoring: bool = False
        self._last_snapshot_hash: Optional[int] = None

    @property
    def config(self) -> HistoryConfig:
        """Get the history configuration."""
        return self._config

    @config.setter
    def config(self, config: HistoryConfig):
        """Set the history configuration.

        Note: Changing max_stack_size won't resize existing stacks.
        """
        self._config = config

    def _create_entry(
        self,
        snapshot_dict: dict,
        action_type: HistoryActionType,
        description: str,
        node_id: Optional[int] = None,
    ) -> HistoryEntry:
        """Create a history entry with the configured compression settings.

        Args:
            snapshot_dict: The flow state dictionary.
            action_type: The type of action.
            description: Human-readable description.
            node_id: Optional affected node ID.

        Returns:
            A new HistoryEntry instance.
        """
        return HistoryEntry.from_dict(
            snapshot_dict=snapshot_dict,
            action_type=action_type,
            description=description,
            timestamp=time(),
            node_id=node_id,
            compression_level=self._config.compression_level if self._config.use_compression else 1,
        )

    def capture_snapshot(
        self,
        flow_graph: "FlowGraph",
        action_type: HistoryActionType,
        description: str,
        node_id: Optional[int] = None,
    ) -> bool:
        """Capture the current state of the flow graph BEFORE a change.

        This method captures state BEFORE an operation, so we don't do duplicate
        detection here (we can't know if the operation will change anything).
        Duplicate detection is done in capture_if_changed() which is called AFTER
        an operation completes.

        Args:
            flow_graph: The FlowGraph to capture.
            action_type: The type of action being performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            True if snapshot was captured, False if skipped (disabled or restoring).
        """
        logger.info(f"History: capture_snapshot called for '{description}' (enabled={self._config.enabled}, restoring={self._is_restoring})")

        if not self._config.enabled:
            logger.info(f"History: Skipping '{description}' - history disabled")
            return False

        if self._is_restoring:
            logger.info(f"History: Skipping '{description}' - currently restoring")
            return False

        try:
            # Get the current state as FlowfileData
            flowfile_data = flow_graph.get_flowfile_data()
            snapshot_dict = flowfile_data.model_dump()

            # Create compressed entry - no duplicate detection for pre-change snapshots
            # since we don't know if the operation will change anything yet
            entry = self._create_entry(snapshot_dict, action_type, description, node_id)

            # Add to undo stack
            self._undo_stack.append(entry)

            # Update hash to current state (will be compared against post-change state later if needed)
            self._last_snapshot_hash = CompressedSnapshot._compute_hash(snapshot_dict)

            # Clear redo stack when new action is performed
            self._redo_stack.clear()

            logger.info(
                f"History: Captured '{description}' "
                f"(undo_stack={len(self._undo_stack)}, redo_stack={len(self._redo_stack)})"
            )
            return True

        except Exception as e:
            logger.error(f"History: Failed to capture snapshot for '{description}': {e}")
            return False

    def capture_if_changed(
        self,
        flow_graph: "FlowGraph",
        pre_snapshot: FlowfileData,
        action_type: HistoryActionType,
        description: str,
        node_id: Optional[int] = None,
    ) -> bool:
        """Capture history only if the flow state actually changed.

        Use this for settings updates where the change might be a no-op.
        Call this AFTER the change is applied.

        Args:
            flow_graph: The FlowGraph after the change.
            pre_snapshot: The FlowfileData captured BEFORE the change.
            action_type: The type of action that was performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            True if a change was detected and snapshot was captured.
        """
        if not self._config.enabled:
            logger.debug(f"History: Skipping '{description}' (if_changed) - history disabled")
            return False

        if self._is_restoring:
            logger.debug(f"History: Skipping '{description}' (if_changed) - currently restoring")
            return False

        try:
            # Get the current (post-change) state
            current_snapshot = flow_graph.get_flowfile_data()
            current_dict = current_snapshot.model_dump()
            pre_dict = pre_snapshot.model_dump()

            # Fast hash comparison (no JSON serialization)
            pre_hash = CompressedSnapshot._compute_hash(pre_dict)
            current_hash = CompressedSnapshot._compute_hash(current_dict)

            if pre_hash == current_hash:
                logger.debug(f"History: No change detected for: {description}")
                return False

            # State changed - capture the BEFORE state (compressed)
            entry = self._create_entry(pre_dict, action_type, description, node_id)

            # Add to undo stack
            self._undo_stack.append(entry)
            self._last_snapshot_hash = current_hash

            # Clear redo stack when new action is performed
            self._redo_stack.clear()

            logger.info(
                f"History: Captured '{description}' (after change detection) "
                f"(undo_stack={len(self._undo_stack)}, redo_stack={len(self._redo_stack)})"
            )
            return True

        except Exception as e:
            logger.error(f"History: Failed to capture snapshot for '{description}': {e}")
            return False

    def undo(self, flow_graph: "FlowGraph") -> UndoRedoResult:
        """Undo the last action by restoring to the previous state.

        Args:
            flow_graph: The FlowGraph to restore.

        Returns:
            UndoRedoResult indicating success or failure.
        """
        if not self._undo_stack:
            return UndoRedoResult(
                success=False,
                error_message="Nothing to undo",
            )

        try:
            # Set flag to prevent capturing during restore
            self._is_restoring = True

            # Get the entry to restore from
            entry = self._undo_stack.pop()

            # Save current state to redo stack BEFORE restoring
            current_snapshot = flow_graph.get_flowfile_data()
            current_dict = current_snapshot.model_dump()
            redo_entry = self._create_entry(
                current_dict,
                entry.action_type,
                entry.description,
                entry.node_id,
            )
            self._redo_stack.append(redo_entry)

            # Decompress and restore the flow graph from the snapshot
            snapshot_dict = entry.get_snapshot()
            snapshot_data = FlowfileData.model_validate(snapshot_dict)
            flow_graph.restore_from_snapshot(snapshot_data)

            # Update last snapshot hash
            self._last_snapshot_hash = entry.snapshot_hash

            logger.info(f"Undo successful: {entry.description}")
            return UndoRedoResult(
                success=True,
                action_description=entry.description,
            )

        except Exception as e:
            logger.error(f"Undo failed: {e}")
            return UndoRedoResult(
                success=False,
                error_message=str(e),
            )

        finally:
            self._is_restoring = False

    def redo(self, flow_graph: "FlowGraph") -> UndoRedoResult:
        """Redo the last undone action.

        Args:
            flow_graph: The FlowGraph to restore.

        Returns:
            UndoRedoResult indicating success or failure.
        """
        if not self._redo_stack:
            return UndoRedoResult(
                success=False,
                error_message="Nothing to redo",
            )

        try:
            # Set flag to prevent capturing during restore
            self._is_restoring = True

            # Get the entry to restore from
            entry = self._redo_stack.pop()

            # Save current state to undo stack BEFORE restoring
            current_snapshot = flow_graph.get_flowfile_data()
            current_dict = current_snapshot.model_dump()
            undo_entry = self._create_entry(
                current_dict,
                entry.action_type,
                entry.description,
                entry.node_id,
            )
            self._undo_stack.append(undo_entry)

            # Decompress and restore the flow graph from the snapshot
            snapshot_dict = entry.get_snapshot()
            snapshot_data = FlowfileData.model_validate(snapshot_dict)
            flow_graph.restore_from_snapshot(snapshot_data)

            # Update last snapshot hash
            self._last_snapshot_hash = entry.snapshot_hash

            logger.info(f"Redo successful: {entry.description}")
            return UndoRedoResult(
                success=True,
                action_description=entry.description,
            )

        except Exception as e:
            logger.error(f"Redo failed: {e}")
            return UndoRedoResult(
                success=False,
                error_message=str(e),
            )

        finally:
            self._is_restoring = False

    def get_state(self) -> HistoryState:
        """Get the current state of the history system.

        Returns:
            HistoryState with information about available undo/redo operations.
        """
        can_undo = len(self._undo_stack) > 0
        can_redo = len(self._redo_stack) > 0

        undo_description = None
        if can_undo:
            undo_description = self._undo_stack[-1].description

        redo_description = None
        if can_redo:
            redo_description = self._redo_stack[-1].description

        return HistoryState(
            can_undo=can_undo,
            can_redo=can_redo,
            undo_description=undo_description,
            redo_description=redo_description,
            undo_count=len(self._undo_stack),
            redo_count=len(self._redo_stack),
        )

    def clear(self) -> None:
        """Clear all history entries."""
        self._undo_stack.clear()
        self._redo_stack.clear()
        self._last_snapshot_hash = None
        logger.debug("History cleared")

    def is_restoring(self) -> bool:
        """Check if a restore operation is currently in progress.

        Returns:
            True if undo/redo is in progress.
        """
        return self._is_restoring

    def get_memory_usage(self) -> dict:
        """Get memory usage statistics for the history stacks.

        Returns:
            Dictionary with memory usage information.
        """
        undo_size = sum(e.compressed_size for e in self._undo_stack)
        redo_size = sum(e.compressed_size for e in self._redo_stack)

        return {
            "undo_stack_entries": len(self._undo_stack),
            "redo_stack_entries": len(self._redo_stack),
            "undo_stack_bytes": undo_size,
            "redo_stack_bytes": redo_size,
            "total_bytes": undo_size + redo_size,
        }
