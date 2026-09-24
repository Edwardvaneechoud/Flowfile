"""
History Manager for undo/redo functionality in flow graphs.

This module provides the HistoryManager class which manages undo/redo stacks
and enables users to revert or reapply changes to their flow graphs.

Recording contract: a mutation records via :meth:`HistoryManager.record` with the
snapshots taken before and after it (``FlowGraph.transaction`` does this), so one
change is one entry and a no-op records nothing. Undo/redo move entries between
the stacks only after the restore succeeded. The dirty flag is always
``persisted_hash(current) != persisted_hash(saved)``. Every snapshot is captured as a
:class:`HistorySnapshot`, so it is serialized, pickled and hashed once.

Optimizations:
- Compressed snapshots using zlib (60-80% memory reduction)
- Pre-computed hashes for O(1) snapshot comparison
- __slots__ for memory-efficient entry storage
"""

from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from time import time
from typing import TYPE_CHECKING

from flowfile_core.configs import logger
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
    HistoryConfig,
    HistoryEntry,
    HistorySnapshot,
    HistoryState,
    UndoRedoResult,
)
from flowfile_core.schemas.schemas import FlowfileData

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph


class HistoryManager:
    """Manages undo/redo history for a FlowGraph.

    Uses two deques (undo_stack and redo_stack) to track state changes.
    Each entry holds the state BEFORE its change, so undo restores to that state.

    Memory Optimization:
    - Snapshots are compressed using zlib (typically 60-80% size reduction)
    - Hashes are pre-computed for O(1) equality checks
    - HistoryEntry uses __slots__ for reduced memory overhead
    """

    __slots__ = (
        "_config",
        "_undo_stack",
        "_redo_stack",
        "_is_restoring",
        "_saved_snapshot_hash",
        "_dirty",
    )

    def __init__(self, config: HistoryConfig | None = None):
        """Initialize the HistoryManager.

        Args:
            config: Optional configuration for history behavior.
        """
        self._config = config or HistoryConfig()
        self._undo_stack: deque[HistoryEntry] = deque(maxlen=self._config.max_stack_size)
        self._redo_stack: deque[HistoryEntry] = deque(maxlen=self._config.max_stack_size)
        self._is_restoring: bool = False
        # persisted_hash of the flow state at the last save point
        self._saved_snapshot_hash: int | None = None
        # Maintained at the end of every mutation/undo/redo/save so reads stay cheap; None = recompute on read
        self._dirty: bool | None = False

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
        snapshot: HistorySnapshot,
        action_type: HistoryActionType,
        description: str,
        node_id: int | None = None,
    ) -> HistoryEntry:
        """Create a history entry holding ``snapshot`` with the configured compression settings.

        Args:
            snapshot: The captured flow state.
            action_type: The type of action.
            description: Human-readable description.
            node_id: Optional affected node ID.

        Returns:
            A new HistoryEntry instance.
        """
        return HistoryEntry.from_snapshot(
            snapshot,
            action_type=action_type,
            description=description,
            timestamp=time(),
            node_id=node_id,
            compression_level=self._config.compression_level if self._config.use_compression else 1,
        )

    @contextmanager
    def restoring(self) -> Iterator[None]:
        """Suppress recording while the graph is rebuilt (entered via ``FlowGraph.rebuilding``)."""
        previous = self._is_restoring
        self._is_restoring = True
        try:
            yield
        finally:
            self._is_restoring = previous

    def record(
        self,
        pre_snapshot: HistorySnapshot,
        post_snapshot: HistorySnapshot,
        action_type: HistoryActionType,
        description: str,
        node_id: int | None = None,
    ) -> HistoryEntry | None:
        """Push ``pre_snapshot`` as one undo entry iff the in-scope graph changed.

        Only a recorded change clears the redo stack. Returns the pushed entry, or None
        when nothing was recorded (disabled, restoring, or no in-scope change).
        """
        if not self._config.enabled or self._is_restoring:
            return None
        if pre_snapshot.graph_hash == post_snapshot.graph_hash:
            return None
        entry = self._create_entry(pre_snapshot, action_type, description, node_id)
        self._undo_stack.append(entry)
        self._redo_stack.clear()
        logger.info(
            f"History: recorded '{description}' "
            f"(undo_stack={len(self._undo_stack)}, redo_stack={len(self._redo_stack)})"
        )
        return entry

    def capture_snapshot(
        self,
        flow_graph: "FlowGraph",
        action_type: HistoryActionType,
        description: str,
        node_id: int | None = None,
    ) -> bool:
        """Capture the current state of the flow graph BEFORE a change.

        Legacy explicit API kept for tests; everything else records through
        ``FlowGraph.transaction``. Duplicates are detected against the last captured
        snapshot (top of the undo stack). The change that follows happens outside any
        transaction, so the dirty flag is left to be recomputed on its next read.

        Args:
            flow_graph: The FlowGraph to capture.
            action_type: The type of action being performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            True if snapshot was captured, False if skipped (disabled or restoring).
        """
        if not self._config.enabled or self._is_restoring:
            return False
        self._dirty = None

        try:
            snapshot = HistorySnapshot(flow_graph.get_flowfile_data().model_dump())
            if self._undo_stack and self._undo_stack[-1].snapshot_hash == snapshot.graph_hash:
                logger.info(f"History: Skipping duplicate snapshot for: {description}")
                return False

            self._undo_stack.append(self._create_entry(snapshot, action_type, description, node_id))
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
        node_id: int | None = None,
    ) -> bool:
        """Capture history only if the flow state actually changed.

        Legacy explicit API kept for tests: call AFTER the change with the snapshot taken before it.

        Args:
            flow_graph: The FlowGraph after the change.
            pre_snapshot: The FlowfileData captured BEFORE the change.
            action_type: The type of action that was performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            True if a change was detected and snapshot was captured.
        """
        try:
            current = HistorySnapshot(flow_graph.get_flowfile_data().model_dump(), keep_payload=False)
            pre = HistorySnapshot(pre_snapshot.model_dump())
            recorded = self.record(pre, current, action_type, description, node_id)
            self.refresh_dirty(current)
            return recorded is not None
        except Exception as e:
            logger.error(f"History: Failed to capture snapshot for '{description}': {e}")
            return False

    def _step(self, flow_graph: "FlowGraph", source: deque, target: deque, verb: str) -> UndoRedoResult:
        """Restore the top of ``source``; move entries only once the restore succeeded."""
        if not source:
            return UndoRedoResult(success=False, error_message=f"Nothing to {verb}")

        entry = source[-1]
        try:
            replaced = self._restore_entry(flow_graph, entry, verb)
        except Exception as e:
            logger.error(f"{verb.capitalize()} failed: {e}")
            return UndoRedoResult(success=False, error_message=str(e))

        source.pop()
        target.append(self._create_entry(replaced, entry.action_type, entry.description, entry.node_id))
        self.refresh_dirty_from(flow_graph)
        logger.info(f"{verb.capitalize()} successful: {entry.description}")
        return UndoRedoResult(success=True, action_description=entry.description)

    def _restore_entry(self, flow_graph: "FlowGraph", entry: HistoryEntry, verb: str) -> HistorySnapshot:
        """Restore ``entry``'s graph and return the graph it replaced; a failed restore is put back, then re-raised."""
        snapshot_data = FlowfileData.model_validate(entry.get_snapshot())
        replaced = HistorySnapshot(flow_graph.get_flowfile_data().model_dump())
        try:
            flow_graph.restore_from_snapshot(snapshot_data)
        except Exception:
            try:
                flow_graph.restore_from_snapshot(FlowfileData.model_validate(replaced.load()))
            except Exception as reinstate_error:
                logger.error(f"History: {verb} could not reinstate the previous graph: {reinstate_error}")
            raise
        return replaced

    def undo(self, flow_graph: "FlowGraph") -> UndoRedoResult:
        """Undo the last action by restoring to the previous state.

        Args:
            flow_graph: The FlowGraph to restore.

        Returns:
            UndoRedoResult indicating success or failure.
        """
        return self._step(flow_graph, self._undo_stack, self._redo_stack, "undo")

    def redo(self, flow_graph: "FlowGraph") -> UndoRedoResult:
        """Redo the last undone action.

        Args:
            flow_graph: The FlowGraph to restore.

        Returns:
            UndoRedoResult indicating success or failure.
        """
        return self._step(flow_graph, self._redo_stack, self._undo_stack, "redo")

    def revert_if_top(self, flow_graph: "FlowGraph", entry: HistoryEntry) -> bool:
        """Silently undo ``entry`` when it is still the latest step; redo is left untouched.

        Used to retract a just-recorded step (e.g. an AI step whose observation failed)
        without offering it as a redo. Returns False when another step came after it.
        """
        if not self._undo_stack or self._undo_stack[-1] is not entry:
            return False
        try:
            self._restore_entry(flow_graph, entry, "retract")
        except Exception as e:
            logger.error(f"History: could not retract '{entry.description}': {e}")
            return False
        self._undo_stack.pop()
        self.refresh_dirty_from(flow_graph)
        return True

    def get_state(self) -> HistoryState:
        """Get the current state of the history system.

        Safe to call without the flow's edit lock: a concurrent push/pop can make the
        counts and descriptions momentarily disagree, but never raises.

        Returns:
            HistoryState with information about available undo/redo operations.
        """
        undo_top = self._peek(self._undo_stack)
        redo_top = self._peek(self._redo_stack)
        return HistoryState(
            can_undo=undo_top is not None,
            can_redo=redo_top is not None,
            undo_description=undo_top.description if undo_top is not None else None,
            redo_description=redo_top.description if redo_top is not None else None,
            undo_count=len(self._undo_stack),
            redo_count=len(self._redo_stack),
        )

    @staticmethod
    def _peek(stack: deque) -> HistoryEntry | None:
        try:
            return stack[-1]
        except IndexError:
            return None

    def clear(self) -> None:
        """Clear all history entries.

        Note: ``_saved_snapshot_hash`` is intentionally preserved here — the
        save point persists across history clears.
        """
        self._undo_stack.clear()
        self._redo_stack.clear()
        logger.debug("History cleared")

    def refresh_dirty(self, snapshot: HistorySnapshot) -> None:
        """Recompute the dirty flag from a capture of the current flow state.

        Only read once a save point exists (see :meth:`has_unsaved_changes`).
        """
        self._dirty = snapshot.persisted_hash != self._saved_snapshot_hash

    def refresh_dirty_from(self, flow_graph: "FlowGraph") -> None:
        """Recompute the dirty flag from the live graph; on failure assume dirty."""
        try:
            self.refresh_dirty(HistorySnapshot(flow_graph.get_flowfile_data().model_dump(), keep_payload=False))
        except Exception as e:
            logger.warning(f"History: failed to compute dirty state: {e}")
            self._dirty = True

    def mark_saved(self, flow_graph: "FlowGraph") -> None:
        """Record the current flow state as the saved baseline.

        Called after a successful save to establish the clean reference point
        for dirty tracking.
        """
        try:
            self._saved_snapshot_hash = HistorySnapshot(
                flow_graph.get_flowfile_data().model_dump(), keep_payload=False
            ).persisted_hash
            self._dirty = False
            logger.debug("History: marked current state as saved")
        except Exception as e:
            # On failure, leave _dirty alone — we didn't actually establish a clean save.
            logger.warning(f"History: failed to mark saved state: {e}")

    def has_unsaved_changes(self, flow_graph: "FlowGraph") -> bool:
        """Check whether the current flow state differs from the last save point.

        Hot path: when a save baseline exists, return the maintained ``_dirty`` flag
        without serializing or hashing the graph (recomputed first after a legacy
        :meth:`capture_snapshot`). Before the first ``mark_saved`` establishes a
        baseline, non-empty content counts as dirty.
        """
        try:
            if self._saved_snapshot_hash is not None:
                if self._dirty is None:
                    self.refresh_dirty_from(flow_graph)
                return self._dirty
            # Never saved: fall back to the "non-empty content means dirty" rule
            snapshot_dict = flow_graph.get_flowfile_data().model_dump()
            return len(snapshot_dict.get("nodes", []) or []) > 0
        except Exception as e:
            logger.warning(f"History: failed to compute dirty state: {e}")
            return True  # conservative — keep the "unsaved changes" prompt

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
