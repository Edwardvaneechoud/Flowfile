"""Edit history of a FlowGraph: the per-flow edit lock, the one-step-per-mutation transaction,
undo/redo, revision counting and snapshot restore (through the single graph builder in
manage.io_flowfile).
"""

import asyncio
import functools
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, NamedTuple

from fastapi.exceptions import HTTPException

from flowfile_core.configs import logger
from flowfile_core.events import publish
from flowfile_core.flowfile.flow_graph._base import GraphMixinBase
from flowfile_core.flowfile.flow_graph._root import root
from flowfile_core.schemas import schemas
from flowfile_core.schemas.history_schema import (
    HistoryActionType,
    HistorySnapshot,
    HistoryState,
    UndoRedoResult,
    in_scope_hash,
)

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph.graph import FlowGraph


# How long a mutation waits for another in-flight mutation of the same flow before giving up.
EDIT_LOCK_TIMEOUT_SECONDS = 30.0


placement_check: ContextVar[Callable[[Any], None] | None] = ContextVar("placement_check", default=None)
"""A check every decorated ``add_*`` runs on its settings before placing anything, in the context that set it.

Notebook build mode sets it so a cell's source or writer is refused on its path and connection rules
before a node exists; canvas requests and other threads never see it.
"""


def with_history_capture(action_type: "HistoryActionType", description_template: str = "Update {node_type} settings"):
    """Decorator that runs a FlowGraph mutator inside :meth:`FlowGraph.transaction`.

    Standalone calls record one undo step when the graph changed; inside an outer
    transaction (an editor route, an AI batch) or a restore the call records nothing
    itself. With ``flow_settings.track_history`` off the method runs as a plain call.
    A :data:`placement_check` set in the calling context runs first, whatever the history setting.

    Args:
        action_type: The type of history action (e.g., HistoryActionType.UPDATE_SETTINGS).
        description_template: Template string for the history description.
            Can use {node_type} placeholder which will be replaced with the actual node type.

    Example:
        @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
        def add_filter(self, filter_settings: input_schema.NodeFilter):
            # ... implementation
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self: "FlowGraph", *args, **kwargs):
            settings_input = args[0] if args else next(iter(kwargs.values()), None)
            check = placement_check.get()
            if check is not None:
                check(settings_input)

            # Remember the session owner so restore_from_snapshot can re-stamp
            # user_id even when the live graph holds no nodes.
            owner_uid = getattr(settings_input, "user_id", None) if settings_input else None
            if owner_uid is not None:
                self._owner_user_id = owner_uid

            if not self.flow_settings.track_history:
                return func(self, *args, **kwargs)

            node_id = getattr(settings_input, "node_id", None) if settings_input else None
            node_type = (
                getattr(settings_input, "node_type", func.__name__.replace("add_", ""))
                if settings_input
                else func.__name__.replace("add_", "")
            )
            with self.transaction(description_template.format(node_type=node_type), action_type, node_id=node_id):
                return func(self, *args, **kwargs)

        return wrapper

    return decorator


class GraphTransaction:
    """Handle yielded by :meth:`FlowGraph.transaction`.

    ``description`` may be refined inside the block (e.g. once the affected node is
    known); ``entry`` holds the recorded history entry after a successful outermost
    transaction that changed the graph, else None. ``history`` is the history state
    taken while the lock was still held, so a response never reports another writer's
    state.
    """

    __slots__ = ("description", "entry", "history")

    def __init__(self, description: str):
        self.description = description
        self.entry = None
        self.history: HistoryState | None = None


def _warn_if_on_event_loop() -> None:
    """Waiting for a flow's edit lock on the event loop would stall every request (or deadlock)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    message = "FlowGraph edit waited for the edit lock on an asyncio event-loop thread; use asyncio.to_thread"
    if os.environ.get("TESTING") == "True":
        raise RuntimeError(message)
    logger.error(message)


class _NodeOwners(NamedTuple):
    """Which user each node belonged to before a snapshot restore.

    History snapshots deliberately omit ``user_id`` so on-disk flows stay portable across
    users, so a replay has to re-stamp it or connection-backed nodes come back with
    ``user_id=None`` and fail to resolve their owner's connection at run time. Nodes the
    snapshot reintroduces — and a graph whose nodes were all deleted before the undo —
    fall back to the session owner, which every node in a session shares.
    """

    by_node_id: dict[int, int]
    session_owner: int | None

    @classmethod
    def capture(cls, graph: "FlowGraph") -> "_NodeOwners":
        by_node_id = {
            node.node_id: uid
            for node in graph.nodes
            if (uid := getattr(node.setting_input, "user_id", None)) is not None
        }
        session_owner = next(iter(by_node_id.values()), None)
        if session_owner is None:
            session_owner = graph._owner_user_id
        return cls(by_node_id=by_node_id, session_owner=session_owner)

    def owner_of(self, node_id: int) -> int | None:
        return self.by_node_id.get(node_id, self.session_owner)


class GraphHistoryMixin(GraphMixinBase):
    @contextmanager
    def edit_lock(self, bounded: bool = True) -> Iterator[None]:
        """Hold the per-flow edit lock; every writer takes it through here.

        A bounded wait answers 409 after ``EDIT_LOCK_TIMEOUT_SECONDS``; ``bounded=False`` waits for
        the in-flight edit instead (a save or run claim must not fail on a busy flow). A contended
        wait on an event-loop thread is logged as an error (raised under TESTING): a to_thread
        worker holding the lock may itself need the loop.
        """
        if not self._edit_lock.acquire(blocking=False):
            _warn_if_on_event_loop()
            timeout = root().EDIT_LOCK_TIMEOUT_SECONDS if bounded else -1
            if not self._edit_lock.acquire(timeout=timeout):
                raise HTTPException(409, "Flow is busy")
        try:
            yield
        finally:
            self._edit_lock.release()

    @contextmanager
    def rebuilding(self) -> Iterator[None]:
        """Hold the edit lock and record nothing while the graph is rebuilt (open, undo/redo, rollback)."""
        with self.edit_lock(), self._history_manager.restoring():
            yield

    @contextmanager
    def transaction(
        self,
        description: str,
        action_type: HistoryActionType = HistoryActionType.BATCH,
        node_id: int | None = None,
        record: bool = True,
    ) -> Iterator[GraphTransaction]:
        """Run one graph mutation atomically under the edit lock and record at most one undo step.

        The outermost transaction snapshots the graph before and after the block. A
        successful one records the pre-block state as one undo entry iff the in-scope graph
        changed (and only then clears redo), provided ``record`` and history tracking are on.
        The dirty flag is refreshed either way, and ``txn.history`` is filled while the lock
        is still held; the revision moves last, and only when the persisted state changed. A
        failure anywhere in that sequence (the block, the post-snapshot, recording, the dirty
        refresh or the history read) restores the pre-block graph,
        discards any entry it already recorded and re-raises, so a failed request changes
        nothing (except that a failure after recording cannot bring back the redo that the
        recording cleared). Nested transactions and calls during a restore run the block as-is.
        """
        txn = GraphTransaction(description)
        with self.edit_lock():
            if self._transaction_depth > 0 or self._history_manager.is_restoring():
                self._transaction_depth += 1
                try:
                    yield txn
                finally:
                    self._transaction_depth -= 1
                txn.history = self.get_history_state()
                return

            try:
                pre = HistorySnapshot(self.get_flowfile_data().model_dump())
            except Exception:
                # An unserializable graph must stay editable; this edit just can't be undone or rolled back.
                logger.exception(f"Flow {self.flow_id}: could not snapshot the graph before '{description}'")
                pre = None
            self._transaction_depth = 1
            try:
                yield txn
                post = None
                if pre is not None:
                    post = HistorySnapshot(self.get_flowfile_data().model_dump(), keep_payload=False)
                if post is None:
                    self._history_manager.refresh_dirty_from(self)
                else:
                    if record and self.flow_settings.track_history:
                        txn.entry = self._history_manager.record(pre, post, action_type, txn.description, node_id)
                    self._history_manager.refresh_dirty(post)
                txn.history = self.get_history_state()
                # Last, so a failure anywhere above leaves the revision where it was; so does a no-op block.
                if post is None or post.persisted_hash != pre.persisted_hash:
                    txn.history.revision = self._bump_revision("graph")
            except BaseException:
                if txn.entry is not None:
                    self._history_manager.discard_if_top(txn.entry)
                    txn.entry = None
                if pre is not None:
                    self._rollback_to(pre)
                raise
            finally:
                self._transaction_depth = 0

    def _rollback_to(self, snapshot: HistorySnapshot) -> None:
        """Restore ``snapshot`` without touching the history stacks, if the graph changed."""
        try:
            changed = in_scope_hash(self.get_flowfile_data().model_dump()) != snapshot.graph_hash
        except Exception:
            changed = True
        if not changed:
            return
        try:
            self.restore_from_snapshot(schemas.FlowfileData.model_validate(snapshot.load()))
        except Exception:
            logger.exception(f"Flow {self.flow_id}: rollback after a failed edit could not restore the graph")
        self._history_manager.refresh_dirty_from(self)

    def _note_graph_write(self) -> None:
        """Count primitive graph writes that bypass the transaction (history on, none active).

        Such a write is also a change of its own: outside a transaction nothing else advances
        the revision for it. Inside one, the transaction's exit does.
        """
        if self._transaction_depth > 0 or self._history_manager.is_restoring():
            return
        if self.flow_settings.track_history:
            self._untracked_writes += 1
        self._bump_revision("graph")

    @property
    def revision(self) -> int:
        """Monotonic change counter: every mutation, undo/redo, run start/end and save moves it.

        Clients compare it to learn that the flow changed under them; the change feed streams
        each move as a ``flow_revision`` event with its ``kind``.
        """
        return self._revision

    def _bump_revision(self, kind: str) -> int:
        with self._revision_lock:
            self._revision += 1
            revision = self._revision
        publish("flow_revision", graph=self, revision=revision, kind=kind)
        return revision

    def capture_history_snapshot(
        self,
        action_type: HistoryActionType,
        description: str,
        node_id: int = None,
    ) -> bool:
        """Capture the current state before a change for undo support.

        Legacy explicit API kept for tests; editor routes and in-process mutators use :meth:`transaction`.

        Args:
            action_type: The type of action being performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            True if snapshot was captured, False if skipped.
        """
        with self.edit_lock():
            return self._history_manager.capture_snapshot(self, action_type, description, node_id)

    def capture_history_if_changed(
        self,
        pre_snapshot: schemas.FlowfileData,
        action_type: HistoryActionType,
        description: str,
        node_id: int = None,
    ) -> bool:
        """Capture history only if the flow state actually changed.

        Legacy explicit API kept for tests: call AFTER the change is applied.

        Args:
            pre_snapshot: The FlowfileData captured BEFORE the change.
            action_type: The type of action that was performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            True if a change was detected and snapshot was captured.
        """
        with self.edit_lock():
            return self._history_manager.capture_if_changed(self, pre_snapshot, action_type, description, node_id)

    def undo(self) -> UndoRedoResult:
        """Undo the last action by restoring to the previous state.

        Returns:
            UndoRedoResult indicating success or failure, carrying the resulting history state.
        """
        with self.edit_lock():
            result = self._history_manager.undo(self)
            if result.success:
                self._bump_revision("graph")
            result.history = self.get_history_state()
            return result

    def redo(self) -> UndoRedoResult:
        """Redo the last undone action.

        Returns:
            UndoRedoResult indicating success or failure, carrying the resulting history state.
        """
        with self.edit_lock():
            result = self._history_manager.redo(self)
            if result.success:
                self._bump_revision("graph")
            result.history = self.get_history_state()
            return result

    def revert_if_top(self, entry) -> bool:
        """Silently retract a just-recorded step if nothing was recorded after it (redo untouched)."""
        if entry is None:
            return False
        with self.edit_lock():
            reverted = self._history_manager.revert_if_top(self, entry)
            if reverted:
                self._bump_revision("graph")
            return reverted

    def clear_history(self) -> None:
        """Drop every undo/redo entry (the save point is kept)."""
        with self.edit_lock():
            self._history_manager.clear()

    def get_history_state(self) -> HistoryState:
        """Get the current state of the history system.

        Returns:
            HistoryState with information about available undo/redo operations.
        """
        state = self._history_manager.get_state()
        state.flow_id = self.flow_id
        state.revision = self.revision
        return state

    def mark_as_saved(self) -> None:
        """Mark the current flow state as the saved baseline (for dirty tracking)."""
        with self.edit_lock():
            self._history_manager.mark_saved(self)
        self._bump_revision("saved")

    def has_unsaved_changes(self) -> bool:
        """Return True if the flow has changed since the last save point."""
        return self._history_manager.has_unsaved_changes(self)

    def _execute_with_history(
        self,
        operation: Callable[[], Any],
        action_type: HistoryActionType,
        description: str,
        node_id: int = None,
    ) -> Any:
        """Run ``operation`` inside :meth:`transaction` (a plain call when tracking is off).

        Args:
            operation: A callable that performs the actual operation.
            action_type: The type of action being performed.
            description: Human-readable description of the action.
            node_id: Optional ID of the affected node.

        Returns:
            The result of the operation (if any).
        """
        if not self.flow_settings.track_history:
            return operation()
        with self.transaction(description, action_type, node_id=node_id):
            return operation()

    def restore_from_snapshot(self, snapshot: schemas.FlowfileData) -> None:
        """Clear the graph and rebuild it from a snapshot, recording nothing.

        Used by undo/redo and transaction rollback. The live flow settings and identity
        are kept as-is (they are outside the undo scope); node owners are re-stamped
        because snapshots deliberately omit ``user_id``.

        Args:
            snapshot: The FlowfileData snapshot to restore from.
        """
        from flowfile_core.flowfile.manage.io_flowfile import (
            _flowfile_data_to_flow_information,
            populate_graph_from_flow_information,
        )

        with self.rebuilding():
            node_owners = _NodeOwners.capture(self)
            flow_info = _flowfile_data_to_flow_information(snapshot)

            # Ids the restore drops (an undone placement) stay below the ceiling, like a deletion.
            self._node_id_seq = self.node_id_ceiling
            self._node_db.clear()
            self._node_ids.clear()
            self._flow_starts.clear()
            self._groups.clear()
            self._comments.clear()
            self._results = None
            # Rebuilt nodes restart at _cache_epoch 0; a stale liveness signature would skip the union invalidation.
            self._any_input_liveness.clear()

            populate_graph_from_flow_information(self, flow_info, owner_of=node_owners.owner_of)

        logger.info(f"Restored flow from snapshot with {len(self._node_db)} nodes")
