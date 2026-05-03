"""``GraphDiff`` staging via ``HistoryManager`` — W41.

Composes the per-op ``staged_node_payload`` shapes that W31's executor emits
under ``mode="stage"`` (see ``executor.py:391-647``) into a single
:class:`GraphDiff` artefact, and applies / rejects them atomically.

Atomicity rests on two existing seams:

1. ``flow.capture_history_snapshot(HistoryActionType.BATCH, ...)`` taken
   *before* any mutation gives the user a single undo point covering the
   whole batch.
2. ``flow.flow_settings.track_history = False`` short-circuits the
   per-method ``with_history_capture`` decorator at ``flow_graph.py:175`` so
   the ``add_<node_type>`` calls fired during the batch don't push their
   own snapshots.

If any op raises mid-batch, ``flow.undo()`` rolls the BATCH snapshot back
and the diff is left in the :data:`_DIFFS` store for the user to fix-and-
retry or explicitly reject. D006's drift detection runs *before* the
snapshot so a drift'd diff doesn't pollute the undo stack.

The store is process-local. W42 swaps ``_DIFFS`` for a disk-backed
repository under ``{user_dir}/ai_sessions/{flow_id}/`` without changing the
``register_diff`` / ``get_diff`` / ``pop_diff`` surface this module
exposes.
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

from flowfile_core.ai.tools.executor import InsertionContext, _apply_add_node
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import HistoryActionType
from flowfile_core.schemas.schemas import get_settings_class_for_node_type

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Per-op staged payload shapes                                                 #
# --------------------------------------------------------------------------- #


class StagedInsertionContext(BaseModel):
    """Where a staged addition attaches to the upstream graph.

    Mirrors :class:`flowfile_core.ai.tools.executor.InsertionContext`
    field-for-field so :func:`apply_diff` can reconstitute the executor's
    type via ``InsertionContext(**staged.model_dump())`` without an import
    cycle (the executor would otherwise need to import ``diff`` for typing).
    """

    upstream_node_ids: list[int] = Field(default_factory=list)
    right_input_node_id: int | None = None
    pos_x: float = 0.0
    pos_y: float = 0.0


class StagedSchemaColumn(BaseModel):
    """One predicted output column for a staged addition."""

    name: str
    data_type: str | None = None
    nullable: bool | None = None


class StagedAddition(BaseModel):
    """A ``flowfile.graph.add_<node_type>`` op pending in a :class:`GraphDiff`."""

    node_type: str
    settings: dict[str, Any]
    insertion_context: StagedInsertionContext
    predicted_output_schema: list[StagedSchemaColumn] | None = None
    audit_id: int | None = None


class StagedConnection(BaseModel):
    """A ``flowfile.graph.connect`` or ``delete_connection`` op pending in a :class:`GraphDiff`."""

    connection: dict[str, Any]
    audit_id: int | None = None


class StagedDeletion(BaseModel):
    """A ``flowfile.graph.delete_node`` op pending in a :class:`GraphDiff`."""

    delete_node_id: int
    audit_id: int | None = None


# --------------------------------------------------------------------------- #
# GraphDiff bundle                                                             #
# --------------------------------------------------------------------------- #


class GraphDiff(BaseModel):
    """A bundle of staged graph mutations awaiting user accept / reject.

    Op order is preserved within and across buckets. :func:`apply_diff`
    walks ``additions`` → ``connections_added`` → ``deletions`` →
    ``connections_removed`` so connections to a freshly-added node land
    after the node itself, and deletions land after any new wiring that
    referenced the about-to-be-removed node.
    """

    diff_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    session_id: str
    flow_id: int
    additions: list[StagedAddition] = Field(default_factory=list)
    connections_added: list[StagedConnection] = Field(default_factory=list)
    deletions: list[StagedDeletion] = Field(default_factory=list)
    connections_removed: list[StagedConnection] = Field(default_factory=list)
    rationale: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ApplyResult(BaseModel):
    """Outcome of a successful :func:`apply_diff`."""

    diff_id: str
    applied_node_ids: list[int]
    applied_connection_count: int
    removed_node_ids: list[int]
    removed_connection_count: int
    history_action: str = "batch"
    audit_ids: list[int] = Field(default_factory=list)


# --------------------------------------------------------------------------- #
# Errors                                                                       #
# --------------------------------------------------------------------------- #


class DiffDriftError(Exception):
    """Raised when a staged diff references nodes that no longer exist.

    D006's snapshot+warn-and-pause shape: the user mutated the canvas
    between staging and accept; the diff cannot apply cleanly. The route
    layer maps this to ``409 Conflict`` and leaves the diff in the store
    so the user can fix the underlying graph and retry.
    """

    def __init__(self, missing_node_ids: list[int]) -> None:
        self.missing_node_ids: list[int] = list(missing_node_ids)
        super().__init__(f"diff references missing node ids: {self.missing_node_ids}")


# --------------------------------------------------------------------------- #
# DiffStore — in-memory, process-local. W42 swaps for disk.                    #
# --------------------------------------------------------------------------- #


_DIFFS: dict[str, GraphDiff] = {}
_LOCK = threading.Lock()


def register_diff(diff: GraphDiff) -> str:
    """Store ``diff`` under its ``diff_id``; return the id.

    Idempotent on the same ``diff_id`` — re-registering overwrites. Callers
    that need atomic-upsert semantics can use ``get_diff`` first.
    """
    with _LOCK:
        _DIFFS[diff.diff_id] = diff
    return diff.diff_id


def get_diff(diff_id: str) -> GraphDiff | None:
    """Look up a previously-registered diff. ``None`` if absent."""
    with _LOCK:
        return _DIFFS.get(diff_id)


def pop_diff(diff_id: str) -> GraphDiff | None:
    """Remove and return a diff. Idempotent — ``None`` if already popped."""
    with _LOCK:
        return _DIFFS.pop(diff_id, None)


def clear_for_tests() -> None:
    """Wipe the in-memory store. Intended for the ``conftest`` autouse fixture."""
    with _LOCK:
        _DIFFS.clear()


# --------------------------------------------------------------------------- #
# Audit-id walker                                                              #
# --------------------------------------------------------------------------- #


def collect_audit_ids(diff: GraphDiff) -> list[int]:
    """Walk every op in op-order, returning the ``audit_id`` for those that have one.

    Order matches :func:`apply_diff` traversal: additions, connections_added,
    deletions, connections_removed. ``None`` values are skipped — the audit
    table only flips rows that actually exist.
    """
    out: list[int] = []
    for add in diff.additions:
        if add.audit_id is not None:
            out.append(add.audit_id)
    for c in diff.connections_added:
        if c.audit_id is not None:
            out.append(c.audit_id)
    for d in diff.deletions:
        if d.audit_id is not None:
            out.append(d.audit_id)
    for c in diff.connections_removed:
        if c.audit_id is not None:
            out.append(c.audit_id)
    return out


# --------------------------------------------------------------------------- #
# D006 drift detection                                                         #
# --------------------------------------------------------------------------- #


def validate_diff_against_flow(flow, diff: GraphDiff) -> None:
    """Raise :class:`DiffDriftError` if any referenced node id is missing.

    Checks:

    * Each addition's ``insertion_context.upstream_node_ids`` and optional
      ``right_input_node_id``.
    * Each deletion's ``delete_node_id``.

    A node id counts as available if it either resolves on the live graph
    *now* OR appears earlier in this diff's ``additions`` bucket — chained
    additions (filter then second filter on top of the first) are a normal
    pattern and don't constitute drift. Connections (added or removed) are
    NOT pre-checked — ``add_connection`` enforces its own cycle / unknown-
    id guards inside :func:`apply_diff`, and any raise rolls back the
    BATCH snapshot. Pre-checking would duplicate that logic.
    """
    missing: set[int] = set()
    pending_additions: set[int] = set()

    def _is_available(node_id: int) -> bool:
        if node_id in pending_additions:
            return True
        return flow.get_node(node_id) is not None

    for add in diff.additions:
        for upstream_id in add.insertion_context.upstream_node_ids:
            if not _is_available(upstream_id):
                missing.add(upstream_id)
        right = add.insertion_context.right_input_node_id
        if right is not None and not _is_available(right):
            missing.add(right)
        try:
            new_id = int(add.settings.get("node_id"))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            new_id = None
        if new_id is not None:
            pending_additions.add(new_id)
    for d in diff.deletions:
        if not _is_available(d.delete_node_id):
            missing.add(d.delete_node_id)
    if missing:
        raise DiffDriftError(sorted(missing))


# --------------------------------------------------------------------------- #
# Apply orchestrator                                                           #
# --------------------------------------------------------------------------- #


def apply_diff(flow, diff: GraphDiff) -> ApplyResult:
    """Atomically apply every op in ``diff`` to ``flow`` under one history snapshot.

    On success, the undo stack grows by exactly **1** entry — the
    :data:`HistoryActionType.BATCH` snapshot taken before the batch — so
    the user undoes the whole AI suggestion in one click.

    On failure, ``flow.undo()`` rolls back the snapshot and the graph is
    left indistinguishable from its pre-apply state. The diff itself stays
    in the :data:`_DIFFS` store; the route layer decides whether to surface
    the error as 422 (mid-batch raise), 409 (drift detected), or other.
    """
    validate_diff_against_flow(flow, diff)

    op_count = len(diff.additions) + len(diff.connections_added) + len(diff.deletions) + len(diff.connections_removed)
    description = f"AI diff: {diff.rationale}" if diff.rationale else f"AI diff ({op_count} ops)"
    flow.capture_history_snapshot(HistoryActionType.BATCH, description)

    prior_track = flow.flow_settings.track_history
    flow.flow_settings.track_history = False

    applied_node_ids: list[int] = []
    applied_connection_count = 0
    removed_node_ids: list[int] = []
    removed_connection_count = 0

    try:
        # Lazy import: ``flow_graph`` pulls in heavy machinery that other
        # AI modules (executor, audit, providers) deliberately keep out of
        # their import-time graph; this ``diff`` module imports lightly so
        # tests can stub ``flow_graph`` symbols where needed.
        from flowfile_core.flowfile.flow_graph import add_connection, delete_connection

        for add in diff.additions:
            ctx = InsertionContext(**add.insertion_context.model_dump())
            settings_cls = get_settings_class_for_node_type(add.node_type)
            if settings_cls is None:
                raise ValueError(f"unknown node type: {add.node_type!r}")
            settings = settings_cls.model_validate(add.settings)
            _apply_add_node(flow, add.node_type, settings, ctx)
            applied_node_ids.append(int(settings.node_id))

        for c in diff.connections_added:
            connection = input_schema.NodeConnection.model_validate(c.connection)
            add_connection(flow, connection)
            applied_connection_count += 1

        for d in diff.deletions:
            flow.delete_node(d.delete_node_id)
            removed_node_ids.append(d.delete_node_id)

        for c in diff.connections_removed:
            connection = input_schema.NodeConnection.model_validate(c.connection)
            delete_connection(flow, connection)
            removed_connection_count += 1
    except Exception:
        try:
            flow.undo()
        except Exception as undo_exc:
            logger.error("rollback after partial apply failed for diff %s: %s", diff.diff_id, undo_exc)
        raise
    finally:
        flow.flow_settings.track_history = prior_track

    return ApplyResult(
        diff_id=diff.diff_id,
        applied_node_ids=applied_node_ids,
        applied_connection_count=applied_connection_count,
        removed_node_ids=removed_node_ids,
        removed_connection_count=removed_connection_count,
        audit_ids=collect_audit_ids(diff),
    )


__all__ = [
    "ApplyResult",
    "DiffDriftError",
    "GraphDiff",
    "StagedAddition",
    "StagedConnection",
    "StagedDeletion",
    "StagedInsertionContext",
    "StagedSchemaColumn",
    "apply_diff",
    "clear_for_tests",
    "collect_audit_ids",
    "get_diff",
    "pop_diff",
    "register_diff",
    "validate_diff_against_flow",
]
