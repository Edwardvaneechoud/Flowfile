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

from flowfile_core.ai.diff_store import (
    DiffRepository,
    DiskDiffRepository,
    InMemoryDiffRepository,
)
from flowfile_core.ai.tools.executor import (
    InsertionContext,
    _apply_add_node,
    _apply_update_node_settings,
)
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.history_schema import HistoryActionType
from flowfile_core.schemas.schemas import get_settings_class_for_node_type
from shared.storage_config import storage

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


class StagedSettingsUpdate(BaseModel):
    """A ``flowfile.graph.update_node_settings`` op pending in a :class:`GraphDiff`.

    Modifications target an *existing* node — distinct from
    :class:`StagedAddition` (which creates a new one) and :class:`StagedDeletion`
    (which removes one). Old settings are captured at stage time so the
    diff-preview UI can render a reliable old-vs-new view, and so a mid-batch
    rollback in :func:`apply_diff` can restore the prior value without
    re-deriving from disk (W41's BATCH-snapshot rollback covers the live
    graph; this field exists for the *diff-preview* truth).
    """

    node_id: int
    node_type: str
    old_settings: dict[str, Any] = Field(default_factory=dict)
    new_settings: dict[str, Any] = Field(default_factory=dict)
    predicted_output_schema: list[StagedSchemaColumn] | None = None
    audit_id: int | None = None


# --------------------------------------------------------------------------- #
# GraphDiff bundle                                                             #
# --------------------------------------------------------------------------- #


class GraphDiff(BaseModel):
    """A bundle of staged graph mutations awaiting user accept / reject.

    Op order is preserved within and across buckets. :func:`apply_diff`
    walks ``additions`` → ``modifications`` → ``connections_added`` →
    ``deletions`` → ``connections_removed`` so:

    * connections to a freshly-added node land after the node itself,
    * settings updates for a freshly-added node land after the addition
      and before any wiring referencing the modified node,
    * deletions land after any new wiring that referenced the
      about-to-be-removed node.

    Modifications target an *existing* node id (or an in-batch addition);
    they do not create new node ids. W45 drift detection is NOT influenced
    by modifications — the planner does NOT append to ``staged_node_ids``
    when a modification is staged.
    """

    diff_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    session_id: str
    flow_id: int
    additions: list[StagedAddition] = Field(default_factory=list)
    modifications: list[StagedSettingsUpdate] = Field(default_factory=list)
    connections_added: list[StagedConnection] = Field(default_factory=list)
    deletions: list[StagedDeletion] = Field(default_factory=list)
    connections_removed: list[StagedConnection] = Field(default_factory=list)
    rationale: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ApplyResult(BaseModel):
    """Outcome of a successful :func:`apply_diff`."""

    diff_id: str
    applied_node_ids: list[int]
    modified_node_ids: list[int] = Field(default_factory=list)
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


class DiffInconsistentError(Exception):
    """Raised when a staged diff is internally inconsistent (W70).

    Distinct from :class:`DiffDriftError` — drift is the canvas mutating
    during the agent's work (D006); inconsistency is the agent's *own*
    diff being broken (e.g. a staged ``connect`` whose ``to_node_id``
    isn't in the live graph nor in this diff's additions). The route
    layer maps this to ``422 Unprocessable Entity`` and leaves the diff
    in the store so the user can Reject and ask the agent to retry.

    ``missing_endpoints`` carries ``(node_id, role)`` tuples where role
    is ``"from"`` (output side) or ``"to"`` (input side) — the offending
    end of the staged connection.
    """

    def __init__(self, missing_endpoints: list[tuple[int, str]]) -> None:
        self.missing_endpoints: list[tuple[int, str]] = list(missing_endpoints)
        super().__init__(f"diff has inconsistent connection endpoints: {self.missing_endpoints}")


# --------------------------------------------------------------------------- #
# DiffStore — repository-backed (W42). Public surface unchanged.               #
# --------------------------------------------------------------------------- #


def _build_default_repo() -> DiffRepository:
    """Default to the on-disk sidecar repo (W42), colocated with sessions.

    See ``shared.storage_config.FlowfileStorage.ai_sessions_directory`` for
    the path resolution. Tests swap to in-memory via
    :func:`clear_for_tests`.
    """
    return DiskDiffRepository(root=storage.ai_sessions_directory)


_REPO: DiffRepository = _build_default_repo()
_REPO_LOCK = threading.Lock()


def set_diff_repo(repo: DiffRepository) -> DiffRepository:
    """Swap the active diff repository; return the previous one. Tests-only."""
    global _REPO
    with _REPO_LOCK:
        prev = _REPO
        _REPO = repo
    return prev


def get_diff_repo() -> DiffRepository:
    """Read the active diff repository."""
    with _REPO_LOCK:
        return _REPO


def register_diff(diff: GraphDiff) -> str:
    """Store ``diff`` under its ``diff_id``; return the id.

    Idempotent on the same ``diff_id`` — re-registering overwrites. Callers
    that need atomic-upsert semantics can use ``get_diff`` first.
    """
    get_diff_repo().put(diff)
    return diff.diff_id


def get_diff(diff_id: str) -> GraphDiff | None:
    """Look up a previously-registered diff. ``None`` if absent."""
    return get_diff_repo().get(diff_id)


def pop_diff(diff_id: str) -> GraphDiff | None:
    """Remove and return a diff. Idempotent — ``None`` if already popped."""
    return get_diff_repo().pop(diff_id)


def clear_for_tests() -> None:
    """Swap the active repo for a fresh :class:`InMemoryDiffRepository`.

    Same posture as :func:`flowfile_core.ai.sessions.clear_for_tests` — never
    touch the production disk sidecar from tests.
    """
    set_diff_repo(InMemoryDiffRepository())


# --------------------------------------------------------------------------- #
# Audit-id walker                                                              #
# --------------------------------------------------------------------------- #


_GRAPH_PREFIX = "flowfile.graph."
_ADD_PREFIX = "flowfile.graph.add_"
_CONNECT_NAME = "flowfile.graph.connect"
_DELETE_NODE_NAME = "flowfile.graph.delete_node"
_DELETE_CONNECTION_NAME = "flowfile.graph.delete_connection"
_UPDATE_SETTINGS_NAME = "flowfile.graph.update_node_settings"


class StagedToolEntry(BaseModel):
    """One staged tool call from a prior ``execute_tool_call(mode="stage")``.

    The shape :func:`bundle_staged_results` accepts. Mirrors the subset of
    :class:`flowfile_core.ai.tools.executor.ToolExecutionResult` the diff
    bundler needs: tool name (for dispatch), audit id (for accept/reject
    flip), and the per-op ``staged_node_payload`` shape W31 emits. Lives in
    :mod:`diff` rather than :mod:`diff_routes` so W40's planner can build a
    ``GraphDiff`` directly without crossing the HTTP boundary.
    """

    tool_name: str = Field(min_length=1)
    audit_id: int | None = None
    staged_node_payload: dict[str, Any] = Field(default_factory=dict)


class AppliedNodeRecord(BaseModel):
    """W71 v2.0 — one node applied LIVE during an ``agent_live``
    session. Unlike :class:`StagedToolEntry` these describe nodes
    that are already in ``flow.nodes`` — applied via
    ``execute_tool_call(mode="apply")`` rather than staged. The
    record is kept on ``AgentSession.applied_results`` purely for
    chat-trail rendering, audit, and the auto-undo path
    (``node_id`` is the value passed to ``flow.delete_node`` on
    runtime failure).
    """

    tool_name: str = Field(min_length=1)
    audit_id: int | None = None
    node_id: int
    """The id assigned by the executor / planner — the live node id
    in ``flow.nodes`` after the apply succeeds."""
    node_type: str = Field(min_length=1)
    rationale: str = ""
    output_schema: list[dict[str, Any]] = Field(default_factory=list)
    """The real (Performance-mode runtime) or sample-derived
    (Development-mode) output schema captured in the post-apply
    observation. Same shape as
    ``StagedToolEntry.staged_node_payload['predicted_output_schema']``
    so frontend rendering can branch on the surface alone, not the
    record shape."""


def bundle_staged_results(staged: list[StagedToolEntry]) -> GraphDiff:
    """Sort staged results into the four :class:`GraphDiff` buckets.

    Tool-name prefix is the discriminator. Anything outside the
    ``flowfile.graph.*`` namespace (or unsupported within it) raises
    ``ValueError`` — the route layer maps that to 422; W40 surfaces it as
    a planner-loop error.

    The returned :class:`GraphDiff` has ``session_id=""`` and ``flow_id=0``;
    callers fill those in alongside ``rationale`` before calling
    :func:`register_diff`. This split keeps the binner pure (no caller
    metadata leaking through the dispatch).
    """
    additions: list[StagedAddition] = []
    modifications: list[StagedSettingsUpdate] = []
    connections_added: list[StagedConnection] = []
    deletions: list[StagedDeletion] = []
    connections_removed: list[StagedConnection] = []

    for entry in staged:
        tool_name = entry.tool_name
        payload = entry.staged_node_payload

        if tool_name.startswith(_ADD_PREFIX):
            node_type = tool_name[len(_ADD_PREFIX) :]
            try:
                additions.append(
                    StagedAddition(
                        node_type=payload.get("node_type", node_type),
                        settings=payload.get("settings", {}),
                        insertion_context=StagedInsertionContext(**payload.get("insertion_context", {})),
                        predicted_output_schema=payload.get("predicted_output_schema"),
                        audit_id=entry.audit_id,
                    )
                )
            except Exception as exc:
                raise ValueError(f"invalid add payload for {tool_name!r}: {exc}") from exc
        elif tool_name == _UPDATE_SETTINGS_NAME:
            kind = payload.get("kind")
            if kind != "modification":
                raise ValueError(
                    f"update_node_settings payload missing kind='modification': {payload!r}",
                )
            node_id = payload.get("node_id")
            node_type = payload.get("node_type")
            if not isinstance(node_id, int) or not isinstance(node_type, str):
                raise ValueError(
                    f"update_node_settings payload missing integer node_id / string node_type: {payload!r}",
                )
            try:
                modifications.append(
                    StagedSettingsUpdate(
                        node_id=node_id,
                        node_type=node_type,
                        old_settings=payload.get("old_settings") or {},
                        new_settings=payload.get("new_settings") or {},
                        predicted_output_schema=payload.get("predicted_output_schema"),
                        audit_id=entry.audit_id,
                    )
                )
            except Exception as exc:
                raise ValueError(f"invalid modification payload for {tool_name!r}: {exc}") from exc
        elif tool_name == _CONNECT_NAME:
            connections_added.append(
                StagedConnection(
                    connection=payload.get("connection", {}),
                    audit_id=entry.audit_id,
                )
            )
        elif tool_name == _DELETE_NODE_NAME:
            node_id = payload.get("delete_node_id")
            if not isinstance(node_id, int):
                raise ValueError(
                    f"delete_node payload missing integer delete_node_id: {payload!r}",
                )
            deletions.append(
                StagedDeletion(
                    delete_node_id=node_id,
                    audit_id=entry.audit_id,
                )
            )
        elif tool_name == _DELETE_CONNECTION_NAME:
            connections_removed.append(
                StagedConnection(
                    connection=payload.get("delete_connection", {}),
                    audit_id=entry.audit_id,
                )
            )
        elif tool_name.startswith(_GRAPH_PREFIX):
            raise ValueError(f"unsupported graph op for diff staging: {tool_name!r}")
        else:
            raise ValueError(f"diff staging only accepts flowfile.graph.* tools; got {tool_name!r}")

    # 2026-05-07 — dedupe modifications and additions by node_id (latest wins).
    # Live trace 14:21 showed the LLM emitting 4 identical
    # ``update_node_settings`` calls on the same node within one agent run;
    # without dedupe the diff preview surfaced *"4 modifications"* with
    # four identical cards stacked. The W47 contract is full-replace, so
    # the latest staged call already reflects the cumulative intent — older
    # calls for the same node are noise that confuse the review surface.
    # Same logic applies to additions: if the LLM emits multiple ``add_*``
    # for the same allocated id (rare but observed), keep the last.
    modifications = _dedupe_by_node_id_keep_last(modifications, lambda m: m.node_id)
    additions = _dedupe_by_node_id_keep_last(
        additions,
        lambda a: a.settings.get("node_id") if isinstance(a.settings, dict) else None,
    )

    return GraphDiff(
        session_id="",
        flow_id=0,
        additions=additions,
        modifications=modifications,
        connections_added=connections_added,
        deletions=deletions,
        connections_removed=connections_removed,
    )


def _dedupe_by_node_id_keep_last(items: list, key: Any) -> list:
    """Keep the LAST occurrence per node-id key, preserving original order minus dupes.

    ``key`` is a callable that returns the node-id (or ``None`` for entries
    that should never dedupe — those pass through unchanged). Walks ``items``
    in reverse, records ``node_id``s as it sees them, drops any subsequent
    (= earlier in original order) item with a node-id we've already seen.
    Reverses again to restore op-order.
    """
    seen: set[int] = set()
    deduped: list = []
    for item in reversed(items):
        node_id = key(item)
        if isinstance(node_id, int):
            if node_id in seen:
                continue
            seen.add(node_id)
        deduped.append(item)
    deduped.reverse()
    return deduped


def collect_audit_ids(diff: GraphDiff) -> list[int]:
    """Walk every op in op-order, returning the ``audit_id`` for those that have one.

    Order matches :func:`apply_diff` traversal: additions, modifications,
    connections_added, deletions, connections_removed. ``None`` values are
    skipped — the audit table only flips rows that actually exist.
    """
    out: list[int] = []
    for add in diff.additions:
        if add.audit_id is not None:
            out.append(add.audit_id)
    for m in diff.modifications:
        if m.audit_id is not None:
            out.append(m.audit_id)
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
    """Raise :class:`DiffDriftError` or :class:`DiffInconsistentError` on bad refs.

    Two parallel checks fire here:

    * **Drift (D006):** addition upstreams + deletion targets must resolve
      on the live graph *now* OR appear earlier in this diff's
      ``additions`` bucket. If not, raise :class:`DiffDriftError` —
      mapped to ``409 Conflict`` by the route layer; the user mutated
      the canvas between staging and accept.
    * **Inconsistency (W70):** every staged ``connections_added`` entry's
      ``from`` and ``to`` endpoint ids must resolve on the live graph
      *now* OR appear in this diff's ``additions`` bucket. If not, raise
      :class:`DiffInconsistentError` — mapped to ``422 Unprocessable
      Entity`` by the route layer; the agent's own diff is broken
      (e.g. an LLM-hallucinated ``to_node_id``).

    A node id counts as available if it either resolves on the live
    graph *now* OR appears earlier in this diff's ``additions`` bucket —
    chained additions (filter then second filter on top of the first)
    are a normal pattern and don't constitute drift. Chained connections
    referencing an id added in the same diff (e.g. wiring a freshly
    staged left input to a freshly staged join) are likewise legal.

    Drift is checked first; if both fire the drift raise wins because it
    is the deeper precondition (a missing upstream means the addition
    itself can't be reconstructed; a missing connection endpoint means
    only the wiring is broken).
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
    for mod in diff.modifications:
        # Modifications target an existing node id OR an in-batch addition
        # (the same chaining tolerance that drives addition upstream resolution).
        if not _is_available(mod.node_id):
            missing.add(mod.node_id)
    for d in diff.deletions:
        if not _is_available(d.delete_node_id):
            missing.add(d.delete_node_id)
    if missing:
        raise DiffDriftError(sorted(missing))

    # W70 — inconsistency check. Walks ``connections_added`` only;
    # ``connections_removed`` references are tolerated by ``delete_connection``'s
    # own no-op-on-missing semantics, and pre-checking them would conflate
    # legitimate "user accepted the staged removal of a connection that's
    # already gone" with the agent emitting a phantom id.
    inconsistent: list[tuple[int, str]] = []
    for c in diff.connections_added:
        connection = c.connection if isinstance(c.connection, dict) else {}
        for role, side_key in (("from", "output_connection"), ("to", "input_connection")):
            side = connection.get(side_key)
            if not isinstance(side, dict):
                continue
            raw = side.get("node_id")
            if not isinstance(raw, int):
                continue
            if not _is_available(raw):
                inconsistent.append((raw, role))
    if inconsistent:
        raise DiffInconsistentError(inconsistent)


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

    op_count = (
        len(diff.additions)
        + len(diff.modifications)
        + len(diff.connections_added)
        + len(diff.deletions)
        + len(diff.connections_removed)
    )
    description = f"AI diff: {diff.rationale}" if diff.rationale else f"AI diff ({op_count} ops)"
    flow.capture_history_snapshot(HistoryActionType.BATCH, description)

    prior_track = flow.flow_settings.track_history
    flow.flow_settings.track_history = False

    applied_node_ids: list[int] = []
    modified_node_ids: list[int] = []
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

        for mod in diff.modifications:
            settings_cls = get_settings_class_for_node_type(mod.node_type)
            if settings_cls is None:
                raise ValueError(f"unknown node type: {mod.node_type!r}")
            settings = settings_cls.model_validate(mod.new_settings)
            _apply_update_node_settings(flow, mod.node_type, settings)
            modified_node_ids.append(int(mod.node_id))

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
        modified_node_ids=modified_node_ids,
        applied_connection_count=applied_connection_count,
        removed_node_ids=removed_node_ids,
        removed_connection_count=removed_connection_count,
        audit_ids=collect_audit_ids(diff),
    )


__all__ = [
    "ApplyResult",
    "DiffDriftError",
    "DiffInconsistentError",
    "GraphDiff",
    "StagedAddition",
    "StagedConnection",
    "StagedDeletion",
    "StagedInsertionContext",
    "StagedSchemaColumn",
    "StagedSettingsUpdate",
    "StagedToolEntry",
    "apply_diff",
    "bundle_staged_results",
    "clear_for_tests",
    "collect_audit_ids",
    "get_diff",
    "get_diff_repo",
    "pop_diff",
    "register_diff",
    "set_diff_repo",
    "validate_diff_against_flow",
]
