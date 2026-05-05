"""Disk-persisted ``AgentSession`` records — owned by W42; in-memory shape by W40.

W40 ships the **in-memory** session lifecycle. W42 swaps the in-memory store
for a disk-backed sidecar under ``{user_data_directory}/ai_sessions/{flow_id}/``
without touching the public surface (``register_session`` / ``get_session`` /
``pop_session`` / ``clear_for_tests``) — same pattern W41's ``_DIFFS`` store
established.

Per D006 (snapshot+warn-and-pause), :func:`capture_graph_snapshot` records the
shape of the live graph at agent-start and :func:`detect_drift` compares the
live graph to that snapshot before each tool dispatch. Drift detection is
**id-set-only** (W45): we surface deletions (``missing_node_ids``) and
external additions (``external_added_node_ids``, excluding the agent's own
``staged_node_ids``). Hash-based mutation detection (settings/schema changes)
was removed in W45 because the executor's upstream schema warm-up
(``_resolve_upstream_schemas`` calling ``get_predicted_schema(force=True)``)
mutated live nodes as a side-effect, making the agent self-drift on its own
staging. Restoring hash-based drift is a separate workstream.

Per D007, sessions are sidecar-by-default and never embedded in ``.flowfile``
unless the user opts in via the export toggle (W43).
"""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field

from flowfile_core.ai.diff import StagedToolEntry
from flowfile_core.ai.providers.base import Message

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Wire types — D006 snapshot + drift                                           #
# --------------------------------------------------------------------------- #


SessionStatus = Literal[
    "running",
    "paused_drift",
    "awaiting_user",
    "completed",
    "aborted",
    "failed",
]

PlannerSurface = Literal["agent", "agent_complex"]
SamplesMode = Literal["off", "regex"]


class GraphSnapshot(BaseModel):
    """D006 snapshot — captured at agent-start; compared per-step for drift.

    Tracks the set of node ids that existed at agent-start, plus each node's
    ``node_type`` (W45 — used by :class:`DriftDetail` so the frontend banner
    can render *"Filter node 6 was deleted"* even after the node is gone).

    Hash-based settings/schema drift was removed in W45 — see module docstring.
    """

    model_config = ConfigDict(frozen=True)

    flow_id: int
    node_ids: tuple[int, ...]
    node_types: dict[int, str] = Field(default_factory=dict)


class DriftDetail(BaseModel):
    """The shape of mutation detected since :class:`GraphSnapshot` was taken.

    Two id-set buckets — populating either is enough to surface a pause:

    * **Missing**: a node that existed at snapshot time has been deleted.
    * **External-added**: a node exists on the live flow that was neither in
      the snapshot nor staged by the agent (``staged_node_ids``). The user
      added it externally mid-run.

    ``node_types`` carries the snapshot-time type for every id appearing in
    either bucket so the frontend banner can render typed messages. Optional
    — the frontend falls back to bare ids when an id is missing from the map.
    """

    missing_node_ids: list[int] = Field(default_factory=list)
    """Nodes that existed at snapshot time and are now gone."""
    external_added_node_ids: list[int] = Field(default_factory=list)
    """Nodes on the live flow that the agent did not stage."""
    node_types: dict[int, str] = Field(default_factory=dict)
    """``node_type`` for each id appearing in either bucket. Snapshot-time
    type for missing ids; live type for external-added ids."""

    def is_empty(self) -> bool:
        return not (self.missing_node_ids or self.external_added_node_ids)


class AgentSession(BaseModel):
    """A planner session.

    In-memory today; W42 will pickle this to disk under
    ``{user_data_directory}/ai_sessions/{flow_id}/``. The shape is chosen to
    round-trip cleanly through ``model_dump(mode="json")``.

    ``staged_results`` accumulates the planner's per-step
    :class:`flowfile_core.ai.diff.StagedToolEntry` entries; on completion they
    are bundled into a :class:`flowfile_core.ai.diff.GraphDiff` via
    :func:`flowfile_core.ai.diff.bundle_staged_results` and registered with
    W41's :func:`flowfile_core.ai.diff.register_diff`.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, protected_namespaces=())

    session_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    flow_id: int
    user_id: int
    user_prompt: str
    surface: PlannerSurface = "agent"
    samples_mode: SamplesMode = "off"
    provider_name: str
    """Name of the BYOK provider — used by the resume route to re-resolve
    the same provider (and model) on continue. Stored at session-start so
    drift-pause/resume doesn't need a provider override on the wire."""
    model_name: str | None = None
    snapshot: GraphSnapshot
    messages: list[Message] = Field(default_factory=list)
    staged_results: list[StagedToolEntry] = Field(default_factory=list)
    status: SessionStatus = "running"
    pause_reason: str | None = None
    drift_detail: DriftDetail | None = None
    step_count: int = 0
    max_steps: int = 12
    diff_id: str | None = None
    rationale: str | None = None
    last_assistant_text: str | None = None
    """Text content of the most recent assistant turn — used as the
    ``GraphDiff.rationale`` when the loop completes."""
    staged_node_ids: list[int] = Field(default_factory=list)
    """Node ids the agent has staged this session via ``add_<node_type>``
    tool calls — used to exclude the agent's own additions from W45's
    ``external_added_node_ids`` drift bucket."""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def touch(self) -> None:
        """Bump ``updated_at`` to the current UTC instant."""
        self.updated_at = datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# Snapshot capture + id-set drift detection (W45)                              #
# --------------------------------------------------------------------------- #


def _node_type(node: object) -> str:
    """Best-effort string ``node_type`` reader; defaults to empty string."""
    raw = getattr(node, "node_type", None)
    return str(raw) if raw else ""


def capture_graph_snapshot(flow: FlowGraph) -> GraphSnapshot:
    """Take a D006 snapshot of ``flow``.

    Pure read: walks ``flow.nodes`` once and records each node's id and
    ``node_type``. No settings hashing, no schema callbacks fired — id-set
    drift only (W45). Safe to call before every agent step (the planner
    only captures once at start and on explicit re-snapshot via
    ``resume(action="continue")``).
    """
    node_ids: list[int] = []
    node_types: dict[int, str] = {}
    for node in flow.nodes:
        try:
            nid_raw = node.node_id
        except Exception:
            continue
        try:
            nid = int(nid_raw)
        except (TypeError, ValueError):
            continue
        node_ids.append(nid)
        node_types[nid] = _node_type(node)
    node_ids.sort()
    return GraphSnapshot(
        flow_id=int(flow.flow_id),
        node_ids=tuple(node_ids),
        node_types=node_types,
    )


_ADD_PREFIX = "flowfile.graph.add_"
_StagedDropReason = Literal["live_id_collision", "upstream_missing"]


def _entry_node_id(entry: StagedToolEntry) -> int | None:
    payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else None
    if payload is None:
        return None
    settings = payload.get("settings")
    if not isinstance(settings, dict):
        return None
    nid = settings.get("node_id")
    return nid if isinstance(nid, int) else None


def _entry_upstream_ids(entry: StagedToolEntry) -> list[int]:
    payload = entry.staged_node_payload if isinstance(entry.staged_node_payload, dict) else None
    if payload is None:
        return []
    ic = payload.get("insertion_context")
    if not isinstance(ic, dict):
        return []
    upstream = ic.get("upstream_node_ids")
    out: list[int] = []
    if isinstance(upstream, list):
        for uid in upstream:
            if isinstance(uid, int):
                out.append(uid)
    right = ic.get("right_input_node_id")
    if isinstance(right, int):
        out.append(right)
    return out


def revalidate_staged_results_against_live(
    session: AgentSession,
    flow: FlowGraph,
) -> tuple[list[StagedToolEntry], list[tuple[StagedToolEntry, _StagedDropReason]]]:
    """Drop staged entries that are inconsistent with the live graph.

    Used by the planner's resume-from-drift path (W54). Two drop reasons:

    * ``live_id_collision`` — an ``add_*`` entry's ``node_id`` is now in the
      live graph (the user manually created a node with that id during the
      pause). Keeping the entry would have the planner stage onto an id
      that's already taken; ``apply_diff`` would fail or worse, the next
      ``_allocate_node_id`` round would collide.
    * ``upstream_missing`` — an ``add_*`` entry references an upstream id
      that no longer exists in the live graph (the user deleted the
      upstream during the pause). Keeping the entry would re-introduce an
      edge to a deleted node, surfacing as a 422 at apply time.

    Mutates ``session.staged_results`` (replaces with the kept entries) and
    rebuilds ``session.staged_node_ids`` from the survivors. Returns a
    ``(kept, dropped_with_reason)`` tuple — the planner walks the dropped
    list to emit one audit row per drop. Non-``add_*`` entries (connect,
    delete) pass through unchanged.
    """
    live_ids: set[int] = set()
    for node in flow.nodes:
        try:
            live_ids.add(int(node.node_id))
        except (TypeError, ValueError, AttributeError):
            continue

    kept: list[StagedToolEntry] = []
    dropped: list[tuple[StagedToolEntry, _StagedDropReason]] = []
    surviving_node_ids: list[int] = []

    for entry in session.staged_results:
        if not entry.tool_name.startswith(_ADD_PREFIX):
            kept.append(entry)
            continue

        nid = _entry_node_id(entry)
        if nid is not None and nid in live_ids:
            dropped.append((entry, "live_id_collision"))
            continue

        upstream_ids = _entry_upstream_ids(entry)
        if any(uid not in live_ids for uid in upstream_ids):
            dropped.append((entry, "upstream_missing"))
            continue

        kept.append(entry)
        if nid is not None:
            surviving_node_ids.append(nid)

    session.staged_results = kept
    session.staged_node_ids = surviving_node_ids
    return kept, dropped


def detect_drift(
    flow: FlowGraph,
    snapshot: GraphSnapshot,
    *,
    agent_staged_node_ids: set[int] | None = None,
) -> DriftDetail | None:
    """Compare ``flow`` to ``snapshot``; return a :class:`DriftDetail` or ``None``.

    Two id-set buckets — populating either is enough to surface a pause:

    * **Missing**: a node that existed at snapshot time has been deleted.
    * **External-added**: a node exists on the live flow that was neither in
      the snapshot nor staged by the agent. The user added it externally
      mid-run and the planner's view of the world is now stale.

    ``agent_staged_node_ids`` is the set of node ids the planner has staged
    this session (passed via :attr:`AgentSession.staged_node_ids`). Excluded
    from the external-added bucket so the agent's own work never looks like
    drift to itself. ``mode="stage"`` doesn't actually leak ids into the live
    graph today (W31's executor refuses live mutation in stage mode), but
    excluding them is defensive against future code paths that might.

    Hash-based mutation detection (settings/schema changes) was removed in
    W45 — see module docstring. Restoring it is a separate workstream.
    """
    staged = agent_staged_node_ids or set()

    live_nodes: dict[int, object] = {}
    for node in flow.nodes:
        try:
            nid = int(node.node_id)
        except (TypeError, ValueError, AttributeError):
            continue
        live_nodes[nid] = node

    snapshot_ids = set(snapshot.node_ids)

    missing = sorted(snapshot_ids - set(live_nodes.keys()))
    external_added = sorted(set(live_nodes.keys()) - snapshot_ids - staged)

    if not (missing or external_added):
        return None

    # Build the node_types map for ids in either bucket: snapshot-time type
    # for missing (live no longer has them), live type for external-added.
    types: dict[int, str] = {}
    for nid in missing:
        snap_type = snapshot.node_types.get(nid)
        if snap_type:
            types[nid] = snap_type
    for nid in external_added:
        live_type = _node_type(live_nodes[nid])
        if live_type:
            types[nid] = live_type

    return DriftDetail(
        missing_node_ids=missing,
        external_added_node_ids=external_added,
        node_types=types,
    )


# --------------------------------------------------------------------------- #
# In-memory session store — mirror W41's _DIFFS shape                          #
# --------------------------------------------------------------------------- #


_SESSIONS: dict[str, AgentSession] = {}
_LOCK = threading.Lock()


def register_session(session: AgentSession) -> str:
    """Store ``session`` under its ``session_id``; return the id.

    Idempotent on the same ``session_id`` — re-registering overwrites.
    Callers that need atomic-upsert semantics can use ``get_session`` first.
    """
    with _LOCK:
        _SESSIONS[session.session_id] = session
    return session.session_id


def get_session(session_id: str, *, user_id: int | None = None) -> AgentSession | None:
    """Look up a session, optionally enforcing ``user_id`` ownership.

    When ``user_id`` is provided, returns ``None`` for sessions belonging
    to a different user — the route layer maps that to 404 rather than 403
    so the existence of someone else's session isn't leaked.
    """
    with _LOCK:
        session = _SESSIONS.get(session_id)
    if session is None:
        return None
    if user_id is not None and session.user_id != user_id:
        return None
    return session


def pop_session(session_id: str, *, user_id: int | None = None) -> AgentSession | None:
    """Remove and return a session, optionally enforcing ``user_id`` ownership.

    Idempotent — ``None`` if already popped or if ``user_id`` doesn't match.
    A non-matching ``user_id`` does *not* pop, preventing one user from
    deleting another's session via id-guess.
    """
    with _LOCK:
        session = _SESSIONS.get(session_id)
        if session is None:
            return None
        if user_id is not None and session.user_id != user_id:
            return None
        return _SESSIONS.pop(session_id, None)


def clear_for_tests() -> None:
    """Wipe the in-memory store. Intended for the ``conftest`` autouse fixture."""
    with _LOCK:
        _SESSIONS.clear()


def list_sessions_for_user(user_id: int) -> list[AgentSession]:
    """Return all sessions belonging to ``user_id``.

    Snapshot the dict under the lock; the result is a plain list (caller
    can iterate freely). Used by the (future) ``GET /ai/agent/sessions``
    route — out of scope for W40 but cheap to expose.
    """
    with _LOCK:
        return [s for s in _SESSIONS.values() if s.user_id == user_id]


__all__ = [
    "AgentSession",
    "DriftDetail",
    "GraphSnapshot",
    "PlannerSurface",
    "SamplesMode",
    "SessionStatus",
    "capture_graph_snapshot",
    "clear_for_tests",
    "detect_drift",
    "get_session",
    "list_sessions_for_user",
    "pop_session",
    "register_session",
    "revalidate_staged_results_against_live",
]
