"""Disk-persisted ``AgentSession`` records — owned by W42; in-memory shape by W40.

W40 ships the **in-memory** session lifecycle. W42 swaps the in-memory store
for a disk-backed sidecar under ``{storage.ai_sessions_directory}/{flow_id}/``
(per plan §5.6) without touching the public surface (``register_session`` /
``get_session`` / ``pop_session`` / ``clear_for_tests``) — same pattern
W41's ``_DIFFS`` store now follows. The active repo is module-level
``_REPO``; ``clear_for_tests`` swaps it for a fresh
:class:`flowfile_core.ai.session_store.InMemorySessionRepository` so tests
never touch real user data.

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

from flowfile_core.ai.diff import AppliedNodeRecord, StagedToolEntry
from flowfile_core.ai.providers.base import Message
from flowfile_core.ai.session_store import (
    DiskSessionRepository,
    InMemorySessionRepository,
    SessionRepository,
)
from shared.storage_config import storage

if TYPE_CHECKING:
    from flowfile_core.flowfile.flow_graph import FlowGraph

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Wire types — D006 snapshot + drift                                           #
# --------------------------------------------------------------------------- #


SessionStatus = Literal[
    "running",
    "paused_drift",
    "paused_user_action",
    "awaiting_user",
    "awaiting_user_input",
    "completed",
    "aborted",
    "failed",
]
"""Session lifecycle. ``paused_user_action`` (W42) is the cold-start state a
``running`` session flips to when the in-memory mirror is empty — the
previous SSE stream is dead, the user must explicitly re-attach via
``POST /ai/agent/{session_id}/resume?action=continue``.

``awaiting_user_input`` (W49) is the post-completion sub-state when the
planner stops with a clarifying question (no tool calls, last assistant
message looks like a question, ``staged_results`` empty). Distinct from
``completed`` so the frontend can render *"Agent waiting for your reply…"*
instead of the misleading *"finished — nothing to stage"*. Both
``awaiting_user_input`` and ``completed`` are followup-resumable via
``POST /ai/agent/{session_id}/followup`` — see W49 for the wire shape."""

PlannerSurface = Literal["agent_complex", "agent_staged", "agent_live"]
"""W71 v1.10 — legacy ``"agent"`` surface (two-stage with
``flowfile.meta.pick_category``) was removed: it was the failure mode
that triggered W71 (small open-weights models silently fall back to
text-JSON-in-content instead of using the function-calling API).
``agent_staged`` covers the small-model case via a one-tool-per-round
state machine; ``agent_complex`` covers single-shot full-catalog use;
W71 v2.0 — ``agent_live`` is a third opt-in surface that mirrors
``agent_staged``'s state machine through fill_settings but applies
each step LIVE to the canvas (mode="apply" not "stage"), runs the
affected subgraph (Performance) or evaluates a sample (Development),
and feeds the runtime observation back to the LLM as the next
tool reply. On runtime failure the just-added node is auto-deleted
and the LLM retries up to ``max_retries_per_step``; the canvas is
always at the last-successful state. No staged_results bundle —
every action is live; the chat trail's ``applied_results`` list is
the equivalent record-of-truth for that surface."""
SamplesMode = Literal["off", "regex"]


PlannerStage = Literal[
    "plan",
    "classify",
    "pick_type",
    "pick_upstream",
    "fill_settings",
    "single_stage_op",
    "verify_completion",
]
"""W71 — current state in the ``agent_staged`` multi-stage state machine.

Each stage exposes exactly one tool to the function-calling API so smaller
models comply with the API rather than emitting text-JSON. The legacy
``agent`` and ``agent_complex`` surfaces ignore this field — only the
``agent_staged`` surface drives transitions through it.

* ``classify`` — exposes ``flowfile.meta.classify_intent``; the LLM picks
  an :data:`PlannerOpKind` value. Default at session start.
* ``pick_type`` — add path only. Exposes ``flowfile.meta.pick_node_type``.
* ``pick_upstream`` — add path only. Exposes
  ``flowfile.meta.pick_upstream`` with the upstream id enum populated
  per-turn from ``live_ids ∪ session.staged_node_ids``.
* ``fill_settings`` — add path only. Exposes the picked node type's
  ``flowfile.graph.add_<type>`` tool with planner-injected fields stripped
  from its parameter schema.
* ``single_stage_op`` — non-add path. Exposes the one matching ops tool
  (``update_node_settings`` / ``delete_node`` / ``connect`` /
  ``delete_connection``) per :attr:`AgentSession.picked_op_kind`.
"""

PlannerOpKind = Literal[
    "add",
    "modify",
    "delete",
    "connect",
    "disconnect",
    "other",
]
"""W71 — the user-intent kind chosen by ``classify_intent`` at stage 0.

``add`` advances through stages ``pick_type → pick_upstream → fill_settings``.
``modify`` / ``delete`` / ``connect`` / ``disconnect`` advance to
``single_stage_op`` exposing exactly one ops tool. ``other`` terminates
the loop — the LLM's accompanying assistant content becomes the final
response (used for clarifying questions, "what does this flow do?", etc.).
"""


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
    surface: PlannerSurface = "agent_staged"
    samples_mode: SamplesMode = "off"
    provider_name: str
    """Name of the BYOK provider — used by the resume route to re-resolve
    the same provider (and model) on continue. Stored at session-start so
    drift-pause/resume doesn't need a provider override on the wire."""
    model_name: str | None = None
    snapshot: GraphSnapshot
    messages: list[Message] = Field(default_factory=list)
    staged_results: list[StagedToolEntry] = Field(default_factory=list)
    applied_results: list[AppliedNodeRecord] = Field(default_factory=list)
    """W71 v2.0 — per-step applied-live record for ``surface=agent_live``.
    Mirrors ``staged_results`` for the chat-trail rendering and the
    audit / undo paths, but the records describe nodes ALREADY in
    ``flow.nodes`` (not staged proposals). Empty for the other
    surfaces; populated one entry per successful apply round on
    ``agent_live``."""
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
    selected_node_ids: list[int] = Field(default_factory=list)
    """W57 — node ids the user had selected on the canvas at session start.
    Read by ``planner._resolve_insertion_context`` as a fallback signal for
    the contextual upstream when the LLM does not provide an explicit
    ``upstream_node_ids`` arg AND no in-batch staged-add chain is in flight.
    Wired in from ``AgentStartRequest.selected_node_ids`` (frontend reads
    from the live flow store at start time)."""
    pinned_node_ids: list[int] = Field(default_factory=list)
    """W57 — node ids the user pinned via ``@``-mention (W24) at session
    start. Read by ``planner._resolve_insertion_context`` as a tier below
    selection. Currently always empty — no v0 wire path populates it; the
    field is structurally present so a future workstream can extract
    ``@``-mentions from the user prompt without an additional schema bump."""
    stage: PlannerStage = "plan"
    """W71 v2.4 — multi-stage state-machine surfaces (agent_staged /
    agent_live) start at the **plan** stage where the LLM emits a
    short markdown plan via ``flowfile.meta.emit_plan`` before the
    classify→pick→fill cycle starts. Single-shot ``agent_complex``
    ignores ``stage`` entirely. ``reset_stage_state`` resets to
    ``"classify"`` (NOT ``"plan"``) so multi-node turns don't
    re-plan after each successful add — the plan covers the whole
    user request and only fires once at session start."""
    """W71 — current stage in the ``agent_staged`` state machine. Ignored
    by legacy ``agent`` / ``agent_complex`` surfaces. Reset to
    ``"classify"`` after each successful ``add_*`` or single-stage non-add
    op via :func:`reset_stage_state` so multi-node turns serialize cleanly."""
    picked_op_kind: PlannerOpKind | None = None
    """W71 — the op_kind chosen by ``classify_intent`` at stage 0. Drives
    ``single_stage_op`` surface selection for non-add intents. Cleared by
    :func:`reset_stage_state`."""
    picked_node_type: str | None = None
    """W71 — the node type chosen by ``pick_node_type`` at stage 1.
    Used by the planner to build a per-turn one-tool catalog for stage 3
    (``fill_settings``). Cleared by :func:`reset_stage_state`."""
    picked_upstream_ids: list[int] = Field(default_factory=list)
    """W71 — primary-input upstream node ids chosen by ``pick_upstream``
    at stage 2. Injected into the ``add_<type>`` call's
    ``InsertionContext`` at stage 3. Cleared by :func:`reset_stage_state`."""
    picked_right_input_id: int | None = None
    """W71 — optional right-input node id chosen by ``pick_upstream`` at
    stage 2 (join-shaped node types only). Injected into the
    ``InsertionContext.right_input_node_id`` at stage 3. Cleared by
    :func:`reset_stage_state`."""
    verify_plan_completion: bool = False
    """W71 v2.12 — opt-in: when True, ``op_kind="other"`` at classify
    advances to ``stage="verify_completion"`` for one extra LLM round
    before terminating. Wired in from ``AgentStartRequest``. Default
    off (no extra LLM round per agent run)."""
    verify_round_consumed: bool = False
    """W71 v2.12 — one-shot guard for the verify-completion gate. Set
    True after the LLM returns from ``flowfile.meta.verify_completion``.
    If the LLM said ``is_complete=false`` and a subsequent classify
    round still ends with ``op_kind="other"``, the loop terminates
    without re-entering verify (capped at one verify round per loop
    so a stubborn ``is_complete=false`` can't ping-pong). Not reset
    by ``reset_stage_state`` — the cap is per-session, not per-stage."""
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def touch(self) -> None:
        """Bump ``updated_at`` to the current UTC instant."""
        self.updated_at = datetime.now(timezone.utc)


def reset_stage_state(session: AgentSession) -> None:
    """W71 — reset the ``agent_staged`` state machine back to stage 0.

    Called by the planner after each successful ``add_*`` (at stage
    ``fill_settings``) or successful single-stage non-add op (at stage
    ``single_stage_op``). The next loop iteration starts a fresh
    classify→pick→fill cycle so multi-node turns work without history
    pruning.

    No-op for legacy ``agent`` / ``agent_complex`` surfaces — they don't
    drive transitions through these fields.
    """
    session.stage = "classify"
    session.picked_op_kind = None
    session.picked_node_type = None
    session.picked_upstream_ids = []
    session.picked_right_input_id = None


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
# Repository delegation — W42 swap; public surface unchanged                   #
# --------------------------------------------------------------------------- #


def _build_default_repo() -> SessionRepository:
    """Default to the on-disk sidecar repo rooted at the storage helper.

    See ``shared.storage_config.FlowfileStorage.ai_sessions_directory`` for
    the path resolution (Docker → ``user_data_directory / "ai_sessions"``;
    local → ``~/.flowfile/ai_sessions/``). Tests never see this default —
    ``clear_for_tests`` swaps in :class:`InMemorySessionRepository` before
    the first call.
    """
    return DiskSessionRepository(root=storage.ai_sessions_directory)


_REPO: SessionRepository = _build_default_repo()
_REPO_LOCK = threading.Lock()


def set_session_repo(repo: SessionRepository) -> SessionRepository:
    """Swap the active session repository; return the previous one.

    Tests use this to point the module-level surface at a ``tmp_path``-rooted
    :class:`DiskSessionRepository` (integration tests) or at a fresh
    :class:`InMemorySessionRepository` (unit tests). Production never calls
    this — the default is set at import time.
    """
    global _REPO
    with _REPO_LOCK:
        prev = _REPO
        _REPO = repo
    return prev


def get_session_repo() -> SessionRepository:
    """Read the active session repository (cheap)."""
    with _REPO_LOCK:
        return _REPO


def register_session(session: AgentSession) -> str:
    """Store ``session`` under its ``session_id``; return the id.

    Idempotent on the same ``session_id`` — re-registering overwrites.
    Callers that need atomic-upsert semantics can use ``get_session`` first.
    """
    repo = get_session_repo()
    repo.put(session)
    return session.session_id


def get_session(session_id: str, *, user_id: int | None = None) -> AgentSession | None:
    """Look up a session, optionally enforcing ``user_id`` ownership.

    When ``user_id`` is provided, returns ``None`` for sessions belonging
    to a different user — the route layer maps that to 404 rather than 403
    so the existence of someone else's session isn't leaked.
    """
    return get_session_repo().get(session_id, user_id=user_id)


def pop_session(session_id: str, *, user_id: int | None = None) -> AgentSession | None:
    """Remove and return a session, optionally enforcing ``user_id`` ownership.

    Idempotent — ``None`` if already popped or if ``user_id`` doesn't match.
    A non-matching ``user_id`` does *not* pop, preventing one user from
    deleting another's session via id-guess.
    """
    return get_session_repo().pop(session_id, user_id=user_id)


def clear_for_tests() -> None:
    """Swap the active repo for a fresh :class:`InMemorySessionRepository`.

    Tests' autouse fixtures call this before/after each case; the swap
    guarantees no test ever writes to (or reads from) the production disk
    sidecar at ``~/.flowfile/ai_sessions/``. After the swap, integration
    tests can point at a ``tmp_path``-rooted disk repo via
    :func:`set_session_repo`.
    """
    set_session_repo(InMemorySessionRepository())


def list_sessions_for_user(user_id: int) -> list[AgentSession]:
    """Return all active sessions belonging to ``user_id``.

    Used by the (future) ``GET /ai/agent/sessions`` route — out of scope
    for W40 but cheap to expose.
    """
    return get_session_repo().list_for_user(user_id)


def list_archived_sessions(user_id: int, flow_id: int) -> list[AgentSession]:
    """W42 — archived terminal sessions for ``(user_id, flow_id)``.

    In-memory backend always returns ``[]`` (no archive). Disk backend
    returns the FIFO archive entries sorted recent-first. Used by W43's
    "open recent chats" UI.
    """
    return get_session_repo().list_archived(user_id=user_id, flow_id=flow_id)


__all__ = [
    "AgentSession",
    "DriftDetail",
    "GraphSnapshot",
    "PlannerOpKind",
    "PlannerStage",
    "PlannerSurface",
    "SamplesMode",
    "SessionStatus",
    "capture_graph_snapshot",
    "clear_for_tests",
    "detect_drift",
    "get_session",
    "get_session_repo",
    "list_archived_sessions",
    "list_sessions_for_user",
    "pop_session",
    "register_session",
    "reset_stage_state",
    "revalidate_staged_results_against_live",
    "set_session_repo",
]
