"""Disk-persisted ``AgentSession`` records — owned by W42; in-memory shape by W40.

W40 ships the **in-memory** session lifecycle. W42 swaps the in-memory store
for a disk-backed sidecar under ``{user_data_directory}/ai_sessions/{flow_id}/``
without touching the public surface (``register_session`` / ``get_session`` /
``pop_session`` / ``clear_for_tests``) — same pattern W41's ``_DIFFS`` store
established.

Per D006 (snapshot+warn-and-pause), :func:`capture_graph_snapshot` records the
shape of the live graph at agent-start and :func:`detect_drift` compares the
live graph to that snapshot before each tool dispatch. Any mutation to a node
that the agent referenced (deletion, settings change, schema change) yields a
:class:`DriftDetail` so the planner loop can yield ``drift_detected`` +
``paused`` and exit cleanly. The user's resume action picks back up against
either the original or a freshly-captured snapshot.

Per D007, sessions are sidecar-by-default and never embedded in ``.flowfile``
unless the user opts in via the export toggle (W43).
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

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

    The snapshot is intentionally cheap: node ids, deterministic hashes of
    each node's settings, and a stable signature of each node's predicted
    schema. No row data, no expensive callbacks.

    Hash determinism rests on
    ``json.dumps(model_dump(mode="json"), sort_keys=True)`` so the same
    settings produce the same hash across pickle/serialise round-trips and
    across W42's eventual disk persistence. ``model_dump(mode="json")``
    coerces datetimes / Pydantic SecretStr / etc. to JSON-stable shapes.
    """

    model_config = ConfigDict(frozen=True)

    flow_id: int
    node_ids: tuple[int, ...]
    settings_hashes: dict[int, str]
    schema_signatures: dict[int, str]


class DriftDetail(BaseModel):
    """The shape of mutation detected since :class:`GraphSnapshot` was taken.

    Empty buckets are valid (a mutation in any one bucket is drift). The
    planner surfaces this verbatim in the ``drift_detected`` SSE event; the
    frontend's drift-pause banner formats it for the user.
    """

    missing_node_ids: list[int] = Field(default_factory=list)
    """Nodes that existed at snapshot time and are now gone."""
    mutated_node_ids: list[int] = Field(default_factory=list)
    """Nodes whose ``settings_hash`` no longer matches the snapshot."""
    schema_changed_node_ids: list[int] = Field(default_factory=list)
    """Nodes whose predicted schema signature has changed."""

    def is_empty(self) -> bool:
        return not (self.missing_node_ids or self.mutated_node_ids or self.schema_changed_node_ids)


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
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def touch(self) -> None:
        """Bump ``updated_at`` to the current UTC instant."""
        self.updated_at = datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# Hashing — deterministic, JSON-stable                                         #
# --------------------------------------------------------------------------- #


def _hash_settings(settings: Any) -> str:
    """Deterministic SHA-256 of a node's settings.

    Accepts a Pydantic ``BaseModel`` or a plain dict / scalar. Falls back to
    ``repr`` for opaque objects so the hash is at least stable under
    same-process comparison (the only mode that matters until W42).
    """
    if settings is None:
        return _hash_payload(None)
    if isinstance(settings, BaseModel):
        try:
            payload = settings.model_dump(mode="json")
        except Exception:
            payload = repr(settings)
    elif isinstance(settings, dict | list | tuple | str | int | float | bool):
        payload = settings
    else:
        # Best-effort — repr is at least stable within a process.
        payload = repr(settings)
    return _hash_payload(payload)


def _hash_payload(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _schema_signature(node: Any) -> str:
    """Stable signature for a node's predicted schema.

    Reads ``node.node_schema.predicted_schema`` only — no force-recompute.
    Empty / ``None`` schema hashes to a sentinel constant so two un-resolved
    upstreams compare equal (they share the same drift class).
    """
    schema = getattr(getattr(node, "node_schema", None), "predicted_schema", None)
    if not schema:
        return _hash_payload({"schema": None})
    cols = [{"name": getattr(col, "column_name", ""), "type": getattr(col, "data_type", None)} for col in schema]
    return _hash_payload({"schema": cols})


def capture_graph_snapshot(flow: FlowGraph) -> GraphSnapshot:
    """Take a D006 snapshot of ``flow``.

    Pure read: walks ``flow.nodes`` once and reads each node's settings and
    predicted schema. No callbacks fired, no I/O, safe to call before every
    agent step (though the planner only captures once at start and on
    explicit re-snapshot via ``resume(action="continue")``).
    """
    node_ids: list[int] = []
    settings_hashes: dict[int, str] = {}
    schema_signatures: dict[int, str] = {}
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
        settings_hashes[nid] = _hash_settings(node.setting_input)
        schema_signatures[nid] = _schema_signature(node)
    node_ids.sort()
    return GraphSnapshot(
        flow_id=int(flow.flow_id),
        node_ids=tuple(node_ids),
        settings_hashes=settings_hashes,
        schema_signatures=schema_signatures,
    )


def detect_drift(flow: FlowGraph, snapshot: GraphSnapshot) -> DriftDetail | None:
    """Compare ``flow`` to ``snapshot``; return a :class:`DriftDetail` or ``None``.

    Three drift classes — any one is enough to surface a pause:

    * **Missing**: a node that existed at snapshot time has been deleted.
    * **Mutated**: a node still exists but its ``setting_input`` hash has
      changed (the user re-configured a setting since agent-start).
    * **Schema-changed**: a node's predicted schema signature changed (often
      a downstream consequence of a mutated upstream, but worth surfacing
      separately so the user knows *what* the planner's view of the world
      is now wrong about).

    New nodes (added by the user mid-run) are *not* drift — they don't
    invalidate the planner's prior tool calls. The planner can simply
    refer to them via ``read_node_schema`` if they become relevant.
    """
    live_nodes: dict[int, Any] = {}
    for node in flow.nodes:
        try:
            nid = int(node.node_id)
        except (TypeError, ValueError, AttributeError):
            continue
        live_nodes[nid] = node

    missing: list[int] = []
    mutated: list[int] = []
    schema_changed: list[int] = []

    for nid in snapshot.node_ids:
        if nid not in live_nodes:
            missing.append(nid)
            continue
        node = live_nodes[nid]
        live_settings_hash = _hash_settings(node.setting_input)
        if live_settings_hash != snapshot.settings_hashes.get(nid):
            mutated.append(nid)
        live_schema_sig = _schema_signature(node)
        if live_schema_sig != snapshot.schema_signatures.get(nid):
            schema_changed.append(nid)

    if not (missing or mutated or schema_changed):
        return None
    return DriftDetail(
        missing_node_ids=sorted(missing),
        mutated_node_ids=sorted(mutated),
        schema_changed_node_ids=sorted(schema_changed),
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
]
