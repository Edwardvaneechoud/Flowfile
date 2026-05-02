"""AI audit log — persistence + query layer for plan §9.4.

Owned by W15. Every Level 1/2/3 surface that mutates a flow or burns tokens
should call :func:`record_event` with an :class:`AuditEvent`. Read-back is via
:func:`query_events`; the ``GET /ai/audit/{flow_id}`` HTTP route lands once
W11/W12 wire auth-gated AI surfaces, and will wrap this same helper.

Design notes:

* ``flow_id`` is an in-memory runtime integer, not an FK to
  ``flow_registrations`` — draft flows aren't always registered but still
  produce auditable AI actions. W43 may revisit when chat-history persistence
  introduces stronger flow identity guarantees.
* ``tool_args`` is JSON-serialised and capped at :data:`MAX_ARGS_BYTES` so a
  misbehaving tool-call payload can't bloat the DB. PII scrubbing of the args
  themselves is W25's responsibility (``safety.py``); the truncation here is
  byte-budget only.
* The function emits a structured log line for the existing logger pipeline
  (plan §6.5) in addition to persisting — useful when running with
  ``FLOWFILE_TESTING=1`` or when a downstream metrics scraper wants a tail.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Literal

from sqlalchemy.orm import Session

from flowfile_core.database.connection import SessionLocal
from flowfile_core.database.models import AiAuditEvent

logger = logging.getLogger(__name__)

#: Hard cap on the JSON-serialised ``tool_args`` payload. Picked to leave
#: room for typical settings-class shapes (a ``filter`` predicate of a few
#: hundred chars; a ``join`` config) while keeping any single audit row
#: small enough that the table stays under a megabyte for thousands of
#: events. Bumping this is fine if real workloads need it; just make sure
#: callers also bump their downstream DB column allowances.
MAX_ARGS_BYTES = 8 * 1024

ResultStatus = Literal["success", "error", "rejected"]
DiffAction = Literal["accepted", "rejected"]


@dataclass(slots=True)
class AuditEvent:
    """In-memory representation of an AI action before persistence.

    Required: ``session_id``, ``user_id``, ``tool_name``, ``result_status``.
    Everything else is optional — the audit row is useful even when the call
    didn't go through a provider (e.g. a refusal) or didn't touch a diff
    (e.g. a read-only Level 1 explain).
    """

    session_id: str
    user_id: int
    tool_name: str
    result_status: ResultStatus
    flow_id: int | None = None
    tool_args: dict[str, Any] | None = None
    error: str | None = None
    provider: str | None = None
    model: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    diff_action: DiffAction | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def record_event(event: AuditEvent, db: Session | None = None) -> AiAuditEvent:
    """Persist an :class:`AuditEvent` to ``ai_audit_events``.

    Pass ``db`` to keep the write inside the caller's transaction; pass
    ``None`` to use a short-lived session that commits before returning. The
    persisted ORM row is returned so callers can update ``diff_action`` later
    (e.g. when the user clicks Accept on a Level 3 diff staged earlier).
    """
    args_json = _truncate_args(event.tool_args) if event.tool_args is not None else None

    row = AiAuditEvent(
        session_id=event.session_id,
        flow_id=event.flow_id,
        user_id=event.user_id,
        tool_name=event.tool_name,
        tool_args=args_json,
        result_status=event.result_status,
        error=event.error,
        provider=event.provider,
        model=event.model,
        prompt_tokens=event.prompt_tokens,
        completion_tokens=event.completion_tokens,
        total_tokens=event.total_tokens,
        diff_action=event.diff_action,
    )

    if db is not None:
        db.add(row)
        db.flush()
    else:
        with SessionLocal() as session:
            session.add(row)
            session.commit()
            session.refresh(row)
            session.expunge(row)

    logger.info(
        "ai_audit session=%s flow=%s tool=%s status=%s tokens=%d provider=%s",
        event.session_id,
        event.flow_id,
        event.tool_name,
        event.result_status,
        event.total_tokens,
        event.provider or "-",
    )
    return row


def update_diff_action(
    audit_id: int,
    action: DiffAction,
    db: Session | None = None,
) -> AiAuditEvent | None:
    """Set ``diff_action`` on a previously-recorded event.

    Returns the updated row, or ``None`` if no row matches ``audit_id`` —
    a stale handle (the event predates a DB reset, or the caller mixed up
    a session id with a row id) shouldn't crash the diff-accept path.
    """

    def _apply(session: Session) -> AiAuditEvent | None:
        row = session.query(AiAuditEvent).filter(AiAuditEvent.id == audit_id).first()
        if row is None:
            return None
        row.diff_action = action
        return row

    if db is not None:
        return _apply(db)
    with SessionLocal() as session:
        row = _apply(session)
        if row is None:
            return None
        session.commit()
        session.refresh(row)
        session.expunge(row)
        return row


def query_events(
    *,
    flow_id: int | None = None,
    user_id: int | None = None,
    session_id: str | None = None,
    tool_name: str | None = None,
    limit: int = 100,
    db: Session | None = None,
) -> list[AiAuditEvent]:
    """Read audit events. Filters compose; ordered DESC by ``created_at``.

    The future ``GET /ai/audit/{flow_id}`` route is a thin auth-gated wrapper
    around this. Until then, callers (e.g. W11's cost-per-flow tooling, W31's
    pass-rate aggregator) drive it directly.
    """

    def _query(session: Session) -> list[AiAuditEvent]:
        q = session.query(AiAuditEvent)
        if flow_id is not None:
            q = q.filter(AiAuditEvent.flow_id == flow_id)
        if user_id is not None:
            q = q.filter(AiAuditEvent.user_id == user_id)
        if session_id is not None:
            q = q.filter(AiAuditEvent.session_id == session_id)
        if tool_name is not None:
            q = q.filter(AiAuditEvent.tool_name == tool_name)
        rows = q.order_by(AiAuditEvent.created_at.desc()).limit(limit).all()
        # Detach so callers can use the rows after the session closes.
        for row in rows:
            session.expunge(row)
        return rows

    if db is not None:
        return _query(db)
    with SessionLocal() as session:
        return _query(session)


def _truncate_args(args: dict[str, Any]) -> str:
    """Serialise ``args`` to JSON, replacing oversize payloads with a marker.

    A truncated payload is replaced by a small JSON object that records the
    original size and a clipped preview, rather than emitting an invalid
    fragment. Downstream consumers (audit-log UI, W31 metrics) can detect
    truncation by the ``__truncated__`` key.
    """
    try:
        blob = json.dumps(args, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        # Some pathological argument tree (e.g. a circular ref) — record
        # that fact rather than failing the whole audit write.
        return json.dumps({"__truncated__": True, "__error__": "non_serialisable"})

    if len(blob.encode("utf-8")) <= MAX_ARGS_BYTES:
        return blob

    preview = blob[: MAX_ARGS_BYTES // 2]
    return json.dumps(
        {
            "__truncated__": True,
            "__original_size__": len(blob),
            "preview": preview,
        }
    )


__all__ = [
    "MAX_ARGS_BYTES",
    "AuditEvent",
    "DiffAction",
    "ResultStatus",
    "query_events",
    "record_event",
    "update_diff_action",
]
