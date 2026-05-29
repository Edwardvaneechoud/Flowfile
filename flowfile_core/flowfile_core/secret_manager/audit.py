"""Secret-access audit log — persistence + query layer.

Every secret CRUD endpoint records exactly one row via :func:`record_event` so
operators can answer "who touched what, when, from where" after the fact. Read
access is via :func:`query_events`, exposed through the audit route.

Decrypt activity during flow execution is *deliberately* not recorded here:
that path runs deep inside the engine where threading a DB session and user
context would be invasive, and the resulting volume would drown the genuinely
interesting CRUD events. If decrypt-time accounting becomes load-bearing later,
add a separate event source rather than expanding this one.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from sqlalchemy.orm import Session

from flowfile_core.database.connection import SessionLocal
from flowfile_core.database.models import SecretAccessEvent

logger = logging.getLogger(__name__)

Action = Literal["create", "list", "read", "update", "delete"]
ResultStatus = Literal["success", "error"]


@dataclass(slots=True)
class SecretEvent:
    """In-memory representation of a secret-access attempt before persistence.

    ``secret_name`` is optional because ``list`` doesn't target a specific name.
    ``secret_id`` is optional because pre-create the row doesn't exist yet and
    post-delete it has already been removed.
    """

    user_id: int
    action: Action
    result_status: ResultStatus
    secret_name: str | None = None
    secret_id: int | None = None
    error: str | None = None
    source: str = "api"
    ip_address: str | None = None


def record_event(event: SecretEvent, db: Session | None = None) -> SecretAccessEvent:
    """Persist a :class:`SecretEvent` to ``secret_access_events``.

    Pass ``db`` to keep the write inside the caller's transaction; pass ``None``
    to use a short-lived session that commits before returning. Audit writes
    are best-effort: an exception here is logged and swallowed rather than
    failing the user-facing request, because a CRUD operation that already
    succeeded shouldn't roll back over a missing audit row.
    """
    row = SecretAccessEvent(
        user_id=event.user_id,
        secret_id=event.secret_id,
        secret_name=event.secret_name,
        action=event.action,
        result_status=event.result_status,
        error=event.error,
        source=event.source,
        ip_address=event.ip_address,
    )

    try:
        if db is not None:
            with db.begin_nested():
                db.add(row)
        else:
            with SessionLocal() as session:
                session.add(row)
                session.commit()
                session.refresh(row)
                session.expunge(row)
    except Exception as exc:
        logger.warning(
            "secret_audit_write_failed user=%s action=%s name=%s err=%s",
            event.user_id,
            event.action,
            event.secret_name,
            exc,
        )
        return row

    logger.info(
        "secret_audit user=%s action=%s name=%s status=%s source=%s",
        event.user_id,
        event.action,
        event.secret_name or "-",
        event.result_status,
        event.source,
    )
    return row


def query_events(
    *,
    user_id: int | None = None,
    secret_name: str | None = None,
    action: Action | None = None,
    limit: int = 100,
    db: Session | None = None,
) -> list[SecretAccessEvent]:
    """Read audit events. Filters compose; rows ordered DESC by ``created_at``."""

    def _query(session: Session) -> list[SecretAccessEvent]:
        q = session.query(SecretAccessEvent)
        if user_id is not None:
            q = q.filter(SecretAccessEvent.user_id == user_id)
        if secret_name is not None:
            q = q.filter(SecretAccessEvent.secret_name == secret_name)
        if action is not None:
            q = q.filter(SecretAccessEvent.action == action)
        rows = (
            q.order_by(SecretAccessEvent.created_at.desc(), SecretAccessEvent.id.desc())
            .limit(limit)
            .all()
        )
        for row in rows:
            session.expunge(row)
        return rows

    if db is not None:
        return _query(db)
    with SessionLocal() as session:
        return _query(session)


__all__ = [
    "Action",
    "ResultStatus",
    "SecretEvent",
    "query_events",
    "record_event",
]
