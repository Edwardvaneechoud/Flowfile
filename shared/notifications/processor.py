"""Evaluates completed runs into outbox rows and drains the outbox over webhooks.

Free of flowfile_core imports: this runs from the scheduler tick and from the CLI
subprocess that just finished a run, neither of which may pull in core. Session and
engine handling mirrors ``shared.run_completion``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, create_engine, or_
from sqlalchemy.orm import Session

from shared.models import (
    FlowRun,
    NotificationChannel,
    NotificationOutbox,
    NotificationRule,
)
from shared.notifications import crypto, senders
from shared.notifications.payload import build_run_event_payload
from shared.storage_config import get_database_url

logger = logging.getLogger("flowfile.notifications")

# ``in_designer_run`` is interactive — the user is already watching it, never alert.
RUN_EVENT_RUN_TYPES = ("scheduled", "manual", "on_demand")

# A run that ended longer ago than this is stamped without alerting: coming back from
# a week of downtime must not replay a week of failures.
MAX_EVENT_AGE_SECONDS = 86400

EVAL_BATCH_LIMIT = 200
DRAIN_BATCH_LIMIT = 25

# ``next_attempt_at`` doubles as the lease expiry, so a drainer killed mid-send
# releases the row after this long instead of wedging it in "sending" forever.
SEND_LEASE_SECONDS = 600

MAX_ATTEMPTS = 5
BACKOFF_SCHEDULE = (60, 300, 900, 3600)

MAX_LAST_ERROR_CHARS = 500

EVENT_RULE_FLAGS = {
    "run_failed": "on_failure",
    "run_orphaned": "on_failure",
    "run_success": "on_success",
    "run_recovered": "on_recovery",
}


def _utcnow() -> datetime:
    """Naive UTC — the catalog DB stores naive datetimes and compares them naively."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _make_session() -> Session:
    url = get_database_url()
    connect_args = {"check_same_thread": False} if "sqlite" in url else {}
    return Session(create_engine(url, connect_args=connect_args))


def _matching_rules(session: Session, run: FlowRun) -> list[NotificationRule]:
    """Enabled rules on enabled channels whose scope covers *run*.

    A NULL run field never matches a scoped rule, so the scope arms are only added
    for the keys the run actually carries.
    """
    scopes = []
    if run.schedule_id is not None:
        scopes.append(NotificationRule.schedule_id == run.schedule_id)
    if run.registration_id is not None:
        scopes.append(
            and_(
                NotificationRule.schedule_id.is_(None),
                NotificationRule.registration_id == run.registration_id,
            )
        )
    scopes.append(
        and_(
            NotificationRule.schedule_id.is_(None),
            NotificationRule.registration_id.is_(None),
            NotificationRule.owner_id == run.user_id,
        )
    )

    return (
        session.query(NotificationRule)
        .join(NotificationChannel, NotificationChannel.id == NotificationRule.channel_id)
        .filter(
            NotificationRule.enabled.is_(True),
            NotificationChannel.enabled.is_(True),
            or_(*scopes),
        )
        .all()
    )


def _enqueue_events(session: Session, run: FlowRun, events: list[str], reason: str | None = None) -> int:
    """Insert one outbox row per (matching rule, event). The caller owns the commit."""
    if not events:
        return 0

    rules = _matching_rules(session, run)
    if not rules:
        return 0

    payloads = {event: build_run_event_payload(session, run, event, reason=reason) for event in events}
    now = _utcnow()
    enqueued = 0

    for rule in rules:
        for event in events:
            if not getattr(rule, EVENT_RULE_FLAGS[event], False):
                continue
            # The unique constraint is the real guard; this pre-check keeps a duplicate
            # from raising inside the transaction that also holds the run claim.
            exists = (
                session.query(NotificationOutbox.id)
                .filter(
                    NotificationOutbox.rule_id == rule.id,
                    NotificationOutbox.run_id == run.id,
                    NotificationOutbox.event_type == event,
                )
                .first()
            )
            if exists:
                continue

            session.add(
                NotificationOutbox(
                    rule_id=rule.id,
                    channel_id=rule.channel_id,
                    run_id=run.id,
                    event_type=event,
                    payload_json=json.dumps(payloads[event]),
                    status="pending",
                    attempts=0,
                    next_attempt_at=None,
                    created_at=now,
                )
            )
            enqueued += 1

    return enqueued


def _claim_run(session: Session, run_id: int, now: datetime) -> bool:
    """Keyed conditional UPDATE — losing the race means another processor took the run."""
    matched = (
        session.query(FlowRun)
        .filter(FlowRun.id == run_id, FlowRun.notification_processed_at.is_(None))
        .update({FlowRun.notification_processed_at: now}, synchronize_session=False)
    )
    return bool(matched)


def _previous_run_failed(session: Session, run: FlowRun) -> bool:
    """Whether the completed run before this one, for the same flow, failed."""
    query = session.query(FlowRun).filter(
        FlowRun.id < run.id,
        FlowRun.ended_at.isnot(None),
        FlowRun.run_type.in_(RUN_EVENT_RUN_TYPES),
    )
    if run.flow_uuid is not None:
        query = query.filter(FlowRun.flow_uuid == run.flow_uuid)
    elif run.registration_id is not None:
        query = query.filter(FlowRun.registration_id == run.registration_id)
    else:
        return False

    previous = query.order_by(FlowRun.id.desc()).first()
    return previous is not None and previous.success is False


def evaluate_completed_runs(session: Session) -> int:
    """Claim newly-ended runs and turn their outcome into outbox rows.

    Returns the number of runs evaluated (claimed), which is not the number of
    alerts enqueued — a run with no matching rule is still stamped.
    """
    runs = (
        session.query(FlowRun)
        .filter(
            FlowRun.ended_at.isnot(None),
            FlowRun.notification_processed_at.is_(None),
            FlowRun.run_type.in_(RUN_EVENT_RUN_TYPES),
        )
        .order_by(FlowRun.id)
        .limit(EVAL_BATCH_LIMIT)
        .all()
    )

    evaluated = 0
    for run in runs:
        now = _utcnow()
        if not _claim_run(session, run.id, now):
            session.rollback()
            logger.debug("Run %s claimed by another processor — skipping", run.id)
            continue

        events: list[str] = []
        age = (now - run.ended_at.replace(tzinfo=None)).total_seconds()
        if age <= MAX_EVENT_AGE_SECONDS:
            if run.success is False:
                events.append("run_failed")
            elif run.success is True:
                events.append("run_success")
                if _previous_run_failed(session, run):
                    events.append("run_recovered")
        else:
            logger.info("Run %s ended %.0fs ago — stamping without alerting", run.id, age)

        _enqueue_events(session, run, events)
        session.commit()
        evaluated += 1

    return evaluated


def enqueue_orphaned_run(session: Session, run: FlowRun, reason: str) -> None:
    """Queue a ``run_orphaned`` alert for a run the reaper just closed.

    Called before the reaper's commit so the close and the alert land together.
    Never raises — reaping must not break on a notification problem.
    """
    try:
        if not _claim_run(session, run.id, _utcnow()):
            return
        _enqueue_events(session, run, ["run_orphaned"], reason=reason)
    except Exception:
        logger.exception("Failed to enqueue orphan notification for run %s", run.id)


def _fail_row(row: NotificationOutbox, error: str, now: datetime) -> None:
    row.last_error = error[:MAX_LAST_ERROR_CHARS]
    if row.attempts >= MAX_ATTEMPTS:
        row.status = "dead"
        return
    backoff = BACKOFF_SCHEDULE[min(row.attempts - 1, len(BACKOFF_SCHEDULE) - 1)]
    row.status = "pending"
    row.next_attempt_at = now + timedelta(seconds=backoff)


def _kill_row(row: NotificationOutbox, error: str) -> None:
    """Terminal failure — retrying cannot help (channel gone, unreadable URL)."""
    row.status = "dead"
    row.last_error = error[:MAX_LAST_ERROR_CHARS]


def _sweep_abandoned_sends(session: Session, now: datetime) -> None:
    """Dead-letter rows whose lease-holder died on the final attempt.

    ``attempts == MAX_ATTEMPTS`` puts a row outside the claim filter, so once its lease
    expires nothing would ever touch it again and it would read "sending" forever.
    An error text already on the row is kept — it says more than this one does.
    """
    abandoned = and_(
        NotificationOutbox.status == "sending",
        NotificationOutbox.attempts >= MAX_ATTEMPTS,
        NotificationOutbox.next_attempt_at <= now,
    )
    session.query(NotificationOutbox).filter(abandoned, NotificationOutbox.last_error.is_(None)).update(
        {NotificationOutbox.last_error: "Delivery interrupted; attempts exhausted"},
        synchronize_session=False,
    )
    session.query(NotificationOutbox).filter(abandoned).update(
        {NotificationOutbox.status: "dead"}, synchronize_session=False
    )
    session.commit()


def drain_outbox(session: Session) -> int:
    """Send due outbox rows. Returns how many were delivered."""
    now = _utcnow()
    _sweep_abandoned_sends(session, now)
    rows = (
        session.query(NotificationOutbox)
        .filter(
            NotificationOutbox.status.in_(("pending", "sending")),
            NotificationOutbox.attempts < MAX_ATTEMPTS,
            or_(NotificationOutbox.next_attempt_at.is_(None), NotificationOutbox.next_attempt_at <= now),
        )
        .order_by(NotificationOutbox.id)
        .limit(DRAIN_BATCH_LIMIT)
        .all()
    )

    sent = 0
    # Circuit break: bounds a pass to ~one timeout per broken channel. Skipped rows are
    # left untouched — not claimed, not counted — so the per-row backoff still governs them.
    broken_channels: set[int] = set()

    for row in rows:
        if row.channel_id in broken_channels:
            continue

        now = _utcnow()
        # Same WHERE as the select, so a concurrent drainer or a still-valid lease loses.
        matched = (
            session.query(NotificationOutbox)
            .filter(
                NotificationOutbox.id == row.id,
                NotificationOutbox.status.in_(("pending", "sending")),
                NotificationOutbox.attempts < MAX_ATTEMPTS,
                or_(NotificationOutbox.next_attempt_at.is_(None), NotificationOutbox.next_attempt_at <= now),
            )
            .update(
                {
                    NotificationOutbox.status: "sending",
                    NotificationOutbox.attempts: NotificationOutbox.attempts + 1,
                    NotificationOutbox.next_attempt_at: now + timedelta(seconds=SEND_LEASE_SECONDS),
                },
                synchronize_session=False,
            )
        )
        session.commit()
        if not matched:
            continue

        channel = session.get(NotificationChannel, row.channel_id)
        if channel is None or not channel.enabled:
            _kill_row(row, "Channel is missing or disabled")
            session.commit()
            continue

        try:
            url = crypto.decrypt_secret(channel.webhook_url_encrypted)
        except Exception as e:
            _kill_row(row, f"Could not decrypt webhook URL: {e}")
            session.commit()
            continue

        # Re-validated at send time: the env (and DNS) may have changed since enqueue.
        error = senders.validate_webhook_url(url)
        if error:
            _kill_row(row, error)
            session.commit()
            continue

        try:
            senders.send_webhook(channel.channel_type, url, json.loads(row.payload_json))
        except Exception as e:
            _fail_row(row, str(e), _utcnow())
            session.commit()
            broken_channels.add(row.channel_id)
            logger.warning("Outbox row %s delivery failed (attempt %s): %s", row.id, row.attempts, e)
            continue

        row.status = "sent"
        row.sent_at = _utcnow()
        row.last_error = None
        session.commit()
        sent += 1

    return sent


def process_pending_notifications() -> tuple[int, int]:
    """One-shot evaluate + drain against its own session. Returns (evaluated, sent)."""
    with _make_session() as session:
        evaluated = evaluate_completed_runs(session)
        sent = drain_outbox(session)

    if evaluated or sent:
        logger.info("Notifications: evaluated %d run(s), sent %d webhook(s)", evaluated, sent)
    return evaluated, sent
