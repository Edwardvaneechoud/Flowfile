"""HTTP routes for run-outcome notifications: webhook channels, rules and history.

A *channel* is a webhook destination (Slack / Discord / Teams / generic JSON); a
*rule* says which run outcomes reach which channel, scoped to a schedule, a flow,
or everything the user runs. Delivery itself is not done here — the run-completion
and scheduler paths enqueue outbox rows and ``shared.notifications.processor``
drains them; these endpoints only manage the configuration and read the log.

Ownership: every row is scoped to ``owner_id == current_user.id``. Another user's
channel or rule answers **404, never 403** — a 403 would confirm that the id exists.
Electron mode has a single ``local_user`` (id 1) that owns everything, so the panel
works there with no special-casing.

The webhook URL is a credential: anyone holding it can post into the workspace. It
is stored only as a ``$ffsec$`` token and **never** returned by any endpoint — reads
get ``webhook_url_preview``, a masked form. Encryption goes through
``shared.notifications.crypto`` rather than core's ``secret_manager`` so the
ciphertext round-trips in the drainer, which must not import flowfile_core (both
sides derive the same per-user key from the same master key).
"""

from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from flowfile_core.auth import secrets as core_secrets
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.configs import logger
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db
from flowfile_core.schemas.catalog_schema import (
    NotificationChannelCreate,
    NotificationChannelOut,
    NotificationChannelTestRequest,
    NotificationChannelUpdate,
    NotificationHistoryItem,
    NotificationRuleCreate,
    NotificationRuleOut,
    NotificationRuleUpdate,
    NotificationTestResult,
)
from shared.notifications import crypto, senders

router = APIRouter(dependencies=[Depends(get_current_active_user)])

# The history table is a local, single-install log; a page bigger than this is never useful.
MAX_HISTORY_LIMIT = 200

# A webhook provider's error body can be long; the UI shows it inline.
MAX_TEST_ERROR_CHARS = 300


def _utcnow() -> datetime:
    """Naive UTC — the catalog DB stores and compares naive datetimes."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _mask_url(url: str) -> str:
    """Mask a webhook URL down to what a human needs to recognise it.

    ``https://hooks.slack.com/services/T0/B0/9f3ab`` → ``https://hooks.slack.com/…f3ab``.
    Any URL we cannot parse into a scheme + host degrades to ``…`` plus the last four
    characters, so a malformed value never leaks its path and never raises. Userinfo is
    dropped rather than echoed (the host is rebuilt from ``hostname``/``port``).
    """
    url = url or ""
    tail = url[-4:]
    try:
        parsed = urlparse(url)
        scheme, host, port = parsed.scheme, parsed.hostname, parsed.port
    except ValueError:
        return f"…{tail}"
    if not scheme or not host:
        return f"…{tail}"
    authority = f"{host}:{port}" if port else host
    return f"{scheme}://{authority}/…{tail}"


def _ensure_master_key() -> None:
    """Make sure a master key exists before encrypting through the shared mirror.

    ``shared.notifications.crypto`` only ever *reads* the key — core owns generation —
    so on a virgin install the first channel would otherwise fail on a missing key.
    """
    try:
        core_secrets.get_master_key()
    except RuntimeError as exc:
        # Docker mode: the key is operator-supplied, nothing can generate it here.
        raise HTTPException(status_code=500, detail=str(exc)) from exc


def _decrypt_channel_url(channel: db_models.NotificationChannel) -> str | None:
    """Plaintext webhook URL, or None when the stored token cannot be read.

    A token written under a master key this install no longer has is a data problem,
    not a request problem: listing must still work, just without a usable preview.
    """
    try:
        return crypto.decrypt_secret(channel.webhook_url_encrypted)
    except Exception:
        logger.warning("Could not decrypt the webhook URL of notification channel %s", channel.id)
        return None


def _channel_out(channel: db_models.NotificationChannel) -> NotificationChannelOut:
    url = _decrypt_channel_url(channel)
    return NotificationChannelOut(
        id=channel.id,
        owner_id=channel.owner_id,
        name=channel.name,
        channel_type=channel.channel_type,
        webhook_url_preview=_mask_url(url) if url is not None else "…",
        enabled=channel.enabled,
        created_at=channel.created_at,
        updated_at=channel.updated_at,
    )


def _get_channel(db: Session, channel_id: int, owner_id: int) -> db_models.NotificationChannel:
    """The owner's channel, or 404 — another owner's id is indistinguishable from a missing one."""
    channel = (
        db.query(db_models.NotificationChannel)
        .filter(
            db_models.NotificationChannel.id == channel_id,
            db_models.NotificationChannel.owner_id == owner_id,
        )
        .first()
    )
    if channel is None:
        raise HTTPException(status_code=404, detail="Channel not found")
    return channel


def _get_rule(db: Session, rule_id: int, owner_id: int) -> db_models.NotificationRule:
    rule = (
        db.query(db_models.NotificationRule)
        .filter(
            db_models.NotificationRule.id == rule_id,
            db_models.NotificationRule.owner_id == owner_id,
        )
        .first()
    )
    if rule is None:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule


def _validated_url(url: str) -> str:
    """Run the shared SSRF guard, turning its message into a 422."""
    error = senders.validate_webhook_url(url)
    if error:
        raise HTTPException(status_code=422, detail=error)
    return url


def _test_result(channel_type: str, url: str) -> NotificationTestResult:
    """Send a sample event. A delivery failure is a 200 with ``ok=False``, never a 500 —
    the user asked "does this webhook work?" and "no" is a valid answer."""
    try:
        senders.send_test_notification(channel_type, url)
    except Exception as exc:
        return NotificationTestResult(ok=False, error=str(exc)[:MAX_TEST_ERROR_CHARS])
    return NotificationTestResult(ok=True)


# Channels


@router.get("/channels", response_model=list[NotificationChannelOut])
def list_channels(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """List the current user's notification channels."""
    channels = (
        db.query(db_models.NotificationChannel)
        .filter(db_models.NotificationChannel.owner_id == current_user.id)
        .order_by(db_models.NotificationChannel.id)
        .all()
    )
    return [_channel_out(channel) for channel in channels]


@router.post("/channels", response_model=NotificationChannelOut, status_code=status.HTTP_201_CREATED)
def create_channel(
    body: NotificationChannelCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Create a channel. The URL is SSRF-validated, then stored encrypted."""
    url = _validated_url(body.webhook_url)
    _ensure_master_key()
    channel = db_models.NotificationChannel(
        owner_id=current_user.id,
        name=body.name,
        channel_type=body.channel_type,
        webhook_url_encrypted=crypto.encrypt_secret(url, current_user.id),
        enabled=body.enabled,
    )
    db.add(channel)
    db.commit()
    db.refresh(channel)
    return _channel_out(channel)


@router.post("/channels/test-url", response_model=NotificationTestResult)
def test_channel_url(
    body: NotificationChannelTestRequest,
    current_user=Depends(get_current_active_user),
):
    """Test a webhook URL before it is saved. Nothing is persisted.

    Declared before the ``/channels/{channel_id}`` routes so the literal path always
    wins the match (the ``int`` path param already excludes it — this is for the reader).
    """
    return _test_result(body.channel_type, body.webhook_url)


@router.put("/channels/{channel_id}", response_model=NotificationChannelOut)
def update_channel(
    channel_id: int,
    body: NotificationChannelUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Update a channel. Only the fields present in the body change; a supplied URL is
    re-validated and re-encrypted (the stored one is never decrypted to compare)."""
    channel = _get_channel(db, channel_id, current_user.id)

    if body.name is not None:
        channel.name = body.name
    if body.channel_type is not None:
        channel.channel_type = body.channel_type
    if body.webhook_url is not None:
        url = _validated_url(body.webhook_url)
        _ensure_master_key()
        channel.webhook_url_encrypted = crypto.encrypt_secret(url, current_user.id)
    if body.enabled is not None:
        channel.enabled = body.enabled
    channel.updated_at = _utcnow()

    db.commit()
    db.refresh(channel)
    return _channel_out(channel)


@router.delete("/channels/{channel_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_channel(
    channel_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Delete a channel, the owner's rules pointing at it, and its delivery history.

    The outbox rows must go with the channel: history ownership is resolved through
    the channel, so orphaned rows would be invisible — until a future channel reuses
    the SQLite rowid and adopts another owner's flow names and error strings.
    """
    channel = _get_channel(db, channel_id, current_user.id)
    db.query(db_models.NotificationOutbox).filter(
        db_models.NotificationOutbox.channel_id == channel.id,
    ).delete(synchronize_session=False)
    db.query(db_models.NotificationRule).filter(
        db_models.NotificationRule.channel_id == channel.id,
        db_models.NotificationRule.owner_id == current_user.id,
    ).delete(synchronize_session=False)
    db.delete(channel)
    db.commit()


@router.post("/channels/{channel_id}/test", response_model=NotificationTestResult)
def test_channel(
    channel_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Send a test notification to a saved channel."""
    channel = _get_channel(db, channel_id, current_user.id)
    url = _decrypt_channel_url(channel)
    if url is None:
        return NotificationTestResult(ok=False, error="Could not decrypt the stored webhook URL")
    return _test_result(channel.channel_type, url)


# Rules


def _rule_out(
    rule: db_models.NotificationRule,
    channel: db_models.NotificationChannel | None,
    registration: db_models.FlowRegistration | None,
    schedule: db_models.FlowSchedule | None,
) -> NotificationRuleOut:
    return NotificationRuleOut(
        id=rule.id,
        owner_id=rule.owner_id,
        channel_id=rule.channel_id,
        channel_name=channel.name if channel else None,
        channel_type=channel.channel_type if channel else None,
        registration_id=rule.registration_id,
        flow_name=registration.name if registration else None,
        schedule_id=rule.schedule_id,
        schedule_name=schedule.name if schedule else None,
        on_failure=rule.on_failure,
        on_success=rule.on_success,
        on_recovery=rule.on_recovery,
        enabled=rule.enabled,
        created_at=rule.created_at,
        updated_at=rule.updated_at,
    )


def _rules_out(db: Session, rules: list[db_models.NotificationRule]) -> list[NotificationRuleOut]:
    """Resolve the display names for a batch of rules in three lookups.

    Referents can have been deleted out from under a rule (a rule survives its flow),
    so every name is optional.
    """
    if not rules:
        return []

    def _by_id(model, ids: set[int]) -> dict[int, object]:
        if not ids:
            return {}
        return {row.id: row for row in db.query(model).filter(model.id.in_(ids)).all()}

    channels = _by_id(db_models.NotificationChannel, {r.channel_id for r in rules})
    registrations = _by_id(db_models.FlowRegistration, {r.registration_id for r in rules if r.registration_id})
    schedules = _by_id(db_models.FlowSchedule, {r.schedule_id for r in rules if r.schedule_id})

    return [
        _rule_out(
            rule,
            channels.get(rule.channel_id),
            registrations.get(rule.registration_id),
            schedules.get(rule.schedule_id),
        )
        for rule in rules
    ]


def _require_owned_channel(db: Session, channel_id: int, owner_id: int) -> None:
    """A rule may only point at a channel its owner holds. 422 (not 404): the id came
    from the request body, so it is a validation problem with the rule being written."""
    exists = (
        db.query(db_models.NotificationChannel.id)
        .filter(
            db_models.NotificationChannel.id == channel_id,
            db_models.NotificationChannel.owner_id == owner_id,
        )
        .first()
    )
    if exists is None:
        raise HTTPException(status_code=422, detail="Channel not found")


@router.get("/rules", response_model=list[NotificationRuleOut])
def list_rules(
    registration_id: int | None = None,
    schedule_id: int | None = None,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """List the current user's rules, optionally narrowed to one scope.

    ``schedule_id`` returns exactly that schedule's rules. ``registration_id`` returns
    the flow-level rules only (``schedule_id IS NULL``) — a rule attached to one of the
    flow's schedules belongs to that schedule's list, not the flow's.
    """
    query = db.query(db_models.NotificationRule).filter(db_models.NotificationRule.owner_id == current_user.id)
    if schedule_id is not None:
        query = query.filter(db_models.NotificationRule.schedule_id == schedule_id)
    if registration_id is not None:
        query = query.filter(
            db_models.NotificationRule.registration_id == registration_id,
            db_models.NotificationRule.schedule_id.is_(None),
        )
    return _rules_out(db, query.order_by(db_models.NotificationRule.id).all())


@router.post("/rules", response_model=NotificationRuleOut, status_code=status.HTTP_201_CREATED)
def create_rule(
    body: NotificationRuleCreate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Create a rule. Both scope keys set is rejected: the two are alternatives, and
    silently letting the schedule win would hide the flow-level rule the user asked for."""
    if body.schedule_id is not None and body.registration_id is not None:
        raise HTTPException(status_code=422, detail="Provide either schedule_id or registration_id, not both")

    _require_owned_channel(db, body.channel_id, current_user.id)

    if body.schedule_id is not None:
        owned_schedule = (
            db.query(db_models.FlowSchedule.id)
            .filter(
                db_models.FlowSchedule.id == body.schedule_id,
                db_models.FlowSchedule.owner_id == current_user.id,
            )
            .first()
        )
        if owned_schedule is None:
            raise HTTPException(status_code=422, detail="Schedule not found")

    if body.registration_id is not None:
        owned_registration = (
            db.query(db_models.FlowRegistration.id)
            .filter(
                db_models.FlowRegistration.id == body.registration_id,
                db_models.FlowRegistration.owner_id == current_user.id,
            )
            .first()
        )
        if owned_registration is None:
            raise HTTPException(status_code=422, detail="Flow not found")

    rule = db_models.NotificationRule(
        owner_id=current_user.id,
        channel_id=body.channel_id,
        registration_id=body.registration_id,
        schedule_id=body.schedule_id,
        on_failure=body.on_failure,
        on_success=body.on_success,
        on_recovery=body.on_recovery,
        enabled=body.enabled,
    )
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return _rules_out(db, [rule])[0]


@router.put("/rules/{rule_id}", response_model=NotificationRuleOut)
def update_rule(
    rule_id: int,
    body: NotificationRuleUpdate,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Update a rule's channel and event toggles. Its scope is immutable."""
    rule = _get_rule(db, rule_id, current_user.id)

    if body.channel_id is not None:
        _require_owned_channel(db, body.channel_id, current_user.id)
        rule.channel_id = body.channel_id
    if body.on_failure is not None:
        rule.on_failure = body.on_failure
    if body.on_success is not None:
        rule.on_success = body.on_success
    if body.on_recovery is not None:
        rule.on_recovery = body.on_recovery
    if body.enabled is not None:
        rule.enabled = body.enabled
    rule.updated_at = _utcnow()

    db.commit()
    db.refresh(rule)
    return _rules_out(db, [rule])[0]


@router.delete("/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_rule(
    rule_id: int,
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Delete a rule. Its outbox rows stay as delivery history."""
    rule = _get_rule(db, rule_id, current_user.id)
    db.delete(rule)
    db.commit()


# History


@router.get("/history", response_model=list[NotificationHistoryItem])
def list_history(
    limit: int = Query(50, ge=1, le=MAX_HISTORY_LIMIT),
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """Recent delivery attempts for the current user, newest first.

    Ownership is resolved through the channel — snapshotted on every row, owner-scoped,
    and the row's lifetime anchor (outbox rows are deleted with their channel). The rule
    is deliberately not consulted: rules are deletable independently, and a future rule
    reusing a deleted rule's SQLite rowid must not adopt another owner's history.
    """
    rows = (
        db.query(db_models.NotificationOutbox, db_models.NotificationChannel)
        .join(
            db_models.NotificationChannel,
            db_models.NotificationChannel.id == db_models.NotificationOutbox.channel_id,
        )
        .filter(db_models.NotificationChannel.owner_id == current_user.id)
        .order_by(db_models.NotificationOutbox.id.desc())
        .limit(limit)
        .all()
    )

    run_ids = {row.run_id for row, _ in rows if row.run_id is not None}
    flow_names = (
        dict(
            db.query(db_models.FlowRun.id, db_models.FlowRun.flow_name).filter(db_models.FlowRun.id.in_(run_ids)).all()
        )
        if run_ids
        else {}
    )

    return [
        NotificationHistoryItem(
            id=row.id,
            event_type=row.event_type,
            run_id=row.run_id,
            flow_name=flow_names.get(row.run_id),
            channel_name=channel.name if channel else None,
            status=row.status,
            attempts=row.attempts,
            last_error=row.last_error,
            created_at=row.created_at,
            sent_at=row.sent_at,
        )
        for row, channel in rows
    ]
