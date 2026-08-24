"""Outbox draining: lease claiming, retry/backoff, and the terminal failure modes."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from shared.models import NotificationChannel, NotificationOutbox, NotificationRule
from shared.notifications import crypto, processor, senders
from shared.notifications.processor import drain_outbox

OWNER = 1
NOW = datetime(2026, 8, 24, 12, 0, 0)
PUBLIC_HOOK = "https://93.184.216.34/hook"
OTHER_HOOK = "https://93.184.216.34/other-hook"


class _Response:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


@pytest.fixture(autouse=True)
def _pin_now(monkeypatch):
    monkeypatch.setattr(processor, "_utcnow", lambda: NOW)


@pytest.fixture
def posts(monkeypatch):
    """Record every outbound webhook; the queued responses drive the outcome."""
    sent: list[tuple[str, dict]] = []
    responses: list[object] = []

    def _post(url, json):
        sent.append((url, json))
        outcome = responses.pop(0) if responses else _Response(200)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(senders, "_post", _post)
    return type("Posts", (), {"sent": sent, "responses": responses})()


def _seed(
    db, *, channel_enabled: bool = True, url_encrypted: str | None = None, url: str = PUBLIC_HOOK
) -> NotificationOutbox:
    channel = NotificationChannel(
        owner_id=OWNER,
        name="ops",
        channel_type="slack",
        webhook_url_encrypted=(url_encrypted if url_encrypted is not None else crypto.encrypt_secret(url, OWNER)),
        enabled=channel_enabled,
        created_at=NOW,
        updated_at=NOW,
    )
    db.add(channel)
    db.commit()
    db.refresh(channel)

    rule = NotificationRule(owner_id=OWNER, channel_id=channel.id, created_at=NOW, updated_at=NOW)
    db.add(rule)
    db.commit()
    db.refresh(rule)

    row = NotificationOutbox(
        rule_id=rule.id,
        channel_id=channel.id,
        run_id=1,
        event_type="run_failed",
        payload_json=json.dumps({"event_type": "run_failed", "flow_name": "etl", "run_id": 1}),
        status="pending",
        attempts=0,
        created_at=NOW,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _extra_row(db, sibling: NotificationOutbox, *, run_id: int) -> NotificationOutbox:
    """A second pending row on the same rule/channel as *sibling*."""
    row = NotificationOutbox(
        rule_id=sibling.rule_id,
        channel_id=sibling.channel_id,
        run_id=run_id,
        event_type="run_failed",
        payload_json=json.dumps({"event_type": "run_failed", "flow_name": "etl", "run_id": run_id}),
        status="pending",
        attempts=0,
        created_at=NOW,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def test_successful_send_marks_the_row_sent(session_factory, posts):
    with session_factory() as db:
        row = _seed(db)

        assert drain_outbox(db) == 1

        row = db.get(NotificationOutbox, row.id)
        assert row.status == "sent"
        assert row.sent_at == NOW
        assert row.attempts == 1
        assert row.last_error is None

        url, body = posts.sent[0]
        assert url == PUBLIC_HOOK
        assert "etl" in body["text"]


def test_a_sent_row_is_not_redelivered(session_factory, posts):
    with session_factory() as db:
        _seed(db)
        drain_outbox(db)
        assert drain_outbox(db) == 0
        assert len(posts.sent) == 1


def test_failed_send_backs_off_then_dies_after_max_attempts(session_factory, posts):
    with session_factory() as db:
        row = _seed(db)

        for attempt in range(1, processor.MAX_ATTEMPTS + 1):
            posts.responses.append(_Response(500, "upstream exploded"))
            # The lease/backoff has to be in the past for the next drain to pick the row up.
            db.query(NotificationOutbox).filter(NotificationOutbox.id == row.id).update(
                {NotificationOutbox.next_attempt_at: None}
            )
            db.commit()

            assert drain_outbox(db) == 0
            row = db.get(NotificationOutbox, row.id)
            assert row.attempts == attempt
            assert "500" in row.last_error

            if attempt < processor.MAX_ATTEMPTS:
                expected = processor.BACKOFF_SCHEDULE[min(attempt - 1, len(processor.BACKOFF_SCHEDULE) - 1)]
                assert row.status == "pending"
                assert row.next_attempt_at == NOW + timedelta(seconds=expected)

        assert row.status == "dead"


def test_a_row_in_backoff_is_skipped(session_factory, posts):
    with session_factory() as db:
        row = _seed(db)
        db.query(NotificationOutbox).filter(NotificationOutbox.id == row.id).update(
            {NotificationOutbox.next_attempt_at: NOW + timedelta(minutes=5)}
        )
        db.commit()

        assert drain_outbox(db) == 0
        assert posts.sent == []


def test_transport_error_is_recorded_as_a_retry(session_factory, posts):
    with session_factory() as db:
        row = _seed(db)
        posts.responses.append(RuntimeError("connection refused"))

        assert drain_outbox(db) == 0
        row = db.get(NotificationOutbox, row.id)
        assert row.status == "pending"
        assert "connection refused" in row.last_error


def test_disabled_channel_kills_the_row(session_factory, posts):
    with session_factory() as db:
        row = _seed(db, channel_enabled=False)

        assert drain_outbox(db) == 0
        row = db.get(NotificationOutbox, row.id)
        assert row.status == "dead"
        assert "disabled" in row.last_error
        assert posts.sent == []


def test_undecryptable_url_kills_the_row(session_factory, posts):
    with session_factory() as db:
        row = _seed(db, url_encrypted="$ffsec$1$1$not-a-fernet-token")

        assert drain_outbox(db) == 0
        row = db.get(NotificationOutbox, row.id)
        assert row.status == "dead"
        assert "decrypt" in row.last_error
        assert posts.sent == []


def test_private_target_kills_the_row_at_send_time(session_factory, posts):
    with session_factory() as db:
        row = _seed(db, url_encrypted=crypto.encrypt_secret("http://127.0.0.1/hook", OWNER))

        assert drain_outbox(db) == 0
        row = db.get(NotificationOutbox, row.id)
        assert row.status == "dead"
        assert "non-public" in row.last_error
        assert posts.sent == []


def test_a_live_sending_lease_is_not_stolen(session_factory, posts):
    with session_factory() as db:
        row = _seed(db)
        db.query(NotificationOutbox).filter(NotificationOutbox.id == row.id).update(
            {
                NotificationOutbox.status: "sending",
                NotificationOutbox.attempts: 1,
                NotificationOutbox.next_attempt_at: NOW + timedelta(seconds=processor.SEND_LEASE_SECONDS),
            }
        )
        db.commit()

        assert drain_outbox(db) == 0
        assert posts.sent == []


def test_an_expired_sending_lease_is_retried(session_factory, posts):
    with session_factory() as db:
        row = _seed(db)
        db.query(NotificationOutbox).filter(NotificationOutbox.id == row.id).update(
            {
                NotificationOutbox.status: "sending",
                NotificationOutbox.attempts: 1,
                NotificationOutbox.next_attempt_at: NOW - timedelta(seconds=1),
            }
        )
        db.commit()

        assert drain_outbox(db) == 1
        assert db.get(NotificationOutbox, row.id).status == "sent"


def test_a_sending_row_out_of_attempts_is_dead_lettered(session_factory, posts):
    """Its lease-holder crashed on the final attempt: no claim filter can reach it again."""
    with session_factory() as db:
        row = _seed(db)
        db.query(NotificationOutbox).filter(NotificationOutbox.id == row.id).update(
            {
                NotificationOutbox.status: "sending",
                NotificationOutbox.attempts: processor.MAX_ATTEMPTS,
                NotificationOutbox.next_attempt_at: NOW - timedelta(seconds=1),
            }
        )
        db.commit()

        assert drain_outbox(db) == 0

        row = db.get(NotificationOutbox, row.id)
        assert row.status == "dead"
        assert row.last_error == "Delivery interrupted; attempts exhausted"
        assert posts.sent == []


def test_the_sweep_keeps_an_error_that_is_already_recorded(session_factory, posts):
    with session_factory() as db:
        row = _seed(db)
        db.query(NotificationOutbox).filter(NotificationOutbox.id == row.id).update(
            {
                NotificationOutbox.status: "sending",
                NotificationOutbox.attempts: processor.MAX_ATTEMPTS,
                NotificationOutbox.next_attempt_at: NOW - timedelta(seconds=1),
                NotificationOutbox.last_error: "HTTP 503 from the far end",
            }
        )
        db.commit()

        drain_outbox(db)

        row = db.get(NotificationOutbox, row.id)
        assert row.status == "dead"
        assert row.last_error == "HTTP 503 from the far end"


def test_a_still_leased_final_attempt_is_left_alone(session_factory, posts):
    with session_factory() as db:
        row = _seed(db)
        db.query(NotificationOutbox).filter(NotificationOutbox.id == row.id).update(
            {
                NotificationOutbox.status: "sending",
                NotificationOutbox.attempts: processor.MAX_ATTEMPTS,
                NotificationOutbox.next_attempt_at: NOW + timedelta(seconds=processor.SEND_LEASE_SECONDS),
            }
        )
        db.commit()

        assert drain_outbox(db) == 0
        assert db.get(NotificationOutbox, row.id).status == "sending"


def test_a_broken_channel_costs_one_attempt_per_pass(session_factory, posts):
    """The second row on the failing channel must not be claimed at all this pass."""
    with session_factory() as db:
        first = _seed(db)
        second = _extra_row(db, first, run_id=2)
        healthy = _seed(db, url=OTHER_HOOK)
        posts.responses.append(RuntimeError("connection refused"))

        assert drain_outbox(db) == 1

        first = db.get(NotificationOutbox, first.id)
        assert first.attempts == 1
        assert first.status == "pending"
        assert first.next_attempt_at == NOW + timedelta(seconds=processor.BACKOFF_SCHEDULE[0])

        second = db.get(NotificationOutbox, second.id)
        assert second.attempts == 0
        assert second.status == "pending"
        assert second.next_attempt_at is None

        assert db.get(NotificationOutbox, healthy.id).status == "sent"
        # One attempt at the broken endpoint, one at the healthy one.
        assert [url for url, _ in posts.sent] == [PUBLIC_HOOK, OTHER_HOOK]


def test_the_skipped_row_is_sent_on_the_next_pass(session_factory, posts):
    with session_factory() as db:
        first = _seed(db)
        second = _extra_row(db, first, run_id=2)
        posts.responses.append(RuntimeError("connection refused"))

        drain_outbox(db)
        assert drain_outbox(db) == 1

        assert db.get(NotificationOutbox, second.id).status == "sent"


def test_process_pending_notifications_evaluates_then_drains(session_factory, posts):
    """The entry point the scheduler tick and the CLI subprocess both call."""
    from shared.models import FlowRun
    from shared.notifications.processor import process_pending_notifications

    with session_factory() as db:
        row = _seed(db)
        db.delete(row)
        db.add(
            FlowRun(
                flow_name="etl",
                user_id=OWNER,
                started_at=NOW,
                ended_at=NOW,
                success=False,
                run_type="scheduled",
            )
        )
        db.commit()

    assert process_pending_notifications() == (1, 1)
    assert posts.sent[0][0] == PUBLIC_HOOK

    with session_factory() as db:
        assert db.query(NotificationOutbox).one().status == "sent"
