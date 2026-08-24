"""The reaper must enqueue the orphan alert in the same transaction that closes the run."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone

from shared.models import FlowRegistration, FlowRun, NotificationChannel, NotificationOutbox, NotificationRule
from shared.notifications import crypto
from shared.run_completion import reap_orphaned_runs

OWNER = 1


def _dead_pid() -> int:
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def _seed_rule(db) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    channel = NotificationChannel(
        owner_id=OWNER,
        name="ops",
        channel_type="slack",
        webhook_url_encrypted=crypto.encrypt_secret("https://93.184.216.34/hook", OWNER),
        enabled=True,
        created_at=now,
        updated_at=now,
    )
    db.add(channel)
    db.commit()
    db.refresh(channel)
    db.add(NotificationRule(owner_id=OWNER, channel_id=channel.id, created_at=now, updated_at=now))
    db.commit()


def _orphan(db, **kwargs) -> FlowRun:
    reg = FlowRegistration(name="nightly etl", flow_path="/tmp/etl.flowfile", owner_id=OWNER)
    db.add(reg)
    db.commit()
    db.refresh(reg)

    defaults = dict(
        registration_id=reg.id,
        flow_name="nightly etl",
        user_id=OWNER,
        started_at=datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=30),
        run_type="scheduled",
        pid=_dead_pid(),
    )
    defaults.update(kwargs)
    run = FlowRun(**defaults)
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def test_reaping_an_orphan_enqueues_run_orphaned(session_factory):
    with session_factory() as db:
        _seed_rule(db)
        run = _orphan(db)

    assert reap_orphaned_runs() == 1

    with session_factory() as db:
        rows = db.query(NotificationOutbox).all()
        assert len(rows) == 1
        assert rows[0].event_type == "run_orphaned"
        assert rows[0].run_id == run.id

        payload = json.loads(rows[0].payload_json)
        assert payload["flow_name"] == "nightly etl"
        assert "no longer alive" in payload["reason"]
        # Built after the close, so the terminal fields are in the alert.
        assert payload["success"] is False
        assert payload["ended_at"] is not None

        assert db.get(FlowRun, run.id).notification_processed_at is not None


def test_pidless_orphan_past_grace_reports_its_reason(session_factory):
    stale = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=30)
    with session_factory() as db:
        _seed_rule(db)
        _orphan(db, pid=None, started_at=stale)

    assert reap_orphaned_runs() == 1

    with session_factory() as db:
        payload = json.loads(db.query(NotificationOutbox).one().payload_json)
        assert "no pid recorded" in payload["reason"]


def test_a_reaped_run_is_not_alerted_twice_by_the_evaluator(session_factory):
    from shared.notifications.processor import evaluate_completed_runs

    with session_factory() as db:
        _seed_rule(db)
        _orphan(db)

    assert reap_orphaned_runs() == 1

    with session_factory() as db:
        assert evaluate_completed_runs(db) == 0
        assert [r.event_type for r in db.query(NotificationOutbox).all()] == ["run_orphaned"]


def test_reaping_still_works_without_any_rules(session_factory):
    with session_factory() as db:
        _orphan(db)

    assert reap_orphaned_runs() == 1

    with session_factory() as db:
        assert db.query(NotificationOutbox).count() == 0
