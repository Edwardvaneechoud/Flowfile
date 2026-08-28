"""Outbox evaluation: which runs produce which events, for which rules, exactly once."""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import pytest

from shared.models import (
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    NotificationChannel,
    NotificationOutbox,
    NotificationRule,
)
from shared.notifications import crypto, processor
from shared.notifications.processor import evaluate_completed_runs

OWNER = 1
NOW = datetime(2026, 8, 24, 12, 0, 0)

NODE_RESULTS = json.dumps(
    [
        {"node_id": 1, "node_name": "read_csv", "success": True, "error": None},
        {"node_id": 2, "node_name": "join", "success": False, "error": "column 'id' not found"},
        {"node_id": 3, "node_name": "write", "success": False, "error": None},
    ]
)


def _channel(db, *, owner_id: int = OWNER, enabled: bool = True) -> NotificationChannel:
    channel = NotificationChannel(
        owner_id=owner_id,
        name="ops",
        channel_type="slack",
        webhook_url_encrypted=crypto.encrypt_secret("https://93.184.216.34/hook", owner_id),
        enabled=enabled,
        created_at=NOW,
        updated_at=NOW,
    )
    db.add(channel)
    db.commit()
    db.refresh(channel)
    return channel


def _rule(db, channel, **kwargs) -> NotificationRule:
    defaults = dict(
        owner_id=OWNER,
        channel_id=channel.id,
        on_failure=True,
        on_success=False,
        on_recovery=True,
        enabled=True,
        created_at=NOW,
        updated_at=NOW,
    )
    defaults.update(kwargs)
    rule = NotificationRule(**defaults)
    db.add(rule)
    db.commit()
    db.refresh(rule)
    return rule


def _registration(db, name: str = "nightly etl") -> FlowRegistration:
    reg = FlowRegistration(name=name, flow_path=f"/tmp/{name}.flowfile", owner_id=OWNER)
    db.add(reg)
    db.commit()
    db.refresh(reg)
    return reg


def _schedule(db, reg, name: str = "nightly 2am") -> FlowSchedule:
    sched = FlowSchedule(
        registration_id=reg.id,
        owner_id=OWNER,
        enabled=True,
        name=name,
        schedule_type="cron",
        created_at=NOW,
        updated_at=NOW,
    )
    db.add(sched)
    db.commit()
    db.refresh(sched)
    return sched


def _run(db, **kwargs) -> FlowRun:
    defaults = dict(
        flow_name="nightly etl",
        user_id=OWNER,
        started_at=NOW - timedelta(minutes=5),
        ended_at=NOW,
        success=False,
        run_type="scheduled",
        nodes_completed=1,
        number_of_nodes=3,
        duration_seconds=300.0,
    )
    defaults.update(kwargs)
    run = FlowRun(**defaults)
    db.add(run)
    db.commit()
    db.refresh(run)
    return run


def _outbox(db) -> list[NotificationOutbox]:
    return db.query(NotificationOutbox).order_by(NotificationOutbox.id).all()


@pytest.fixture(autouse=True)
def _pin_now(monkeypatch):
    monkeypatch.setattr(processor, "_utcnow", lambda: NOW)


def test_failed_run_enqueues_run_failed_with_payload(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        reg = _registration(db)
        sched = _schedule(db, reg)
        _rule(db, channel, schedule_id=sched.id)
        _run(db, registration_id=reg.id, schedule_id=sched.id, node_results_json=NODE_RESULTS)

        assert evaluate_completed_runs(db) == 1

        rows = _outbox(db)
        assert len(rows) == 1
        assert rows[0].event_type == "run_failed"
        assert rows[0].status == "pending"
        assert rows[0].next_attempt_at is None

        payload = json.loads(rows[0].payload_json)
        assert payload["flow_name"] == "nightly etl"
        assert payload["schedule_name"] == "nightly 2am"
        assert payload["nodes_completed"] == 1
        assert payload["number_of_nodes"] == 3
        # The success=False node without an error is not a reportable failure.
        assert payload["failed_nodes"] == [
            {"node_id": 2, "node_name": "join", "error": "column 'id' not found"}
        ]


def test_run_is_stamped_even_without_a_matching_rule(session_factory):
    with session_factory() as db:
        reg = _registration(db)
        run = _run(db, registration_id=reg.id)

        assert evaluate_completed_runs(db) == 1
        assert _outbox(db) == []
        assert db.get(FlowRun, run.id).notification_processed_at == NOW


def test_success_run_enqueues_run_success(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        reg = _registration(db)
        _rule(db, channel, registration_id=reg.id, on_success=True)
        _run(db, registration_id=reg.id, success=True)

        assert evaluate_completed_runs(db) == 1
        assert [r.event_type for r in _outbox(db)] == ["run_success"]


def test_success_after_failure_also_enqueues_run_recovered(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        reg = _registration(db)
        _rule(db, channel, registration_id=reg.id, on_success=True)
        _run(db, registration_id=reg.id, success=False, notification_processed_at=NOW)
        _run(db, registration_id=reg.id, success=True)

        assert evaluate_completed_runs(db) == 1
        assert [r.event_type for r in _outbox(db)] == ["run_success", "run_recovered"]


def test_success_after_success_does_not_enqueue_run_recovered(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        reg = _registration(db)
        _rule(db, channel, registration_id=reg.id, on_success=True)
        _run(db, registration_id=reg.id, success=True, notification_processed_at=NOW)
        _run(db, registration_id=reg.id, success=True)

        assert evaluate_completed_runs(db) == 1
        assert [r.event_type for r in _outbox(db)] == ["run_success"]


def test_recovery_is_skipped_when_the_rule_disables_it(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        reg = _registration(db)
        _rule(db, channel, registration_id=reg.id, on_success=True, on_recovery=False)
        _run(db, registration_id=reg.id, success=False, notification_processed_at=NOW)
        _run(db, registration_id=reg.id, success=True)

        evaluate_completed_runs(db)
        assert [r.event_type for r in _outbox(db)] == ["run_success"]


def test_registration_scoped_rule_ignores_other_flows(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        watched = _registration(db, "watched")
        other = _registration(db, "other")
        _rule(db, channel, registration_id=watched.id)
        _run(db, registration_id=other.id)

        assert evaluate_completed_runs(db) == 1
        assert _outbox(db) == []


def test_schedule_scoped_rule_ignores_runs_of_other_schedules(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        reg = _registration(db)
        sched = _schedule(db, reg)
        _rule(db, channel, schedule_id=sched.id)
        _run(db, registration_id=reg.id, schedule_id=sched.id + 1)

        assert evaluate_completed_runs(db) == 1
        assert _outbox(db) == []


def test_global_rule_matches_only_the_owners_runs(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        _rule(db, channel)
        _run(db, user_id=OWNER)
        _run(db, user_id=OWNER + 1)

        assert evaluate_completed_runs(db) == 2
        rows = _outbox(db)
        assert len(rows) == 1
        assert json.loads(rows[0].payload_json)["run_id"] is not None


def test_disabled_rule_and_disabled_channel_are_ignored(session_factory):
    with session_factory() as db:
        live = _channel(db)
        muted = _channel(db, enabled=False)
        _rule(db, live, enabled=False)
        _rule(db, muted)
        _run(db)

        assert evaluate_completed_runs(db) == 1
        assert _outbox(db) == []


def test_in_designer_runs_are_never_evaluated(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        _rule(db, channel)
        run = _run(db, run_type="in_designer_run")

        assert evaluate_completed_runs(db) == 0
        assert _outbox(db) == []
        assert db.get(FlowRun, run.id).notification_processed_at is None


def test_old_runs_are_stamped_without_alerting(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        _rule(db, channel)
        stale = NOW - timedelta(seconds=processor.MAX_EVENT_AGE_SECONDS + 60)
        run = _run(db, started_at=stale - timedelta(minutes=1), ended_at=stale)

        assert evaluate_completed_runs(db) == 1
        assert _outbox(db) == []
        assert db.get(FlowRun, run.id).notification_processed_at == NOW


def test_unfinished_runs_are_left_alone(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        _rule(db, channel)
        _run(db, ended_at=None, success=None)

        assert evaluate_completed_runs(db) == 0
        assert _outbox(db) == []


def test_second_evaluation_enqueues_nothing_new(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        reg = _registration(db)
        _rule(db, channel, registration_id=reg.id)
        _run(db, registration_id=reg.id)

        assert evaluate_completed_runs(db) == 1
        assert evaluate_completed_runs(db) == 0
        assert len(_outbox(db)) == 1


def test_reevaluating_a_cleared_stamp_does_not_duplicate(session_factory):
    """The unique key is (rule, run, event) — a re-stamped run must not double-alert."""
    with session_factory() as db:
        channel = _channel(db)
        reg = _registration(db)
        _rule(db, channel, registration_id=reg.id)
        run = _run(db, registration_id=reg.id)

        evaluate_completed_runs(db)
        db.get(FlowRun, run.id).notification_processed_at = None
        db.commit()
        evaluate_completed_runs(db)

        assert len(_outbox(db)) == 1


def test_payload_omits_a_log_path_that_does_not_exist(session_factory):
    with session_factory() as db:
        channel = _channel(db)
        _rule(db, channel)
        _run(db)

        evaluate_completed_runs(db)
        payload = json.loads(_outbox(db)[0].payload_json)
        assert payload["started_at"] is not None
        assert payload["ended_at"] is not None
        assert payload["log_path"] is None
