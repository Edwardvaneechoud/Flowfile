"""Tests for cron schedule evaluation in the FlowScheduler engine.

These exercise ``_process_cron_schedules`` directly against a temp SQLite
database, with ``_utcnow`` pinned and ``_spawn_flow`` stubbed so no real
subprocess is launched. They cover due/not-due evaluation, timezone (incl.
DST) semantics, fire-once catch-up, and graceful skipping of disabled /
malformed schedules.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from flowfile_scheduler import engine as engine_mod
from flowfile_scheduler.engine import FlowScheduler
from shared.models import FlowRegistration, FlowRun, FlowSchedule


@pytest.fixture
def sched(tmp_path, monkeypatch):
    """A FlowScheduler bound to a throwaway SQLite DB with subprocess spawning stubbed."""
    url = f"sqlite:///{tmp_path / 'sched.db'}"
    monkeypatch.setattr(engine_mod, "get_database_url", lambda: url)
    s = FlowScheduler(poll_interval=1)
    s.spawned: list[tuple[str, int]] = []
    monkeypatch.setattr(
        s, "_spawn_flow", lambda flow_path, run_id: (s.spawned.append((flow_path, run_id)) or 4242)
    )
    return s


def _set_now(monkeypatch, dt: datetime) -> None:
    monkeypatch.setattr(engine_mod, "_utcnow", lambda: dt)


def _seed(
    sched: FlowScheduler,
    *,
    cron_expression: str | None = "*/15 * * * *",
    cron_timezone: str | None = "UTC",
    enabled: bool = True,
    last_triggered_at: datetime | None = None,
    created_at: datetime | None = None,
) -> int:
    """Insert a flow registration + cron schedule; return the registration id."""
    default_ts = created_at or datetime(2026, 5, 25, 9, 0, 0)
    with sched._session_factory() as db:
        reg = FlowRegistration(name="flow", flow_path="/tmp/flow.flowfile", owner_id=1)
        db.add(reg)
        db.commit()
        db.refresh(reg)
        schedule = FlowSchedule(
            registration_id=reg.id,
            owner_id=1,
            enabled=enabled,
            schedule_type="cron",
            cron_expression=cron_expression,
            cron_timezone=cron_timezone,
            last_triggered_at=last_triggered_at,
            created_at=default_ts,
            updated_at=default_ts,
        )
        db.add(schedule)
        db.commit()
        return reg.id


def _run_tick(sched: FlowScheduler) -> int:
    with sched._session_factory() as db:
        return sched._process_cron_schedules(db)


def _runs(sched: FlowScheduler, registration_id: int) -> list[FlowRun]:
    with sched._session_factory() as db:
        return db.query(FlowRun).filter_by(registration_id=registration_id).all()


def test_cron_due_launches(sched, monkeypatch):
    reg_id = _seed(sched, cron_expression="*/15 * * * *", last_triggered_at=datetime(2026, 5, 25, 10, 0, 0))
    _set_now(monkeypatch, datetime(2026, 5, 25, 10, 16, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 1
    assert len(_runs(sched, reg_id)) == 1
    assert sched.spawned  # subprocess spawn attempted


def test_cron_not_due(sched, monkeypatch):
    reg_id = _seed(sched, cron_expression="0 2 * * *", last_triggered_at=datetime(2026, 5, 25, 10, 0, 0))
    _set_now(monkeypatch, datetime(2026, 5, 25, 10, 16, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 0
    assert _runs(sched, reg_id) == []


def test_first_run_uses_created_at(sched, monkeypatch):
    # Never triggered → base is created_at; next minute boundary is already past.
    reg_id = _seed(
        sched,
        cron_expression="* * * * *",
        last_triggered_at=None,
        created_at=datetime(2026, 5, 25, 9, 58, 0),
    )
    _set_now(monkeypatch, datetime(2026, 5, 25, 10, 0, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 1
    assert len(_runs(sched, reg_id)) == 1


def test_cron_timezone_due(sched, monkeypatch):
    # "0 2 * * *" Amsterdam. now = 02:30 CEST → 02:00 already passed today.
    _seed(
        sched,
        cron_expression="0 2 * * *",
        cron_timezone="Europe/Amsterdam",
        last_triggered_at=datetime(2026, 5, 24, 0, 0, 0),
    )
    _set_now(monkeypatch, datetime(2026, 5, 25, 0, 30, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 1


def test_cron_timezone_not_due(sched, monkeypatch):
    # now = 01:30 CEST → 02:00 today not reached yet.
    _seed(
        sched,
        cron_expression="0 2 * * *",
        cron_timezone="Europe/Amsterdam",
        last_triggered_at=datetime(2026, 5, 24, 3, 0, 0),
    )
    _set_now(monkeypatch, datetime(2026, 5, 24, 23, 30, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 0


def test_catch_up_fires_once(sched, monkeypatch):
    # Scheduler "down" for 3 days; a daily cron should fire exactly once, not backfill.
    reg_id = _seed(
        sched,
        cron_expression="0 2 * * *",
        last_triggered_at=datetime(2026, 5, 22, 2, 0, 0),
    )
    _set_now(monkeypatch, datetime(2026, 5, 25, 10, 0, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 1
    # Same pinned "now": an active run now exists, so a second pass must not re-fire.
    assert _run_tick(sched) == 0
    assert len(_runs(sched, reg_id)) == 1


def test_disabled_schedule_skipped(sched, monkeypatch):
    reg_id = _seed(sched, enabled=False, cron_expression="* * * * *", last_triggered_at=datetime(2020, 1, 1, 0, 0, 0))
    _set_now(monkeypatch, datetime(2026, 5, 25, 10, 0, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 0
    assert _runs(sched, reg_id) == []


def test_missing_expression_skipped(sched, monkeypatch):
    reg_id = _seed(sched, cron_expression=None, last_triggered_at=datetime(2020, 1, 1, 0, 0, 0))
    _set_now(monkeypatch, datetime(2026, 5, 25, 10, 0, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 0
    assert _runs(sched, reg_id) == []


def test_invalid_expression_skipped(sched, monkeypatch):
    reg_id = _seed(sched, cron_expression="nonsense", last_triggered_at=datetime(2020, 1, 1, 0, 0, 0))
    _set_now(monkeypatch, datetime(2026, 5, 25, 10, 0, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 0  # no exception raised, just skipped
    assert _runs(sched, reg_id) == []


def test_invalid_timezone_skipped(sched, monkeypatch):
    reg_id = _seed(
        sched,
        cron_expression="* * * * *",
        cron_timezone="Not/AZone",
        last_triggered_at=datetime(2020, 1, 1, 0, 0, 0),
    )
    _set_now(monkeypatch, datetime(2026, 5, 25, 10, 0, 0, tzinfo=timezone.utc))
    assert _run_tick(sched) == 0
    assert _runs(sched, reg_id) == []
