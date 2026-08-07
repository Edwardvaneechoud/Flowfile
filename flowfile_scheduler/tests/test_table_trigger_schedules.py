"""Tests for table-trigger schedule evaluation and orphaned-run reaping.

These exercise ``_process_table_trigger_schedules`` /
``_process_table_set_trigger_schedules`` and ``shared.run_completion.reap_orphaned_runs``
directly against a temp SQLite database, with ``_utcnow`` pinned and ``_spawn_flow``
stubbed so no real subprocess is launched. They pin the arm-don't-replay rule for a
NULL watermark, watermark advancement per launch outcome, and which unfinished runs
the reaper may close.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import pytest

from flowfile_scheduler import engine as engine_mod
from flowfile_scheduler.engine import FlowScheduler
from shared import run_completion
from shared.models import (
    CatalogTable,
    FlowRegistration,
    FlowRun,
    FlowSchedule,
    ScheduleTriggerTable,
)
from shared.run_completion import reap_orphaned_runs

BASE_TS = datetime(2026, 5, 25, 9, 0, 0)
NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def sched(tmp_path, monkeypatch):
    """A FlowScheduler bound to a throwaway SQLite DB with subprocess spawning stubbed."""
    url = f"sqlite:///{tmp_path / 'sched.db'}"
    monkeypatch.setattr(engine_mod, "get_database_url", lambda: url)
    monkeypatch.setattr(run_completion, "get_database_url", lambda: url)
    s = FlowScheduler(poll_interval=1)
    s.spawned: list[tuple[str, int]] = []
    monkeypatch.setattr(
        s, "_spawn_flow", lambda flow_path, run_id: (s.spawned.append((flow_path, run_id)) or 4242)
    )
    return s


@pytest.fixture
def sleeper():
    """Factory for long-running child processes, killed at teardown."""
    procs: list[subprocess.Popen] = []

    def _spawn() -> subprocess.Popen:
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True)
        procs.append(proc)
        return proc

    yield _spawn

    for proc in procs:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


def _set_now(monkeypatch, dt: datetime = NOW) -> None:
    monkeypatch.setattr(engine_mod, "_utcnow", lambda: dt)


def _add_registration(db) -> FlowRegistration:
    reg = FlowRegistration(name="flow", flow_path="/tmp/flow.flowfile", owner_id=1)
    db.add(reg)
    db.commit()
    db.refresh(reg)
    return reg


def _seed(
    sched: FlowScheduler,
    *,
    table_updated_at: datetime = BASE_TS,
    last_seen: datetime | None = None,
    enabled: bool = True,
) -> tuple[int, int, int]:
    """Insert a registration + table + table_trigger schedule.

    Returns ``(registration_id, table_id, schedule_id)``.
    """
    with sched._session_factory() as db:
        reg = _add_registration(db)
        table = CatalogTable(name="watched", updated_at=table_updated_at)
        db.add(table)
        db.commit()
        db.refresh(table)
        schedule = FlowSchedule(
            registration_id=reg.id,
            owner_id=1,
            enabled=enabled,
            schedule_type="table_trigger",
            trigger_table_id=table.id,
            last_trigger_table_updated_at=last_seen,
            created_at=BASE_TS,
            updated_at=BASE_TS,
        )
        db.add(schedule)
        db.commit()
        return reg.id, table.id, schedule.id


def _seed_set(
    sched: FlowScheduler,
    *,
    stamps: tuple[datetime, datetime] = (BASE_TS, BASE_TS),
    last_seen: datetime | None = None,
) -> tuple[int, list[int], int]:
    """Insert a registration + two tables + table_set_trigger schedule."""
    with sched._session_factory() as db:
        reg = _add_registration(db)
        tables = []
        for idx, stamp in enumerate(stamps):
            table = CatalogTable(name=f"watched_{idx}", updated_at=stamp)
            db.add(table)
            db.commit()
            db.refresh(table)
            tables.append(table.id)
        schedule = FlowSchedule(
            registration_id=reg.id,
            owner_id=1,
            enabled=True,
            schedule_type="table_set_trigger",
            last_trigger_table_updated_at=last_seen,
            created_at=BASE_TS,
            updated_at=BASE_TS,
        )
        db.add(schedule)
        db.commit()
        db.refresh(schedule)
        for table_id in tables:
            db.add(ScheduleTriggerTable(schedule_id=schedule.id, table_id=table_id, created_at=BASE_TS))
        db.commit()
        return reg.id, tables, schedule.id


def _tick(sched: FlowScheduler) -> int:
    with sched._session_factory() as db:
        return sched._process_table_trigger_schedules(db)


def _tick_set(sched: FlowScheduler) -> int:
    with sched._session_factory() as db:
        return sched._process_table_set_trigger_schedules(db)


def _touch(sched: FlowScheduler, table_id: int, ts: datetime) -> None:
    with sched._session_factory() as db:
        db.get(CatalogTable, table_id).updated_at = ts
        db.commit()


def _watermark(sched: FlowScheduler, schedule_id: int) -> datetime | None:
    with sched._session_factory() as db:
        return db.get(FlowSchedule, schedule_id).last_trigger_table_updated_at


def _runs(sched: FlowScheduler, registration_id: int) -> list[FlowRun]:
    with sched._session_factory() as db:
        return db.query(FlowRun).filter_by(registration_id=registration_id).all()


def _end_runs(sched: FlowScheduler, registration_id: int) -> None:
    with sched._session_factory() as db:
        for run in (
            db.query(FlowRun)
            .filter(FlowRun.registration_id == registration_id, FlowRun.ended_at.is_(None))
            .all()
        ):
            run.ended_at = datetime(2026, 5, 25, 11, 0, 0)
        db.commit()


# --------------------------------------------------------------------------
# table_trigger
# --------------------------------------------------------------------------


def test_null_watermark_arms_without_launching(sched, monkeypatch):
    reg_id, _table_id, schedule_id = _seed(sched, table_updated_at=BASE_TS)
    _set_now(monkeypatch)

    assert _tick(sched) == 0
    assert _runs(sched, reg_id) == []
    assert _watermark(sched, schedule_id) == BASE_TS

    # A second tick over an unchanged table stays quiet.
    assert _tick(sched) == 0
    assert _runs(sched, reg_id) == []


def test_table_change_launches_once(sched, monkeypatch):
    reg_id, table_id, schedule_id = _seed(sched, table_updated_at=BASE_TS, last_seen=BASE_TS)
    _set_now(monkeypatch)
    changed_at = BASE_TS + timedelta(hours=1)
    _touch(sched, table_id, changed_at)

    assert _tick(sched) == 1
    assert len(_runs(sched, reg_id)) == 1
    assert _watermark(sched, schedule_id) == changed_at

    # Watermark caught up: even with the run finished, no re-fire.
    _end_runs(sched, reg_id)
    assert _tick(sched) == 0
    assert len(_runs(sched, reg_id)) == 1


def test_active_run_blocks_launch_and_keeps_watermark(sched, monkeypatch):
    reg_id, table_id, schedule_id = _seed(sched, table_updated_at=BASE_TS, last_seen=BASE_TS)
    _set_now(monkeypatch)
    changed_at = BASE_TS + timedelta(hours=1)
    _touch(sched, table_id, changed_at)

    with sched._session_factory() as db:
        db.add(
            FlowRun(
                registration_id=reg_id,
                flow_name="flow",
                user_id=1,
                started_at=BASE_TS,
                run_type="scheduled",
                pid=4242,
            )
        )
        db.commit()

    assert _tick(sched) == 0
    assert len(_runs(sched, reg_id)) == 1  # only the pre-existing active run
    assert _watermark(sched, schedule_id) == BASE_TS

    _end_runs(sched, reg_id)
    assert _tick(sched) == 1
    assert len(_runs(sched, reg_id)) == 2
    assert _watermark(sched, schedule_id) == changed_at


@pytest.mark.parametrize("mode", ["returns_none", "raises"])
def test_spawn_failure_advances_watermark(sched, monkeypatch, mode):
    def _fail(flow_path, run_id):
        if mode == "raises":
            raise OSError("boom")
        return None

    monkeypatch.setattr(sched, "_spawn_flow", _fail)
    reg_id, table_id, schedule_id = _seed(sched, table_updated_at=BASE_TS, last_seen=BASE_TS)
    _set_now(monkeypatch)
    changed_at = BASE_TS + timedelta(hours=1)
    _touch(sched, table_id, changed_at)

    assert _tick(sched) == 0
    runs = _runs(sched, reg_id)
    assert len(runs) == 1
    assert runs[0].ended_at is not None and runs[0].success is False
    assert _watermark(sched, schedule_id) == changed_at

    # A broken spawn must not retry every tick.
    assert _tick(sched) == 0
    assert len(_runs(sched, reg_id)) == 1


# --------------------------------------------------------------------------
# table_set_trigger
# --------------------------------------------------------------------------


def test_set_trigger_null_watermark_arms_without_launching(sched, monkeypatch):
    later = BASE_TS + timedelta(hours=1)
    reg_id, _tables, schedule_id = _seed_set(sched, stamps=(BASE_TS, later))
    _set_now(monkeypatch)

    assert _tick_set(sched) == 0
    assert _runs(sched, reg_id) == []
    assert _watermark(sched, schedule_id) == later  # armed at max(observed)


def test_set_trigger_requires_all_tables_to_advance(sched, monkeypatch):
    reg_id, tables, schedule_id = _seed_set(sched, stamps=(BASE_TS, BASE_TS), last_seen=BASE_TS)
    _set_now(monkeypatch)

    first_change = BASE_TS + timedelta(hours=1)
    _touch(sched, tables[0], first_change)
    assert _tick_set(sched) == 0
    assert _runs(sched, reg_id) == []
    assert _watermark(sched, schedule_id) == BASE_TS

    second_change = BASE_TS + timedelta(hours=2)
    _touch(sched, tables[1], second_change)
    assert _tick_set(sched) == 1
    assert len(_runs(sched, reg_id)) == 1
    assert _watermark(sched, schedule_id) == second_change  # max of the observed stamps

    _end_runs(sched, reg_id)
    assert _tick_set(sched) == 0


# --------------------------------------------------------------------------
# flow_uuid plumbing
# --------------------------------------------------------------------------


def test_scheduled_run_carries_flow_uuid(sched, monkeypatch):
    reg_id, table_id, _schedule_id = _seed(sched, table_updated_at=BASE_TS, last_seen=BASE_TS)
    _set_now(monkeypatch)
    _touch(sched, table_id, BASE_TS + timedelta(hours=1))

    assert _tick(sched) == 1
    with sched._session_factory() as db:
        reg = db.get(FlowRegistration, reg_id)
        run = db.query(FlowRun).filter_by(registration_id=reg_id).one()
        assert run.flow_uuid == reg.flow_uuid
        assert reg.flow_uuid


def test_has_active_run_matches_uuid_only(sched):
    with sched._session_factory() as db:
        reg = _add_registration(db)
        db.add(
            FlowRun(
                registration_id=999,  # no registration_id match
                flow_uuid=reg.flow_uuid,
                flow_name="flow",
                user_id=1,
                started_at=BASE_TS,
                run_type="scheduled",
            )
        )
        db.commit()
        assert sched._has_active_run(db, reg) is True


def test_has_active_run_matches_registration_only(sched):
    with sched._session_factory() as db:
        reg = _add_registration(db)
        db.add(
            FlowRun(
                registration_id=reg.id,
                flow_uuid=None,  # pre-fix row
                flow_name="flow",
                user_id=1,
                started_at=BASE_TS,
                run_type="scheduled",
            )
        )
        db.commit()
        assert sched._has_active_run(db, reg) is True


# --------------------------------------------------------------------------
# Orphaned-run reaper
# --------------------------------------------------------------------------


def _dead_pid() -> int:
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    proc.wait()
    return proc.pid


def _add_run(sched: FlowScheduler, **kwargs) -> int:
    defaults = dict(
        flow_name="flow",
        user_id=1,
        started_at=datetime.now(timezone.utc).replace(tzinfo=None),
        run_type="scheduled",
    )
    defaults.update(kwargs)
    with sched._session_factory() as db:
        run = FlowRun(**defaults)
        db.add(run)
        db.commit()
        db.refresh(run)
        return run.id


def _get_run(sched: FlowScheduler, run_id: int) -> FlowRun:
    with sched._session_factory() as db:
        return db.get(FlowRun, run_id)


def test_reaper_closes_dead_pid(sched):
    run_id = _add_run(sched, pid=_dead_pid())
    assert reap_orphaned_runs() == 1
    run = _get_run(sched, run_id)
    assert run.ended_at is not None
    assert run.success is False
    assert run.duration_seconds is not None


def test_reaper_harvests_zombie_child(sched):
    """The real wedge case: spawn_flow_subprocess drops the Popen handle and nobody
    wait()s, so an exited child lingers as a zombie that kill(0) reports alive."""
    # Spawned exactly like spawn_flow_subprocess: handle dropped, never waited on.
    pid = subprocess.Popen([sys.executable, "-c", "pass"], start_new_session=True).pid
    run_id = _add_run(sched, pid=pid)

    deadline = time.monotonic() + 10
    while reap_orphaned_runs() != 1:
        assert time.monotonic() < deadline, "reaper never harvested the zombie child"
        time.sleep(0.1)

    run = _get_run(sched, run_id)
    assert run.ended_at is not None
    assert run.success is False


def test_reaper_leaves_live_pid(sched):
    run_id = _add_run(sched, pid=os.getpid())
    assert reap_orphaned_runs() == 0
    assert _get_run(sched, run_id).ended_at is None


def test_reaper_closes_pidless_run_past_grace(sched):
    stale = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
    run_id = _add_run(sched, pid=None, started_at=stale)
    assert reap_orphaned_runs() == 1
    assert _get_run(sched, run_id).success is False


def test_reaper_leaves_fresh_pidless_run(sched):
    run_id = _add_run(sched, pid=None)
    assert reap_orphaned_runs() == 0
    assert _get_run(sched, run_id).ended_at is None


def test_reaper_never_touches_in_designer_runs(sched):
    ancient = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
    run_id = _add_run(sched, pid=None, started_at=ancient, run_type="in_designer_run")
    assert reap_orphaned_runs() == 0
    assert _get_run(sched, run_id).ended_at is None


def test_reaper_max_age_backstop_terminates_the_live_process(sleeper, sched):
    """Closing the row releases the double-launch guard, so the backstop must also
    kill the process — otherwise the next tick launches a duplicate alongside it."""
    proc = sleeper()
    old = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=2)
    run_id = _add_run(sched, pid=proc.pid, started_at=old)

    assert reap_orphaned_runs(max_age_seconds=1) == 1
    assert _get_run(sched, run_id).success is False
    assert proc.wait(timeout=10) is not None


def test_reaper_max_age_zero_disables_backstop(sleeper, sched):
    proc = sleeper()
    old = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)
    run_id = _add_run(sched, pid=proc.pid, started_at=old)
    assert reap_orphaned_runs(max_age_seconds=0) == 0
    assert _get_run(sched, run_id).ended_at is None
    assert proc.poll() is None


def test_reaper_reads_max_age_from_env(sleeper, sched, monkeypatch):
    proc = sleeper()
    monkeypatch.setenv("FLOWFILE_RUN_MAX_AGE_SECONDS", "60")
    old = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=2)
    run_id = _add_run(sched, pid=proc.pid, started_at=old)
    assert reap_orphaned_runs() == 1
    assert _get_run(sched, run_id).success is False


def test_reaper_does_not_clobber_a_concurrently_finished_run(sched, monkeypatch):
    run_id = _add_run(sched, pid=_dead_pid())
    ended = datetime(2026, 5, 25, 11, 0, 0)

    def _probe(pid):
        # The child's own complete_run lands between the reaper's load and its write.
        with sched._session_factory() as db:
            run = db.get(FlowRun, run_id)
            if run.ended_at is None:
                run.ended_at = ended
                run.success = True
                db.commit()
        return False

    monkeypatch.setattr(run_completion, "_pid_is_alive", _probe)

    assert reap_orphaned_runs() == 0
    run = _get_run(sched, run_id)
    assert run.success is True
    assert run.ended_at == ended


def test_tick_reaps_orphan_then_launches(sched, monkeypatch):
    """The whole wedge, end to end: a dead-pid run row blocks the active-run guard
    until the same tick's reap clears it."""
    reg_id, table_id, _schedule_id = _seed(sched, table_updated_at=BASE_TS, last_seen=BASE_TS)
    _set_now(monkeypatch)
    _touch(sched, table_id, BASE_TS + timedelta(hours=1))
    wedged_id = _add_run(sched, pid=_dead_pid(), registration_id=reg_id)

    sched._tick()

    assert _get_run(sched, wedged_id).ended_at is not None
    assert len(_runs(sched, reg_id)) == 2
    assert sched.spawned
