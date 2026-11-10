"""The scheduler's launch is a seam: a launcher its caller passes in starts the run in place of the subprocess.

A real ``FlowScheduler`` on a temp SQLite database. The launcher is a plain function that records its calls and
answers a ``Launch``; the reaper is the real one, pointed at the same database.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from flowfile_scheduler import engine as engine_mod
from flowfile_scheduler.engine import FlowScheduler, Launch
from shared import run_completion, subprocess_utils
from shared.models import FlowRegistration, FlowRun, FlowSchedule

NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
BASE_TS = datetime(2026, 5, 25, 9, 0, 0)


@pytest.fixture
def db_url(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'sched.db'}"
    monkeypatch.setattr(engine_mod, "get_database_url", lambda: url)
    monkeypatch.setattr(run_completion, "get_database_url", lambda: url)
    monkeypatch.setattr(engine_mod, "_utcnow", lambda: NOW)
    return url


def _scheduler(answer: Launch | None, calls: list) -> FlowScheduler:
    def launcher(db, schedule, registration, now):
        calls.append((schedule.id, registration.id, now))
        return answer

    return FlowScheduler(poll_interval=1, launcher=launcher if answer is not None else None)


def _seed(sched: FlowScheduler, flow_path: str = "no/such/file.flowfile") -> tuple[int, int]:
    """An interval schedule that is due, of a flow whose file need not exist."""
    with sched._session_factory() as db:
        reg = FlowRegistration(name="flow", flow_path=flow_path, owner_id=1)
        db.add(reg)
        db.commit()
        row = FlowSchedule(
            registration_id=reg.id,
            owner_id=1,
            enabled=True,
            schedule_type="interval",
            interval_seconds=60,
            backend="worker:team",
            created_at=BASE_TS,
            updated_at=BASE_TS,
        )
        db.add(row)
        db.commit()
        return row.id, reg.id


def _fire(sched: FlowScheduler) -> int:
    with sched._session_factory() as db:
        return sched._process_interval_schedules(db)


def _runs(sched: FlowScheduler) -> list[FlowRun]:
    with sched._session_factory() as db:
        return db.query(FlowRun).all()


def test_a_due_schedule_calls_the_launcher_once_with_the_schedule_and_its_flow(db_url):
    calls: list = []
    sched = _scheduler(Launch(pid=4242), calls)
    schedule_id, reg_id = _seed(sched)

    assert _fire(sched) == 1
    assert calls == [(schedule_id, reg_id, NOW)]
    (run,) = _runs(sched)
    assert (run.schedule_id, run.run_type, run.pid, run.ended_at) == (schedule_id, "scheduled", 4242, None)


def test_a_run_launched_elsewhere_stays_active_and_the_reaper_leaves_it(db_url):
    sched = _scheduler(Launch(remote_run_id="run-abc"), [])
    schedule_id, _ = _seed(sched)

    assert _fire(sched) == 1
    with sched._session_factory() as db:
        run = db.query(FlowRun).one()
        assert (run.pid, run.remote_run_id, run.schedule_id, run.ended_at) == (None, "run-abc", schedule_id, None)
        # Well past the grace a pid-less run gets, inside the age backstop.
        run.started_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)
        db.commit()

    assert run_completion.reap_orphaned_runs() == 0
    assert _runs(sched)[0].ended_at is None


def test_a_pid_less_run_with_no_run_id_elsewhere_is_still_reaped(db_url):
    sched = _scheduler(Launch(pid=1), [])
    _seed(sched)
    assert _fire(sched) == 1
    with sched._session_factory() as db:
        run = db.query(FlowRun).one()
        run.pid = None
        run.started_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=1)
        db.commit()

    assert run_completion.reap_orphaned_runs() == 1


def test_a_refusal_disables_the_schedule_with_its_reason_and_it_does_not_fire_again(db_url):
    calls: list = []
    sched = _scheduler(Launch(refused="Its owner may not run there"), calls)
    schedule_id, _ = _seed(sched)

    assert _fire(sched) == 0
    assert _fire(sched) == 0
    assert len(calls) == 1
    assert _runs(sched) == []
    with sched._session_factory() as db:
        row = db.get(FlowSchedule, schedule_id)
        assert (row.enabled, row.disabled_reason) == (False, "Its owner may not run there")


@pytest.mark.parametrize("answer", [Launch(error="no answer"), Launch(), Launch(remote_run_id="r1", error="refused")])
def test_a_failed_launch_closes_its_run_as_failed_and_keeps_the_schedule(db_url, answer):
    sched = _scheduler(answer, [])
    schedule_id, _ = _seed(sched)

    assert _fire(sched) == 0
    (run,) = _runs(sched)
    assert (run.success, run.ended_at is not None, run.remote_run_id) == (False, True, answer.remote_run_id)
    with sched._session_factory() as db:
        assert db.get(FlowSchedule, schedule_id).enabled is True


def test_no_launcher_spawns_todays_command(db_url, tmp_path, monkeypatch):
    argv: list[list[str]] = []
    monkeypatch.setattr(
        subprocess_utils,
        "subprocess",
        SimpleNamespace(Popen=lambda cmd, **_kw: argv.append(list(cmd)) or SimpleNamespace(pid=4242)),
    )
    path = tmp_path / "flow.flowfile"
    path.write_text("")
    sched = _scheduler(None, [])
    _seed(sched, str(path.resolve()))

    assert _fire(sched) == 1
    (run,) = _runs(sched)
    head = [sys.executable, "-m", "flowfile", "run", "flow"]
    if getattr(sys, "frozen", False):
        head = [sys.executable, "--run-flow"]
    assert argv == [head + [str(path.resolve()), "--run-id", str(run.id), "--backend", "worker:team"]]
    assert (run.pid, run.remote_run_id) == (4242, None)
