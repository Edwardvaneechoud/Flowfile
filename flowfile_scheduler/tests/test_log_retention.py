"""Tests for the throttled log-retention sweep on the scheduler tick.

A standalone scheduler never runs core's startup sweep, so the tick is the only
place its logs get expired — but the tick runs every 30s, so the sweep is
throttled to once per ``LOG_SWEEP_INTERVAL``. The throttle is driven through the
instance's ``_last_log_sweep`` stamp rather than by patching ``time.monotonic``,
which is shared with sqlalchemy's pool.
"""

from __future__ import annotations

import time
from datetime import datetime

import pytest

from flowfile_scheduler import engine as engine_mod
from flowfile_scheduler.engine import LOG_SWEEP_INTERVAL, FlowScheduler
from shared import run_completion
from shared.models import FlowRegistration, FlowSchedule

BASE_TS = datetime(2026, 5, 25, 9, 0, 0)


@pytest.fixture
def sched(tmp_path, monkeypatch):
    """A FlowScheduler bound to a throwaway SQLite DB with spawning stubbed."""
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
def sweeps(monkeypatch) -> list[int]:
    """Replace the retention sweep with a counting stub."""
    calls: list[int] = []
    monkeypatch.setattr(engine_mod, "cleanup_old_logs", lambda: calls.append(1))
    return calls


def _seed_due_interval_schedule(sched: FlowScheduler) -> None:
    with sched._session_factory() as db:
        reg = FlowRegistration(name="flow", flow_path="/tmp/flow.flowfile", owner_id=1)
        db.add(reg)
        db.commit()
        db.refresh(reg)
        db.add(
            FlowSchedule(
                registration_id=reg.id,
                owner_id=1,
                enabled=True,
                schedule_type="interval",
                interval_seconds=60,
                last_triggered_at=None,
                created_at=BASE_TS,
                updated_at=BASE_TS,
            )
        )
        db.commit()


def test_tick_sweeps_logs_once_per_interval(sched, sweeps):
    sched._tick()
    sched._tick()
    sched._tick()
    assert len(sweeps) == 1

    sched._last_log_sweep = time.monotonic() - LOG_SWEEP_INTERVAL - 1
    sched._tick()
    assert len(sweeps) == 2


def test_tick_survives_sweep_failure(sched, monkeypatch):
    def boom():
        raise RuntimeError("sweep exploded")

    monkeypatch.setattr(engine_mod, "cleanup_old_logs", boom)
    _seed_due_interval_schedule(sched)

    sched._tick()

    assert len(sched.spawned) == 1
