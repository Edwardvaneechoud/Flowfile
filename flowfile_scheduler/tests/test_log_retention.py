"""Tests for the throttled log-retention sweep on the scheduler tick.

A standalone scheduler never runs core's startup sweep, so the tick is the only
place its logs get expired — but the tick runs every 30s, so the sweep is
throttled to once per ``LOG_SWEEP_INTERVAL``. The throttle is driven through the
instance's ``_last_log_sweep`` stamp rather than by patching ``time.monotonic``,
which is shared with sqlalchemy's pool.
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from flowfile_scheduler import engine as engine_mod
from flowfile_scheduler.engine import LOG_SWEEP_INTERVAL, FlowScheduler
from shared import run_completion
from shared.models import FlowRegistration, FlowSchedule
from shared.run_logs import cleanup_old_logs
from shared.notifications import processor as notifications
from shared.storage_config import storage

BASE_TS = datetime(2026, 5, 25, 9, 0, 0)


@pytest.fixture
def sched(tmp_path, monkeypatch):
    """A FlowScheduler bound to a throwaway SQLite DB with spawning stubbed."""
    url = f"sqlite:///{tmp_path / 'sched.db'}"
    monkeypatch.setattr(engine_mod, "get_database_url", lambda: url)
    monkeypatch.setattr(run_completion, "get_database_url", lambda: url)
    monkeypatch.setattr(notifications, "get_database_url", lambda: url)
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


def test_first_tick_sweeps_on_a_freshly_booted_host(sched, sweeps, monkeypatch):
    """time.monotonic() is seconds-since-boot on Linux.

    A 0.0 seed makes the first sweep wait until uptime exceeds LOG_SWEEP_INTERVAL,
    so on a fresh CI VM the standalone scheduler never expires its logs for an hour.
    """
    # Scoped to engine's namespace (its only `time` use is monotonic) so
    # sqlalchemy's pool keeps the real clock.
    monkeypatch.setattr(engine_mod, "time", SimpleNamespace(monotonic=lambda: 120.0))
    assert sched._last_log_sweep is None

    sched._tick()

    assert len(sweeps) == 1
    assert sched._last_log_sweep == 120.0


def test_suite_storage_is_isolated_from_the_real_logs_dir():
    """The conftest redirect is load-bearing: tests below drive ``_tick``, which unlinks logs."""
    logs_dir = storage.logs_directory
    assert Path.home() not in logs_dir.parents
    assert logs_dir != Path.home() / ".flowfile" / "logs"


def test_tick_sweep_only_touches_the_redirected_logs_dir(sched, monkeypatch):
    """End-to-end: a real (unstubbed) sweep on tick expires only isolated files."""
    monkeypatch.delenv("FLOWFILE_RUN_LOG_RETENTION_DAYS", raising=False)
    logs_dir = storage.logs_directory
    logs_dir.mkdir(parents=True, exist_ok=True)
    stale = logs_dir / "scheduled_run_424242.log"
    stale.write_text("stale")
    old = time.time() - 400 * 86400
    os.utime(stale, (old, old))

    assert cleanup_old_logs is engine_mod.cleanup_old_logs
    sched._tick()

    assert not stale.exists()


def test_tick_survives_sweep_failure(sched, monkeypatch):
    def boom():
        raise RuntimeError("sweep exploded")

    monkeypatch.setattr(engine_mod, "cleanup_old_logs", boom)
    _seed_due_interval_schedule(sched)

    sched._tick()

    assert len(sched.spawned) == 1
