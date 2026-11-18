"""A schedule's backend reaches the spawned command as ``--backend``; a schedule without one spawns it unchanged.

A real ``FlowScheduler`` on a temp SQLite database with ``_spawn_flow`` left as it is: only the ``Popen`` that
``shared.subprocess_utils`` calls is captured, so the argv asserted is the one a child would get.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from flowfile_scheduler import engine as engine_mod
from flowfile_scheduler.engine import FlowScheduler
from shared import subprocess_utils
from shared.models import CatalogTable, FlowRegistration, FlowRun, FlowSchedule

BASE_TS = datetime(2026, 5, 25, 9, 0, 0)
NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def popen_calls(monkeypatch):
    calls: list[list[str]] = []

    def _popen(cmd, **_kwargs):
        calls.append(list(cmd))
        return SimpleNamespace(pid=4242)

    monkeypatch.setattr(subprocess_utils, "subprocess", SimpleNamespace(Popen=_popen))
    return calls


@pytest.fixture
def sched(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'sched.db'}"
    monkeypatch.setattr(engine_mod, "get_database_url", lambda: url)
    monkeypatch.setattr(engine_mod, "_utcnow", lambda: NOW)
    return FlowScheduler(poll_interval=1)


def _flow_path(tmp_path) -> str:
    path = tmp_path / "flow.flowfile"
    path.write_text("")
    return str(path.resolve())


def _seed(sched: FlowScheduler, flow_path: str, schedule_type: str, backend: str | None) -> tuple[int, int | None]:
    with sched._session_factory() as db:
        reg = FlowRegistration(name="flow", flow_path=flow_path, owner_id=1)
        db.add(reg)
        db.commit()
        table_id = None
        if schedule_type == "table_trigger":
            table = CatalogTable(name="watched", updated_at=BASE_TS)
            db.add(table)
            db.commit()
            table_id = table.id
        db.add(
            FlowSchedule(
                registration_id=reg.id,
                owner_id=1,
                enabled=True,
                schedule_type=schedule_type,
                interval_seconds=60 if schedule_type == "interval" else None,
                trigger_table_id=table_id,
                last_trigger_table_updated_at=BASE_TS if table_id else None,
                backend=backend,
                created_at=BASE_TS,
                updated_at=BASE_TS,
            )
        )
        db.commit()
        return reg.id, table_id


def _fire(sched: FlowScheduler, schedule_type: str, table_id: int | None) -> int:
    with sched._session_factory() as db:
        if schedule_type == "interval":
            return sched._process_interval_schedules(db)
        db.get(CatalogTable, table_id).updated_at = datetime(2026, 5, 25, 10, 0, 0)
        db.commit()
        return sched._process_table_trigger_schedules(db)


def _run_id(sched: FlowScheduler, registration_id: int) -> int:
    with sched._session_factory() as db:
        (run,) = db.query(FlowRun).filter_by(registration_id=registration_id).all()
        return run.id


def _todays_argv(flow_path: str, run_id: int) -> list[str]:
    if getattr(sys, "frozen", False):
        return [sys.executable, "--run-flow", flow_path, "--run-id", str(run_id)]
    return [sys.executable, "-m", "flowfile", "run", "flow", flow_path, "--run-id", str(run_id)]


@pytest.mark.parametrize("schedule_type", ["interval", "table_trigger"])
def test_a_schedule_naming_a_backend_spawns_with_it(sched, popen_calls, tmp_path, schedule_type):
    flow_path = _flow_path(tmp_path)
    reg_id, table_id = _seed(sched, flow_path, schedule_type, "worker:team")

    assert _fire(sched, schedule_type, table_id) == 1
    assert popen_calls == [_todays_argv(flow_path, _run_id(sched, reg_id)) + ["--backend", "worker:team"]]


@pytest.mark.parametrize("schedule_type", ["interval", "table_trigger"])
def test_a_schedule_without_a_backend_spawns_todays_command(sched, popen_calls, tmp_path, schedule_type):
    flow_path = _flow_path(tmp_path)
    reg_id, table_id = _seed(sched, flow_path, schedule_type, None)

    assert _fire(sched, schedule_type, table_id) == 1
    assert popen_calls == [_todays_argv(flow_path, _run_id(sched, reg_id))]
