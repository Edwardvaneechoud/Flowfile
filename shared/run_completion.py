"""Lightweight run-completion helper for CLI subprocesses.

This module updates a ``FlowRun`` record directly via SQLAlchemy without
importing anything from ``flowfile_core``, keeping the CLI completion
path fast and free of heavy dependencies (FastAPI, Pydantic, etc.).
"""

from __future__ import annotations

import logging
import os
import signal
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from shared.models import FlowRun
from shared.notifications.processor import enqueue_orphaned_run
from shared.storage_config import get_database_url

logger = logging.getLogger("flowfile.run_completion")

# Run types executed in a spawned subprocess. ``in_designer_run`` is deliberately
# excluded: it runs in-process in core with pid NULL, so reaping it would close live runs.
REAPABLE_RUN_TYPES = ("scheduled", "manual", "on_demand")

# A spawn path stamps ``pid`` inside the same synchronous call that inserts the row,
# so a pid-less row older than this never got a process.
PID_GRACE_SECONDS = 300

DEFAULT_RUN_MAX_AGE_SECONDS = 86400


def get_run_user_id(run_id: int) -> int | None:
    """Return the FlowRun's ``user_id``, or ``None`` if the run row is missing.

    Used by subprocess CLI entrypoints to resolve the owning user for a
    pre-created run record, so the flow is loaded against the right
    user's connections/secrets.
    """
    url = get_database_url()
    connect_args = {"check_same_thread": False} if "sqlite" in url else {}
    engine = create_engine(url, connect_args=connect_args)

    with Session(engine) as session:
        run = session.get(FlowRun, run_id)
        return run.user_id if run else None


def _close_run(run: FlowRun, success: bool, now: datetime) -> None:
    """Stamp the terminal fields on a run row. The caller owns the commit."""
    run.ended_at = now
    run.success = success
    if run.started_at:
        started_utc = run.started_at.replace(tzinfo=None)
        now_utc = now.replace(tzinfo=None)
        run.duration_seconds = (now_utc - started_utc).total_seconds()


def _pid_is_alive_windows(pid: int) -> bool:
    import ctypes
    from ctypes import wintypes

    process_query_limited_information = 0x1000
    error_access_denied = 5
    still_active = 259

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)

    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if not handle:
        return ctypes.get_last_error() == error_access_denied
    try:
        exit_code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return True
        return exit_code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


def _pid_is_alive(pid: int) -> bool:
    """Whether *pid* names a live process. Unknowable ⇒ assume alive."""
    if pid is None or pid <= 0:
        return False
    if os.name == "nt":
        # os.kill(pid, 0) *terminates* the target on Windows — never use it here.
        try:
            return _pid_is_alive_windows(pid)
        except Exception:
            logger.exception("Windows liveness probe failed for pid %s", pid)
            return True
    try:
        # Spawned runs are never wait()ed, so a dead child lingers as a zombie that
        # kill(0) still reports alive — harvest it first.
        if os.waitpid(pid, os.WNOHANG)[0] == pid:
            return False
    except ChildProcessError:
        pass  # not our child (e.g. after a restart) — fall through to the signal probe
    except OSError:
        pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


def _terminate_pid(pid: int) -> str:
    """Best-effort SIGTERM, mirroring ``FlowRunService.cancel_run``. Returns a log phrase."""
    try:
        os.kill(pid, signal.SIGTERM)
        return f"terminated pid {pid}"
    except (ProcessLookupError, PermissionError):
        return f"pid {pid} not ours or already gone"
    except OSError:
        logger.warning("Failed to signal pid %s", pid, exc_info=True)
        return f"pid {pid} could not be signalled"


def reap_orphaned_runs(max_age_seconds: int | None = None) -> int:
    """Close run rows whose subprocess died without recording completion.

    A dead child never writes ``ended_at``, so the row stays active forever and
    blocks every later launch behind the active-run guard. Returns the number of
    rows closed. ``max_age_seconds`` defaults to ``FLOWFILE_RUN_MAX_AGE_SECONDS``
    (86400); 0 or negative disables the age backstop.
    """
    if max_age_seconds is None:
        raw = os.environ.get("FLOWFILE_RUN_MAX_AGE_SECONDS")
        try:
            max_age_seconds = int(raw) if raw else DEFAULT_RUN_MAX_AGE_SECONDS
        except ValueError:
            logger.warning("Invalid FLOWFILE_RUN_MAX_AGE_SECONDS=%r — using default", raw)
            max_age_seconds = DEFAULT_RUN_MAX_AGE_SECONDS

    url = get_database_url()
    connect_args = {"check_same_thread": False} if "sqlite" in url else {}
    engine = create_engine(url, connect_args=connect_args)

    now = datetime.now(timezone.utc)
    now_naive = now.replace(tzinfo=None)
    reaped = 0

    with Session(engine) as session:
        runs = session.query(FlowRun).filter(FlowRun.ended_at.is_(None), FlowRun.run_type.in_(REAPABLE_RUN_TYPES)).all()
        for run in runs:
            age = (now_naive - run.started_at.replace(tzinfo=None)).total_seconds() if run.started_at else None

            pid_alive = run.pid is not None and _pid_is_alive(run.pid)

            reason: str | None = None
            if run.pid is None:
                if age is not None and age > PID_GRACE_SECONDS:
                    reason = f"no pid recorded after {age:.0f}s"
            elif not pid_alive:
                reason = "process no longer alive"

            if reason is None and max_age_seconds > 0 and age is not None and age > max_age_seconds:
                reason = f"exceeded max age of {max_age_seconds}s"
                if pid_alive:
                    # Closing the row releases the double-launch guard, so the process
                    # must not outlive it — otherwise the next tick spawns a duplicate.
                    reason = f"{reason}; {_terminate_pid(run.pid)}"

            if reason is None:
                continue

            values = {FlowRun.ended_at: now, FlowRun.success: False}
            if run.started_at:
                values[FlowRun.duration_seconds] = (now_naive - run.started_at.replace(tzinfo=None)).total_seconds()
            # Keyed conditional UPDATE: a child whose completion landed since the load wins.
            matched = (
                session.query(FlowRun)
                .filter(FlowRun.id == run.id, FlowRun.ended_at.is_(None))
                .update(values, synchronize_session=False)
            )
            if not matched:
                logger.info("Run %s completed concurrently — not reaping", run.id)
                continue

            # Before the commit, so the close and its alert land in one transaction.
            session.refresh(run)
            enqueue_orphaned_run(session, run, reason)

            reaped += 1
            logger.warning("Reaped orphaned run %s (run_type=%s, pid=%s): %s", run.id, run.run_type, run.pid, reason)

        session.commit()

    return reaped


def complete_run(
    run_id: int,
    success: bool,
    nodes_completed: int,
    number_of_nodes: int = 0,
    node_results_json: str | None = None,
) -> None:
    """Mark a pre-created ``FlowRun`` record as completed.

    Creates a one-shot SQLAlchemy session against the shared database,
    updates the run record, and tears down immediately. ``node_results_json`` is the
    serialised per-node result list; notification payloads read the failed nodes from it.
    """
    url = get_database_url()
    connect_args = {"check_same_thread": False} if "sqlite" in url else {}
    engine = create_engine(url, connect_args=connect_args)

    with Session(engine) as session:
        run = session.get(FlowRun, run_id)
        if run is None:
            logger.warning("Run %s not found — skipping completion", run_id)
            return

        _close_run(run, success=success, now=datetime.now(timezone.utc))
        run.nodes_completed = nodes_completed
        if number_of_nodes > 0:
            run.number_of_nodes = number_of_nodes
        if node_results_json is not None:
            run.node_results_json = node_results_json

        session.commit()
        logger.info("Run %s completed: success=%s", run_id, success)
