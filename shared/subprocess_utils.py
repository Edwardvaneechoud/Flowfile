"""Shared subprocess utilities for spawning flow runs.

This module is intentionally free of ``flowfile_core`` imports so that
both the core service and the lightweight scheduler can use it without
pulling in the full application stack.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys

from shared.run_logs import run_log_path
from shared.telemetry import ENV_KILL_SWITCH as _TELEMETRY_KILL_SWITCH

logger = logging.getLogger("flowfile.subprocess")


def spawn_flow_subprocess(
    flow_path: str,
    run_id: int,
    extra_env: dict[str, str] | None = None,
    *,
    suppress_telemetry: bool = False,
) -> int | None:
    """Fire-and-forget a ``flowfile run flow`` subprocess.

    Uses ``os.open`` / ``os.close`` to pass a raw file descriptor to
    ``Popen``.  ``Popen`` internally duplicates the fd for the child
    process, so closing it in the parent afterwards is safe — no race
    condition with child fd inheritance.

    ``extra_env`` is merged over the inherited environment for this child only;
    the parent's ``os.environ`` is never mutated. ``suppress_telemetry`` adds
    the telemetry kill switch to that merge, for runs the app itself started.

    Returns the child PID on success, or ``None`` on failure.
    """
    frozen = getattr(sys, "frozen", False)
    logger.debug("Frozen mode: %s, sys.executable: %s", frozen, sys.executable)
    if frozen:
        cmd = [sys.executable, "--run-flow", flow_path, "--run-id", str(run_id)]
    else:
        cmd = [sys.executable, "-m", "flowfile", "run", "flow", flow_path, "--run-id", str(run_id)]
    logger.info("Spawning: %s", " ".join(cmd))
    overrides = dict(extra_env or {})
    if suppress_telemetry:
        overrides[_TELEMETRY_KILL_SWITCH] = "0"
    env = {**os.environ, **overrides} if overrides else None
    try:
        log_file = run_log_path(run_id)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(log_file), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=fd,
                stderr=fd,
                start_new_session=True,
                env=env,
            )
        finally:
            os.close(fd)
        logger.info("Subprocess log: %s (pid=%s)", log_file, proc.pid)
        return proc.pid
    except Exception:
        logger.exception("Failed to spawn flow subprocess for flow_path=%s, run_id=%s", flow_path, run_id)
        return None
