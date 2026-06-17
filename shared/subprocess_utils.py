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
from pathlib import Path

logger = logging.getLogger("flowfile.subprocess")


def spawn_flow_subprocess(flow_path: str, run_id: int) -> int | None:
    """Fire-and-forget a ``flowfile run flow`` subprocess.

    Uses ``os.open`` / ``os.close`` to pass a raw file descriptor to
    ``Popen``.  ``Popen`` internally duplicates the fd for the child
    process, so closing it in the parent afterwards is safe — no race
    condition with child fd inheritance.

    Returns the child PID on success, or ``None`` on failure.
    """
    frozen = getattr(sys, "frozen", False)
    logger.debug("Frozen mode: %s, sys.executable: %s", frozen, sys.executable)
    if frozen:
        cmd = [sys.executable, "--run-flow", flow_path, "--run-id", str(run_id)]
    else:
        cmd = [sys.executable, "-m", "flowfile", "run", "flow", flow_path, "--run-id", str(run_id)]
    logger.info("Spawning: %s", " ".join(cmd))
    try:
        log_dir = Path.home() / ".flowfile" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / f"scheduled_run_{run_id}.log"
        fd = os.open(str(log_file), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=fd,
                stderr=fd,
                start_new_session=True,
            )
        finally:
            os.close(fd)
        logger.info("Subprocess log: %s (pid=%s)", log_file, proc.pid)
        return proc.pid
    except Exception:
        logger.exception("Failed to spawn flow subprocess for flow_path=%s, run_id=%s", flow_path, run_id)
        return None
