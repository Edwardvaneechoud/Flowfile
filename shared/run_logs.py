"""Per-run subprocess log paths and log retention.

Lives in ``shared`` because the run log is written by the subprocess launcher,
read back by core's catalog routes, and expired by both core startup and the
scheduler tick — none of which may import each other.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

from shared.storage_config import storage

logger = logging.getLogger("flowfile.run_logs")

RUN_LOG_PREFIX = "scheduled_run_"
FLOW_LOG_PREFIX = "flow_"

# Mirrors shared.run_completion.REAPABLE_RUN_TYPES; duplicated rather than
# imported so this module stays free of the sqlalchemy import.
SUBPROCESS_RUN_TYPES = ("scheduled", "manual", "on_demand")

DEFAULT_LOG_RETENTION_DAYS = 30


def run_log_path(run_id: int) -> Path:
    """Absolute path of a run's subprocess log.

    Resolved per call so FLOWFILE_STORAGE_DIR / docker mode / TESTING are honored.
    The ``scheduled_run_`` prefix is legacy — it covers manual and on-demand runs
    too, and is kept so logs written by earlier versions stay readable.
    """
    return storage.logs_directory / f"{RUN_LOG_PREFIX}{run_id}.log"


def _retention_days() -> int:
    raw = os.environ.get("FLOWFILE_RUN_LOG_RETENTION_DAYS")
    if not raw:
        return DEFAULT_LOG_RETENTION_DAYS
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid FLOWFILE_RUN_LOG_RETENTION_DAYS=%r; using default %d", raw, DEFAULT_LOG_RETENTION_DAYS)
        return DEFAULT_LOG_RETENTION_DAYS


def cleanup_old_logs(max_age_days: int | None = None) -> int:
    """Delete run and flow logs older than the retention window.

    Returns the number of files removed. ``max_age_days`` defaults to
    ``FLOWFILE_RUN_LOG_RETENTION_DAYS`` (30); 0 or negative disables retention.
    """
    days = _retention_days() if max_age_days is None else max_age_days
    if days <= 0:
        return 0

    cutoff = time.time() - days * 86400
    logs_dir = storage.logs_directory
    deleted = 0
    for prefix in (RUN_LOG_PREFIX, FLOW_LOG_PREFIX):
        for log_file in logs_dir.glob(f"{prefix}*.log"):
            try:
                if log_file.stat().st_mtime < cutoff:
                    log_file.unlink()
                    deleted += 1
            except OSError:
                logger.warning("Could not remove expired log %s", log_file, exc_info=True)

    if deleted:
        logger.info("Removed %d expired log file(s) older than %d day(s)", deleted, days)
    return deleted
