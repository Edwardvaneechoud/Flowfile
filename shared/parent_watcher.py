"""Parent-death watcher for desktop-sidecar mode.

When Flowfile runs as a Tauri sidecar the shell sets ``FLOWFILE_SUPERVISOR_PID``.
If the shell is force-killed or crashes it cannot run its own shutdown ladder,
which would orphan this process (it reparents to launchd/init). This watcher
detects the reparent and triggers the service's normal graceful shutdown.

It is a no-op unless ``FLOWFILE_SUPERVISOR_PID`` is set, so standalone/CLI/Docker
runs are unaffected.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable

logger = logging.getLogger("flowfile.parent_watcher")


def start_parent_death_watcher(
    on_parent_death: Callable[[], None],
    *,
    poll_interval: float = 1.0,
) -> threading.Thread | None:
    """Watch the parent process and call ``on_parent_death`` once it dies.

    Returns the watcher thread, or ``None`` when not running as a sidecar.
    """
    if not os.environ.get("FLOWFILE_SUPERVISOR_PID"):
        return None

    start_ppid = os.getppid()

    def _watch() -> None:
        while True:
            time.sleep(poll_interval)
            current_ppid = os.getppid()
            if current_ppid != start_ppid:
                logger.warning(
                    "parent %s died (reparented to %s); shutting down sidecar",
                    start_ppid,
                    current_ppid,
                )
                try:
                    on_parent_death()
                except Exception as exc:
                    logger.error("parent-death shutdown callback failed: %s", exc)
                return

    thread = threading.Thread(target=_watch, name="parent-death-watcher", daemon=True)
    thread.start()
    logger.info("parent-death watcher started (supervisor pid %s)", start_ppid)
    return thread
