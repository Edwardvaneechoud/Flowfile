"""Parent-death watcher for desktop-sidecar mode.

When Flowfile runs as a Tauri sidecar the shell sets ``FLOWFILE_SUPERVISOR_PID``.
If the shell is force-killed or crashes it cannot run its own shutdown ladder,
so the sidecar watches its parent and runs its own graceful shutdown instead.

It is a no-op unless ``FLOWFILE_SUPERVISOR_PID`` is set, so standalone/CLI/Docker
runs are unaffected.
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from collections.abc import Callable

logger = logging.getLogger("flowfile.parent_watcher")

_WINDOWS = sys.platform.startswith("win")


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
        if _WINDOWS:
            _wait_for_parent_windows(start_ppid, poll_interval)
        else:
            _wait_for_parent_posix(start_ppid, poll_interval)
        try:
            on_parent_death()
        except Exception as exc:
            logger.error("parent-death shutdown callback failed: %s", exc)

    thread = threading.Thread(target=_watch, name="parent-death-watcher", daemon=True)
    thread.start()
    logger.info("parent-death watcher started (supervisor pid %s)", start_ppid)
    return thread


def _wait_for_parent_posix(start_ppid: int, poll_interval: float) -> None:
    while True:
        time.sleep(poll_interval)
        current_ppid = os.getppid()
        if current_ppid != start_ppid:
            logger.warning("parent %s died (reparented to %s); shutting down sidecar", start_ppid, current_ppid)
            return


def _wait_for_parent_windows(start_ppid: int, poll_interval: float) -> None:
    # getppid never changes on Windows; wait on a handle to the parent instead.
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)

    synchronize = 0x0010_0000
    wait_object_0 = 0x0000_0000
    wait_timeout = 0x0000_0102
    error_invalid_parameter = 87

    handle = kernel32.OpenProcess(synchronize, False, start_ppid)
    if not handle:
        err = ctypes.get_last_error()
        if err == error_invalid_parameter:
            logger.warning("parent %s is already gone; shutting down sidecar", start_ppid)
            return
        logger.error("cannot open parent %s (error %s); watcher disabled", start_ppid, err)
        threading.Event().wait()
        return

    timeout_ms = max(1, int(poll_interval * 1000))
    try:
        while True:
            result = kernel32.WaitForSingleObject(handle, timeout_ms)
            if result == wait_timeout:
                continue
            if result == wait_object_0:
                logger.warning("parent %s died; shutting down sidecar", start_ppid)
                return
            logger.error("waiting on parent %s failed (code %s); watcher disabled", start_ppid, result)
            threading.Event().wait()
            return
    finally:
        kernel32.CloseHandle(handle)
