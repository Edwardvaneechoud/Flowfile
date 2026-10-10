"""Parent-death watcher for desktop-sidecar mode.

When Flowfile runs as a Tauri sidecar the shell sets ``FLOWFILE_SUPERVISOR_PID``
to its own pid. If the shell is force-killed or crashes it cannot run its own
shutdown ladder, so the sidecar watches that pid and runs its own graceful
shutdown once it is gone.

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
    """Watch the supervisor process and call ``on_parent_death`` once it dies.

    Returns the watcher thread, or ``None`` when not running as a sidecar.
    """
    raw = os.environ.get("FLOWFILE_SUPERVISOR_PID")
    if not raw:
        return None
    try:
        supervisor = int(raw)
    except ValueError:
        logger.error("FLOWFILE_SUPERVISOR_PID=%r is not a pid; parent-death watcher disabled", raw)
        return None

    def _watch() -> None:
        if _WINDOWS:
            _wait_for_exit_windows(supervisor, poll_interval)
        else:
            _wait_for_exit_posix(supervisor, poll_interval)
        logger.warning("supervisor %s died; shutting down sidecar", supervisor)
        try:
            on_parent_death()
        except Exception as exc:
            logger.error("parent-death shutdown callback failed: %s", exc)

    thread = threading.Thread(target=_watch, name="parent-death-watcher", daemon=True)
    thread.start()
    logger.info("parent-death watcher started (supervisor pid %s)", supervisor)
    return thread


def _wait_for_exit_posix(supervisor: int, poll_interval: float) -> None:
    # Still our parent means alive; once reparented, probe the pid until it is reaped.
    while True:
        if os.getppid() != supervisor and not _posix_pid_alive(supervisor):
            return
        time.sleep(poll_interval)


def _posix_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_for_exit_windows(supervisor: int, poll_interval: float) -> None:
    # getppid never changes on Windows; wait on a handle to the process instead.
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

    handle = kernel32.OpenProcess(synchronize, False, supervisor)
    if not handle:
        err = ctypes.get_last_error()
        if err == error_invalid_parameter:
            return
        logger.error("cannot open supervisor %s (error %s); parent-death watcher disabled", supervisor, err)
        threading.Event().wait()
        return

    timeout_ms = max(1, int(poll_interval * 1000))
    try:
        while True:
            result = kernel32.WaitForSingleObject(handle, timeout_ms)
            if result == wait_timeout:
                continue
            if result == wait_object_0:
                return
            logger.error("waiting on supervisor %s failed (code %s); parent-death watcher disabled", supervisor, result)
            threading.Event().wait()
            return
    finally:
        kernel32.CloseHandle(handle)
