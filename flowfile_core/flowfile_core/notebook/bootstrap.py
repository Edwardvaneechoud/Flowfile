"""How core launches a notebook session process: argv, env, creation flags and the Windows Job Object.

Dev and pip installs run ``[sys.executable, "-c", BOOTSTRAP]``; a frozen build runs
``[sys.executable, "--notebook-session"]``, the verb checked at the very top of ``flowfile_core/main.py``.
Both move the protocol off fd 1 before importing anything from flowfile, because ``flowfile_core``'s
logger binds a ``StreamHandler`` to ``sys.__stdout__`` at import: the protocol keeps a dup of the
original stdout (and stdin), fd 1 becomes stderr (a per-session log file) and fd 0 ``/dev/null``, so a
cell's ``input()`` can never read protocol bytes. ``-m`` is not used because it imports
``flowfile_core/__init__.py`` before any code of ours runs, and the empty ``sys.path`` entry ``-c`` adds is
dropped so a file in core's working directory cannot shadow a module.

Never ``multiprocessing`` (core has no ``freeze_support``) and never ``start_new_session``: the session
stays in core's process group, so the desktop shell's ``killpg`` / ``taskkill /T`` reap it. On Windows it
runs with ``CREATE_NO_WINDOW`` and joins a Job Object with ``KILL_ON_JOB_CLOSE``, so a hard-killed core
takes its sessions with it.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping

SESSION_VERB = "--notebook-session"

BOOTSTRAP = (
    "import os, sys\n"
    "sys.path[:] = [p for p in sys.path if p not in ('', '.')]\n"
    "proto_out = os.fdopen(os.dup(1), 'wb', 0)\n"
    "proto_in = os.fdopen(os.dup(0), 'rb', 0)\n"
    "os.dup2(2, 1)\n"
    "os.dup2(os.open(os.devnull, os.O_RDONLY), 0)\n"
    "from flowfile_core.notebook.session_main import main\n"
    "sys.exit(main(proto_out, proto_in))\n"
)

CREATE_NO_WINDOW = 0x08000000
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9

DROPPED_ENV = ("FLOWFILE_ADMIN_PASSWORD",)


def session_command(frozen: bool | None = None) -> list[str]:
    """The argv of a session process; argv carries only the verb, everything else goes by env or message."""
    if frozen is None:
        frozen = bool(getattr(sys, "frozen", False))
    if frozen:
        return [sys.executable, SESSION_VERB]
    return [sys.executable, "-c", BOOTSTRAP]


def session_env(user_id: int, spill_dir: str, base: Mapping[str, str] | None = None) -> dict[str, str]:
    """Core's env minus the admin password, pointed at core's worker and with every startup side effect off.

    ``CORE_PORT`` and ``FLOWFILE_WORKER_URL`` come from core's resolved settings (core never writes them to
    its own env); ``FLOWFILE_SINGLE_FILE_MODE`` / ``FLOWFILE_WORKER_PORT`` / the storage and DB paths are
    inherited as core has them.
    """
    from flowfile_core.configs import settings

    env = {k: v for k, v in (os.environ if base is None else base).items() if k not in DROPPED_ENV}
    env.update(
        {
            "CORE_PORT": str(settings.SERVER_PORT),
            "FLOWFILE_WORKER_URL": str(settings.WORKER_URL),
            "FLOWFILE_OFFLOAD_TO_WORKER": "0",
            "FLOWFILE_SKIP_STARTUP_MIGRATION": "1",
            "FLOWFILE_SKIP_INIT_DB": "1",
            "FLOWFILE_KERNEL_GC": "0",
            "FLOWFILE_TELEMETRY": "0",
            "FLOWFILE_SESSION_USER_ID": str(user_id),
            "FLOWFILE_NOTEBOOK_SPILL_DIR": spill_dir,
            "PYTHONIOENCODING": "utf-8",
            "PYTHONUNBUFFERED": "1",
        }
    )
    return env


def creation_flags(platform: str | None = None) -> int:
    """``CREATE_NO_WINDOW`` on Windows, 0 elsewhere."""
    return CREATE_NO_WINDOW if (platform or sys.platform) == "win32" else 0


def job_limit_flags() -> int:
    """The Job Object limit a session's job carries: kill every member when the last handle closes."""
    return JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE


def attach_kill_on_close_job(process: subprocess.Popen) -> object | None:
    """Put ``process`` in a new Job Object with ``KILL_ON_JOB_CLOSE`` (Windows only; ``None`` elsewhere).

    The returned handle must stay referenced for the session's lifetime: when core dies the OS closes it
    and the session dies with it. Failures are swallowed; stdin EOF remains the fallback.
    """
    if sys.platform != "win32":
        return None
    try:
        import ctypes
        from ctypes import wintypes

        class _BasicLimits(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class _IoCounters(ctypes.Structure):
            _fields_ = [(name, ctypes.c_uint64) for name in ("r_ops", "w_ops", "o_ops", "r_bytes", "w_bytes", "o_b")]

        class _ExtendedLimits(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", _BasicLimits),
                ("IoInfo", _IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return None
        info = _ExtendedLimits()
        info.BasicLimitInformation.LimitFlags = job_limit_flags()
        ok = kernel32.SetInformationJobObject(
            job, _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION, ctypes.byref(info), ctypes.sizeof(info)
        )
        if not ok or not kernel32.AssignProcessToJobObject(job, wintypes.HANDLE(int(process._handle))):
            kernel32.CloseHandle(job)
            return None
        return job
    except Exception:
        return None
