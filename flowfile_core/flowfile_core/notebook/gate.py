"""Who may run the canvas notebook on a kernel: the mode policy and the local-connection check.

A kernel session runs the user's cells as Python in a notebook kernel (one with ``flowfile`` installed),
so it is offered only in ``electron`` mode (the desktop app and a default ``pip install flowfile``), and
there only to a caller on the same machine, and only with a SQLite file catalog. The mode is read per call
through ``auth.sharing.sharing_enabled`` (false only in electron).
"""

from __future__ import annotations

import ipaddress

from fastapi import HTTPException, Request

from flowfile_core.auth import sharing

DISABLED_DETAIL = "Notebook kernel sessions are only available in the desktop app with a SQLite catalog database"
REMOTE_DETAIL = "Notebook kernel sessions only accept local connections"


def kernel_sessions_allowed(user) -> bool:
    """Whether ``user`` may run notebook cells on a kernel: ``electron`` mode and a SQLite file catalog,
    which core copies for the kernel (``kernel.notebook_db``)."""
    from shared.database import sqlite_database_path

    return not sharing.sharing_enabled() and sqlite_database_path() is not None


def is_loopback(http: Request) -> bool:
    """Whether the request comes from this machine."""
    host = http.client.host if http.client is not None else ""
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def require_kernel_sessions(http: Request, user) -> None:
    """Raise 403 unless ``user`` may run kernel sessions and the request is local."""
    if not kernel_sessions_allowed(user):
        raise HTTPException(status_code=403, detail=DISABLED_DETAIL)
    if not is_loopback(http):
        raise HTTPException(status_code=403, detail=REMOTE_DETAIL)
