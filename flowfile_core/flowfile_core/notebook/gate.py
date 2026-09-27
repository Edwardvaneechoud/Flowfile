"""Gates for the canvas notebook: the ``FEATURE_FLAG_CANVAS_NOTEBOOK`` switch and the session mode policy.

``FEATURE_FLAG_CANVAS_NOTEBOOK`` is a ``MutableBool`` in ``configs.settings`` read on every call, so an
in-process flip takes effect immediately; ``require_canvas_notebook_enabled`` is the router-level
dependency that answers 503 while it is off, like ``require_ai_enabled`` does for ``/ai/*``.

``notebook_sessions_allowed`` decides who may start a notebook session (a Python subprocess of core).
Every user may in ``electron`` mode. In ``docker`` and ``package`` mode (both multi-user) only an
admin may, and only when the operator opts in with ``FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER=admin``:
the subprocess inherits core's Docker socket, master key and JWT secret, so enabling it grants the
admin host-level access. Both env vars are read per call, since ``settings.FLOWFILE_MODE`` is cached
at import (the same reason as ``auth.sharing.sharing_enabled``).
"""

from __future__ import annotations

import os

from fastapi import HTTPException, status

from flowfile_core.configs import settings as _settings

DISABLED_DETAIL = "The canvas notebook is disabled. Set FEATURE_FLAG_CANVAS_NOTEBOOK=true to enable."


def is_canvas_notebook_enabled() -> bool:
    return bool(_settings.FEATURE_FLAG_CANVAS_NOTEBOOK)


def require_canvas_notebook_enabled() -> None:
    """FastAPI dependency gating every notebook route: 503 while the flag is off."""
    if not is_canvas_notebook_enabled():
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=DISABLED_DETAIL)


def notebook_sessions_allowed(user) -> bool:
    """Whether ``user`` may start a notebook session under the current ``FLOWFILE_MODE``."""
    if os.environ.get("FLOWFILE_MODE", "electron") == "electron":
        return True
    opt_in = os.environ.get("FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER", "off").strip().lower()
    return opt_in == "admin" and bool(getattr(user, "is_admin", False))
