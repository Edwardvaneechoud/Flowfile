"""The canvas notebook's session mode policy.

``notebook_sessions_allowed`` decides who may start a notebook session (a Python subprocess of core).
Every user may in ``electron`` mode. In ``docker`` and ``package`` mode (both multi-user) only an
admin may, and only when the operator opts in with ``FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER=admin``:
the subprocess inherits core's Docker socket, master key and JWT secret, so enabling it grants the
admin host-level access. Both env vars are read per call, since ``settings.FLOWFILE_MODE`` is cached
at import (the same reason as ``auth.sharing.sharing_enabled``).
"""

from __future__ import annotations

import os


def notebook_sessions_allowed(user) -> bool:
    """Whether ``user`` may start a notebook session under the current ``FLOWFILE_MODE``."""
    if os.environ.get("FLOWFILE_MODE", "electron") == "electron":
        return True
    opt_in = os.environ.get("FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER", "off").strip().lower()
    return opt_in == "admin" and bool(getattr(user, "is_admin", False))
