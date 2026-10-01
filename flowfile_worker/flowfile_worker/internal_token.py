"""The internal token core and the worker share, resolvable from any worker process.

Stdlib-only at import: every spawned child imports it (through ``flow_logger``) to sign
its log posts to core, and must not pay for FastAPI or pydantic to do so.
"""

import os

_token: str | None = None


def resolve_internal_token() -> str | None:
    """``FLOWFILE_INTERNAL_TOKEN``, else the secure store core persists the token to.

    Resolved lazily so a worker started before core minted the token needs no restart;
    cached only on success.
    """
    global _token
    if _token is None:
        token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")
        if not token:
            from flowfile_worker.secrets import get_password

            token = get_password("flowfile", "internal_token")
        if token:
            _token = token
    return _token
