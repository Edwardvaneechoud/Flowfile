"""Internal-token auth for the worker service boundary.

Core is the worker's only intended client; it signs every request with the
shared internal token (``FLOWFILE_INTERNAL_TOKEN``).
"""

import os
import secrets

from fastapi import HTTPException, Request, WebSocket

INTERNAL_TOKEN_HEADER = "X-Flowfile-Internal"

_token: str | None = None


def _resolve_token() -> str | None:
    """Resolve lazily per request: env var, else the secure store core persists
    the token to, so a worker started before core minted it needs no restart.
    Cached only on success; no token found means every request is rejected."""
    global _token
    if _token is None:
        token = os.environ.get("FLOWFILE_INTERNAL_TOKEN")
        if not token:
            from flowfile_worker.secrets import get_password

            token = get_password("flowfile", "internal_token")
        if token:
            _token = token
    return _token


def _header_matches(supplied: str) -> bool:
    token = _resolve_token()
    return bool(token) and secrets.compare_digest(supplied, token)


def verify_internal_token(request: Request) -> None:
    if not _header_matches(request.headers.get(INTERNAL_TOKEN_HEADER, "")):
        raise HTTPException(status_code=401, detail="Invalid or missing internal token")


def websocket_authorized(websocket: WebSocket) -> bool:
    return _header_matches(websocket.headers.get(INTERNAL_TOKEN_HEADER, ""))
