"""Internal-token auth for the worker service boundary.

Core is the worker's only intended client; it signs every request with the
shared internal token (``FLOWFILE_INTERNAL_TOKEN``).
"""

import secrets

from fastapi import HTTPException, Request, WebSocket

from flowfile_worker.internal_token import resolve_internal_token

INTERNAL_TOKEN_HEADER = "X-Flowfile-Internal"


def _header_matches(supplied: str) -> bool:
    # No token found means every request is rejected.
    token = resolve_internal_token()
    return bool(token) and secrets.compare_digest(supplied, token)


def verify_internal_token(request: Request) -> None:
    if not _header_matches(request.headers.get(INTERNAL_TOKEN_HEADER, "")):
        raise HTTPException(status_code=401, detail="Invalid or missing internal token")


def websocket_authorized(websocket: WebSocket) -> bool:
    return _header_matches(websocket.headers.get(INTERNAL_TOKEN_HEADER, ""))
