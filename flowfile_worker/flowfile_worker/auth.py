"""Internal-token auth for the worker service boundary.

Core is the worker's only intended client; it signs every request with the
shared internal token (``FLOWFILE_INTERNAL_TOKEN``).
"""

import secrets

from fastapi import HTTPException, Request, WebSocket

from flowfile_worker.internal_token import resolve_internal_token

INTERNAL_TOKEN_HEADER = "X-Flowfile-Internal"


def _header_matches(supplied: str) -> bool:
    """True only when a token is configured and the supplied header matches it."""
    token = resolve_internal_token()
    return bool(token) and secrets.compare_digest(supplied, token)


def carries_internal_token(request: Request) -> bool:
    """Whether the request's ``X-Flowfile-Internal`` header is the internal token."""
    return _header_matches(request.headers.get(INTERNAL_TOKEN_HEADER, ""))


async def verify_internal_token(request: Request) -> None:
    """Answer 401 unless the request carries the internal token; ``async``, so it never waits for a pool thread."""
    if not carries_internal_token(request):
        raise HTTPException(status_code=401, detail="Invalid or missing internal token")


def websocket_authorized(websocket: WebSocket) -> bool:
    return _header_matches(websocket.headers.get(INTERNAL_TOKEN_HEADER, ""))
