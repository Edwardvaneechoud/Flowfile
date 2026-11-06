"""Who may call the worker: core with the internal token, and the holder of a token from a configured issuer.

Core is the worker's usual client; it signs every request with the shared internal token (``FLOWFILE_INTERNAL_TOKEN``).
A worker that is a node on a hub (joined to one, or named on one by ``FLOWFILE_HUB_URL`` and ``FLOWFILE_NODE_NAME``)
also takes a short-lived token the hub signed for this node, in the same ``X-Flowfile-Internal`` header a backend row's
token travels in, so a desktop handed such a token reaches the node without ever holding the internal token.
``shared.hub_tokens`` verifies it against the hub's published keys; a JWT is told from the internal token by its shape,
and nothing here holds a secret from the hub.
"""

import secrets

from fastapi import HTTPException, Request, WebSocket
from starlette.concurrency import run_in_threadpool

from flowfile_worker.internal_token import resolve_internal_token
from shared import hub_tokens
from shared.hub_tokens import HubTokenRefused

INTERNAL_TOKEN_HEADER = "X-Flowfile-Internal"


def _header_matches(supplied: str) -> bool:
    """True only when a token is configured and the supplied header matches it."""
    token = resolve_internal_token()
    return bool(token) and secrets.compare_digest(supplied, token)


def _hub_token_accepted(supplied: str) -> bool:
    """Whether ``supplied`` is a hub token this node accepts; one it refuses, or no JWT at all, is ``False``."""
    try:
        return hub_tokens.verify(supplied) is not None
    except HubTokenRefused:
        return False


def carries_internal_token(request: Request) -> bool:
    """Whether the request's ``X-Flowfile-Internal`` header is the internal token."""
    return _header_matches(request.headers.get(INTERNAL_TOKEN_HEADER, ""))


async def verify_internal_token(request: Request) -> None:
    """Answer 401 unless the request carries the internal token or a hub token this node accepts.

    ``async``, so the internal-token check never waits for a pool thread; a hub token is verified on one, since the
    first use and a rotation fetch the hub's keys. A refused hub token's 401 names the reason, never the token.
    """
    supplied = request.headers.get(INTERNAL_TOKEN_HEADER, "")
    if _header_matches(supplied):
        return
    try:
        claims = await run_in_threadpool(hub_tokens.verify, supplied)
    except HubTokenRefused as exc:
        raise HTTPException(status_code=401, detail=f"Hub token refused: {exc.reason}") from None
    if claims is None:
        raise HTTPException(status_code=401, detail="Invalid or missing internal token")


async def websocket_authorized(websocket: WebSocket) -> bool:
    """Whether the socket may connect: the internal token at once, a hub token on a pool thread, as for a request."""
    supplied = websocket.headers.get(INTERNAL_TOKEN_HEADER, "")
    return _header_matches(supplied) or await run_in_threadpool(_hub_token_accepted, supplied)
