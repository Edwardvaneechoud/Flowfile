"""Who may call the worker: core with the internal token, and the holder of a token from a configured issuer.

Core is the worker's usual client; it signs every request with the shared internal token (``FLOWFILE_INTERNAL_TOKEN``).
A worker that trusts an issuer also takes a short-lived token the issuer signed for this server, in the same
``X-Flowfile-Internal`` header a backend row's token travels in, so a desktop handed such a token reaches the server
without ever holding the internal token. ``verify_issuer_token`` is the issuer's own check against its published keys,
set with ``issuer_name`` where the worker starts; a JWT is told from the internal token by its shape, and nothing here
holds a secret of the issuer's.
"""

import secrets

from fastapi import HTTPException, Request, WebSocket
from starlette.concurrency import run_in_threadpool

from flowfile_worker.internal_token import resolve_internal_token
from shared.issuer_tokens import TokenRefused

INTERNAL_TOKEN_HEADER = "X-Flowfile-Internal"
issuer_name = "Issuer"


def verify_issuer_token(supplied: str) -> object | None:
    """The claims of ``supplied`` when the issuer this worker trusts signed it; none is trusted until one is set."""
    return None


def _header_matches(supplied: str) -> bool:
    """True only when a token is configured and the supplied header matches it."""
    token = resolve_internal_token()
    return bool(token) and secrets.compare_digest(supplied, token)


def _issuer_token_accepted(supplied: str) -> bool:
    """Whether ``supplied`` is an issuer's token this server accepts; one it refuses, or no JWT at all, is ``False``."""
    try:
        return verify_issuer_token(supplied) is not None
    except TokenRefused:
        return False


def carries_internal_token(request: Request) -> bool:
    """Whether the request's ``X-Flowfile-Internal`` header is the internal token."""
    return _header_matches(request.headers.get(INTERNAL_TOKEN_HEADER, ""))


async def verify_internal_token(request: Request) -> None:
    """Answer 401 unless the request carries the internal token or an issuer's token this server accepts.

    ``async``, so the internal-token check never waits for a pool thread; an issuer's token is verified on one, since
    the first use and a rotation fetch the issuer's keys. A refused one's 401 names the reason, never the token.
    """
    supplied = request.headers.get(INTERNAL_TOKEN_HEADER, "")
    if _header_matches(supplied):
        return
    try:
        claims = await run_in_threadpool(verify_issuer_token, supplied)
    except TokenRefused as exc:
        raise HTTPException(status_code=401, detail=f"{issuer_name} token refused: {exc.reason}") from None
    if claims is None:
        raise HTTPException(status_code=401, detail="Invalid or missing internal token")


async def websocket_authorized(websocket: WebSocket) -> bool:
    """Whether the socket may connect: the internal token at once, an issuer's on a pool thread, as for a request."""
    supplied = websocket.headers.get(INTERNAL_TOKEN_HEADER, "")
    return _header_matches(supplied) or await run_in_threadpool(_issuer_token_accepted, supplied)
