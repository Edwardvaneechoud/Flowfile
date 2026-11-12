"""Each local user's stored hub connection (secure store, ``flowfile_hub`` / ``session:<user_id>``) and the access
tokens minted from it.

It is no ``Secret`` row, so no node can reference it, and it holds no password. Access tokens live only in memory; each
refresh rotates the refresh token and the new one is stored at once. A sign-in through the hub on a joined server is
``kind: "delegation"``: opaque tokens whose expiries are counted from the hub's answer, ended when a refresh is
answered ``invalid_grant``. Expiries of JWTs are read unverified: the hub verifies, this side only needs to know when
to refresh.
"""

import json
import threading
import time
import uuid

import jwt

from flowfile_core.auth import secrets
from flowfile_core.configs import logger
from flowfile_core.hub_client import client
from shared import server_extension

SERVICE = "flowfile_hub"
_REFRESH_MARGIN_SECONDS = 60

_access_tokens: dict[int, tuple[str, float | None]] = {}
_lock = threading.Lock()


def _key(user_id: int) -> str:
    return f"session:{user_id}"


def _expiry(token: str) -> float | None:
    try:
        exp = jwt.decode(token, options={"verify_signature": False}).get("exp")
    except jwt.PyJWTError:
        return None
    return float(exp) if isinstance(exp, int | float) else None


def account(stored: dict) -> str | None:
    """The hub username of a desktop connection: its refresh token's ``sub``; ``None`` for a delegation."""
    if stored.get("kind") == "delegation":
        return None
    try:
        sub = jwt.decode(stored.get("refresh_token") or "", options={"verify_signature": False}).get("sub")
    except jwt.PyJWTError:
        return None
    return sub if isinstance(sub, str) and sub else None


def load(user_id: int) -> dict | None:
    raw = secrets.get_password(SERVICE, _key(user_id))
    if not raw:
        return None
    try:
        session = json.loads(raw)
    except ValueError:
        return None
    return session if isinstance(session, dict) else None


def save(user_id: int, session: dict) -> None:
    """Replace the connection of ``user_id``; ``refresh_expires_at`` is read from the refresh token itself."""
    stored = {
        "hub_url": session["hub_url"],
        "refresh_token": session["refresh_token"],
        "refresh_expires_at": _expiry(session["refresh_token"]),
        "device_name": session.get("device_name"),
        "org_name": session.get("org_name"),
    }
    with _lock:
        _access_tokens.pop(user_id, None)
        secrets.set_password(SERVICE, _key(user_id), json.dumps(stored))
    _forget_node_tokens(user_id)


def save_delegation(user_id: int, hub_url: str, granted) -> str:
    """Store a sign-in through the hub (``granted``: its two tokens and their lifetimes in seconds) and return its
    ``sid``, which only core's own tokens for it carry, so only they renew it. The access token is cached, never
    stored."""
    now = time.time()
    sid = uuid.uuid4().hex
    stored = {
        "kind": "delegation",
        "hub_url": hub_url.rstrip("/"),
        "refresh_token": granted.refresh_token,
        "refresh_expires_at": now + granted.refresh_expires_in,
        "sid": sid,
    }
    with _lock:
        secrets.set_password(SERVICE, _key(user_id), json.dumps(stored))
        _access_tokens[user_id] = (granted.access_token, now + granted.expires_in)
    _forget_node_tokens(user_id)
    return sid


def clear(user_id: int) -> None:
    """Forget the connection and its node tokens on this machine; the hub is not told."""
    with _lock:
        _access_tokens.pop(user_id, None)
        secrets.delete_password(SERVICE, _key(user_id))
    _forget_node_tokens(user_id)


def _forget_node_tokens(user_id: int) -> None:
    from flowfile_core.hub_client import node_tokens  # imported here: it imports this module

    node_tokens.clear(user_id)


def _ended(refused: client.HubRefused) -> bool:
    return isinstance(refused.payload, dict) and refused.payload.get("error") == "invalid_grant"


def _refresh_delegation(user_id: int, stored: dict):
    """Refresh with ``_lock`` held, through the server's extension, which signs the request as this server.
    ``invalid_grant`` removes the session before re-raising; a server that cannot sign for itself raises ``HubRefused``
    (401) and keeps it."""
    try:
        if not server_extension.defines("refresh_delegation"):
            raise LookupError("nothing on this server signs for it at its hub")
        granted = server_extension.call("refresh_delegation", stored["hub_url"], stored["refresh_token"])
    except client.HubRefused as e:
        if _ended(e):
            _access_tokens.pop(user_id, None)
            secrets.delete_password(SERVICE, _key(user_id))
        raise
    except LookupError as e:
        logger.warning("The sign-in through the hub of user %s could not be refreshed: %s", user_id, e)
        raise client.HubRefused(401, "This server cannot sign for itself at its hub.") from None
    now = time.time()
    stored["refresh_token"] = granted.refresh_token
    stored["refresh_expires_at"] = now + granted.refresh_expires_in
    secrets.set_password(SERVICE, _key(user_id), json.dumps(stored))
    _access_tokens[user_id] = (granted.access_token, now + granted.expires_in)
    return granted


def access_token(user_id: int) -> str | None:
    """An access token for ``user_id``'s hub, ``None`` without a connection; reused until a minute before ``exp``.

    A refused refresh leaves a desktop connection as it was; a delegation answered ``invalid_grant`` is cleared,
    node tokens included, before the refusal is raised.
    """
    try:
        with _lock:
            cached = _access_tokens.get(user_id)
            if cached is not None:
                token, exp = cached
                if exp is not None and exp - time.time() > _REFRESH_MARGIN_SECONDS:
                    return token

            session = load(user_id)
            if session is None:
                return None
            if session.get("kind") == "delegation":
                return _refresh_delegation(user_id, session).access_token
            tokens = client.refresh(session["hub_url"], session["refresh_token"])
            session["refresh_token"] = tokens["refresh_token"]
            session["refresh_expires_at"] = _expiry(tokens["refresh_token"])
            secrets.set_password(SERVICE, _key(user_id), json.dumps(session))
            _access_tokens[user_id] = (tokens["access_token"], _expiry(tokens["access_token"]))
            return tokens["access_token"]
    except client.HubRefused as e:
        if _ended(e):
            _forget_node_tokens(user_id)
        raise


def bearer(user_id: int | None, missing: str, *, ended: tuple[int, ...] = (401,)) -> tuple[str, str]:
    """``(hub_url, access_token)`` of ``user_id``'s session; ``LookupError(missing)`` without one or when the hub
    refuses its refresh with a status in ``ended``. Other hub errors propagate."""
    stored = load(user_id)
    if stored is None:
        raise LookupError(missing)
    try:
        token = access_token(user_id)
    except client.HubRefused as e:
        if e.status in ended:
            raise LookupError(missing) from e
        raise
    if token is None:
        raise LookupError(missing)
    return stored["hub_url"], token


def refresh_delegation(user_id: int, sid: str | None):
    """Refresh the sign-in ``sid`` at the hub now, whatever is cached; ``None``, nothing asked, when the kept sign-in
    is not ``sid``. Raises as ``access_token`` does."""
    try:
        with _lock:
            stored = load(user_id)
            if stored is None or stored.get("kind") != "delegation" or sid is None or stored.get("sid") != sid:
                return None
            return _refresh_delegation(user_id, stored)
    except client.HubRefused as e:
        if _ended(e):
            _forget_node_tokens(user_id)
        raise
