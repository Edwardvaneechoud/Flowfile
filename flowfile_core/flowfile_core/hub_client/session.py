"""The stored hub connection of each local user, and the access tokens minted from it.

A connection is one JSON string in the secure store (service ``flowfile_hub``, key ``session:<user_id>``) holding
``hub_url``, ``refresh_token``, ``refresh_expires_at``, ``device_id`` and ``device_name``. It is not a ``Secret`` row,
so it never shows in the Secrets page and no node can reference it. No password is ever part of it. Access tokens
live only in this process's memory; each refresh rotates the refresh token and the new one is stored straight away.
The node tokens minted through a connection (``node_tokens``) go with it when it is replaced or cleared.

A token's expiry is read from its own ``exp`` claim without verifying the signature: the hub verifies its tokens,
this side only needs to know when to ask for a new one.
"""

import json
import threading
import time

import jwt

from flowfile_core.auth import secrets
from flowfile_core.hub_client import client

SERVICE = "flowfile_hub"
_REFRESH_MARGIN_SECONDS = 60

_access_tokens: dict[int, tuple[str, float | None]] = {}
_lock = threading.Lock()


def _key(user_id: int) -> str:
    return f"session:{user_id}"


def _expiry(token: str) -> float | None:
    """The token's unverified ``exp`` claim in epoch seconds, ``None`` when it has none or is no JWT."""
    try:
        exp = jwt.decode(token, options={"verify_signature": False}).get("exp")
    except jwt.PyJWTError:
        return None
    return float(exp) if isinstance(exp, int | float) else None


def load(user_id: int) -> dict | None:
    """The stored connection of ``user_id``, ``None`` when there is none."""
    raw = secrets.get_password(SERVICE, _key(user_id))
    if not raw:
        return None
    try:
        session = json.loads(raw)
    except ValueError:
        return None
    return session if isinstance(session, dict) else None


def save(user_id: int, session: dict) -> None:
    """Store ``session`` as the connection of ``user_id``, replacing any earlier one and its cached access token.

    ``refresh_expires_at`` is taken from the refresh token itself; whatever the caller passed for it is ignored.
    """
    stored = {
        "hub_url": session["hub_url"],
        "refresh_token": session["refresh_token"],
        "refresh_expires_at": _expiry(session["refresh_token"]),
        "device_id": session.get("device_id"),
        "device_name": session.get("device_name"),
    }
    with _lock:
        _access_tokens.pop(user_id, None)
        secrets.set_password(SERVICE, _key(user_id), json.dumps(stored))
    _forget_node_tokens(user_id)


def clear(user_id: int) -> None:
    """Forget the connection of ``user_id`` and its node tokens on this machine; the hub is not told."""
    with _lock:
        _access_tokens.pop(user_id, None)
        secrets.delete_password(SERVICE, _key(user_id))
    _forget_node_tokens(user_id)


def _forget_node_tokens(user_id: int) -> None:
    from flowfile_core.hub_client import node_tokens  # imported here: it imports this module

    node_tokens.clear(user_id)


def access_token(user_id: int) -> str | None:
    """An access token for the hub ``user_id`` is connected to, ``None`` when there is no connection.

    The token cached in this process is reused until it is within a minute of its ``exp``; otherwise a new one is
    minted through ``client.refresh``, and the rotated refresh token is stored before the access token is returned.
    A hub that refuses the refresh raises the client's error and leaves the stored connection as it was.
    """
    with _lock:
        cached = _access_tokens.get(user_id)
        if cached is not None:
            token, exp = cached
            if exp is not None and exp - time.time() > _REFRESH_MARGIN_SECONDS:
                return token

        session = load(user_id)
        if session is None:
            return None
        tokens = client.refresh(session["hub_url"], session["refresh_token"])
        session["refresh_token"] = tokens["refresh_token"]
        session["refresh_expires_at"] = _expiry(tokens["refresh_token"])
        secrets.set_password(SERVICE, _key(user_id), json.dumps(session))
        _access_tokens[user_id] = (tokens["access_token"], _expiry(tokens["access_token"]))
        return tokens["access_token"]
