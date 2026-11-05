"""The stored hub connection of each local user, and the access tokens minted from it.

A connection is one JSON string in the secure store (service ``flowfile_hub``, key ``session:<user_id>``) holding
``hub_url``, ``refresh_token``, ``refresh_expires_at``, ``device_name`` and ``org_name``, the hub's organisation as
its ``/hub/info`` named it when the connection was made (so the catalog tree names the hub without asking it again).
It is not a ``Secret`` row,
so it never shows in the Secrets page and no node can reference it. No password is ever part of it. Access tokens
live only in this process's memory; each refresh rotates the refresh token and the new one is stored straight away.
The node tokens minted through a connection (``node_tokens``) go with it when it is replaced or cleared.

On a server that joined a hub, a person who signed in through the hub has a session of ``kind: "delegation"`` instead
(``save_delegation``): ``hub_url``, the delegation's ``refresh_token`` and ``refresh_expires_at``, and ``sid``, the id
of that sign-in, which core's own refresh tokens for it carry. Its tokens are opaque, so their expiries are the hub's
answer counted from when it came, not ``exp`` claims. It is refreshed through ``delegation.refresh``, signed by this
node, and it ends when the hub says so: a refresh the hub answers ``invalid_grant`` (the person signed out everywhere,
disabled or no longer allowed on this node) clears it.

A token's expiry is read from its own ``exp`` claim without verifying the signature: the hub verifies its tokens,
this side only needs to know when to ask for a new one.
"""

import json
import threading
import time
import uuid

import jwt

from flowfile_core.auth import secrets
from flowfile_core.configs import logger
from flowfile_core.hub_client import client, delegation
from flowfile_core.hub_client.delegation import Delegation
from shared import hub_node

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


def account(stored: dict) -> str | None:
    """The hub username a desktop's connection ``stored`` signs in as: its refresh token's ``sub``, read unverified as
    its expiry is. ``None`` for a sign-in through the hub, whose tokens are opaque, or a token without one."""
    if stored.get("kind") == "delegation":
        return None
    try:
        sub = jwt.decode(stored.get("refresh_token") or "", options={"verify_signature": False}).get("sub")
    except jwt.PyJWTError:
        return None
    return sub if isinstance(sub, str) and sub else None


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
        "device_name": session.get("device_name"),
        "org_name": session.get("org_name"),
    }
    with _lock:
        _access_tokens.pop(user_id, None)
        secrets.set_password(SERVICE, _key(user_id), json.dumps(stored))
    _forget_node_tokens(user_id)


def save_delegation(user_id: int, hub_url: str, granted: Delegation) -> str:
    """Store a sign-in through the hub as the session of ``user_id``, replacing any earlier one, cache its access token
    in this process until the hub's ``expires_in`` runs out, and return the sign-in's ``sid``.

    The session holds ``kind: "delegation"``, ``hub_url``, the refresh token, ``refresh_expires_at`` (the hub's
    ``refresh_expires_in`` from now) and ``sid``, a random id core's own tokens for this sign-in carry so that only they
    renew it (``refresh_delegation``); the access token is never stored.
    """
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
    """Forget the connection of ``user_id`` and its node tokens on this machine; the hub is not told."""
    with _lock:
        _access_tokens.pop(user_id, None)
        secrets.delete_password(SERVICE, _key(user_id))
    _forget_node_tokens(user_id)


def _forget_node_tokens(user_id: int) -> None:
    from flowfile_core.hub_client import node_tokens  # imported here: it imports this module

    node_tokens.clear(user_id)


def _ended(refused: client.HubRefused) -> bool:
    """Whether the hub refused a delegation's refresh because the sign-in is over: OAuth's ``invalid_grant``."""
    return isinstance(refused.payload, dict) and refused.payload.get("error") == "invalid_grant"


def _refresh_delegation(user_id: int, stored: dict) -> Delegation:
    """Refresh the delegation ``stored`` at its hub with ``_lock`` held, store the rotated refresh token and cache the
    new access token until the hub's ``expires_in`` runs out.

    A refresh the hub answers ``invalid_grant`` removes the session and its cached access token before the refusal is
    raised; the caller forgets the node tokens once ``_lock`` is released. A node that cannot sign for itself (it left
    the hub, or its keys cannot be read) raises ``HubRefused`` (401) and keeps the session.
    """
    try:
        granted = delegation.refresh(stored["hub_url"], hub_node.resolved().issuer, stored["refresh_token"])
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
    """An access token for the hub ``user_id`` is connected to, ``None`` when there is no connection.

    The token cached in this process is reused until it is within a minute of its ``exp``; otherwise a new one is
    minted through ``client.refresh``, and the rotated refresh token is stored before the access token is returned.
    A hub that refuses the refresh raises the client's error and leaves the stored connection as it was. A sign-in
    through the hub (``kind: "delegation"``) is refreshed through ``delegation.refresh`` instead, and one the hub
    answers ``invalid_grant`` is cleared, node tokens included, before the refusal is raised.
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


def refresh_delegation(user_id: int, sid: str | None) -> Delegation | None:
    """Refresh the sign-in through the hub ``sid`` names at the hub now, whatever access token is cached, and keep what
    it answers; ``None``, with nothing asked, when ``user_id`` has no sign-in through the hub here or the one kept is
    not ``sid`` (it ended, or a later sign-in replaced it).

    Raises as ``access_token`` does for a delegation: ``HubRefused``, the session cleared first when the hub answered
    ``invalid_grant``, or ``HubUnreachable`` with the session kept.
    """
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
