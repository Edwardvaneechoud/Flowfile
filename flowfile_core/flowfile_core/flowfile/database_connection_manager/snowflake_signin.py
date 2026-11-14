"""Pending Snowflake browser sign-ins, held in this process until a connection is saved with one.

A sign-in starts in core (``/snowflake/oauth/start``), finishes when Snowflake redirects the browser to the
sign-in's own loopback listener (``snowflake_callback``), and is claimed once by the save of a connection
(``oauth_flow_id``). Between those steps the PKCE verifier and, after the callback, the tokens live only here: in
memory, keyed by a random id, behind one lock, for at most ``TTL_SECONDS``. Nothing is written to disk, so a sign-in
dies with the process; the user signs in again. The verifier and the tokens never appear in a URL or a response.

Each sign-in may carry a ``closer`` (its listener's ``close``), called whenever the sign-in leaves the table:
expired, claimed or forgotten. Closers run after the lock is released, so a closer may itself read the table.
"""

from __future__ import annotations

import secrets
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, Literal

from shared.snowflake_auth import SnowflakeAuthError

TTL_SECONDS = 600

Status = Literal["pending", "exchanging", "ok", "error"]

_EXPIRED = "This Snowflake sign-in has expired. Sign in again."


def _now() -> float:
    return time.monotonic()


@dataclass
class SignIn:
    flow_id: str
    user_id: int
    host: str
    role: str | None
    verifier: str
    redirect_uri: str
    created: float
    status: Status = "pending"
    message: str | None = None
    refresh_token: str | None = field(default=None, repr=False)
    access_token: str | None = field(default=None, repr=False)
    username: str | None = None
    session: dict[str, Any] = field(default_factory=dict)

    @property
    def public_status(self) -> Literal["pending", "ok", "error"]:
        return "pending" if self.status == "exchanging" else self.status


_flows: dict[str, SignIn] = {}
_closers: dict[str, Callable[[], None]] = {}
_lock = threading.Lock()


def _drop_locked(flow_id: str) -> list[Callable[[], None]]:
    _flows.pop(flow_id, None)
    closer = _closers.pop(flow_id, None)
    return [closer] if closer else []


def _purge_locked() -> list[Callable[[], None]]:
    cutoff = _now() - TTL_SECONDS
    closers: list[Callable[[], None]] = []
    for flow_id in [k for k, v in _flows.items() if v.created <= cutoff]:
        closers += _drop_locked(flow_id)
    return closers


def _run(closers: list[Callable[[], None]]) -> None:
    for close in closers:
        try:
            close()
        except Exception:
            pass


def start(
    *,
    user_id: int,
    host: str,
    role: str | None,
    verifier: str,
    redirect_uri: str,
    closer: Callable[[], None] | None = None,
) -> SignIn:
    """Record a new pending sign-in and return it (its ``flow_id`` is random); ``closer`` runs when it leaves."""
    flow = SignIn(
        flow_id=secrets.token_urlsafe(24),
        user_id=user_id,
        host=host,
        role=role,
        verifier=verifier,
        redirect_uri=redirect_uri,
        created=_now(),
    )
    with _lock:
        expired = _purge_locked()
        _flows[flow.flow_id] = flow
        if closer is not None:
            _closers[flow.flow_id] = closer
    _run(expired)
    return replace(flow)


def get(flow_id: str) -> SignIn | None:
    """A copy of the sign-in, or None when it is unknown or expired."""
    with _lock:
        expired = _purge_locked()
        flow = _flows.get(flow_id)
        found = replace(flow) if flow is not None else None
    _run(expired)
    return found


def get_for_user(flow_id: str, user_id: int) -> SignIn | None:
    """A copy of the sign-in when it belongs to ``user_id``; None when it is unknown, expired or someone else's."""
    flow = get(flow_id)
    return flow if flow is not None and flow.user_id == user_id else None


def begin_exchange(flow_id: str) -> SignIn | None:
    """Move a pending sign-in to exchanging and return it; None when it is unknown, expired or already past pending.

    The one gate a callback passes, so a second callback for the same sign-in never overwrites the first.
    """
    with _lock:
        expired = _purge_locked()
        flow = _flows.get(flow_id)
        taken = None
        if flow is not None and flow.status == "pending":
            flow.status = "exchanging"
            taken = replace(flow)
    _run(expired)
    return taken


def finish(flow_id: str, *, status: Literal["ok", "error"], message: str | None = None, **values: Any) -> None:
    """Record how an exchanging sign-in ended; a sign-in that is gone or already finished is left alone."""
    with _lock:
        flow = _flows.get(flow_id)
        if flow is None or flow.status in ("ok", "error"):
            return
        flow.status = status
        flow.message = message
        for key, value in values.items():
            setattr(flow, key, value)


def claim(flow_id: str, *, user_id: int, host: str, role: str | None) -> SignIn:
    """Take a finished sign-in for the connection being saved; a sign-in can be claimed once.

    It must exist, have succeeded, belong to ``user_id`` and have been made for the same account ``host`` and
    ``role`` as the connection, since a Snowflake OAuth token is bound to the role it was issued for. Raises
    ``SnowflakeAuthError`` with the reason otherwise, and then leaves the sign-in where it was.
    """
    with _lock:
        closers = _purge_locked()
        flow = _flows.get(flow_id)
        problem = None
        if flow is None or flow.user_id != user_id:
            problem = _EXPIRED
        elif flow.status != "ok":
            problem = "The Snowflake sign-in has not finished. Complete it in your browser first."
        elif flow.host != host or (flow.role or None) != (role or None):
            problem = (
                "The account or role changed after you signed in, so the sign-in no longer applies. Sign in again."
            )
        else:
            closers += _drop_locked(flow_id)
    _run(closers)
    if problem:
        raise SnowflakeAuthError(problem)
    return flow


def forget(flow_id: str) -> None:
    """Drop one sign-in (closing its listener)."""
    with _lock:
        closers = _drop_locked(flow_id)
    _run(closers)


def forget_all() -> None:
    """Drop every sign-in and close every listener (tests, shutdown)."""
    with _lock:
        closers = list(_closers.values())
        _flows.clear()
        _closers.clear()
    _run(closers)
