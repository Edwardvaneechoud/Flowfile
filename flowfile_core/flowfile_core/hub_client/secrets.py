"""A person's secrets on their hub, for any machine signed in to it: a desktop's Connect or a joined server's
delegation, both ``session.load``.

The hub's secrets are never copied here and have no value on any machine. The Secrets page lists them after the
machine's own, read-only and marked ``source="hub"`` (``listed``, ``hub_rows``); a step that names one carries it only
as the run's vend sealed it to the hub server that runs the step (``run_secrets.hub_value``).

Requests log method, path and status only, never a body, a value or a token.
"""

from __future__ import annotations

import time
from collections.abc import Callable

from flowfile_core.configs import logger
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.catalog import HUB
from flowfile_core.hub_client.client import HubRefused

SECRETS_PATH = "/secrets/secrets"
NOT_SIGNED_IN = "Your sign-in through the hub has ended; sign in again"
LISTED_SECONDS = 10
_listed: dict[tuple, tuple[float, list[dict]]] = {}


def listed_once(key: tuple, read: Callable[[], list[dict]], what: str) -> list[dict]:
    """``read()``'s rows, kept ``LISTED_SECONDS`` under ``key``: a run asks for the same list at every step that names
    a connection. A hub that does not answer lists none, for as long."""
    held = _listed.get(key)
    if held is not None and time.monotonic() - held[0] < LISTED_SECONDS:
        return held[1]
    try:
        rows = read()
    except Exception as e:
        logger.warning(f"The hub's {what} are not listed: {e}")
        rows = []
    _listed[key] = (time.monotonic(), rows)
    return rows


def _signed_in(user_id: int) -> tuple[str, str]:
    """The hub's URL and an access token from ``user_id``'s sign-in through it; ``LookupError`` when there is none."""
    stored = session.load(user_id)
    if stored is None:
        raise LookupError(NOT_SIGNED_IN)
    try:
        token = session.access_token(user_id)
    except HubRefused as e:
        if e.status in (400, 401):
            raise LookupError(NOT_SIGNED_IN) from e
        raise
    if token is None:
        raise LookupError(NOT_SIGNED_IN)
    return stored["hub_url"], token


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def marked(row: dict) -> dict:
    """A row the hub lists, marked ``source="hub"``; the hub's id, negated so it is never one of this machine's."""
    row_id = row.get("id")
    return {**row, "source": HUB, "id": -row_id if isinstance(row_id, int) else row_id}


def listed(user_id: int) -> list[dict]:
    """The person's secrets as the hub lists them: names and metadata, never a value, each marked ``source="hub"``."""
    hub_url, token = _signed_in(user_id)
    rows = client._request_list("GET", hub_url, SECRETS_PATH, None, headers=_bearer(token))
    return [{**marked(row), "value": None} for row in rows]


def hub_rows(user_id: int) -> list[dict]:
    """``listed``, or none when the person has no sign-in to a hub here or the hub does not answer."""
    if session.load(user_id) is None:
        return []
    return listed_once((user_id, "secret"), lambda: listed(user_id), "secrets")


def found(user_id: int, name: str | None) -> dict | None:
    """The hub's list row of the person's secret ``name``; ``None`` when the hub lists none."""
    if not name:
        return None
    return next((row for row in hub_rows(user_id) if row.get("name") == name), None)
