"""A person's secrets on their hub, listed read-only after the machine's own; a value is never copied here and reaches a
step only as the run's vend sealed it to the hub server that runs the step.

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
    """``read()``'s rows kept ``LISTED_SECONDS``, since a run asks at every step; a hub that does not answer lists
    none for as long."""
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
    """A hub row marked ``source="hub"``, its id negated so it never meets a local one."""
    row_id = row.get("id")
    return {**row, "source": HUB, "id": -row_id if isinstance(row_id, int) else row_id}


def listed(user_id: int) -> list[dict]:
    hub_url, token = _signed_in(user_id)
    rows = client._request_list("GET", hub_url, SECRETS_PATH, None, headers=_bearer(token))
    return [{**marked(row), "value": None} for row in rows]


def hub_rows(user_id: int) -> list[dict]:
    """``listed``, or none without a hub session or when the hub does not answer."""
    if session.load(user_id) is None:
        return []
    return listed_once((user_id, "secret"), lambda: listed(user_id), "secrets")
