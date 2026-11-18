"""A person's secrets on their hub, listed read-only after the machine's own; a value is never copied here and reaches a
step only as the run's vend sealed it to the hub server that runs the step.

Requests log method, path and status only, never a body, a value or a token.
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable

from flowfile_core.configs import logger
from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.catalog import HUB

SECRETS_PATH = "/secrets/secrets"
NOT_SIGNED_IN = "Your sign-in through the hub has ended; sign in again"
LISTED_SECONDS = 10
_PRINCIPAL = re.compile(r"(team|org):\S+")
_listed: dict[tuple, tuple[float, list[dict]]] = {}


def listed_once(user_id: int, key: str, read: Callable[[], list[dict]], what: str) -> list[dict]:
    """``read()``'s rows kept ``LISTED_SECONDS``, since a run asks at every step; none without a hub session, and a hub
    that does not answer lists none for as long."""
    if session.load(user_id) is None:
        return []
    held = _listed.get((user_id, key))
    if held is not None and time.monotonic() - held[0] < LISTED_SECONDS:
        return held[1]
    try:
        rows = read()
    except Exception as e:
        logger.warning(f"The hub's {what} are not listed: {e}")
        rows = []
    _listed[(user_id, key)] = (time.monotonic(), rows)
    return rows


def marked(row: dict, place: str) -> dict:
    """A hub row marked ``source="hub"``, its id negated so it never meets a local one, and shared for use, by the
    person who shared it on the hub or else by the hub named ``place`` (a team's or the organisation's account id is
    never shown): this machine changes none of the hub's."""
    row_id = row.get("id")
    given = (row.get("access") or {}).get("shared_by")
    shared_by = given if given and not _PRINCIPAL.fullmatch(given) else place
    access = {"is_owner": False, "access_level": "use", "shared_by": shared_by}
    return {**row, "source": HUB, "id": -row_id if isinstance(row_id, int) else row_id, "access": access}


def rows_at(user_id: int, path: str) -> list[dict]:
    hub_url, token = session.bearer(user_id, NOT_SIGNED_IN, ended=(400, 401))
    place = (session.load(user_id) or {}).get("org_name") or hub_url
    listed = client._request_list("GET", hub_url, path, None, headers=client._bearer(token))
    return [marked(row, place) for row in listed]


def listed(user_id: int) -> list[dict]:
    return [{**row, "value": None} for row in rows_at(user_id, SECRETS_PATH)]


def hub_rows(user_id: int) -> list[dict]:
    return listed_once(user_id, "secret", lambda: listed(user_id), "secrets")
