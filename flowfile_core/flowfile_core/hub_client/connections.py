"""The stored connections a person reaches on their hub, for any machine signed in to it: a desktop's Connect or a
joined server's delegation, both ``session.load``.

A connection is settings plus secrets, and the hub's are never copied here. Each connection page lists them after the
machine's own, read-only and marked ``source="hub"`` (``listed``, ``hub_rows``); a step that names one gets its list
row, its settings with no secret (``found``), and its secrets only as the run's vend sealed them to the hub server that
runs the step (``run_secrets.hub_value``). Core never opens them.

Requests log method, path and status only, never a body, a value or a token.
"""

from __future__ import annotations

from typing import Literal

from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.secrets import _bearer, _signed_in, listed_once, marked

Kind = Literal["cloud", "kafka", "database"]

LIST_PATHS = {
    "cloud": "/cloud_connections/cloud_connections",
    "kafka": "/kafka/connections",
    "database": "/db_connection_lib",
}
LABELS = {"cloud": "cloud storage connection", "kafka": "Kafka connection", "database": "database connection"}


def listed(user_id: int, kind: Kind) -> list[dict]:
    """The person's connections of ``kind`` as the hub lists them: their own and those shared with them, no secret,
    each marked ``source="hub"`` (``secrets.marked``)."""
    hub_url, token = _signed_in(user_id)
    rows = client._request_list("GET", hub_url, LIST_PATHS[kind], None, headers=_bearer(token))
    return [marked(row) for row in rows]


def hub_rows(user_id: int, kind: Kind) -> list[dict]:
    """``listed``, or none when the person has no sign-in to a hub here or the hub does not answer."""
    if session.load(user_id) is None:
        return []
    return listed_once((user_id, kind), lambda: listed(user_id, kind), f"{LABELS[kind]}s")


def found(user_id: int, kind: Kind, name: str | None) -> dict | None:
    """The hub's list row of the person's ``kind`` connection ``name``, no secret; ``None`` when the hub lists none."""
    if not name:
        return None
    return next((row for row in hub_rows(user_id, kind) if row.get("connection_name") == name), None)
