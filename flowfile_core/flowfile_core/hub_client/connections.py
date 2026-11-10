"""The stored connections a person reaches on their hub, for any machine signed in to it: a desktop's Connect or a
joined server's delegation, both ``session.load``.

A connection is settings plus secrets, and the hub's are never copied here. Each connection page lists them after the
machine's own, read-only and marked ``source="hub"`` (``listed``, ``hub_rows``); a step that names one gets its list
row, its settings with no secret, and its secrets only as the run's vend sealed them to the hub server that
runs the step (``run_secrets.granted_value``). Core never opens them. ``_HubCredentials`` is the hub as core's provider
of credentials (``secret_manager.credential_sources``), registered when this module is imported.

Requests log method, path and status only, never a body, a value or a token.
"""

from __future__ import annotations

from typing import Literal

from flowfile_core.hub_client import client, session
from flowfile_core.hub_client import secrets as hub_secrets
from flowfile_core.hub_client.secrets import _bearer, _signed_in, listed_once, marked
from flowfile_core.secret_manager import credential_sources
from shared import hub_sealed

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


class _HubCredentials:
    """The hub as ``credential_sources.CredentialSource``: its list rows, and the forms its vend seals a run's
    secrets in, which only the hub server that runs the step opens."""

    def secret_rows(self, user_id: int) -> list[dict]:
        return hub_secrets.hub_rows(user_id)

    def connection_rows(self, user_id: int, kind: Kind) -> list[dict]:
        return hub_rows(user_id, kind)

    carried = staticmethod(hub_sealed.sealed_connection_secret)
    storage_provider = staticmethod(hub_sealed.SealedConnectionProvider)

    def unopened(self, value: object) -> str | None:
        if hub_sealed.is_sealed_secret(value):
            return "A hub secret is opened only by the hub server that runs the step, never by core"
        return None

    def refusal(self, name: str | None, *, secret: bool = False, why: str | None = None) -> str:
        what = f"{name} is a hub {'secret' if secret else 'connection'}, which only a hub server opens"
        return f"{what}, and {why}" if why else f"{what}: run this step on a hub server"


credential_sources.register(_HubCredentials())
