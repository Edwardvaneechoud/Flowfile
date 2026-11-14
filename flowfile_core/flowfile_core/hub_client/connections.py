"""The stored connections a person reaches on their hub, listed read-only after the machine's own; their secrets reach
a step only as the run's vend sealed them to the hub server that runs it, and core never opens them.
``_HubCredentials`` registers the hub as core's credential source on import.

Requests log method, path and status only, never a body, a value or a token.
"""

from __future__ import annotations

from typing import Literal

from flowfile_core.hub_client import secrets as hub_secrets
from flowfile_core.hub_client.secrets import listed_once, rows_at
from flowfile_core.secret_manager import credential_sources
from shared import sealed

Kind = Literal["cloud", "kafka", "database"]

LIST_PATHS = {
    "cloud": "/cloud_connections/cloud_connections",
    "kafka": "/kafka/connections",
    "database": "/db_connection_lib",
}
LABELS = {"cloud": "cloud storage connection", "kafka": "Kafka connection", "database": "database connection"}


def listed(user_id: int, kind: Kind) -> list[dict]:
    return rows_at(user_id, LIST_PATHS[kind])


def hub_rows(user_id: int, kind: Kind) -> list[dict]:
    return listed_once(user_id, kind, lambda: listed(user_id, kind), f"{LABELS[kind]}s")


class _HubCredentials:
    """The hub as ``credential_sources.CredentialSource``."""

    def secret_rows(self, user_id: int) -> list[dict]:
        return hub_secrets.hub_rows(user_id)

    def connection_rows(self, user_id: int, kind: Kind) -> list[dict]:
        return hub_rows(user_id, kind)

    carried = staticmethod(sealed.sealed_connection_secret)
    storage_provider = staticmethod(sealed.SealedConnectionProvider)

    def unopened(self, value: object) -> str | None:
        if sealed.is_sealed_secret(value):
            return "A hub secret is opened only by the hub server that runs the step, never by core"
        return None

    def refusal(self, name: str | None, *, secret: bool = False, why: str | None = None) -> str:
        what = f"{name} is a hub {'secret' if secret else 'connection'}, which only a hub server opens"
        return f"{what}, and {why}" if why else f"{what}: run this step on a hub server"


credential_sources.register(_HubCredentials())
