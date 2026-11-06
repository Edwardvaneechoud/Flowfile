"""A person's secrets on the hub, for a server that joined it with keys of its own: there every secret lives on the hub.

The Secrets page's routes forward to the hub's own ``/secrets`` routes with the person's delegated access token
(``listed``, ``found``, ``create``, ``change``, ``remove``), so nothing of them is written to this server's database
and sharing stays on the hub's pages. A lookup by name asks the hub's vend for the secret sealed to this server itself
(``sealed_here``), which this server's core and worker open with its own key, as a server of the hub opens what is
sealed to it. A named secret this server's database held before it joined stays there, unused; a stored connection
lives on the hub with its secrets (``hub_client.connections``). The order a name resolves in is the hub's.

Requests log method, path and status only, never a body, a value or a token.
"""

from __future__ import annotations

import uuid

from flowfile_core.hub_client import client, session
from flowfile_core.hub_client.client import HubRefused, HubUnreachable, HubVersionRefused
from shared import hub_node, hub_sealed

SECRETS_PATH = "/secrets/secrets"
NOT_SIGNED_IN = "Your secrets are on the hub, and your sign-in through it has ended; sign in again"
NO_SUCH = "You have no secret named {name} on the hub"
"""How the hub's 403 to a vend begins for a name the person reaches no secret of."""


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


def _path(name: str) -> str:
    from urllib.parse import quote

    return f"{SECRETS_PATH}/{quote(name, safe='')}"


def listed(user_id: int) -> list[dict]:
    """The person's secrets as the hub lists them: names and metadata, never a value."""
    hub_url, token = _signed_in(user_id)
    return client._request_list("GET", hub_url, SECRETS_PATH, None, headers=_bearer(token))


def found(user_id: int, name: str) -> dict:
    """The person's secret ``name`` as the hub answers it; ``HubRefused`` 404 when the hub has none they reach."""
    hub_url, token = _signed_in(user_id)
    return client._request("GET", hub_url, _path(name), None, headers=_bearer(token))


def create(user_id: int, name: str, value: str) -> dict:
    """Add the secret ``name`` to the person's own secrets on the hub; the hub's 400 when they hold one of that name."""
    hub_url, token = _signed_in(user_id)
    body = {"name": name, "value": value}
    return client._request("POST", hub_url, SECRETS_PATH, None, json=body, headers=_bearer(token))


def change(user_id: int, name: str, value: str) -> dict:
    """Give the person's own secret ``name`` on the hub a new value, keeping it and its shares; the hub's 404 else."""
    hub_url, token = _signed_in(user_id)
    return client._request("PUT", hub_url, _path(name), None, json={"value": value}, headers=_bearer(token))


def remove(user_id: int, name: str) -> None:
    """Delete the person's own secret ``name`` on the hub; the hub's 404 when they own none of that name."""
    hub_url, token = _signed_in(user_id)
    client._send("DELETE", hub_url, _path(name), None, headers=_bearer(token))


def lookup_run_id() -> str:
    """The run id a lookup outside a run's vend asks with; the hub's ledger records it as given."""
    return f"core-lookup-{uuid.uuid4().hex}"


def sealed_here(user_id: int, name: str, run_id: str | None = None) -> str | None:
    """The person's secret ``name`` as the hub resolves it for them, sealed to this server: a ``sealed_secret`` string
    that this server's core and worker open with its own key. ``None`` when the hub says they reach no secret of that
    name.

    Raises:
        LookupError: A sentence: the sign-in through the hub ended, the hub refused for another reason, or it did not
            answer.
    """
    from flowfile_core.hub_client import vend

    here = hub_node.resolved().name
    hub_url, token = _signed_in(user_id)
    try:
        answer = client.vend(hub_url, token, run_id or lookup_run_id(), [], [], targets=[here], secrets=[name])
    except HubRefused as e:
        if e.status == 403 and e.detail.startswith(NO_SUCH.format(name=name)):
            return None
        if e.status == 401:
            raise LookupError(NOT_SIGNED_IN) from e
        raise LookupError(vend.refusal_sentence(e.detail, e.status)) from e
    except HubVersionRefused as e:
        raise LookupError(vend.refusal_sentence(e.detail, 426)) from e
    except HubUnreachable as e:
        raise LookupError(str(e)) from e
    sealed = next(secret["sealed"] for secret in answer["secrets"] if secret.get("name") == name)
    return hub_sealed.sealed_secret(name, sealed)
