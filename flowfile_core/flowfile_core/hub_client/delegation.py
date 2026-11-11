"""A person's sign-in through the hub on a joined server: the token exchange, its refresh and its revocation.

Each request carries a node assertion signed with this node's own key, addressed to the hub's issuer and the route's
path, so only the node the sign-in was made for can trade it. Form-encoded, as OAuth's token endpoint is.

Requests log method, path and status only, never a body, an assertion or a token.
"""

from dataclasses import dataclass, field

import httpx

from flowfile_core.configs import logger
from flowfile_core.hub_client import client
from flowfile_core.hub_client.client import HubRefused, HubUnreachable
from shared import hub_node, hub_node_key

TOKEN_PATH = "/hub/token"
REVOKE_PATH = "/hub/token/revoke"
TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
JWT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt"
JWT_BEARER = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"


@dataclass(frozen=True)
class Delegation:
    """The hub's answer to a sign-in or a refresh; lifetimes are seconds from the answer."""

    access_token: str = field(repr=False)
    expires_in: int
    refresh_token: str = field(repr=False)
    refresh_expires_in: int
    user: dict


def _seconds(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _text(value) -> bool:
    return isinstance(value, str) and bool(value)


def _delegation(body, hub_url: str) -> Delegation:
    user = body.get("user") if isinstance(body, dict) else None
    fits = (
        isinstance(user, dict)
        and _seconds(user.get("uid"))
        and _text(user.get("username"))
        and (user.get("full_name") is None or isinstance(user.get("full_name"), str))
        and isinstance(user.get("node_admin"), bool)
        and all(_text(body.get(name)) for name in ("access_token", "refresh_token", "token_type"))
        and all(_seconds(body.get(name)) for name in ("expires_in", "refresh_expires_in"))
    )
    if not fits:
        raise HubRefused(200, f"{hub_url} did not answer {TOKEN_PATH} as a Flowfile hub does.")
    return Delegation(
        access_token=body["access_token"],
        expires_in=body["expires_in"],
        refresh_token=body["refresh_token"],
        refresh_expires_in=body["refresh_expires_in"],
        user={name: user[name] for name in ("uid", "username", "full_name", "node_admin")},
    )


def _assertion(issuer: str, path: str) -> dict:
    """The client assertion fields for ``path``; ``LookupError`` when this storage dir has no joined node with keys,
    so nothing is sent."""
    node = hub_node.read()
    if node is None:
        raise LookupError(f"no node has joined a hub from {hub_node.path().parent}")
    try:
        keys = hub_node_key.load()
    except (OSError, ValueError) as e:
        raise LookupError(f"could not read this node's keys at {hub_node_key.path()}: {type(e).__name__}") from None
    if keys is None:
        raise LookupError(f"this node has no keys of its own at {hub_node_key.path()}")
    return {
        "client_assertion_type": JWT_BEARER,
        "client_assertion": keys.assertion(node.name, issuer.rstrip("/") + path),
    }


def _post(hub_url: str, path: str, form: dict, http: httpx.Client | None):
    """A refusal is the hub's OAuth ``error`` with its ``error_description`` as the detail."""
    url = hub_url.rstrip("/")
    try:
        resp = client._http(http).post(f"{url}{path}", data=form)
    except httpx.TransportError as e:
        raise HubUnreachable(f"Could not reach the hub at {url}.") from e
    logger.info("hub POST %s -> %s", path, resp.status_code)
    try:
        body = resp.json()
    except ValueError:
        body = None
    if resp.status_code >= 400:
        error = body if isinstance(body, dict) and isinstance(body.get("error"), str) else None
        description = error.get("error_description") if error else None
        if not (isinstance(description, str) and description):
            description = f"The hub answered {resp.status_code}."
        raise HubRefused(resp.status_code, description, error)
    return body


def exchange(hub_url: str, issuer: str, sign_in_token: str, *, http: httpx.Client | None = None) -> Delegation:
    """Trade a sign-in token the hub signed for this node for a delegation; ``LookupError`` when this node cannot sign
    for itself."""
    form = {
        "grant_type": TOKEN_EXCHANGE,
        "subject_token": sign_in_token,
        "subject_token_type": JWT_TOKEN_TYPE,
        **_assertion(issuer, TOKEN_PATH),
    }
    return _delegation(_post(hub_url, TOKEN_PATH, form, http), hub_url)


def refresh(hub_url: str, issuer: str, refresh_token: str, *, http: httpx.Client | None = None) -> Delegation:
    """A new delegation for the same sign-in; one the hub no longer takes is ``invalid_grant``."""
    form = {"grant_type": "refresh_token", "refresh_token": refresh_token, **_assertion(issuer, TOKEN_PATH)}
    return _delegation(_post(hub_url, TOKEN_PATH, form, http), hub_url)


def revoke(hub_url: str, issuer: str, refresh_token: str, *, http: httpx.Client | None = None) -> None:
    form = {"token": refresh_token, "token_type_hint": "refresh_token", **_assertion(issuer, REVOKE_PATH)}
    _post(hub_url, REVOKE_PATH, form, http)
