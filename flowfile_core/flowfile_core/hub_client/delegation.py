"""A person's sign-in through the hub on a server that joined it: the token exchange, its refresh and its revocation.

The hub's Open page sends a person's browser to this server's editor with a one-time sign-in token. The server trades
it at the hub's ``POST /hub/token`` for a delegation: a short access token it sends the hub's routes for that person,
as a desktop sends its session's, and a refresh token that rotates on every use. Each request carries a node
assertion signed with this node's own key (``shared.hub_node_key``), addressed to the issuer the hub signs with and the
route's path, so only the node the sign-in was made for can trade it. The requests are form-encoded, as OAuth's token
endpoint is; the hub answers a refusal with ``error`` and one sentence in ``error_description``.

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
    """The hub's answer to a sign-in or a refresh: the two tokens, how long each lives, and who the person is.

    ``expires_in`` and ``refresh_expires_in`` are seconds from the answer; ``user`` is the hub's ``uid``,
    ``username``, ``full_name`` and ``node_admin``. Neither token is part of its ``repr``. The answer's
    ``token_type`` is checked to be there, as an OAuth token answer must have one, and the access token is sent as a
    bearer whatever it says.
    """

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
    """The client assertion fields for a request to ``path``, signed as the node this storage dir joined.

    ``LookupError`` when no node joined a hub from this storage dir or it has no keys of its own, so nothing is sent.
    """
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
    """Trade a sign-in token the hub signed for this node for a delegation to act for the person it names.

    Raises ``HubRefused`` with the hub's ``error_description`` (``payload`` holds the hub's ``error`` and the
    description) when the hub refuses, ``HubUnreachable`` when it does not answer, ``LookupError`` when this node
    cannot sign for itself.
    """
    form = {
        "grant_type": TOKEN_EXCHANGE,
        "subject_token": sign_in_token,
        "subject_token_type": JWT_TOKEN_TYPE,
        **_assertion(issuer, TOKEN_PATH),
    }
    return _delegation(_post(hub_url, TOKEN_PATH, form, http), hub_url)


def refresh(hub_url: str, issuer: str, refresh_token: str, *, http: httpx.Client | None = None) -> Delegation:
    """A new delegation for the same sign-in; ``refresh_token`` is refused by the hub from then on.

    The sign-in's twelve hours run from the sign-in, so ``refresh_expires_in`` shrinks with every refresh. Raises as
    ``exchange`` does; a refresh the hub no longer takes is ``HubRefused`` with ``payload["error"] == "invalid_grant"``.
    """
    form = {"grant_type": "refresh_token", "refresh_token": refresh_token, **_assertion(issuer, TOKEN_PATH)}
    return _delegation(_post(hub_url, TOKEN_PATH, form, http), hub_url)


def revoke(hub_url: str, issuer: str, refresh_token: str, *, http: httpx.Client | None = None) -> None:
    """End the delegation ``refresh_token`` belongs to; the hub answers the same whether or not it was alive.

    Raises as ``exchange`` does.
    """
    form = {"token": refresh_token, "token_type_hint": "refresh_token", **_assertion(issuer, REVOKE_PATH)}
    _post(hub_url, REVOKE_PATH, form, http)
