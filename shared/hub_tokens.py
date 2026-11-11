"""Hub tokens, verified offline by core and the worker against the hub's JWKS (``shared.issuer_tokens.decode``).

Settings are read at call time (``hub_node.resolved``); tokens are accepted only when the node has a hub URL and a
name, the audience a token must name.
"""

import logging
from dataclasses import dataclass

from shared import hub_node
from shared.issuer_tokens import JWKS_PATH, REQUIRED_CLAIMS, TokenRefused, decode

logger = logging.getLogger("flowfile.hub_tokens")

SCOPES = ("run", "signin", "dispatch")
DISPATCH_CLAIMS = ["run_id", "flow_uuid", "flow_version", "uid"]

HubTokenRefused = TokenRefused


@dataclass(frozen=True)
class HubClaims:
    """A verified hub token; a sign-in's person is read from the hub's delegation, not from the token."""

    subject: str
    user_id: int | None
    scope: str


@dataclass(frozen=True)
class DispatchClaims:
    """A verified ``dispatch`` token; ``expires_at`` bounds how long the run id is remembered against a replay."""

    subject: str
    user_id: int
    run_id: str
    flow_uuid: str
    flow_version: int
    expires_at: int


_warned_no_node_name = False


def _settings() -> tuple[str, str, str] | None:
    global _warned_no_node_name
    node = hub_node.resolved()
    if not node.hub_url:
        return None
    if not node.name:
        if not _warned_no_node_name:
            logger.warning(
                "A hub URL is set but this node has no name on it (FLOWFILE_NODE_NAME, or join the hub): "
                "no hub token is accepted until it has one"
            )
            _warned_no_node_name = True
        return None
    return node.hub_url + JWKS_PATH, node.name, node.issuer


def verify(token: str, scopes: tuple[str, ...] = ("run",)) -> HubClaims | None:
    """The claims of a hub token accepted for one of ``scopes``; ``None`` for a non-JWT or with hub tokens off.

    A token of a scope the caller does not name is refused, so a sign-in token is no user's token anywhere but on the
    sign-in route.
    """
    if not isinstance(token, str) or token.count(".") < 2:
        return None
    settings = _settings()
    if settings is None:
        return None
    payload = decode(token, *settings)
    scope = payload["scope"]
    if scope not in SCOPES:
        raise HubTokenRefused("unknown scope")
    if scope not in scopes:
        raise HubTokenRefused(f"a {scope} token is not accepted here")
    uid = payload.get("uid")
    try:
        user_id = None if uid is None else int(uid)
    except (TypeError, ValueError):
        raise HubTokenRefused("malformed claim uid") from None
    return HubClaims(subject=str(payload["sub"]), user_id=user_id, scope=scope)


def verify_dispatch(token: str) -> DispatchClaims | None:
    """``verify`` for a ``dispatch`` token, every claim of ``DISPATCH_CLAIMS`` there and well formed."""
    if not isinstance(token, str) or token.count(".") < 2:
        return None
    settings = _settings()
    if settings is None:
        return None
    payload = decode(token, *settings, required=REQUIRED_CLAIMS + DISPATCH_CLAIMS)
    if payload["scope"] != "dispatch":
        raise HubTokenRefused(f"a {payload['scope']} token is not accepted here")
    run_id, flow_uuid = payload["run_id"], payload["flow_uuid"]
    for name, value in (("run_id", run_id), ("flow_uuid", flow_uuid)):
        if not isinstance(value, str) or not 0 < len(value) <= 64:
            raise HubTokenRefused(f"malformed claim {name}")
    for name in ("flow_version", "uid"):
        if not isinstance(payload[name], int) or isinstance(payload[name], bool):
            raise HubTokenRefused(f"malformed claim {name}")
    return DispatchClaims(
        subject=str(payload["sub"]),
        user_id=payload["uid"],
        run_id=run_id,
        flow_uuid=flow_uuid,
        flow_version=payload["flow_version"],
        expires_at=int(payload["exp"]),
    )
