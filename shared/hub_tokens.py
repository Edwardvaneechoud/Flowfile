"""Tokens from a configured issuer, verified offline by core and the worker.

A node on a hub has a caller beside its own core: the hub signs short-lived tokens with a private key and publishes
the public half at ``<FLOWFILE_HUB_URL>/.well-known/jwks.json``. This module says who that issuer is on this machine and
what a hub token must carry; the check of the signature is ``shared.issuer_tokens.decode``, the one verifier core, the
worker and a sealed credential (``shared.hub_sealed``) share. ``verify`` is the check of a hub token proper.

The settings are read at call time, from the environment first and then from ``hub_node.json`` in the storage dir, the
file a node writes when it joins a hub (``shared.hub_node``), field by field and the file only for the hub it names.
``FLOWFILE_HUB_URL`` is where the hub is reached and the keys are fetched; ``FLOWFILE_NODE_NAME`` is this node's name on
the hub, the audience a token must name; the ``iss`` claim to expect is the one the joined file keeps, else the URL.
Hub tokens are accepted only when the node has a hub URL and a name; a URL without a name accepts none and says so
once. Nothing runs at import.
"""

import logging
from dataclasses import dataclass

from shared import hub_node
from shared.issuer_tokens import JWKS_PATH, REQUIRED_CLAIMS, TokenRefused, decode

logger = logging.getLogger("flowfile.hub_tokens")

SCOPES = ("run", "signin", "dispatch")
"""What a hub token is for: ``run`` sends work to this node's worker as a user; ``signin`` signs a person in on this
node's core once, and is taken only by the route that does that; ``dispatch`` has this node's core run one version of a
hub flow as a person, and is taken only by ``POST /hub/dispatch``."""
DISPATCH_CLAIMS = ["run_id", "flow_uuid", "flow_version", "uid"]

HubTokenRefused = TokenRefused
"""A hub token that does not verify: the verifier's own refusal under the name the hub's callers catch."""


@dataclass(frozen=True)
class HubClaims:
    """What a verified hub token says: who (``subject``, and ``user_id`` when it names a user) and for what (``scope``).

    The token's audience, expiry and ``jti`` are checked, not carried: this node is the audience, and a sign-in's
    person is read from the delegation the hub answers, not from the token.
    """

    subject: str
    user_id: int | None
    scope: str


@dataclass(frozen=True)
class DispatchClaims:
    """What a verified ``dispatch`` token says: the person (``subject``, ``user_id``), the run the hub keeps it as
    (``run_id``), the flow version to check out and run (``flow_uuid``, ``flow_version``), and when the token expires
    (``expires_at``, epoch seconds), which bounds how long the run id is remembered against a replay."""

    subject: str
    user_id: int
    run_id: str
    flow_uuid: str
    flow_version: int
    expires_at: int


_warned_no_node_name = False


def _settings() -> tuple[str, str, str] | None:
    """``(jwks_url, node_name, issuer)`` while hub tokens are on, else ``None``."""
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
    """The claims of ``token`` when it is a hub token this node accepts for one of ``scopes``.

    ``None`` when ``token`` is not a JWT (fewer than two dots), decided before the node's settings are read, or when
    hub tokens are off, so a caller can try the token it does know first and ask here after; ``HubTokenRefused`` for a
    JWT that does not verify, with the reason. A token of a scope the caller does not name is refused like any other,
    so a sign-in token is no user's token anywhere but on the sign-in route, which names ``("signin",)``.
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
    """The claims of ``token`` when it is a ``dispatch`` token this node accepts; ``None`` and ``HubTokenRefused`` as
    ``verify`` answers them. Every claim of ``DISPATCH_CLAIMS`` must be there and well formed: ``run_id`` and
    ``flow_uuid`` non-empty text of at most 64 characters, ``flow_version`` and ``uid`` whole numbers."""
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
