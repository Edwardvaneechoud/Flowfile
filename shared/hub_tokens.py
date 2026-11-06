"""Tokens from a configured issuer, verified offline by core and the worker.

A node on a hub has a caller beside its own core: the hub signs short-lived tokens with a private key and publishes
the public half at ``<FLOWFILE_HUB_URL>/.well-known/jwks.json``. This module is the one verifier of such tokens, shared
by core and the worker so one set of rules applies, and it holds no secret: the keys are public and come from the hub.
``decode`` is the one check of anything the hub signs with that key, the JWS inside a sealed credential included
(``shared.hub_sealed``); ``verify`` is the check of a hub token proper.

The settings are read at call time, from the environment first and then from ``hub_node.json`` in the storage dir, the
file a node writes when it joins a hub (``shared.hub_node``), field by field and the file only for the hub it names.
``FLOWFILE_HUB_URL`` is where the hub is reached and the keys are fetched; ``FLOWFILE_NODE_NAME`` is this node's name on
the hub, the audience a token must name; the ``iss`` claim to expect is the one the joined file keeps, else the URL.
Hub tokens are accepted only when the node has a hub URL and a name; a URL without a name accepts none and says so
once. Nothing runs at import.
"""

import logging
import threading
from dataclasses import dataclass

import jwt

from shared import hub_node

logger = logging.getLogger("flowfile.hub_tokens")

SCOPES = ("run", "signin")
"""What a hub token is for: ``run`` sends work to this node's worker as a user; ``signin`` signs a person in on this
node's core once, and is taken only by the route that does that."""
ALGORITHMS = ["ES256", "RS256"]
LEEWAY_SECONDS = 30
REQUIRED_CLAIMS = ["exp", "iat", "sub", "aud", "scope", "jti"]
JWKS_PATH = "/.well-known/jwks.json"


class HubTokenRefused(Exception):
    """A hub token that does not verify. ``reason`` is fit to answer to the caller and never names the token."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class HubClaims:
    """What a verified hub token says: who (``subject``, and ``user_id`` when it names a user) and for what (``scope``).

    The token's audience, expiry and ``jti`` are checked, not carried: this node is the audience, and a sign-in's
    person is read from the delegation the hub answers, not from the token.
    """

    subject: str
    user_id: int | None
    scope: str


_client: jwt.PyJWKClient | None = None
_client_url: str | None = None
_client_lock = threading.Lock()
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


def _jwks_client(jwks_url: str) -> jwt.PyJWKClient:
    """The process's one key client, built on first use and again when the URL changes.

    PyJWT caches the key set and the keys it matched; an unknown ``kid`` makes it fetch the set again, which is how a
    rotated key reaches a running node.
    """
    global _client, _client_url
    with _client_lock:
        if _client is None or _client_url != jwks_url:
            _client = jwt.PyJWKClient(jwks_url, cache_keys=True)
            _client_url = jwks_url
        return _client


def decode(token: str, jwks_url: str, audience: str, issuer: str, required: list[str] = REQUIRED_CLAIMS) -> dict:
    """The claims of the JWS ``token``, signed with a key the hub publishes at ``jwks_url`` for ``audience`` by
    ``issuer``, with every claim in ``required`` present and ``exp`` not past the leeway.

    Raises:
        HubTokenRefused: The token does not verify; ``reason`` says why in words fit for the caller.
    """
    try:
        key = _jwks_client(jwks_url).get_signing_key_from_jwt(token)
        return jwt.decode(
            token,
            key,
            algorithms=ALGORITHMS,
            audience=audience,
            issuer=issuer,
            leeway=LEEWAY_SECONDS,
            options={"require": required},
        )
    except jwt.ExpiredSignatureError:
        raise HubTokenRefused("expired") from None
    except jwt.InvalidAudienceError:
        raise HubTokenRefused("wrong audience") from None
    except jwt.InvalidIssuerError:
        raise HubTokenRefused("wrong issuer") from None
    except jwt.MissingRequiredClaimError as exc:
        raise HubTokenRefused(f"missing claim {exc.claim}") from None
    except jwt.InvalidAlgorithmError:
        raise HubTokenRefused("algorithm not allowed") from None
    except jwt.InvalidSignatureError:
        raise HubTokenRefused("bad signature") from None
    except jwt.PyJWKClientConnectionError:
        raise HubTokenRefused("keys unavailable") from None
    except jwt.PyJWKClientError:
        raise HubTokenRefused("unknown key") from None
    except jwt.PyJWTError as exc:
        raise HubTokenRefused(f"invalid: {exc}") from None


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
