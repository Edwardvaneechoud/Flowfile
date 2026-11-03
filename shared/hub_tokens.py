"""Tokens from a configured issuer, verified offline by core and the worker.

A node on a hub has a caller beside its own core: the hub signs short-lived tokens with a private key and publishes
the public half at ``<FLOWFILE_HUB_URL>/.well-known/jwks.json``. This module is the one verifier of such tokens, shared
by core and the worker so one set of rules applies, and it holds no secret: the keys are public and come from the hub.

The settings are read at call time, from the environment first and then from ``hub_node.json`` in the storage dir, the
file a node writes when it joins a hub (``shared.hub_node``), field by field and the file only for the hub it names.
``FLOWFILE_HUB_URL`` is where the hub is reached and the keys are fetched; ``FLOWFILE_NODE_NAME`` is this node's name on
the hub, the audience a token must name; ``FLOWFILE_HUB_ISSUER`` is the ``iss`` claim to expect, the URL when unset, for
a node that reaches the hub by another address than the one the hub signs with. Hub tokens are accepted only when the
node has a hub URL and a name; a URL without a name accepts none and says so once. Nothing runs at import.
"""

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

import jwt

from shared import hub_node

logger = logging.getLogger("flowfile.hub_tokens")

SCOPES = ("run", "dispatch")
"""What a hub token is for: ``run`` sends work to this node as a user, ``dispatch`` starts a run as the hub itself."""
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
    """What a verified hub token says: who (``subject``, and ``user_id`` when it names a user), for what, which node.

    ``node`` is this node's name, the audience the token was checked against.
    """

    subject: str
    user_id: int | None
    scope: str
    node: str
    jti: str
    expires_at: datetime


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


def _decode(token: str, jwks_url: str, node_name: str, issuer: str) -> dict:
    try:
        key = _jwks_client(jwks_url).get_signing_key_from_jwt(token)
        return jwt.decode(
            token,
            key,
            algorithms=ALGORITHMS,
            audience=node_name,
            issuer=issuer,
            leeway=LEEWAY_SECONDS,
            options={"require": REQUIRED_CLAIMS},
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


def verify(token: str) -> HubClaims | None:
    """The claims of ``token`` when it is a hub token this node accepts.

    ``None`` when hub tokens are off or ``token`` is not a JWT (fewer than two dots), so a caller can try the token
    it does know first and ask here after; ``HubTokenRefused`` for a JWT that does not verify, with the reason.
    """
    settings = _settings()
    if settings is None or not isinstance(token, str) or token.count(".") < 2:
        return None
    payload = _decode(token, *settings)
    scope = payload["scope"]
    if scope not in SCOPES:
        raise HubTokenRefused("unknown scope")
    uid = payload.get("uid")
    try:
        user_id = None if uid is None else int(uid)
    except (TypeError, ValueError):
        raise HubTokenRefused("malformed claim uid") from None
    return HubClaims(
        subject=str(payload["sub"]),
        user_id=user_id,
        scope=scope,
        node=settings[1],
        jti=str(payload["jti"]),
        expires_at=datetime.fromtimestamp(payload["exp"], tz=timezone.utc),
    )
