"""Tokens an external issuer signed, verified offline against the keys it publishes.

An issuer signs short-lived tokens with a private key and publishes the public half as a JWKS document. ``decode`` is
the one check of anything such an issuer signs: the caller says where the keys are, who the token must be for and who
must have signed it, and gets the claims or a ``TokenRefused`` with a reason fit to answer. This module holds no secret
and knows no issuer: who the issuer is on a machine is its caller's to say. Nothing runs at import.
"""

import threading

import jwt

ALGORITHMS = ["ES256", "RS256"]
LEEWAY_SECONDS = 30
REQUIRED_CLAIMS = ["exp", "iat", "sub", "aud", "scope", "jti"]
JWKS_PATH = "/.well-known/jwks.json"


class TokenRefused(Exception):
    """A token that does not verify. ``reason`` is fit to answer to the caller and never names the token."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


_client: jwt.PyJWKClient | None = None
_client_url: str | None = None
_client_lock = threading.Lock()


def _jwks_client(jwks_url: str) -> jwt.PyJWKClient:
    """The process's one key client, built on first use and again when the URL changes.

    PyJWT caches the key set and the keys it matched; an unknown ``kid`` makes it fetch the set again, which is how a
    rotated key reaches a running process.
    """
    global _client, _client_url
    with _client_lock:
        if _client is None or _client_url != jwks_url:
            _client = jwt.PyJWKClient(jwks_url, cache_keys=True)
            _client_url = jwks_url
        return _client


def decode(token: str, jwks_url: str, audience: str, issuer: str, required: list[str] = REQUIRED_CLAIMS) -> dict:
    """The claims of the JWS ``token``, signed with a key the issuer publishes at ``jwks_url`` for ``audience`` by
    ``issuer``, with every claim in ``required`` present and ``exp`` not past the leeway.

    Raises:
        TokenRefused: The token does not verify; ``reason`` says why in words fit for the caller.
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
        raise TokenRefused("expired") from None
    except jwt.InvalidAudienceError:
        raise TokenRefused("wrong audience") from None
    except jwt.InvalidIssuerError:
        raise TokenRefused("wrong issuer") from None
    except jwt.MissingRequiredClaimError as exc:
        raise TokenRefused(f"missing claim {exc.claim}") from None
    except jwt.InvalidAlgorithmError:
        raise TokenRefused("algorithm not allowed") from None
    except jwt.InvalidSignatureError:
        raise TokenRefused("bad signature") from None
    except jwt.PyJWKClientConnectionError:
        raise TokenRefused("keys unavailable") from None
    except jwt.PyJWKClientError:
        raise TokenRefused("unknown key") from None
    except jwt.PyJWTError as exc:
        raise TokenRefused(f"invalid: {exc}") from None
