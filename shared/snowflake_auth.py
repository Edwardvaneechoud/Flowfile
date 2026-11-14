"""Snowflake account, key and OAuth helpers shared by core and the worker.

A Snowflake connection stores one secret whatever its login method: an access
token, a private key (kept as unencrypted base64 PKCS8 DER, since Flowfile
encrypts every secret itself) or an OAuth refresh token. The helpers here turn
what a user typed into those stored forms and a refresh token into the
short-lived access token a session needs.

The OAuth client is Snowflake's built-in public client for local applications
(``SNOWFLAKE$LOCAL_APPLICATION``): no admin setup, PKCE required, and a redirect
that must be a loopback address, so the browser sign-in only works where core
runs on the user's own machine. Its client id and secret are both
``LOCAL_APPLICATION`` when sent as HTTP Basic credentials.

Module level stays stdlib-only: every worker child imports the dialect registry,
which imports this module. ``cryptography`` and ``httpx`` are imported at use.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import re
import secrets
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlencode, urlsplit

AUTH_OAUTH = "oauth"
AUTH_ACCESS_TOKEN = "access_token"
AUTH_KEY_PAIR = "key_pair"
AUTH_METHODS: tuple[str, ...] = (AUTH_OAUTH, AUTH_ACCESS_TOKEN, AUTH_KEY_PAIR)

HOST_SUFFIXES: tuple[str, ...] = (".snowflakecomputing.com", ".snowflakecomputing.cn")
_SNOWSIGHT_SUFFIXES = {"app.snowflake.com": HOST_SUFFIXES[0], "app.snowflake.cn": HOST_SUFFIXES[1]}
_HOST_LABELS = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$")

OAUTH_CLIENT_ID = "LOCAL_APPLICATION"
OAUTH_CLIENT_SECRET = "LOCAL_APPLICATION"
_HTTP_TIMEOUT_SECONDS = 20
# An access token is reused until this long before Snowflake's stated expiry.
_EXPIRY_MARGIN_SECONDS = 60


class SnowflakeAuthError(ValueError):
    """What the user gave cannot be used to sign in to Snowflake; the message says why."""


def normalize_account(text: str) -> str:
    """The Snowflake hostname for what a user typed or pasted as their account.

    Accepts an account identifier (``myorg-myaccount``, ``xy12345.eu-west-1``), a
    hostname, a ``https://…snowflakecomputing.com`` URL, or a Snowsight URL
    (``https://app.snowflake.com/myorg/myaccount/…``, or the older
    ``https://app.snowflake.com/<region>/<locator>/…``). The result always ends in a
    Snowflake domain, and a URL naming any other host is refused, so a typed value
    can never point a request at another host.
    """
    original = (text or "").strip()
    if not original:
        raise SnowflakeAuthError("Enter your Snowflake account, for example myorg-myaccount.")
    value = original
    suffix = HOST_SUFFIXES[0]
    if "@" in value:
        raise _not_an_account(original)
    if any(mark in value for mark in ("://", "/", ":")):
        try:
            parts = urlsplit(value if "://" in value else f"https://{value}")
            host = (parts.hostname or "").lower()
        except ValueError:
            raise _not_an_account(original) from None
        if host in _SNOWSIGHT_SUFFIXES:
            segments = [s for s in parts.path.split("/") if s]
            if len(segments) < 2:
                raise SnowflakeAuthError(
                    "That Snowflake link does not name an account. Open your account in Snowflake and copy the link."
                )
            value = _identifier_from_snowsight(segments[0], segments[1])
            suffix = _SNOWSIGHT_SUFFIXES[host]
        elif host.endswith(HOST_SUFFIXES) or (host and "." not in host):
            value = host
        else:
            raise _not_an_account(original)
    host = value.lower().rstrip(".").replace("_", "-")
    if not host.endswith(HOST_SUFFIXES):
        host += suffix
    if not account_from_host(host) or not _HOST_LABELS.match(host):
        raise _not_an_account(original)
    return host


def _not_an_account(text: str) -> SnowflakeAuthError:
    return SnowflakeAuthError(
        f"'{text}' is not a Snowflake account. Use the form myorg-myaccount, or paste the link of your account."
    )


def _identifier_from_snowsight(first: str, second: str) -> str:
    # An organization name holds no hyphen or dot, so either marks the older region/locator link.
    if "-" in first or "." in first:
        return f"{second}.{first}"
    return f"{first}-{second}"


def account_from_host(host: str) -> str:
    """The account identifier of a Snowflake hostname (``myorg-myaccount`` for its ``.snowflakecomputing.com`` host)."""
    lowered = host.lower()
    for suffix in HOST_SUFFIXES:
        if lowered.endswith(suffix):
            return lowered[: -len(suffix)]
    return lowered


def normalize_private_key(text: str, passphrase: str | None = None) -> str:
    """A user's private key as unencrypted PKCS8 DER in base64, the form a connection stores.

    ``text`` is the content of a ``.p8``/``.pem`` file (PKCS8 or traditional
    ``RSA PRIVATE KEY``), encrypted or not, or a key already in the stored form.
    The passphrase only opens the key here and is not kept.
    """
    from cryptography.exceptions import UnsupportedAlgorithm
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    value = (text or "").strip()
    if not value:
        raise SnowflakeAuthError("Choose your private key file (.p8).")
    if "PUBLIC KEY" in value:
        raise SnowflakeAuthError("That is the public key. Choose the private key file (rsa_key.p8).")

    def load(password: bytes | None):
        if "-----BEGIN" in value:
            return serialization.load_pem_private_key(value.encode(), password=password)
        try:
            der = base64.b64decode("".join(value.split()), validate=True)
        except (binascii.Error, ValueError):
            raise ValueError("not base64") from None
        return serialization.load_der_private_key(der, password=password)

    password = passphrase.encode() if passphrase else None
    try:
        key = load(password)
    except TypeError as exc:
        if password is None:
            raise SnowflakeAuthError("This private key is encrypted. Enter its passphrase.") from exc
        raise SnowflakeAuthError("This private key is not encrypted. Leave the passphrase empty.") from exc
    except (ValueError, UnsupportedAlgorithm) as exc:
        if password is not None and _is_encrypted(load):
            raise SnowflakeAuthError("The passphrase does not open this private key.") from exc
        raise SnowflakeAuthError("That file is not a private key Snowflake accepts (PEM, PKCS#8).") from exc
    if not isinstance(key, rsa.RSAPrivateKey):
        raise SnowflakeAuthError("Snowflake key-pair sign-in needs an RSA private key.")
    der = key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return base64.b64encode(der).decode()


def _is_encrypted(load) -> bool:
    """Whether a key that failed to open with a passphrase is an encrypted key (so the passphrase was wrong)."""
    try:
        load(None)
    except TypeError:
        return True
    except Exception:
        return False
    return False


def private_key_der(stored: str) -> bytes:
    """The DER bytes of a key in its stored form, as the Snowflake driver takes it."""
    return base64.b64decode(stored)


@dataclass(frozen=True)
class PkcePair:
    verifier: str
    challenge: str


def new_pkce_pair() -> PkcePair:
    """A fresh PKCE verifier (86 URL-safe characters) and its S256 challenge."""
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode()).digest()
    return PkcePair(verifier=verifier, challenge=base64.urlsafe_b64encode(digest).rstrip(b"=").decode())


def _base_url(host: str) -> str:
    return f"https://{host}"


def role_scope(role: str | None) -> str:
    """The OAuth scope of a sign-in: a refresh token, and the session role when one is asked for."""
    if not role:
        return "refresh_token"
    return f"refresh_token session:role:{role}"


def authorize_url(host: str, *, redirect_uri: str, state: str, code_challenge: str, role: str | None = None) -> str:
    """The Snowflake page a browser opens to sign the user in."""
    params = {
        "client_id": OAUTH_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "state": state,
        "scope": role_scope(role),
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    return f"{_base_url(host)}/oauth/authorize?{urlencode(params)}"


@dataclass(frozen=True)
class OAuthTokens:
    access_token: str
    refresh_token: str | None
    expires_in: int
    username: str | None


def _token_request(host: str, data: dict[str, str]) -> dict:
    import httpx

    try:
        response = httpx.post(
            f"{_base_url(host)}/oauth/token-request",
            data=data,
            auth=(OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET),
            timeout=_HTTP_TIMEOUT_SECONDS,
        )
    except httpx.HTTPError as exc:
        raise SnowflakeAuthError(
            f"Could not reach Snowflake at {host} ({type(exc).__name__}). Check the account and your network."
        ) from None
    try:
        body = response.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    if response.status_code >= 400 or not body.get("access_token"):
        raise SnowflakeAuthError(_token_error(body, response.status_code))
    return body


def _token_error(body: dict, status_code: int) -> str:
    code = str(body.get("error") or "")
    detail = str(body.get("message") or body.get("error_description") or "").strip()
    if code == "invalid_client":
        return (
            "This Snowflake account does not offer the built-in sign-in for local applications, "
            "or an administrator switched it off. Use an access token or a key-pair."
        )
    if code == "invalid_grant":
        return "The Snowflake sign-in has expired or was revoked. Sign in again."
    suffix = f": {detail}" if detail else ""
    return f"Snowflake refused the sign-in ({code or status_code}){suffix}"


def exchange_code(host: str, *, code: str, verifier: str, redirect_uri: str) -> OAuthTokens:
    """Trade the code a browser sign-in returned for the session's tokens.

    The access token is cached under the refresh token, so the session opened right
    after the sign-in (to list warehouses and databases) needs no second request.
    """
    body = _token_request(
        host,
        {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri, "code_verifier": verifier},
    )
    tokens = OAuthTokens(
        access_token=body["access_token"],
        refresh_token=body.get("refresh_token"),
        expires_in=int(body.get("expires_in") or 0),
        username=body.get("username"),
    )
    if tokens.refresh_token:
        remember_access_token(host, tokens.refresh_token, tokens.access_token, tokens.expires_in)
    return tokens


_access_tokens: dict[tuple[str, str], tuple[str, float]] = {}
_access_tokens_lock = threading.Lock()


def _cache_key(host: str, refresh_token: str) -> tuple[str, str]:
    return host.lower(), hashlib.sha256(refresh_token.encode()).hexdigest()


def remember_access_token(host: str, refresh_token: str, access_token: str, expires_in: int) -> None:
    """Keep an access token for the refresh token it belongs to until shortly before it expires."""
    if expires_in <= _EXPIRY_MARGIN_SECONDS:
        return
    now = time.monotonic()
    with _access_tokens_lock:
        for key in [k for k, (_, until) in _access_tokens.items() if until <= now]:
            del _access_tokens[key]
        _access_tokens[_cache_key(host, refresh_token)] = (access_token, now + expires_in - _EXPIRY_MARGIN_SECONDS)


def access_token_for(host: str, refresh_token: str) -> str:
    """An access token for a stored sign-in: the cached one while it is valid, else a new one from Snowflake."""
    key = _cache_key(host, refresh_token)
    with _access_tokens_lock:
        cached = _access_tokens.get(key)
        if cached and cached[1] > time.monotonic():
            return cached[0]
    body = _token_request(host, {"grant_type": "refresh_token", "refresh_token": refresh_token})
    remember_access_token(host, refresh_token, body["access_token"], int(body.get("expires_in") or 0))
    return body["access_token"]


def forget_access_tokens() -> None:
    with _access_tokens_lock:
        _access_tokens.clear()
