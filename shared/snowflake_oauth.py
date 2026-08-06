"""OAuth token-endpoint client for Snowflake SSO connections.

Pure HTTP helpers used by flowfile_core to exchange an authorization code and
to refresh access tokens for ``auth_method="oauth"`` database connections.
Works against both Snowflake's built-in OAuth server and an external IdP
(Okta / Entra ID / PingFederate): the caller passes the token endpoint, so
this module needs no knowledge of which flavor is configured.

Secrets never enter ``shared``: every input is an already-decrypted plain
string and the caller encrypts whatever it persists. Client credentials are
sent as HTTP Basic auth (``client_secret_basic``), which Snowflake requires
and the major IdPs accept.
"""

from __future__ import annotations

from typing import NamedTuple

import httpx

_TIMEOUT_SECONDS = 20.0


class TokenResponse(NamedTuple):
    """The useful subset of an OAuth token-endpoint response."""

    access_token: str
    expires_in: int | None
    refresh_token: str | None


class SnowflakeOAuthError(Exception):
    """A failed token-endpoint call.

    ``error`` carries the OAuth error code when the endpoint returned one
    (e.g. ``invalid_grant`` for an expired or revoked refresh token) so the
    caller can distinguish "re-authenticate" from transient failures.
    """

    def __init__(self, message: str, *, error: str | None = None, status_code: int | None = None):
        super().__init__(message)
        self.error = error
        self.status_code = status_code

    @property
    def requires_reauthentication(self) -> bool:
        return self.error in ("invalid_grant", "invalid_token")


def derive_snowflake_endpoints(account: str) -> tuple[str, str]:
    """Default authorize/token endpoints for Snowflake's built-in OAuth server."""
    base = f"https://{account}.snowflakecomputing.com/oauth"
    return f"{base}/authorize", f"{base}/token-request"


def _post_token_request(token_endpoint: str, client_id: str, client_secret: str, data: dict[str, str]) -> TokenResponse:
    try:
        response = httpx.post(
            token_endpoint,
            data=data,
            auth=(client_id, client_secret),
            headers={"Accept": "application/json"},
            timeout=_TIMEOUT_SECONDS,
        )
    except httpx.HTTPError as e:
        raise SnowflakeOAuthError(f"Could not reach OAuth token endpoint: {e}") from e

    if response.status_code >= 400:
        error_code = None
        detail = response.text[:300]
        try:
            payload = response.json()
            error_code = payload.get("error")
            detail = payload.get("error_description") or payload.get("message") or detail
        except ValueError:
            pass
        raise SnowflakeOAuthError(
            f"OAuth token request failed ({response.status_code}): {detail}",
            error=error_code,
            status_code=response.status_code,
        )

    try:
        payload = response.json()
    except ValueError as e:
        raise SnowflakeOAuthError("OAuth token endpoint returned a non-JSON response") from e
    access_token = payload.get("access_token")
    if not access_token:
        raise SnowflakeOAuthError("OAuth token endpoint response is missing access_token")
    expires_in = payload.get("expires_in")
    return TokenResponse(
        access_token=access_token,
        expires_in=int(expires_in) if expires_in is not None else None,
        refresh_token=payload.get("refresh_token") or None,
    )


def exchange_authorization_code(
    token_endpoint: str,
    client_id: str,
    client_secret: str,
    code: str,
    redirect_uri: str,
) -> TokenResponse:
    """Exchange an authorization code for tokens (the interactive callback leg)."""
    return _post_token_request(
        token_endpoint,
        client_id,
        client_secret,
        {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri},
    )


def refresh_access_token(
    token_endpoint: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> TokenResponse:
    """Mint a short-lived access token from a stored refresh token.

    Some IdPs rotate the refresh token on use; when the response carries one,
    the caller must persist it or the next refresh fails.
    """
    return _post_token_request(
        token_endpoint,
        client_id,
        client_secret,
        {"grant_type": "refresh_token", "refresh_token": refresh_token},
    )
