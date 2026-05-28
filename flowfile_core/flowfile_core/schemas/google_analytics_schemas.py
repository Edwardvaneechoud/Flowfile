"""Google Analytics 4 connection schemas.

A GA connection is a named, per-user bundle whose credential is an OAuth
refresh token stored encrypted via Fernet + HKDF per-user key derivation.
The refresh token is minted by the OAuth callback in
``routes/ga_connections.py`` — never accepted as form input.
"""

from pydantic import BaseModel


class GoogleAnalyticsConnectionMetadata(BaseModel):
    """PUT /ga_connection body. Edits description / default_property_id only —
    the OAuth credential is managed via the /oauth/start + /oauth/callback flow.
    """

    connection_name: str
    description: str | None = None
    default_property_id: str | None = None


class FullGoogleAnalyticsConnectionWorkerInterface(BaseModel):
    """Worker-facing model. ``refresh_token_encrypted`` is the
    ``$ffsec$1$<user_id>$<token>`` string emitted by ``encrypt_secret``.
    The worker decrypts it and uses it to mint an access token via Google's
    OAuth token endpoint before calling the Data API.
    """

    connection_name: str
    default_property_id: str | None = None
    refresh_token_encrypted: str


class FullGoogleAnalyticsConnectionInterface(BaseModel):
    """Public API response — no secret material exposed."""

    connection_name: str
    description: str | None = None
    default_property_id: str | None = None
    oauth_user_email: str | None = None
