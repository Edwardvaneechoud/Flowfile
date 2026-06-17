"""Google Analytics 4 connection schemas.

A GA connection is a named, per-user bundle whose credential is stored encrypted
via Fernet + HKDF per-user key derivation. It authenticates one of two ways,
selected by ``auth_method``:

- ``"oauth"``: a refresh token minted by the OAuth callback in
  ``routes/ga_connections.py`` — never accepted as form input.
- ``"service_account"``: a service-account JSON key submitted to
  ``POST /ga_connection/service_account`` and encrypted at rest immediately.
"""

from typing import Literal

from pydantic import BaseModel, SecretStr

from flowfile_core.schemas.sharing_schema import AccessInfo

GoogleAnalyticsAuthMethod = Literal["oauth", "service_account"]


class GoogleAnalyticsConnectionMetadata(BaseModel):
    """PUT /ga_connection body. Edits description / default_property_id only —
    the OAuth credential is managed via the /oauth/start + /oauth/callback flow.
    """

    connection_name: str
    description: str | None = None
    default_property_id: str | None = None


class GoogleAnalyticsServiceAccountInput(BaseModel):
    """POST /ga_connection/service_account body. Unlike the OAuth routes, this
    one accepts credential material over the wire — the JSON key is encrypted at
    rest immediately (mirroring how GCS accepts ``gcs_service_account_key``).
    """

    connection_name: str
    service_account_key: SecretStr
    description: str | None = None
    default_property_id: str | None = None


class FullGoogleAnalyticsConnectionInterface(BaseModel):
    """Public API response — no secret material exposed."""

    connection_name: str
    description: str | None = None
    default_property_id: str | None = None
    auth_method: GoogleAnalyticsAuthMethod = "oauth"
    oauth_user_email: str | None = None
    id: int | None = None
    access: AccessInfo | None = None
