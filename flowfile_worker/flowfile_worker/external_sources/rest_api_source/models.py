"""Worker-side wire models for REST API reads.

The pure-data wire shape lives in :mod:`shared.rest_api.models` so the core can
build the request without depending on this package. We subclass
``RestApiReadSettings`` here to attach the secret decryption helpers, which need
the worker-local ``decrypt_secret``.
"""

from flowfile_worker.secrets import decrypt_secret
from shared.rest_api.models import (
    ApiKeyLocation,
    AuthConfig,
    AuthType,
    CursorLocation,
    HttpMethod,
    PaginationConfig,
    PaginationType,
)
from shared.rest_api.models import (
    RestApiReadSettings as _BaseRestApiReadSettings,
)

__all__ = [
    "ApiKeyLocation",
    "AuthConfig",
    "AuthType",
    "CursorLocation",
    "HttpMethod",
    "PaginationConfig",
    "PaginationType",
    "RestApiReadSettings",
]


class RestApiReadSettings(_BaseRestApiReadSettings):
    """REST API read settings with worker-side decryption helpers."""

    def get_api_key(self) -> str | None:
        """Return the plaintext API key, or ``None`` if not configured. Never logged."""
        if self.auth.api_key_encrypted:
            return decrypt_secret(self.auth.api_key_encrypted).get_secret_value()
        return None

    def get_bearer_token(self) -> str | None:
        """Return the plaintext bearer token, or ``None`` if not configured. Never logged."""
        if self.auth.bearer_token_encrypted:
            return decrypt_secret(self.auth.bearer_token_encrypted).get_secret_value()
        return None

    def get_basic_password(self) -> str | None:
        """Return the plaintext basic-auth password, or ``None`` if not configured. Never logged."""
        if self.auth.basic_password_encrypted:
            return decrypt_secret(self.auth.basic_password_encrypted).get_secret_value()
        return None
