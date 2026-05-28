"""Worker-side wire models for GA4 reads.

The pure-data wire shape lives in :mod:`shared.google_analytics.models` so
the core can build the request without depending on this package. We
subclass ``GoogleAnalyticsReadSettings`` here to attach the secret
decryption helpers, which need the worker-local ``decrypt_secret``.
"""

from flowfile_worker.secrets import decrypt_secret
from shared.google_analytics.models import (
    GoogleAnalyticsFilter,
    GoogleAnalyticsOrderBy,
)
from shared.google_analytics.models import (
    GoogleAnalyticsReadSettings as _BaseGoogleAnalyticsReadSettings,
)

__all__ = [
    "GoogleAnalyticsFilter",
    "GoogleAnalyticsOrderBy",
    "GoogleAnalyticsReadSettings",
]


class GoogleAnalyticsReadSettings(_BaseGoogleAnalyticsReadSettings):
    """GA4 read settings with worker-side decryption helpers."""

    def get_decrypted_refresh_token(self) -> str:
        """Return the plaintext OAuth refresh token. Never logged."""
        return decrypt_secret(self.refresh_token_encrypted).get_secret_value()

    def get_decrypted_client_secret(self) -> str:
        """Return the plaintext OAuth client secret. Never logged."""
        return decrypt_secret(self.oauth_client_secret_encrypted).get_secret_value()
