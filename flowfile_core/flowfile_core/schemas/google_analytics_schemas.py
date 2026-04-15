"""Google Analytics 4 connection schemas.

A GA connection is a named, per-user bundle whose only secret is a service-account
JSON key file (stored encrypted via Fernet + HKDF per-user key derivation).
The key follows the same handling pattern as other cloud-provider credentials.
"""

from pydantic import BaseModel, SecretStr

from flowfile_core.secret_manager.secret_manager import encrypt_secret


class FullGoogleAnalyticsConnection(BaseModel):
    """Input model for creating/updating a GA4 connection. Carries the raw
    service-account JSON as a ``SecretStr`` until it is persisted or passed to
    the worker in encrypted form.
    """

    connection_name: str
    description: str | None = None
    default_property_id: str | None = None
    # The full JSON body of the service-account key file, pasted or uploaded by the user.
    service_account_json: SecretStr | None = None

    def get_worker_interface(self, user_id: int) -> "FullGoogleAnalyticsConnectionWorkerInterface":
        """Convert to a worker interface with the service-account JSON encrypted
        using the user's derived key. The worker uses ``decrypt_secret`` (which
        reads the embedded user_id from the token) to recover the plaintext."""
        if self.service_account_json is None:
            raise ValueError("Service account key is required to build a worker interface")
        encrypted = encrypt_secret(self.service_account_json.get_secret_value(), user_id)
        return FullGoogleAnalyticsConnectionWorkerInterface(
            connection_name=self.connection_name,
            default_property_id=self.default_property_id,
            service_account_json_encrypted=encrypted,
        )


class FullGoogleAnalyticsConnectionWorkerInterface(BaseModel):
    """Worker-facing model. ``service_account_json_encrypted`` is the
    ``$ffsec$1$<user_id>$<token>`` string emitted by ``encrypt_secret``.
    The worker decrypts it just before constructing the Google credentials.
    """

    connection_name: str
    default_property_id: str | None = None
    service_account_json_encrypted: str


class FullGoogleAnalyticsConnectionInterface(BaseModel):
    """Public API response — no secret material exposed."""

    connection_name: str
    description: str | None = None
    default_property_id: str | None = None
