"""Worker-side request model for a GA4 read.

The ``service_account_json_encrypted`` field carries the Fernet token emitted
by the core's ``encrypt_secret``. The worker's ``decrypt_secret`` parses the
embedded user id and reuses the same HKDF scheme to recover the plaintext —
so the service-account JSON is never transmitted unencrypted.
"""

from pydantic import BaseModel

from flowfile_worker.secrets import decrypt_secret


class GoogleAnalyticsReadSettings(BaseModel):
    """Payload for ``POST /store_google_analytics_read_result``."""

    # Encrypted service-account JSON key (``$ffsec$1$<uid>$<token>``).
    service_account_json_encrypted: str

    property_id: str
    start_date: str = "7daysAgo"
    end_date: str = "yesterday"
    metrics: list[str]
    dimensions: list[str]
    # Maximum number of rows to fetch in total (``None`` = fetch everything).
    limit: int | None = None

    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1

    def get_decrypted_service_account_json(self) -> str:
        """Return the plaintext service-account JSON. Never logged."""
        return decrypt_secret(self.service_account_json_encrypted).get_secret_value()
