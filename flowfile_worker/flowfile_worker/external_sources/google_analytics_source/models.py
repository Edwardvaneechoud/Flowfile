"""Worker-side request model for a GA4 read.

``refresh_token_encrypted`` carries the Fernet token emitted by the core's
``encrypt_secret`` (OAuth refresh token). The worker's ``decrypt_secret``
parses the embedded user id and reuses the same HKDF scheme to recover the
plaintext — so the refresh token is never transmitted unencrypted.
"""

from pydantic import BaseModel, Field

from flowfile_worker.secrets import decrypt_secret


class GoogleAnalyticsFilter(BaseModel):
    """Row-level filter applied to the GA4 report.

    Mirrors ``flowfile_core.schemas.input_schema.GoogleAnalyticsFilter`` — kept
    local to the worker so this package doesn't depend on ``flowfile_core``.
    """

    field: str
    operator: str
    value: str = ""
    case_sensitive: bool = False


class GoogleAnalyticsOrderBy(BaseModel):
    """Sort entry applied to the GA4 report.

    Mirrors ``flowfile_core.schemas.input_schema.GoogleAnalyticsOrderBy``.
    """

    field: str
    descending: bool = False


class GoogleAnalyticsReadSettings(BaseModel):
    """Payload for ``POST /store_google_analytics_read_result``."""

    refresh_token_encrypted: str
    oauth_client_id: str
    # Master-key encrypted (Fernet). The worker's ``decrypt_secret`` falls
    # back to master-key decryption for tokens without the ``$ffsec$`` prefix.
    oauth_client_secret_encrypted: str

    property_id: str
    start_date: str = "7daysAgo"
    end_date: str = "yesterday"
    metrics: list[str]
    dimensions: list[str]
    # Maximum number of rows to fetch in total (``None`` = fetch everything).
    limit: int | None = None
    # Row-level filters, routed into ``dimension_filter`` / ``metric_filter``
    # by inspecting which of ``dimensions`` / ``metrics`` the ``field`` appears in.
    filters: list[GoogleAnalyticsFilter] = Field(default_factory=list)
    # Sort entries; each ``field`` must be one of the selected metrics or dimensions.
    order_bys: list[GoogleAnalyticsOrderBy] = Field(default_factory=list)

    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1

    def get_decrypted_refresh_token(self) -> str:
        """Return the plaintext OAuth refresh token. Never logged."""
        return decrypt_secret(self.refresh_token_encrypted).get_secret_value()

    def get_decrypted_client_secret(self) -> str:
        """Return the plaintext OAuth client secret. Never logged."""
        return decrypt_secret(self.oauth_client_secret_encrypted).get_secret_value()
