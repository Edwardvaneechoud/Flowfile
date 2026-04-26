"""Wire models for the GA4 read RPC between core and worker.

These are pure-data Pydantic models — no decryption logic, no worker-specific
imports — so the core can construct a request without pulling worker code.
The worker subclasses ``GoogleAnalyticsReadSettings`` to add the decryption
helpers; see ``flowfile_worker.external_sources.google_analytics_source.models``.

``refresh_token_encrypted`` carries the Fernet token emitted by the core's
``encrypt_secret``; the worker's ``decrypt_secret`` parses the embedded user
id and reuses the same HKDF scheme to recover the plaintext, so the refresh
token is never transmitted unencrypted.
"""

from pydantic import BaseModel, Field


class GoogleAnalyticsFilter(BaseModel):
    """Row-level filter applied to the GA4 report."""

    field: str
    operator: str
    value: str = ""
    case_sensitive: bool = False


class GoogleAnalyticsOrderBy(BaseModel):
    """Sort entry applied to the GA4 report."""

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
