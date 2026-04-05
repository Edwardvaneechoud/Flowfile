"""REST API source schemas for authentication, pagination, and request configuration."""

from __future__ import annotations

import base64
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, SecretStr

# ---------------------------------------------------------------------------
# JSON Path type — typed tuple of keys, NOT dot-notation strings
# ("data", "users") means response["data"]["users"]
# ---------------------------------------------------------------------------

JsonPath = tuple[str, ...]


# ---------------------------------------------------------------------------
# Auth models (discriminated union on auth_type)
# ---------------------------------------------------------------------------


class ApiKeyAuth(BaseModel):
    """API key authentication, sent as a header or query parameter."""

    auth_type: Literal["api_key"] = "api_key"
    key: str  # Header or query param name (e.g. "X-API-Key")
    value: SecretStr
    location: Literal["header", "query"] = "header"


class BearerAuth(BaseModel):
    """Bearer token authentication (Authorization: Bearer <token>)."""

    auth_type: Literal["bearer"] = "bearer"
    token: SecretStr


class BasicAuth(BaseModel):
    """HTTP Basic authentication."""

    auth_type: Literal["basic"] = "basic"
    username: str
    password: SecretStr


class OAuth2ClientCredentials(BaseModel):
    """OAuth2 client-credentials grant flow."""

    auth_type: Literal["oauth2_client_credentials"] = "oauth2_client_credentials"
    token_url: str
    client_id: str
    client_secret: SecretStr
    scope: str | None = None


class CustomHeaderAuth(BaseModel):
    """Custom headers authentication (arbitrary secret headers)."""

    auth_type: Literal["custom_headers"] = "custom_headers"
    headers: dict[str, SecretStr]


ApiAuth = Annotated[
    ApiKeyAuth | BearerAuth | BasicAuth | OAuth2ClientCredentials | CustomHeaderAuth,
    Field(discriminator="auth_type"),
]


# ---------------------------------------------------------------------------
# Worker-safe auth models (encrypted strings instead of SecretStr)
# ---------------------------------------------------------------------------


class ApiKeyAuthWorker(BaseModel):
    auth_type: Literal["api_key"] = "api_key"
    key: str
    value: str  # encrypted $ffsec$ string
    location: Literal["header", "query"] = "header"


class BearerAuthWorker(BaseModel):
    auth_type: Literal["bearer"] = "bearer"
    token: str  # encrypted


class BasicAuthWorker(BaseModel):
    auth_type: Literal["basic"] = "basic"
    username: str
    password: str  # encrypted


class OAuth2ClientCredentialsWorker(BaseModel):
    auth_type: Literal["oauth2_client_credentials"] = "oauth2_client_credentials"
    token_url: str
    client_id: str
    client_secret: str  # encrypted
    scope: str | None = None


class CustomHeaderAuthWorker(BaseModel):
    auth_type: Literal["custom_headers"] = "custom_headers"
    headers: dict[str, str]  # encrypted values


ApiAuthWorker = Annotated[
    ApiKeyAuthWorker | BearerAuthWorker | BasicAuthWorker | OAuth2ClientCredentialsWorker | CustomHeaderAuthWorker,
    Field(discriminator="auth_type"),
]


# ---------------------------------------------------------------------------
# Pagination models (discriminated union on pagination_type)
# ---------------------------------------------------------------------------


class NoPagination(BaseModel):
    """Single request, no pagination."""

    pagination_type: Literal["none"] = "none"


class OffsetPagination(BaseModel):
    """Offset/limit pagination (e.g. Elasticsearch, many REST APIs).

    Example API: GET /users?offset=0&limit=100
    Response: {"data": [...], "meta": {"total": 523}}
    Next page: offset += limit until offset >= total
    """

    pagination_type: Literal["offset"] = "offset"
    limit_param: str = "limit"
    offset_param: str = "offset"
    limit: int = 100
    total_path: JsonPath | None = None  # e.g. ("meta", "total")
    max_records: int | None = None


class PageNumberPagination(BaseModel):
    """Classic page-number pagination (e.g. Django REST Framework, Rails).

    Example API: GET /users?page=1&page_size=50
    Response: {"items": [...], "meta": {"total_pages": 11}}
    Next page: page += 1 until page > total_pages
    """

    pagination_type: Literal["page_number"] = "page_number"
    page_param: str = "page"
    page_size_param: str = "page_size"
    page_size: int = 100
    start_page: int = 1
    total_pages_path: JsonPath | None = None  # e.g. ("meta", "total_pages")
    total_records_path: JsonPath | None = None
    max_pages: int | None = None


class CursorPagination(BaseModel):
    """Cursor/token-based pagination (e.g. Stripe, Slack, Twitter/X).

    Example API: GET /events?cursor=evt_abc123&limit=100
    Response: {"data": [...], "meta": {"next_cursor": "evt_xyz789"}}
    Next page: pass next_cursor as cursor param. Stop when cursor is null/missing.
    """

    pagination_type: Literal["cursor"] = "cursor"
    cursor_param: str = "cursor"
    cursor_path: JsonPath  # e.g. ("meta", "next_cursor")
    cursor_location: Literal["query", "header", "body"] = "query"
    page_size_param: str | None = None
    page_size: int | None = None
    max_pages: int | None = None


class LinkHeaderPagination(BaseModel):
    """RFC 5988 Link header pagination (e.g. GitHub API, GitLab API).

    Response header: Link: <https://api.github.com/repos?page=2>; rel="next"
    Next page: follow the URL with rel="next". Stop when absent.
    """

    pagination_type: Literal["link_header"] = "link_header"
    rel: str = "next"
    max_pages: int | None = None


class KeysetPagination(BaseModel):
    """Keyset/seek pagination (e.g. APIs sorted by ID or timestamp).

    Example API: GET /events?after_id=12345&limit=100
    Response: {"data": [{"id": 12346, ...}, ..., {"id": 12445, ...}]}
    Next page: extract last record's key field, pass as query param. Stop on empty page.
    """

    pagination_type: Literal["keyset"] = "keyset"
    key_field: str  # field name in each record (e.g. "id")
    param_name: str  # query param to send it as (e.g. "after_id")
    page_size_param: str | None = None
    page_size: int | None = None
    max_pages: int | None = None


ApiPagination = Annotated[
    NoPagination | OffsetPagination | PageNumberPagination | CursorPagination | LinkHeaderPagination | KeysetPagination,
    Field(discriminator="pagination_type"),
]


# ---------------------------------------------------------------------------
# Main API settings model
# ---------------------------------------------------------------------------


class ApiReadSettings(BaseModel):
    """Complete configuration for reading from a REST API."""

    url: str
    method: Literal["GET", "POST", "PUT", "PATCH"] = "GET"
    headers: dict[str, str] | None = None
    query_params: dict[str, str] | None = None
    body: dict[str, Any] | str | None = None
    body_content_type: Literal["json", "form"] = "json"
    auth: ApiAuth | None = None
    pagination: ApiPagination = Field(default_factory=NoPagination)
    records_path: JsonPath | None = None  # e.g. ("data", "users")
    timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 1.0
    rate_limit_delay: float = 0.0
    verify_ssl: bool = True
    connection_name: str | None = None
    connection_mode: Literal["inline", "reference"] = "inline"

    def to_worker_settings(self, user_id: int) -> ApiReadSettingsWorker:
        """Convert to worker-safe settings with encrypted secrets."""
        from flowfile_core.schemas.cloud_storage_schemas import encrypt_for_worker

        worker_auth: ApiAuthWorker | None = None
        if self.auth is not None:
            worker_auth = _encrypt_auth(self.auth, user_id, encrypt_for_worker)

        return ApiReadSettingsWorker(
            url=self.url,
            method=self.method,
            headers=self.headers,
            query_params=self.query_params,
            body=self.body,
            body_content_type=self.body_content_type,
            auth=worker_auth,
            pagination=self.pagination,
            records_path=self.records_path,
            timeout=self.timeout,
            max_retries=self.max_retries,
            retry_backoff=self.retry_backoff,
            rate_limit_delay=self.rate_limit_delay,
            verify_ssl=self.verify_ssl,
        )


class ApiReadSettingsWorker(BaseModel):
    """Worker-safe settings with secrets encrypted as $ffsec$ strings."""

    url: str
    method: Literal["GET", "POST", "PUT", "PATCH"] = "GET"
    headers: dict[str, str] | None = None
    query_params: dict[str, str] | None = None
    body: dict[str, Any] | str | None = None
    body_content_type: Literal["json", "form"] = "json"
    auth: ApiAuthWorker | None = None
    pagination: ApiPagination = Field(default_factory=NoPagination)
    records_path: JsonPath | None = None
    timeout: float = 30.0
    max_retries: int = 3
    retry_backoff: float = 1.0
    rate_limit_delay: float = 0.0
    verify_ssl: bool = True
    flowfile_flow_id: int = 0
    flowfile_node_id: int | str = -1


# ---------------------------------------------------------------------------
# Connection models (for stored API connections)
# ---------------------------------------------------------------------------


class FullApiConnection(BaseModel):
    """Full API connection with decrypted secrets."""

    connection_name: str
    base_url: str | None = None
    auth: ApiAuth | None = None
    default_headers: dict[str, str] | None = None
    verify_ssl: bool = True


class FullApiConnectionInterface(BaseModel):
    """API connection for UI display — no secrets exposed."""

    connection_name: str
    base_url: str | None = None
    auth_type: str | None = None
    verify_ssl: bool = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _encrypt_auth(auth: ApiAuth, user_id: int, encrypt_fn) -> ApiAuthWorker:
    """Encrypt auth secrets for the worker."""
    if isinstance(auth, ApiKeyAuth):
        return ApiKeyAuthWorker(
            key=auth.key,
            value=encrypt_fn(auth.value, user_id),
            location=auth.location,
        )
    elif isinstance(auth, BearerAuth):
        return BearerAuthWorker(token=encrypt_fn(auth.token, user_id))
    elif isinstance(auth, BasicAuth):
        return BasicAuthWorker(
            username=auth.username,
            password=encrypt_fn(auth.password, user_id),
        )
    elif isinstance(auth, OAuth2ClientCredentials):
        return OAuth2ClientCredentialsWorker(
            token_url=auth.token_url,
            client_id=auth.client_id,
            client_secret=encrypt_fn(auth.client_secret, user_id),
            scope=auth.scope,
        )
    elif isinstance(auth, CustomHeaderAuth):
        return CustomHeaderAuthWorker(
            headers={k: encrypt_fn(v, user_id) for k, v in auth.headers.items()},
        )
    raise ValueError(f"Unknown auth type: {type(auth)}")


def _decrypt_auth_value(encrypted_value: str) -> str:
    """Decrypt a single $ffsec$ encrypted value. For use in the worker."""
    from flowfile_core.secret_manager.secret_manager import decrypt_secret

    return decrypt_secret(encrypted_value).get_secret_value()


def _basic_auth_header(username: str, password: str) -> str:
    """Build a Basic auth header value."""
    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {credentials}"
