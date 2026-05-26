"""Wire models for the REST API read RPC between core and worker.

These are pure-data Pydantic models — no decryption logic, no worker-specific
imports — so the core can construct a request without pulling worker code. The
worker subclasses ``RestApiReadSettings`` to add the decryption helpers; see
``flowfile_worker.external_sources.rest_api_source.models``.

Secret fields (``*_encrypted``) carry the Fernet token emitted by the core's
``_encrypt_with_master_key``; the worker's ``decrypt_secret`` falls back to
master-key decryption for tokens without the ``$ffsec$`` prefix, so credentials
are never transmitted unencrypted.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class HttpMethod(str, Enum):
    """HTTP verbs supported by the reader."""

    GET = "GET"
    POST = "POST"


class AuthType(str, Enum):
    """Supported authentication schemes."""

    NONE = "none"
    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"


class ApiKeyLocation(str, Enum):
    """Where an API key is placed on the request."""

    HEADER = "header"
    QUERY = "query"


class PaginationType(str, Enum):
    """Supported pagination strategies."""

    NONE = "none"
    OFFSET = "offset"
    PAGE = "page"
    CURSOR = "cursor"


class CursorLocation(str, Enum):
    """Where the next-page cursor/token is read from in the response."""

    BODY = "body"  # a dot-path into the JSON response body
    HEADER = "header"  # a response header name


class AuthConfig(BaseModel):
    """Authentication settings. Secret material is carried encrypted."""

    auth_type: AuthType = AuthType.NONE

    # API key (header or query param)
    api_key_encrypted: str | None = None
    api_key_name: str = "X-API-Key"  # header name or query-param name
    api_key_location: ApiKeyLocation = ApiKeyLocation.HEADER

    # Bearer token -> "Authorization: Bearer <token>"
    bearer_token_encrypted: str | None = None

    # HTTP Basic
    basic_username: str = ""
    basic_password_encrypted: str | None = None


class PaginationConfig(BaseModel):
    """Pagination strategy and its parameters, plus safety caps."""

    pagination_type: PaginationType = PaginationType.NONE

    # offset / limit
    offset_param: str = "offset"
    limit_param: str = "limit"
    page_size: int = 100

    # page number
    page_param: str = "page"
    start_page: int = 1

    # cursor / next-page token
    cursor_param: str = "cursor"  # request param that carries the cursor
    cursor_location: CursorLocation = CursorLocation.BODY
    cursor_response_path: str = ""  # dot-path into the body, or a header name
    initial_cursor: str = ""  # value to send on the first request (usually empty)

    # safety caps
    max_pages: int = 1000
    max_records: int | None = None
    page_delay_seconds: float = 0.0


class RestApiReadSettings(BaseModel):
    """Payload for ``POST /store_rest_api_read_result``."""

    url: str
    method: HttpMethod = HttpMethod.GET
    headers: dict[str, str] = Field(default_factory=dict)
    query_params: dict[str, str] = Field(default_factory=dict)
    # Optional JSON request body (POST). ``None`` sends no body.
    json_body: Any | None = None

    auth: AuthConfig = Field(default_factory=AuthConfig)
    pagination: PaginationConfig = Field(default_factory=PaginationConfig)

    # Dot-path locating the record array in the JSON response (e.g. ``data.items``).
    # Empty -> use the top-level response (a list, or a single object => one row).
    record_path: str = ""

    # request behaviour
    timeout_seconds: float = 30.0
    max_retries: int = 3

    # When set, cap the read to roughly this many records and a single page so a
    # quick schema sample can be fetched. Overrides the pagination caps.
    sample_size: int | None = None

    flowfile_flow_id: int = 1
    flowfile_node_id: int | str = -1
