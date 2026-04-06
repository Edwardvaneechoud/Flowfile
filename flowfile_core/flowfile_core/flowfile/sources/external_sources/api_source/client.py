"""HTTP client for REST API data source with auth, pagination, retries, and response parsing."""

from __future__ import annotations

import logging
import math
import re
import time
from collections.abc import Generator
from typing import Any

import httpx

from flowfile_core.schemas.api_schemas import (
    ApiAuth,
    ApiAuthWorker,
    ApiKeyAuth,
    ApiKeyAuthWorker,
    ApiReadSettings,
    ApiReadSettingsWorker,
    BasicAuth,
    BasicAuthWorker,
    BearerAuth,
    BearerAuthWorker,
    CursorPagination,
    CustomHeaderAuth,
    CustomHeaderAuthWorker,
    JsonPath,
    KeysetPagination,
    LinkHeaderPagination,
    NoPagination,
    OAuth2ClientCredentials,
    OAuth2ClientCredentialsWorker,
    OffsetPagination,
    PageNumberPagination,
)

logger = logging.getLogger(__name__)

# Simple in-memory cache for OAuth2 tokens: {token_url -> (token, expires_at)}
_oauth2_token_cache: dict[str, tuple[str, float]] = {}


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _basic_auth_header(username: str, password: str) -> str:
    """Build a Basic auth header value."""
    import base64

    credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {credentials}"


def _decrypt_worker_secret(value: str) -> str:
    """Decrypt a $ffsec$ encrypted string (worker context)."""
    from flowfile_core.secret_manager.secret_manager import decrypt_secret

    return decrypt_secret(value)


def flatten_record(record: dict[str, Any], separator: str = "_", prefix: str = "") -> dict[str, Any]:
    """Recursively flatten nested dicts. Lists are preserved as-is.

    >>> flatten_record({"a": {"b": 1, "c": {"d": 2}}, "e": [3]})
    {"a_b": 1, "a_c_d": 2, "e": [3]}
    """
    flat: dict[str, Any] = {}
    for key, value in record.items():
        new_key = f"{prefix}{separator}{key}" if prefix else key
        if isinstance(value, dict):
            flat.update(flatten_record(value, separator, new_key))
        else:
            flat[new_key] = value
    return flat


# ---------------------------------------------------------------------------
# JSON path traversal (typed, no string splitting)
# ---------------------------------------------------------------------------


def resolve_json_path(data: Any, path: JsonPath) -> Any:
    """Traverse into nested JSON using a typed tuple of keys.

    >>> resolve_json_path({"data": {"users": [1, 2]}}, ("data", "users"))
    [1, 2]
    """
    for key in path:
        if isinstance(data, dict):
            data = data[key]
        else:
            raise KeyError(f"Cannot traverse into {type(data).__name__} with key {key!r}")
    return data


def resolve_json_path_safe(data: Any, path: JsonPath) -> Any | None:
    """Like resolve_json_path but returns None on missing keys."""
    try:
        return resolve_json_path(data, path)
    except (KeyError, TypeError, IndexError):
        return None


def extract_records(response_json: Any, records_path: JsonPath | None) -> list[dict[str, Any]]:
    """Extract the records array from an API response."""
    if records_path is None:
        if isinstance(response_json, list):
            return response_json
        return [response_json] if isinstance(response_json, dict) else []
    result = resolve_json_path(response_json, records_path)
    if isinstance(result, list):
        return result
    return [result] if isinstance(result, dict) else []


# ---------------------------------------------------------------------------
# Auth resolution
# ---------------------------------------------------------------------------


def _resolve_secret(value: Any) -> str:
    """Resolve a secret value — works for both SecretStr and encrypted strings."""
    if hasattr(value, "get_secret_value"):
        return value.get_secret_value()
    if isinstance(value, str) and value.startswith("$ffsec$"):
        return _decrypt_worker_secret(value)
    return str(value)


def _fetch_oauth2_token(token_url: str, client_id: str, client_secret: str, scope: str | None) -> str:
    """Fetch an OAuth2 access token via client-credentials grant."""
    cache_key = token_url
    now = time.time()

    cached = _oauth2_token_cache.get(cache_key)
    if cached is not None:
        token, expires_at = cached
        if now < expires_at - 30:  # 30s safety margin
            return token

    data: dict[str, str] = {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
    if scope:
        data["scope"] = scope

    with httpx.Client(timeout=30) as client:
        resp = client.post(token_url, data=data)
        resp.raise_for_status()
        body = resp.json()

    token = body["access_token"]
    expires_in = body.get("expires_in", 3600)
    _oauth2_token_cache[cache_key] = (token, now + expires_in)
    return token


def build_auth(auth: ApiAuth | ApiAuthWorker | None) -> tuple[dict[str, str], dict[str, str]]:
    """Resolve auth config to (extra_headers, extra_params)."""
    if auth is None:
        return {}, {}

    headers: dict[str, str] = {}
    params: dict[str, str] = {}

    if isinstance(auth, ApiKeyAuth | ApiKeyAuthWorker):
        secret = _resolve_secret(auth.value)
        if auth.location == "header":
            headers[auth.key] = secret
        else:
            params[auth.key] = secret

    elif isinstance(auth, BearerAuth | BearerAuthWorker):
        headers["Authorization"] = f"Bearer {_resolve_secret(auth.token)}"

    elif isinstance(auth, BasicAuth | BasicAuthWorker):
        password = _resolve_secret(auth.password)
        headers["Authorization"] = _basic_auth_header(auth.username, password)

    elif isinstance(auth, OAuth2ClientCredentials | OAuth2ClientCredentialsWorker):
        token = _fetch_oauth2_token(
            auth.token_url,
            auth.client_id,
            _resolve_secret(auth.client_secret),
            auth.scope,
        )
        headers["Authorization"] = f"Bearer {token}"

    elif isinstance(auth, CustomHeaderAuth | CustomHeaderAuthWorker):
        for k, v in auth.headers.items():
            headers[k] = _resolve_secret(v)

    return headers, params


# ---------------------------------------------------------------------------
# Single page fetch with retries
# ---------------------------------------------------------------------------


def fetch_page(
    client: httpx.Client,
    url: str,
    method: str,
    headers: dict[str, str],
    params: dict[str, str],
    body: Any | None,
    body_content_type: str,
    timeout: float,
    max_retries: int,
    retry_backoff: float,
) -> httpx.Response:
    """Fetch a single page with retries and exponential backoff."""
    last_exc: Exception | None = None

    for attempt in range(max_retries + 1):
        try:
            kwargs: dict[str, Any] = {"headers": headers, "params": params, "timeout": timeout}
            if body is not None and method in ("POST", "PUT", "PATCH"):
                if body_content_type == "json":
                    kwargs["json"] = body
                else:
                    kwargs["data"] = body

            resp = client.request(method, url, **kwargs)

            # Handle rate limiting
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else retry_backoff * (2**attempt)
                logger.warning("Rate limited (429). Waiting %.1fs before retry.", wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except (httpx.HTTPStatusError, httpx.TransportError) as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = retry_backoff * (2**attempt)
                logger.warning(
                    "Request failed (attempt %d/%d): %s. Retrying in %.1fs.",
                    attempt + 1,
                    max_retries + 1,
                    exc,
                    wait,
                )
                time.sleep(wait)

    raise RuntimeError(f"API request failed after {max_retries + 1} attempts") from last_exc


# ---------------------------------------------------------------------------
# Link header parsing (RFC 5988)
# ---------------------------------------------------------------------------

_LINK_PATTERN = re.compile(r'<([^>]+)>;\s*rel="([^"]+)"')


def parse_link_header(link_header: str | None) -> dict[str, str]:
    """Parse a Link header into {rel: url}."""
    if not link_header:
        return {}
    return {rel: url for url, rel in _LINK_PATTERN.findall(link_header)}


# ---------------------------------------------------------------------------
# Paginated iterator (generator)
# ---------------------------------------------------------------------------


def _maybe_flatten(record: dict[str, Any], settings: ApiReadSettings | ApiReadSettingsWorker) -> dict[str, Any]:
    """Apply flattening if response_format is 'flat'."""
    if settings.response_format == "flat":
        return flatten_record(record, settings.flatten_separator)
    return record


def paginated_iter(settings: ApiReadSettings | ApiReadSettingsWorker) -> Generator[dict[str, Any], None, None]:
    """Yield records one-by-one across all pages. Pages are fetched lazily."""
    auth_headers, auth_params = build_auth(settings.auth)

    base_headers = dict(settings.headers or {})
    base_headers.update(auth_headers)

    base_params = dict(settings.query_params or {})
    base_params.update(auth_params)

    pagination = settings.pagination
    total_yielded = 0

    with httpx.Client(verify=settings.verify_ssl) as client:
        # --- NoPagination ---
        if isinstance(pagination, NoPagination):
            resp = fetch_page(
                client,
                settings.url,
                settings.method,
                base_headers,
                base_params,
                settings.body,
                settings.body_content_type,
                settings.timeout,
                settings.max_retries,
                settings.retry_backoff,
            )
            for record in extract_records(resp.json(), settings.records_path):
                yield _maybe_flatten(record, settings)
            return

        # --- OffsetPagination ---
        if isinstance(pagination, OffsetPagination):
            offset = 0
            while True:
                params = {
                    **base_params,
                    pagination.offset_param: str(offset),
                    pagination.limit_param: str(pagination.limit),
                }
                resp = fetch_page(
                    client,
                    settings.url,
                    settings.method,
                    base_headers,
                    params,
                    settings.body,
                    settings.body_content_type,
                    settings.timeout,
                    settings.max_retries,
                    settings.retry_backoff,
                )
                data = resp.json()
                records = extract_records(data, settings.records_path)
                if not records:
                    return
                for record in records:
                    yield _maybe_flatten(record, settings)
                    total_yielded += 1
                    if pagination.max_records and total_yielded >= pagination.max_records:
                        return

                offset += pagination.limit
                if pagination.total_path:
                    total = resolve_json_path_safe(data, pagination.total_path)
                    if total is not None and offset >= int(total):
                        return

                if len(records) < pagination.limit:
                    return
                if settings.rate_limit_delay > 0:
                    time.sleep(settings.rate_limit_delay)

        # --- PageNumberPagination ---
        elif isinstance(pagination, PageNumberPagination):
            page = pagination.start_page
            pages_fetched = 0
            while True:
                params = {
                    **base_params,
                    pagination.page_param: str(page),
                    pagination.page_size_param: str(pagination.page_size),
                }
                resp = fetch_page(
                    client,
                    settings.url,
                    settings.method,
                    base_headers,
                    params,
                    settings.body,
                    settings.body_content_type,
                    settings.timeout,
                    settings.max_retries,
                    settings.retry_backoff,
                )
                data = resp.json()
                records = extract_records(data, settings.records_path)
                if not records:
                    return
                for record in records:
                    yield _maybe_flatten(record, settings)
                pages_fetched += 1

                if pagination.max_pages and pages_fetched >= pagination.max_pages:
                    return

                # Check total pages
                if pagination.total_pages_path:
                    total_pages = resolve_json_path_safe(data, pagination.total_pages_path)
                    if total_pages is not None and page >= int(total_pages):
                        return

                # Check total records (derive pages)
                if pagination.total_records_path:
                    total_records = resolve_json_path_safe(data, pagination.total_records_path)
                    if total_records is not None:
                        total_pages = math.ceil(int(total_records) / pagination.page_size)
                        if page >= total_pages:
                            return

                if len(records) < pagination.page_size:
                    return

                page += 1
                if settings.rate_limit_delay > 0:
                    time.sleep(settings.rate_limit_delay)

        # --- CursorPagination ---
        elif isinstance(pagination, CursorPagination):
            cursor: str | None = None
            pages_fetched = 0
            while True:
                params = dict(base_params)
                body = settings.body
                extra_headers = dict(base_headers)

                if pagination.page_size_param and pagination.page_size:
                    params[pagination.page_size_param] = str(pagination.page_size)

                if cursor is not None:
                    if pagination.cursor_location == "query":
                        params[pagination.cursor_param] = cursor
                    elif pagination.cursor_location == "header":
                        extra_headers[pagination.cursor_param] = cursor
                    elif pagination.cursor_location == "body":
                        if isinstance(body, dict):
                            body = {**body, pagination.cursor_param: cursor}

                resp = fetch_page(
                    client,
                    settings.url,
                    settings.method,
                    extra_headers,
                    params,
                    body,
                    settings.body_content_type,
                    settings.timeout,
                    settings.max_retries,
                    settings.retry_backoff,
                )
                data = resp.json()
                records = extract_records(data, settings.records_path)
                if not records:
                    return
                for record in records:
                    yield _maybe_flatten(record, settings)
                pages_fetched += 1

                if pagination.max_pages and pages_fetched >= pagination.max_pages:
                    return

                cursor = resolve_json_path_safe(data, pagination.cursor_path)
                if cursor is None:
                    return
                cursor = str(cursor)

                if settings.rate_limit_delay > 0:
                    time.sleep(settings.rate_limit_delay)

        # --- LinkHeaderPagination ---
        elif isinstance(pagination, LinkHeaderPagination):
            url = settings.url
            pages_fetched = 0
            while True:
                resp = fetch_page(
                    client,
                    url,
                    settings.method,
                    base_headers,
                    base_params if pages_fetched == 0 else {},
                    settings.body if pages_fetched == 0 else None,
                    settings.body_content_type,
                    settings.timeout,
                    settings.max_retries,
                    settings.retry_backoff,
                )
                records = extract_records(resp.json(), settings.records_path)
                if not records:
                    return
                for record in records:
                    yield _maybe_flatten(record, settings)
                pages_fetched += 1

                if pagination.max_pages and pages_fetched >= pagination.max_pages:
                    return

                links = parse_link_header(resp.headers.get("link"))
                next_url = links.get(pagination.rel)
                if not next_url:
                    return
                url = next_url

                if settings.rate_limit_delay > 0:
                    time.sleep(settings.rate_limit_delay)

        # --- KeysetPagination ---
        elif isinstance(pagination, KeysetPagination):
            last_key: str | None = None
            pages_fetched = 0
            while True:
                params = dict(base_params)
                if pagination.page_size_param and pagination.page_size:
                    params[pagination.page_size_param] = str(pagination.page_size)
                if last_key is not None:
                    params[pagination.param_name] = last_key

                resp = fetch_page(
                    client,
                    settings.url,
                    settings.method,
                    base_headers,
                    params,
                    settings.body,
                    settings.body_content_type,
                    settings.timeout,
                    settings.max_retries,
                    settings.retry_backoff,
                )
                records = extract_records(resp.json(), settings.records_path)
                if not records:
                    return
                for record in records:
                    yield _maybe_flatten(record, settings)
                pages_fetched += 1

                if pagination.max_pages and pages_fetched >= pagination.max_pages:
                    return

                last_record = records[-1]
                if pagination.key_field not in last_record:
                    return
                last_key = str(last_record[pagination.key_field])

                if settings.rate_limit_delay > 0:
                    time.sleep(settings.rate_limit_delay)


def fetch_sample(settings: ApiReadSettings | ApiReadSettingsWorker, n: int = 10) -> list[dict[str, Any]]:
    """Fetch a small sample of records for schema inference."""
    result: list[dict[str, Any]] = []
    for record in paginated_iter(settings):
        result.append(record)
        if len(result) >= n:
            break
    return result
