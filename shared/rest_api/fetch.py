"""Pure HTTP fetch engine for the REST API reader.

Shared by the worker (remote execution) and the core (local execution, used by
``flowfile_frame.read_api``). It takes a :class:`RestApiReadSettings` plus the
single already-decrypted ``secret`` relevant to ``auth.auth_type``, so it has no
dependency on either side's secret manager — the caller decrypts and passes the
plaintext.

JSON is the only supported response format; the located record array is
flattened with ``pl.json_normalize``.
"""

from __future__ import annotations

import time
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import polars as pl

from shared.rest_api.models import (
    ApiKeyLocation,
    AuthType,
    CursorLocation,
    PaginationType,
    RestApiReadSettings,
)

# Exponential-backoff bounds for transient failures (network errors, 5xx, 429
# without a Retry-After hint).
_BACKOFF_BASE_SECONDS = 0.5
_BACKOFF_CAP_SECONDS = 30.0


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff capped at ``_BACKOFF_CAP_SECONDS``."""
    return min(_BACKOFF_CAP_SECONDS, _BACKOFF_BASE_SECONDS * (2**attempt))


def _parse_retry_after(value: str | None) -> float | None:
    """Parse an HTTP ``Retry-After`` header (delta-seconds or HTTP-date)."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        retry_dt = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if retry_dt is None:
        return None
    import datetime as _dt

    now = _dt.datetime.now(tz=retry_dt.tzinfo) if retry_dt.tzinfo else _dt.datetime.now()
    return max(0.0, (retry_dt - now).total_seconds())


def _extract_by_path(obj: Any, path: str) -> Any:
    """Walk a dot-separated path into a nested mapping. Empty path returns ``obj``."""
    if not path:
        return obj
    current = obj
    for key in path.split("."):
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


def _to_records(extracted: Any) -> list[dict]:
    """Normalise the located payload into a list of record dicts.

    Handles a list of objects, a single object, and (defensively) lists of
    scalars, which are wrapped as ``{"value": <scalar>}``.
    """
    if extracted is None:
        return []
    if isinstance(extracted, dict):
        return [extracted]
    if isinstance(extracted, list):
        return [item if isinstance(item, dict) else {"value": item} for item in extracted]
    return [{"value": extracted}]


def _request_with_retries(
    client: httpx.Client,
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    params: dict[str, str],
    json_body: Any | None,
    max_retries: int,
) -> httpx.Response:
    """Issue one request, retrying transient failures with backoff.

    Retries network errors and 5xx responses with exponential backoff, and 429
    responses honoring ``Retry-After`` when present. 4xx (other than 429) are
    raised immediately — they are not transient.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            response = client.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body if (method == "POST" and json_body is not None) else None,
            )
        except httpx.HTTPError as exc:
            last_exc = exc
            if attempt >= max_retries:
                raise
            time.sleep(_backoff_delay(attempt))
            continue

        if response.status_code == 429:
            if attempt >= max_retries:
                response.raise_for_status()
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            time.sleep(retry_after if retry_after is not None else _backoff_delay(attempt))
            continue
        if response.status_code >= 500:
            if attempt >= max_retries:
                response.raise_for_status()
            time.sleep(_backoff_delay(attempt))
            continue

        # 2xx -> done; 4xx (non-429) -> raise (not retryable).
        response.raise_for_status()
        return response

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("REST API request failed after retries")


def _read_next_cursor(body: Any, response: httpx.Response, pagination) -> str | None:
    """Read the next-page cursor/token from the response body or a header."""
    if pagination.cursor_location == CursorLocation.HEADER:
        return response.headers.get(pagination.cursor_response_path) or None
    value = _extract_by_path(body, pagination.cursor_response_path)
    if value in (None, ""):
        return None
    return str(value)


def fetch_rest_api(settings: RestApiReadSettings, *, secret: str | None = None) -> pl.DataFrame:
    """Fetch all (or a capped sample of) records from a REST API as a typed frame.

    ``secret`` is the plaintext credential matching ``settings.auth.auth_type``
    (the API key, bearer token, or basic password); the caller decrypts it.
    """
    pagination = settings.pagination
    is_sample = settings.sample_size is not None

    # A sample fetch is a single page, capped to ``sample_size`` records.
    max_pages = 1 if is_sample else max(1, pagination.max_pages)
    max_records = settings.sample_size if is_sample else pagination.max_records

    headers = dict(settings.headers)
    base_params: dict[str, str] = {k: str(v) for k, v in settings.query_params.items()}
    httpx_auth: httpx.Auth | None = None

    auth = settings.auth
    if auth.auth_type == AuthType.API_KEY and secret is not None:
        if auth.api_key_location == ApiKeyLocation.HEADER:
            headers[auth.api_key_name] = secret
        else:
            base_params[auth.api_key_name] = secret
    elif auth.auth_type == AuthType.BEARER and secret is not None:
        headers["Authorization"] = f"Bearer {secret}"
    elif auth.auth_type == AuthType.BASIC:
        httpx_auth = httpx.BasicAuth(auth.basic_username, secret or "")

    method = settings.method.value
    json_body = settings.json_body if method == "POST" else None
    ptype = pagination.pagination_type

    all_records: list[dict] = []
    cursor = pagination.initial_cursor
    offset = 0
    page_number = pagination.start_page
    page_index = 0

    with httpx.Client(timeout=settings.timeout_seconds, auth=httpx_auth, follow_redirects=True) as client:
        while page_index < max_pages:
            params = dict(base_params)
            if ptype == PaginationType.OFFSET:
                params[pagination.offset_param] = str(offset)
                params[pagination.limit_param] = str(pagination.page_size)
            elif ptype == PaginationType.PAGE:
                params[pagination.page_param] = str(page_number)
            elif ptype == PaginationType.CURSOR and cursor:
                params[pagination.cursor_param] = cursor

            response = _request_with_retries(
                client,
                method=method,
                url=settings.url,
                headers=headers,
                params=params,
                json_body=json_body,
                max_retries=settings.max_retries,
            )

            try:
                body = response.json()
            except Exception as exc:  # noqa: BLE001 - surface any decode failure uniformly
                raise RuntimeError(f"REST API response was not valid JSON: {exc}") from exc

            page_records = _to_records(_extract_by_path(body, settings.record_path))
            all_records.extend(page_records)
            page_index += 1

            if max_records is not None and len(all_records) >= max_records:
                all_records = all_records[:max_records]
                break

            if ptype == PaginationType.NONE:
                break
            if ptype == PaginationType.OFFSET:
                if not page_records or len(page_records) < pagination.page_size:
                    break
                offset += pagination.page_size
            elif ptype == PaginationType.PAGE:
                if not page_records:
                    break
                page_number += 1
            elif ptype == PaginationType.CURSOR:
                next_cursor = _read_next_cursor(body, response, pagination)
                if not next_cursor:
                    break
                cursor = next_cursor

            if pagination.page_delay_seconds > 0:
                time.sleep(pagination.page_delay_seconds)

    if not all_records:
        return pl.DataFrame()

    return pl.json_normalize(all_records, infer_schema_length=None)
