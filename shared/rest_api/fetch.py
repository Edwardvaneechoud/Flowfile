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

import base64
import copy
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote, quote_plus

import httpx
import polars as pl

from shared.rest_api.models import (
    ApiKeyLocation,
    AuthType,
    CursorLocation,
    CursorRequestLocation,
    PaginationConfig,
    PaginationType,
    RestApiReadSettings,
)

# Exponential-backoff bounds for transient failures (network errors, 5xx, 429
# without a Retry-After hint).
_BACKOFF_BASE_SECONDS = 0.5
_BACKOFF_CAP_SECONDS = 30.0
# A sample is the drawer's test request: it gives up on an unreachable host sooner (core caps its read and retries).
_SAMPLE_CONNECT_TIMEOUT_SECONDS = 10.0

SECRET_MASK = "REDACTED"
_SUBSTRING_MASK_MIN_CHARS = 4
_ERROR_BODY_CHARS = 300
_PREVIEW_CHARS = 80

STOP_REASONS = {
    "single_request": "one request, no pagination",
    "empty_page": "the API returned an empty page",
    "short_page": "the last page held fewer records than the page size",
    "no_next_cursor": "the API returned no next cursor",
    "cursor_repeated": "the API returned a cursor it already gave",
    "max_records": "the record limit was reached",
    "max_pages": "the page limit was reached; the API may have more",
}


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
    """Walk a dot-path into nested JSON; on a list a segment is an index, so ``data.-1._id`` reads the last item."""
    if not path:
        return obj
    current = obj
    for key in path.split("."):
        if isinstance(current, dict):
            if key not in current:
                return None
            current = current[key]
        elif isinstance(current, list):
            try:
                current = current[int(key)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def _set_by_path(obj: dict, path: str, value: Any) -> None:
    *parents, leaf = path.split(".")
    current = obj
    for key in parents:
        child = current.setdefault(key, {})
        if not isinstance(child, dict):
            raise ValueError(f"Cannot place the cursor at '{path}': '{key}' in the JSON body is not an object.")
        current = child
    current[leaf] = value


def _url_with(url: str, params: dict[str, str]) -> str:
    try:
        return str(httpx.URL(url).copy_merge_params(params)) if params else url
    except httpx.InvalidURL:
        return url


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


def secret_forms(settings: RestApiReadSettings, secret: str | None) -> tuple[str, ...]:
    """Every spelling of the secret a request carries: as given, percent-encoded, and the Basic credential."""
    if not secret:
        return ()
    forms = {secret, quote(secret, safe=""), quote_plus(secret)}
    if settings.auth.auth_type == AuthType.BASIC:
        pair = f"{settings.auth.basic_username}:{secret}".encode()
        forms.add(base64.b64encode(pair).decode())
    return tuple(sorted(forms, key=len, reverse=True))


def mask_secrets(value: Any, forms: tuple[str, ...]) -> Any:
    """*value* with each of *forms* masked: a text, or every key and string of a JSON value.

    A form shorter than ``_SUBSTRING_MASK_MIN_CHARS`` is masked only where it is the whole string, since replacing a
    one- or two-character secret wherever it occurs would garble unrelated text and keys.
    """
    if not forms:
        return value
    if isinstance(value, str):
        for form in forms:
            if len(form) >= _SUBSTRING_MASK_MIN_CHARS:
                value = value.replace(form, SECRET_MASK)
            elif value == form:
                return SECRET_MASK
        return value
    if isinstance(value, dict):
        return {mask_secrets(key, forms): mask_secrets(item, forms) for key, item in value.items()}
    if isinstance(value, list):
        return [mask_secrets(item, forms) for item in value]
    return value


class RestApiRequestError(RuntimeError):
    """A request that failed; the message names it as sent, secrets masked, and what the API answered."""


def _failed(shown: str, response: httpx.Response, scrub: Callable[[str], str]) -> RestApiRequestError:
    answer = " ".join(scrub(response.text[:_ERROR_BODY_CHARS]).split())
    told = f": {answer}" if answer else ""
    return RestApiRequestError(f"{shown} answered {response.status_code} {response.reason_phrase}{told}")


def _request_with_retries(
    client: httpx.Client,
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    params: dict[str, str],
    json_body: Any | None,
    max_retries: int,
    shown: str,
    scrub: Callable[[str], str],
    retry_connect_failures: bool = True,
) -> httpx.Response:
    """Issue one request, retrying transient failures with backoff.

    Retries network errors and 5xx responses with exponential backoff, and 429
    responses honoring ``Retry-After`` when present. 4xx (other than 429) are
    raised immediately — they are not transient. A connection that could not be
    made is raised at once when not *retry_connect_failures*. A failure is a
    ``RestApiRequestError`` built from *shown*, the request with its secret masked,
    never from httpx's own message, which repeats the URL with its query.
    """
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
            gave_up = not retry_connect_failures and isinstance(exc, httpx.ConnectError | httpx.ConnectTimeout)
            if attempt >= max_retries or gave_up:
                detail = scrub(str(exc))
                reason = f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__
                raise RestApiRequestError(f"{shown} failed: {reason}") from None
            time.sleep(_backoff_delay(attempt))
            continue

        if response.status_code == 429:
            if attempt >= max_retries:
                raise _failed(shown, response, scrub)
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            time.sleep(retry_after if retry_after is not None else _backoff_delay(attempt))
            continue
        if response.status_code >= 500:
            if attempt >= max_retries:
                raise _failed(shown, response, scrub)
            time.sleep(_backoff_delay(attempt))
            continue
        if not response.is_success:
            raise _failed(shown, response, scrub)
        return response

    raise RuntimeError("REST API request failed after retries")


def _read_next_cursor(body: Any, response: httpx.Response, pagination) -> Any:
    """Read the next-page cursor from the response body or a header; ``None`` when there is none."""
    if pagination.cursor_location == CursorLocation.HEADER:
        return response.headers.get(pagination.cursor_response_path) or None
    value = _extract_by_path(body, pagination.cursor_response_path)
    if isinstance(value, dict | list):
        kind = "a list" if isinstance(value, list) else "an object"
        raise ValueError(
            f"The cursor path '{pagination.cursor_response_path}' points at {kind} in the response, "
            "not a single value. Choose the field that holds the next cursor."
        )
    return None if value == "" else value


def _repeats_page(page: list[dict], previous: list[dict]) -> bool:
    seen = {json.dumps(r, sort_keys=True, default=str) for r in previous}
    return all(json.dumps(r, sort_keys=True, default=str) in seen for r in page)


_CURSOR_KEYS = {
    "next", "nextcursor", "nextpage", "nextpagetoken", "nexttoken", "cursor", "endcursor", "after",
    "continuationtoken", "pagetoken", "scrollid", "nextmarker", "marker", "nextkey",
}  # fmt: skip
_ID_KEYS = {"id", "_id", "uuid"}


def suggest_paths(body: Any, limit: int = 8) -> dict[str, list[str]]:
    """Record and cursor paths a response offers: its lists of objects, cursor-named fields and last-record ids."""
    records: list[tuple[int, str]] = []
    cursors: list[str] = []

    def is_cursor(key: str, value: Any) -> bool:
        if isinstance(value, bool) or not (value is None or isinstance(value, str | int)):
            return False
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            return False
        return key.replace("_", "").replace("-", "").lower() in _CURSOR_KEYS

    def walk(node: Any, path: str, depth: int) -> None:
        if depth > 8:
            return
        if isinstance(node, dict):
            for key, value in node.items():
                child = f"{path}.{key}" if path else key
                if isinstance(value, dict | list):
                    walk(value, child, depth + 1)
                elif is_cursor(key, value):
                    cursors.append(child)
        elif isinstance(node, list) and node and all(isinstance(item, dict) for item in node):
            if path:
                records.append((len(node), path))
            last = f"{path}.-1" if path else "-1"
            for key, value in node[-1].items():
                if is_cursor(key, value) or (key.lower() in _ID_KEYS and isinstance(value, str | int)):
                    cursors.append(f"{last}.{key}")

    walk(body, "", 0)
    record_paths = [path for _, path in sorted(records, key=lambda r: (-r[0], r[1].count(".")))]
    return {"record_paths": record_paths[:limit], "cursor_paths": list(dict.fromkeys(cursors))[:limit]}


def describe_response(body: Any, max_children: int = 40, max_depth: int = 6) -> dict:
    """The shape of a response as a tree: each node's path, kind and size, a list described by its first item."""

    def node(value: Any, key: str, path: str, depth: int) -> dict:
        out: dict[str, Any] = {"key": key, "path": path}
        if isinstance(value, dict):
            out["kind"] = "object"
            items = list(value.items())
        elif isinstance(value, list):
            out["kind"] = "list"
            out["length"] = len(value)
            out["records"] = bool(value) and all(isinstance(item, dict) for item in value)
            first = f"{path}.0" if path else "0"
            if not value:
                items = []
            elif isinstance(value[0], dict):
                items = [(k, v, f"{first}.{k}") for k, v in value[0].items()]
            else:
                out["preview"] = json.dumps(value[:3], default=str)[:_PREVIEW_CHARS]
                items = []
        else:
            out["kind"] = "value"
            out["preview"] = json.dumps(value, default=str)[:_PREVIEW_CHARS]
            return out
        if depth >= max_depth:
            items = []
        children = []
        for item in items[:max_children]:
            child_path = item[2] if len(item) == 3 else (f"{path}.{item[0]}" if path else item[0])
            children.append(node(item[1], str(item[0]), child_path, depth + 1))
        out["children"] = children
        out["more"] = max(0, len(items) - max_children)
        return out

    return node(body, "", "", 0)


@dataclass(frozen=True)
class _Authorized:
    """The headers and query every request sends, their shown copies, and the httpx auth for Basic.

    Named fields, never a tuple: the real and shown copies have the same type, and swapping them would send the
    mask or show the secret.
    """

    headers: dict[str, str]
    params: dict[str, str]
    shown_headers: dict[str, str]
    shown_params: dict[str, str]
    auth: httpx.Auth | None


def _authorized(settings: RestApiReadSettings, secret: str | None) -> _Authorized:
    """What every request sends for *settings* and *secret*, and how it is shown.

    The shown copies are built beside the real ones and never from them, so they cannot hold the secret.
    """
    headers = dict(settings.headers)
    base_params: dict[str, str] = {k: str(v) for k, v in settings.query_params.items()}
    shown_headers = dict(headers)
    shown_base_params = dict(base_params)
    httpx_auth: httpx.Auth | None = None

    auth = settings.auth
    if auth.auth_type == AuthType.API_KEY and secret is not None:
        if auth.api_key_location == ApiKeyLocation.HEADER:
            headers[auth.api_key_name] = secret
            shown_headers[auth.api_key_name] = SECRET_MASK
        else:
            base_params[auth.api_key_name] = secret
            shown_base_params[auth.api_key_name] = SECRET_MASK
    elif auth.auth_type == AuthType.BEARER and secret is not None:
        headers["Authorization"] = f"Bearer {secret}"
        shown_headers["Authorization"] = f"Bearer {SECRET_MASK}"
    elif auth.auth_type == AuthType.BASIC:
        httpx_auth = httpx.BasicAuth(auth.basic_username, secret or "")
        shown_headers["Authorization"] = f"Basic {SECRET_MASK}"
    return _Authorized(
        headers=headers,
        params=base_params,
        shown_headers=shown_headers,
        shown_params=shown_base_params,
        auth=httpx_auth,
    )


def _page_limits(settings: RestApiReadSettings) -> tuple[int, int | None]:
    """The pages and records a read of *settings* may take: a sample is one page of at most ``sample_size``."""
    if settings.sample_size is not None:
        return 1, settings.sample_size
    return max(1, settings.pagination.max_pages), settings.pagination.max_records


def _starting_body(settings: RestApiReadSettings, max_pages: int) -> tuple[Any | None, bool]:
    """The JSON body requests start from and whether the cursor goes into it.

    Refuses pagination no request could carry, before anything is sent.
    """
    pagination = settings.pagination
    method = settings.method.value
    json_body = settings.json_body if method == "POST" else None
    ptype = pagination.pagination_type
    if ptype == PaginationType.CURSOR and max_pages > 1 and not pagination.cursor_response_path:
        raise ValueError("Cursor pagination needs a cursor path: where the next cursor is in the response.")
    cursor_in_body = ptype == PaginationType.CURSOR and pagination.cursor_request_location == CursorRequestLocation.BODY
    if cursor_in_body:
        if method != "POST":
            raise ValueError("A cursor sent in the JSON body needs the POST method.")
        if json_body is not None and not isinstance(json_body, dict):
            raise ValueError("A cursor sent in the JSON body needs the body to be a JSON object.")
        json_body = json_body or {}
    return json_body, cursor_in_body


def _page_request(
    pagination: PaginationConfig,
    json_body: Any | None,
    cursor_in_body: bool,
    *,
    offset: int,
    page_number: int,
    cursor: Any,
) -> tuple[dict[str, str], Any | None]:
    """The query parameters a page's request adds and the body it sends, for where the read is in its pages."""
    page_params: dict[str, str] = {}
    request_body = json_body
    ptype = pagination.pagination_type
    if ptype == PaginationType.OFFSET:
        page_params[pagination.offset_param] = str(offset)
        page_params[pagination.limit_param] = str(pagination.page_size)
    elif ptype == PaginationType.PAGE:
        page_params[pagination.page_param] = str(page_number)
    elif ptype == PaginationType.CURSOR and cursor is not None:
        if cursor_in_body:
            request_body = copy.deepcopy(json_body)
            _set_by_path(request_body, pagination.cursor_param, cursor)
        else:
            page_params[pagination.cursor_param] = str(cursor)
    return page_params, request_body


def first_request(settings: RestApiReadSettings, *, has_secret: bool) -> dict:
    """The first request a read of *settings* sends, as its trace entry names it: method, url, headers and body.

    Built by the read's own steps without sending anything, the secret shown as ``SECRET_MASK`` when
    *has_secret*, so a test request that got no answer can still show what it sent. Raises ``ValueError``
    for settings the read refuses before any request.
    """
    shown = _authorized(settings, SECRET_MASK if has_secret else None)
    pagination = settings.pagination
    max_pages, _ = _page_limits(settings)
    json_body, cursor_in_body = _starting_body(settings, max_pages)
    page_params, body = _page_request(
        pagination,
        json_body,
        cursor_in_body,
        offset=0,
        page_number=pagination.start_page,
        cursor=pagination.initial_cursor or None,
    )
    method = settings.method.value
    return {
        "method": method,
        "url": _url_with(settings.url, {**shown.shown_params, **page_params}),
        "headers": shown.shown_headers,
        "body": body if method == "POST" else None,
    }


def fetch_rest_api(
    settings: RestApiReadSettings,
    *,
    secret: str | None = None,
    on_page: Callable[[dict, Any], None] | None = None,
) -> pl.DataFrame:
    """Fetch all (or a capped sample of) records from a REST API as a typed frame.

    ``secret`` is the plaintext credential matching ``settings.auth.auth_type``
    (the API key, bearer token, or basic password); the caller decrypts it.
    ``on_page`` is called after every response with the request as sent and what
    came back (method, url, headers, body, status, elapsed_ms, bytes, records,
    next_cursor, and ``stopped``, a ``STOP_REASONS`` key on the last page), the
    secret replaced by ``SECRET_MASK``, and with the response's JSON.

    A sample (``sample_size`` set) stops trying to connect after ``_SAMPLE_CONNECT_TIMEOUT_SECONDS`` and does not
    retry a connection it could not make, so an unreachable host fails in seconds; its other retries and its
    timeout are the settings' own, which core's test request caps before it sends them.
    """
    pagination = settings.pagination
    is_sample = settings.sample_size is not None
    max_pages, max_records = _page_limits(settings)
    timeout: httpx.Timeout | float = settings.timeout_seconds
    if is_sample:
        timeout = httpx.Timeout(
            settings.timeout_seconds, connect=min(settings.timeout_seconds, _SAMPLE_CONNECT_TIMEOUT_SECONDS)
        )

    authorized = _authorized(settings, secret)
    forms = secret_forms(settings, secret)

    def scrub(text: str) -> str:
        return mask_secrets(text, forms)

    method = settings.method.value
    json_body, cursor_in_body = _starting_body(settings, max_pages)
    ptype = pagination.pagination_type

    all_records: list[dict] = []
    cursor: Any = pagination.initial_cursor or None
    sent_cursors: set[str] = set()
    previous_page: list[dict] = []
    offset = 0
    page_number = pagination.start_page
    page_index = 0

    with httpx.Client(timeout=timeout, auth=authorized.auth, follow_redirects=True) as client:
        while True:
            if ptype == PaginationType.CURSOR and cursor is not None:
                sent_cursors.add(str(cursor))
            page_params, request_body = _page_request(
                pagination, json_body, cursor_in_body, offset=offset, page_number=page_number, cursor=cursor
            )

            params = {**authorized.params, **page_params}
            shown_url = _url_with(settings.url, {**authorized.shown_params, **page_params})
            response = _request_with_retries(
                client,
                method=method,
                url=settings.url,
                headers=authorized.headers,
                params=params,
                json_body=request_body,
                max_retries=settings.max_retries,
                shown=f"{method} {shown_url}",
                scrub=scrub,
                retry_connect_failures=not is_sample,
            )

            try:
                body = response.json()
            except Exception:  # noqa: BLE001 - surface any decode failure uniformly
                starts = " ".join(scrub(response.text[:_ERROR_BODY_CHARS]).split())
                raise RestApiRequestError(
                    f"{method} {shown_url} answered {response.status_code}, but the response was not valid JSON. "
                    f"It starts with: {starts}"
                ) from None

            page_records = _to_records(_extract_by_path(body, settings.record_path))
            next_cursor = None
            repeated = False
            if ptype == PaginationType.CURSOR and page_index + 1 < max_pages:
                next_cursor = _read_next_cursor(body, response, pagination)
                if str(next_cursor) in sent_cursors and _repeats_page(page_records, previous_page):
                    page_records = []
                    repeated = True
            all_records.extend(page_records)
            page_index += 1

            stopped: str | None = None
            if max_records is not None and len(all_records) >= max_records:
                all_records = all_records[:max_records]
                stopped = "max_records"
            elif ptype == PaginationType.NONE:
                stopped = "single_request"
            elif repeated:
                stopped = "cursor_repeated"
            elif not page_records:
                stopped = "empty_page"
            elif ptype == PaginationType.OFFSET and len(page_records) < pagination.page_size:
                stopped = "short_page"
            elif page_index >= max_pages:
                stopped = "max_pages"
            elif ptype == PaginationType.CURSOR and next_cursor is None:
                stopped = "no_next_cursor"
            elif ptype == PaginationType.CURSOR and str(next_cursor) in sent_cursors:
                stopped = "cursor_repeated"

            if on_page is not None:
                on_page(
                    {
                        "page": page_index,
                        "method": method,
                        "url": shown_url,
                        "headers": authorized.shown_headers,
                        "body": request_body if method == "POST" else None,
                        "status": response.status_code,
                        "elapsed_ms": round(response.elapsed.total_seconds() * 1000),
                        "bytes": len(response.content),
                        "records": len(page_records),
                        "next_cursor": None if next_cursor is None else str(next_cursor),
                        "stopped": stopped,
                    },
                    body,
                )
            if stopped is not None:
                break

            if ptype == PaginationType.OFFSET:
                offset += pagination.page_size
            elif ptype == PaginationType.PAGE:
                page_number += 1
            elif ptype == PaginationType.CURSOR:
                cursor = next_cursor
                previous_page = page_records

            if pagination.page_delay_seconds > 0:
                time.sleep(pagination.page_delay_seconds)

    if not all_records:
        return pl.DataFrame()

    return pl.json_normalize(all_records, infer_schema_length=None)
