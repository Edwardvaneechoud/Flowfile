"""Worker/shared tests for the REST API reader engine.

The HTTP layer is mocked with ``httpx.MockTransport`` so the tests never hit the
network. They cover each auth scheme, each pagination strategy, ``record_path``
extraction, retry/429 handling, JSON->Polars typing, sampling, and the
worker-side credential decryption path.
"""

from __future__ import annotations

import base64
import contextlib

import httpx
import polars as pl
import pytest

from flowfile_worker.external_sources.rest_api_source.main import read_rest_api
from flowfile_worker.external_sources.rest_api_source.models import RestApiReadSettings as WorkerRestApiReadSettings
from flowfile_worker.secrets import encrypt_secret
from shared.rest_api.fetch import _parse_retry_after, fetch_rest_api
from shared.rest_api.models import (
    AuthConfig,
    PaginationConfig,
    RestApiReadSettings,
)


@contextlib.contextmanager
def mock_transport(handler):
    """Swap ``httpx.Client`` for one wired to a MockTransport for the block."""
    transport = httpx.MockTransport(handler)
    original = httpx.Client

    def factory(*args, **kwargs):
        kwargs["transport"] = transport
        return original(*args, **kwargs)

    httpx.Client = factory
    try:
        yield
    finally:
        httpx.Client = original


def _run(handler, settings: RestApiReadSettings, secret: str | None = None) -> pl.DataFrame:
    with mock_transport(handler):
        return fetch_rest_api(settings, secret=secret)


# --- record_path extraction + typing -----------------------------------------


def test_record_path_list_of_objects():
    def handler(_req):
        return httpx.Response(200, json={"data": {"items": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}})

    df = _run(handler, RestApiReadSettings(url="https://x/api", record_path="data.items"))
    assert df.shape == (2, 2)
    assert df.columns == ["id", "name"]
    assert df.schema["id"] == pl.Int64
    assert df.schema["name"] == pl.String


def test_single_object_becomes_one_row_and_flattens_nested():
    def handler(_req):
        return httpx.Response(200, json={"id": 9, "nested": {"city": "ams"}})

    df = _run(handler, RestApiReadSettings(url="https://x/api"))
    assert df.shape == (1, 2)
    assert "nested.city" in df.columns


def test_empty_result_returns_empty_frame():
    def handler(_req):
        return httpx.Response(200, json={"items": []})

    df = _run(handler, RestApiReadSettings(url="https://x/api", record_path="items"))
    assert df.shape == (0, 0)


def test_missing_record_path_returns_empty():
    def handler(_req):
        return httpx.Response(200, json={"data": []})

    df = _run(handler, RestApiReadSettings(url="https://x/api", record_path="nope.missing"))
    assert df.height == 0


def test_non_json_response_raises():
    def handler(_req):
        return httpx.Response(200, text="not json", headers={"content-type": "text/plain"})

    with pytest.raises(RuntimeError, match="not valid JSON"):
        _run(handler, RestApiReadSettings(url="https://x/api"))


# --- authentication -----------------------------------------------------------


def test_api_key_in_header():
    seen = {}

    def handler(req):
        seen["key"] = req.headers.get("X-API-Key")
        return httpx.Response(200, json=[{"id": 1}])

    settings = RestApiReadSettings(
        url="https://x/api", auth=AuthConfig(auth_type="api_key", api_key_name="X-API-Key", api_key_location="header")
    )
    _run(handler, settings, secret="abc123")
    assert seen["key"] == "abc123"


def test_api_key_in_query():
    seen = {}

    def handler(req):
        seen["key"] = req.url.params.get("api_key")
        return httpx.Response(200, json=[{"id": 1}])

    settings = RestApiReadSettings(
        url="https://x/api", auth=AuthConfig(auth_type="api_key", api_key_name="api_key", api_key_location="query")
    )
    _run(handler, settings, secret="qval")
    assert seen["key"] == "qval"


def test_bearer_header():
    seen = {}

    def handler(req):
        seen["auth"] = req.headers.get("Authorization")
        return httpx.Response(200, json=[{"id": 1}])

    _run(handler, RestApiReadSettings(url="https://x/api", auth=AuthConfig(auth_type="bearer")), secret="tok")
    assert seen["auth"] == "Bearer tok"


def test_basic_auth_header():
    seen = {}

    def handler(req):
        seen["auth"] = req.headers.get("Authorization")
        return httpx.Response(200, json=[{"id": 1}])

    settings = RestApiReadSettings(url="https://x/api", auth=AuthConfig(auth_type="basic", basic_username="user"))
    _run(handler, settings, secret="pw")
    assert seen["auth"] is not None and seen["auth"].startswith("Basic ")


# --- pagination ---------------------------------------------------------------


def test_offset_pagination_walks_pages_and_sends_params():
    seen_params = []

    def handler(req):
        seen_params.append(dict(req.url.params))
        off = int(req.url.params.get("offset", "0"))
        if off == 0:
            return httpx.Response(200, json=[{"id": 1}, {"id": 2}])
        return httpx.Response(200, json=[{"id": 3}])  # short page -> stop

    settings = RestApiReadSettings(
        url="https://x/api", pagination=PaginationConfig(pagination_type="offset", page_size=2)
    )
    df = _run(handler, settings)
    assert df.height == 3
    assert seen_params[0] == {"offset": "0", "limit": "2"}
    assert seen_params[1] == {"offset": "2", "limit": "2"}


def test_page_number_pagination_stops_on_empty():
    pages = []

    def handler(req):
        page = int(req.url.params.get("page", "0"))
        pages.append(page)
        if page <= 2:
            return httpx.Response(200, json=[{"id": page}])
        return httpx.Response(200, json=[])

    settings = RestApiReadSettings(
        url="https://x/api", pagination=PaginationConfig(pagination_type="page", start_page=1)
    )
    df = _run(handler, settings)
    assert df.height == 2
    assert pages[:3] == [1, 2, 3]


def test_cursor_pagination_from_body():
    def handler(req):
        cursor = req.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(200, json={"rows": [{"id": 1}], "next": "abc"})
        if cursor == "abc":
            return httpx.Response(200, json={"rows": [{"id": 2}], "next": None})
        return httpx.Response(200, json={"rows": [], "next": None})

    settings = RestApiReadSettings(
        url="https://x/api",
        record_path="rows",
        pagination=PaginationConfig(
            pagination_type="cursor", cursor_param="cursor", cursor_location="body", cursor_response_path="next"
        ),
    )
    df = _run(handler, settings)
    assert sorted(df["id"].to_list()) == [1, 2]


def test_cursor_pagination_from_header():
    def handler(req):
        cursor = req.url.params.get("cursor")
        if cursor is None:
            return httpx.Response(200, json=[{"id": 1}], headers={"X-Next": "p2"})
        return httpx.Response(200, json=[{"id": 2}])  # no next header -> stop

    settings = RestApiReadSettings(
        url="https://x/api",
        pagination=PaginationConfig(
            pagination_type="cursor", cursor_param="cursor", cursor_location="header", cursor_response_path="X-Next"
        ),
    )
    df = _run(handler, settings)
    assert sorted(df["id"].to_list()) == [1, 2]


def test_max_pages_cap():
    def handler(_req):
        return httpx.Response(200, json=[{"id": 1}, {"id": 2}])  # always a full page

    settings = RestApiReadSettings(
        url="https://x/api", pagination=PaginationConfig(pagination_type="offset", page_size=2, max_pages=3)
    )
    df = _run(handler, settings)
    assert df.height == 6  # 3 pages * 2


def test_max_records_cap():
    def handler(_req):
        return httpx.Response(200, json=[{"id": i} for i in range(10)])

    settings = RestApiReadSettings(
        url="https://x/api", pagination=PaginationConfig(pagination_type="offset", page_size=10, max_records=4)
    )
    df = _run(handler, settings)
    assert df.height == 4


def test_sample_size_caps_records():
    def handler(_req):
        return httpx.Response(200, json=[{"id": i} for i in range(100)])

    df = _run(handler, RestApiReadSettings(url="https://x/api", sample_size=5))
    assert df.height == 5


# --- retries / 429 ------------------------------------------------------------


def test_retry_on_429_then_success(monkeypatch):
    monkeypatch.setattr("shared.rest_api.fetch.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def handler(_req):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, json={})
        return httpx.Response(200, json=[{"id": 1}])

    df = _run(handler, RestApiReadSettings(url="https://x/api", max_retries=2))
    assert calls["n"] == 2
    assert df.height == 1


def test_retry_on_5xx_then_success(monkeypatch):
    monkeypatch.setattr("shared.rest_api.fetch.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def handler(_req):
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, json={})
        return httpx.Response(200, json=[{"id": 1}])

    df = _run(handler, RestApiReadSettings(url="https://x/api", max_retries=2))
    assert calls["n"] == 2
    assert df.height == 1


def test_4xx_raises_without_retry(monkeypatch):
    monkeypatch.setattr("shared.rest_api.fetch.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def handler(_req):
        calls["n"] += 1
        return httpx.Response(404, json={"error": "nope"})

    with pytest.raises(httpx.HTTPStatusError):
        _run(handler, RestApiReadSettings(url="https://x/api", max_retries=3))
    assert calls["n"] == 1  # not retried


# --- POST body ----------------------------------------------------------------


def test_post_sends_json_body():
    seen = {}

    def handler(req):
        seen["body"] = req.content
        return httpx.Response(200, json=[{"id": 1}])

    settings = RestApiReadSettings(url="https://x/api", method="POST", json_body={"q": "search"})
    _run(handler, settings)
    assert b'"q"' in seen["body"]


# --- worker decrypt path ------------------------------------------------------


def test_read_rest_api_decrypts_bearer_token():
    """The worker wrapper decrypts the stored ciphertext before the request."""
    seen = {}

    def handler(req):
        seen["auth"] = req.headers.get("Authorization")
        return httpx.Response(200, json=[{"id": 1}])

    encrypted = encrypt_secret("supersecret")  # legacy master-key form
    settings = WorkerRestApiReadSettings(
        url="https://x/api", auth=AuthConfig(auth_type="bearer", bearer_token_encrypted=encrypted)
    )
    with mock_transport(handler):
        df = read_rest_api(settings)
    assert seen["auth"] == "Bearer supersecret"
    assert df.height == 1


def test_read_rest_api_decrypts_api_key():
    """The worker wrapper decrypts an API key and places it in the header."""
    seen = {}

    def handler(req):
        seen["key"] = req.headers.get("X-Key")
        return httpx.Response(200, json=[{"id": 1}])

    encrypted = encrypt_secret("apikeyval")
    settings = WorkerRestApiReadSettings(
        url="https://x/api",
        auth=AuthConfig(
            auth_type="api_key", api_key_name="X-Key", api_key_location="header", api_key_encrypted=encrypted
        ),
    )
    with mock_transport(handler):
        df = read_rest_api(settings)
    assert seen["key"] == "apikeyval"
    assert df.height == 1


def test_read_rest_api_decrypts_basic_password():
    """The worker wrapper decrypts the basic-auth password into the Authorization header."""
    seen = {}

    def handler(req):
        seen["auth"] = req.headers.get("Authorization")
        return httpx.Response(200, json=[{"id": 1}])

    encrypted = encrypt_secret("pw123")
    settings = WorkerRestApiReadSettings(
        url="https://x/api",
        auth=AuthConfig(auth_type="basic", basic_username="user", basic_password_encrypted=encrypted),
    )
    with mock_transport(handler):
        df = read_rest_api(settings)
    assert seen["auth"].startswith("Basic ")
    assert base64.b64decode(seen["auth"].split(" ", 1)[1]).decode() == "user:pw123"
    assert df.height == 1


# --- retry-after parsing + network-error retry --------------------------------


def test_parse_retry_after_numeric_and_invalid():
    assert _parse_retry_after("5") == 5.0
    assert _parse_retry_after("not-a-date") is None
    assert _parse_retry_after(None) is None


def test_parse_retry_after_http_date_in_past_clamps_to_zero():
    # A fixed HTTP-date in the past exercises the parsedate_to_datetime branch
    # without depending on the wall clock; elapsed time clamps to 0.
    assert _parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0


def test_network_error_then_success(monkeypatch):
    monkeypatch.setattr("shared.rest_api.fetch.time.sleep", lambda *_: None)
    calls = {"n": 0}

    def handler(_req):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("boom")
        return httpx.Response(200, json=[{"id": 1}])

    df = _run(handler, RestApiReadSettings(url="https://x/api", max_retries=2))
    assert calls["n"] == 2
    assert df.height == 1


# --- misc fetch branches ------------------------------------------------------


def test_initial_cursor_sent_on_first_request():
    seen = []

    def handler(req):
        seen.append(req.url.params.get("cursor"))
        return httpx.Response(200, json={"rows": [{"id": 1}], "next": None})

    settings = RestApiReadSettings(
        url="https://x/api",
        record_path="rows",
        pagination=PaginationConfig(
            pagination_type="cursor", cursor_param="cursor", initial_cursor="seed", cursor_response_path="next"
        ),
    )
    df = _run(handler, settings)
    assert seen[0] == "seed"
    assert df.height == 1


def test_get_ignores_json_body():
    seen = {}

    def handler(req):
        seen["content"] = req.content
        return httpx.Response(200, json=[{"id": 1}])

    _run(handler, RestApiReadSettings(url="https://x/api", method="GET", json_body={"q": "x"}))
    assert seen["content"] == b""


def test_scalar_list_wrapped_as_value_column():
    def handler(_req):
        return httpx.Response(200, json={"items": [1, 2, 3]})

    df = _run(handler, RestApiReadSettings(url="https://x/api", record_path="items"))
    assert df.columns == ["value"]
    assert df["value"].to_list() == [1, 2, 3]
