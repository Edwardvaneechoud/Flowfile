"""REST API reader engine tests, against a real HTTP server (``test_utils.rest_api_server``)."""

from __future__ import annotations

import base64
import json

import httpx
import polars as pl
import pytest

from flowfile_worker.external_sources.rest_api_source.main import read_rest_api
from flowfile_worker.external_sources.rest_api_source.models import RestApiReadSettings as WorkerRestApiReadSettings
from flowfile_worker.secrets import encrypt_secret
from shared.rest_api.fetch import (
    SECRET_MASK,
    RestApiRequestError,
    _extract_by_path,
    _parse_retry_after,
    describe_response,
    fetch_rest_api,
    suggest_paths,
)
from shared.rest_api.models import AuthConfig, PaginationConfig, RestApiReadSettings
from test_utils.rest_api_server import KEYSET_RECORDS, Reply, closed_port_url, start_api_server


@pytest.fixture(scope="module")
def _server():
    with start_api_server() as server:
        yield server


@pytest.fixture()
def api(_server):
    _server.reset()
    yield _server


@pytest.fixture()
def no_sleep(monkeypatch):
    monkeypatch.setattr("shared.rest_api.fetch.time.sleep", lambda *_: None)


def _answer(payload=None, status: int = 200, headers: dict | None = None, text: str | None = None):
    return lambda _req, _srv: Reply(status, payload, headers or {}, text)


def _keyset(path: str = "/keyset", **pagination) -> RestApiReadSettings:
    return RestApiReadSettings(
        url=path,
        method="POST",
        json_body={"app_id": "app-1", "entry_id": "entry-1", "limit": 3},
        record_path="data",
        pagination=PaginationConfig(
            pagination_type="cursor",
            cursor_param="data_id",
            cursor_request_location="body",
            cursor_response_path="data.-1._id",
            **pagination,
        ),
    )


def _with(settings: RestApiReadSettings, **update) -> RestApiReadSettings:
    return RestApiReadSettings.model_validate({**settings.model_dump(), **update})


def _at(api, settings: RestApiReadSettings) -> RestApiReadSettings:
    return _with(settings, url=api.url(settings.url))


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("data.0._id", "a"),
        ("data.-1._id", "c"),
        ("data.1", {"_id": "b"}),
        ("data.3._id", None),
        ("data.-4._id", None),
        ("data.x._id", None),
        ("keyed.0", "dict key wins"),
        ("data.0._id.more", None),
        ("", {"data": [{"_id": "a"}, {"_id": "b"}, {"_id": "c"}], "keyed": {"0": "dict key wins"}}),
    ],
)
def test_extract_by_path_walks_keys_and_list_indices(path, expected):
    body = {"data": [{"_id": "a"}, {"_id": "b"}, {"_id": "c"}], "keyed": {"0": "dict key wins"}}
    assert _extract_by_path(body, path) == expected


def test_record_path_list_of_objects(api):
    url = api.route("/r", _answer({"data": {"items": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]}}))
    df = fetch_rest_api(RestApiReadSettings(url=url, record_path="data.items"))
    assert df.shape == (2, 2)
    assert df.columns == ["id", "name"]
    assert df.schema["id"] == pl.Int64
    assert df.schema["name"] == pl.String


def test_record_path_through_a_list_index(api):
    url = api.route("/r", _answer({"data": [{"items": [{"id": 1}, {"id": 2}]}, {"items": [{"id": 3}]}]}))
    df = fetch_rest_api(RestApiReadSettings(url=url, record_path="data.0.items"))
    assert df["id"].to_list() == [1, 2]


def test_single_object_becomes_one_row_and_flattens_nested(api):
    url = api.route("/r", _answer({"id": 9, "nested": {"city": "ams"}}))
    df = fetch_rest_api(RestApiReadSettings(url=url))
    assert df.shape == (1, 2)
    assert "nested.city" in df.columns


def test_empty_result_returns_empty_frame(api):
    url = api.route("/r", _answer({"items": []}))
    df = fetch_rest_api(RestApiReadSettings(url=url, record_path="items"))
    assert df.shape == (0, 0)


def test_missing_record_path_returns_empty(api):
    url = api.route("/r", _answer({"data": []}))
    df = fetch_rest_api(RestApiReadSettings(url=url, record_path="nope.missing"))
    assert df.height == 0


def test_non_json_response_raises(api):
    url = api.route("/r", _answer(text="not json"))
    with pytest.raises(RuntimeError, match="not valid JSON"):
        fetch_rest_api(RestApiReadSettings(url=url))


def test_scalar_list_wrapped_as_value_column(api):
    url = api.route("/r", _answer({"items": [1, 2, 3]}))
    df = fetch_rest_api(RestApiReadSettings(url=url, record_path="items"))
    assert df.columns == ["value"]
    assert df["value"].to_list() == [1, 2, 3]


def test_api_key_in_header(api):
    settings = RestApiReadSettings(
        url=api.url("/items"),
        auth=AuthConfig(auth_type="api_key", api_key_name="X-API-Key", api_key_location="header"),
    )
    fetch_rest_api(settings, secret="abc123")
    assert api.requests[0].headers["X-API-Key"] == "abc123"


def test_api_key_in_query(api):
    settings = RestApiReadSettings(
        url=api.url("/items"),
        auth=AuthConfig(auth_type="api_key", api_key_name="api_key", api_key_location="query"),
    )
    fetch_rest_api(settings, secret="qval")
    assert api.requests[0].query["api_key"] == "qval"


def test_bearer_header(api):
    fetch_rest_api(RestApiReadSettings(url=api.url("/items"), auth=AuthConfig(auth_type="bearer")), secret="tok")
    assert api.requests[0].headers["Authorization"] == "Bearer tok"


def test_basic_auth_header(api):
    settings = RestApiReadSettings(url=api.url("/items"), auth=AuthConfig(auth_type="basic", basic_username="user"))
    fetch_rest_api(settings, secret="pw")
    scheme, token = api.requests[0].headers["Authorization"].split(" ", 1)
    assert scheme == "Basic"
    assert base64.b64decode(token).decode() == "user:pw"


def test_offset_pagination_walks_pages_and_sends_params(api):
    settings = RestApiReadSettings(
        url=api.url("/offset"), pagination=PaginationConfig(pagination_type="offset", page_size=2)
    )
    df = fetch_rest_api(settings)
    assert df["id"].to_list() == [0, 1, 2, 3, 4]
    assert [r.query for r in api.requests] == [
        {"offset": "0", "limit": "2"},
        {"offset": "2", "limit": "2"},
        {"offset": "4", "limit": "2"},
    ]


def test_page_number_pagination_stops_on_empty(api):
    settings = RestApiReadSettings(url=api.url("/page"), pagination=PaginationConfig(pagination_type="page"))
    df = fetch_rest_api(settings)
    assert df["id"].to_list() == [1, 2, 3]
    assert [r.query["page"] for r in api.requests] == ["1", "2", "3", "4"]


def test_cursor_pagination_from_body(api):
    settings = RestApiReadSettings(
        url=api.url("/cursor"),
        record_path="rows",
        pagination=PaginationConfig(pagination_type="cursor", cursor_response_path="next"),
    )
    df = fetch_rest_api(settings)
    assert df["id"].to_list() == [1, 2, 3]
    assert [r.query.get("cursor") for r in api.requests] == [None, "c2", "c3"]


def test_cursor_pagination_from_header(api):
    def handler(req, _srv):
        if "cursor" not in req.query:
            return Reply(payload=[{"id": 1}], headers={"X-Next": "p2"})
        return Reply(payload=[{"id": 2}])

    settings = RestApiReadSettings(
        url=api.route("/h", handler),
        pagination=PaginationConfig(
            pagination_type="cursor", cursor_location="header", cursor_response_path="X-Next"
        ),
    )
    df = fetch_rest_api(settings)
    assert df["id"].to_list() == [1, 2]


def test_initial_cursor_sent_on_first_request(api):
    url = api.route("/r", _answer({"rows": [{"id": 1}], "next": None}))
    settings = RestApiReadSettings(
        url=url,
        record_path="rows",
        pagination=PaginationConfig(pagination_type="cursor", initial_cursor="seed", cursor_response_path="next"),
    )
    df = fetch_rest_api(settings)
    assert api.requests[0].query["cursor"] == "seed"
    assert df.height == 1


def test_max_pages_cap(api):
    url = api.route("/r", _answer([{"id": 1}, {"id": 2}]))
    settings = RestApiReadSettings(
        url=url, pagination=PaginationConfig(pagination_type="offset", page_size=2, max_pages=3)
    )
    assert fetch_rest_api(settings).height == 6
    assert api.request_count == 3


def test_max_records_cap(api):
    url = api.route("/r", _answer([{"id": i} for i in range(10)]))
    settings = RestApiReadSettings(
        url=url, pagination=PaginationConfig(pagination_type="offset", page_size=10, max_records=4)
    )
    assert fetch_rest_api(settings).height == 4


def test_sample_size_caps_records(api):
    url = api.route("/r", _answer([{"id": i} for i in range(100)]))
    assert fetch_rest_api(RestApiReadSettings(url=url, sample_size=5)).height == 5


def test_keyset_cursor_in_body_walks_every_page(api):
    df = fetch_rest_api(_at(api, _keyset()))

    assert df["_id"].to_list() == [r["_id"] for r in KEYSET_RECORDS]
    bodies = [r.body for r in api.requests]
    assert len(bodies) == 4  # three pages and the empty one that ends it
    assert "data_id" not in bodies[0]
    assert [b.get("data_id") for b in bodies[1:]] == ["id03", "id06", "id07"]
    assert all(b["app_id"] == "app-1" and b["entry_id"] == "entry-1" and b["limit"] == 3 for b in bodies)
    assert all(not r.query for r in api.requests)


def test_body_cursor_goes_into_a_nested_field(api):
    def handler(req, _srv):
        after = (req.body.get("page") or {}).get("after")
        return Reply(payload={"items": [{"id": 1}], "next": "n2"} if after is None else {"items": [], "next": None})

    settings = RestApiReadSettings(
        url=api.route("/nested", handler),
        method="POST",
        json_body={"page": {"size": 1}},
        record_path="items",
        pagination=PaginationConfig(
            pagination_type="cursor",
            cursor_param="page.after",
            cursor_request_location="body",
            cursor_response_path="next",
        ),
    )
    df = fetch_rest_api(settings)
    assert df["id"].to_list() == [1]
    assert api.requests[1].body == {"page": {"size": 1, "after": "n2"}}


def test_body_cursor_with_no_configured_body_starts_from_an_empty_object(api):
    settings = _with(_at(api, _keyset()), json_body=None)
    df = fetch_rest_api(settings)
    assert df.height == len(KEYSET_RECORDS)
    assert api.requests[0].body == {}
    assert api.requests[1].body == {"data_id": "id03"}


def test_numeric_body_cursor_keeps_its_type(api):
    def handler(req, _srv):
        after = req.body.get("after")
        rows = [{"id": 1}, {"id": 2}] if after is None else []
        return Reply(payload={"rows": rows})

    settings = RestApiReadSettings(
        url=api.route("/numeric", handler),
        method="POST",
        json_body={},
        record_path="rows",
        pagination=PaginationConfig(
            pagination_type="cursor",
            cursor_param="after",
            cursor_request_location="body",
            cursor_response_path="rows.-1.id",
        ),
    )
    fetch_rest_api(settings)
    assert api.requests[1].body == {"after": 2}


def test_body_cursor_refuses_a_get_before_any_request(api):
    settings = _with(_at(api, _keyset()), method="GET")
    with pytest.raises(ValueError, match="POST"):
        fetch_rest_api(settings)
    assert api.request_count == 0


def test_body_cursor_refuses_a_body_that_is_no_object(api):
    settings = _with(_at(api, _keyset()), json_body=[1, 2])
    with pytest.raises(ValueError, match="JSON object"):
        fetch_rest_api(settings)
    assert api.request_count == 0


def test_cursor_path_into_an_empty_array_ends_after_one_page(api):
    url = api.route("/r", _answer({"data": []}))
    settings = _with(_at(api, _keyset()), url=url)
    assert fetch_rest_api(settings).height == 0
    assert api.request_count == 1


def test_cursor_path_out_of_range_ends_after_one_page(api):
    url = api.route("/r", _answer({"data": [{"_id": "a"}]}))
    settings = _with(_at(api, _keyset()), url=url)
    settings.pagination.cursor_response_path = "data.5._id"
    assert fetch_rest_api(settings).height == 1
    assert api.request_count == 1


def test_first_list_item_as_cursor(api):
    url = api.route("/r", _answer({"data": [{"_id": "first"}, {"_id": "second"}]}))
    settings = _with(_at(api, _keyset(max_pages=2)), url=url)
    settings.pagination.cursor_response_path = "data.0._id"
    fetch_rest_api(settings)
    assert api.requests[1].body["data_id"] == "first"


def test_cursor_stops_on_an_empty_page_even_with_a_next_cursor(api):
    def handler(req, _srv):
        if "cursor" not in req.query:
            return Reply(payload={"rows": [{"id": 1}], "next": "c2"})
        return Reply(payload={"rows": [], "next": "c3"})

    settings = RestApiReadSettings(
        url=api.route("/r", handler),
        record_path="rows",
        pagination=PaginationConfig(pagination_type="cursor", cursor_response_path="next"),
    )
    assert fetch_rest_api(settings)["id"].to_list() == [1]
    assert api.request_count == 2


def test_repeated_cursor_stops_without_duplicate_rows(api):
    df = fetch_rest_api(_at(api, _keyset("/keyset-echo")))

    assert df["_id"].to_list() == [r["_id"] for r in KEYSET_RECORDS]
    assert api.request_count == 4
    assert api.requests[-1].body["data_id"] == "id07"


def test_a_cursor_that_cycles_back_stops(api):
    def handler(req, _srv):
        cursor = req.query.get("cursor")
        nxt = {None: "a", "a": "b", "b": "a"}[cursor]
        return Reply(payload={"rows": [{"at": cursor or "start"}], "next": nxt})

    settings = RestApiReadSettings(
        url=api.route("/cycle", handler),
        record_path="rows",
        pagination=PaginationConfig(pagination_type="cursor", cursor_response_path="next"),
    )
    df = fetch_rest_api(settings)
    assert df["at"].to_list() == ["start", "a", "b"]
    assert api.request_count == 3


def test_retry_on_429_then_success(api, no_sleep):
    df = fetch_rest_api(RestApiReadSettings(url=api.url("/flaky"), max_retries=2))
    assert api.request_count == 2
    assert df.height == 1


def test_retry_on_5xx_then_success(api, no_sleep):
    df = fetch_rest_api(RestApiReadSettings(url=api.url("/fail-once"), max_retries=2))
    assert api.request_count == 2
    assert df.height == 1


def test_4xx_raises_without_retry(api, no_sleep):
    with pytest.raises(RestApiRequestError, match="answered 404"):
        fetch_rest_api(RestApiReadSettings(url=api.url("/nope"), max_retries=3))
    assert api.request_count == 1


def test_network_error_is_retried_then_raised(monkeypatch):
    sleeps = []
    monkeypatch.setattr("shared.rest_api.fetch.time.sleep", sleeps.append)
    with pytest.raises(RestApiRequestError, match="failed: ConnectError"):
        fetch_rest_api(RestApiReadSettings(url=closed_port_url(), max_retries=2))
    assert len(sleeps) == 2


def test_post_sends_json_body(api):
    fetch_rest_api(RestApiReadSettings(url=api.url("/items"), method="POST", json_body={"q": "search"}))
    assert api.requests[0].method == "POST"
    assert api.requests[0].body == {"q": "search"}


def test_get_ignores_json_body(api):
    fetch_rest_api(RestApiReadSettings(url=api.url("/items"), method="GET", json_body={"q": "x"}))
    assert api.requests[0].raw_body == b""


def test_read_rest_api_decrypts_bearer_token(api):
    settings = WorkerRestApiReadSettings(
        url=api.url("/items"), auth=AuthConfig(auth_type="bearer", bearer_token_encrypted=encrypt_secret("s3cret"))
    )
    df = read_rest_api(settings)
    assert api.requests[0].headers["Authorization"] == "Bearer s3cret"
    assert df.height == 3


def test_read_rest_api_decrypts_api_key(api):
    settings = WorkerRestApiReadSettings(
        url=api.url("/items"),
        auth=AuthConfig(
            auth_type="api_key",
            api_key_name="X-Key",
            api_key_location="header",
            api_key_encrypted=encrypt_secret("apikeyval"),
        ),
    )
    read_rest_api(settings)
    assert api.requests[0].headers["X-Key"] == "apikeyval"


def test_read_rest_api_decrypts_basic_password(api):
    settings = WorkerRestApiReadSettings(
        url=api.url("/items"),
        auth=AuthConfig(auth_type="basic", basic_username="user", basic_password_encrypted=encrypt_secret("pw123")),
    )
    read_rest_api(settings)
    token = api.requests[0].headers["Authorization"].split(" ", 1)[1]
    assert base64.b64decode(token).decode() == "user:pw123"


def test_parse_retry_after_numeric_and_invalid():
    assert _parse_retry_after("5") == 5.0
    assert _parse_retry_after("not-a-date") is None
    assert _parse_retry_after(None) is None


def test_parse_retry_after_http_date_in_past_clamps_to_zero():
    assert _parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0


@pytest.mark.parametrize(
    ("body", "records", "cursors"),
    [
        (
            {"data": {"characters": {"info": {"next": 2}, "results": [{"id": "1"}, {"id": "2"}]}}},
            ["data.characters.results"],
            ["data.characters.info.next", "data.characters.results.-1.id"],
        ),
        ({"data": [{"_id": "a"}, {"_id": "b"}]}, ["data"], ["data.-1._id"]),
        ([{"id": 1}], [], ["-1.id"]),
        (
            {"page": {"endCursor": "Y3", "hasNextPage": True}, "edges": [{"cursor": "a", "node": {"id": 1}}]},
            ["edges"],
            ["page.endCursor", "edges.-1.cursor"],
        ),
        ({"next": "https://api.example.com/?page=2", "items": [{"name": "x"}]}, ["items"], []),
    ],
)
def test_suggest_paths(body, records, cursors):
    assert suggest_paths(body) == {"record_paths": records, "cursor_paths": cursors}


def test_paginated_cursor_without_a_path_is_refused_before_any_request(api):
    settings = RestApiReadSettings(url=api.url("/cursor"), pagination=PaginationConfig(pagination_type="cursor"))
    with pytest.raises(ValueError, match="cursor path"):
        fetch_rest_api(settings)
    assert api.request_count == 0


def test_a_sample_reads_one_page_without_a_cursor_path_and_reports_the_first_response(api):
    seen = []
    settings = RestApiReadSettings(
        url=api.url("/cursor"), sample_size=5, pagination=PaginationConfig(pagination_type="cursor")
    )
    df = fetch_rest_api(settings, on_page=lambda _entry, body: seen.append(body))
    assert df.height == 1
    assert seen == [{"rows": [{"id": 1}], "next": "c2"}]


def test_a_cursor_path_at_a_list_is_refused_after_the_first_page(api):
    url = api.route("/r", _answer({"feed": [{"id": 1}], "cursor": "c2"}))
    settings = RestApiReadSettings(
        url=url, record_path="feed", pagination=PaginationConfig(pagination_type="cursor", cursor_response_path="feed")
    )
    with pytest.raises(ValueError, match="points at a list"):
        fetch_rest_api(settings)
    assert api.request_count == 1

    sample = settings.model_copy(update={"sample_size": 5})
    assert fetch_rest_api(sample).height == 1


def _trace(settings: RestApiReadSettings, secret: str | None = None) -> list[dict]:
    entries: list[dict] = []
    fetch_rest_api(settings, secret=secret, on_page=lambda entry, _body: entries.append(entry))
    return entries


def test_the_trace_names_every_request_as_sent_and_why_the_read_stopped(api):
    settings = RestApiReadSettings(
        url=api.url("/cursor"),
        record_path="rows",
        headers={"X-Team": "data"},
        pagination=PaginationConfig(pagination_type="cursor", cursor_response_path="next"),
    )
    entries = _trace(settings)
    assert [e["url"] for e in entries] == [
        api.url("/cursor"),
        api.url("/cursor") + "?cursor=c2",
        api.url("/cursor") + "?cursor=c3",
    ]
    assert [e["next_cursor"] for e in entries] == ["c2", "c3", None]
    assert [e["stopped"] for e in entries] == [None, None, "no_next_cursor"]
    first = entries[0]
    assert (first["page"], first["method"], first["status"], first["records"]) == (1, "GET", 200, 1)
    assert first["headers"] == {"X-Team": "data"} and first["body"] is None
    assert first["bytes"] > 0 and first["elapsed_ms"] >= 0


def test_the_trace_shows_the_body_a_post_sent_with_its_cursor(api):
    entries = _trace(_at(api, _keyset()))
    assert entries[0]["body"] == {"app_id": "app-1", "entry_id": "entry-1", "limit": 3}
    assert entries[1]["body"]["data_id"] == "id03"
    assert entries[-1]["stopped"] == "empty_page"


@pytest.mark.parametrize(
    ("path", "pagination", "stopped"),
    [
        ("/items", PaginationConfig(), "single_request"),
        ("/offset", PaginationConfig(pagination_type="offset", page_size=2), "short_page"),
        ("/page", PaginationConfig(pagination_type="page"), "empty_page"),
        ("/page", PaginationConfig(pagination_type="page", max_pages=2), "max_pages"),
        ("/page", PaginationConfig(pagination_type="page", max_records=2), "max_records"),
    ],
)
def test_the_last_trace_entry_says_why_the_read_stopped(api, path, pagination, stopped):
    entries = _trace(RestApiReadSettings(url=api.url(path), pagination=pagination))
    assert [e["stopped"] for e in entries[:-1]] == [None] * (len(entries) - 1)
    assert entries[-1]["stopped"] == stopped


def test_a_repeated_cursor_is_the_stop_reason(api):
    entries = _trace(_at(api, _keyset("/keyset-echo")))
    assert entries[-1]["stopped"] == "cursor_repeated"


@pytest.mark.parametrize(
    "auth",
    [
        AuthConfig(auth_type="api_key", api_key_name="api_key", api_key_location="query"),
        AuthConfig(auth_type="api_key", api_key_name="X-API-Key", api_key_location="header"),
        AuthConfig(auth_type="bearer"),
        AuthConfig(auth_type="basic", basic_username="alice"),
    ],
)
def test_the_secret_never_reaches_the_trace(api, auth):
    entries = _trace(RestApiReadSettings(url=api.url("/items"), auth=auth), secret="s3cr3t-value")
    assert "s3cr3t-value" not in str(entries)
    assert SECRET_MASK in str(entries)


def test_the_shown_url_is_the_one_sent_with_only_the_secret_replaced(api):
    settings = RestApiReadSettings(
        url=api.url("/page"),
        query_params={"team": "data", "api_key": "typed-by-hand"},
        auth=AuthConfig(auth_type="api_key", api_key_name="api_key", api_key_location="query"),
        pagination=PaginationConfig(pagination_type="page", max_pages=2),
    )
    entries = _trace(settings, secret="s3cr3t-value")
    sent = [request.query for request in api.requests]
    assert sent == [
        {"team": "data", "api_key": "s3cr3t-value", "page": "1"},
        {"team": "data", "api_key": "s3cr3t-value", "page": "2"},
    ]
    shown = [dict(httpx.URL(entry["url"]).params) for entry in entries]
    assert shown == [{**query, "api_key": SECRET_MASK} for query in sent]


def test_a_failed_request_names_itself_and_the_answer_without_the_secret(api):
    api.route("/denied", _answer({"error": "token expired for key s3cr3t-value"}, status=401))
    settings = RestApiReadSettings(
        url=api.url("/denied"),
        query_params={"team": "data"},
        auth=AuthConfig(auth_type="api_key", api_key_name="api_key", api_key_location="query"),
    )
    with pytest.raises(RestApiRequestError) as raised:
        fetch_rest_api(settings, secret="s3cr3t-value")
    message = str(raised.value)
    assert message.startswith(f"GET {api.url('/denied')}?team=data&api_key=REDACTED answered 401")
    assert "token expired for key REDACTED" in message
    assert "s3cr3t-value" not in message
    assert raised.value.__cause__ is None and raised.value.__context__ is None


def test_a_failed_request_masks_the_secret_as_the_url_encodes_it(api):
    secret = "a+b/c=d&e f"
    api.route("/denied", _answer(text="bad key a%2Bb%2Fc%3Dd%26e+f or a%2Bb%2Fc%3Dd%26e%20f", status=401))
    settings = RestApiReadSettings(
        url=api.url("/denied"),
        auth=AuthConfig(auth_type="api_key", api_key_name="api_key", api_key_location="query"),
    )
    with pytest.raises(RestApiRequestError) as raised:
        fetch_rest_api(settings, secret=secret)
    assert str(raised.value).endswith("bad key REDACTED or REDACTED")


def test_read_rest_api_masks_a_secret_the_response_echoes(api, tmp_path):
    echoed = base64.b64encode(b"alice:s3cr3t-value").decode()
    api.route("/echo", _answer({"headers": {"Authorization": f"Basic {echoed}"}, "items": [{"key": "s3cr3t-value"}]}))
    settings = WorkerRestApiReadSettings(
        url=api.url("/echo"),
        record_path="items",
        sample_size=10,
        auth=AuthConfig(
            auth_type="basic", basic_username="alice", basic_password_encrypted=encrypt_secret("s3cr3t-value")
        ),
    )
    debug_file = tmp_path / "debug.json"
    df = read_rest_api(settings, debug_file=str(debug_file))
    assert df["key"].to_list() == ["s3cr3t-value"]

    written = debug_file.read_text()
    assert "s3cr3t-value" not in written and echoed not in written
    shown = json.loads(json.loads(written)["response_text"])
    assert shown == {"headers": {"Authorization": "Basic REDACTED"}, "items": [{"key": "REDACTED"}]}


def test_a_response_that_is_no_json_names_the_request_and_how_it_starts(api):
    url = api.route("/login", _answer(text="<html><title>Sign in</title></html>"))
    with pytest.raises(RestApiRequestError, match=r"answered 200, but the response was not valid JSON.*<html>"):
        fetch_rest_api(RestApiReadSettings(url=url))


def test_describe_response_gives_each_node_its_path_kind_and_size():
    tree = describe_response({"meta": {"next": "c2"}, "data": [{"id": 1, "tags": ["a", "b"]}, {"id": 2}], "n": []})
    assert (tree["kind"], tree["path"]) == ("object", "")
    meta, data, empty = tree["children"]
    assert meta["children"] == [{"key": "next", "path": "meta.next", "kind": "value", "preview": '"c2"'}]
    assert (data["kind"], data["length"], data["records"], data["path"]) == ("list", 2, True, "data")
    assert [(c["path"], c["kind"]) for c in data["children"]] == [("data.0.id", "value"), ("data.0.tags", "list")]
    assert data["children"][1]["preview"] == '["a", "b"]' and data["children"][1]["records"] is False
    assert (empty["length"], empty["records"], empty["children"]) == (0, False, [])


def test_describe_response_of_a_top_level_list_and_its_child_cap():
    tree = describe_response([{f"c{i}": i for i in range(5)}], max_children=3)
    assert (tree["kind"], tree["records"], tree["more"]) == ("list", True, 2)
    assert [c["path"] for c in tree["children"]] == ["0.c0", "0.c1", "0.c2"]


class _Lines:
    def __init__(self):
        self.lines: list[tuple[str, str]] = []

    def info(self, message, *args):
        self.lines.append(("info", message % args if args else message))

    def warning(self, message, *args):
        self.lines.append(("warning", message % args if args else message))


def test_read_rest_api_logs_each_page_and_writes_the_trace_of_a_run(api, tmp_path):
    token = encrypt_secret("s3cr3t-value")
    settings = WorkerRestApiReadSettings(
        url=api.url("/page"),
        auth=AuthConfig(auth_type="api_key", api_key_name="key", api_key_location="query", api_key_encrypted=token),
        pagination=PaginationConfig(pagination_type="page", max_pages=2),
    )
    log, debug_file = _Lines(), tmp_path / "debug.json"
    df = read_rest_api(settings, debug_file=str(debug_file), logger=log)
    assert df.height == 2

    pages = [line for _level, line in log.lines if line.startswith("Page ")]
    assert pages == [
        f"Page 1: GET {api.url('/page')}?key=REDACTED&page=1 answered 200 in {pages[0].split(' in ')[1]}",
        f"Page 2: GET {api.url('/page')}?key=REDACTED&page=2 answered 200 in {pages[1].split(' in ')[1]}",
    ]
    assert ("warning", "Stopped after 2 pages: the page limit was reached; the API may have more.") in log.lines
    assert "s3cr3t-value" not in str(log.lines)

    debug = json.loads(debug_file.read_text())
    assert set(debug) == {"trace"}
    assert [e["page"] for e in debug["trace"]] == [1, 2]
    assert "s3cr3t-value" not in debug_file.read_text()


def test_read_rest_api_writes_what_a_test_request_shows_of_the_first_response(api, tmp_path):
    settings = WorkerRestApiReadSettings(
        url=api.url("/page"), pagination=PaginationConfig(pagination_type="page", max_pages=2), sample_size=5
    )
    debug_file = tmp_path / "debug.json"
    df = read_rest_api(settings, debug_file=str(debug_file))
    assert df.height == 1

    debug = json.loads(debug_file.read_text())
    assert set(debug) == {"suggested_paths", "response_tree", "response_text", "response_truncated", "trace"}
    assert [e["page"] for e in debug["trace"]] == [1]
    assert debug["response_tree"]["records"] is True and debug["response_truncated"] is False
    assert json.loads(debug["response_text"]) == [{"id": 1}]
