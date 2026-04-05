"""Tests for the API client — auth resolution, JSON path traversal, and pagination."""

from unittest.mock import MagicMock, patch

import httpx
import pytest
from pydantic import SecretStr

from flowfile_core.flowfile.sources.external_sources.api_source.client import (
    build_auth,
    extract_records,
    fetch_sample,
    paginated_iter,
    parse_link_header,
    resolve_json_path,
    resolve_json_path_safe,
)
from flowfile_core.schemas.api_schemas import (
    ApiKeyAuth,
    ApiReadSettings,
    BasicAuth,
    BearerAuth,
    CursorPagination,
    KeysetPagination,
    LinkHeaderPagination,
    NoPagination,
    OffsetPagination,
    PageNumberPagination,
)


class TestResolveJsonPath:
    def test_simple_path(self):
        data = {"data": {"users": [1, 2, 3]}}
        assert resolve_json_path(data, ("data", "users")) == [1, 2, 3]

    def test_single_key(self):
        data = {"results": [{"id": 1}]}
        assert resolve_json_path(data, ("results",)) == [{"id": 1}]

    def test_missing_key_raises(self):
        with pytest.raises(KeyError):
            resolve_json_path({"a": 1}, ("b",))

    def test_non_dict_raises(self):
        with pytest.raises(KeyError):
            resolve_json_path([1, 2], ("key",))

    def test_safe_returns_none(self):
        assert resolve_json_path_safe({"a": 1}, ("b", "c")) is None
        assert resolve_json_path_safe(None, ("a",)) is None


class TestExtractRecords:
    def test_none_path_with_list(self):
        assert extract_records([{"id": 1}], None) == [{"id": 1}]

    def test_none_path_with_dict(self):
        assert extract_records({"id": 1}, None) == [{"id": 1}]

    def test_with_path(self):
        data = {"data": {"items": [{"id": 1}, {"id": 2}]}}
        assert extract_records(data, ("data", "items")) == [{"id": 1}, {"id": 2}]

    def test_single_object_at_path(self):
        data = {"data": {"item": {"id": 1}}}
        assert extract_records(data, ("data", "item")) == [{"id": 1}]


class TestBuildAuth:
    def test_none_auth(self):
        headers, params = build_auth(None)
        assert headers == {}
        assert params == {}

    def test_bearer_auth(self):
        auth = BearerAuth(token=SecretStr("my-token"))
        headers, params = build_auth(auth)
        assert headers == {"Authorization": "Bearer my-token"}
        assert params == {}

    def test_api_key_header(self):
        auth = ApiKeyAuth(key="X-API-Key", value=SecretStr("secret"), location="header")
        headers, params = build_auth(auth)
        assert headers == {"X-API-Key": "secret"}

    def test_api_key_query(self):
        auth = ApiKeyAuth(key="api_key", value=SecretStr("secret"), location="query")
        headers, params = build_auth(auth)
        assert params == {"api_key": "secret"}

    def test_basic_auth(self):
        auth = BasicAuth(username="user", password=SecretStr("pass"))
        headers, params = build_auth(auth)
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Basic ")


class TestParseLinkHeader:
    def test_single_link(self):
        header = '<https://api.example.com/items?page=2>; rel="next"'
        links = parse_link_header(header)
        assert links["next"] == "https://api.example.com/items?page=2"

    def test_multiple_links(self):
        header = '<https://api.example.com?page=2>; rel="next", <https://api.example.com?page=5>; rel="last"'
        links = parse_link_header(header)
        assert links["next"] == "https://api.example.com?page=2"
        assert links["last"] == "https://api.example.com?page=5"

    def test_none_header(self):
        assert parse_link_header(None) == {}


class TestPaginatedIter:
    """Test pagination strategies using mocked HTTP responses."""

    def _mock_response(self, json_data, status_code=200, headers=None):
        resp = MagicMock(spec=httpx.Response)
        resp.json.return_value = json_data
        resp.status_code = status_code
        resp.headers = headers or {}
        resp.raise_for_status = MagicMock()
        return resp

    @patch("flowfile_core.flowfile.sources.external_sources.api_source.client.fetch_page")
    def test_no_pagination(self, mock_fetch):
        mock_fetch.return_value = self._mock_response([{"id": 1}, {"id": 2}])
        settings = ApiReadSettings(url="https://example.com", pagination=NoPagination())
        records = list(paginated_iter(settings))
        assert len(records) == 2
        assert mock_fetch.call_count == 1

    @patch("flowfile_core.flowfile.sources.external_sources.api_source.client.fetch_page")
    def test_offset_pagination_with_total(self, mock_fetch):
        # Page 1: 2 records, total=3
        # Page 2: 1 record
        mock_fetch.side_effect = [
            self._mock_response({"data": [{"id": 1}, {"id": 2}], "meta": {"total": 3}}),
            self._mock_response({"data": [{"id": 3}], "meta": {"total": 3}}),
        ]
        settings = ApiReadSettings(
            url="https://example.com",
            pagination=OffsetPagination(limit=2, total_path=("meta", "total")),
            records_path=("data",),
        )
        records = list(paginated_iter(settings))
        assert len(records) == 3
        assert mock_fetch.call_count == 2

    @patch("flowfile_core.flowfile.sources.external_sources.api_source.client.fetch_page")
    def test_offset_pagination_stops_on_empty(self, mock_fetch):
        mock_fetch.side_effect = [
            self._mock_response({"data": [{"id": 1}]}),
            self._mock_response({"data": []}),
        ]
        settings = ApiReadSettings(
            url="https://example.com",
            pagination=OffsetPagination(limit=10),
            records_path=("data",),
        )
        records = list(paginated_iter(settings))
        assert len(records) == 1

    @patch("flowfile_core.flowfile.sources.external_sources.api_source.client.fetch_page")
    def test_offset_pagination_max_records(self, mock_fetch):
        mock_fetch.return_value = self._mock_response({"data": [{"id": i} for i in range(100)]})
        settings = ApiReadSettings(
            url="https://example.com",
            pagination=OffsetPagination(limit=100, max_records=50),
            records_path=("data",),
        )
        records = list(paginated_iter(settings))
        assert len(records) == 50

    @patch("flowfile_core.flowfile.sources.external_sources.api_source.client.fetch_page")
    def test_cursor_pagination(self, mock_fetch):
        mock_fetch.side_effect = [
            self._mock_response({"data": [{"id": 1}], "meta": {"next_cursor": "abc"}}),
            self._mock_response({"data": [{"id": 2}], "meta": {"next_cursor": "def"}}),
            self._mock_response({"data": [{"id": 3}], "meta": {"next_cursor": None}}),
        ]
        settings = ApiReadSettings(
            url="https://example.com",
            pagination=CursorPagination(cursor_path=("meta", "next_cursor"), cursor_param="cursor"),
            records_path=("data",),
        )
        records = list(paginated_iter(settings))
        assert len(records) == 3

    @patch("flowfile_core.flowfile.sources.external_sources.api_source.client.fetch_page")
    def test_page_number_pagination(self, mock_fetch):
        mock_fetch.side_effect = [
            self._mock_response({"items": [{"id": 1}, {"id": 2}], "total_pages": 2}),
            self._mock_response({"items": [{"id": 3}], "total_pages": 2}),
        ]
        settings = ApiReadSettings(
            url="https://example.com",
            pagination=PageNumberPagination(page_size=2, total_pages_path=("total_pages",)),
            records_path=("items",),
        )
        records = list(paginated_iter(settings))
        assert len(records) == 3

    @patch("flowfile_core.flowfile.sources.external_sources.api_source.client.fetch_page")
    def test_link_header_pagination(self, mock_fetch):
        mock_fetch.side_effect = [
            self._mock_response(
                [{"id": 1}],
                headers={"link": '<https://api.example.com?page=2>; rel="next"'},
            ),
            self._mock_response([{"id": 2}], headers={}),
        ]
        settings = ApiReadSettings(
            url="https://api.example.com",
            pagination=LinkHeaderPagination(),
        )
        records = list(paginated_iter(settings))
        assert len(records) == 2

    @patch("flowfile_core.flowfile.sources.external_sources.api_source.client.fetch_page")
    def test_keyset_pagination(self, mock_fetch):
        mock_fetch.side_effect = [
            self._mock_response({"data": [{"id": 1}, {"id": 2}]}),
            self._mock_response({"data": [{"id": 3}]}),
            self._mock_response({"data": []}),
        ]
        settings = ApiReadSettings(
            url="https://example.com",
            pagination=KeysetPagination(key_field="id", param_name="after_id"),
            records_path=("data",),
        )
        records = list(paginated_iter(settings))
        assert len(records) == 3


class TestFetchSample:
    @patch("flowfile_core.flowfile.sources.external_sources.api_source.client.fetch_page")
    def test_fetch_sample_limits(self, mock_fetch):
        mock_fetch.return_value = MagicMock(
            json=MagicMock(return_value=[{"id": i} for i in range(100)]),
            status_code=200,
            headers={},
            raise_for_status=MagicMock(),
        )
        settings = ApiReadSettings(url="https://example.com")
        sample = fetch_sample(settings, n=5)
        assert len(sample) == 5
