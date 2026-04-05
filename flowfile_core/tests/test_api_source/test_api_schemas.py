"""Tests for API schema models — validation, serialization, discriminated unions."""

import json

import pytest
from pydantic import SecretStr

from flowfile_core.schemas.api_schemas import (
    ApiKeyAuth,
    ApiReadSettings,
    BasicAuth,
    BearerAuth,
    CursorPagination,
    CustomHeaderAuth,
    KeysetPagination,
    LinkHeaderPagination,
    NoPagination,
    OAuth2ClientCredentials,
    OffsetPagination,
    PageNumberPagination,
)


class TestAuthModels:
    def test_api_key_auth(self):
        auth = ApiKeyAuth(key="X-API-Key", value=SecretStr("secret123"))
        assert auth.auth_type == "api_key"
        assert auth.key == "X-API-Key"
        assert auth.value.get_secret_value() == "secret123"
        assert auth.location == "header"

    def test_api_key_auth_query(self):
        auth = ApiKeyAuth(key="api_key", value=SecretStr("secret"), location="query")
        assert auth.location == "query"

    def test_bearer_auth(self):
        auth = BearerAuth(token=SecretStr("my-token"))
        assert auth.auth_type == "bearer"
        assert auth.token.get_secret_value() == "my-token"

    def test_basic_auth(self):
        auth = BasicAuth(username="user", password=SecretStr("pass"))
        assert auth.auth_type == "basic"

    def test_oauth2_client_credentials(self):
        auth = OAuth2ClientCredentials(
            token_url="https://auth.example.com/token",
            client_id="my-client",
            client_secret=SecretStr("my-secret"),
            scope="read:data",
        )
        assert auth.auth_type == "oauth2_client_credentials"
        assert auth.scope == "read:data"

    def test_custom_header_auth(self):
        auth = CustomHeaderAuth(headers={"X-Custom": SecretStr("val1"), "X-Other": SecretStr("val2")})
        assert auth.auth_type == "custom_headers"
        assert len(auth.headers) == 2

    def test_auth_discriminated_union_from_dict(self):
        data = {"auth_type": "bearer", "token": "my-token"}
        settings = ApiReadSettings(url="https://example.com", auth=data)
        assert isinstance(settings.auth, BearerAuth)

        data = {"auth_type": "api_key", "key": "X-Key", "value": "secret"}
        settings = ApiReadSettings(url="https://example.com", auth=data)
        assert isinstance(settings.auth, ApiKeyAuth)


class TestPaginationModels:
    def test_no_pagination(self):
        p = NoPagination()
        assert p.pagination_type == "none"

    def test_offset_pagination(self):
        p = OffsetPagination(limit=50, total_path=("meta", "total"), max_records=1000)
        assert p.pagination_type == "offset"
        assert p.total_path == ("meta", "total")
        assert p.limit == 50

    def test_page_number_pagination(self):
        p = PageNumberPagination(page_size=25, start_page=0, total_pages_path=("pagination", "total_pages"))
        assert p.pagination_type == "page_number"
        assert p.start_page == 0

    def test_cursor_pagination(self):
        p = CursorPagination(cursor_path=("meta", "next_cursor"), cursor_param="after")
        assert p.pagination_type == "cursor"
        assert p.cursor_path == ("meta", "next_cursor")

    def test_link_header_pagination(self):
        p = LinkHeaderPagination(max_pages=10)
        assert p.rel == "next"

    def test_keyset_pagination(self):
        p = KeysetPagination(key_field="id", param_name="after_id", page_size=100, page_size_param="limit")
        assert p.pagination_type == "keyset"

    def test_pagination_discriminated_union(self):
        data = {"pagination_type": "cursor", "cursor_path": ["meta", "next"], "cursor_param": "c"}
        settings = ApiReadSettings(url="https://example.com", pagination=data)
        assert isinstance(settings.pagination, CursorPagination)


class TestApiReadSettings:
    def test_defaults(self):
        s = ApiReadSettings(url="https://example.com")
        assert s.method == "GET"
        assert isinstance(s.pagination, NoPagination)
        assert s.records_path is None
        assert s.timeout == 30.0
        assert s.max_retries == 3

    def test_full_settings(self):
        s = ApiReadSettings(
            url="https://api.example.com/data",
            method="POST",
            headers={"Accept": "application/json"},
            query_params={"format": "json"},
            body={"query": "test"},
            auth=BearerAuth(token=SecretStr("token")),
            pagination=OffsetPagination(limit=50),
            records_path=("data", "items"),
            timeout=60.0,
            rate_limit_delay=0.5,
        )
        assert s.method == "POST"
        assert s.records_path == ("data", "items")
        assert isinstance(s.auth, BearerAuth)
        assert isinstance(s.pagination, OffsetPagination)

    def test_json_serialization_roundtrip(self):
        s = ApiReadSettings(
            url="https://example.com",
            pagination=CursorPagination(cursor_path=("next",), cursor_param="cursor"),
            records_path=("data",),
        )
        json_str = s.model_dump_json()
        restored = ApiReadSettings.model_validate_json(json_str)
        assert restored.records_path == ("data",)
        assert isinstance(restored.pagination, CursorPagination)
        assert restored.pagination.cursor_path == ("next",)
