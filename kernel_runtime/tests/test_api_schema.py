"""Tests for the editor API-schema introspection endpoint."""

from fastapi.testclient import TestClient

from kernel_runtime.api_schema import build_api_schema


def _find(schema, namespace, name):
    return next((e for e in schema if e["namespace"] == namespace and e["name"] == name), None)


class TestBuildApiSchema:
    def test_includes_flowfile_ctx_functions(self):
        schema = build_api_schema()
        read_input = _find(schema, "flowfile_ctx", "read_input")
        assert read_input is not None
        assert read_input["kind"] == "function"
        assert "name" in read_input["signature"]
        assert read_input["doc"]  # has a doc summary

    def test_includes_polars_frame_and_expr_methods(self):
        schema = build_api_schema()
        assert _find(schema, "LazyFrame", "filter") is not None
        assert _find(schema, "Expr", "alias") is not None
        col = _find(schema, "pl", "col")
        assert col is not None

    def test_no_private_symbols_and_unique_keys(self):
        schema = build_api_schema()
        keys = [(e["namespace"], e["name"]) for e in schema]
        assert all(not name.startswith("_") for _, name in keys)
        assert len(keys) == len(set(keys))  # de-duplicated


class TestApiSchemaEndpoint:
    def test_endpoint_returns_symbols(self, client: TestClient):
        resp = client.get("/api_schema")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) > 0
        sample = data[0]
        assert {"name", "kind", "namespace", "signature", "return_type", "doc"} <= set(sample)
        assert any(e["namespace"] == "flowfile_ctx" for e in data)
