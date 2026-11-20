"""Contract checks for the Catalog-cell transport (no graph/database imports)."""

import httpx
import pytest

from kernel_runtime import catalog_lookup, flowfile_client


@pytest.fixture
def route_transport(monkeypatch):
    real_client = httpx.Client
    requests = []

    def install(handler):
        def dispatch(request):
            requests.append(request)
            assert request.headers["X-Internal-Token"] == "test-internal-token"
            assert request.headers["X-Kernel-Id"] == "test-kernel"
            assert request.url.path.startswith("/catalog/")
            return handler(request)

        monkeypatch.setattr(flowfile_client, "_CORE_URL", "http://core")
        monkeypatch.setattr(
            flowfile_client,
            "_get_internal_auth_headers",
            lambda: {
                "X-Internal-Token": "test-internal-token",
                "X-Kernel-Id": "test-kernel",
            },
        )

        class RouteClient(real_client):
            def __init__(self, **kwargs):
                super().__init__(transport=httpx.MockTransport(dispatch), **kwargs)

        monkeypatch.setattr(catalog_lookup.httpx, "Client", RouteClient)
        return requests

    return install


@pytest.mark.parametrize(
    "reference",
    [
        {"catalog_table_id": 7},
        {"catalog_table_name": "orders", "catalog_namespace_id": 2},
        {"catalog_full_table_name": "General.default.orders"},
    ],
)
def test_resolves_table_with_authenticated_existing_routes(route_transport, reference):
    row = {
        "id": 7,
        "name": "orders",
        "namespace_id": 2,
        "table_type": "physical",
        "file_path": None,
        "scd2": {"is_current_column": "current"},
        "schema_columns": [{"name": "id", "dtype": "Int64"}],
    }

    def respond(request):
        if "catalog_table_id" in reference:
            assert request.url.path == "/catalog/tables/7"
            return httpx.Response(200, json=row)
        assert request.url.path == "/catalog/tables/resolve"
        assert request.url.params["strict"] == "true"
        assert request.url.params["q"] == reference.get("catalog_full_table_name", reference.get("catalog_table_name"))
        assert request.url.params.get("namespace_id") == ("2" if "catalog_namespace_id" in reference else None)
        return httpx.Response(200, json={"table": row})

    requests = route_transport(respond)
    result = catalog_lookup.lookup({"kind": "catalog_table", "args": reference})["result"]
    assert len(requests) == 1
    assert result["table_id"] == 7
    assert result["authorized"] is True
    assert result["scd2_config"] == row["scd2"]
    assert result["producer_fingerprint"] is None


@pytest.mark.parametrize("kind", ["catalog_table", "catalog_sql_tables"])
def test_catalog_graph_reads_explain_cloud_storage_limitation(route_transport, kind):
    row = {"id": 7, "name": "orders", "table_type": "physical", "file_path": "s3://bucket/orders"}
    requests = route_transport(lambda request: httpx.Response(200, json=row if kind == "catalog_table" else [row]))

    with pytest.raises(ValueError, match="Cloud-backed physical tables.*Catalog notebooks"):
        catalog_lookup.lookup({"kind": kind, "args": {"catalog_table_id": 7, "sql_query": "SELECT * FROM orders"}})
    assert len(requests) == 1


@pytest.mark.parametrize("status", [401, 403, 409, 500])
def test_route_errors_do_not_fall_back_to_local_or_session_lookup(route_transport, status):
    requests = route_transport(lambda request: httpx.Response(status, json={"detail": "denied or ambiguous"}))
    with pytest.raises(httpx.HTTPStatusError) as error:
        catalog_lookup.lookup({"kind": "catalog_table", "args": {"catalog_table_name": "orders"}})
    assert error.value.response.status_code == status
    assert len(requests) == 1


def test_missing_reference_has_a_clear_error(route_transport):
    route_transport(lambda request: httpx.Response(404, json={"detail": "missing"}))
    with pytest.raises(LookupError, match="orders.*not found"):
        catalog_lookup.lookup({"kind": "catalog_table", "args": {"catalog_table_name": "orders"}})


def test_sql_registers_only_tables_returned_by_authenticated_route(route_transport):
    route_transport(
        lambda request: httpx.Response(
            200,
            json=[
                {
                    "id": 7,
                    "name": "visible",
                    "namespace_id": 2,
                    "table_type": "physical",
                    "file_path": "/shared/catalog/table",
                },
                {"id": 8, "name": "unrelated_cloud", "table_type": "physical", "file_path": "s3://bucket/table"},
                {"id": 9, "name": "unrelated_view", "table_type": "virtual"},
            ],
        )
    )
    payload = {"kind": "catalog_sql_tables", "args": {"sql_query": 'SELECT * FROM "visible"'}}
    answer = catalog_lookup.lookup(payload)["result"]
    assert answer["table_paths"] == {"visible": "/shared/catalog/table"}
    assert answer["table_namespaces"] == {"visible": 2}
