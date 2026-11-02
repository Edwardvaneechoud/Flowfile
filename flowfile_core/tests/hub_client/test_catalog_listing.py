"""The hub's catalogs and tables in core's catalog tree, beside the local ones, and the node fields naming a hub table.

A connected user's ``GET /catalog/namespaces/tree`` answers the local catalogs first and then the hub's, each hub row
marked ``source: "hub"`` with the negative of the hub's id as its ``id``. Every other catalog route stays local.
"""

import logging
import time

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from flowfile_core import main
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.configs import logger as core_logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.hub_client import client, session
from flowfile_core.schemas import input_schema
from tests.hub_client.conftest import HUB_URL, FakeHubWithCatalog

CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}
SHARED = {"is_owner": False, "access_level": "use", "shared_by": "bob"}
READ_AS_IT_IS = "A table on the hub is read as it is now: no version, history view, SQL or change feed"


@pytest.fixture
def hub(monkeypatch) -> FakeHubWithCatalog:
    fake = FakeHubWithCatalog()
    monkeypatch.setattr(client, "_module_http", fake.http())
    return fake


@pytest.fixture
def api(store) -> TestClient:
    c = TestClient(main.app)
    token = c.post("/auth/token").json()["access_token"]
    c.headers["Authorization"] = f"Bearer {token}"
    return c


@pytest.fixture
def connected(api, hub) -> TestClient:
    assert api.post("/hub_client/connection", json=CONNECT).status_code == 200
    hub.calls.clear()
    return api


@pytest.fixture
def user_id(api) -> int:
    return api.get("/auth/users/me").json()["id"]


@pytest.fixture
def warnings():
    """The warnings core's logger writes during the test (it does not propagate, so caplog sees nothing)."""
    records: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if record.levelno == logging.WARNING:
                records.append(record.getMessage())

    capture = _Capture()
    core_logger.addHandler(capture)
    yield records
    core_logger.removeHandler(capture)


def _local_tree(user_id: int) -> list[dict]:
    with get_db_context() as db:
        tree = CatalogService(SQLAlchemyCatalogRepository(db)).get_namespace_tree(user_id=user_id)
        return [node.model_dump(mode="json") for node in tree]


def _seed(hub: FakeHubWithCatalog) -> dict:
    sales = hub.seed_catalog("sales", storage_uri="s3://acme/sales", storage_connection_name="acme-s3", access=SHARED)
    raw = hub.seed_schema(sales, "raw", description="Landed as it came")
    curated = hub.seed_schema(sales, "curated")
    finance = hub.seed_catalog("finance")
    return {
        "sales": sales,
        "raw": raw,
        "curated": curated,
        "finance": finance,
        "orders": hub.seed_table(raw, "orders", row_count=120, access=SHARED),
        "customers": hub.seed_table(raw, "customers"),
        "revenue": hub.seed_table(curated, "revenue", is_remote_storage=True),
        "ledger": hub.seed_table(finance, "ledger"),
    }


def test_not_connected_answers_the_local_tree_and_never_asks_a_hub(api, user_id, monkeypatch):
    def refuse(request):
        pytest.fail(f"the hub was called: {request.method} {request.url}")

    monkeypatch.setattr(client, "_module_http", httpx.Client(transport=httpx.MockTransport(refuse)))
    resp = api.get("/catalog/namespaces/tree")
    assert resp.status_code == 200
    assert resp.json() == _local_tree(user_id)


def test_connected_lists_the_hub_catalogs_after_the_local_ones(connected, hub, user_id):
    seeded = _seed(hub)
    resp = connected.get("/catalog/namespaces/tree")
    assert resp.status_code == 200
    tree = resp.json()
    local = _local_tree(user_id)
    assert local, "the local catalog has its seeded General catalog"
    assert tree[: len(local)] == local
    assert all(node["source"] == "local" and node["hub_id"] is None for node in local)

    hub_rows = tree[len(local) :]
    assert [node["name"] for node in hub_rows] == ["sales", "finance"]
    sales, finance = hub_rows
    assert sales["id"] == -seeded["sales"]["id"] and sales["hub_id"] == seeded["sales"]["id"]
    assert sales["parent_id"] is None and sales["level"] == 0
    assert sales["storage_uri"] == "s3://acme/sales" and sales["storage_connection_name"] == "acme-s3"
    assert sales["access"] == SHARED
    assert [schema["name"] for schema in sales["children"]] == ["raw", "curated"]
    raw, curated = sales["children"]
    assert raw["id"] == -seeded["raw"]["id"] and raw["hub_id"] == seeded["raw"]["id"]
    assert raw["parent_id"] == sales["id"] and raw["description"] == "Landed as it came"
    assert [t["name"] for t in raw["tables"]] == ["orders", "customers"]
    assert [t["name"] for t in curated["tables"]] == ["revenue"]
    assert [t["name"] for t in finance["tables"]] == ["ledger"] and finance["children"] == []
    assert sales["tables"] == []

    orders = raw["tables"][0]
    assert orders["id"] == -seeded["orders"]["id"] and orders["hub_id"] == seeded["orders"]["id"]
    assert orders["hub_namespace_id"] == seeded["raw"]["id"] and orders["namespace_id"] == raw["id"]
    assert orders["row_count"] == 120 and orders["access"] == SHARED
    assert curated["tables"][0]["is_remote_storage"] is True

    namespaces = [sales, finance, raw, curated]
    tables = [t for ns in namespaces for t in ns["tables"]]
    for row in namespaces + tables:
        assert row["source"] == "hub" and row["id"] < 0 and row["id"] == -row["hub_id"]
    for table in tables:
        assert table["namespace_id"] == -table["hub_namespace_id"]
    for ns in namespaces:
        assert ns["flows"] == [] and ns["notebooks"] == []
        assert ns["artifacts"] == [] and ns["visualizations"] == []

    assert hub.catalog_calls == [
        ("/catalog/namespaces", ""),
        ("/catalog/namespaces", f"parent_id={seeded['sales']['id']}"),
        ("/catalog/namespaces", f"parent_id={seeded['finance']['id']}"),
        ("/catalog/tables", ""),
    ]
    assert not any(path.startswith("/hub/flows") for _, path in hub.calls)


def test_a_hub_that_is_down_leaves_its_catalogs_out_with_one_warning(connected, hub, user_id, warnings):
    _seed(hub)
    hub.down = True
    resp = connected.get("/catalog/namespaces/tree")
    assert resp.status_code == 200
    assert resp.json() == _local_tree(user_id)
    assert len(warnings) == 1 and HUB_URL in warnings[0]


@pytest.mark.parametrize("refusal", ["expired session", "token refused"])
def test_a_hub_answering_401_leaves_its_catalogs_out_with_one_warning(
    connected, hub, user_id, warnings, monkeypatch, refusal
):
    _seed(hub)
    if refusal == "expired session":
        monkeypatch.setattr(session, "_access_tokens", {})
        hub.live_refresh.clear()
    else:
        monkeypatch.setattr(session, "_access_tokens", {user_id: ("not-a-hub-token", time.time() + 3600)})
    resp = connected.get("/catalog/namespaces/tree")
    assert resp.status_code == 200
    assert resp.json() == _local_tree(user_id)
    assert len(warnings) == 1 and HUB_URL in warnings[0]
    if refusal == "expired session":
        assert ("POST", "/auth/refresh") in hub.calls and hub.catalog_calls == []
    else:
        assert hub.catalog_calls == [("/catalog/namespaces", "")]


def test_the_other_catalog_routes_stay_local(connected, hub):
    seeded = _seed(hub)
    assert connected.get("/catalog/tables").status_code == 200
    assert connected.get("/catalog/namespaces").status_code == 200
    assert connected.get(f"/catalog/tables/{-seeded['orders']['id']}").status_code == 404
    assert hub.calls == []


class TestHubTableReader:
    def test_local_readers_validate_as_before(self):
        reader = input_schema.NodeCatalogReader(
            flow_id=1, node_id=1, catalog_table_id=4, delta_version=2, sql_query=None, cdc_mode="off"
        )
        assert reader.hub_table_id is None and reader.hub_namespace_id is None
        input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=4, cdc_mode="since_last_run")

    def test_a_hub_table_read_as_it_is_now_validates(self):
        reader = input_schema.NodeCatalogReader(flow_id=1, node_id=1, hub_table_id=101, hub_namespace_id=2)
        assert (reader.hub_table_id, reader.hub_namespace_id) == (101, 2)

    def test_a_hub_table_needs_its_namespace(self):
        with pytest.raises(ValidationError, match="hub_table_id and hub_namespace_id together"):
            input_schema.NodeCatalogReader(flow_id=1, node_id=1, hub_table_id=101)

    @pytest.mark.parametrize(
        "extra",
        [
            {"delta_version": 3},
            {"scd2_view": "active"},
            {"scd2_view": "active_at", "scd2_as_of": "2026-11-01T00:00:00Z"},
            {"sql_query": "SELECT * FROM orders"},
            {"cdc_mode": "since_last_run"},
            {"cdc_mode": "since_version", "cdc_from_version": 2},
        ],
    )
    def test_a_hub_table_refuses_versions_views_sql_and_change_feeds(self, extra):
        with pytest.raises(ValidationError, match=READ_AS_IT_IS):
            input_schema.NodeCatalogReader(flow_id=1, node_id=1, hub_table_id=101, hub_namespace_id=2, **extra)


class TestHubNamespaceWriter:
    def test_local_writes_validate_as_before(self):
        for mode in ("virtual", "overwrite"):
            assert input_schema.CatalogWriteSettings(table_name="t", write_mode=mode).hub_namespace_id is None
        input_schema.CatalogWriteSettings(table_name="t", write_mode="scd2", merge_keys=["id"])
        input_schema.CatalogWriteSettings(table_name="t", write_mode="append", track_changes=True)

    @pytest.mark.parametrize("mode", ["overwrite", "append", "error"])
    def test_a_physical_write_to_a_hub_namespace_validates(self, mode):
        settings = input_schema.CatalogWriteSettings(table_name="t", write_mode=mode, hub_namespace_id=2)
        assert settings.hub_namespace_id == 2

    @pytest.mark.parametrize(
        "fields, message",
        [
            ({"write_mode": "virtual"}, "A virtual table is kept in the local catalog, not on the hub"),
            ({"write_mode": "scd2", "merge_keys": ["id"]}, "An SCD2 table on the hub is not supported yet"),
            ({"write_mode": "append", "track_changes": True}, "Change tracking is set on the hub"),
        ],
    )
    def test_a_hub_namespace_refuses_virtual_scd2_and_tracked_writes(self, fields, message):
        with pytest.raises(ValidationError, match=message):
            input_schema.CatalogWriteSettings(table_name="t", hub_namespace_id=2, **fields)
