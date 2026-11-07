"""The hub's catalogs, tables and flows in core's catalog tree, beside the local ones, and the node fields naming a hub
table.

A connected user's ``GET /catalog/namespaces/tree`` answers the local catalogs first and then the hub's, each hub row
marked ``source: "hub"`` with the negative of the hub's id as its ``id``. Beside the hub's, every row says where it
lives (``place``), and a hub catalog this Flowfile cannot use says why (``unavailable_reason``). A hub schema lists
its published flows with the newest version and who published it; a hub that refuses the flows read keeps its tables.
Every other catalog route stays local.
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
from flowfile_core.hub_client import catalog as hub_catalog
from flowfile_core.hub_client import client, session
from flowfile_core.schemas import input_schema
from tests.hub_client.conftest import HUB_URL, INFO, FakeHubWithCatalog

CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}
SHARED = {"is_owner": False, "access_level": "use", "shared_by": "bob"}
READ_AS_IT_IS = "A table on the hub is read as it is now: no version, history view, SQL or change feed"
ON_ITS_OWN_DISK = (
    "The hub keeps the tables of {} on its own disk; this Flowfile cannot read them. "
    "Pick a catalog on the organisation's storage, or ask the hub's admin to put this one there."
)
ON_AZURE = (
    "Flowfile reads and writes hub tables on S3 storage; this catalog is on az:// storage. "
    "Pick a catalog on the organisation's S3 storage, or ask the hub's admin to put this one there."
)


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


def _local_tree(user_id: int, place: str | None = None) -> list[dict]:
    """The service's own tree; with ``place``, every namespace row in it said to live there."""
    with get_db_context() as db:
        tree = CatalogService(SQLAlchemyCatalogRepository(db)).get_namespace_tree(user_id=user_id)
        rows = [node.model_dump(mode="json") for node in tree]
    if place is not None:
        _set_place(rows, place)
    return rows


def _set_place(rows: list[dict], place: str) -> None:
    for row in rows:
        row["place"] = place
        _set_place(row["children"], place)


def _namespace_rows(rows: list[dict]) -> list[dict]:
    return [row for root in rows for row in (root, *root["children"])]


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
    local = _local_tree(user_id, place="On this computer")
    assert local, "the local catalog has its seeded General catalog"
    assert tree[: len(local)] == local
    assert all(node["source"] == "local" and node["hub_id"] is None for node in local)
    assert all(row["offered"] and row["unavailable_reason"] is None for row in _namespace_rows(local))

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
        assert ns["place"] == INFO["org_name"] and ns["offered"] is True
    assert [ns["unavailable_reason"] for ns in (sales, raw, curated)] == [None, None, None]
    assert finance["unavailable_reason"] == ON_ITS_OWN_DISK.format("finance")

    assert hub.catalog_calls == [
        ("/catalog/namespaces", ""),
        ("/catalog/namespaces", f"parent_id={seeded['sales']['id']}"),
        ("/catalog/namespaces", f"parent_id={seeded['finance']['id']}"),
        ("/catalog/tables", ""),
        ("/hub/flows", ""),
    ]


def test_a_hub_schemas_published_flows_are_listed_under_it_with_the_newest_version(connected, hub):
    seeded = _seed(hub)
    clean = hub.seed_flow(seeded["raw"], "orders-clean", latest_version=3, author_name="ana", owner_id=7)
    hub.seed_flow(seeded["raw"], "draft", latest_version=None)
    hub.seed_flow(seeded["finance"], "close-the-books", author_name=None)

    tree = connected.get("/catalog/namespaces/tree").json()

    [sales] = [root for root in tree if root["name"] == "sales"]
    raw, curated = sales["children"]
    assert sales["flows"] == [] and curated["flows"] == []
    [flow] = raw["flows"]
    assert flow["source"] == "hub" and flow["id"] == -clean["registration_id"]
    assert flow["hub_id"] == clean["registration_id"] and flow["flow_uuid"] == clean["flow_uuid"]
    assert flow["name"] == "orders-clean" and flow["namespace_id"] == raw["id"] and flow["owner_id"] == 7
    assert flow["latest_version"] == 3 and flow["author_name"] == "ana"
    assert flow["flow_path"] == f"hub://flows/{clean['flow_uuid']}"
    assert flow["file_exists"] is True and flow["run_count"] == 0 and flow["last_run_success"] is None
    assert flow["created_at"] == flow["updated_at"] == "2026-11-01T12:00:00Z"
    [finance] = [root for root in tree if root["name"] == "finance"]
    assert [(f["name"], f["latest_version"], f["author_name"]) for f in finance["flows"]] == [
        ("close-the-books", 1, None)
    ]
    assert hub.catalog_calls[-1] == ("/hub/flows", "")


def test_a_hub_that_refuses_its_flows_keeps_its_tables_in_the_tree_with_one_warning(connected, hub, warnings):
    seeded = _seed(hub)
    hub.seed_flow(seeded["raw"], "orders-clean")
    hub.flows_failure = 404

    tree = connected.get("/catalog/namespaces/tree").json()

    [sales] = [root for root in tree if root["name"] == "sales"]
    raw = sales["children"][0]
    assert [t["name"] for t in raw["tables"]] == ["orders", "customers"] and raw["flows"] == []
    assert len(warnings) == 1 and "flows of the hub" in warnings[0] and HUB_URL in warnings[0]


def test_a_hub_answering_flow_rows_core_cannot_read_keeps_its_tables_with_one_warning(connected, hub, warnings):
    seeded = _seed(hub)
    hub.seed_flow(seeded["raw"], "orders-clean")
    hub.flows[0]["published_at"] = None

    tree = connected.get("/catalog/namespaces/tree").json()

    [sales] = [root for root in tree if root["name"] == "sales"]
    assert [t["name"] for t in sales["children"][0]["tables"]] == ["orders", "customers"]
    assert sales["children"][0]["flows"] == []
    assert len(warnings) == 1 and "did not answer /hub/flows as a Flowfile hub does" in warnings[0]


def test_a_hub_catalog_this_flowfile_cannot_use_says_why_on_each_of_its_rows(connected, hub):
    lake = hub.seed_catalog("lake", storage_uri="az://lake/data", storage_connection_name="acme-adls")
    landing = hub.seed_schema(lake, "landing")
    hub.seed_table(landing, "events")
    scratch = hub.seed_catalog("scratch")
    hub.seed_schema(scratch, "notes")
    main = hub.seed_catalog("main", storage_uri="s3://acme/main")
    hub.seed_schema(main, "default")
    hub.seed_catalog("bare", storage_uri="/srv/hub/catalogs/bare")

    tree = connected.get("/catalog/namespaces/tree").json()

    reasons = {
        ".".join(filter(None, (root["name"], schema and schema["name"]))): (schema or root)["unavailable_reason"]
        for root in tree
        if root["source"] == "hub"
        for schema in (None, *root["children"])
    }
    assert reasons == {
        "lake": ON_AZURE,
        "lake.landing": ON_AZURE,
        "scratch": ON_ITS_OWN_DISK.format("scratch"),
        "scratch.notes": ON_ITS_OWN_DISK.format("scratch.notes"),
        "main": None,
        "main.default": None,
        "bare": ON_ITS_OWN_DISK.format("bare"),
    }
    [lake_row] = [root for root in tree if root["name"] == "lake"]
    assert [t["name"] for t in lake_row["children"][0]["tables"]] == ["events"]


def test_a_connection_stored_without_the_hubs_name_is_named_by_its_url(connected, hub, user_id):
    """A connection made before the hub's name was kept with it."""
    hub.seed_schema(hub.seed_catalog("main", storage_uri="s3://acme/main"), "default")
    stored = session.load(user_id)
    session.save(user_id, {k: v for k, v in stored.items() if k != "org_name"})

    tree = connected.get("/catalog/namespaces/tree").json()

    assert {row["place"] for row in _namespace_rows(tree) if row["source"] == "hub"} == {HUB_URL}
    assert {row["place"] for row in _namespace_rows(tree) if row["source"] == "local"} == {"On this computer"}


def test_a_tree_read_names_the_hub_from_the_stored_connection_and_never_asks_it(connected, hub):
    hub.seed_schema(hub.seed_catalog("main", storage_uri="s3://acme/main"), "default")
    hub.info = {**INFO, "org_name": "Acme Renamed"}

    tree = connected.get("/catalog/namespaces/tree").json()

    assert {row["place"] for row in _namespace_rows(tree) if row["source"] == "hub"} == {INFO["org_name"]}
    assert ("GET", "/hub/info") not in hub.calls


def test_a_server_says_its_own_catalogs_are_on_this_server(connected, hub, user_id, monkeypatch):
    hub.seed_schema(hub.seed_catalog("main", storage_uri="s3://acme/main"), "default")
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    with get_db_context() as db:
        local = CatalogService(SQLAlchemyCatalogRepository(db)).get_namespace_tree(user_id=user_id)

    rows = [node.model_dump(mode="json") for node in hub_catalog.merged(user_id, local)]

    assert {row["place"] for row in _namespace_rows(rows) if row["source"] == "local"} == {"On this server"}
    assert all(row["offered"] for row in _namespace_rows(rows))


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


@pytest.mark.parametrize(
    "malformed", ["a root without an id", "a schema with a text id", "a table in a text namespace"]
)
def test_a_hub_answering_rows_core_cannot_read_leaves_its_catalogs_out_with_one_warning(
    connected, hub, user_id, warnings, malformed
):
    seeded = _seed(hub)
    if malformed == "a root without an id":
        del next(ns for ns in hub.namespaces if ns["id"] == seeded["finance"]["id"])["id"]
    elif malformed == "a schema with a text id":
        next(ns for ns in hub.namespaces if ns["id"] == seeded["raw"]["id"])["id"] = "raw"
    else:
        next(t for t in hub.tables if t["id"] == seeded["ledger"]["id"])["namespace_id"] = "finance"
    resp = connected.get("/catalog/namespaces/tree")
    assert resp.status_code == 200
    assert resp.json() == _local_tree(user_id)
    assert len(warnings) == 1 and HUB_URL in warnings[0]


def test_the_other_catalog_routes_stay_local(connected, hub):
    seeded = _seed(hub)
    assert connected.get("/catalog/tables").status_code == 200
    assert connected.get("/catalog/namespaces").status_code == 200
    assert connected.get(f"/catalog/tables/{-seeded['orders']['id']}").status_code == 404
    assert hub.calls == []


def test_the_overview_tree_is_the_listing_tree(connected, hub, user_id):
    _seed(hub)
    overview = connected.get("/catalog/overview")
    assert overview.status_code == 200
    tree = overview.json()["tree"]
    assert tree == connected.get("/catalog/namespaces/tree").json()
    assert [node["name"] for node in tree[len(_local_tree(user_id)) :]] == ["sales", "finance"]
    assert all(table["source"] == "local" for table in overview.json()["tables"])


def test_the_overview_of_a_user_not_connected_never_asks_a_hub(api, user_id, monkeypatch):
    def refuse(request):
        pytest.fail(f"the hub was called: {request.method} {request.url}")

    monkeypatch.setattr(client, "_module_http", httpx.Client(transport=httpx.MockTransport(refuse)))
    overview = api.get("/catalog/overview")
    assert overview.status_code == 200
    assert overview.json()["tree"] == _local_tree(user_id)


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
