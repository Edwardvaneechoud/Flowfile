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
from flowfile_core.catalog import CatalogService, table_sources
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.configs import logger as core_logger
from flowfile_core.database.connection import get_db_context
from flowfile_core.hub_client import catalog as hub_catalog
from flowfile_core.hub_client import client, session
from flowfile_core.schemas import input_schema
from tests.hub_client.conftest import HUB_URL, INFO, NEVER_RUNS, FakeHubWithCatalog

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

    assert hub.catalog_calls == [("/catalog/namespaces/tree", "")]


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
    assert hub.catalog_calls == [("/catalog/namespaces/tree", "")]


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

    rows = [node.model_dump(mode="json") for node in table_sources.merged_tree(user_id, local)]

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
        assert hub.catalog_calls == [("/catalog/namespaces/tree", "")]


@pytest.mark.parametrize(
    "malformed", ["a root without an id", "a schema with a text id", "a table in a text namespace"]
)
def test_a_hub_answering_rows_core_cannot_read_leaves_its_catalogs_out_with_one_warning(
    connected, hub, user_id, warnings, malformed
):
    _seed(hub)
    sales, finance = hub.tree_answer = hub.tree()
    if malformed == "a root without an id":
        del finance["id"]
    elif malformed == "a schema with a text id":
        sales["children"][0]["id"] = "raw"
    else:
        finance["tables"][0]["namespace_id"] = "finance"
    resp = connected.get("/catalog/namespaces/tree")
    assert resp.status_code == 200
    assert resp.json() == _local_tree(user_id)
    assert len(warnings) == 1 and HUB_URL in warnings[0]


def test_a_hub_tables_detail_is_the_hubs_own_answer_marked_as_the_tree_marks_it(connected, hub):
    seeded = _seed(hub)
    [sales] = [root for root in connected.get("/catalog/namespaces/tree").json() if root["name"] == "sales"]
    in_tree = sales["children"][0]["tables"][0]
    resp = connected.get(f"/catalog/tables/{in_tree['id']}")
    assert resp.status_code == 200
    assert resp.json() == in_tree
    assert resp.json() == {
        **seeded["orders"],
        "id": -seeded["orders"]["id"],
        "namespace_id": -seeded["raw"]["id"],
        "source": "hub",
        "hub_id": seeded["orders"]["id"],
        "hub_namespace_id": seeded["raw"]["id"],
    }
    assert hub.catalog_calls[-1] == (f"/catalog/tables/{seeded['orders']['id']}", "")


def test_a_hub_table_is_renamed_favourited_and_deleted_on_the_hub(connected, hub):
    seeded = _seed(hub)
    row = -seeded["orders"]["id"]
    renamed = connected.put(f"/catalog/tables/{row}", json={"name": "orders_v2", "namespace_id": -2})
    assert renamed.status_code == 200 and renamed.json()["name"] == "orders_v2" and renamed.json()["id"] == row
    assert hub.tables[0]["name"] == "orders_v2" and hub.tables[0]["namespace_id"] == 2
    favourite = connected.post(f"/catalog/tables/{row}/favorite")
    assert favourite.status_code == 201 and favourite.json()["table_id"] == row
    deleted = connected.delete(f"/catalog/tables/{row}")
    assert deleted.status_code == 204 and deleted.content == b""
    assert seeded["orders"]["id"] not in [t["id"] for t in hub.tables]


def test_a_hub_flows_run_history_is_asked_of_the_hub_by_the_hubs_own_id(connected, hub):
    hub.runs = [{"id": 5, "registration_id": 201, "flow_name": "orders-clean", "success": True}]
    resp = connected.get("/catalog/runs", params={"registration_id": -201, "limit": 10})
    assert resp.status_code == 200
    assert resp.json() == {
        "items": [
            {"id": -5, "hub_id": 5, "source": "hub", "registration_id": -201, "flow_name": "orders-clean",
             "success": True}
        ],
        "total": 1,
        "total_success": 0,
        "total_failed": 0,
        "total_running": 1,
    }
    assert hub.catalog_calls[-1] == ("/catalog/runs", "registration_id=201&limit=10")


def test_the_hubs_refusal_of_a_catalog_route_is_answered_as_the_hub_gave_it(connected, hub, api):
    _seed(hub)
    missing = connected.get("/catalog/tables/-999")
    assert missing.status_code == 404 and missing.json() == {"detail": "Catalog table not found"}
    hub.down = True
    assert connected.get("/catalog/tables/-101").status_code == 502


def test_a_catalog_route_of_the_hub_without_a_hub_connection_answers_400(api):
    resp = api.get("/catalog/tables/-101")
    assert resp.status_code == 400 and "not connected" in resp.json()["detail"]


def test_a_hub_flows_detail_is_the_hubs_own_answer_through_cores_own_address(connected, hub):
    flow = hub.seed_flow(_seed(hub)["raw"], "orders-clean")
    resp = connected.get(f"/catalog/flows/{-flow['registration_id']}")
    assert resp.status_code == 200
    assert resp.json()["id"] == -flow["registration_id"] and resp.json()["source"] == "hub"
    assert resp.json()["name"] == "orders-clean" and resp.json()["namespace_id"] == -flow["namespace_id"]
    assert hub.catalog_calls[-1] == (f"/catalog/flows/{flow['registration_id']}", "")
    assert connected.get("/catalog/flows/-999").json() == {"detail": "Flow not found"}


def _flow_with_schedule(hub) -> tuple[int, dict]:
    """A flow on the hub and a schedule of it; the flow's row id here and the schedule as the hub keeps it."""
    flow = hub.seed_flow(_seed(hub)["raw"], "orders-clean")
    schedule = {"id": 7, "registration_id": flow["registration_id"], "schedule_type": "interval", "enabled": True}
    hub.schedules.append(schedule)
    return -flow["registration_id"], schedule


def test_a_hub_flows_schedules_are_listed_paused_resumed_and_removed_on_the_hub(connected, hub):
    row, schedule = _flow_with_schedule(hub)
    listed = connected.get("/catalog/schedules", params={"registration_id": row})
    assert listed.status_code == 200
    assert listed.json() == [
        {"id": -7, "hub_id": 7, "source": "hub", "registration_id": row, "schedule_type": "interval", "enabled": True}
    ]
    assert hub.catalog_calls[-1] == ("/catalog/schedules", f"registration_id={-row}")
    paused = connected.put("/catalog/schedules/-7", json={"enabled": False})
    assert paused.status_code == 200 and paused.json()["enabled"] is False and schedule["enabled"] is False
    resumed = connected.put("/catalog/schedules/-7", json={"enabled": True})
    assert resumed.status_code == 200 and schedule["enabled"] is True
    removed = connected.delete("/catalog/schedules/-7")
    assert removed.status_code == 204 and hub.schedules == []
    assert connected.get("/catalog/schedules/-7").status_code == 404


def test_the_list_of_every_schedule_holds_the_hubs_after_this_machines_marked_as_the_hubs(connected, hub, api):
    flow = hub.seed_flow(_seed(hub)["raw"], "orders-clean")
    schedule = hub.seed_schedule(flow, server="bmax")
    hub.seed_schedule(hub.seed_flow(hub.namespaces[1], "other"))
    listed = connected.get("/catalog/schedules")
    assert listed.status_code == 200
    theirs = [row for row in listed.json() if row["source"] == "hub"]
    assert [(row["id"], row["hub_id"], row["registration_id"]) for row in theirs] == [(-7, 7, -201), (-8, 8, -202)]
    assert theirs[0]["backend"] == "worker:bmax" and theirs[0]["interval_seconds"] == schedule["interval_seconds"]
    assert all(row["source"] == "local" and row["id"] > 0 for row in listed.json() if row not in theirs)
    assert hub.catalog_calls[-1] == ("/catalog/schedules", "")
    of_the_flow = connected.get("/catalog/schedules", params={"registration_id": -201}).json()
    assert [row["id"] for row in of_the_flow] == [-7] and of_the_flow[0]["backend"] == "worker:bmax"
    assert connected.get("/catalog/overview").json()["schedules"] == listed.json()
    paused = connected.put("/catalog/schedules/-7", json={"enabled": False})
    assert paused.status_code == 200 and hub.schedules[0]["enabled"] is False


def test_a_hub_that_does_not_answer_leaves_the_list_of_schedules_this_machines(connected, hub, api):
    hub.seed_schedule(hub.seed_flow(_seed(hub)["raw"], "orders-clean"))
    hub.down = True
    listed = connected.get("/catalog/schedules")
    assert listed.status_code == 200 and all(row["source"] == "local" for row in listed.json())


def test_the_list_of_schedules_without_a_hub_connection_is_this_machines(api, hub):
    hub.seed_schedule(hub.seed_flow(_seed(hub)["raw"], "orders-clean"))
    listed = api.get("/catalog/schedules")
    assert listed.status_code == 200 and all(row["source"] == "local" for row in listed.json())
    assert hub.catalog_calls == []


def test_run_history_and_the_runs_under_way_hold_the_hubs_runs_marked_as_the_hubs(connected, hub):
    flow = hub.seed_flow(_seed(hub)["raw"], "orders-clean")
    schedule = hub.seed_schedule(flow)
    hub.seed_run(flow, schedule_id=schedule["id"], executed_on="bmax", remote_run_id="r-1")
    hub.seed_run(flow, ended=False, schedule_id=schedule["id"], remote_run_id="r-2")
    page = connected.get("/catalog/runs", params={"limit": 500}).json()
    theirs = [run for run in page["items"] if run["source"] == "hub"]
    assert sorted((run["id"], run["registration_id"], run["schedule_id"]) for run in theirs) == [
        (-6, -201, -7),
        (-5, -201, -7),
    ]
    assert hub.catalog_calls[-1] == ("/catalog/runs", "limit=500&offset=0")
    assert page["total"] >= 2 and page["total_running"] >= 1
    active = connected.get("/catalog/active-runs").json()
    assert [(run["id"], run["schedule_id"], run["source"]) for run in active if run["id"] < 0] == [(-6, -7, "hub")]
    of_the_schedule = connected.get("/catalog/runs", params={"schedule_id": -7}).json()
    assert [run["id"] for run in of_the_schedule["items"]] == [-5, -6]
    assert hub.catalog_calls[-1] == ("/catalog/runs", "schedule_id=7")
    overview = connected.get("/catalog/overview").json()
    assert [run["id"] for run in overview["active_runs"] if run["id"] < 0] == [-6]
    assert {run["id"] for run in overview["runs"]["items"] if run["id"] < 0} == {-5, -6}


def test_a_hub_flow_is_favourited_and_unfavourited_on_the_hub(connected, hub):
    row, _ = _flow_with_schedule(hub)
    favourite = connected.post(f"/catalog/flows/{row}/favorite")
    assert favourite.status_code == 201 and favourite.json()["registration_id"] == row
    assert connected.delete(f"/catalog/flows/{row}/favorite").status_code == 204
    assert hub.calls[-1] == ("DELETE", f"/catalog/flows/{-row}/favorite")


def test_running_a_hub_flow_now_and_a_new_schedule_of_it_are_refused_by_the_hub_not_here(connected, hub):
    """The hub answers neither core route today, so the refusal a person reads is the hub's own sentence."""
    row, _ = _flow_with_schedule(hub)
    run = connected.post(f"/catalog/flows/{row}/run")
    assert run.status_code == 403 and run.json() == {"detail": NEVER_RUNS}
    assert hub.calls[-1] == ("POST", f"/catalog/flows/{-row}/run")
    made = connected.post("/catalog/schedules", json={"registration_id": row, "schedule_type": "interval"})
    assert made.status_code == 403 and made.json() == {"detail": NEVER_RUNS}
    assert hub.calls[-1] == ("POST", "/catalog/schedules")


def test_the_catalog_routes_that_name_no_hub_row_stay_local(connected, hub):
    _seed(hub)
    assert connected.get("/catalog/tables").status_code == 200
    assert connected.get("/catalog/namespaces").status_code == 200
    assert connected.get("/catalog/tables/999999").status_code == 404
    assert connected.get("/catalog/runs", params={"registration_id": 999999, "limit": 5}).status_code == 200
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
        assert reader.source == "local" and reader.hub_namespace_name is None
        input_schema.NodeCatalogReader(flow_id=1, node_id=1, catalog_table_id=4, cdc_mode="since_last_run")

    def test_a_hub_table_read_as_it_is_now_validates_and_names_its_schema(self):
        reader = input_schema.NodeCatalogReader(
            flow_id=1, node_id=1, source="hub", catalog_full_table_name="sales.raw.orders", catalog_table_name="orders"
        )
        assert (reader.source, reader.catalog_full_table_name) == ("hub", "sales.raw.orders")
        assert reader.hub_namespace_name == "sales.raw"
        assert input_schema.NodeCatalogReader(flow_id=1, node_id=1, source="hub").hub_namespace_name is None

    @pytest.mark.parametrize("ids", [{"catalog_table_id": 4}, {"catalog_namespace_id": 2}])
    def test_a_hub_table_takes_no_local_id(self, ids):
        with pytest.raises(ValidationError, match="named by its qualified name"):
            input_schema.NodeCatalogReader(
                flow_id=1, node_id=1, source="hub", catalog_full_table_name="sales.raw.orders", **ids
            )

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
            input_schema.NodeCatalogReader(
                flow_id=1, node_id=1, source="hub", catalog_full_table_name="sales.raw.orders", **extra
            )


class TestHubNamespaceWriter:
    def test_local_writes_validate_as_before(self):
        for mode in ("virtual", "overwrite"):
            assert input_schema.CatalogWriteSettings(table_name="t", write_mode=mode).source == "local"
        input_schema.CatalogWriteSettings(table_name="t", write_mode="scd2", merge_keys=["id"])
        input_schema.CatalogWriteSettings(table_name="t", write_mode="append", track_changes=True)

    @pytest.mark.parametrize("mode", ["overwrite", "append", "error"])
    def test_a_physical_write_to_a_hub_namespace_validates(self, mode):
        settings = input_schema.CatalogWriteSettings(
            table_name="t", write_mode=mode, source="hub", namespace_full_name="sales.curated"
        )
        assert (settings.source, settings.namespace_full_name, settings.namespace_id) == ("hub", "sales.curated", None)

    def test_a_hub_namespace_takes_no_local_id(self):
        with pytest.raises(ValidationError, match="named by its qualified name"):
            input_schema.CatalogWriteSettings(table_name="t", source="hub", namespace_full_name="a.b", namespace_id=2)

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
            input_schema.CatalogWriteSettings(table_name="t", source="hub", namespace_full_name="a.b", **fields)
