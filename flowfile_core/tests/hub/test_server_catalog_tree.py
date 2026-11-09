"""On a server that joined its hub with keys of its own, the catalog tree offers only the hub's catalogs.

The server's own catalogs stay in ``GET /catalog/namespaces/tree`` for the flow pickers, but every row of them is marked
not ``offered``, whoever asks (a person without a hub session too), so the Catalog panel and the catalog reader's and
writer's pickers leave them out. The hub's catalogs come after them by place, and one the server cannot use carries
the sentence a run over it is refused with. The hub is ``OrganisationHub`` (``test_server_run_report``): a thread-served
hub that signs people in and answers core's catalog reads and the vend. Core runs in docker mode, joined with keys of
its own under the test's ``tmp_path``.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.init_db import create_default_catalog_namespace
from flowfile_core.flowfile.flow_graph.catalog_write import NO_HUB_CATALOG, refuse_without_hub_catalog
from flowfile_core.hub_client import catalog as hub_catalog
from flowfile_core.schemas import input_schema
from tests import test_hub_sign_in
from tests.flowfile.conftest import CATALOG_SAMPLE_DATA, add_test_manual_input
from tests.flowfile.execution.graph_helpers import add_promise, connect
from tests.hub import test_server_run_report
from tests.test_hub_sign_in import bearer

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
local_user = test_hub_sign_in.local_user
hub = test_server_run_report.hub
open_flow = test_server_run_report.open_flow
clean_catalog = test_server_run_report.clean_catalog
sign_in = test_server_run_report.sign_in

client = TestClient(main.app)
ON_ITS_OWN_DISK = (
    "The hub keeps the tables of {} on its own disk; this Flowfile cannot read them. "
    "Pick a catalog on the organisation's storage, or ask the hub's admin to put this one there."
)


@pytest.fixture(autouse=True)
def general():
    """The server's own seeded ``General`` with its schemas, public as every install has it."""
    with get_db_context() as db:
        create_default_catalog_namespace(db)


@pytest.fixture
def organisation(hub) -> dict:
    """The hub's ``main`` on S3 with ``main.default`` holding ``orders``, and ``scratch.notes`` on the hub's disk."""
    main_catalog = hub.catalog.seed_catalog("main", storage_uri="s3://acme/main")
    default = hub.catalog.seed_schema(main_catalog, "default")
    orders = hub.catalog.seed_table(default, "orders", file_path="s3://acme/main/default/orders")
    scratch = hub.catalog.seed_catalog("scratch")
    notes = hub.catalog.seed_schema(scratch, "notes")
    return {"main": main_catalog, "default": default, "orders": orders, "scratch": scratch, "notes": notes}


def namespace_rows(tree: list[dict]) -> list[dict]:
    return [row for root in tree for row in (root, *root["children"])]


def test_a_person_on_a_joined_server_is_offered_only_the_hubs_catalogs(hub, organisation, username):
    _, access_token = sign_in(hub, username)

    resp = client.get("/catalog/namespaces/tree", headers=bearer(access_token))

    assert resp.status_code == 200, resp.text
    tree = resp.json()
    own = [root for root in tree if root["source"] == "local"]
    assert "General" in [root["name"] for root in own], "the server's own catalogs stay in the listing"
    assert {(row["offered"], row["place"]) for row in namespace_rows(own)} == {(False, "On this server")}

    hubs = [root for root in tree if root["source"] == "hub"]
    assert tree == own + hubs
    assert [root["name"] for root in hubs] == ["main", "scratch"]
    assert {(row["offered"], row["place"]) for row in namespace_rows(hubs)} == {(True, hub.url)}
    main_row, scratch_row = hubs
    [default_row] = main_row["children"]
    [notes_row] = scratch_row["children"]
    assert (main_row["unavailable_reason"], default_row["unavailable_reason"]) == (None, None)
    assert [table["hub_id"] for table in default_row["tables"]] == [organisation["orders"]["id"]]
    assert scratch_row["unavailable_reason"] == ON_ITS_OWN_DISK.format("scratch")
    assert notes_row["unavailable_reason"] == ON_ITS_OWN_DISK.format("scratch.notes")

    overview = client.get("/catalog/overview", headers=bearer(access_token))
    assert overview.status_code == 200, overview.text
    assert overview.json()["tree"] == tree


def test_an_account_without_a_hub_session_is_offered_none_of_the_servers_own_catalogs(
    hub, organisation, local_user, username
):
    account = local_user(username)
    with get_db_context() as db:
        own = CatalogService(SQLAlchemyCatalogRepository(db)).get_namespace_tree(user_id=account)

    tree = [node.model_dump(mode="json") for node in hub_catalog.merged(account, own)]

    assert [root["name"] for root in tree] == [node.name for node in own] and tree
    assert {row["offered"] for row in namespace_rows(tree)} == {False}
    assert hub.received == []


def test_a_run_writing_to_a_hub_catalog_on_its_own_disk_is_refused_with_the_trees_reason(
    hub, organisation, username, open_flow
):
    person, access_token = sign_in(hub, username)
    tree = client.get("/catalog/namespaces/tree", headers=bearer(access_token)).json()
    [notes_row] = [row for row in namespace_rows(tree) if row.get("hub_id") == organisation["notes"]["id"]]
    flow = open_flow(person, "into_the_hubs_disk")
    add_test_manual_input(flow, CATALOG_SAMPLE_DATA, node_id=1)
    add_promise(flow, 2, "catalog_writer")
    flow.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=person,
            catalog_write_settings=input_schema.CatalogWriteSettings(
                table_name="memo", source="hub", namespace_full_name="scratch.notes", write_mode="overwrite"
            ),
        )
    )
    connect(flow, 1, 2)

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))

    assert ran.status_code == 200, ran.text
    run_info = flow.get_run_info()
    assert run_info.success is False
    assert {result.node_id: result.error for result in run_info.node_step_result} == {
        2: notes_row["unavailable_reason"]
    }
    assert hub.catalog.vend_calls == []


OWNER = {"is_owner": True, "access_level": "owner", "shared_by": None}
USE = {"is_owner": False, "access_level": "use", "shared_by": "admin"}


def test_the_hubs_catalogs_carry_the_access_the_hub_sent_and_nothing_stamped_here(hub, username):
    """The hub says ``owner`` on a row the person owns; a row it sent no access for stays without, owned or not."""
    main_catalog = hub.catalog.seed_catalog(
        "main", storage_uri="s3://acme/main", owner_id=test_server_run_report.UID, access=OWNER
    )
    hub.catalog.seed_schema(main_catalog, "default", owner_id=test_server_run_report.UID, access=OWNER)
    hub.catalog.seed_schema(main_catalog, "shared", owner_id=test_server_run_report.UID, access=USE)
    hub.catalog.seed_schema(main_catalog, "unsaid", owner_id=test_server_run_report.UID)
    team = hub.catalog.seed_catalog("team", storage_uri="s3://acme/team")
    hub.catalog.seed_schema(team, "raw")
    _, access_token = sign_in(hub, username)

    tree = client.get("/catalog/namespaces/tree", headers=bearer(access_token)).json()

    access = {
        (root["name"], row["name"]): row["access"]
        for root in tree
        if root["source"] == "hub"
        for row in (root, *root["children"])
    }
    assert access == {
        ("main", "main"): OWNER,
        ("main", "default"): OWNER,
        ("main", "shared"): USE,
        ("main", "unsaid"): None,
        ("team", "team"): None,
        ("team", "raw"): None,
    }


def test_a_writer_naming_no_catalog_on_a_joined_server_is_refused_and_writes_nothing_here(
    hub, organisation, username, open_flow
):
    person, access_token = sign_in(hub, username)
    flow = open_flow(person, "into_no_catalog")
    add_test_manual_input(flow, CATALOG_SAMPLE_DATA, node_id=1)
    add_promise(flow, 2, "catalog_writer")
    flow.add_catalog_writer(
        input_schema.NodeCatalogWriter(
            flow_id=flow.flow_id,
            node_id=2,
            depending_on_id=1,
            user_id=person,
            catalog_write_settings=input_schema.CatalogWriteSettings(table_name="memo", write_mode="overwrite"),
        )
    )
    connect(flow, 1, 2)

    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=bearer(access_token))

    assert ran.status_code == 200, ran.text
    run_info = flow.get_run_info()
    assert run_info.success is False
    errors = {result.node_id: result.error for result in run_info.node_step_result}
    assert errors[2] == NO_HUB_CATALOG
    with get_db_context() as db:
        assert db.query(db_models.CatalogTable).filter(db_models.CatalogTable.name == "memo").count() == 0
    assert hub.catalog.vend_calls == []


def test_off_a_joined_server_a_writer_naming_no_catalog_is_not_refused(node_file):
    assert refuse_without_hub_catalog(input_schema.CatalogWriteSettings(table_name="memo")) is None
