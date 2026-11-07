"""On a server joined to its hub with keys of its own, a flow reaches the hub only as the person whose session holds it.

A node's ``user_id`` is whose sign-in the hub is asked through for the node's tables and secrets, so no request body, AI
diff, undo or flow file may name another person there. Two people sign in through the hub, whose catalog (``hub``,
``OrganisationHub`` of ``test_server_run_report``) keeps the bearer of every catalog read, vend and run report; each
case has the second person try to run under the first's sign-in, or pins a path that was never open. A run ends at its
vend (``hub.catalog.vend_failure``) unless it needs storage: whose token asked is what these tests read. A server that
never joined keeps taking the id it is given.
"""

import json
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog.service import CatalogService
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import ConfiguredBackend
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.manage.io_flowfile import open_flow as open_flow_file
from flowfile_core.routes import routes
from flowfile_core.schemas import input_schema
from shared.storage_config import storage
from tests import test_hub_sign_in
from tests.conftest import closed_url
from tests.flowfile.conftest import CATALOG_SAMPLE_DATA, add_test_manual_input
from tests.hub.test_server_run_report import (  # noqa: F401
    OrganisationHub,
    clean_catalog,
    hub,
    keep_a_sign_in,
    minio_catalog,
    open_flow,
)
from tests.test_catalog_cloud_virtual import _BUCKET, requires_minio
from tests.test_hub_sign_in import NODE, bearer

node_file = test_hub_sign_in.node_file
local_user = test_hub_sign_in.local_user
client = TestClient(main.app)

TEAM = "worker:team"
REST_SECRET = "api-token"


@dataclass(frozen=True)
class Person:
    """Someone signed in through the hub: their local account, core's headers for them, the hub's token for them."""

    id: int
    headers: dict
    hub_token: str


def sign_in(fake: OrganisationHub, uid: int, name: str) -> Person:
    before = set(fake.issued)
    signed_in = client.post("/auth/hub/signin", json={"token": fake.sign_in_token(NODE, uid=uid, username=name)})
    assert signed_in.status_code == 200, signed_in.text
    headers = bearer(signed_in.json()["access_token"])
    me = client.get("/auth/users/me", headers=headers)
    assert me.status_code == 200, me.text
    [hub_token] = set(fake.issued) - before
    return Person(me.json()["id"], headers, hub_token)


@pytest.fixture
def ada(hub) -> Person:  # noqa: F811
    return sign_in(hub, 7, f"ada-{uuid.uuid4().hex[:8]}")


@pytest.fixture
def dee(hub) -> Person:  # noqa: F811
    return sign_in(hub, 8, f"dee-{uuid.uuid4().hex[:8]}")


@pytest.fixture
def orders(hub) -> tuple[dict, dict]:  # noqa: F811
    """The hub's schema ``sales`` and its table ``orders``, on storage these runs never reach."""
    name = f"hub_{uuid.uuid4().hex[:8]}"
    catalog = hub.catalog.seed_catalog(name, storage_uri=f"s3://{_BUCKET}/{name}")
    sales = hub.catalog.seed_schema(catalog, "sales")
    columns = [{"name": "id", "dtype": "Int64"}]
    table = hub.catalog.seed_table(
        sales, "orders", file_path=f"s3://{_BUCKET}/{name}/orders", is_remote_storage=True, schema_columns=columns
    )
    hub.catalog.grants.update({catalog["id"]: "manage", sales["id"]: "manage"})
    return sales, table


def reader_settings(flow, node_id: int, sales: dict, table: dict, **more) -> dict:
    return {
        "flow_id": flow.flow_id,
        "node_id": node_id,
        "hub_table_id": table["id"],
        "hub_namespace_id": sales["id"],
        "catalog_table_name": table["name"],
        **more,
    }


def accept_a_diff(person: Person, flow, node_type: str, settings: dict) -> None:
    """``settings`` become a node of ``flow`` the way the AI's diff does, staged and accepted over its routes."""
    staged = client.post(
        "/ai/diff/stage",
        headers=person.headers,
        json={
            "session_id": "session",
            "flow_id": flow.flow_id,
            "staged_results": [
                {
                    "tool_name": f"flowfile.graph.add_{node_type}",
                    "staged_node_payload": {"node_type": node_type, "settings": settings},
                }
            ],
        },
    )
    assert staged.status_code == 200, staged.text
    accepted = client.post(
        f"/ai/diff/{staged.json()['diff_id']}/accept", headers=person.headers, json={"flow_id": flow.flow_id}
    )
    assert accepted.status_code == 200, accepted.text


def add_set_up_node(person: Person, flow, node_type: str, settings: dict) -> None:
    """A node placed and saved as the editor does it."""
    node_id = settings["node_id"]
    params = {"flow_id": flow.flow_id, "node_id": node_id, "node_type": node_type}
    placed = client.post("/editor/add_node/", params=params, headers=person.headers)
    assert placed.status_code == 200, placed.text
    saved = client.post("/update_settings/", params={"node_type": node_type}, json=settings, headers=person.headers)
    assert saved.status_code == 200, saved.text


def run(person: Person, flow) -> None:
    ran = client.post("/flow/run/", params={"flow_id": flow.flow_id}, headers=person.headers)
    assert ran.status_code == 200, ran.text


def tokens_seen(fake: OrganisationHub) -> set[str]:
    return {token for _, _, token in list(fake.received)}


def test_a_diff_naming_another_person_reaches_the_hub_only_as_the_person_who_runs_it(
    hub, ada, dee, orders, open_flow  # noqa: F811
):
    sales, table = orders
    flow = open_flow(dee.id, "named_ada_in_a_diff")

    accept_a_diff(dee, flow, "catalog_reader", reader_settings(flow, 1, sales, table, user_id=ada.id))
    hub.catalog.vend_failure = 403
    run(dee, flow)

    assert flow.get_node(1).setting_input.user_id == dee.id
    assert hub.sent_with("POST", "/hub/vend") == [dee.hub_token]
    assert tokens_seen(hub) == {dee.hub_token}


def test_a_diff_naming_nobody_asks_as_the_person_who_runs_it(hub, dee, orders, open_flow):  # noqa: F811
    sales, table = orders
    flow = open_flow(dee.id, "named_nobody_in_a_diff")

    accept_a_diff(dee, flow, "catalog_reader", reader_settings(flow, 1, sales, table))
    hub.catalog.vend_failure = 403
    run(dee, flow)

    assert flow.get_node(1).setting_input.user_id == dee.id
    assert hub.sent_with("POST", "/hub/vend") == [dee.hub_token]


def post_manual_input(person: Person, flow, node_id: int, user_id: int) -> None:
    posted = client.post(
        "/transform/manual_input",
        headers=person.headers,
        json={
            "flow_id": flow.flow_id,
            "node_id": node_id,
            "user_id": user_id,
            "raw_data_format": {"columns": [{"name": "id", "data_type": "Integer"}], "data": [[1, 2]]},
        },
    )
    assert posted.status_code == 200, posted.text


def post_file_input(person: Person, flow, node_id: int, user_id: int) -> None:
    source = storage.uploads_directory / f"{uuid.uuid4().hex[:8]}.csv"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("id\n1\n")
    try:
        posted = client.post(
            "/transform/add_input/",
            headers=person.headers,
            json={"flow_id": flow.flow_id, "node_id": node_id, "user_id": user_id, "file_ref": str(source)},
        )
        assert posted.status_code == 200, posted.text
    finally:
        source.unlink(missing_ok=True)


@pytest.mark.parametrize("post", [post_manual_input, post_file_input], ids=["manual-input", "file-input"])
def test_a_source_posted_with_another_persons_id_lends_it_to_no_step_an_undo_brings_back(
    hub, ada, dee, orders, open_flow, post  # noqa: F811
):
    sales, table = orders
    flow = open_flow(dee.id, "named_ada_on_a_source")
    post(dee, flow, 1, ada.id)
    add_set_up_node(dee, flow, "catalog_reader", reader_settings(flow, 2, sales, table))

    deleted = client.post("/editor/delete_node/", params={"flow_id": flow.flow_id, "node_id": 2}, headers=dee.headers)
    assert deleted.status_code == 200, deleted.text
    undone = client.post("/editor/undo/", params={"flow_id": flow.flow_id}, headers=dee.headers)
    assert undone.status_code == 200, undone.text
    hub.catalog.vend_failure = 403
    run(dee, flow)

    assert {node.setting_input.user_id for node in flow.nodes} == {dee.id}
    assert hub.sent_with("POST", "/hub/vend") == [dee.hub_token]
    assert ada.hub_token not in tokens_seen(hub)


@pytest.fixture
def team_server(dee):
    """The hub's server ``team`` as a hub-filled row at an address nothing listens on; these runs end before they
    call it. Removed afterwards."""
    registry.reset()
    handshake.reset_handshake()
    with get_db_context() as db:
        db.add(ConfiguredBackend(id=TEAM, url=closed_url(), hub_node_id=5, hub_user_id=dee.id))
        db.commit()
    yield
    with get_db_context() as db:
        db.query(ConfiguredBackend).filter(ConfiguredBackend.id == TEAM).delete()
        db.commit()
    registry.reset()
    handshake.reset_handshake()


def test_a_steps_secrets_are_asked_for_as_the_person_who_runs_it(hub, ada, dee, team_server, open_flow):  # noqa: F811
    flow = open_flow(dee.id, "secret_on_the_hubs_server")
    flow.flow_settings.execution_location = TEAM
    auth = {"auth_type": "bearer", "secret_name": REST_SECRET}
    settings = {"url": "http://rest.example.test/rows", "auth": auth}
    accept_a_diff(
        dee,
        flow,
        "rest_api_reader",
        {"flow_id": flow.flow_id, "node_id": 1, "user_id": ada.id, "rest_api_settings": settings},
    )
    hub.catalog.vend_failure = 403
    run(dee, flow)

    assert any(call.get("secrets") == [REST_SECRET] for call in hub.catalog.vend_calls)
    assert set(hub.sent_with("POST", "/hub/vend")) == {dee.hub_token}
    assert ada.hub_token not in tokens_seen(hub)


def flow_file_naming(person: Person, flow, node_id: int) -> Path:
    """``flow`` saved beside an upload as a JSON flow file whose node ``node_id`` names ``person``, as someone could
    write one by hand and upload it to a flow folder."""
    path = storage.uploads_directory / f"named_{uuid.uuid4().hex[:8]}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    flow.save_flow(str(path))
    data = json.loads(path.read_text())
    [node] = [node for node in data["nodes"] if node["id"] == node_id]
    node["setting_input"]["user_id"] = person.id
    path.write_text(json.dumps(data))
    return path


def test_a_flow_file_naming_a_person_asks_the_hub_as_nobody_when_it_opens_for_nobody(
    hub, ada, dee, orders, open_flow  # noqa: F811
):
    sales, table = orders
    flow = open_flow(dee.id, "to_be_written_by_hand")
    add_set_up_node(dee, flow, "catalog_reader", reader_settings(flow, 1, sales, table))
    path = flow_file_naming(ada, flow, 1)
    hub.received.clear()
    try:
        CatalogService._compute_laziness_blockers(str(path))
        opened = open_flow_file(path)
    finally:
        path.unlink(missing_ok=True)

    assert opened.get_node(1).setting_input.user_id is None
    assert ada.hub_token not in tokens_seen(hub)


def test_a_flow_file_naming_a_person_opens_as_the_person_who_opens_it(hub, ada, dee, orders, open_flow):  # noqa: F811
    sales, table = orders
    flow = open_flow(dee.id, "to_be_opened_by_dee")
    add_set_up_node(dee, flow, "catalog_reader", reader_settings(flow, 1, sales, table))
    path = flow_file_naming(ada, flow, 1)
    hub.received.clear()
    opened_id = routes.flow_file_handler.import_flow(path, user_id=dee.id)
    try:
        opened = routes.flow_file_handler.get_flow(opened_id)
        assert opened.get_node(1).setting_input.user_id == dee.id
        hub.catalog.vend_failure = 403
        run(dee, opened)
        assert hub.sent_with("POST", "/hub/vend") == [dee.hub_token]
        assert ada.hub_token not in tokens_seen(hub)
    finally:
        routes.flow_file_handler.delete_flow(opened_id)
        path.unlink(missing_ok=True)


def test_two_people_opening_one_flow_file_each_run_it_as_themselves(hub, ada, dee, orders, open_flow):  # noqa: F811
    sales, table = orders
    flow = open_flow(ada.id, "opened_by_both")
    add_set_up_node(ada, flow, "catalog_reader", reader_settings(flow, 1, sales, table))
    flow.save_flow(flow.flow_settings.path)
    opened_id = routes.flow_file_handler.import_flow(Path(flow.flow_settings.path), user_id=dee.id)
    try:
        opened = routes.flow_file_handler.get_flow(opened_id)
        assert opened_id != flow.flow_id
        hub.catalog.vend_failure = 403
        run(ada, flow)
        run(dee, opened)
        assert hub.sent_with("POST", "/hub/vend") == [ada.hub_token, dee.hub_token]
    finally:
        routes.flow_file_handler.delete_flow(opened_id)


def test_saved_settings_naming_another_person_are_stamped_with_the_person_who_saved_them(
    hub, ada, dee, orders, open_flow  # noqa: F811
):
    sales, table = orders
    flow = open_flow(dee.id, "named_ada_in_a_save")

    add_set_up_node(dee, flow, "catalog_reader", reader_settings(flow, 1, sales, table, user_id=ada.id))
    hub.catalog.vend_failure = 403
    run(dee, flow)

    assert flow.get_node(1).setting_input.user_id == dee.id
    assert hub.sent_with("POST", "/hub/vend") == [dee.hub_token]
    assert tokens_seen(hub) == {dee.hub_token}


@requires_minio
@pytest.mark.usefixtures("protocol_on")
def test_a_writer_a_diff_left_without_a_person_never_writes_through_the_first_accounts_session(
    hub, dee, minio_catalog, open_flow  # noqa: F811
):
    _, sales, orders_table = minio_catalog
    first_account = keep_a_sign_in(hub, 1, 99, "first-account")
    flow = open_flow(dee.id, "writer_named_nobody")
    add_set_up_node(dee, flow, "catalog_reader", reader_settings(flow, 1, sales, orders_table))
    accept_a_diff(
        dee,
        flow,
        "catalog_writer",
        {
            "flow_id": flow.flow_id,
            "node_id": 2,
            "depending_on_id": 1,
            "catalog_write_settings": {
                "table_name": "orders_copy",
                "hub_namespace_id": sales["id"],
                "write_mode": "overwrite",
            },
        },
    )
    connected = client.post(
        "/editor/connect_node/",
        params={"flow_id": flow.flow_id},
        headers=dee.headers,
        json=input_schema.NodeConnection.create_from_simple_input(1, 2).model_dump(),
    )
    assert connected.status_code == 200, connected.text
    hub.received.clear()

    run(dee, flow)

    run_info = flow.get_run_info()
    assert run_info.success is True, [(row.node_id, row.error) for row in run_info.node_step_result]
    assert [row["name"] for row in hub.catalog.registered] == ["orders_copy"]
    assert first_account not in tokens_seen(hub)
    assert tokens_seen(hub) == {dee.hub_token}


def test_a_server_that_never_joined_takes_the_id_it_is_given(node_file, local_user):
    other = local_user(f"other-{uuid.uuid4().hex[:8]}")
    owner = local_user(f"owner-{uuid.uuid4().hex[:8]}")
    storage.flows_directory.mkdir(parents=True, exist_ok=True)
    path = storage.flows_directory / f"never_joined_{uuid.uuid4().hex[:8]}.yaml"
    flow_id = routes.flow_file_handler.add_flow(name="never_joined", flow_path=str(path), user_id=owner)
    try:
        flow = routes.flow_file_handler.get_flow(flow_id)
        assert flow._owner_user_id is None
        add_test_manual_input(flow, CATALOG_SAMPLE_DATA, node_id=1)
        flow.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=flow_id,
                node_id=1,
                user_id=other,
                raw_data_format=flow.get_node(1).setting_input.raw_data_format,
            )
        )
        assert flow.get_node(1).setting_input.user_id == other
        assert flow._owner_user_id == other
    finally:
        routes.flow_file_handler.delete_flow(flow_id)
        path.unlink(missing_ok=True)
