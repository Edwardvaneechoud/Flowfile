"""On a server that joined its hub with keys of its own, nothing a person does registers a flow in the server's own
catalog.

The organisation's catalog is the hub's there (``table_sources.local_catalog_offered`` is false): a flow opened from
the hub is a working file only, through its open, its save and its run, whose record is reported as the hub flow's; a
save or a create that names one of the server's own schemas is refused before anything is written; and a copy saved
under another name is filed nowhere by the save. The hub is ``DispatchHub`` (``test_dispatch``): a thread-served hub
that signs people in and answers a checkout. Core runs in docker mode, joined with keys under the test's ``tmp_path``.
"""

import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.catalog import CatalogService, table_sources
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.init_db import create_default_catalog_namespace
from flowfile_core.database.models import FlowRegistration, FlowRun
from flowfile_core.routes import routes
from shared.storage_config import storage
from tests import test_hub_sign_in
from tests.flowfile.conftest import CATALOG_SAMPLE_DATA, add_test_manual_input
from tests.hub import test_dispatch, test_server_run_report
from tests.test_hub_sign_in import bearer

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
hub = test_dispatch.hub
open_flow = test_server_run_report.open_flow
clean_catalog = test_server_run_report.clean_catalog
sign_in = test_server_run_report.sign_in

client = TestClient(main.app)


@pytest.fixture(autouse=True)
def general():
    """The server's own seeded ``General`` with its schemas, public as every install has it."""
    with get_db_context() as db:
        create_default_catalog_namespace(db)


@pytest.fixture
def opened():
    """Closes the flows a test opened from a file and removes their files."""
    flow_ids: list[int] = []
    paths: list[Path] = []
    yield flow_ids, paths
    for flow_id in flow_ids:
        routes.flow_file_handler.delete_flow(flow_id)
    for path in paths:
        path.unlink(missing_ok=True)


def registrations() -> list[tuple[str, str]]:
    with get_db_context() as db:
        return [(row.name, row.flow_path) for row in db.query(FlowRegistration).all()]


def own_schema_id() -> int:
    with get_db_context() as db:
        return CatalogService(SQLAlchemyCatalogRepository(db)).get_default_namespace_id()


def test_a_plain_core_offers_its_own_catalog():
    assert table_sources.local_catalog_offered() is True


def test_a_flow_opened_from_the_hub_is_registered_nowhere_here_by_its_open_its_save_or_its_run(
    hub, username, opened
):
    flow_ids, paths = opened
    person, access_token = sign_in(hub, username)
    assert table_sources.local_catalog_offered() is False
    flow_uuid = str(uuid.uuid4())
    hub.seed(flow_uuid, "demo-flow", test_dispatch.definition())

    answered = client.post("/hub_client/open", json={"flow_uuid": flow_uuid}, headers=bearer(access_token))

    assert answered.status_code == 200, answered.text
    flow_path = Path(answered.json()["flow_path"])
    paths.append(flow_path)
    assert flow_path.parent == storage.flows_directory / "hub" / str(person) and flow_path.exists()
    assert registrations() == []

    flow_id = routes.flow_file_handler.import_flow(flow_path, user_id=person)
    flow_ids.append(flow_id)
    flow = routes.flow_file_handler.get_flow(flow_id)
    flow.flow_settings.execution_location = "local"
    assert flow.flow_settings.hub_origin.flow_uuid == flow_uuid

    saved = client.post("/save_flow", params={"flow_id": flow_id}, headers=bearer(access_token))

    assert saved.status_code == 200, saved.text
    assert registrations() == []

    ran = client.post("/flow/run/", params={"flow_id": flow_id}, headers=bearer(access_token))

    assert ran.status_code == 200, ran.text
    assert registrations() == []
    with get_db_context() as db:
        [run] = db.query(FlowRun).filter_by(flow_path=str(flow_path)).all()
        assert (run.success, run.registration_id) == (True, None)
    [posted] = hub.catalog.reports
    assert (posted["flow_uuid"], posted["success"]) == (flow_uuid, True)


def test_a_save_or_a_create_into_one_of_the_servers_own_schemas_is_refused_before_anything_is_written(
    hub, username, open_flow
):
    person, access_token = sign_in(hub, username)
    flow = open_flow(person, "made_on_the_server")
    add_test_manual_input(flow, CATALOG_SAMPLE_DATA, node_id=1)
    schema = own_schema_id()
    elsewhere = storage.flows_directory / f"filed_{uuid.uuid4().hex[:8]}.yaml"
    created = storage.flows_directory / f"created_{uuid.uuid4().hex[:8]}.yaml"

    into_catalog = client.post(
        "/save_flow_to_catalog",
        params={"flow_id": flow.flow_id, "flow_name": "filed", "namespace_id": schema},
        headers=bearer(access_token),
    )
    save_as = client.post(
        "/save_flow",
        params={"flow_id": flow.flow_id, "flow_path": str(elsewhere), "namespace_id": schema},
        headers=bearer(access_token),
    )
    create = client.post(
        "/editor/create_flow/",
        params={"flow_path": str(created), "namespace_id": schema},
        headers=bearer(access_token),
    )

    for refused in (into_catalog, save_as, create):
        assert (refused.status_code, refused.json()["detail"]) == (409, routes.LOCAL_CATALOG_NOT_OFFERED), refused.text
    assert not elsewhere.exists() and not created.exists()
    assert registrations() == []


def test_a_copy_saved_under_another_name_is_a_file_the_save_files_nowhere(hub, username, open_flow, opened):
    flow_ids, paths = opened
    person, access_token = sign_in(hub, username)
    flow = open_flow(person, "made_on_the_server")
    add_test_manual_input(flow, CATALOG_SAMPLE_DATA, node_id=1)
    copy = storage.flows_directory / f"copy_{uuid.uuid4().hex[:8]}.yaml"
    paths.append(copy)

    saved = client.post(
        "/save_flow", params={"flow_id": flow.flow_id, "flow_path": str(copy)}, headers=bearer(access_token)
    )

    assert saved.status_code == 200, saved.text
    flow_ids.append(saved.json())
    assert copy.exists()
    assert registrations() == []
