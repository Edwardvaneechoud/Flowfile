"""On a server that joined its hub with keys of its own, nothing a person does registers a flow in the server's own
catalog, and a flow that came from the hub is saved to the hub.

The organisation's catalog is the hub's there (``table_sources.local_catalog_offered`` is false): a flow opened from
the hub is a working file only, through its open, its save and its run, whose record is reported as the hub flow's; a
save or a create that names one of the server's own schemas is refused before anything is written; and a copy saved
under another name is filed nowhere by the save. A save of a flow from the hub sends its content to the hub
(``table_sources.save_flow``), and what the hub says of it is the flow's ``save_note``; the next open reads what the hub
answers for that person. The hub is ``ContentHub``: ``DispatchHub`` (``test_dispatch``, a thread-served hub that signs
people in and answers a checkout) that also keeps each person's saved content of a flow, or, as an older hub, has no
such routes. Core runs in docker mode, joined with keys under the test's ``tmp_path``.
"""

import json
import re

import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cryptography.fernet import Fernet

from flowfile_core import main
from flowfile_core.catalog import CatalogService, table_sources
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database import models as db_models
from flowfile_core.database.init_db import create_default_catalog_namespace
from flowfile_core.database.models import FlowRegistration, FlowRun
from flowfile_core.routes import routes
from shared.storage_config import storage
from shared.tests.hub_jwks import HubSigner
from tests import test_hub_sign_in
from tests.flowfile.conftest import CATALOG_SAMPLE_DATA, add_test_manual_input
from tests.hub import test_dispatch, test_server_run_report
from tests.test_hub_sign_in import bearer, join

node_file = test_hub_sign_in.node_file
username = test_hub_sign_in.username
open_flow = test_server_run_report.open_flow
clean_catalog = test_server_run_report.clean_catalog
sign_in = test_server_run_report.sign_in

client = TestClient(main.app)
CONTENT = re.compile(r"^/hub/flows/([^/?]+)/(edit|draft)(\?.*)?$")
KEPT = "Kept as yours on the hub, not published yet."
YOURS = "This is what you saved, not published yet."


class ContentHub(test_dispatch.DispatchHub):
    """``DispatchHub`` that keeps what each person saved of a flow and answers it when they open the flow again.

    ``kept`` maps ``(flow uuid, uid)`` to the body of that person's last save; ``refuse_save`` set makes every save a
    403 with that sentence; ``older`` set answers both routes as a hub without them does.
    """

    def __init__(self, signer: HubSigner):
        super().__init__(signer)
        self.kept: dict[tuple[str, int], dict] = {}
        self.refuse_save: str | None = None
        self.older = False
        hub = self

        class Handler(self._server.RequestHandlerClass):
            def do_GET(self):
                match = CONTENT.match(self.path)
                if match is None or match.group(2) != "edit":
                    super().do_GET()
                    return
                self._answer(*hub.for_editing(match.group(1), self.path, self._uid()))

            def do_PUT(self):
                match = CONTENT.match(self.path)
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
                if match is None or hub.older:
                    self._answer(404, {"detail": "Not Found"})
                elif hub.refuse_save:
                    self._answer(403, {"detail": hub.refuse_save})
                else:
                    hub.kept[(match.group(1), self._uid())] = body
                    self._answer(200, {"flow_uuid": match.group(1), "base_version": body["base_version"], "note": KEPT})

            def _uid(self):
                _, _, token = (self.headers.get("Authorization") or "").partition(" ")
                return hub.issued.get(token)

        self._server.RequestHandlerClass = Handler

    def for_editing(self, flow_uuid: str, path: str, uid) -> tuple[int, dict]:
        if self.older:
            return 404, {"detail": "Not Found"}
        flow, kept = self.flows.get(flow_uuid), self.kept.get((flow_uuid, uid))
        if flow is None:
            return 404, {"detail": "flow not registered on this hub"}
        if kept is None:
            return 200, {**flow, "registration_id": 7, "namespace_id": 2, "draft": False, "note": None}
        answer = {**flow, "version": kept["base_version"], "definition": kept["definition"], "draft": True}
        return 200, {**answer, "registration_id": 7, "namespace_id": 2, "note": YOURS}


@pytest.fixture
def hub(node_file, monkeypatch):
    """A ``ContentHub`` this server joined with its own keys, as ``test_dispatch.hub`` makes its hub."""
    monkeypatch.setenv("FLOWFILE_MASTER_KEY", Fernet.generate_key().decode())
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", "local")
    signer = HubSigner()
    fake = ContentHub(signer)
    join(fake)
    try:
        yield fake
    finally:
        fake.close()
        signer.close()
        with get_db_context() as db:
            mappings = db.query(db_models.IssuerAccount).filter(db_models.IssuerAccount.issuer == fake.issuer)
            user_ids = [row.user_id for row in mappings]
            mappings.delete(synchronize_session=False)
            db.query(db_models.User).filter(db_models.User.id.in_(user_ids)).delete(synchronize_session=False)
            db.commit()


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
    [(kept_for, kept)] = hub.kept.items()
    assert kept_for == (flow_uuid, test_server_run_report.UID) and kept["base_version"] == test_dispatch.VERSION
    assert [node["id"] for node in kept["definition"]["nodes"]] == [1]
    assert "hub_origin" not in kept["definition"]["flowfile_settings"] or not kept["definition"]["flowfile_settings"][
        "hub_origin"
    ]
    settings = client.get("/flow_settings", params={"flow_id": flow_id}, headers=bearer(access_token))
    assert settings.json()["save_note"] == KEPT

    again = client.post("/hub_client/open", json={"flow_uuid": flow_uuid}, headers=bearer(access_token))

    assert (again.status_code, again.json()) == (200, {"flow_path": str(flow_path), "note": YOURS}), again.text

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


def open_from_hub(hub, access_token: str, person: int, opened) -> tuple[int, str]:
    """A flow seeded on the hub, opened from it and loaded in the person's session: its flow id and uuid."""
    flow_ids, paths = opened
    flow_uuid = str(uuid.uuid4())
    hub.seed(flow_uuid, "demo-flow", test_dispatch.definition())
    answered = client.post("/hub_client/open", json={"flow_uuid": flow_uuid}, headers=bearer(access_token))
    assert answered.status_code == 200, answered.text
    paths.append(Path(answered.json()["flow_path"]))
    flow_id = routes.flow_file_handler.import_flow(paths[-1], user_id=person)
    flow_ids.append(flow_id)
    return flow_id, flow_uuid


def test_a_save_the_hub_refuses_says_so_in_the_hubs_sentence_with_the_working_file_written(hub, username, opened):
    person, access_token = sign_in(hub, username)
    flow_id, _ = open_from_hub(hub, access_token, person, opened)
    hub.refuse_save = "You can read this flow but not change it."

    saved = client.post("/save_flow", params={"flow_id": flow_id}, headers=bearer(access_token))

    assert saved.status_code == 409, saved.text
    assert saved.json()["detail"] == f"{routes.NOT_SAVED_TO_ITS_CATALOG} {hub.refuse_save}"
    assert hub.kept == {} and registrations() == []


def test_a_flow_from_an_older_hub_and_a_flow_made_here_are_saved_as_files_with_nothing_to_say(
    hub, username, opened, open_flow
):
    person, access_token = sign_in(hub, username)
    hub.older = True
    flow_id, _ = open_from_hub(hub, access_token, person, opened)
    made_here = open_flow(person, "made_on_the_server")

    for saved_id in (flow_id, made_here.flow_id):
        saved = client.post("/save_flow", params={"flow_id": saved_id}, headers=bearer(access_token))
        assert (saved.status_code, saved.json()) == (200, saved_id), saved.text
        settings = client.get("/flow_settings", params={"flow_id": saved_id}, headers=bearer(access_token))
        assert settings.json()["save_note"] is None
    assert hub.kept == {}
