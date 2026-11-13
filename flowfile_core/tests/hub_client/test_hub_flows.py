"""Publish to the hub and open from the hub, against a fake hub that keeps flows and linear versions.

The fake copies the hub's publish rules: a first publish registers the flow (name from the body or the document,
409 on a name taken in the namespace), a later one needs the base to be the newest version, else 409 with both
versions in its detail. Checkout answers the stored document as it was published.
"""

import json
import re
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.auth.jwt import invalidate_user_cache
from flowfile_core.auth.password import get_password_hash
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.catalog_helpers import auto_register_flow, register_flow_in_namespace
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.utils import create_unique_id
from flowfile_core.hub_client import client
from flowfile_core.schemas import input_schema, schemas
from shared.storage_config import storage
from tests.hub_client.conftest import HUB_URL, FakeHub

CONNECT = {"hub_url": HUB_URL, "username": "ana", "password": "s3cret", "device_name": "ana-laptop"}
NAMESPACES = [
    {"id": 1, "name": "General", "parent_id": None, "level": 0, "description": None, "owner_id": 1},
    {"id": 2, "name": "sales", "parent_id": 1, "level": 1, "description": "The sales team", "owner_id": 1},
]
_FLOW_PATH = re.compile(r"^/hub/flows/([^/]+)/(versions|checkout|publish)$")


class FakeHubWithFlows(FakeHub):
    """The connection fake plus ``/catalog/namespaces`` and ``/hub/flows``."""

    def __init__(self):
        super().__init__()
        self.flows: dict[str, dict] = {}
        self.versions: dict[str, list[dict]] = {}
        self.publishes: list[tuple[str, dict]] = []
        self._registrations = 0

    def seed(self, flow_uuid: str, name: str, definition: dict, namespace_id: int = 2, author_id: int = 7) -> dict:
        """A flow published by someone else, with one version."""
        self._registrations += 1
        self.flows[flow_uuid] = {
            "registration_id": self._registrations,
            "flow_uuid": flow_uuid,
            "name": name,
            "namespace_id": namespace_id,
            "owner_id": author_id,
        }
        return self.add_version(flow_uuid, definition, author_id=author_id, message="seeded")

    def add_version(self, flow_uuid: str, definition: dict, author_id: int = 7, message: str | None = None) -> dict:
        rows = self.versions.setdefault(flow_uuid, [])
        row = {
            "flow_uuid": flow_uuid,
            "version": (rows[0]["version"] + 1) if rows else 1,
            "author_id": author_id,
            "published_at": f"2026-11-01T12:0{len(rows)}:00Z",
            "message": message,
            "definition": definition,
        }
        rows.insert(0, row)
        return row

    @staticmethod
    def _stale(current: int | None, base: int | None) -> dict:
        return {"message": "base version is not the latest", "current_version": current, "base_version": base}

    def handle(self, request):
        path = request.url.path
        if path == "/catalog/namespaces" or path.startswith("/hub/flows"):
            self.calls.append((request.method, path))
            if self._bearer_user(request) is None:
                return self._json(401, {"detail": "Could not validate credentials"})
            if path == "/catalog/namespaces":
                parent = request.url.params.get("parent_id")
                parent_id = None if parent is None else int(parent)
                return self._json(200, [ns for ns in NAMESPACES if ns["parent_id"] == parent_id])
            if path == "/hub/flows":
                namespace = request.url.params.get("namespace_id")
                items = [
                    {**{k: v for k, v in f.items()}, "latest_version": self._latest(f["flow_uuid"])}
                    for f in self.flows.values()
                    if namespace is None or f["namespace_id"] == int(namespace)
                ]
                return self._json(200, items)
            match = _FLOW_PATH.match(path)
            if match is None:
                # A hub from before a route answers as FastAPI does for a path it does not have.
                self.calls.pop()
                return self._json(404, {"detail": "Not Found"})
            flow_uuid, action = match.group(1), match.group(2)
            if action == "versions":
                if flow_uuid not in self.flows:
                    return self._json(404, {"detail": "flow not registered on this hub"})
                return self._json(200, [self._public(v) for v in self.versions.get(flow_uuid, [])])
            if action == "checkout":
                if flow_uuid not in self.flows:
                    return self._json(404, {"detail": "flow not registered on this hub"})
                wanted = request.url.params.get("version")
                rows = [v for v in self.versions.get(flow_uuid, []) if wanted is None or v["version"] == int(wanted)]
                if not rows:
                    return self._json(404, {"detail": "no published version"})
                reg = self.flows[flow_uuid]
                return self._json(
                    200,
                    {
                        **self._public(rows[0]),
                        "registration_id": reg["registration_id"],
                        "name": reg["name"],
                        "namespace_id": reg["namespace_id"],
                        "definition": rows[0]["definition"],
                    },
                )
            return self._publish(flow_uuid, json.loads(request.content), self._bearer_user(request))
        return super().handle(request)

    def _publish(self, flow_uuid: str, body: dict, user: str):
        self.publishes.append((flow_uuid, body))
        base = body.get("base_version") or None
        reg = self.flows.get(flow_uuid)
        if reg is None:
            if base is not None:
                return self._json(409, {"detail": self._stale(None, body.get("base_version"))})
            name = body.get("name") or body["definition"].get("flowfile_name")
            namespace_id = body.get("namespace_id") or 1
            if any(f["name"] == name and f["namespace_id"] == namespace_id for f in self.flows.values()):
                return self._json(409, {"detail": "a flow with this name already exists in this namespace"})
            self._registrations += 1
            reg = self.flows[flow_uuid] = {
                "registration_id": self._registrations,
                "flow_uuid": flow_uuid,
                "name": name,
                "namespace_id": namespace_id,
                "owner_id": 1,
            }
        elif body.get("namespace_id") is not None and body["namespace_id"] != reg["namespace_id"]:
            return self._json(400, {"detail": "publish does not move a flow; use the catalog to move it"})
        current = self._latest(flow_uuid)
        if current != base:
            return self._json(409, {"detail": self._stale(current, body.get("base_version"))})
        row = self.add_version(flow_uuid, body["definition"], author_id=1, message=body.get("message"))
        return self._json(
            201,
            {
                **self._public(row),
                "registration_id": reg["registration_id"],
                "namespace_id": reg["namespace_id"],
                "name": reg["name"],
            },
        )

    def _latest(self, flow_uuid: str) -> int | None:
        rows = self.versions.get(flow_uuid)
        return rows[0]["version"] if rows else None

    @staticmethod
    def _public(row: dict) -> dict:
        return {k: v for k, v in row.items() if k != "definition"}

    @staticmethod
    def _json(status: int, body):
        import httpx

        return httpx.Response(status, json=body)


@pytest.fixture
def hub(monkeypatch) -> FakeHubWithFlows:
    fake = FakeHubWithFlows()
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
def shared_core(store, hub, monkeypatch, tmp_path):
    """A multi-user core with two regular users, each signed in and connected to the fake hub.

    Only an administrator connects a server to a hub, so each is one while connecting and a regular user after.

    Yields ``{"owner": (user_id, client), "stranger": (user_id, client)}``; the users and the flows registered
    under them are removed afterwards.
    """
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setenv("FLOWFILE_USER_DATA_DIR", str(tmp_path / "user_data"))
    monkeypatch.setattr(storage, "_user_data_dir", tmp_path / "user_data")
    monkeypatch.setenv("JWT_SECRET_KEY", "hub-flows-test-secret")
    password = "hub-flows-test-password"
    people: dict[str, tuple[int, str]] = {}
    with get_db_context() as db:
        for key in ("owner", "stranger"):
            username = f"hub_flows_{key}"
            stale = db.query(db_models.User).filter(db_models.User.username == username).first()
            if stale is not None:
                db.delete(stale)
                db.commit()
            user = db_models.User(
                username=username,
                email=f"{username}@flowfile.app",
                full_name=username,
                hashed_password=get_password_hash(password),
                disabled=False,
                is_admin=True,
                must_change_password=False,
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            people[key] = (user.id, username)
    clients = {}
    for key, (uid, username) in people.items():
        c = TestClient(main.app)
        token = c.post("/auth/token", data={"username": username, "password": password})
        assert token.status_code == 200, token.text
        c.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
        assert c.post("/hub_client/connection", json={**CONNECT, "device_name": username}).status_code == 200
        clients[key] = (uid, c)
    with get_db_context() as db:
        for uid, _ in clients.values():
            db.get(db_models.User, uid).is_admin = False
        db.commit()
    invalidate_user_cache()
    hub.calls.clear()
    yield clients
    ids = [uid for uid, _ in clients.values()]
    for _, c in clients.values():
        c.close()
    with get_db_context() as db:
        db.query(db_models.FlowRegistration).filter(db_models.FlowRegistration.owner_id.in_(ids)).delete(
            synchronize_session=False
        )
        for uid in ids:
            user = db.get(db_models.User, uid)
            if user is not None:
                db.delete(user)
        db.commit()


@pytest.fixture
def hub_copies():
    """Files the open route writes under the flows folder, removed afterwards."""
    written: list[str] = []
    yield written
    for path in written:
        Path(path).unlink(missing_ok=True)


def _new_flow(user_id: int, path: Path | None, name: str = "sales_report"):
    flow_id = create_unique_id()
    flow_file_handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name=name,
            path=str(path) if path else "",
            execution_mode="Development",
            execution_location="local",
        ),
        user_id=user_id,
    )
    flow = flow_file_handler.get_flow(flow_id)
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"region": "north", "n": 1}])
        )
    )
    if path is not None:
        flow.save_flow(str(path))
    return flow


def _add_csv_read(flow, csv_path: Path, node_id: int = 2) -> None:
    csv_path.write_text("a,b\n1,2\n", encoding="utf-8")
    flow.add_read(
        input_schema.NodeRead(
            flow_id=flow.flow_id,
            node_id=node_id,
            received_file=input_schema.ReceivedTable(name=csv_path.name, path=str(csv_path), file_type="csv"),
        )
    )


def _registration(**by):
    with get_db_context() as db:
        repo = SQLAlchemyCatalogRepository(db)
        reg = repo.get_flow_by_path(by["path"]) if "path" in by else repo.get_flow_by_uuid(by["uuid"])
        if reg is None:
            return None
        return {"id": reg.id, "flow_uuid": reg.flow_uuid, "name": reg.name, "flow_path": reg.flow_path}


def _saved_origin(path: Path) -> dict | None:
    return yaml.safe_load(path.read_text(encoding="utf-8"))["flowfile_settings"]["hub_origin"]


def test_every_flow_route_needs_a_hub_connection_and_never_answers_401(api, hub, user_id):
    own_flow = _new_flow(user_id, None)
    for method, path, kwargs in (
        ("GET", "/hub_client/flows/3f7b4b8e-1b2a-4c3d-9e8f-0a1b2c3d4e5f/versions", {}),
        ("POST", "/hub_client/publish", {"json": {"flow_id": own_flow.flow_id}}),
        ("POST", "/hub_client/open", {"json": {"flow_uuid": "3f7b4b8e-1b2a-4c3d-9e8f-0a1b2c3d4e5f"}}),
    ):
        response = api.request(method, path, **kwargs)
        assert response.status_code == 400, (path, response.text)
        assert "Connect to a hub" in response.json()["detail"]
    assert hub.calls == []


def test_a_flow_s_versions_are_passed_through_from_the_hub(connected, hub):
    hub.seed("11111111-2222-4333-8444-555555555555", "orders", {"flowfile_name": "orders"}, namespace_id=2)

    versions = connected.get("/hub_client/flows/11111111-2222-4333-8444-555555555555/versions").json()
    assert [v["version"] for v in versions] == [1]
    assert "definition" not in versions[0]
    assert hub.calls[-1] == ("GET", "/hub/flows/11111111-2222-4333-8444-555555555555/versions")


def test_a_first_publish_sends_no_base_the_registrations_uuid_the_stripped_document_name_and_namespace(
    connected, hub, user_id, tmp_path
):
    path = tmp_path / "sales_report.yaml"
    flow = _new_flow(user_id, path)
    auto_register_flow(str(path), "Sales report", user_id)
    registration = _registration(path=str(path))

    response = connected.post(
        "/hub_client/publish", json={"flow_id": flow.flow_id, "namespace_id": 2, "message": "first cut"}
    )

    assert response.status_code == 201, response.text
    assert response.json() == {"version": 1, "name": "Sales report"}
    flow_uuid, body = hub.publishes[0]
    assert flow_uuid == registration["flow_uuid"]
    assert body["base_version"] is None
    assert body["message"] == "first cut"
    assert body["name"] == "Sales report"
    assert body["namespace_id"] == 2
    assert body["definition"]["flowfile_settings"]["source_registration_id"] is None
    assert body["definition"]["flowfile_settings"]["hub_origin"] is None
    assert [n["type"] for n in body["definition"]["nodes"]] == ["manual_input"]

    origin = {"hub_url": HUB_URL, "flow_uuid": registration["flow_uuid"], "version": 1}
    assert flow.flow_settings.hub_origin.model_dump() == origin
    assert _saved_origin(path) == origin
    reopened = open_flow(path)
    assert reopened.flow_settings.hub_origin.model_dump() == origin
    assert reopened.flow_settings.source_registration_id == registration["id"]


def test_a_second_publish_sends_the_origins_version_as_its_base(connected, hub, user_id, tmp_path):
    path = tmp_path / "again.yaml"
    flow = _new_flow(user_id, path)
    assert connected.post("/hub_client/publish", json={"flow_id": flow.flow_id}).status_code == 201

    response = connected.post("/hub_client/publish", json={"flow_id": flow.flow_id, "message": "second"})

    assert response.status_code == 201, response.text
    assert response.json()["version"] == 2
    assert hub.publishes[1][1]["base_version"] == 1
    assert hub.publishes[1][0] == hub.publishes[0][0]
    assert flow.flow_settings.hub_origin.version == 2
    assert _saved_origin(path)["version"] == 2


def test_a_stale_base_answers_stale_base_with_both_versions_and_publish_over_succeeds(
    connected, hub, user_id, tmp_path
):
    path = tmp_path / "stale.yaml"
    flow = _new_flow(user_id, path)
    assert connected.post("/hub_client/publish", json={"flow_id": flow.flow_id, "message": "mine"}).status_code == 201
    flow_uuid = flow.flow_settings.hub_origin.flow_uuid
    hub.add_version(flow_uuid, {"flowfile_name": "theirs"}, author_id=7, message="theirs")

    response = connected.post("/hub_client/publish", json={"flow_id": flow.flow_id, "message": "mine again"})

    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert detail["kind"] == "stale_base"
    assert detail["flow_uuid"] == flow_uuid
    assert detail["base"]["version"] == 1 and detail["base"]["message"] == "mine"
    assert detail["current"]["version"] == 2 and detail["current"]["author_id"] == 7
    assert detail["current"]["message"] == "theirs"
    assert flow.flow_settings.hub_origin.version == 1
    assert _saved_origin(path)["version"] == 1

    over = connected.post(
        "/hub_client/publish", json={"flow_id": flow.flow_id, "message": "mine again", "overwrite_version": 2}
    )

    assert over.status_code == 201, over.text
    assert over.json()["version"] == 3
    assert hub.publishes[-1][1]["base_version"] == 2
    assert [v["version"] for v in hub.versions[flow_uuid]] == [3, 2, 1]
    assert flow.flow_settings.hub_origin.version == 3


def test_a_name_taken_on_the_hub_is_refused_with_the_hubs_status_and_stamps_nothing(connected, hub, user_id, tmp_path):
    hub.seed("11111111-2222-4333-8444-555555555555", "sales_report", {"flowfile_name": "sales_report"}, namespace_id=2)
    path = tmp_path / "sales_report.yaml"
    flow = _new_flow(user_id, path)

    response = connected.post("/hub_client/publish", json={"flow_id": flow.flow_id, "namespace_id": 2})

    assert response.status_code == 409, response.text
    assert response.json()["detail"] == {
        "kind": "refused",
        "status": 409,
        "detail": "a flow with this name already exists in this namespace",
    }
    assert flow.flow_settings.hub_origin is None
    assert _saved_origin(path) is None


def test_a_hub_401_at_publish_answers_400(connected, hub, user_id, tmp_path, monkeypatch):
    flow = _new_flow(user_id, tmp_path / "unauth.yaml")
    monkeypatch.setattr(hub, "_bearer_user", lambda request: None)

    response = connected.post("/hub_client/publish", json={"flow_id": flow.flow_id})

    assert response.status_code == 400
    assert response.json()["detail"]["kind"] == "refused"


def test_an_unsaved_flow_is_refused_before_the_hub_is_called(connected, hub, user_id):
    flow = _new_flow(user_id, None)

    response = connected.post("/hub_client/publish", json={"flow_id": flow.flow_id})

    assert response.status_code == 422
    assert response.json()["detail"] == "save the flow first"
    assert hub.publishes == []


def test_a_document_from_another_hub_is_refused(connected, hub, user_id, tmp_path):
    path = tmp_path / "elsewhere.yaml"
    flow = _new_flow(user_id, path)
    flow.flow_settings.hub_origin = schemas.HubOrigin(
        hub_url="https://other-hub.example.test", flow_uuid="11111111-2222-4333-8444-555555555555", version=4
    )

    response = connected.post("/hub_client/publish", json={"flow_id": flow.flow_id})

    assert response.status_code == 409
    assert response.json()["detail"] == {"kind": "other_hub", "hub_url": "https://other-hub.example.test"}
    assert hub.publishes == []


def test_another_users_flow_is_not_found(connected, hub, user_id, tmp_path):
    flow = _new_flow(user_id + 1000, tmp_path / "theirs.yaml")
    assert connected.post("/hub_client/publish", json={"flow_id": flow.flow_id}).status_code == 404
    assert connected.get("/hub_client/publish_check", params={"flow_id": flow.flow_id}).status_code == 404


def test_another_users_flow_is_not_found_without_a_hub_connection_too(api, hub, user_id, tmp_path):
    """The flow is resolved for the caller before the hub session is checked, so a 400 never reveals a flow id."""
    flow = _new_flow(user_id + 1000, tmp_path / "theirs.yaml")

    response = api.post("/hub_client/publish", json={"flow_id": flow.flow_id})

    assert response.status_code == 404
    assert hub.calls == []


def test_open_writes_a_registered_copy_and_asks_only_before_losing_unpublished_edits(
    connected, hub, user_id, tmp_path, hub_copies
):
    source = _new_flow(user_id, tmp_path / "source.yaml", name="Quarterly sales")
    flow_uuid = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    hub.seed(flow_uuid, "Quarterly sales", source.get_flowfile_data().model_dump(mode="json"), namespace_id=2)

    response = connected.post("/hub_client/open", json={"flow_uuid": flow_uuid})

    assert response.status_code == 200, response.text
    body = response.json()
    hub_copies.append(body["flow_path"])
    assert set(body) == {"flow_path"}
    copy = Path(body["flow_path"])
    assert copy.parent == storage.flows_directory / "hub"
    assert copy.name == f"Quarterly_sales_{flow_uuid[:8]}.yaml"
    assert _saved_origin(copy) == {"hub_url": HUB_URL, "flow_uuid": flow_uuid, "version": 1}
    registration = _registration(uuid=flow_uuid)
    assert registration["flow_path"] == str(copy) and registration["name"] == "Quarterly sales"
    first_id = yaml.safe_load(copy.read_text(encoding="utf-8"))["flowfile_id"]
    assert first_id != source.flow_id

    opened = flow_file_handler.import_flow(copy, user_id=user_id)
    opened_flow = flow_file_handler.get_flow(opened)
    assert opened_flow.flow_settings.hub_origin.model_dump() == {
        "hub_url": HUB_URL,
        "flow_uuid": flow_uuid,
        "version": 1,
    }
    assert [n.node_type for n in opened_flow.nodes] == ["manual_input"]

    repeat = connected.post("/hub_client/open", json={"flow_uuid": flow_uuid})
    assert repeat.status_code == 200, repeat.text
    assert repeat.json() == {"flow_path": str(copy)}
    assert _saved_origin(copy)["version"] == 1

    # A copy that still equals the version it came from moves to the version asked for without a question.
    hub.add_version(flow_uuid, {**source.get_flowfile_data().model_dump(mode="json"), "flowfile_name": "v2"})
    moved = connected.post("/hub_client/open", json={"flow_uuid": flow_uuid})
    assert moved.status_code == 200, moved.text
    assert _saved_origin(copy)["version"] == 2
    back = connected.post("/hub_client/open", json={"flow_uuid": flow_uuid, "version": 1})
    assert back.status_code == 200, back.text
    assert _saved_origin(copy)["version"] == 1

    edited = yaml.safe_load(copy.read_text(encoding="utf-8"))
    edited["flowfile_settings"]["description"] = "edited here, never published"
    copy.write_text(yaml.safe_dump(edited, sort_keys=False), encoding="utf-8")
    refused = connected.post("/hub_client/open", json={"flow_uuid": flow_uuid})
    assert refused.status_code == 409
    assert refused.json()["detail"] == {"kind": "local_copy_exists", "flow_path": str(copy)}
    assert "never published" in copy.read_text(encoding="utf-8")

    replaced = connected.post("/hub_client/open", json={"flow_uuid": flow_uuid, "replace": True})
    assert replaced.status_code == 200, replaced.text
    assert replaced.json() == {"flow_path": str(copy)}
    assert _saved_origin(copy)["version"] == 2
    assert "never published" not in copy.read_text(encoding="utf-8")
    assert yaml.safe_load(copy.read_text(encoding="utf-8"))["flowfile_id"] == first_id
    assert _registration(uuid=flow_uuid)["id"] == registration["id"]
    with get_db_context() as db:
        assert SQLAlchemyCatalogRepository(db).count_flows_by_path(str(copy)) == 1


def test_open_of_a_flow_published_from_here_points_at_its_own_file(connected, hub, user_id, tmp_path):
    path = tmp_path / "mine.yaml"
    flow = _new_flow(user_id, path)
    auto_register_flow(str(path), "mine", user_id)
    assert connected.post("/hub_client/publish", json={"flow_id": flow.flow_id}).status_code == 201
    flow_uuid = flow.flow_settings.hub_origin.flow_uuid
    hub.add_version(flow_uuid, {**hub.versions[flow_uuid][0]["definition"]}, author_id=7)

    opened = connected.post("/hub_client/open", json={"flow_uuid": flow_uuid})
    assert opened.status_code == 200, opened.text
    assert opened.json()["flow_path"] == str(path)
    assert _saved_origin(path)["version"] == 2
    assert yaml.safe_load(path.read_text(encoding="utf-8"))["flowfile_id"] == flow.flow_id


def test_open_on_a_shared_core_gives_each_person_a_copy_of_their_own(shared_core, hub, tmp_path, hub_copies):
    owner_id, owner = shared_core["owner"]
    stranger_id, stranger = shared_core["stranger"]
    flow_uuid = "99999999-8888-4777-8666-555555555555"
    hub.seed(flow_uuid, "Payroll", _new_flow(owner_id, None).get_flowfile_data().model_dump(mode="json"))
    # A shared core registers a flow only inside its flow folders, so the owner's copy lives there.
    storage.flows_directory.mkdir(parents=True, exist_ok=True)
    owners_copy = storage.flows_directory / f"owners_copy_{flow_uuid[:8]}.yaml"
    owners_copy.write_text("flowfile_id: 1\n", encoding="utf-8")
    with get_db_context() as db:
        namespace_id = CatalogService(SQLAlchemyCatalogRepository(db)).ensure_local_flows_namespace().id
    register_flow_in_namespace(str(owners_copy), "Payroll", owner_id, namespace_id, flow_uuid=flow_uuid)
    registration = _registration(uuid=flow_uuid)

    opened = stranger.post("/hub_client/open", json={"flow_uuid": flow_uuid})
    assert opened.status_code == 200, opened.text
    own_copy = Path(opened.json()["flow_path"])
    hub_copies.append(str(own_copy))
    assert own_copy == (storage.flows_directory / "hub" / str(stranger_id) / f"Payroll_{flow_uuid[:8]}.yaml").resolve()
    assert _saved_origin(own_copy) == {"hub_url": HUB_URL, "flow_uuid": flow_uuid, "version": 1}
    own_registration = _registration(path=str(own_copy))
    assert own_registration["name"] == "Payroll" and own_registration["flow_uuid"] != flow_uuid
    # The owner's copy and the registration holding the hub's uuid are as they were.
    assert owners_copy.read_text(encoding="utf-8") == "flowfile_id: 1\n"
    assert _registration(uuid=flow_uuid) == registration

    # A second open is the stranger's own copy again, written over only when nothing of theirs is lost.
    again = stranger.post("/hub_client/open", json={"flow_uuid": flow_uuid})
    assert again.status_code == 200, again.text
    assert again.json()["flow_path"] == str(own_copy)
    edited = yaml.safe_load(own_copy.read_text(encoding="utf-8"))
    edited["flowfile_settings"]["description"] = "the stranger's edit"
    own_copy.write_text(yaml.safe_dump(edited, sort_keys=False), encoding="utf-8")
    asked = stranger.post("/hub_client/open", json={"flow_uuid": flow_uuid})
    assert asked.status_code == 409, asked.text
    assert asked.json()["detail"] == {"kind": "local_copy_exists", "flow_path": str(own_copy)}

    # The owner's file is no flow document, so it counts as edited: named, and written over only on replace.
    repeat = owner.post("/hub_client/open", json={"flow_uuid": flow_uuid})
    assert repeat.status_code == 409, repeat.text
    assert repeat.json()["detail"] == {"kind": "local_copy_exists", "flow_path": str(owners_copy)}
    replaced = owner.post("/hub_client/open", json={"flow_uuid": flow_uuid, "replace": True})
    assert replaced.status_code == 200, replaced.text
    assert _saved_origin(owners_copy) == {"hub_url": HUB_URL, "flow_uuid": flow_uuid, "version": 1}
    assert _registration(uuid=flow_uuid) == registration
    assert "the stranger's edit" in own_copy.read_text(encoding="utf-8")
    owners_copy.unlink(missing_ok=True)


def test_open_on_a_shared_core_checks_the_owner_of_a_copy_stored_under_a_links_spelling(shared_core, hub, tmp_path):
    """A copy registered before the rule under a link's spelling of the flows folder: a stranger's replace writes
    a copy of their own, the owner's writes the file where the link leads."""
    owner_id, owner = shared_core["owner"]
    _, stranger = shared_core["stranger"]
    flow_uuid = "99999999-8888-4777-8666-333333333333"
    hub.seed(flow_uuid, "Invoices", _new_flow(owner_id, None).get_flowfile_data().model_dump(mode="json"))
    storage.flows_directory.mkdir(parents=True, exist_ok=True)
    link = tmp_path / "store_link"
    link.symlink_to(storage.flows_directory, target_is_directory=True)
    real_copy = storage.flows_directory / f"linked_copy_{flow_uuid[:8]}.yaml"
    real_copy.write_text("flowfile_id: 1\n", encoding="utf-8")
    with get_db_context() as db:
        db.add(
            db_models.FlowRegistration(
                flow_uuid=flow_uuid, name="Invoices", flow_path=str(link / real_copy.name), owner_id=owner_id
            )
        )
        db.commit()
    strangers_copy = None
    try:
        own = stranger.post("/hub_client/open", json={"flow_uuid": flow_uuid, "replace": True})
        assert own.status_code == 200, own.text
        strangers_copy = Path(own.json()["flow_path"])
        assert strangers_copy != real_copy.resolve()
        assert real_copy.read_text(encoding="utf-8") == "flowfile_id: 1\n"
        replaced = owner.post("/hub_client/open", json={"flow_uuid": flow_uuid, "replace": True})
        assert replaced.status_code == 200, replaced.text
        assert replaced.json()["flow_path"] == str(real_copy.resolve())
        assert _saved_origin(real_copy)["flow_uuid"] == flow_uuid
    finally:
        real_copy.unlink(missing_ok=True)
        if strangers_copy is not None:
            strangers_copy.unlink(missing_ok=True)


def test_open_on_a_shared_core_never_writes_over_a_local_copy_outside_the_flow_folders(shared_core, hub, tmp_path):
    """A registration from before the rule may still point elsewhere; the hub's open leaves that file alone."""
    owner_id, owner = shared_core["owner"]
    flow_uuid = "99999999-8888-4777-8666-444444444444"
    hub.seed(flow_uuid, "Ledger", _new_flow(owner_id, None).get_flowfile_data().model_dump(mode="json"))
    elsewhere = tmp_path / "elsewhere.yaml"
    elsewhere.write_text("token: canary-7f3a\n", encoding="utf-8")
    with get_db_context() as db:
        db.add(
            db_models.FlowRegistration(flow_uuid=flow_uuid, name="Ledger", flow_path=str(elsewhere), owner_id=owner_id)
        )
        db.commit()
    for body in ({"flow_uuid": flow_uuid}, {"flow_uuid": flow_uuid, "replace": True}):
        refused = owner.post("/hub_client/open", json=body)
        assert refused.status_code == 422, refused.text
        assert refused.json()["detail"] == "the flow's file is outside the server's flow folders"
    assert elsewhere.read_text(encoding="utf-8") == "token: canary-7f3a\n"


def test_open_of_an_unknown_flow_or_a_malformed_uuid_is_refused(connected, hub):
    missing = connected.post("/hub_client/open", json={"flow_uuid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"})
    assert missing.status_code == 404
    assert missing.json()["detail"] == {"kind": "refused", "status": 404, "detail": "flow not registered on this hub"}
    assert connected.post("/hub_client/open", json={"flow_uuid": "not-a-uuid"}).status_code == 422


def test_publish_check_names_a_read_node_with_a_csv_path_and_nothing_for_a_flow_without_local_paths(
    connected, user_id, tmp_path
):
    flow = _new_flow(user_id, tmp_path / "check.yaml")
    assert connected.get("/hub_client/publish_check", params={"flow_id": flow.flow_id}).json() == []

    _add_csv_read(flow, tmp_path / "sales.csv")

    uses = connected.get("/hub_client/publish_check", params={"flow_id": flow.flow_id}).json()
    assert len(uses) == 1 and set(uses[0]) == {"label", "path"}
    assert uses[0]["path"] == str(tmp_path / "sales.csv")
    assert "sales.csv" in uses[0]["label"]

    publish = connected.post("/hub_client/publish", json={"flow_id": flow.flow_id})
    assert publish.status_code == 201, publish.text
