"""
Auth tests for /user_defined_components: every route must reject unauthenticated
requests with 401, and the node CRUD + rescan happy path must work with a JWT.
"""
import uuid

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from shared import storage

VALID_NODE_CODE = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class AuthTestNode(CustomNodeBase):
    node_name: str = "{node_name}"
    node_category: str = "Testing"

    settings_schema: NodeSettings = NodeSettings(
        main_section=Section(title="Main", value_input=TextInput(label="Value", default="x")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''


def get_authed_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


unauthed_client = TestClient(main.app)
authed_client = get_authed_client()


UNAUTHENTICATED_REQUESTS = [
    ("GET", "/user_defined_components/custom-node-schema", {"params": {"flow_id": 1, "node_id": 1}}),
    (
        "POST",
        "/user_defined_components/update_user_defined_node",
        {"params": {"node_type": "x"}, "json": {"flow_id": 1, "node_id": 1, "settings": {}}},
    ),
    ("POST", "/user_defined_components/save-custom-node", {"json": {"file_name": "x", "code": "x = 1"}}),
    ("GET", "/user_defined_components/list-custom-nodes", {}),
    ("GET", "/user_defined_components/get-custom-node/somefile", {}),
    ("DELETE", "/user_defined_components/delete-custom-node/somefile", {}),
    ("POST", "/user_defined_components/rescan", {}),
    ("GET", "/user_defined_components/list-icons", {}),
    ("POST", "/user_defined_components/upload-icon", {"files": {"file": ("t.png", b"fake", "image/png")}}),
    ("GET", "/user_defined_components/icon/some.png", {}),
    ("DELETE", "/user_defined_components/delete-icon/some.png", {}),
]


@pytest.mark.parametrize(
    "method,url,kwargs", UNAUTHENTICATED_REQUESTS, ids=[f"{m} {u}" for m, u, _ in UNAUTHENTICATED_REQUESTS]
)
def test_every_route_requires_auth(method, url, kwargs):
    response = unauthed_client.request(method, url, **kwargs)
    assert response.status_code == 401, f"{method} {url} returned {response.status_code}, expected 401"


class TestAuthedHappyPath:
    @pytest.fixture
    def node_file(self):
        """Unique node file name + cleanup of both disk and registry."""
        stem = f"auth_test_node_{uuid.uuid4().hex[:8]}"
        yield stem
        file_path = storage.user_defined_nodes_directory / f"{stem}.py"
        if file_path.exists():
            authed_client.delete(f"/user_defined_components/delete-custom-node/{stem}.py")
        if file_path.exists():
            file_path.unlink()

    def test_save_list_get_rescan_delete_cycle(self, node_file):
        node_name = f"Auth Node {node_file[-8:]}"
        code = VALID_NODE_CODE.format(node_name=node_name)

        save_response = authed_client.post(
            "/user_defined_components/save-custom-node", json={"file_name": node_file, "code": code}
        )
        assert save_response.status_code == 200, save_response.text
        body = save_response.json()
        assert body["success"] is True
        assert body["load_error"] is None
        assert body["node"]["node_name"] == node_name
        assert body["node"]["node_key"] == node_name.replace(" ", "_").lower()
        assert body["node"]["environment"] == "local"
        assert len(body["node"]["source_hash"]) == 64

        list_response = authed_client.get("/user_defined_components/list-custom-nodes")
        assert list_response.status_code == 200
        listed = {n["file_name"]: n for n in list_response.json()}
        assert f"{node_file}.py" in listed
        assert listed[f"{node_file}.py"]["error"] is None

        get_response = authed_client.get(f"/user_defined_components/get-custom-node/{node_file}")
        assert get_response.status_code == 200
        detail = get_response.json()
        assert detail["content"] == code
        assert detail["file_hash"] == body["node"]["source_hash"]
        assert detail["designer_state"] is None
        assert detail["supports_visual_edit"] is False
        assert detail["metadata"]["node_name"] == node_name

        rescan_response = authed_client.post("/user_defined_components/rescan")
        assert rescan_response.status_code == 200
        assert rescan_response.json()["total"] >= 1

        delete_response = authed_client.delete(f"/user_defined_components/delete-custom-node/{node_file}.py")
        assert delete_response.status_code == 200
        assert delete_response.json()["success"] is True
        after = authed_client.get("/user_defined_components/list-custom-nodes").json()
        assert f"{node_file}.py" not in {n["file_name"] for n in after}

    def test_saved_but_broken_is_success_with_load_error(self, node_file):
        response = authed_client.post(
            "/user_defined_components/save-custom-node",
            json={"file_name": node_file, "code": "x = 1  # valid python, no node class\n"},
        )
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["success"] is True
        assert "No CustomNodeBase subclass" in body["load_error"]

        listed = {n["file_name"]: n for n in authed_client.get("/user_defined_components/list-custom-nodes").json()}
        assert f"{node_file}.py" in listed
        assert "No CustomNodeBase subclass" in listed[f"{node_file}.py"]["error"]

    def test_syntax_error_rejected_before_write(self, node_file):
        response = authed_client.post(
            "/user_defined_components/save-custom-node", json={"file_name": node_file, "code": "def broken(:\n"}
        )
        assert response.status_code == 400
        assert not (storage.user_defined_nodes_directory / f"{node_file}.py").exists()

    def test_get_missing_node_404(self):
        response = authed_client.get("/user_defined_components/get-custom-node/never_saved_node_xyz")
        assert response.status_code == 404

    def test_delete_missing_node_404(self):
        response = authed_client.delete("/user_defined_components/delete-custom-node/never_saved_node_xyz")
        assert response.status_code == 404

    def test_list_icons_authed(self):
        response = authed_client.get("/user_defined_components/list-icons")
        assert response.status_code == 200
        assert isinstance(response.json(), list)
