"""The notebook session behind the kernel routes as ``flow-session:<flow_id>``, with the TestClient: mode and
loopback gates, ownership, execute_cell, clear_namespace, the synthetic kernel and column schemas, against a
real session process and without Docker."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.notebook import registry as session_registry
from shared.notebook_display import TABLE_MIME
from tests.notebook.conftest import no_kernel_manager
from tests.notebook.session_helpers import small_flow

OWNER_ID = 1
OTHER_ID = 2
LOOPBACK = ("127.0.0.1", 50123)


@pytest.fixture
def open_flow(monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    graph = small_flow()
    flow_file_handler._flows[graph.flow_id] = graph
    flow_file_handler._register_user_session(OWNER_ID, graph.flow_id)
    yield graph
    session_registry.get_registry().close(graph.flow_id)
    flow_file_handler.delete_flow(graph.flow_id)
    flow_file_handler._unregister_user_session(OWNER_ID, graph.flow_id)


@pytest.fixture
def client_as():
    def _as(user_id: int, client: tuple[str, int] = LOOPBACK, is_admin: bool = True) -> TestClient:
        user = PydanticUser(username=f"nb_{user_id}", id=user_id, disabled=False, is_admin=is_admin)
        main.app.dependency_overrides[get_current_active_user] = lambda: user
        main.app.dependency_overrides[get_current_user] = lambda: user
        return TestClient(main.app, client=client)

    yield _as
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)


def _kernel(flow) -> str:
    return f"/kernels/flow-session:{flow.flow_id}"


def _execute(client: TestClient, flow, code: str, node_id: int = 0) -> dict:
    response = client.post(
        f"{_kernel(flow)}/execute_cell", json={"node_id": node_id, "code": code, "flow_id": flow.flow_id}
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_electron_mode_refuses_a_non_loopback_client(open_flow, client_as):
    client = client_as(OWNER_ID, client=("192.168.1.20", 50000))
    response = client.post(f"{_kernel(open_flow)}/execute_cell", json={"node_id": 0, "code": "1"})
    assert response.status_code == 403


def test_multi_user_mode_refuses_without_the_opt_in(open_flow, client_as, monkeypatch):
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.delenv("FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER", raising=False)
    owner = client_as(OWNER_ID)
    assert owner.post(f"{_kernel(open_flow)}/execute_cell", json={"node_id": 0, "code": "1"}).status_code == 403
    assert owner.get(_kernel(open_flow)).status_code == 403


def test_another_users_flow_is_404(open_flow, client_as):
    other = client_as(OTHER_ID)
    assert other.post(f"{_kernel(open_flow)}/execute_cell", json={"node_id": 0, "code": "1"}).status_code == 404
    assert other.post(f"{_kernel(open_flow)}/clear_namespace", params={"flow_id": open_flow.flow_id}).status_code == 404
    assert other.get(_kernel(open_flow)).status_code == 404


def test_the_session_behaves_as_a_kernel(open_flow, client_as):
    owner = client_as(OWNER_ID)
    with no_kernel_manager() as calls:
        result = _execute(owner, open_flow, "print('hi')\nx = fl.from_dict({'a': [1, 2]}); display(x)", node_id=7)
        assert result["success"] is True and result["error"] is None
        assert result["stdout"] == "hi\n"
        [table] = result["display_outputs"]
        assert table["mime_type"] == TABLE_MIME and table["title"] == ""
        assert len(json.loads(table["data"])["data"]) == 2
        assert result["namespace_generation"] and result["revision"] >= 1

        [text] = _execute(owner, open_flow, "x")["display_outputs"]
        assert text["mime_type"] == "text/plain"
        assert "  a: Int64" in text["data"] and "Run on canvas" in text["data"] and "display(" in text["data"]

        kernel = owner.get(_kernel(open_flow)).json()
        assert kernel["id"] == f"flow-session:{open_flow.flow_id}" and kernel["state"] == "idle"

        schemas = owner.post(f"{_kernel(open_flow)}/lsp/dataframe_schemas", json={"flow_id": open_flow.flow_id})
        frames = {frame["name"]: frame for frame in schemas.json()["dataframes"]}
        assert schemas.json()["state"] == "ready"
        assert frames["x"]["columns"] == [{"name": "a", "dtype": "Int64"}]

        failing = _execute(owner, open_flow, "1 / 0")
        assert failing["success"] is False
        assert failing["error"] == "ZeroDivisionError: division by zero"
        assert "Traceback" in failing["stderr"]

        cleared = owner.post(f"{_kernel(open_flow)}/clear_namespace", params={"flow_id": open_flow.flow_id})
        assert cleared.status_code == 200
        gone = _execute(owner, open_flow, "x")
        assert gone["success"] is False and gone["error"].startswith("NameError")
        assert gone["namespace_generation"] != result["namespace_generation"]
    assert calls == []


def test_closing_the_flow_closes_its_session(open_flow, client_as):
    owner = client_as(OWNER_ID)
    assert owner.get(_kernel(open_flow)).status_code == 200
    session = session_registry.get_registry().find(OWNER_ID, open_flow.flow_id)
    assert session is not None
    assert owner.post("/editor/close_flow/", params={"flow_id": open_flow.flow_id}).status_code == 200
    assert session.closed
    assert session_registry.get_registry().find(OWNER_ID, open_flow.flow_id) is None
