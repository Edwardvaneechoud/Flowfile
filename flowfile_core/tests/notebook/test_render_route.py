"""``GET /notebook/render?flow_id=``: the owner's rendering, and 404 for flows the caller has not open."""

import pytest
from fastapi.testclient import TestClient

import flowfile_frame as ff
from flowfile_core import flow_file_handler, main
from flowfile_core.auth.jwt import get_current_active_user, get_current_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.notebook.render import render

OWNER_ID = 1
OTHER_ID = 2


@pytest.fixture
def open_flow():
    """A small frame-built flow opened in the owner's editor session."""
    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 20, 30]})
    result = orders.filter(ff.col("amount") > 10).with_columns((ff.col("amount") * 2).alias("double"))
    graph = result.flow_graph
    flow_file_handler._flows[graph.flow_id] = graph
    flow_file_handler._register_user_session(OWNER_ID, graph.flow_id)
    yield graph
    flow_file_handler.delete_flow(graph.flow_id)
    flow_file_handler._unregister_user_session(OWNER_ID, graph.flow_id)


@pytest.fixture
def client_as():
    def _as(user_id: int) -> TestClient:
        user = PydanticUser(username=f"nb_{user_id}", id=user_id, disabled=False, is_admin=user_id == OWNER_ID)
        main.app.dependency_overrides[get_current_active_user] = lambda: user
        main.app.dependency_overrides[get_current_user] = lambda: user
        return TestClient(main.app)

    yield _as
    main.app.dependency_overrides.pop(get_current_active_user, None)
    main.app.dependency_overrides.pop(get_current_user, None)


def test_render_route_returns_the_owner_rendering(open_flow, client_as):
    response = client_as(OWNER_ID).get("/notebook/render", params={"flow_id": open_flow.flow_id})
    assert response.status_code == 200
    body = response.json()
    assert set(body) == {"cells", "warnings", "var_by_node", "code_fingerprint"}
    assert body == render(open_flow).model_dump(mode="json")
    assert body["cells"][0]["cell_id"] == "imports"
    node_cells = [cell for cell in body["cells"] if cell["kind"] == "node"]
    assert sorted(n for cell in node_cells for n in cell["node_ids"]) == sorted(n.node_id for n in open_flow.nodes)
    assert all(cell["status"] == "code" for cell in node_cells)
    for cell in body["cells"]:
        compile(cell["code"], cell["cell_id"], "exec")


def test_render_route_404_for_another_users_flow(open_flow, client_as):
    response = client_as(OTHER_ID).get("/notebook/render", params={"flow_id": open_flow.flow_id})
    assert response.status_code == 404


def test_render_route_404_for_a_flow_that_is_not_open(client_as):
    response = client_as(OWNER_ID).get("/notebook/render", params={"flow_id": 987654321})
    assert response.status_code == 404


def test_render_route_requires_auth():
    assert TestClient(main.app).get("/notebook/render", params={"flow_id": 1}).status_code == 401
