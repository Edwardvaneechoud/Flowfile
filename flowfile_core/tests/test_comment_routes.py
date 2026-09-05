"""
Route tests for the canvas-comment editor endpoints.

Run with:
    pytest flowfile_core/tests/test_comment_routes.py -v
"""
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.schemas import input_schema, schemas


def get_test_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = get_test_client()


def make_flow(flow_id: int) -> int:
    if flow_file_handler.get_flow(flow_id) is not None:
        flow_file_handler.delete_flow(flow_id)
    flow_file_handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name="cmt", path="."))
    graph = flow_file_handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    return flow_id


def test_create_update_delete_comment():
    flow_id = make_flow(960)

    created = client.post(
        "/editor/create_comment/",
        params={"flow_id": flow_id},
        json={"text": "Watch out", "x_position": 12.0, "y_position": 34.0},
    )
    assert created.status_code == 200, created.text
    body = created.json()
    assert body["success"] is True
    assert body["comment"]["text"] == "Watch out"
    assert "history" in body
    comment_id = body["comment"]["id"]

    data = client.get("/flow_data/v2", params={"flow_id": flow_id}).json()
    assert [comment["id"] for comment in data["comments"]] == [comment_id]

    updated = client.post(
        "/editor/update_comment/",
        params={"flow_id": flow_id, "comment_id": comment_id},
        json={"text": "Resolved", "x_position": 1.0, "y_position": 2.0, "width": 300.0, "height": 90.0},
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["comment"]["text"] == "Resolved"
    assert updated.json()["comment"]["width"] == 300.0

    deleted = client.post("/editor/delete_comment/", params={"flow_id": flow_id, "comment_id": comment_id})
    assert deleted.status_code == 200, deleted.text
    assert client.get("/flow_data/v2", params={"flow_id": flow_id}).json()["comments"] == []


def test_update_layout_persists_comment_bounds():
    flow_id = make_flow(961)
    comment_id = client.post(
        "/editor/create_comment/", params={"flow_id": flow_id}, json={"text": "n"}
    ).json()["comment"]["id"]
    resp = client.post(
        "/editor/update_layout/",
        params={"flow_id": flow_id},
        json={
            "comment_bounds": [
                {"comment_id": comment_id, "x_position": 10.0, "y_position": 20.0, "width": 500.0, "height": 300.0}
            ]
        },
    )
    assert resp.status_code == 200, resp.text
    stored = flow_file_handler.get_flow(flow_id)._comments[comment_id]
    assert (stored.x_position, stored.y_position, stored.width, stored.height) == (10.0, 20.0, 500.0, 300.0)


def test_update_unknown_comment_returns_404():
    flow_id = make_flow(962)
    resp = client.post(
        "/editor/update_comment/", params={"flow_id": flow_id, "comment_id": 12345}, json={"text": "x"}
    )
    assert resp.status_code == 404
