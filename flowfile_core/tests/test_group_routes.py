"""
Route tests for the node-group editor endpoints.

Run with:
    pytest flowfile_core/tests/test_group_routes.py -v
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
    """Register a flow with two manual_input nodes (ids 1 and 2)."""
    if flow_file_handler.get_flow(flow_id) is not None:
        flow_file_handler.delete_flow(flow_id)
    flow_file_handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name="grp", path="."))
    graph = flow_file_handler.get_flow(flow_id)
    for node_id, value in ((1, 1), (2, 2)):
        graph.add_node_promise(
            input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type="manual_input")
        )
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=flow_id, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist([{"a": value}])
            )
        )
    return flow_id


def test_create_update_delete_group():
    flow_id = make_flow(950)

    created = client.post(
        "/editor/create_group/",
        params={"flow_id": flow_id},
        json={"node_ids": [1, 2], "name": "Cleaning", "color": "blue"},
    )
    assert created.status_code == 200, created.text
    body = created.json()
    assert body["success"] is True
    assert body["group"]["name"] == "Cleaning"
    assert body["group"]["color"] == "blue"
    assert "history" in body
    group_id = body["group"]["id"]

    # Group + member group_ids appear in the VueFlow payload
    data = client.get("/flow_data/v2", params={"flow_id": flow_id}).json()
    assert len(data["groups"]) == 1
    assert data["groups"][0]["id"] == group_id
    assert all(node["group_id"] == group_id for node in data["node_inputs"])

    updated = client.post(
        "/editor/update_group/",
        params={"flow_id": flow_id, "group_id": group_id},
        json={"name": "Clean", "color": "green"},
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["group"]["name"] == "Clean"
    assert updated.json()["group"]["color"] == "green"

    deleted = client.post("/editor/delete_group/", params={"flow_id": flow_id, "group_id": group_id})
    assert deleted.status_code == 200, deleted.text
    after = client.get("/flow_data/v2", params={"flow_id": flow_id}).json()
    assert after["groups"] == []
    assert len(after["node_inputs"]) == 2  # nodes survive ungroup


def test_update_layout_persists_positions_and_bounds():
    flow_id = make_flow(951)
    group_id = client.post(
        "/editor/create_group/", params={"flow_id": flow_id}, json={"node_ids": [1, 2], "name": "G"}
    ).json()["group"]["id"]

    resp = client.post(
        "/editor/update_layout/",
        params={"flow_id": flow_id},
        json={
            "node_positions": [{"node_id": 1, "pos_x": 111.0, "pos_y": 222.0}],
            "group_bounds": [
                {"group_id": group_id, "x_position": 10.0, "y_position": 20.0, "width": 500.0, "height": 300.0}
            ],
        },
    )
    assert resp.status_code == 200, resp.text
    graph = flow_file_handler.get_flow(flow_id)
    assert graph.get_node(1).setting_input.pos_x == 111.0
    assert graph.get_node(1).setting_input.pos_y == 222.0
    assert graph._groups[group_id].width == 500.0


def test_add_and_remove_group_members():
    flow_id = make_flow(952)
    group_id = client.post(
        "/editor/create_group/", params={"flow_id": flow_id}, json={"node_ids": [1], "name": "G"}
    ).json()["group"]["id"]

    added = client.post(
        "/editor/group/add_nodes/", params={"flow_id": flow_id, "group_id": group_id}, json={"node_ids": [2]}
    )
    assert added.status_code == 200, added.text
    graph = flow_file_handler.get_flow(flow_id)
    assert sorted(graph._member_node_ids(group_id)) == [1, 2]

    removed = client.post(
        "/editor/group/remove_nodes/", params={"flow_id": flow_id}, json={"node_ids": [1, 2]}
    )
    assert removed.status_code == 200, removed.text
    assert group_id not in graph._groups  # pruned when emptied


def test_update_group_collapse():
    flow_id = make_flow(955)
    group_id = client.post(
        "/editor/create_group/", params={"flow_id": flow_id}, json={"node_ids": [1, 2], "name": "G"}
    ).json()["group"]["id"]

    resp = client.post(
        "/editor/update_group/", params={"flow_id": flow_id, "group_id": group_id}, json={"collapsed": True}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["group"]["collapsed"] is True
    assert flow_file_handler.get_flow(flow_id)._groups[group_id].collapsed is True


def test_update_unknown_group_returns_404():
    flow_id = make_flow(953)
    resp = client.post(
        "/editor/update_group/", params={"flow_id": flow_id, "group_id": 999}, json={"name": "x"}
    )
    assert resp.status_code == 404


def test_update_layout_record_history_false_skips_undo_entry():
    flow_id = make_flow(956)
    created = client.post(
        "/editor/create_group/", params={"flow_id": flow_id}, json={"node_ids": [1, 2], "name": "G"}
    ).json()
    group_id = created["group"]["id"]
    undo_after_create = created["history"]["undo_count"]

    folded = client.post(
        "/editor/update_layout/",
        params={"flow_id": flow_id},
        json={
            "group_bounds": [
                {"group_id": group_id, "x_position": 1.0, "y_position": 2.0, "width": 300.0, "height": 200.0}
            ],
            "record_history": False,
        },
    )
    assert folded.status_code == 200, folded.text
    # Bounds applied, but no new undo step (folded into the create entry).
    assert folded.json()["history"]["undo_count"] == undo_after_create
    assert flow_file_handler.get_flow(flow_id)._groups[group_id].width == 300.0

    # Omitting the flag keeps the default: one new undo step.
    recorded = client.post(
        "/editor/update_layout/",
        params={"flow_id": flow_id},
        json={
            "group_bounds": [
                {"group_id": group_id, "x_position": 3.0, "y_position": 4.0, "width": 320.0, "height": 220.0}
            ]
        },
    )
    assert recorded.status_code == 200, recorded.text
    assert recorded.json()["history"]["undo_count"] == undo_after_create + 1


def test_create_group_rejected_while_running():
    flow_id = make_flow(954)
    flow_file_handler.get_flow(flow_id).flow_settings.is_running = True
    try:
        resp = client.post(
            "/editor/create_group/", params={"flow_id": flow_id}, json={"node_ids": [1], "name": "G"}
        )
        assert resp.status_code == 422
    finally:
        flow_file_handler.get_flow(flow_id).flow_settings.is_running = False
