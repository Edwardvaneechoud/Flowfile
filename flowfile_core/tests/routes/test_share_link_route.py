"""GET /editor/share_link — auth, 404, and the response contract the UI codes against."""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.routes.routes import flow_file_handler
from flowfile_core.schemas import input_schema, transform_schema

SHARE_LINK_URL = "/editor/share_link"


def get_authed_client() -> TestClient:
    with TestClient(main.app) as client:
        token = client.post("/auth/token").json()["access_token"]
    authed = TestClient(main.app)
    authed.headers = {"Authorization": f"Bearer {token}"}
    return authed


unauthed_client = TestClient(main.app)
authed_client = get_authed_client()


@pytest.fixture()
def shared_flow(tmp_path):
    """A two-node flow: a local read feeding an advanced (placeholder) filter.

    Two conditions, so the expression is not translatable into a basic filter
    and the node stays a placeholder — which is what the assertions below read.
    """
    flow_id = flow_file_handler.add_flow(name="share_route", flow_path=str(tmp_path / "share.yaml"), user_id=1)
    graph = flow_file_handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="read"))
    graph.add_read(
        input_schema.NodeRead(
            flow_id=flow_id,
            node_id=1,
            received_file=input_schema.ReceivedTable(
                name="sales.csv",
                path=str(tmp_path / "sales.csv"),
                file_type="csv",
                table_settings=input_schema.InputCsvTable(),
            ),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="filter"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter='[amount] > 10 and [region] = "EU"'),
        )
    )
    yield flow_id
    flow_file_handler.delete_flow(flow_id)


def test_route_requires_auth():
    assert unauthed_client.get(SHARE_LINK_URL, params={"flow_id": 1}).status_code == 401


def test_unknown_flow_returns_404():
    response = authed_client.get(SHARE_LINK_URL, params={"flow_id": 987654})
    assert response.status_code == 404
    assert response.json()["detail"] == "could not find the flow"


def test_route_is_registered_without_a_trailing_slash():
    """The frontend must call the exact path: a trailing slash costs a silent 307."""
    assert SHARE_LINK_URL in {route.path for route in main.app.routes}
    assert f"{SHARE_LINK_URL}/" not in {route.path for route in main.app.routes}
    redirected = authed_client.get(f"{SHARE_LINK_URL}/", params={"flow_id": 1}, follow_redirects=False)
    assert redirected.status_code == 307


def test_share_link_response_contract(shared_flow):
    response = authed_client.get(SHARE_LINK_URL, params={"flow_id": shared_flow})
    assert response.status_code == 200
    body = response.json()

    assert set(body) == {
        "url",
        "hash_chars",
        "compatible",
        "nodes_report",
        "warnings",
        "placeholder_count",
        "local_file_nodes",
    }
    assert body["url"].startswith("https://demo.flowfile.org/designer#flow=")
    assert body["hash_chars"] > 0
    assert body["compatible"] is False
    assert body["placeholder_count"] == 1
    assert body["local_file_nodes"] == [1]

    reports = {row["node_id"]: row for row in body["nodes_report"]}
    assert reports[1]["status"] == "supported"
    assert reports[1]["node_type"] == "read"
    assert reports[1]["reason"] is None
    assert reports[2]["status"] == "placeholder"
    assert "executable code" in reports[2]["reason"]


def test_share_link_never_carries_the_filter_expression(shared_flow):
    from flowfile_core.flowfile.share import decode_share_hash

    body = authed_client.get(SHARE_LINK_URL, params={"flow_id": shared_flow}).json()
    envelope = decode_share_hash(body["url"])
    assert "[amount] > 10" not in str(envelope)
    assert envelope["v"] == 1


def test_a_translatable_filter_makes_the_flow_compatible(tmp_path):
    """A one-comparison filter is reported as travelling, not as a placeholder."""
    flow_id = flow_file_handler.add_flow(name="share_route_basic", flow_path=str(tmp_path / "basic.yaml"), user_id=1)
    graph = flow_file_handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"amount": 5}, {"amount": 20}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="filter"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=flow_id,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(mode="advanced", advanced_filter="[amount] > 10"),
        )
    )
    try:
        body = authed_client.get(SHARE_LINK_URL, params={"flow_id": flow_id}).json()
        assert body["compatible"] is True
        assert body["placeholder_count"] == 0
        assert {row["node_id"]: row["status"] for row in body["nodes_report"]} == {1: "supported", 2: "supported"}
    finally:
        flow_file_handler.delete_flow(flow_id)


def test_building_a_link_never_saves_the_flow(shared_flow, tmp_path):
    """Sharing serialises the in-memory graph only — it is not a save path."""
    graph = flow_file_handler.get_flow(shared_flow)
    saved_file = tmp_path / "share.yaml"
    before_dirty = graph.has_unsaved_changes()
    before_mtime = saved_file.stat().st_mtime_ns if saved_file.exists() else None

    assert authed_client.get(SHARE_LINK_URL, params={"flow_id": shared_flow}).status_code == 200

    assert graph.has_unsaved_changes() == before_dirty
    after_mtime = saved_file.stat().st_mtime_ns if saved_file.exists() else None
    assert after_mtime == before_mtime
