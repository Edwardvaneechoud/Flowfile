"""``GET /flow/placement`` answers where a run would place each node now, running and changing nothing.

The designer reads it to show, before a run, the nodes that would run on the default backend because theirs cannot run
them, and the nodes no backend can. With the compute protocol off nothing is placed and it answers ``available`` false.
"""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import exchange, registry
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.schemas import input_schema, schemas
from tests.flowfile.execution import test_placement
from tests.flowfile.execution.test_placement import BACKEND_ID, DeclaringBackend, everything, without

csv_url = test_placement.csv_url
FLOW_ID = 8690


@pytest.fixture
def client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    authed = TestClient(main.app)
    authed.headers = {"Authorization": f"Bearer {token}"}
    return authed


@pytest.fixture
def http_read_flow(monkeypatch, csv_url):
    """Read node 1 over HTTP in an open flow on a backend that reads no ``http``, with an exchange store configured."""
    registry.reset()
    handshake.reset_handshake()
    monkeypatch.setenv("FLOWFILE_COMPUTE_BACKEND", BACKEND_ID)
    monkeypatch.setenv("FLOWFILE_EXCHANGE_URI", "s3://flowfile-test/exchange")
    monkeypatch.setattr(exchange, "build_s3_client", lambda *args, **kwargs: pytest.fail("no S3 client is made"))
    backend = DeclaringBackend(BACKEND_ID, without(everything(), scheme="http"))
    registry.register_backend(BACKEND_ID, lambda: backend)
    if flow_file_handler.get_flow(FLOW_ID) is not None:
        flow_file_handler.delete_flow(FLOW_ID)
    flow_file_handler.register_flow(
        schemas.FlowSettings(flow_id=FLOW_ID, name="placement_route", path=".", execution_location="auto"), user_id=1
    )
    graph = flow_file_handler.get_flow(FLOW_ID)
    graph.add_node_promise(input_schema.NodePromise(flow_id=FLOW_ID, node_id=1, node_type="read"))
    received = input_schema.ReceivedTable(path=csv_url, file_type="csv", table_settings={"encoding": "utf-8"})
    graph.add_read(input_schema.NodeRead(flow_id=FLOW_ID, node_id=1, received_file=received))
    yield graph, backend
    flow_file_handler.delete_flow(FLOW_ID)
    registry.reset()
    handshake.reset_handshake()


def test_it_answers_the_node_that_would_run_on_the_default_backend(client, http_read_flow):
    graph, backend = http_read_flow
    fingerprint = graph.get_node(1).settings_fingerprint()

    response = client.get("/flow/placement", params={"flow_id": FLOW_ID})

    assert response.status_code == 200, response.text
    assert response.json() == {
        "available": True,
        "assignment": {"1": registry.LOCAL_BACKEND_ID},
        "fallbacks": [
            {
                "node_id": 1,
                "backend_id": BACKEND_ID,
                "fallback_id": registry.LOCAL_BACKEND_ID,
                "reason": "it reads storage this backend does not read: http",
            }
        ],
        "refusals": [],
        "boundaries": [],
    }
    assert backend.work == []
    assert graph.get_node(1).settings_fingerprint() == fingerprint


def test_it_answers_unavailable_with_the_protocol_off(client, http_read_flow, monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", False)

    response = client.get("/flow/placement", params={"flow_id": FLOW_ID})

    assert response.json() == {"available": False, "assignment": {}, "fallbacks": [], "refusals": [], "boundaries": []}


def test_an_unknown_flow_is_not_found(client):
    assert client.get("/flow/placement", params={"flow_id": 999998}).status_code == 404
