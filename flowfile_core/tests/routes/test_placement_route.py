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
        "cached_node_ids": [],
        "unchecked_backend_ids": [],
        "assignment": {"1": registry.LOCAL_BACKEND_ID},
        "fallbacks": [
            {
                "node_id": 1,
                "backend_id": BACKEND_ID,
                "fallback_id": registry.LOCAL_BACKEND_ID,
                "reason": "it reads http URLs",
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

    assert response.json() == {
        "available": False, "cached_node_ids": [], "unchecked_backend_ids": [],
        "assignment": {}, "fallbacks": [], "refusals": [], "boundaries": []
    }


@pytest.mark.parametrize("cache_enabled,state", [(False, "materialised"), (True, None), (True, "deferred")])
def test_cache_check_skips_ineligible_nodes(client, http_read_flow, monkeypatch, cache_enabled, state):
    from types import SimpleNamespace

    graph, _ = http_read_flow
    node = graph.get_node(1)
    node.node_settings.cache_results = cache_enabled
    node.results.result_ref = None if state is None else SimpleNamespace(state=state)
    monkeypatch.setattr(node, "held_result", lambda _: pytest.fail("must not hash an ineligible node"))
    # Placement has its own purpose; the badge must not compute it again.
    placement = graph.preview_placement()
    calls = []

    def preview(*, probe=True):
        calls.append(True)
        return placement

    monkeypatch.setattr(graph, "preview_placement", preview)
    response = client.get("/flow/placement", params={"flow_id": FLOW_ID})
    assert response.status_code == 200
    assert response.json()["cached_node_ids"] == []
    assert calls == [True]


def test_cache_error_does_not_break_placement(client, http_read_flow, monkeypatch):
    from types import SimpleNamespace

    graph, _ = http_read_flow
    node = graph.get_node(1)
    node.node_settings.cache_results = True
    node.results.result_ref = SimpleNamespace(state="materialised")

    def broken_cache(_):
        raise RuntimeError("cache lookup failed")

    monkeypatch.setattr(node, "is_cached_on", broken_cache)
    response = client.get("/flow/placement", params={"flow_id": FLOW_ID})
    assert response.status_code == 200
    assert response.json()["available"] is True
    assert response.json()["cached_node_ids"] == []


@pytest.mark.parametrize("warm", [False, True])
def test_network_free_preview_uses_only_known_verdicts(client, http_read_flow, monkeypatch, warm):
    graph, _ = http_read_flow
    if warm:
        expected = client.get("/flow/placement", params={"flow_id": FLOW_ID}).json()
    else:
        expected = None

    def no_live_check(_):
        pytest.fail("A network-free preview must not check workers")

    monkeypatch.setattr(handshake, "verdict_for", no_live_check)
    response = client.get("/flow/placement", params={"flow_id": FLOW_ID, "probe": False})
    assert response.status_code == 200
    data = response.json()
    if warm:
        assert data == expected
    else:
        assert data["assignment"] == {"1": BACKEND_ID}
        assert BACKEND_ID in data["unchecked_backend_ids"]
        assert data["fallbacks"] == []
        assert data["refusals"] == []


def test_rejected_backend_preview_requests_recheck_and_recovers(client, http_read_flow):
    _, backend = http_read_flow
    compatible = backend.declared
    backend.declared = compatible.model_copy(update={"polars_version": "0.0.0"})
    handshake.reset_handshake()
    params = {"flow_id": FLOW_ID}
    assert client.get("/flow/placement", params=params).json()["refusals"]

    backend.declared = compatible
    asked = backend.asked
    preview = client.get("/flow/placement", params={**params, "probe": False})
    assert preview.status_code == 200
    assert preview.json()["refusals"]
    assert preview.json()["unchecked_backend_ids"] == []
    assert backend.asked == asked  # The fast response itself must stay network-free.

    live = client.get("/flow/placement", params=params)
    assert live.status_code == 200
    assert live.json()["refusals"] == []
    assert live.json()["unchecked_backend_ids"] == []
    assert backend.asked > asked


def test_unreachable_backend_preview_matches_live_without_refusals(client, http_read_flow):
    from flowfile_core.flowfile.execution.errors import ComputeUnavailable

    _, backend = http_read_flow
    backend.declared = ComputeUnavailable("worker down", backend_id=BACKEND_ID)
    params = {"flow_id": FLOW_ID}
    live = client.get("/flow/placement", params=params).json()
    assert live["refusals"] == []
    asked = backend.asked
    preview = client.get("/flow/placement", params={**params, "probe": False}).json()
    assert preview == live
    assert backend.asked == asked


def test_an_unknown_flow_is_not_found(client):
    assert client.get("/flow/placement", params={"flow_id": 999998}).status_code == 404
