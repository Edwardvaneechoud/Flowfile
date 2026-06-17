"""End-to-end HTTP tests for hosting flows as data APIs.

Unlike test_flow_api.py (which calls route functions directly with an in-memory DB),
these drive the *real* FastAPI app over HTTP with ``TestClient``: log in for a JWT,
register a flow (manual_input -> filter on ``${region}`` -> api_response), publish it,
mint an API key, then call ``GET /api/data/{slug}`` with the ``X-API-Key`` header and
assert on the JSON — exactly how an external client would use it.

In the default test session the public run actually offloads to the worker (the
session-autouse ``flowfile_worker`` fixture starts one); with ``SKIP_WORKER_TESTS=1`` it
falls back to an in-core collect. The asserted data is identical either way.

Run it:
    SKIP_WORKER_TESTS=1 poetry run pytest flowfile_core/tests/test_flow_api_e2e.py -v
"""

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.schemas import FlowParameter

# Fixtures / helpers


@pytest.fixture(scope="module")
def client():
    """One authenticated, context-managed TestClient for the whole module.

    Module scope on purpose: the public route holds a module-level ``asyncio.Semaphore``
    that binds to the event loop it's first used on, so every request must share a single
    loop — a fresh client per call would raise "bound to a different event loop". Electron
    mode means ``POST /auth/token`` needs no credentials and yields the ``local_user`` JWT.
    """
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
        c.headers.update({"Authorization": f"Bearer {token}"})
        yield c


def _build_and_save_flow(path, flow_id: int, orientation: str = "records") -> None:
    """manual_input -> polars filter on ${region} -> api_response, saved to *path*."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="api_flow",
            path=str(path),
            execution_mode="Development",
            execution_location="local",
        )
    )
    graph = handler.get_flow(flow_id)
    graph.flow_settings.parameters = [FlowParameter(name="region", default_value="EU")]

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"region": "EU", "v": 1}, {"region": "US", "v": 2}]),
        )
    )

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="polars_code"))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.filter(pl.col('region') == '${region}')"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=3, node_type="api_response"))
    graph.add_api_response(
        input_schema.NodeApiResponse(flow_id=flow_id, node_id=3, depending_on_id=2, orientation=orientation)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))

    graph.save_flow(str(path))


def _register_flow(flow_path) -> int:
    """Insert a FlowRegistration owned by local_user (id=1) into the test DB the routes use."""
    with get_db_context() as db:
        reg = db_models.FlowRegistration(
            flow_uuid=str(uuid4()),
            name="api_flow",
            flow_path=str(flow_path),
            owner_id=1,
            is_api_compatible=True,
        )
        db.add(reg)
        db.commit()
        db.refresh(reg)
        return reg.id


def _publish(client, tmp_path, slug, flow_id, parameters=None):
    """Build + register a flow and publish it as an endpoint. Returns the endpoint JSON."""
    flow_path = tmp_path / f"{slug}.yaml"
    _build_and_save_flow(flow_path, flow_id)
    reg_id = _register_flow(flow_path)
    body = {"registration_id": reg_id, "slug": slug}
    if parameters is not None:
        body["parameters"] = parameters
    resp = client.post("/flow-api/endpoints", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


_REGION_PARAM = [{"name": "region", "type": "string"}]


# Tests


def test_publish_key_and_call_with_param_filtering(client, tmp_path):
    """The headline flow: publish -> mint key -> call with a parameter over real HTTP."""
    endpoint = _publish(client, tmp_path, "sales", flow_id=11, parameters=_REGION_PARAM)
    assert endpoint["slug"] == "sales"
    assert endpoint["path"] == "/api/data/sales"

    created = client.post(f"/flow-api/endpoints/{endpoint['id']}/keys", json={"name": "prod"})
    assert created.status_code == 201, created.text
    api_key = created.json()["api_key"]
    assert api_key.startswith("ffk_")

    us = client.get("/api/data/sales", params={"region": "US"}, headers={"X-API-Key": api_key})
    assert us.status_code == 200, us.text
    assert us.json()["row_count"] == 1
    assert us.json()["data"] == [{"region": "US", "v": 2}]

    # Same key, different parameter value -> the filter substitutes the new value.
    eu = client.get("/api/data/sales", params={"region": "EU"}, headers={"X-API-Key": api_key})
    assert eu.status_code == 200
    assert eu.json()["data"] == [{"region": "EU", "v": 1}]


def test_public_endpoint_requires_a_valid_key(client, tmp_path):
    endpoint = _publish(client, tmp_path, "sales-auth", flow_id=12, parameters=_REGION_PARAM)
    client.post(f"/flow-api/endpoints/{endpoint['id']}/keys", json={"name": "k"})

    no_key = client.get("/api/data/sales-auth", params={"region": "US"})
    assert no_key.status_code == 401

    wrong = client.get("/api/data/sales-auth", params={"region": "US"}, headers={"X-API-Key": "ffk_wrong"})
    assert wrong.status_code == 401


def test_disabled_endpoint_returns_403(client, tmp_path):
    endpoint = _publish(client, tmp_path, "sales-disabled", flow_id=13, parameters=_REGION_PARAM)
    api_key = client.post(f"/flow-api/endpoints/{endpoint['id']}/keys", json={"name": "k"}).json()["api_key"]

    ok = client.get("/api/data/sales-disabled", params={"region": "US"}, headers={"X-API-Key": api_key})
    assert ok.status_code == 200

    upd = client.put(f"/flow-api/endpoints/{endpoint['id']}", json={"enabled": False})
    assert upd.status_code == 200

    resp = client.get("/api/data/sales-disabled", params={"region": "US"}, headers={"X-API-Key": api_key})
    assert resp.status_code == 403
    assert "disabled" in resp.json()["detail"].lower()


def test_consumer_key_grants_and_revokes_access(client, tmp_path):
    """A consumer's key can call every granted flow; revoking the grant cuts it off."""
    endpoint = _publish(client, tmp_path, "sales-consumer", flow_id=14, parameters=_REGION_PARAM)
    eid = endpoint["id"]

    consumer = client.post("/api-consumers", json={"name": "partner-acme"})
    assert consumer.status_code == 201, consumer.text
    cid = consumer.json()["id"]

    grant = client.post(f"/api-consumers/{cid}/endpoints", json={"endpoint_id": eid})
    assert grant.status_code == 201, grant.text

    api_key = client.post(f"/api-consumers/{cid}/keys", json={"name": "prod"}).json()["api_key"]
    ok = client.get("/api/data/sales-consumer", params={"region": "US"}, headers={"X-API-Key": api_key})
    assert ok.status_code == 200
    assert ok.json()["data"] == [{"region": "US", "v": 2}]

    # Revoke the grant -> the same key can no longer reach this endpoint.
    rev = client.delete(f"/api-consumers/{cid}/endpoints/{eid}")
    assert rev.status_code == 204
    denied = client.get("/api/data/sales-consumer", params={"region": "US"}, headers={"X-API-Key": api_key})
    assert denied.status_code == 401


def test_missing_required_parameter_returns_400(client, tmp_path):
    endpoint = _publish(
        client,
        tmp_path,
        "sales-required",
        flow_id=15,
        parameters=[{"name": "region", "type": "string", "required": True}],
    )
    api_key = client.post(f"/flow-api/endpoints/{endpoint['id']}/keys", json={"name": "k"}).json()["api_key"]

    resp = client.get("/api/data/sales-required", headers={"X-API-Key": api_key})
    assert resp.status_code == 400
