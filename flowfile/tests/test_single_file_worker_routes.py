"""Single-file mode serves the worker's versioned routes and their WebSocket submit under ``/worker``.

``include_worker_routes`` is what ``extend_app`` adds to core's app; it runs here on a bare app so the
test does not change ``flowfile_core.main.app`` for the rest of the session.
"""

import json
import uuid

import polars as pl
import pytest
from fastapi import FastAPI, WebSocketDisconnect
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal
from starlette.testclient import WebSocketDenialResponse

from flowfile.web import include_worker_routes
from flowfile_worker import internal_token, models, status_dict, status_dict_lock
from flowfile_worker.protocol_routes import worker_capabilities
from shared.compute_protocol import (
    PROTOCOL_VERSION,
    PROTOCOL_VERSION_HEADER,
    Capabilities,
    SubmitResult,
)

TOKEN = "single-file-test-token"
TOKEN_HEADER = "X-Flowfile-Internal"
FLOW_ID = 4343


@pytest.fixture
def app(monkeypatch) -> FastAPI:
    """A bare app given the worker routes single-file mode mounts, with the worker's token set for this test.

    ``FLOWFILE_WORKER_LOCAL_FILES`` starts off and the mount turns it on; setting it here lets teardown restore it.
    """
    monkeypatch.setenv("FLOWFILE_INTERNAL_TOKEN", TOKEN)
    monkeypatch.setattr(internal_token, "_token", None)
    monkeypatch.setenv("FLOWFILE_WORKER_LOCAL_FILES", "0")
    app = FastAPI()
    include_worker_routes(app)
    return app


@pytest.fixture
def client(app) -> TestClient:
    return TestClient(app, headers={TOKEN_HEADER: TOKEN, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)})


@pytest.fixture
def held(client):
    """Result ids a test submits under; each is released when the test ends."""
    ids: list[str] = []

    def new_id() -> str:
        ids.append(uuid.uuid4().hex)
        return ids[-1]

    yield new_id
    for result_id in ids:
        client.delete(f"/worker/v1/results/{result_id}")


def _plan() -> pl.LazyFrame:
    return pl.LazyFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})


def _meta(result_id: str, *, must_materialise: bool, preview_rows: int | None = None) -> str:
    return json.dumps(
        {
            "hints": {"must_materialise": must_materialise, "wide": False, "fanout": 1, "preview_rows": preview_rows},
            "inputs": [],
            "result_id": result_id,
            "flow_id": FLOW_ID,
        }
    )


def test_capabilities_answer_under_worker_behind_the_token_and_the_version(app, client):
    response = client.get("/worker/v1/capabilities")

    assert response.status_code == 200
    assert Capabilities.model_validate(response.json()) == worker_capabilities()
    assert TestClient(app).get("/worker/v1/capabilities").status_code == 401
    foreign = TestClient(app, headers={TOKEN_HEADER: TOKEN, PROTOCOL_VERSION_HEADER: "2"})
    assert foreign.get("/worker/v1/capabilities").status_code == 426


def test_the_co_hosted_worker_declares_local_files(client):
    capabilities = Capabilities.model_validate(client.get("/worker/v1/capabilities").json())

    assert "file" in capabilities.read_schemes
    assert "file" in capabilities.write_schemes


def test_a_deprecated_task_route_under_worker_answers_with_the_header(client, tmp_path):
    task_id = uuid.uuid4().hex
    with status_dict_lock:
        status_dict[task_id] = models.Status(
            background_task_id=task_id, status="Completed", file_ref=str(tmp_path / "result.arrow")
        )
    try:
        response = client.get(f"/worker/kafka_offsets/{task_id}")
    finally:
        with status_dict_lock:
            status_dict.pop(task_id, None)

    assert response.status_code == 200
    assert response.headers["Deprecation"] == "true"


def test_a_materialised_submit_is_held_and_released_under_worker(client, held):
    result_id = held()

    response = client.post(
        "/worker/v1/submit",
        files={"plan": ("plan", _plan().serialize(), "application/octet-stream")},
        data={"meta": _meta(result_id, must_materialise=True)},
    )

    assert response.status_code == 200, response.text
    assert SubmitResult.model_validate_json(response.text).ref.state == "materialised"
    assert client.head(f"/worker/v1/results/{result_id}").status_code == 200
    assert client.delete(f"/worker/v1/results/{result_id}").status_code == 204


def test_a_deferred_websocket_submit_answers_under_worker_with_its_sample(client, held):
    result_id = held()
    plan = _plan()

    with client.websocket_connect("/worker/v1/ws/submit") as websocket:
        websocket.send_text(_meta(result_id, must_materialise=False, preview_rows=2))
        websocket.send_bytes(plan.serialize())
        while (frame := websocket.receive_json())["type"] == "progress":
            pass

    assert frame["type"] == "complete", frame
    result = SubmitResult.model_validate_json(json.dumps(frame["result"]))
    assert result.ref.result_id == result_id
    assert result.ref.state == "deferred"
    assert_frame_equal(pl.read_ipc_stream(result.sample), plan.head(2).collect())
    assert client.head(f"/worker/v1/results/{result_id}").status_code == 200


def test_the_websocket_submit_under_worker_denies_a_foreign_version_and_closes_on_a_bad_token(app):
    signed = TestClient(app, headers={TOKEN_HEADER: TOKEN})
    with pytest.raises(WebSocketDenialResponse) as denied:
        with signed.websocket_connect("/worker/v1/ws/submit", headers={PROTOCOL_VERSION_HEADER: "2"}):
            pass

    unsigned = TestClient(app, headers={TOKEN_HEADER: "not-the-token"})
    with pytest.raises(WebSocketDisconnect) as closed:
        with unsigned.websocket_connect("/worker/v1/ws/submit", headers={PROTOCOL_VERSION_HEADER: "2"}):
            pass

    assert denied.value.status_code == 426
    assert f"protocol version {PROTOCOL_VERSION}" in denied.value.json()["detail"]
    assert not isinstance(closed.value, WebSocketDenialResponse)
    assert closed.value.code == 1008
