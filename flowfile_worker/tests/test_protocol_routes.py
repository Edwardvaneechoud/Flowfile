"""The worker's ``/v1`` routes: submit into a held result reference, then sample, count, probe and release it."""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import polars as pl
import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal
from starlette.testclient import WebSocketDenialResponse

from flowfile_worker import main, protocol_routes, status_dict, status_dict_lock, streaming
from flowfile_worker.results import result_path
from shared.compute_protocol import (
    PROTOCOL_VERSION,
    PROTOCOL_VERSION_HEADER,
    Capabilities,
    SubmitResult,
    WorkerLocation,
    local_capabilities,
    schema_to_wire,
)
from tests.conftest import INTERNAL_AUTH_HEADERS

pytestmark = pytest.mark.worker

client = TestClient(main.app, headers={**INTERNAL_AUTH_HEADERS, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)})
token_only = TestClient(main.app, headers=INTERNAL_AUTH_HEADERS)

FLOW_ID = 4242
NODE_ID = 7
GEOARROW_WKB = pl.Extension("geoarrow.wkb", pl.Binary, "{}")

ROUTES = [
    ("GET", "/v1/capabilities"),
    ("POST", "/v1/submit"),
    ("GET", "/v1/results/some-id/sample?n=1"),
    ("GET", "/v1/results/some-id/count"),
    ("HEAD", "/v1/results/some-id"),
    ("DELETE", "/v1/results/some-id"),
    ("GET", "/v1/jobs/some-id"),
    ("POST", "/v1/jobs/some-id/cancel"),
]


def _plan() -> pl.LazyFrame:
    return pl.LazyFrame({"id": [1, 2, 3, 4, 5], "name": ["a", "b", "c", "d", "e"]})


def _typed_plan() -> pl.LazyFrame:
    return pl.LazyFrame(
        {
            "id": [1, 2, 3, 4],
            "at": pl.Series(
                [datetime(2026, 1, day, tzinfo=timezone.utc) for day in range(1, 5)],
                dtype=pl.Datetime("us", "Europe/Amsterdam"),
            ),
            "size": pl.Series(["small", "large", "small", "large"], dtype=pl.Enum(["small", "large"])),
            "geom": pl.Series([b"\x01", b"\x02", b"\x03", b"\x04"], dtype=GEOARROW_WKB),
        }
    )


def _meta(
    result_id: str,
    *,
    must_materialise: bool = False,
    wide: bool = False,
    fanout: int = 1,
    preview_rows: int | None = None,
    inputs: tuple[str, ...] = (),
) -> str:
    return json.dumps(
        {
            "hints": {
                "must_materialise": must_materialise,
                "wide": wide,
                "fanout": fanout,
                "preview_rows": preview_rows,
            },
            "inputs": list(inputs),
            "result_id": result_id,
            "flow_id": FLOW_ID,
            "node_id": NODE_ID,
        }
    )


def _post(plan: bytes, meta: str):
    return client.post("/v1/submit", files={"plan": ("plan", plan, "application/octet-stream")}, data={"meta": meta})


@pytest.fixture
def held():
    """Result ids a test submits under; each is released and its job entry dropped when the test ends."""
    ids: list[str] = []

    def new_id() -> str:
        ids.append(uuid.uuid4().hex)
        return ids[-1]

    yield new_id
    for result_id in ids:
        client.delete(f"/v1/results/{result_id}")
        with status_dict_lock:
            status_dict.pop(result_id, None)


def _submit(result_id: str, plan: pl.LazyFrame, **hints) -> SubmitResult:
    response = _post(plan.serialize(), _meta(result_id, **hints))
    assert response.status_code == 200, response.text
    return SubmitResult.model_validate_json(response.text)


def _location(result: SubmitResult) -> WorkerLocation:
    return WorkerLocation.model_validate_json(result.ref.location)


def test_the_token_is_checked_before_the_version():
    unsigned = TestClient(main.app)
    assert unsigned.get("/v1/capabilities", headers={PROTOCOL_VERSION_HEADER: "2"}).status_code == 401
    assert unsigned.get("/v1/capabilities").status_code == 401


@pytest.mark.parametrize("sent", ["2", None], ids=["foreign", "missing"])
@pytest.mark.parametrize(("method", "path"), ROUTES, ids=[f"{m} {p}" for m, p in ROUTES])
def test_every_route_answers_426_to_a_foreign_or_missing_version(method, path, sent):
    headers = {PROTOCOL_VERSION_HEADER: sent} if sent is not None else {}

    response = token_only.request(method, path, headers=headers)

    assert response.status_code == 426
    if method != "HEAD":
        detail = response.json()["detail"]
        assert f"protocol version {PROTOCOL_VERSION}" in detail
        assert f"carries {sent if sent is not None else 'none'}" in detail


def test_the_v1_paths_are_in_the_openapi_document():
    paths = main.app.openapi()["paths"]

    assert {path for path in paths if path.startswith("/v1/")} == {
        "/v1/capabilities",
        "/v1/submit",
        "/v1/results/{result_id}/sample",
        "/v1/results/{result_id}/count",
        "/v1/results/{result_id}",
        "/v1/jobs/{job_id}",
        "/v1/jobs/{job_id}/cancel",
    }
    assert set(paths["/v1/results/{result_id}"]) == {"head", "delete"}


def test_capabilities_reports_this_process():
    response = client.get("/v1/capabilities")

    assert response.status_code == 200
    assert Capabilities.model_validate(response.json()) == local_capabilities()


@pytest.mark.parametrize(
    ("must_materialise", "wide", "fanout", "preview_rows", "state"),
    [
        (True, False, 1, None, "materialised"),
        (False, True, 1, 100, "materialised"),
        (False, True, 1, None, "deferred"),
        (False, False, 1, 100, "deferred"),
        (False, False, 3, 100, "deferred"),
    ],
)
def test_the_hints_decide_whether_a_result_is_written(held, must_materialise, wide, fanout, preview_rows, state):
    result_id = held()

    result = _submit(
        result_id, _plan(), must_materialise=must_materialise, wide=wide, fanout=fanout, preview_rows=preview_rows
    )

    assert result.ref.state == state
    assert result_path(FLOW_ID, result_id).is_file() == (state == "materialised")


def test_a_materialised_submit_writes_the_file_and_answers_the_plans_columns_and_sample(held):
    result_id = held()
    plan = _typed_plan()

    result = _submit(result_id, plan, must_materialise=True, preview_rows=2)

    assert result.job_id == result_id
    assert result.ref.result_id == result_id
    assert result.ref.state == "materialised"
    assert result.ref.row_count == 4
    assert result.ref.columns == schema_to_wire(plan.collect_schema())
    assert result.ref.columns[3].data_type == "Extension('geoarrow.wkb', Binary, '{}')"
    location = _location(result)
    assert location.kind == "file"
    assert location.path == str(result_path(FLOW_ID, result_id))
    assert os.path.isfile(location.path)
    sample = pl.read_ipc_stream(result.sample)
    expected = plan.head(2).collect()
    assert sample.height == 2
    assert_frame_equal(sample.drop("geom"), expected.drop("geom"))
    assert sample["geom"].to_list() == expected["geom"].to_list()


def test_a_deferred_submit_keeps_the_plan_and_writes_nothing(held):
    result_id = held()
    plan = _typed_plan()
    plan_bytes = plan.serialize()

    response = _post(plan_bytes, _meta(result_id, preview_rows=3))

    assert response.status_code == 200, response.text
    result = SubmitResult.model_validate_json(response.text)
    assert result.ref.state == "deferred"
    assert result.ref.row_count is None
    assert result.ref.columns == schema_to_wire(plan.collect_schema())
    location = _location(result)
    assert location.kind == "plan"
    assert location.path is None
    assert location.plan == plan_bytes
    assert not result_path(FLOW_ID, result_id).exists()
    sample = pl.read_ipc_stream(result.sample)
    expected = plan.head(3).collect()
    assert_frame_equal(sample.drop("geom"), expected.drop("geom"))


@pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred"])
def test_no_sample_comes_back_without_preview_rows(held, must_materialise):
    result = _submit(held(), _plan(), must_materialise=must_materialise, preview_rows=None)

    assert result.sample is None


def test_a_plan_over_a_megabyte_submits_as_a_file_part(held):
    plan = pl.LazyFrame({"x": list(range(200_000))})
    plan_bytes = plan.serialize()
    assert len(plan_bytes) > 2**20

    response = _post(plan_bytes, _meta(held(), must_materialise=True))

    assert response.status_code == 200, response.text
    assert SubmitResult.model_validate_json(response.text).ref.row_count == 200_000


def test_a_plan_reads_a_held_input(held):
    first_id = held()
    first = _submit(first_id, _plan(), must_materialise=True)
    chained = pl.scan_ipc(_location(first).path).filter(pl.col("id") > 2)

    second = _submit(held(), chained, must_materialise=True, inputs=(first_id,))

    assert second.ref.row_count == 3


def test_an_input_that_is_not_held_is_refused_before_anything_runs(held):
    result_id = held()

    response = _post(_plan().serialize(), _meta(result_id, must_materialise=True, inputs=("never-submitted",)))

    assert response.status_code == 404
    assert "never-submitted" in response.json()["detail"]
    assert result_id not in status_dict


@pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred"])
def test_sample_and_count_of_a_held_result(held, must_materialise):
    result_id = held()
    plan = _plan()
    _submit(result_id, plan, must_materialise=must_materialise)
    jobs_before = set(status_dict)

    three = client.get(f"/v1/results/{result_id}/sample", params={"n": 3})
    everything = client.get(f"/v1/results/{result_id}/sample", params={"n": 50})
    count = client.get(f"/v1/results/{result_id}/count")

    assert three.status_code == 200
    assert three.headers["content-type"] == "application/vnd.apache.arrow.stream"
    assert_frame_equal(pl.read_ipc_stream(three.content), plan.head(3).collect())
    assert_frame_equal(pl.read_ipc_stream(everything.content), plan.collect())
    assert count.json() == {"row_count": 5}
    assert set(status_dict) == jobs_before


def test_sample_and_count_of_an_unknown_result_are_404():
    assert client.get("/v1/results/unknown/sample", params={"n": 1}).status_code == 404
    assert client.get("/v1/results/unknown/count").status_code == 404


def test_release_removes_the_file_the_record_and_the_job(held):
    result_id = held()
    path = _location(_submit(result_id, _plan(), must_materialise=True)).path

    assert client.head(f"/v1/results/{result_id}").status_code == 200
    assert client.delete(f"/v1/results/{result_id}").status_code == 204
    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert client.delete(f"/v1/results/{result_id}").status_code == 404
    assert not os.path.exists(path)
    assert result_id not in status_dict


def test_a_result_whose_file_is_gone_is_not_held(held):
    result_id = held()
    os.remove(_location(_submit(result_id, _plan(), must_materialise=True)).path)

    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert client.get(f"/v1/results/{result_id}/count").status_code == 404


def test_a_plan_that_fails_on_its_data_is_a_task_failure_and_holds_nothing(held):
    result_id = held()

    response = _post(pl.LazyFrame({"a": [1]}).select("missing").serialize(), _meta(result_id, must_materialise=True))

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["status"] == "Error"
    assert detail["error_kind"] == "task"
    assert detail["error_message"].startswith("ColumnNotFoundError")
    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert not result_path(FLOW_ID, result_id).exists()


def test_a_plan_this_worker_cannot_load_is_an_environment_failure(held, unloadable_plan):
    response = _post(unloadable_plan, _meta(held(), must_materialise=True))

    assert response.status_code == 500
    assert response.json()["detail"]["error_kind"] == "environment"


def test_bytes_that_are_not_a_plan_are_an_environment_failure(held):
    response = _post(b"not a plan", _meta(held(), preview_rows=10))

    assert response.status_code == 500
    assert response.json()["detail"]["error_kind"] == "environment"


@pytest.mark.parametrize(
    "meta",
    [
        "{not json",
        json.dumps({"hints": {"must_materialise": True, "wide": False, "fanout": 1, "preview_rows": None}}),
        json.dumps({**json.loads(_meta("abc")), "sink": "parquet"}),
        _meta("../escape"),
        _meta("a/b"),
    ],
    ids=["not-json", "missing-fields", "unknown-field", "dot-dot", "slash"],
)
def test_a_bad_meta_part_is_422(meta):
    response = _post(_plan().serialize(), meta)

    assert response.status_code == 422


@pytest.mark.parametrize(
    ("must_materialise", "wide"),
    [(True, False), (False, True), (False, False)],
    ids=["must-materialise", "wide", "deferred"],
)
def test_a_negative_preview_rows_is_refused_before_anything_runs(held, must_materialise, wide):
    result_id = held()

    response = _post(
        _plan().serialize(), _meta(result_id, must_materialise=must_materialise, wide=wide, preview_rows=-2)
    )

    assert response.status_code == 422
    assert "preview_rows" in response.text
    assert result_id not in status_dict
    assert not result_path(FLOW_ID, result_id).exists()


def test_a_submits_job_is_its_result_id(held):
    result_id = held()
    _submit(result_id, _plan(), must_materialise=True)

    job = client.get(f"/v1/jobs/{result_id}")

    assert job.status_code == 200
    assert job.json()["status"] == "Completed"
    assert job.json()["results"] is None


def test_a_failed_submits_job_carries_the_error(held):
    result_id = held()
    _post(pl.LazyFrame({"a": [1]}).select("missing").serialize(), _meta(result_id))

    job = client.get(f"/v1/jobs/{result_id}").json()

    assert job["status"] == "Error"
    assert job["error_message"].startswith("ColumnNotFoundError")


def test_an_unknown_job_is_404():
    assert client.get("/v1/jobs/unknown").status_code == 404
    assert client.post("/v1/jobs/unknown/cancel").status_code == 404


def test_a_finished_job_cannot_be_cancelled(held):
    result_id = held()
    _submit(result_id, _plan())

    assert client.post(f"/v1/jobs/{result_id}/cancel").status_code == 404


def _ws_submit(plan: bytes, meta: str) -> tuple[list[dict], dict, int]:
    """Send *meta* and *plan* over ``/v1/ws/submit``; answer the progress frames, the final frame and the close code."""
    progress = []
    with client.websocket_connect("/v1/ws/submit") as websocket:
        websocket.send_text(meta)
        websocket.send_bytes(plan)
        while (frame := websocket.receive_json())["type"] == "progress":
            progress.append(frame)
        with pytest.raises(WebSocketDisconnect) as closed:
            websocket.receive_text()
    return progress, frame, closed.value.code


@pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred"])
def test_a_websocket_submit_streams_progress_then_the_result_with_its_sample(held, must_materialise):
    result_id = held()
    plan = _plan()
    plan_bytes = plan.serialize()

    progress, final, close_code = _ws_submit(
        plan_bytes, _meta(result_id, must_materialise=must_materialise, preview_rows=3)
    )

    assert progress
    assert all(0 <= frame["progress"] <= 100 for frame in progress)
    assert final["type"] == "complete"
    result = SubmitResult.model_validate_json(json.dumps(final["result"]))
    assert result.job_id == result_id
    assert result.ref.result_id == result_id
    assert result.ref.state == ("materialised" if must_materialise else "deferred")
    assert result.ref.row_count == (5 if must_materialise else None)
    assert result.ref.columns == schema_to_wire(plan.collect_schema())
    location = _location(result)
    if must_materialise:
        assert location.kind == "file"
        assert location.path == str(result_path(FLOW_ID, result_id))
        assert os.path.isfile(location.path)
    else:
        assert location.kind == "plan"
        assert location.plan == plan_bytes
        assert not result_path(FLOW_ID, result_id).exists()
    assert_frame_equal(pl.read_ipc_stream(result.sample), plan.head(3).collect())
    assert close_code == 1000
    assert client.head(f"/v1/results/{result_id}").status_code == 200


@pytest.mark.parametrize("sent", ["2", None], ids=["foreign", "missing"])
def test_the_websocket_submit_is_denied_426_before_accepting(sent):
    headers = {PROTOCOL_VERSION_HEADER: sent} if sent is not None else {}

    with pytest.raises(WebSocketDenialResponse) as denied:
        with token_only.websocket_connect("/v1/ws/submit", headers=headers):
            pass

    assert denied.value.status_code == 426
    detail = denied.value.json()["detail"]
    assert f"protocol version {PROTOCOL_VERSION}" in detail
    assert f"carries {sent if sent is not None else 'none'}" in detail


@pytest.mark.parametrize("token", [None, "not-the-token"], ids=["missing", "wrong"])
def test_the_websocket_submit_closes_1008_on_a_bad_token_before_the_version_is_checked(token):
    headers = {PROTOCOL_VERSION_HEADER: "2"}
    if token is not None:
        headers["X-Flowfile-Internal"] = token

    with pytest.raises(WebSocketDisconnect) as closed:
        with TestClient(main.app).websocket_connect("/v1/ws/submit", headers=headers):
            pass

    assert not isinstance(closed.value, WebSocketDenialResponse)
    assert closed.value.code == 1008


def test_a_bad_meta_frame_is_an_error_frame_422():
    progress, final, close_code = _ws_submit(_plan().serialize(), "{}")

    assert progress == []
    assert final["type"] == "error"
    assert final["status_code"] == 422
    assert close_code == 1000


def test_an_input_that_is_not_held_is_an_error_frame_404(held):
    result_id = held()

    _, final, close_code = _ws_submit(
        _plan().serialize(), _meta(result_id, must_materialise=True, inputs=("never-submitted",))
    )

    assert final["type"] == "error"
    assert final["status_code"] == 404
    assert "never-submitted" in final["detail"]
    assert result_id not in status_dict
    assert close_code == 1000


def test_a_plan_that_fails_on_its_data_is_an_error_frame_500_and_holds_nothing(held):
    result_id = held()

    _, final, close_code = _ws_submit(
        pl.LazyFrame({"a": [1]}).select("missing").serialize(), _meta(result_id, must_materialise=True)
    )

    assert final["type"] == "error"
    assert final["status_code"] == 500
    assert final["detail"]["status"] == "Error"
    assert final["detail"]["error_kind"] == "task"
    assert final["detail"]["error_message"].startswith("ColumnNotFoundError")
    assert close_code == 1000
    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert not result_path(FLOW_ID, result_id).exists()


def test_an_unchanged_progress_is_sent_again_as_a_heartbeat(held, monkeypatch):
    monkeypatch.setattr(streaming, "_HEARTBEAT_INTERVAL", 0)

    progress, final, _ = _ws_submit(_plan().serialize(), _meta(held(), must_materialise=True))

    assert final["type"] == "complete"
    values = [frame["progress"] for frame in progress]
    assert len(values) > len(set(values))


def test_a_client_that_leaves_does_not_stop_the_submit(held):
    result_id = held()

    with client.websocket_connect("/v1/ws/submit") as websocket:
        websocket.send_text(_meta(result_id, must_materialise=True))
        websocket.send_bytes(_plan().serialize())

    deadline = time.monotonic() + 30
    while True:
        job = client.get(f"/v1/jobs/{result_id}")
        if job.status_code == 200 and job.json()["status"] == "Completed":
            if client.head(f"/v1/results/{result_id}").status_code == 200:
                break
        assert time.monotonic() < deadline, "the submit did not complete after its client left"
        time.sleep(0.05)
    assert os.path.isfile(result_path(FLOW_ID, result_id))


def test_a_submit_sweeps_results_and_jobs_whose_submit_ended_too_long_ago(held, monkeypatch):
    kept, failed, later, last = held(), held(), held(), held()
    kept_path = _location(_submit(kept, _plan(), must_materialise=True)).path
    assert _post(pl.LazyFrame({"a": [1]}).select("missing").serialize(), _meta(failed)).status_code == 500

    _submit(later, _plan(), must_materialise=True)

    assert client.head(f"/v1/results/{kept}").status_code == 200
    assert client.get(f"/v1/jobs/{failed}").status_code == 200

    monkeypatch.setattr(protocol_routes, "CACHE_EXPIRATION_TIME", 0)
    _submit(last, _plan(), must_materialise=True)

    assert client.head(f"/v1/results/{kept}").status_code == 404
    assert not os.path.exists(kept_path)
    assert client.get(f"/v1/jobs/{kept}").status_code == 404
    assert client.get(f"/v1/jobs/{failed}").status_code == 404
    assert client.head(f"/v1/results/{later}").status_code == 404
    assert client.head(f"/v1/results/{last}").status_code == 200
    assert client.get(f"/v1/jobs/{last}").json()["status"] == "Completed"
