"""The worker's ``/v1`` routes: submit into a held result reference, then sample, count, probe and release it.

A held result is also exported to a URI as parquet, and a URI imported as a held result. A blob is imported as a copy
of its bytes and exported as those bytes, to and from a ``file://`` URI and a presigned ``https://`` URL on MinIO.
"""

import asyncio
import importlib
import ipaddress
import json
import logging
import os
import socket
import ssl
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from multiprocessing import Queue
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest
import requests
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient
from polars.testing import assert_frame_equal
from starlette.testclient import WebSocketDenialResponse

from flowfile_worker import (
    funcs,
    main,
    models,
    mp_context,
    pool,
    protocol_routes,
    service,
    spawner,
    status_dict,
    status_dict_lock,
    streaming,
)
from flowfile_worker.results import result_path
from shared.compute_protocol import (
    PROTOCOL_VERSION,
    PROTOCOL_VERSION_HEADER,
    Capabilities,
    ResultRef,
    SubmitResult,
    WorkerLocation,
    file_uri,
    schema_to_wire,
)
from tests.conftest import INTERNAL_AUTH_HEADERS

pytestmark = pytest.mark.worker

client = TestClient(main.app, headers={**INTERNAL_AUTH_HEADERS, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)})
token_only = TestClient(main.app, headers=INTERNAL_AUTH_HEADERS)

FLOW_ID = 4242
NODE_ID = 7
GEOARROW_WKB = pl.Extension("geoarrow.wkb", pl.Binary, "{}")
BLOB = bytes(range(256)) * 64
MINIO_BUCKET = "worker-test-bucket"

ROUTES = [
    ("GET", "/v1/capabilities"),
    ("POST", "/v1/submit"),
    ("GET", "/v1/results/some-id/sample?n=1"),
    ("GET", "/v1/results/some-id/count"),
    ("HEAD", "/v1/results/some-id"),
    ("DELETE", "/v1/results/some-id"),
    ("POST", "/v1/results/some-id/export"),
    ("POST", "/v1/results/some-id/import"),
    ("GET", "/v1/jobs/some-id"),
    ("POST", "/v1/jobs/some-id/cancel"),
    *[
        ("POST", f"/v1/ops/{kind}?job_id=some-id")
        for kind in (
            "database_read",
            "kafka_read",
            "google_analytics_read",
            "rest_api_read",
            "file_read",
            "fuzzy_join",
            "custom_node",
            "random_split",
            "ml_train",
            "ml_apply",
            "file_write",
            "database_write",
            "cloud_write",
            "polars_code",
            "sql_query",
        )
    ],
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


class _SleepWhenLoaded:
    """Pickled into a plan's UDF, it sleeps *seconds* when the plan is loaded, as a slow import or library load can."""

    def __init__(self, seconds: float):
        self.seconds = seconds

    def __reduce__(self):
        return time.sleep, (self.seconds,)


@pytest.fixture
def one_member_pool(monkeypatch):
    """A pool of one member, spawned at its first lease, in place of the worker's own; shut down when the test ends."""
    test_pool = pool.TaskPool(size=1, max_tasks_per_member=100, idle_ttl_seconds=300, rss_limit_mb=100_000)
    monkeypatch.setattr(pool, "task_pool", test_pool)
    yield test_pool
    test_pool.shutdown()


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
        "/v1/results/{result_id}/export",
        "/v1/results/{result_id}/import",
        "/v1/jobs/{job_id}",
        "/v1/jobs/{job_id}/cancel",
        "/v1/ops/database_read",
        "/v1/ops/kafka_read",
        "/v1/ops/google_analytics_read",
        "/v1/ops/rest_api_read",
        "/v1/ops/file_read",
        "/v1/ops/fuzzy_join",
        "/v1/ops/custom_node",
        "/v1/ops/random_split",
        "/v1/ops/ml_train",
        "/v1/ops/ml_apply",
        "/v1/ops/file_write",
        "/v1/ops/database_write",
        "/v1/ops/cloud_write",
        "/v1/ops/polars_code",
        "/v1/ops/sql_query",
    }
    assert set(paths["/v1/results/{result_id}"]) == {"head", "delete"}


def test_capabilities_reports_this_process():
    response = client.get("/v1/capabilities")

    assert response.status_code == 200
    assert Capabilities.model_validate(response.json()) == protocol_routes.worker_capabilities()


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


def test_a_deferred_submit_without_a_preview_runs_in_a_pooled_child(held, one_member_pool):
    result_id = held()
    plan = _typed_plan()
    plan_bytes = plan.serialize()

    response = _post(plan_bytes, _meta(result_id, wide=True, preview_rows=None))

    assert response.status_code == 200, response.text
    result = SubmitResult.model_validate_json(response.text)
    assert result.ref.state == "deferred"
    assert result.ref.row_count is None
    assert result.ref.columns == schema_to_wire(plan.collect_schema())
    assert result.ref.columns[3].data_type == "Extension('geoarrow.wkb', Binary, '{}')"
    assert result.sample is None
    assert _location(result).plan == plan_bytes
    assert not result_path(FLOW_ID, result_id).exists()
    assert client.get(f"/v1/jobs/{result_id}").json()["status"] == "Completed"
    described = one_member_pool.describe()
    assert described["tasks_completed"] == 1
    assert described["idle"] == 1


@pytest.mark.parametrize("stop", ["cancel", "time-limit"])
def test_a_deferred_submit_whose_plan_hangs_answers_in_time(held, one_member_pool, monkeypatch, stop):
    """The plan blocks while it loads, as a UDF module's import or a plugin library's load can, in the child."""
    result_id = held()
    sleeper = _SleepWhenLoaded(30)
    plan = _plan().with_columns(pl.col("id").map_batches(lambda s, _=sleeper: s, return_dtype=pl.Int64)).serialize()
    if stop == "time-limit":
        monkeypatch.setattr(spawner, "_TASK_TIMEOUT", 1.0)
    answers = []
    submit = threading.Thread(target=lambda: answers.append(_post(plan, _meta(result_id))))
    submit.start()
    try:
        if stop == "cancel":
            deadline = time.monotonic() + 15
            while client.get(f"/v1/jobs/{result_id}").json().get("status") != "Processing":
                assert time.monotonic() < deadline, "the submit's child never started"
                time.sleep(0.01)
            cancel = client.post(f"/v1/jobs/{result_id}/cancel")
            assert cancel.status_code == 200, cancel.text

        submit.join(10)

        assert not submit.is_alive(), "the submit did not answer in time"
    finally:
        submit.join(60)
    [response] = answers
    assert response.status_code == 500
    detail = response.json()["detail"]
    if stop == "cancel":
        assert detail["status"] == "Cancelled"
    else:
        assert detail["error_kind"] == "environment"
        assert "time limit" in detail["error_message"]
    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert not result_path(FLOW_ID, result_id).exists()
    assert one_member_pool.stats()["total"] == 0


def test_a_plan_over_a_udf_module_leaves_the_serving_process_modules_unchanged(
    held, one_member_pool, tmp_path, monkeypatch
):
    """A spawned member starts with this process's ``sys.path``, so it imports the module the plan names."""
    module_name = f"udf_seen_by_a_child_{tmp_path.name}"
    (tmp_path / f"{module_name}.py").write_text("def plus_one(s):\n    return s + 1\n")
    monkeypatch.syspath_prepend(tmp_path)
    module = importlib.import_module(module_name)
    plan = pl.LazyFrame({"a": [1, 2]}).select(pl.col("a").map_batches(module.plus_one, return_dtype=pl.Int64))
    plan_bytes = plan.serialize()
    sys.modules.pop(module_name)

    response = _post(plan_bytes, _meta(held()))

    assert response.status_code == 200, response.text
    assert SubmitResult.model_validate_json(response.text).ref.columns == schema_to_wire(pl.Schema({"a": pl.Int64}))
    assert module_name not in sys.modules


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


def _record_children(monkeypatch) -> list[str]:
    """The operation of every child the routes run from here on, in order."""
    operations: list[str] = []
    run = service._run

    def recording(task_id, plan, operation, *args, **kwargs):
        operations.append(operation)
        return run(task_id, plan, operation, *args, **kwargs)

    monkeypatch.setattr(service, "_run", recording)
    return operations


def test_more_than_a_thousand_rows_of_a_stored_result_are_read_in_a_child(held, monkeypatch):
    result_id = held()
    plan = pl.LazyFrame({"id": range(1_500)})
    _submit(result_id, plan, must_materialise=True)
    jobs_before = set(status_dict)
    children = _record_children(monkeypatch)

    in_parent = client.get(f"/v1/results/{result_id}/sample", params={"n": 1_000})
    assert children == []
    in_child = client.get(f"/v1/results/{result_id}/sample", params={"n": 1_001})

    assert children == ["submit_result"]
    assert_frame_equal(pl.read_ipc_stream(in_parent.content), plan.head(1_000).collect())
    assert_frame_equal(pl.read_ipc_stream(in_child.content), plan.head(1_001).collect())
    assert set(status_dict) == jobs_before


def test_a_sample_above_the_cap_is_refused_before_anything_runs(held, monkeypatch):
    result_id = held()
    plan = _plan()
    _submit(result_id, plan, must_materialise=True)
    children = _record_children(monkeypatch)

    refused = client.get(f"/v1/results/{result_id}/sample", params={"n": 10_001})
    assert refused.status_code == 422
    assert "10000" in refused.text
    assert children == []

    capped = client.get(f"/v1/results/{result_id}/sample", params={"n": 10_000})
    assert capped.status_code == 200
    assert_frame_equal(pl.read_ipc_stream(capped.content), plan.collect())
    assert children == ["submit_result"]


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


@pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred"])
def test_export_writes_the_result_as_parquet_at_the_uri_in_a_child(held, monkeypatch, tmp_path, must_materialise):
    result_id = held()
    plan = _plan()
    _submit(result_id, plan, must_materialise=must_materialise)
    jobs_before = set(status_dict)
    children = _record_children(monkeypatch)
    uri = file_uri(tmp_path / "out.parquet")

    response = client.post(f"/v1/results/{result_id}/export", json={"uri": uri})

    assert response.status_code == 200, response.text
    assert response.json() == {"uri": uri}
    assert children == ["export_result"]
    assert_frame_equal(pl.read_parquet(tmp_path / "out.parquet"), plan.collect())
    assert set(status_dict) == jobs_before
    assert client.head(f"/v1/results/{result_id}").status_code == 200


def test_export_of_an_unknown_result_is_404(monkeypatch, tmp_path):
    children = _record_children(monkeypatch)

    response = client.post("/v1/results/unknown/export", json={"uri": file_uri(tmp_path / "out.parquet")})

    assert response.status_code == 404
    assert children == []
    assert not (tmp_path / "out.parquet").exists()


def test_an_export_whose_sink_fails_is_500_with_the_childs_error(held):
    result_id = held()
    _submit(result_id, _plan(), must_materialise=True)

    response = client.post(f"/v1/results/{result_id}/export", json={"uri": "nosuch://bucket/out.parquet"})

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["status"] == "Error"
    assert detail["error_kind"] == "task"
    assert "nosuch://bucket/out.parquet" in detail["error_message"]
    assert client.head(f"/v1/results/{result_id}").status_code == 200


def test_a_failed_export_logs_its_target_without_the_query(monkeypatch):
    shipped = []

    def post(url, **kwargs):
        shipped.append(kwargs["json"]["log_message"])
        return SimpleNamespace(status_code=200, text="")

    monkeypatch.setattr(requests, "post", post)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        target = f"https://127.0.0.1:{probe.getsockname()[1]}/bucket/out.parquet"
    query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=minio%2F20261020&X-Amz-Signature=5e22signature"
    progress, error_message = mp_context.Value("i", 0), mp_context.Array("c", 1024)

    funcs.export_result(
        _plan().serialize(), progress, error_message, Queue(), "", FLOW_ID, NODE_ID, f"{target}?{query}", None
    )

    assert progress.value == -1
    [line] = [message for message in shipped if "Error during export" in message]
    assert target in line
    assert "X-Amz-Signature" not in line


def test_import_holds_a_deferred_result_over_the_uri_with_the_given_columns_and_count(held, monkeypatch, tmp_path):
    result_id = held()
    plan = _plan()
    path = tmp_path / "in.parquet"
    columns = schema_to_wire(plan.collect_schema())
    children = _record_children(monkeypatch)
    body = {"uri": file_uri(path), "columns": [column.model_dump() for column in columns], "row_count": 5}

    response = client.post(f"/v1/results/{result_id}/import", json=body)

    assert response.status_code == 200, response.text
    ref = ResultRef.model_validate_json(response.text)
    assert (ref.result_id, ref.state, ref.row_count, ref.columns) == (result_id, "deferred", 5, columns)
    location = WorkerLocation.model_validate_json(ref.location)
    assert (location.kind, location.path) == ("plan", None)
    assert client.head(f"/v1/results/{result_id}").status_code == 200
    assert client.get(f"/v1/results/{result_id}/count").json() == {"row_count": 5}
    assert children == []

    plan.collect().write_parquet(path)
    sample = client.get(f"/v1/results/{result_id}/sample", params={"n": 2})

    assert children == ["submit_result"]
    assert_frame_equal(pl.read_ipc_stream(sample.content), plan.head(2).collect())
    assert client.delete(f"/v1/results/{result_id}").status_code == 204
    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert path.is_file()


def test_import_under_a_held_id_replaces_it(held, tmp_path):
    result_id = held()
    stored_file = _location(_submit(result_id, _plan(), must_materialise=True)).path
    body = {"uri": file_uri(tmp_path / "in.parquet"), "columns": [], "row_count": 0}

    response = client.post(f"/v1/results/{result_id}/import", json=body)

    assert response.status_code == 200, response.text
    assert ResultRef.model_validate_json(response.text).state == "deferred"
    assert not os.path.exists(stored_file)


def test_an_import_whose_catalog_cannot_be_read_is_the_500_of_a_failed_export(held, monkeypatch, tmp_path):
    import shared.database

    def unreadable(connection_id):
        raise RuntimeError("the catalog is gone")

    monkeypatch.setattr(shared.database, "cloud_connection_record", unreadable)
    result_id = held()
    body = {
        "uri": file_uri(tmp_path / "in.parquet"),
        "columns": [],
        "credential": {"connection_id": 3, "user_id": 1},
    }

    response = client.post(f"/v1/results/{result_id}/import", json=body)

    assert response.status_code == 500
    assert response.json()["detail"] == {
        "status": "Error",
        "error_message": "CredentialResolutionError: Could not read cloud connection 3 (user 1) from the catalog "
        "database: RuntimeError",
        "error_kind": "task",
    }
    assert client.head(f"/v1/results/{result_id}").status_code == 404


@pytest.mark.parametrize(
    ("route", "body"),
    [
        ("export", {"uri": "file:///out.parquet", "format": "csv"}),
        ("export", {"uri": "file:///out.parquet", "credential": {"connection_id": 1, "user_id": 1, "secret": "s"}}),
        ("import", {"uri": "file:///in.parquet", "columns": [], "mode": "copy"}),
        ("import", {"uri": "file:///in.parquet"}),
        (
            "import",
            {"uri": "file:///in.blob", "columns": [], "kind": "blob", "credential": {"connection_id": 1, "user_id": 1}},
        ),
        ("import", {"uri": "file:///in.blob", "columns": [{"name": "a", "data_type": "Int64"}], "kind": "blob"}),
        (
            "import",
            {
                "uri": "https://minio.test/bucket/in.blob?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=5e22signature",
                "columns": [],
                "kind": "blob",
                "credential": {"connection_id": 1, "user_id": 1},
            },
        ),
    ],
    ids=[
        "export-unknown-field",
        "export-credential-field",
        "import-unknown-field",
        "import-no-columns",
        "import-blob-credential",
        "import-blob-columns",
        "import-presigned-blob-credential",
    ],
)
def test_a_bad_export_or_import_body_is_422(held, route, body):
    result_id = held()

    response = client.post(f"/v1/results/{result_id}/{route}", json=body)

    assert response.status_code == 422
    assert all("input" not in error for error in response.json()["detail"])
    assert "X-Amz-Signature" not in response.text
    assert client.head(f"/v1/results/{result_id}").status_code == 404


def _blob_body(uri: str) -> dict:
    return {"uri": uri, "columns": [], "kind": "blob"}


def test_a_blob_import_holds_a_copy_of_the_file(held, monkeypatch, tmp_path):
    result_id = held()
    source = tmp_path / "model.bin"
    source.write_bytes(BLOB)
    children = _record_children(monkeypatch)

    response = client.post(f"/v1/results/{result_id}/import", json=_blob_body(file_uri(source)))

    assert response.status_code == 200, response.text
    ref = ResultRef.model_validate_json(response.text)
    assert (ref.result_id, ref.kind, ref.state, ref.columns, ref.row_count) == (
        result_id,
        "blob",
        "materialised",
        [],
        None,
    )
    location = WorkerLocation.model_validate_json(ref.location)
    copy = Path(location.path)
    assert (location.kind, copy) == ("blob", result_path(-1, result_id).with_suffix(".blob"))
    assert copy.read_bytes() == BLOB
    source.unlink()
    assert client.head(f"/v1/results/{result_id}").status_code == 200
    target = tmp_path / "out.bin"
    exported = client.post(f"/v1/results/{result_id}/export", json={"uri": file_uri(target)})
    assert exported.status_code == 200, exported.text
    assert exported.json() == {"uri": file_uri(target)}
    assert target.read_bytes() == BLOB
    assert children == []
    assert client.delete(f"/v1/results/{result_id}").status_code == 204
    assert not copy.exists()
    assert target.read_bytes() == BLOB


def test_a_blob_uri_of_another_scheme_or_with_a_credential_is_422(held, tmp_path):
    source = tmp_path / "model.bin"
    source.write_bytes(BLOB)
    refused = held()

    response = client.post(f"/v1/results/{refused}/import", json=_blob_body("s3://bucket/model.bin"))

    assert response.status_code == 422
    assert "s3://" in response.json()["detail"]
    assert client.head(f"/v1/results/{refused}").status_code == 404
    assert not result_path(-1, refused).with_suffix(".blob").exists()
    fitted = held()
    assert client.post(f"/v1/results/{fitted}/import", json=_blob_body(file_uri(source))).status_code == 200
    credential = {"connection_id": 1, "user_id": 1}
    with_credential = client.post(
        f"/v1/results/{fitted}/export", json={"uri": file_uri(tmp_path / "out.bin"), "credential": credential}
    )
    to_s3 = client.post(f"/v1/results/{fitted}/export", json={"uri": "s3://bucket/out.bin"})
    assert (with_credential.status_code, to_s3.status_code) == (422, 422)
    assert not (tmp_path / "out.bin").exists()
    assert client.head(f"/v1/results/{fitted}").status_code == 200


@pytest.fixture(scope="module")
def minio_over_tls(tmp_path_factory):
    """MinIO behind an HTTPS forwarder with a self-signed certificate: the forwarder's URL and the certificate's path.

    MinIO is had as the cloud end-to-end suite has it, and the tests skip when Docker cannot give one. The forwarder is
    the one the cloud storage options tests build, so a URL presigned on its endpoint is an ``https://`` one that MinIO
    accepts: the Host header passes through.
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    from test_utils.s3 import fixtures as s3

    available = s3.is_docker_available() and (
        s3.is_container_running(s3.MINIO_CONTAINER_NAME) or s3.start_minio_container()
    )
    if not (available and s3.wait_for_minio(max_retries=10)):
        pytest.skip("MinIO (Docker) is not available")
    folder = tmp_path_factory.mktemp("tls")
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    (folder / "cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (folder / "key.pem").write_bytes(
        key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(folder / "cert.pem", folder / "key.pem")
    upstream_host, upstream_port = s3.MINIO_ENDPOINT_URL.removeprefix("http://").split(":")

    async def pipe(reader, writer):
        try:
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            writer.close()

    async def forward(client_reader, client_writer):
        upstream_reader, upstream_writer = await asyncio.open_connection(upstream_host, int(upstream_port))
        await asyncio.gather(pipe(client_reader, upstream_writer), pipe(upstream_reader, client_writer))

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(lambda _loop, _context: None)
    server = loop.run_until_complete(asyncio.start_server(forward, "127.0.0.1", 0, ssl=context))
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    try:
        yield f"https://localhost:{server.sockets[0].getsockname()[1]}", folder / "cert.pem"
    finally:
        loop.call_soon_threadsafe(server.close)
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)


@pytest.fixture
def presign(minio_over_tls, monkeypatch):
    """A function presigning ``put_object`` or ``get_object`` for a name under a key prefix of the test's; the prefix.

    ``requests`` trusts the forwarder's certificate through ``REQUESTS_CA_BUNDLE``; every key under the prefix is
    deleted when the test ends.
    """
    import boto3
    from botocore.client import Config

    from test_utils.s3 import fixtures as s3

    endpoint, certificate = minio_over_tls
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(certificate))
    presigner = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=s3.MINIO_ACCESS_KEY,
        aws_secret_access_key=s3.MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    prefix = f"blob-{uuid.uuid4().hex}"

    def url(method: str, name: str) -> str:
        params = {"Bucket": MINIO_BUCKET, "Key": f"{prefix}/{name}"}
        return presigner.generate_presigned_url(method, Params=params, ExpiresIn=600)

    yield url, prefix
    minio = s3.get_minio_client()
    for listed in minio.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=f"{prefix}/").get("Contents", []):
        minio.delete_object(Bucket=MINIO_BUCKET, Key=listed["Key"])


def test_a_blob_crosses_to_a_presigned_https_url_and_back(held, presign, tmp_path):
    from test_utils.s3.fixtures import get_minio_client

    url, prefix = presign
    model = os.urandom(3 * 1024 * 1024)
    source = tmp_path / "model.bin"
    source.write_bytes(model)
    fitted = held()
    assert client.post(f"/v1/results/{fitted}/import", json=_blob_body(file_uri(source))).status_code == 200
    put_url = url("put_object", "model.bin")
    assert put_url.startswith("https://")

    exported = client.post(f"/v1/results/{fitted}/export", json={"uri": put_url})

    assert exported.status_code == 200, exported.text
    assert exported.json() == {"uri": put_url}
    assert get_minio_client().get_object(Bucket=MINIO_BUCKET, Key=f"{prefix}/model.bin")["Body"].read() == model
    fetched = held()
    imported = client.post(f"/v1/results/{fetched}/import", json=_blob_body(url("get_object", "model.bin")))
    assert imported.status_code == 200, imported.text
    ref = ResultRef.model_validate_json(imported.text)
    assert (ref.result_id, ref.kind, ref.state) == (fetched, "blob", "materialised")
    copy = tmp_path / "copy.bin"
    assert client.post(f"/v1/results/{fetched}/export", json={"uri": file_uri(copy)}).status_code == 200
    assert copy.read_bytes() == model


def test_a_failed_blob_transfer_is_500_and_names_no_signature(held, presign, tmp_path, caplog):
    url, _ = presign
    source = tmp_path / "model.bin"
    source.write_bytes(BLOB)
    fitted = held()
    assert client.post(f"/v1/results/{fitted}/import", json=_blob_body(file_uri(source))).status_code == 200
    tampered = url("put_object", "model.bin").replace("X-Amz-Signature=", "X-Amz-Signature=00")
    missing = held()

    with caplog.at_level(logging.INFO), caplog.at_level(logging.DEBUG, logger="urllib3.connectionpool"):
        refused = client.post(f"/v1/results/{fitted}/export", json={"uri": tampered})
        not_found = client.post(f"/v1/results/{missing}/import", json=_blob_body(url("get_object", "missing.bin")))

    assert refused.status_code == 500
    assert refused.json()["detail"]["error_kind"] == "task"
    assert " answered 403 " in refused.json()["detail"]["error_message"]
    assert client.head(f"/v1/results/{fitted}").status_code == 200
    assert not_found.status_code == 500
    assert " answered 404 " in not_found.json()["detail"]["error_message"]
    assert client.head(f"/v1/results/{missing}").status_code == 404
    assert not result_path(-1, missing).with_suffix(".blob").exists()
    for said in (refused.text, not_found.text, caplog.text):
        assert "X-Amz-Signature" not in said


@pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred-without-a-preview"])
def test_a_plan_that_fails_on_its_data_is_a_task_failure_and_holds_nothing(held, must_materialise):
    result_id = held()

    response = _post(
        pl.LazyFrame({"a": [1]}).select("missing").serialize(), _meta(result_id, must_materialise=must_materialise)
    )

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


@pytest.mark.parametrize("preview_rows", [10, None], ids=["sampled", "deferred-without-a-preview"])
def test_bytes_that_are_not_a_plan_are_an_environment_failure(held, preview_rows):
    response = _post(b"not a plan", _meta(held(), preview_rows=preview_rows))

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


def test_a_jobs_answer_leaves_out_what_its_child_reported(held):
    result_id = held()
    reported = {"columns": _plan().collect_schema(), "row_count": 5, "sample": b"\x00"}
    with status_dict_lock:
        status_dict[result_id] = models.Status(
            background_task_id=result_id, status="Completed", file_ref="", result_type="other", results=reported
        )

    job = client.get(f"/v1/jobs/{result_id}")

    assert job.status_code == 200
    assert job.json()["status"] == "Completed"
    assert job.json()["results"] is None
    assert status_dict[result_id].results is reported


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


@pytest.mark.parametrize("hints", [{"must_materialise": True}, {}], ids=["materialised", "deferred-without-a-preview"])
def test_a_cancel_posted_while_a_submit_is_starting_stops_it_when_it_starts(held, monkeypatch, hints):
    """The submit is held in ``Starting`` before its child spawns."""
    result_id = held()
    meta, real = _meta(result_id, **hints), service.start_process
    go = threading.Event()

    def held_back(*args, **kwargs):
        go.wait(30)
        return real(*args, **kwargs)

    monkeypatch.setattr(service, "start_process", held_back)
    answers = []
    submit = threading.Thread(target=lambda: answers.append(_post(_plan().serialize(), meta)))
    submit.start()
    try:
        deadline = time.monotonic() + 30
        while (job := client.get(f"/v1/jobs/{result_id}")).status_code != 200 or job.json()["status"] != "Starting":
            assert time.monotonic() < deadline, "the submit never registered its job"
            time.sleep(0.01)

        cancel = client.post(f"/v1/jobs/{result_id}/cancel")

        assert cancel.status_code == 200, cancel.text
        job = client.get(f"/v1/jobs/{result_id}").json()
        assert job["status"] == "Cancelled"
        models.Status.model_validate(job)
    finally:
        go.set()
        submit.join(60)
    [response] = answers
    assert response.status_code == 500
    assert response.json()["detail"]["status"] == "Cancelled"
    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert not result_path(FLOW_ID, result_id).exists()


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
    # Long enough for several polls even when a warm pool member starts it at once
    slow = _plan().with_columns(pl.col("id").map_batches(lambda s: (time.sleep(0.5), s)[1], return_dtype=pl.Int64))

    progress, final, _ = _ws_submit(slow.serialize(), _meta(held(), must_materialise=True))

    assert final["type"] == "complete"
    values = [frame["progress"] for frame in progress]
    assert len(values) > len(set(values))


def test_a_client_that_leaves_does_not_stop_the_submit(held):
    """The client leaves once a progress frame says its submit runs.

    Leaving a test client's socket cancels the route and closes the event loop it ran on, so a submit not yet in the
    threadpool would never start; a real server keeps its loop and reads both frames before the close.
    """
    result_id = held()

    with client.websocket_connect("/v1/ws/submit") as websocket:
        websocket.send_text(_meta(result_id, must_materialise=True))
        websocket.send_bytes(_plan().serialize())
        assert websocket.receive_json()["type"] == "progress"

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

    monkeypatch.setattr(service, "CACHE_EXPIRATION_TIME", 0)
    _submit(last, _plan(), must_materialise=True)

    assert client.head(f"/v1/results/{kept}").status_code == 404
    assert not os.path.exists(kept_path)
    assert client.get(f"/v1/jobs/{kept}").status_code == 404
    assert client.get(f"/v1/jobs/{failed}").status_code == 404
    assert client.head(f"/v1/results/{later}").status_code == 404
    assert client.head(f"/v1/results/{last}").status_code == 200
    assert client.get(f"/v1/jobs/{last}").json()["status"] == "Completed"


def _slow_plan() -> bytes:
    """A plan whose child sleeps 30 s as it loads, long enough to be replaced or released while it runs."""
    sleeper = _SleepWhenLoaded(30)
    return _plan().with_columns(pl.col("id").map_batches(lambda s, _=sleeper: s, return_dtype=pl.Int64)).serialize()


def _wait_for_job(result_id: str, status: str, on: TestClient = client) -> None:
    deadline = time.monotonic() + 30
    while (job := on.get(f"/v1/jobs/{result_id}")).status_code != 200 or job.json()["status"] != status:
        assert time.monotonic() < deadline, f"the job never reached {status}"
        time.sleep(0.01)


def test_a_submit_under_an_id_a_job_still_runs_under_replaces_that_job(held, one_member_pool):
    """A rerun after core's socket dropped: the first child would otherwise write the same file as the second."""
    result_id = held()
    answers = []
    first = threading.Thread(
        target=lambda: answers.append(_post(_slow_plan(), _meta(result_id, must_materialise=True)))
    )
    first.start()
    try:
        _wait_for_job(result_id, "Processing")

        replacement = _submit(result_id, pl.LazyFrame({"y": ["b"]}), must_materialise=True)

        first.join(30)
        assert not first.is_alive(), "the replaced submit did not answer"
    finally:
        first.join(60)
    [response] = answers
    assert response.status_code == 500
    assert response.json()["detail"]["status"] == "Cancelled"
    assert [column.name for column in replacement.ref.columns] == ["y"]
    assert pl.read_ipc(_location(replacement).path).columns == ["y"]
    assert client.head(f"/v1/results/{result_id}").status_code == 200
    assert client.get(f"/v1/jobs/{result_id}").json()["status"] == "Completed"


def test_a_release_of_an_id_a_submit_still_runs_under_stops_it_and_holds_nothing(held, one_member_pool):
    result_id = held()
    answers = []
    submit = threading.Thread(
        target=lambda: answers.append(_post(_slow_plan(), _meta(result_id, must_materialise=True)))
    )
    submit.start()
    try:
        _wait_for_job(result_id, "Processing")

        assert client.delete(f"/v1/results/{result_id}").status_code == 204

        submit.join(30)
        assert not submit.is_alive(), "the released submit did not answer"
    finally:
        submit.join(60)
    [response] = answers
    assert response.status_code == 500
    assert response.json()["detail"]["status"] == "Cancelled"
    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert not result_path(FLOW_ID, result_id).exists()


def test_a_submit_released_before_its_job_registers_never_starts(held, monkeypatch):
    """The release lands between the submit's claim on the id and its job's entry."""
    result_id = held()
    go, started = threading.Event(), []
    real_materialise, real_start = service._materialise, service.start_process

    def held_back(hints):
        go.wait(30)
        return real_materialise(hints)

    monkeypatch.setattr(service, "_materialise", held_back)
    monkeypatch.setattr(service, "start_process", lambda *a, **k: started.append(1) or real_start(*a, **k))
    answers = []
    submit = threading.Thread(target=lambda: answers.append(_post(_plan().serialize(), _meta(result_id))))
    submit.start()
    try:
        deadline = time.monotonic() + 30
        while result_id not in service._claims:
            assert time.monotonic() < deadline, "the submit never claimed its id"
            time.sleep(0.01)

        assert client.delete(f"/v1/results/{result_id}").status_code == 204
    finally:
        go.set()
        submit.join(60)
    [response] = answers
    assert response.status_code == 500
    assert response.json()["detail"]["status"] == "Cancelled"
    assert started == []
    assert client.head(f"/v1/results/{result_id}").status_code == 404


def test_a_job_that_completes_without_reporting_its_result_is_a_failed_task(held, monkeypatch):
    """A pool member that died after its last progress leaves the job ``Completed`` with nothing reported."""
    result_id = held()

    def completes_empty(*args, task_id: str, **kwargs):
        with status_dict_lock:
            status_dict[task_id].status = "Completed"
            status_dict[task_id].progress = 100

    monkeypatch.setattr(service, "start_process", completes_empty)

    response = _post(_plan().serialize(), _meta(result_id, must_materialise=True))

    assert response.status_code == 500
    detail = response.json()["detail"]
    assert detail["status"] == "Error"
    assert detail["error_kind"] == "task"
    assert "without reporting its result" in detail["error_message"]
    assert client.get(f"/v1/jobs/{result_id}").json()["status"] == "Error"
    assert client.head(f"/v1/results/{result_id}").status_code == 404
    assert not result_path(FLOW_ID, result_id).exists()


@pytest.mark.parametrize("result_id", ["a.b", "..%5Cescaped", "has%20space"])
def test_an_import_under_an_id_that_could_name_another_file_is_422(result_id, tmp_path):
    body = {"uri": file_uri(tmp_path / "x.parquet"), "credential": None, "columns": [], "row_count": None}

    assert client.post(f"/v1/results/{result_id}/import", json=body).status_code == 422


def _one_pool_thread() -> None:
    import anyio.to_thread

    anyio.to_thread.current_default_thread_limiter().total_tokens = 1


def test_the_short_routes_answer_while_every_pool_thread_waits_on_a_child(held, monkeypatch):
    """A submit holds the only thread of the pool; a job's status, a probe, a release and a cancel still answer."""
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(protocol_routes.router)
    result_id, other = held(), held()
    go = threading.Event()
    real = service.start_process

    def held_back(*args, **kwargs):
        go.wait(30)
        return real(*args, **kwargs)

    monkeypatch.setattr(service, "start_process", held_back)
    headers = {**INTERNAL_AUTH_HEADERS, PROTOCOL_VERSION_HEADER: str(PROTOCOL_VERSION)}
    answers: dict[str, int] = {}
    with TestClient(app, headers=headers) as one_loop:
        one_loop.portal.call(_one_pool_thread)
        files = {"plan": ("plan", _plan().serialize(), "application/octet-stream")}
        submit = threading.Thread(
            target=lambda: one_loop.post("/v1/submit", files=files, data={"meta": _meta(result_id)})
        )
        submit.start()

        def probe() -> None:
            answers["status"] = one_loop.get(f"/v1/jobs/{result_id}").status_code
            answers["head"] = one_loop.head(f"/v1/results/{other}").status_code
            answers["delete"] = one_loop.delete(f"/v1/results/{other}").status_code
            answers["cancel"] = one_loop.post(f"/v1/jobs/{other}/cancel").status_code

        prober = threading.Thread(target=probe)
        try:
            _wait_for_job(result_id, "Starting", on=one_loop)
            prober.start()
            prober.join(10)
            assert not prober.is_alive(), "a short route waited for the pool thread the submit holds"
        finally:
            go.set()
            submit.join(60)
            if prober.is_alive():
                prober.join(60)
    assert answers == {"status": 200, "head": 404, "delete": 404, "cancel": 404}
