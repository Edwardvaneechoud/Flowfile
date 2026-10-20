"""A published flow whose worker cannot run it fails the request typed; core never collects the response instead.

Drives the real app over HTTP: the public ``/api/data/{slug}`` route answers with generic detail (it never
exposes node or worker detail to an API-key holder), the owner's ``/flow-api/endpoints/{id}/test`` route with
the typed message. With the protocol flag on, a remote response is a sample of the result the worker holds.
"""

import socket
import time
from types import SimpleNamespace
from uuid import uuid4

import polars as pl
import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.configs import settings
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile import api_runner
from flowfile_core.flowfile.api_runner import ApiExecutionError
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.execution.errors import ComputeUnavailable, PlanRejected, TaskFailed
from flowfile_core.flowfile.flow_data_engine.bounded import DEFERRED_HINTS
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import BaseFetcher
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.routes import flow_api
from flowfile_core.schemas import input_schema, schemas

PUBLIC_UNAVAILABLE_DETAIL = "Compute unavailable"
PUBLIC_FAILED_DETAIL = "Flow execution failed"


class _NotAPlan:
    """Stands in for a LazyFrame: its serialised plan is bytes the worker cannot load."""

    def serialize(self) -> bytes:
        return b"not a plan"


class _UnserialisablePlan:
    """Stands in for a LazyFrame that core itself cannot serialise for the worker."""

    message = "cannot serialise a plan holding a core-only object"

    def serialize(self) -> bytes:
        raise RuntimeError(self.message)


class _KilledChildFetcher(BaseFetcher):
    """A fetcher whose run finished with the worker child gone (code -1, no kind), as over the WebSocket."""

    description = "Process ended unexpectedly"

    def __init__(self, *, file_ref, worker_url, **_kwargs):
        super().__init__(file_ref=file_ref, worker_url=worker_url)
        with self._lock:
            self._started = True  # the run is over, so get_result must not start polling the worker
        self._record_task_failure(self.description, -1, None)


@pytest.fixture(scope="module")
def client():
    """One authenticated TestClient for the module: the public route's semaphore binds to one event loop."""
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
        c.headers.update({"Authorization": f"Bearer {token}"})
        yield c


def _save_flow(path, flow_id: int) -> None:
    """manual_input -> api_response, saved to *path*."""
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name="no_fallback", path=str(path)))
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"region": "EU", "v": 1}]),
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="api_response"))
    graph.add_api_response(input_schema.NodeApiResponse(flow_id=flow_id, node_id=2, depending_on_id=1))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.save_flow(str(path))


@pytest.fixture(scope="module")
def endpoint(client, tmp_path_factory):
    """A published flow with an API key, owned by the local user the client is logged in as."""
    flow_path = tmp_path_factory.mktemp("no_fallback") / "flow.yaml"
    _save_flow(flow_path, flow_id=9401)
    with get_db_context() as db:
        registration = db_models.FlowRegistration(
            flow_uuid=str(uuid4()), name="no_fallback", flow_path=str(flow_path), owner_id=1, is_api_compatible=True
        )
        db.add(registration)
        db.commit()
        db.refresh(registration)
        registration_id = registration.id

    slug = f"no-fallback-{uuid4().hex[:8]}"
    published = client.post("/flow-api/endpoints", json={"registration_id": registration_id, "slug": slug})
    assert published.status_code == 201, published.text
    endpoint_id = published.json()["id"]
    key = client.post(f"/flow-api/endpoints/{endpoint_id}/keys", json={"name": "k"})
    assert key.status_code == 201, key.text
    return SimpleNamespace(id=endpoint_id, slug=slug, api_key=key.json()["api_key"])


def _call(client, endpoint, route: str):
    if route == "public":
        return client.get(f"/api/data/{endpoint.slug}", headers={"X-API-Key": endpoint.api_key})
    return client.post(f"/flow-api/endpoints/{endpoint.id}/test", json={"params": {}})


@pytest.fixture
def run_record(monkeypatch):
    """Pin published runs to the worker and record what ``_materialize`` raised and every in-core collect."""
    monkeypatch.setattr(api_runner, "get_global_execution_location", lambda: "remote")
    record = SimpleNamespace(raised=[], core_collects=0)
    real_materialize = api_runner._materialize
    real_collect = FlowDataEngine.collect

    def _recording_materialize(*args, **kwargs):
        try:
            return real_materialize(*args, **kwargs)
        except Exception as exc:
            record.raised.append(exc)
            raise

    def _counting_collect(self, *args, **kwargs):
        record.core_collects += 1
        return real_collect(self, *args, **kwargs)

    monkeypatch.setattr(api_runner, "_materialize", _recording_materialize)
    monkeypatch.setattr(FlowDataEngine, "collect", _counting_collect)
    return record


@pytest.fixture
def closed_worker_port(monkeypatch) -> int:
    """Point the worker backend, whose URL the published run sends its plan to, at a port nothing listens on."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    monkeypatch.setattr(registry.backend(registry.WORKER_BACKEND_ID), "url", f"http://127.0.0.1:{port}")
    return port


@pytest.fixture
def running_worker():
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")


@pytest.fixture
def task_routes(monkeypatch):
    """The protocol flag off: these tests reach the worker through the task routes' fetcher, which the flag replaces."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", False)


@pytest.fixture
def protocol_on(monkeypatch):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)


@pytest.fixture
def api_raised(monkeypatch) -> list[Exception]:
    """Every exception the published run raised to the route, ``_materialize``'s or the run's own."""
    raised: list[Exception] = []
    real_run = flow_api.run_flow_as_api

    def _recording_run(*args, **kwargs):
        try:
            return real_run(*args, **kwargs)
        except Exception as exc:
            raised.append(exc)
            raise

    monkeypatch.setattr(flow_api, "run_flow_as_api", _recording_run)
    return raised


@pytest.fixture
def sampled(monkeypatch) -> list[tuple[str, int]]:
    """Every ``WorkerBackend.sample`` as ``(result id, n)``; the task routes' fetcher fails the test if it is built."""
    calls: list[tuple[str, int]] = []
    real_sample = WorkerBackend.sample

    def _recording_sample(self, ref, n):
        calls.append((ref.result_id, n))
        return real_sample(self, ref, n)

    def _no_fetcher(**_kwargs):
        pytest.fail("the response was read through the task routes' fetcher")

    monkeypatch.setattr(WorkerBackend, "sample", _recording_sample)
    monkeypatch.setattr(api_runner, "ExternalDfFetcher", _no_fetcher)
    return calls


def test_counter_sees_the_in_core_collect_of_a_local_run(client, endpoint, run_record, monkeypatch):
    monkeypatch.setattr(api_runner, "get_global_execution_location", lambda: "local")

    response = _call(client, endpoint, "owner")

    assert response.status_code == 200, response.text
    assert response.json()["data"] == [{"region": "EU", "v": 1}]
    assert run_record.core_collects == 1


@pytest.mark.parametrize("route", ["public", "owner"])
def test_unreachable_worker_answers_503(client, endpoint, task_routes, run_record, closed_worker_port, route):
    response = _call(client, endpoint, route)

    assert response.status_code == 503, response.text
    [raised] = run_record.raised
    assert type(raised) is ComputeUnavailable
    assert f"port={closed_worker_port}" in str(raised)
    expected_detail = PUBLIC_UNAVAILABLE_DETAIL if route == "public" else str(raised)
    assert response.json()["detail"] == expected_detail
    assert run_record.core_collects == 0


@pytest.mark.parametrize("route", ["public", "owner"])
def test_plan_the_worker_cannot_load_answers_500(
    client, endpoint, task_routes, run_record, running_worker, route, monkeypatch
):
    real_fetcher = api_runner.ExternalDfFetcher

    def _ship_not_a_plan(*, lf, file_ref, **kwargs):
        return real_fetcher(lf=_NotAPlan(), file_ref=f"{file_ref}-{time.monotonic_ns()}", **kwargs)

    monkeypatch.setattr(api_runner, "ExternalDfFetcher", _ship_not_a_plan)

    response = _call(client, endpoint, route)

    assert response.status_code == 500, response.text
    [raised] = run_record.raised
    assert type(raised) is PlanRejected
    assert "DSL_VERSION" in str(raised)
    expected_detail = PUBLIC_FAILED_DETAIL if route == "public" else str(raised)
    assert response.json()["detail"] == expected_detail
    assert run_record.core_collects == 0


@pytest.mark.parametrize("route", ["public", "owner"])
def test_killed_worker_child_answers_500(client, endpoint, task_routes, run_record, route, monkeypatch):
    monkeypatch.setattr(api_runner, "ExternalDfFetcher", _KilledChildFetcher)

    response = _call(client, endpoint, route)

    assert response.status_code == 500, response.text
    [raised] = run_record.raised
    assert type(raised) is TaskFailed
    assert str(raised) == _KilledChildFetcher.description
    expected_detail = PUBLIC_FAILED_DETAIL if route == "public" else _KilledChildFetcher.description
    assert response.json()["detail"] == expected_detail
    assert run_record.core_collects == 0


def test_plan_core_cannot_serialise_answers_500_with_its_detail_to_the_owner(
    client, endpoint, task_routes, run_record, monkeypatch
):
    real_fetcher = api_runner.ExternalDfFetcher

    def _ship_unserialisable(*, lf, **kwargs):
        return real_fetcher(lf=_UnserialisablePlan(), **kwargs)

    monkeypatch.setattr(api_runner, "ExternalDfFetcher", _ship_unserialisable)

    response = _call(client, endpoint, "owner")

    assert response.status_code == 500, response.text
    [raised] = run_record.raised
    assert type(raised) is ApiExecutionError
    assert isinstance(raised.__cause__, RuntimeError)
    assert response.json()["detail"] == _UnserialisablePlan.message
    assert run_record.core_collects == 0


@pytest.mark.parametrize("route", ["public", "owner"])
def test_with_the_protocol_flag_the_response_is_a_sample_of_the_result_the_worker_holds(
    client, endpoint, protocol_on, run_record, sampled, running_worker, route
):
    response = _call(client, endpoint, route)

    assert response.status_code == 200, response.text
    assert response.json()["data"] == [{"region": "EU", "v": 1}]
    assert [n for _, n in sampled] == [10_000]
    assert run_record.raised == []
    assert run_record.core_collects == 0


def test_with_the_protocol_flag_a_worker_gone_before_the_read_answers_503(
    client, endpoint, protocol_on, run_record, running_worker, monkeypatch
):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    recording_materialize = api_runner._materialize

    def _worker_gone_first(*args, **kwargs):
        monkeypatch.setattr(registry.backend(registry.WORKER_BACKEND_ID), "url", f"http://127.0.0.1:{port}")
        return recording_materialize(*args, **kwargs)

    monkeypatch.setattr(api_runner, "_materialize", _worker_gone_first)

    response = _call(client, endpoint, "public")

    assert response.status_code == 503, response.text
    [raised] = run_record.raised
    assert type(raised) is ComputeUnavailable
    assert f"port={port}" in str(raised)
    assert response.json()["detail"] == PUBLIC_UNAVAILABLE_DETAIL
    assert run_record.core_collects == 0


@pytest.mark.parametrize("route", ["public", "owner"])
def test_with_the_protocol_flag_a_worker_unreachable_when_the_run_starts_answers_503(
    client, endpoint, protocol_on, run_record, closed_worker_port, api_raised, route
):
    response = _call(client, endpoint, route)

    assert response.status_code == 503, response.text
    [raised] = api_raised
    assert type(raised) is ComputeUnavailable
    assert str(raised).startswith("node 1: ")
    assert f"port={closed_worker_port}" in str(raised)
    assert str(raised).count('Is backend "worker" running? Start it, or check the Backends page.') == 1
    expected_detail = PUBLIC_UNAVAILABLE_DETAIL if route == "public" else str(raised)
    assert response.json()["detail"] == expected_detail
    assert run_record.raised == []
    assert run_record.core_collects == 0


@pytest.mark.parametrize("route", ["public", "owner"])
@pytest.mark.parametrize(
    "error, status, public_detail",
    [
        (ComputeUnavailable("Connection refused"), 503, PUBLIC_UNAVAILABLE_DETAIL),
        (PlanRejected("the worker could not load the plan"), 500, PUBLIC_FAILED_DETAIL),
    ],
    ids=["unavailable", "rejected"],
)
@pytest.mark.parametrize("flag", [False, True], ids=["flag-off", "flag-on"])
def test_with_the_protocol_flag_a_compute_error_that_fails_a_node_reaches_the_route_typed(
    client, endpoint, run_record, api_raised, monkeypatch, flag, error, status, public_detail, route
):
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", flag)

    def _fail(self, *args, **kwargs):
        raise error

    monkeypatch.setattr(WorkerBackend, "submit", _fail)

    response = _call(client, endpoint, route)

    [raised] = api_raised
    assert str(raised) == f"node 1: {error}"
    if flag:
        assert response.status_code == status, response.text
        assert type(raised) is type(error)
    else:
        assert response.status_code == 500, response.text
        assert type(raised) is ApiExecutionError
        public_detail = PUBLIC_FAILED_DETAIL
    assert response.json()["detail"] == (public_detail if route == "public" else str(raised))
    assert run_record.raised == []
    assert run_record.core_collects == 0


@pytest.mark.parametrize("max_rows, n", [(None, 10_000), (2, 2), (20_000, 10_000)])
def test_with_the_protocol_flag_max_rows_is_clamped_to_the_workers_sample_cap(
    protocol_on, sampled, running_worker, max_rows, n
):
    backend = registry.backend(registry.WORKER_BACKEND_ID)
    plan = pl.LazyFrame({"v": [1, 2, 3]})
    ref = backend.submit(plan, [], DEFERRED_HINTS, f"api_{uuid4().hex}", flow_id=-1, node_id=-1).ref
    remote_flow = SimpleNamespace(flow_settings=SimpleNamespace(execution_location="remote"))
    api_node = SimpleNamespace(results=SimpleNamespace(result_ref=ref))
    try:
        response = api_runner._materialize(None, max_rows, remote_flow, api_node)
    finally:
        backend.release(ref)

    assert sampled == [(ref.result_id, n)]
    assert response.equals(plan.head(n).collect())
