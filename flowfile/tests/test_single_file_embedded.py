"""The pip server computes on the worker embedded in its own process, its jobs in that worker's pool.

``start_server`` runs here with ``flowfile_core.main.run`` stubbed to mark the process as one that serves and return,
and a bare app in place of core's, so no port is bound and ``flowfile_core.main.app`` is left as it is; every setting
it changes is restored after the test.
"""

import os
import socket

import polars as pl
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import flowfile_core.main as core_main
from flowfile.web import start_server
from flowfile_core.configs import settings
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.execution.backends.embedded import EmbeddedWorkerBackend
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake, subprocess_operations
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_worker import pool, spawner

MOUNT = "http://127.0.0.1:63578/worker"
ROWS = pl.DataFrame({"region": ["north", "south", "north", "east"], "amount": [12, 8, 30, 22]})


@pytest.fixture
def single_file_app(monkeypatch) -> FastAPI:
    """The process as ``start_server`` leaves it before uvicorn starts: marked as serving, in single-file mode."""
    app = FastAPI()
    monkeypatch.setattr(core_main, "app", app)
    monkeypatch.setattr(core_main, "run", lambda host, port: registry.mark_serving())
    monkeypatch.setattr(registry, "_serving", False)
    monkeypatch.setattr(settings.OFFLOAD_TO_WORKER, "value", settings.OFFLOAD_TO_WORKER.value)
    monkeypatch.setattr(settings.SINGLE_FILE_MODE, "value", settings.SINGLE_FILE_MODE.value)
    monkeypatch.setattr(settings, "WORKER_URL", settings.WORKER_URL)
    monkeypatch.setenv("FLOWFILE_MODE", os.environ.get("FLOWFILE_MODE", "electron"))
    monkeypatch.setenv("FLOWFILE_WORKER_LOCAL_FILES", "0")
    monkeypatch.setattr(spawner, "in_process", False)
    registry.reset()
    start_server(open_browser=False)
    yield app
    registry.reset()
    handshake.reset_handshake()


@pytest.fixture
def no_worker_reached(monkeypatch) -> dict[str, list]:
    """Fails any ``WorkerBackend`` made, any worker-session request sent and any socket listening in this process."""
    seen: dict[str, list] = {"worker_backends": [], "session_requests": [], "listening": []}

    def refuse(kind: str):
        def call(*args, **kwargs):
            seen[kind].append((args[1:], kwargs))
            raise AssertionError(f"{kind}: {args[1:]} {kwargs}")

        return call

    monkeypatch.setattr(WorkerBackend, "__init__", refuse("worker_backends"))
    monkeypatch.setattr(subprocess_operations._WorkerSession, "request", refuse("session_requests"))
    monkeypatch.setattr(socket.socket, "listen", refuse("listening"))
    return seen


@pytest.fixture
def jobs(monkeypatch) -> dict[str, list]:
    """Records each pool lease (the operation and whether a member took it) and fails any job run in this process."""
    seen: dict[str, list] = {"leases": [], "inline": []}
    acquire = pool.task_pool.acquire

    def leased(operation):
        member = acquire(operation)
        seen["leases"].append((operation, member is not None))
        return member

    def inline(task_id, target, kwargs):
        seen["inline"].append(task_id)
        raise AssertionError(f"job {task_id} ran in the serving process")

    monkeypatch.setattr(pool.task_pool, "acquire", leased)
    monkeypatch.setattr(spawner, "run_inline", inline)
    return seen


def listening(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=0.5).close()
    except OSError:
        return False
    return True


def read_and_group_flow(csv_path: str) -> FlowGraph:
    """A CSV read (node 1) and a group-by over it (node 2), on the default backend, in Development mode."""
    graph = FlowGraph(
        schemas.FlowSettings(
            flow_id=5454, name="single_file", path=".", execution_location="auto", execution_mode="Development"
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=1, node_type="read"))
    received = input_schema.ReceivedTable(path=csv_path, file_type="csv", table_settings={"encoding": "utf-8"})
    graph.add_read(input_schema.NodeRead(flow_id=graph.flow_id, node_id=1, received_file=received))
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=2, node_type="group_by"))
    graph.add_group_by(
        input_schema.NodeGroupBy(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            groupby_input=transform_schema.GroupByInput(
                agg_cols=[
                    transform_schema.AggColl("region", "groupby"),
                    transform_schema.AggColl("amount", "sum", "total"),
                ]
            ),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return graph


def test_start_server_puts_the_process_in_single_file_mode_with_the_worker_url_at_its_mount(
    single_file_app, no_worker_reached
):
    client = TestClient(single_file_app)

    assert registry.serving() is True
    assert bool(settings.SINGLE_FILE_MODE) is True
    assert registry.worker_url() == MOUNT
    assert client.get("/single_mode").json() is True
    assert client.get("/worker/v1/capabilities").status_code == 401
    assert no_worker_reached == {"worker_backends": [], "session_requests": [], "listening": []}


@pytest.mark.skipif(
    not settings.FLOWFILE_COMPUTE_PROTOCOL,
    reason="with the compute protocol off a flow posts its jobs to the worker routes over HTTP, the path the task "
    "routes' removal takes away",
)
def test_a_local_read_runs_on_the_embedded_worker_s_pool_and_opens_no_port_for_a_worker(
    single_file_app, no_worker_reached, jobs, tmp_path
):
    csv_path = tmp_path / "sales.csv"
    ROWS.write_csv(csv_path)
    assert not listening(settings.WORKER_PORT)
    graph = read_and_group_flow(str(csv_path))
    try:
        run_info = graph.run_graph()

        assert run_info.success is True, [result.error for result in run_info.node_step_result]
        worker = registry.backend(registry.WORKER_BACKEND_ID)
        totals = pl.read_ipc_stream(worker.sample(graph.get_node(2).results.result_ref, 10))
    finally:
        graph.close_flow()

    assert isinstance(worker, EmbeddedWorkerBackend)
    assert worker.id == registry.WORKER_BACKEND_ID
    assert totals.sort("region").rows() == [("east", 22), ("north", 42), ("south", 8)]
    assert ("submit_result", True) in jobs["leases"]
    assert jobs["inline"] == []
    assert spawner.in_process is False
    assert no_worker_reached == {"worker_backends": [], "session_requests": [], "listening": []}
    assert not listening(settings.WORKER_PORT)
