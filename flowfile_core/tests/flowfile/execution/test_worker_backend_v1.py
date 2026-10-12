"""With the protocol flag on, the worker backend submits plans and reads results over the worker's ``/v1`` routes.

Every test here turns ``settings.FLOWFILE_COMPUTE_PROTOCOL`` on; the dispatch tests turn it off again mid-test to show
that a result is read over the routes that minted it.
"""

import io
import re
import socket
import uuid
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
import requests

from flowfile_core.configs import settings
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.worker import WorkerBackend, _flowfile_schema
from flowfile_core.flowfile.execution.errors import (
    ComputeUnavailable,
    PlanRejected,
    ResultMissing,
    TaskFailed,
    VersionMismatch,
)
from flowfile_core.flowfile.execution.payloads import FileReadPayload, op_payload
from flowfile_core.flowfile.execution.protocol import JobHandle, OperationKind, ResultRef, SubmitHints, SubmitResult
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.handshake import fetch_worker_capabilities
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.input_schema import NodeRead, ReceivedTable
from shared.compute_protocol import PROTOCOL_VERSION_HEADER, Capabilities, WorkerLocation
from shared.storage_config import storage

PEOPLE = pl.DataFrame(
    {"id": list(range(10)), "name": [f"n{i}" for i in range(10)], "score": [i * 1.5 for i in range(10)]}
)
GEOARROW_WKB = pl.Extension("geoarrow.wkb", pl.Binary, "{}")
STATES = pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred"])


@pytest.fixture(autouse=True)
def protocol_on(monkeypatch):
    """The protocol flag is on for every test here and back to what it was when the test ends."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)


@pytest.fixture
def running_worker():
    from tests.conftest import is_worker_running

    if not is_worker_running():
        pytest.skip("flowfile_worker is not running")


@pytest.fixture
def backend(running_worker):
    return WorkerBackend(url=settings.WORKER_URL)


@pytest.fixture
def kept(backend):
    """Results a test made, released from the worker when it ends."""
    refs: list[ResultRef] = []
    yield refs
    for ref in refs:
        backend.release(ref)


@pytest.fixture
def sent(running_worker, monkeypatch):
    """Every HTTP request core sends from here on, as (method, path on the worker, ``X-Protocol-Version``).

    The handshake runs first, so its own calls are not among them.
    """
    handshake.ensure_handshake(settings.WORKER_URL)
    requests_sent: list[tuple[str, str, str | None]] = []
    real_send = requests.Session.send

    def recording_send(self, request, **kwargs):
        path = request.url.removeprefix(settings.WORKER_URL)
        requests_sent.append((request.method, path, request.headers.get(PROTOCOL_VERSION_HEADER)))
        return real_send(self, request, **kwargs)

    monkeypatch.setattr(requests.Session, "send", recording_send)
    return requests_sent


@pytest.fixture
def closed_port_url() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}"


@pytest.fixture
def core_only_udf_plan(tmp_path, monkeypatch) -> pl.LazyFrame:
    """A plan whose UDF lives in a module only this (core) process can import, as with separate images."""
    module_name = f"core_only_udf_{tmp_path.name.replace('-', '_')}"
    (tmp_path / f"{module_name}.py").write_text("def plus_one(s):\n    return s + 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    module = __import__(module_name)
    return pl.LazyFrame({"a": [1, 2]}).select(pl.col("a").map_batches(module.plus_one, return_dtype=pl.Int64))


def people() -> pl.LazyFrame:
    return PEOPLE.lazy().filter(pl.col("id") >= 0)


def typed_plan() -> pl.LazyFrame:
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


def hints(must_materialise: bool, preview_rows: int | None = None) -> SubmitHints:
    return SubmitHints(must_materialise=must_materialise, wide=False, fanout=1, preview_rows=preview_rows)


def new_result_id() -> str:
    return f"t-v1-backend-{uuid.uuid4().hex}"


def submit(
    backend: WorkerBackend,
    kept: list[ResultRef],
    *,
    must_materialise: bool,
    preview_rows: int | None = None,
    plan: pl.LazyFrame | None = None,
    inputs: list[ResultRef] | None = None,
) -> SubmitResult:
    result = backend.submit(
        people() if plan is None else plan,
        inputs or [],
        hints(must_materialise, preview_rows),
        new_result_id(),
        flow_id=-1,
        node_id=-1,
    )
    kept.append(result.ref)
    return result


def read_stream(data: bytes) -> pa.Table:
    return pa.ipc.open_stream(io.BytesIO(data)).read_all()


def location(ref: ResultRef) -> WorkerLocation:
    return WorkerLocation.model_validate_json(ref.location)


def v1_ref(result_id: str) -> ResultRef:
    """A reference as ``/v1`` mints one, for a result the worker never held."""
    nowhere = WorkerLocation(kind="file", path="/nowhere.arrow").model_dump_json().encode()
    return ResultRef("worker", result_id, [], None, "materialised", nowhere)


def handshake_fields(capabilities: Capabilities) -> dict:
    return capabilities.model_dump(exclude={"operations"})


def test_a_materialised_submit_is_held_in_the_workers_own_file(backend, kept):
    result = submit(backend, kept, must_materialise=True, preview_rows=4)

    ref = result.ref
    assert ref.backend_id == "worker"
    assert ref.result_id.startswith("t-v1-backend-")
    assert (ref.state, ref.row_count) == ("materialised", 10)
    assert result.handle == JobHandle("worker", ref.result_id)
    held = location(ref)
    assert held.kind == "file"
    assert Path(held.path) == storage.cache_directory / "-1" / f"{ref.result_id}.arrow"
    assert backend.result_file(ref) is None
    assert pl.from_arrow(read_stream(result.sample)).equals(PEOPLE.head(4))


def test_a_deferred_submit_is_held_as_the_plan_it_sent(backend, kept):
    plan = people()

    result = submit(backend, kept, must_materialise=False, preview_rows=4, plan=plan)

    ref = result.ref
    assert (ref.state, ref.row_count) == ("deferred", None)
    assert result.handle == JobHandle("worker", ref.result_id)
    held = location(ref)
    assert (held.kind, held.path) == ("plan", None)
    assert held.plan == plan.serialize()
    assert backend.result_file(ref) is None
    assert pl.from_arrow(read_stream(result.sample)).equals(PEOPLE.head(4))


@STATES
def test_no_sample_comes_back_without_preview_rows(backend, kept, must_materialise):
    assert submit(backend, kept, must_materialise=must_materialise).sample is None


@STATES
def test_the_reference_carries_the_columns_a_task_route_reference_would(backend, kept, must_materialise):
    plan = typed_plan()

    ref = submit(backend, kept, must_materialise=must_materialise, plan=plan).ref

    expected = _flowfile_schema(plan.collect_schema())
    assert [column.get_column_repr() for column in ref.schema] == [column.get_column_repr() for column in expected]
    assert [column.semantic_type for column in ref.schema] == [None, None, None, "geometry"]


def test_every_call_goes_to_the_v1_routes_with_the_protocol_version(backend, kept, sent):
    result = submit(backend, kept, must_materialise=True, preview_rows=4)
    ref, handle = result.ref, result.handle

    backend.sample(ref, 2)
    backend.count(ref)
    backend.exists(ref)
    backend.status(handle)
    backend.cancel(handle)
    backend.capabilities()
    backend.release(ref)

    held = f"/v1/results/{ref.result_id}"
    assert [(method, path) for method, path, _ in sent] == [
        ("POST", "/v1/submit"),
        ("GET", f"{held}/sample?n=2"),
        ("GET", f"{held}/count"),
        ("HEAD", held),
        ("GET", f"/v1/jobs/{ref.result_id}"),
        ("POST", f"/v1/jobs/{ref.result_id}/cancel"),
        ("GET", "/v1/capabilities"),
        ("DELETE", held),
    ]
    assert {version for _, _, version in sent} == {"1"}


@STATES
def test_sample_count_and_release_go_over_the_wire(backend, kept, must_materialise):
    ref = submit(backend, kept, must_materialise=must_materialise).ref

    assert [read_stream(backend.sample(ref, n)).num_rows for n in (0, 3, 50)] == [0, 3, 10]
    assert backend.count(ref) == 10
    assert backend.exists(ref) is True

    backend.release(ref)

    assert backend.exists(ref) is False
    backend.release(ref)
    with pytest.raises(ResultMissing, match=ref.result_id):
        backend.sample(ref, 1)
    with pytest.raises(ResultMissing, match=ref.result_id):
        backend.count(ref)


@STATES
def test_plan_for_decodes_the_location_into_a_leaf_without_a_call(backend, kept, sent, must_materialise):
    ref = submit(backend, kept, must_materialise=must_materialise).ref
    sent.clear()

    leaf = backend.plan_for(ref)

    assert sent == []
    assert leaf.filter(pl.col("score") > 9).select("id").collect()["id"].to_list() == [7, 8, 9]


@STATES
def test_a_submit_over_a_held_input_reads_it_on_the_worker(backend, kept, must_materialise):
    upstream = submit(backend, kept, must_materialise=must_materialise).ref

    result = submit(
        backend,
        kept,
        must_materialise=True,
        plan=backend.plan_for(upstream).filter(pl.col("id") > 6),
        inputs=[upstream],
    )

    assert result.ref.row_count == 3


@STATES
def test_a_submit_over_a_released_input_names_it(backend, kept, must_materialise):
    upstream = submit(backend, kept, must_materialise=must_materialise).ref
    plan = backend.plan_for(upstream).filter(pl.col("id") > 6)
    backend.release(upstream)

    with pytest.raises(ResultMissing, match=upstream.result_id):
        submit(backend, kept, must_materialise=True, plan=plan, inputs=[upstream])


def test_a_task_route_result_in_a_plan_is_read_from_its_file_and_stays_on_the_task_routes(
    backend, kept, sent, tmp_path
):
    """The file read's result is not in the worker's ``/v1`` registry; had it gone as an input, the submit would 404."""
    csv_path = tmp_path / "people.csv"
    PEOPLE.write_csv(csv_path)
    received_table = ReceivedTable.create_from_path(str(csv_path), "csv")
    received_table.set_absolute_filepath()
    read = FileReadPayload(
        flow_id=1, node_id=1, settings=NodeRead(flow_id=1, node_id=1, received_file=received_table), node_logger=None
    )
    read_ref = backend.run_op(OperationKind.file_read, op_payload(read)).refs["result"]
    kept.append(read_ref)

    result = submit(
        backend,
        kept,
        must_materialise=True,
        plan=backend.plan_for(read_ref).filter(pl.col("id") > 6),
        inputs=[read_ref],
    )

    assert result.ref.row_count == 3
    sent.clear()
    assert backend.exists(read_ref) is True
    assert [path for _, path, _ in sent] == [f"/status/{read_ref.result_id}"]
    assert backend.result_file(read_ref) == read_ref.location.decode()
    assert backend.plan_for(read_ref).collect().equals(PEOPLE)


@STATES
def test_a_v1_result_is_read_over_v1_after_the_flag_goes_off(backend, kept, sent, must_materialise):
    ref = submit(backend, kept, must_materialise=must_materialise).ref
    settings.FLOWFILE_COMPUTE_PROTOCOL.value = False
    sent.clear()

    assert backend.exists(ref) is True
    assert read_stream(backend.sample(ref, 3)).num_rows == 3
    assert backend.count(ref) == 10
    backend.release(ref)
    assert backend.exists(ref) is False

    assert sent and all(path.startswith("/v1/results/") for _, path, _ in sent), sent


@STATES
def test_a_task_route_result_stays_on_the_task_routes_after_the_flag_goes_on(backend, kept, sent, must_materialise):
    settings.FLOWFILE_COMPUTE_PROTOCOL.value = False
    ref = submit(backend, kept, must_materialise=must_materialise).ref
    settings.FLOWFILE_COMPUTE_PROTOCOL.value = True
    sent.clear()

    assert backend.exists(ref) is True
    assert read_stream(backend.sample(ref, 3)).num_rows == 3
    assert backend.count(ref) == 10
    assert backend.plan_for(ref).collect().equals(PEOPLE)

    assert not ref.location.startswith(b"{")
    assert not any(path.startswith("/v1/") for _, path, _ in sent), sent


def test_a_plan_that_fails_on_the_worker_names_the_error(backend):
    with pytest.raises(TaskFailed, match="ColumnNotFoundError"):
        backend.submit(people().select("missing"), [], hints(True), new_result_id(), flow_id=-1, node_id=-1)


def test_a_plan_the_worker_cannot_load_is_rejected(backend, core_only_udf_plan):
    with pytest.raises(PlanRejected, match="core_only_udf"):
        backend.submit(core_only_udf_plan, [], hints(True), new_result_id(), flow_id=-1, node_id=-1)


def test_a_backend_on_another_protocol_version_is_refused_and_nothing_is_held(backend):
    result_id = new_result_id()
    backend.protocol_version = 2

    with pytest.raises(VersionMismatch, match=r"protocol version 1; the request carries 2"):
        backend.submit(people(), [], hints(True), result_id, flow_id=-1, node_id=-1)

    assert WorkerBackend(url=settings.WORKER_URL).exists(v1_ref(result_id)) is False


def test_a_worker_out_of_reach_is_unavailable_naming_it(running_worker, closed_port_url):
    dead = WorkerBackend(url=closed_port_url)
    unreachable = re.escape(f"Could not reach the worker at {closed_port_url}")

    with pytest.raises(ComputeUnavailable, match=unreachable):
        dead.submit(people(), [], hints(True), new_result_id(), flow_id=-1, node_id=-1)
    with pytest.raises(ComputeUnavailable, match=unreachable):
        dead.exists(v1_ref(new_result_id()))
    with pytest.raises(ComputeUnavailable, match=unreachable):
        dead.capabilities()


def test_status_follows_the_job_and_cancel_leaves_an_ended_one(backend, kept):
    handle = submit(backend, kept, must_materialise=True).handle

    assert backend.status(handle).state == "done"
    backend.cancel(handle)
    assert backend.status(handle).state == "done"


def test_a_failed_submit_leaves_a_failed_job_with_the_workers_message(backend):
    result_id = new_result_id()
    with pytest.raises(TaskFailed):
        backend.submit(people().select("missing"), [], hints(True), result_id, flow_id=-1, node_id=-1)

    status = backend.status(JobHandle("worker", result_id))

    assert status.state == "failed"
    assert "ColumnNotFoundError" in status.error


def test_a_job_the_worker_does_not_know_is_a_lookup_error(backend):
    with pytest.raises(LookupError, match="t-v1-backend-"):
        backend.status(JobHandle("worker", new_result_id()))


def test_capabilities_are_the_workers_answer_with_the_operations_run_op_runs(backend):
    capabilities = backend.capabilities()

    assert handshake_fields(capabilities) == handshake_fields(fetch_worker_capabilities(settings.WORKER_URL))
    assert capabilities.operations == {kind.value for kind in OperationKind}


@pytest.fixture
def remote_worker():
    from flowfile_core.schemas.schemas import is_valid_execution_location_in_current_global_settings
    from tests.conftest import is_worker_running

    if not (is_worker_running() and is_valid_execution_location_in_current_global_settings("remote")):
        pytest.skip("Remote execution not active")


def add_promise(graph: FlowGraph, node_id: int, node_type: str) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def test_a_remote_node_previews_the_inline_sample_without_reading_a_worker_file(remote_worker, monkeypatch):
    def no_file_read(*args, **kwargs):
        pytest.fail("core read a worker file for a /v1 result")

    monkeypatch.setattr(worker_module, "get_read_top_n", no_file_read)
    graph = FlowGraph(
        schemas.FlowSettings(
            flow_id=9701, name="v1_preview", path=".", execution_location="remote", execution_mode="Development"
        )
    )
    raw = input_schema.RawData.from_pylist([{"name": f"r{i}", "rank": i} for i in range(3, 0, -1)])
    add_promise(graph, 1, "manual_input")
    graph.add_manual_input(input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=1, raw_data_format=raw))
    add_promise(graph, 2, "sort")
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            sort_input=[transform_schema.SortByInput(column="rank", how="asc")],
        )
    )
    try:
        assert graph.run_graph().success

        sort = graph.get_node(2)
        assert location(sort.results.result_ref).kind == "file"
        assert sort.results.example_data_path is None
        assert [row["rank"] for row in sort.get_table_example(True).data] == [1, 2, 3]
    finally:
        graph.close_flow()
