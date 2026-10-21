"""With the protocol flag on, the worker backend submits plans, runs operations and reads results over ``/v1``.

Every test here turns ``settings.FLOWFILE_COMPUTE_PROTOCOL`` on and runs over both submit transports (``transport``):
``socket`` submits over ``WS /v1/ws/submit``, ``rest`` has every dial refused, as a proxy that passes no upgrade would,
so each submit falls back to ``POST /v1/submit``. The socket-only tests at the end play the worker's frames from a fake
socket. The dispatch tests turn the flag off again mid-test to show that a result is read over the routes that minted
it. The operation tests run the reads, the fuzzy join, the random split and a custom node over
``POST /v1/ops/{kind}`` (socket only: the transport is a submit's concern) and read every output over the wire; the
model tests train a model there, which the worker holds as a blob, and score with it by reference. The export and
import tests write a result to a URI on the worker and hold a URI there as a result, and hold a blob there, which no
call reads as rows.
"""

import dataclasses
import hashlib
import io
import json
import logging
import re
import socket
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest
import requests
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedError, InvalidStatus
from websockets.frames import Close
from websockets.http11 import Response

from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.configs import settings
from flowfile_core.flowfile.execution.backends import worker as worker_module
from flowfile_core.flowfile.execution.backends.local import LocalBackend
from flowfile_core.flowfile.execution.backends.worker import TOKEN_RULE, WorkerBackend, _flowfile_schema
from flowfile_core.flowfile.execution.errors import (
    ComputeUnavailable,
    ForeignRef,
    OperationUnsupported,
    PlanRejected,
    ResultMissing,
    TaskFailed,
    VersionMismatch,
)
from flowfile_core.flowfile.execution.payloads import (
    CustomNodePayload,
    DatabaseReadPayload,
    FileReadPayload,
    FuzzyJoinPayload,
    GoogleAnalyticsReadPayload,
    MlApplyPayload,
    MlTrainPayload,
    RandomSplitPayload,
    RestApiReadPayload,
    op_payload,
)
from flowfile_core.flowfile.execution.protocol import (
    CredentialRef,
    JobHandle,
    OperationKind,
    ResultRef,
    SubmitHints,
    SubmitResult,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import handshake
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.handshake import fetch_worker_capabilities
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.node_designer import CustomNodeBase
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.input_schema import (
    DatabaseConnection,
    DatabaseSettings,
    NodeDatabaseReader,
    NodeRead,
    ReceivedTable,
)
from flowfile_core.secret_manager.secret_manager import encrypt_secret
from shared import compute_protocol as wire
from shared.compute_protocol import PROTOCOL_VERSION_HEADER, WorkerLocation, file_uri
from shared.storage_config import storage

PEOPLE = pl.DataFrame(
    {"id": list(range(10)), "name": [f"n{i}" for i in range(10)], "score": [i * 1.5 for i in range(10)]}
)
GEOARROW_WKB = pl.Extension("geoarrow.wkb", pl.Binary, "{}")
BLOB = bytes(range(256)) * 64
STATES = pytest.mark.parametrize("must_materialise", [True, False], ids=["materialised", "deferred"])
SOCKET_ONLY = pytest.mark.parametrize("transport", ["socket"], indirect=True)


@pytest.fixture(autouse=True)
def protocol_on(monkeypatch):
    """The protocol flag is on for every test here and back to what it was when the test ends."""
    monkeypatch.setattr(settings.FLOWFILE_COMPUTE_PROTOCOL, "value", True)


@pytest.fixture(autouse=True, params=["socket", "rest"])
def transport(request, monkeypatch) -> str:
    """How a submit reaches the worker: its socket, or ``POST /v1/submit`` after every dial is refused 400.

    A refused upgrade is not a network failure, so the handshake is not re-armed by each submit.
    """
    if request.param == "rest":

        def refused(uri, **kwargs):
            raise InvalidStatus(Response(400, "Bad Request", Headers()))

        monkeypatch.setattr(worker_module, "connect", refused)
    return request.param


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
def sent(running_worker, transport, monkeypatch):
    """Every request core sends from here on, as (method, path on the worker, ``X-Protocol-Version``).

    A submit socket that opened counts as ``("WS", path, version)``. The handshake runs first, so its own calls are
    not among them.
    """
    handshake.ensure_handshake(settings.WORKER_URL)
    requests_sent: list[tuple[str, str, str | None]] = []
    real_send = requests.Session.send
    real_connect = worker_module.connect
    socket_base = settings.WORKER_URL.replace("http://", "ws://")

    def recording_send(self, request, **kwargs):
        path = request.url.removeprefix(settings.WORKER_URL)
        requests_sent.append((request.method, path, request.headers.get(PROTOCOL_VERSION_HEADER)))
        return real_send(self, request, **kwargs)

    def recording_connect(uri, **kwargs):
        ws = real_connect(uri, **kwargs)
        version = kwargs["additional_headers"].get(PROTOCOL_VERSION_HEADER)
        requests_sent.append(("WS", uri.removeprefix(socket_base), version))
        return ws

    monkeypatch.setattr(requests.Session, "send", recording_send)
    monkeypatch.setattr(worker_module, "connect", recording_connect)
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


def file_read_payload(tmp_path: Path) -> FileReadPayload:
    """A read of ``PEOPLE`` written as a CSV under ``tmp_path``, which the worker reads with its file reader."""
    csv_path = tmp_path / "people.csv"
    PEOPLE.write_csv(csv_path)
    received_table = ReceivedTable.create_from_path(str(csv_path), "csv")
    received_table.set_absolute_filepath()
    return FileReadPayload(
        flow_id=1, node_id=1, settings=NodeRead(flow_id=1, node_id=1, received_file=received_table), node_logger=None
    )


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


def test_every_call_goes_to_the_v1_routes_with_the_protocol_version(backend, kept, sent, transport):
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
        ("WS", "/v1/ws/submit") if transport == "socket" else ("POST", "/v1/submit"),
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
    settings.FLOWFILE_COMPUTE_PROTOCOL.value = False
    read_ref = backend.run_op(OperationKind.file_read, op_payload(file_read_payload(tmp_path))).refs["result"]
    kept.append(read_ref)
    settings.FLOWFILE_COMPUTE_PROTOCOL.value = True
    assert [path.partition("?")[0] for _, path, _ in sent if not path.startswith("/status/")] == ["/create_table/csv"]

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


@STATES
def test_export_goes_to_the_v1_export_route_and_the_worker_writes_the_uri(
    backend, kept, sent, must_materialise, tmp_path
):
    ref = submit(backend, kept, must_materialise=must_materialise).ref
    target = file_uri(tmp_path / "out.parquet")
    sent.clear()

    assert backend.export(ref, target, None) == target

    assert sent == [("POST", f"/v1/results/{ref.result_id}/export", "1")]
    assert pl.read_parquet(tmp_path / "out.parquet").equals(PEOPLE)
    assert backend.exists(ref) is True


def test_an_export_carries_only_the_credential_reference(backend, kept, monkeypatch, tmp_path):
    """Polars never calls the provider for a local file, so the export of a ``file://`` target still succeeds."""
    ref = submit(backend, kept, must_materialise=True).ref
    bodies = []
    real_send = requests.Session.send

    def recording_send(self, request, **kwargs):
        if request.url.endswith("/export"):
            bodies.append(json.loads(request.body))
        return real_send(self, request, **kwargs)

    monkeypatch.setattr(requests.Session, "send", recording_send)
    target = file_uri(tmp_path / "out.parquet")

    assert backend.export(ref, target, CredentialRef(connection_id=7, user_id=3)) == target

    assert bodies == [{"uri": target, "credential": {"connection_id": 7, "user_id": 3}}]
    assert pl.read_parquet(tmp_path / "out.parquet").equals(PEOPLE)


def test_import_uri_posts_to_the_v1_import_route_and_answers_a_deferred_v1_reference(backend, sent, tmp_path):
    """The file is written only after the import: the worker reads nothing until the rows are asked for."""
    path = tmp_path / "in.parquet"
    schema = _flowfile_schema(PEOPLE.schema)
    sent.clear()

    ref = backend.import_uri(file_uri(path), schema, PEOPLE.height, None)

    try:
        assert sent == [("POST", f"/v1/results/{ref.result_id}/import", "1")]
        assert (ref.backend_id, ref.state, ref.row_count) == ("worker", "deferred", PEOPLE.height)
        assert [(c.column_name, c.data_type) for c in ref.schema] == [(c.column_name, c.data_type) for c in schema]
        assert location(ref).kind == "plan"
        sent.clear()
        assert isinstance(backend.plan_for(ref), pl.LazyFrame)
        assert sent == []
        assert backend.count(ref) == PEOPLE.height

        PEOPLE.write_parquet(path)

        assert pl.from_arrow(read_stream(backend.sample(ref, 3))).equals(PEOPLE.head(3))
    finally:
        backend.release(ref)
    assert ("DELETE", f"/v1/results/{ref.result_id}", "1") in sent
    assert backend.exists(ref) is False
    assert path.is_file()


def test_an_export_of_a_result_the_worker_no_longer_holds_is_missing(backend, tmp_path):
    gone = v1_ref(new_result_id())

    with pytest.raises(ResultMissing, match=gone.result_id):
        backend.export(gone, file_uri(tmp_path / "out.parquet"), None)
    assert not (tmp_path / "out.parquet").exists()


@STATES
def test_a_v1_result_is_exported_over_v1_after_the_flag_goes_off(backend, kept, sent, must_materialise, tmp_path):
    ref = submit(backend, kept, must_materialise=must_materialise).ref
    settings.FLOWFILE_COMPUTE_PROTOCOL.value = False
    target = file_uri(tmp_path / "out.parquet")
    sent.clear()

    assert backend.export(ref, target, None) == target

    assert [(method, path) for method, path, _ in sent] == [("POST", f"/v1/results/{ref.result_id}/export")]
    assert pl.read_parquet(tmp_path / "out.parquet").equals(PEOPLE)


def test_a_blob_import_posts_kind_blob_and_answers_a_blob_reference(backend, sent, monkeypatch, tmp_path):
    """The source is removed once imported, so what the worker exports is the copy it holds."""
    source = tmp_path / "model.bin"
    source.write_bytes(BLOB)
    bodies = []
    recording_send = requests.Session.send

    def body_recording_send(self, request, **kwargs):
        if request.url.endswith("/import"):
            bodies.append(json.loads(request.body))
        return recording_send(self, request, **kwargs)

    monkeypatch.setattr(requests.Session, "send", body_recording_send)
    sent.clear()

    ref = backend.import_uri(file_uri(source), None, None, None, kind="blob")

    try:
        assert sent == [("POST", f"/v1/results/{ref.result_id}/import", "1")]
        assert bodies == [
            {"uri": file_uri(source), "credential": None, "columns": [], "row_count": None, "kind": "blob"}
        ]
        assert (ref.backend_id, ref.kind, ref.state, ref.schema, ref.row_count) == (
            "worker",
            "blob",
            "materialised",
            [],
            None,
        )
        assert location(ref).kind == "blob"
        source.unlink()
        sent.clear()
        for read in (backend.plan_for, backend.count, lambda blob: backend.sample(blob, 1)):
            with pytest.raises(OperationUnsupported, match="is a blob"):
                read(ref)
        assert sent == []
        target = tmp_path / "copy.bin"
        assert backend.export(ref, file_uri(target), None) == file_uri(target)
        assert sent == [("POST", f"/v1/results/{ref.result_id}/export", "1")]
        assert target.read_bytes() == BLOB
    finally:
        backend.release(ref)
    assert ("DELETE", f"/v1/results/{ref.result_id}", "1") in sent
    assert backend.exists(ref) is False
    assert target.read_bytes() == BLOB


def blob_ref(result_id: str) -> ResultRef:
    """A blob reference as ``/v1`` mints one, for a blob the worker never held."""
    nowhere = WorkerLocation(kind="blob", path="/nowhere.blob").model_dump_json().encode()
    return ResultRef("worker", result_id, [], None, "materialised", nowhere, kind="blob")


@pytest.mark.parametrize(
    "given",
    [{"schema": []}, {"row_count": 0}, {"credential": CredentialRef(connection_id=7, user_id=3)}],
    ids=["schema", "row_count", "credential"],
)
def test_a_blob_takes_no_credential(backend, sent, tmp_path, given):
    arguments = {"schema": None, "row_count": None, "credential": None, **given}
    sent.clear()

    with pytest.raises(ValueError, match="A blob import takes no"):
        backend.import_uri(file_uri(tmp_path / "model.bin"), **arguments, kind="blob")
    with pytest.raises(ValueError, match="takes no credential"):
        backend.export(blob_ref(new_result_id()), file_uri(tmp_path / "out.bin"), CredentialRef(7, 3))

    assert sent == []


def test_a_table_import_needs_its_schema(backend, sent, tmp_path):
    sent.clear()

    with pytest.raises(ValueError, match="needs its schema"):
        backend.import_uri(file_uri(tmp_path / "in.parquet"), None, 10, None)

    assert sent == []


def test_a_blob_among_a_submits_inputs_is_refused(backend, sent):
    sent.clear()

    with pytest.raises(OperationUnsupported, match="is a blob"):
        backend.submit(people(), [blob_ref(new_result_id())], hints(True), new_result_id(), flow_id=-1, node_id=-1)

    assert sent == []


def test_a_plan_that_fails_on_the_worker_names_the_error(backend):
    with pytest.raises(TaskFailed, match="ColumnNotFoundError"):
        backend.submit(people().select("missing"), [], hints(True), new_result_id(), flow_id=-1, node_id=-1)


def test_a_plan_the_worker_cannot_load_is_rejected(backend, core_only_udf_plan):
    with pytest.raises(PlanRejected, match="core_only_udf"):
        backend.submit(core_only_udf_plan, [], hints(True), new_result_id(), flow_id=-1, node_id=-1)


def test_a_backend_on_another_protocol_version_is_refused_and_nothing_is_held(backend, sent, transport):
    """A 426 on the socket's upgrade is final: the plan is not posted instead."""
    result_id = new_result_id()
    backend.protocol_version = 2

    with pytest.raises(VersionMismatch, match=r"protocol version 1; the request carries 2"):
        backend.submit(people(), [], hints(True), result_id, flow_id=-1, node_id=-1)

    submits = [(method, path) for method, path, _ in sent if "submit" in path]
    assert submits == ([] if transport == "socket" else [("POST", "/v1/submit")])
    assert WorkerBackend(url=settings.WORKER_URL).exists(v1_ref(result_id)) is False


def test_a_worker_out_of_reach_is_unavailable_naming_it(running_worker, closed_port_url):
    """The URL and the backend's id: from the gate at submit, from ``_v1`` past a gate another worker passed, and
    from ``capabilities``."""
    dead = WorkerBackend(url=closed_port_url, backend_id="worker:team")
    unreachable = re.escape(f"Could not reach the worker at {closed_port_url}")
    hint = 'Is backend "worker:team" running? Start it, or check the Backends page.'

    with pytest.raises(ComputeUnavailable, match=unreachable) as at_submit:
        dead.submit(people(), [], hints(True), new_result_id(), flow_id=-1, node_id=-1)
    handshake.ensure_handshake(settings.WORKER_URL)
    with pytest.raises(ComputeUnavailable, match=unreachable) as past_the_gate:
        dead.exists(dataclasses.replace(v1_ref(new_result_id()), backend_id="worker:team"))
    with pytest.raises(ComputeUnavailable, match=unreachable) as reading_capabilities:
        dead.capabilities()

    for raised in (at_submit, past_the_gate, reading_capabilities):
        assert str(raised.value).endswith(hint)


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

    assert capabilities == fetch_worker_capabilities(settings.WORKER_URL)
    assert capabilities.operations == {kind.value for kind in OperationKind}


def test_a_backend_with_its_own_token_signs_every_v1_call_and_the_socket_with_it(
    running_worker, transport, monkeypatch
):
    """The session worker knows only the process's token, so it answers 401 to every call this backend signs."""
    handshake.ensure_handshake(settings.WORKER_URL)
    tokens: list[str | None] = []
    real_send = requests.Session.send

    def recording_send(self, request, **kwargs):
        tokens.append(request.headers.get("X-Flowfile-Internal"))
        return real_send(self, request, **kwargs)

    def recording_refusal(uri, **kwargs):
        tokens.append(kwargs["additional_headers"].get("X-Flowfile-Internal"))
        raise InvalidStatus(Response(400, "Bad Request", Headers()))

    monkeypatch.setattr(requests.Session, "send", recording_send)
    monkeypatch.setattr(worker_module, "connect", recording_refusal)
    team = WorkerBackend(url=settings.WORKER_URL, backend_id="worker:team", token="row-token")

    with pytest.raises(TaskFailed, match="The worker answered 401"):
        team.capabilities()
    with pytest.raises(TaskFailed, match="The worker answered 401"):
        team.submit(people(), [], hints(True), new_result_id(), flow_id=-1, node_id=-1)

    assert tokens == ["row-token"] * 3, "capabilities, the socket's upgrade, then the post"

    tokens.clear()
    WorkerBackend(url=settings.WORKER_URL).capabilities()

    assert tokens == [get_internal_token()]


@SOCKET_ONLY
@pytest.mark.parametrize(
    "token", ["row-token\n", "row-token\x00", "row-token\u20ac"], ids=["line-feed", "nul", "beyond-latin-1"]
)
def test_a_token_that_is_no_header_value_fails_each_call_typed_and_is_never_quoted(
    running_worker, token, monkeypatch, caplog
):
    """``http.client`` quotes such a token in its error and ``websockets`` logs it, so neither is ever handed it."""
    dialled = []
    real_dial = worker_module.connect

    def recording_dial(uri, **kwargs):
        dialled.append(uri)
        return real_dial(uri, **kwargs)

    monkeypatch.setattr(worker_module, "connect", recording_dial)
    team = WorkerBackend(url=settings.WORKER_URL, backend_id="worker:team", token=token)
    calls = {
        "capabilities": team.capabilities,
        "submit": lambda: team.submit(people(), [], hints(True), new_result_id(), flow_id=-1, node_id=-1),
        "status": lambda: team.status(JobHandle("worker:team", new_result_id())),
    }

    with caplog.at_level(logging.DEBUG):
        for name, call in calls.items():
            with pytest.raises(TaskFailed) as failed:
                call()
            assert str(failed.value) == f"The token configured for the worker:team backend cannot be sent: {TOKEN_RULE}"
            assert "row-token" not in "".join(traceback.format_exception(failed.type, failed.value, failed.tb)), name

    assert dialled == [], "the submit socket is never dialled with it"
    assert "row-token" not in caplog.text


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


SPLIT_NODE_SOURCE = """
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase


class WorkerBackendSplit(CustomNodeBase):
    node_name: str = "Worker Backend Split"
    number_of_outputs: int = 2
    output_names: list[str] = ["big", "small"]

    def process(self, *inputs):
        rows = inputs[0]
        return {"big": rows.filter(pl.col("id") > 6), "small": rows.filter(pl.col("id") <= 6)}
"""

SLEEPING_NODE_SOURCE = """
import time

from flowfile_core.flowfile.node_designer import CustomNodeBase


class WorkerBackendSleep(CustomNodeBase):
    node_name: str = "Worker Backend Sleep"

    def process(self, *inputs):
        time.sleep(30)
        return inputs[0]
"""


class PlacedSplit(CustomNodeBase):
    """The class the split node was placed with in core; it selects no secret."""

    node_name: str = "Worker Backend Split"
    number_of_outputs: int = 2
    output_names: list[str] = ["big", "small"]


class PlacedSleep(CustomNodeBase):
    """The class the sleeping node was placed with in core."""

    node_name: str = "Worker Backend Sleep"


@pytest.fixture
def no_file_read_in_core(monkeypatch):
    """Fails the test when core reads a schema, a count or rows from a worker file, as the task routes do."""

    def no_file_read(*args, **kwargs):
        pytest.fail("core read a worker file for an operation's output")

    for name in ("_row_count", "_read_sample", "get_read_top_n"):
        monkeypatch.setattr(worker_module, name, no_file_read)
    monkeypatch.setattr(WorkerBackend, "_result_ref", no_file_read)


@pytest.fixture
def postgres():
    from test_utils.postgres.fixtures import can_connect_to_db

    if not can_connect_to_db():
        pytest.skip("the test PostgreSQL database is not running")


@pytest.fixture
def json_api():
    """A JSON API on a free loopback port, answering every GET with the rows of ``PEOPLE``."""
    body = json.dumps(PEOPLE.to_dicts()).encode()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def sampled(backend: WorkerBackend, ref: ResultRef) -> pl.DataFrame:
    return pl.from_arrow(read_stream(backend.sample(ref, 1000)))


def fuzzy_settings() -> input_schema.NodeFuzzyMatch:
    return input_schema.NodeFuzzyMatch(
        flow_id=1,
        node_id=2,
        join_input={
            "join_mapping": [
                {"left_col": "name", "right_col": "name", "threshold_score": 75, "fuzzy_type": "levenshtein"}
            ],
            "left_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
            "right_select": {"renames": [{"old_name": "name", "new_name": "name", "join_key": True}]},
            "how": "inner",
        },
    )


def custom_node_payload(source: str, class_name: str, placed: CustomNodeBase, output_names: list[str]):
    return CustomNodePayload(
        inputs=(pl.LazyFrame({"id": list(range(10))}),),
        custom_node=placed,
        node_source=source,
        class_name=class_name,
        settings_values={},
        output_names=output_names,
        user_id=None,
        flow_id=1,
        node_id=4,
        job_id=f"{new_result_id()}-custom-node",
    )


@SOCKET_ONLY
def test_a_file_read_runs_over_v1_and_its_result_is_read_over_the_wire(
    backend, kept, sent, tmp_path, no_file_read_in_core
):
    read = file_read_payload(tmp_path)

    result = backend.run_op(OperationKind.file_read, op_payload(read))

    ref = result.refs["result"]
    kept.append(ref)
    assert (list(result.refs), result.metadata) == (["result"], {})
    assert (ref.result_id, ref.state, ref.row_count) == (f"{read.job_id}_0", "materialised", 10)
    assert location(ref).kind == "file"
    assert backend.result_file(ref) is None
    assert [column.column_name for column in ref.schema] == ["id", "name", "score"]
    assert pl.from_arrow(read_stream(backend.sample(ref, 3))).equals(PEOPLE.head(3))
    assert backend.count(ref) == 10
    assert backend.exists(ref) is True
    backend.release(ref)
    assert backend.exists(ref) is False
    paths = [path for _, path, _ in sent]
    assert paths[0] == f"/v1/ops/file_read?job_id={read.job_id}&flowfile_flow_id=1&flowfile_node_id=1"
    assert all(path.startswith(f"/v1/results/{ref.result_id}") for path in paths[1:]), paths


@SOCKET_ONLY
def test_a_database_read_runs_over_v1(backend, kept, postgres, no_file_read_in_core):
    connection = DatabaseConnection(
        database_type="postgresql", username="testuser", host="localhost", port=5433, database="testdb"
    )
    settings_ = NodeDatabaseReader(
        flow_id=1,
        node_id=1,
        database_settings=DatabaseSettings(database_connection=connection, schema_name="public", table_name="movies"),
    )
    read = DatabaseReadPayload(
        flow_id=1,
        node_id=1,
        settings=settings_,
        connection=connection,
        encrypted_password=encrypt_secret("testpass", user_id=1),
        cancel_check=lambda: False,
    )

    ref = backend.run_op(OperationKind.database_read, op_payload(read)).refs["result"]
    kept.append(ref)

    assert (location(ref).kind, ref.result_id) == ("file", f"{read.job_id}_0")
    assert ref.row_count > 0
    assert read_stream(backend.sample(ref, 5)).num_rows == 5


@SOCKET_ONLY
def test_a_rest_api_read_runs_over_v1(backend, kept, json_api, no_file_read_in_core):
    node = input_schema.NodeRestApiReader(
        flow_id=1, node_id=1, user_id=1, rest_api_settings=input_schema.RestApiSettings(url=f"{json_api}/people")
    )
    read = RestApiReadPayload(flow_id=1, node_id=1, settings=node, encrypted_secret=None)

    ref = backend.run_op(OperationKind.rest_api_read, op_payload(read)).refs["result"]
    kept.append(ref)

    assert (location(ref).kind, ref.row_count) == ("file", 10)
    assert sampled(backend, ref)["name"].to_list() == PEOPLE["name"].to_list()


@SOCKET_ONLY
def test_a_read_that_fails_on_the_worker_names_the_error_and_fails_its_job(backend):
    node = input_schema.NodeGoogleAnalyticsReader(
        flow_id=1,
        node_id=1,
        google_analytics_settings=input_schema.GoogleAnalyticsSettings(
            ga_connection_name="none", property_id="123", metrics=["sessions"], dimensions=["date"]
        ),
    )
    read = GoogleAnalyticsReadPayload(
        flow_id=1, node_id=1, settings=node, auth_method="oauth", encrypted_credential="unused"
    )

    with pytest.raises(TaskFailed, match="OAuth client credentials missing"):
        backend.run_op(OperationKind.google_analytics_read, op_payload(read))

    assert backend.status(JobHandle("worker", read.job_id)).state == "failed"


@SOCKET_ONLY
def test_a_fuzzy_join_runs_over_v1_and_matches_as_it_does_in_core(backend, kept, no_file_read_in_core):
    names = pl.LazyFrame({"name": ["eduward", "edward", "courtney"]})
    join = FuzzyJoinPayload(
        left=names, right=names, settings=fuzzy_settings(), flow_id=1, node_id=2, job_id=f"{new_result_id()}_fm"
    )

    ref = backend.run_op(OperationKind.fuzzy_join, op_payload(join)).refs["result"]
    kept.append(ref)

    assert (ref.result_id, ref.state, ref.row_count) == (f"{join.job_id}_0", "materialised", 5)
    in_core = LocalBackend()
    local_ref = in_core.run_op(OperationKind.fuzzy_join, op_payload(join)).refs["result"]
    columns = ["name", "name_right", "name_vs_name_right_levenshtein"]
    assert sampled(backend, ref).sort(columns).equals(in_core.plan_for(local_ref).collect().sort(columns))
    assert backend.status(JobHandle("worker", join.job_id)).state == "done"


@SOCKET_ONLY
def test_a_fuzzy_join_over_results_the_worker_holds_resolves_neither_side_in_core(
    backend, kept, no_file_read_in_core, monkeypatch
):
    names = pl.LazyFrame({"name": ["eduward", "edward", "courtney"]})
    stored = submit(backend, kept, must_materialise=True, plan=names).ref
    join = FuzzyJoinPayload(
        left=backend.plan_for(stored),
        right=backend.plan_for(stored),
        left_schema=stored.schema,
        right_schema=stored.schema,
        settings=fuzzy_settings(),
        flow_id=1,
        node_id=2,
        job_id=f"{new_result_id()}_fm",
    )

    def no_schema_resolved(*args, **kwargs):
        pytest.fail("core resolved a side of the fuzzy join")

    with monkeypatch.context() as patched:
        patched.setattr(pl.LazyFrame, "collect_schema", no_schema_resolved)
        ref = backend.run_op(OperationKind.fuzzy_join, op_payload(join)).refs["result"]
    kept.append(ref)

    assert (ref.state, ref.row_count) == ("materialised", 5)
    in_core = LocalBackend()
    in_memory = dataclasses.replace(join, left=names, right=names, left_schema=None, right_schema=None)
    local_ref = in_core.run_op(OperationKind.fuzzy_join, op_payload(in_memory)).refs["result"]
    columns = ["name", "name_right", "name_vs_name_right_levenshtein"]
    assert sampled(backend, ref).sort(columns).equals(in_core.plan_for(local_ref).collect().sort(columns))


@SOCKET_ONLY
def test_a_random_split_runs_over_v1_and_partitions_like_core(backend, kept, no_file_read_in_core):
    settings_ = input_schema.NodeRandomSplit(flow_id=1, node_id=3, seed=11)
    split = RandomSplitPayload(
        plan=pl.LazyFrame({"n": list(range(50))}), settings=settings_, flow_id=1, node_id=3, job_id=new_result_id()
    )

    refs = backend.run_op(OperationKind.random_split, op_payload(split)).refs
    kept.extend(refs.values())

    assert list(refs) == ["train", "test"]
    assert [(ref.result_id, ref.state, ref.row_count) for ref in refs.values()] == [
        (f"{split.job_id}_0", "materialised", 40),
        (f"{split.job_id}_1", "materialised", 10),
    ]
    on_worker = {name: sampled(backend, ref)["n"].to_list() for name, ref in refs.items()}
    in_core = LocalBackend()
    local_refs = in_core.run_op(OperationKind.random_split, op_payload(split)).refs
    assert on_worker == {name: in_core.plan_for(ref).collect()["n"].to_list() for name, ref in local_refs.items()}
    assert backend.status(JobHandle("worker", split.job_id)).state == "done"


@SOCKET_ONLY
def test_a_custom_node_runs_over_v1_and_holds_each_output(backend, kept, no_file_read_in_core):
    job = custom_node_payload(SPLIT_NODE_SOURCE, "WorkerBackendSplit", PlacedSplit(), ["big", "small"])

    refs = backend.run_op(OperationKind.custom_node, op_payload(job)).refs
    kept.extend(refs.values())

    assert list(refs) == ["output-0", "output-1"]
    assert [(ref.result_id, ref.state, ref.row_count) for ref in refs.values()] == [
        (f"{job.job_id}_0", "materialised", 3),
        (f"{job.job_id}_1", "materialised", 7),
    ]
    assert sampled(backend, refs["output-0"])["id"].to_list() == [7, 8, 9]
    assert sampled(backend, refs["output-1"])["id"].to_list() == list(range(7))
    assert backend.status(JobHandle("worker", job.job_id)).state == "done"

    backend.release(refs["output-0"])

    assert backend.exists(refs["output-0"]) is False
    assert backend.exists(refs["output-1"]) is True


@SOCKET_ONLY
def test_a_cancelled_operation_fails_and_its_job_is_cancelled(backend):
    job = custom_node_payload(SLEEPING_NODE_SOURCE, "WorkerBackendSleep", PlacedSleep(), ["main"])
    handle = JobHandle("worker", job.job_id)

    def cancel_while_running():
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                state = backend.status(handle).state
            except LookupError:
                state = None
            if state == "running":
                backend.cancel(handle)
            elif state is not None:
                return
            time.sleep(0.1)

    canceller = threading.Thread(target=cancel_while_running)
    canceller.start()
    with pytest.raises(TaskFailed):
        backend.run_op(OperationKind.custom_node, op_payload(job))
    canceller.join(timeout=30)

    assert backend.status(handle).state == "cancelled"


def ml_train_payload() -> MlTrainPayload:
    """A linear fit of ``y = 2x + 1`` on four rows."""
    return MlTrainPayload(
        plan=pl.LazyFrame({"x": [1.0, 2.0, 3.0, 4.0], "y": [3.0, 5.0, 7.0, 9.0]}),
        model_type="linear_regression",
        target_column="y",
        feature_columns=["x"],
        params={"add_bias": True},
        flow_id=1,
        node_id=5,
        job_id=new_result_id(),
    )


def ml_apply_payload(model: ResultRef) -> MlApplyPayload:
    """A score of ``x = 10`` with ``model`` into ``pred``."""
    return MlApplyPayload(
        plan=pl.LazyFrame({"x": [10.0]}),
        model=model,
        output_column="pred",
        flow_id=1,
        node_id=6,
        job_id=new_result_id(),
    )


def test_a_model_is_trained_and_applied_over_the_v1_operations(backend, kept, sent, no_file_read_in_core, tmp_path):
    train = ml_train_payload()
    sent.clear()

    trained = backend.run_op(OperationKind.ml_train, op_payload(train))

    model = trained.refs["model"]
    kept.append(model)
    assert list(trained.refs) == ["model"]
    assert (model.result_id, model.kind, model.state, model.schema, model.row_count) == (
        f"{train.job_id}_0",
        "blob",
        "materialised",
        [],
        None,
    )
    assert location(model).kind == "blob"
    assert trained.metadata["model_type"] == "linear_regression"
    target = tmp_path / "model.json"
    backend.export(model, file_uri(target), None)
    exported = target.read_bytes()
    assert (len(exported), hashlib.sha256(exported).hexdigest()) == (
        trained.metadata["size_bytes"],
        trained.metadata["sha256"],
    )

    apply = ml_apply_payload(model)
    scored = backend.run_op(OperationKind.ml_apply, op_payload(apply)).refs["result"]
    kept.append(scored)

    assert (scored.result_id, scored.state, scored.row_count) == (f"{apply.job_id}_0", "materialised", 1)
    assert [column.column_name for column in scored.schema] == ["x", "pred"]
    assert sampled(backend, scored)["pred"].round(6).to_list() == [21.0]
    assert backend.exists(model) is True, "scoring leaves the model held"
    assert [(method, path.partition("?")[0]) for method, path, _ in sent] == [
        ("POST", "/v1/ops/ml_train"),
        ("POST", f"/v1/results/{model.result_id}/export"),
        ("POST", "/v1/ops/ml_apply"),
        ("GET", f"/v1/results/{scored.result_id}/sample"),
        ("HEAD", f"/v1/results/{model.result_id}"),
    ]


def test_an_apply_whose_model_the_worker_no_longer_holds_is_missing(backend):
    model = backend.run_op(OperationKind.ml_train, op_payload(ml_train_payload())).refs["model"]
    backend.release(model)
    apply = ml_apply_payload(model)

    with pytest.raises(ResultMissing, match=model.result_id):
        backend.run_op(OperationKind.ml_apply, op_payload(apply))

    with pytest.raises(LookupError):
        backend.status(JobHandle("worker", apply.job_id))


def test_an_apply_with_another_backends_model_is_refused_before_any_call(backend, sent):
    foreign = ResultRef("local", "elsewhere", [], None, "materialised", b"/elsewhere.blob", kind="blob")
    sent.clear()

    with pytest.raises(ForeignRef, match="elsewhere"):
        backend.run_op(OperationKind.ml_apply, op_payload(ml_apply_payload(foreign)))

    assert sent == []


class FakeClock:
    """``worker_module.monotonic`` for the silence tests: time moves only when the fake socket lets it."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class FakeSocket:
    """A submit socket the test plays the worker's side of: ``recv`` plays ``script``; ``send`` and ``close`` are kept.

    ``"timeout"`` in the script, and an exhausted script, let ``gap`` seconds pass on ``clock`` and raise
    ``TimeoutError``; an exception is raised; anything else is the frame received. ``refuse_plan`` is raised by the
    send of the plan, the binary frame.
    """

    def __init__(self, script=(), *, clock: FakeClock | None = None, gap: float = 0.0, refuse_plan=None):
        self.script = list(script)
        self.clock = clock
        self.gap = gap
        self.refuse_plan = refuse_plan
        self.frames_sent: list[str | bytes] = []
        self.closed = False

    def send(self, message):
        if isinstance(message, bytes) and self.refuse_plan is not None:
            raise self.refuse_plan
        self.frames_sent.append(message)

    def recv(self, timeout=None):
        item = self.script.pop(0) if self.script else "timeout"
        if item == "timeout":
            if self.clock is not None:
                self.clock.now += self.gap
            raise TimeoutError
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        self.closed = True


@pytest.fixture
def clock(monkeypatch) -> FakeClock:
    fake = FakeClock()
    monkeypatch.setattr(worker_module, "monotonic", fake)
    return fake


def dial_into(monkeypatch, socket: FakeSocket) -> list[tuple[str, dict]]:
    """Make every dial of the submit socket open ``socket``; the dials made, as (URI, keyword arguments)."""
    dials: list[tuple[str, dict]] = []

    def fake_connect(uri, **kwargs):
        dials.append((uri, kwargs))
        return socket

    monkeypatch.setattr(worker_module, "connect", fake_connect)
    return dials


def complete_frame(result_id: str, plan: pl.LazyFrame) -> str:
    """The ``complete`` frame the worker sends for ``plan`` held deferred as ``result_id``."""
    held = WorkerLocation(kind="plan", plan=plan.serialize()).model_dump_json().encode()
    answer = wire.SubmitResult(
        ref=wire.ResultRef(result_id=result_id, columns=[], row_count=None, state="deferred", location=held),
        job_id=result_id,
    )
    return json.dumps({"type": "complete", "result": answer.model_dump(mode="json")})


@SOCKET_ONLY
def test_a_complete_frame_over_one_mebibyte_is_read_whole(backend, kept, sent):
    """A deferred result's ``complete`` frame carries its plan, base64 inside base64, so it outgrows any frame cap."""
    plan = pl.LazyFrame({"n": pl.int_range(150_000, eager=True)})

    result = submit(backend, kept, must_materialise=False, preview_rows=3, plan=plan)

    ref = result.ref
    assert ref.state == "deferred"
    assert len(ref.location) > 2**20
    assert location(ref).plan == plan.serialize()
    assert read_stream(result.sample).num_rows == 3
    assert [(method, path) for method, path, _ in sent] == [("WS", "/v1/ws/submit")]


@SOCKET_ONLY
def test_a_plan_the_socket_could_not_send_is_posted_instead(backend, kept, sent, monkeypatch):
    socket = FakeSocket(refuse_plan=ConnectionClosedError(None, None))
    dial_into(monkeypatch, socket)

    ref = submit(backend, kept, must_materialise=True).ref

    assert ref.row_count == 10
    assert socket.closed
    assert [(method, path) for method, path, _ in sent] == [("POST", "/v1/submit")]


@SOCKET_ONLY
def test_a_silent_worker_fails_the_submit_without_posting_it(backend, sent, monkeypatch, clock):
    monkeypatch.setattr(worker_module, "_WS_INACTIVITY_TIMEOUT", 0.05)
    socket = FakeSocket(clock=clock, gap=0.01)
    dial_into(monkeypatch, socket)

    with pytest.raises(TaskFailed, match="FLOWFILE_WORKER_WS_TIMEOUT"):
        backend.submit(people(), [], hints(True), new_result_id(), flow_id=-1, node_id=-1)

    assert socket.closed
    assert sent == []


@SOCKET_ONLY
def test_progress_frames_keep_the_socket_alive(backend, monkeypatch, clock):
    """Each silence (0.1) stays under the timeout (0.25) and the three add up past it, so only a reset passes."""
    monkeypatch.setattr(worker_module, "_WS_INACTIVITY_TIMEOUT", 0.25)
    result_id = new_result_id()
    plan = people()
    progress = json.dumps({"type": "progress", "progress": 50})
    script = ["timeout", progress, "timeout", progress, "timeout", complete_frame(result_id, plan)]
    socket = FakeSocket(script, clock=clock, gap=0.1)
    dial_into(monkeypatch, socket)

    ref = backend.submit(plan, [], hints(False), result_id, flow_id=-1, node_id=-1).ref

    assert (ref.result_id, ref.state) == (result_id, "deferred")
    assert location(ref).plan == plan.serialize()
    assert json.loads(socket.frames_sent[0])["result_id"] == result_id
    assert socket.frames_sent[1] == plan.serialize()


@SOCKET_ONLY
def test_a_socket_closed_after_the_plan_is_unavailable_and_not_posted(backend, sent, monkeypatch):
    socket = FakeSocket([ConnectionClosedError(Close(1011, "internal error"), None)])
    dial_into(monkeypatch, socket)

    with pytest.raises(ComputeUnavailable, match=re.escape(settings.WORKER_URL)):
        backend.submit(people(), [], hints(True), new_result_id(), flow_id=-1, node_id=-1)

    assert socket.closed
    assert sent == []


@SOCKET_ONLY
@pytest.mark.parametrize(
    ("worker_url", "socket_url"),
    [
        ("http://127.0.0.1:63578/worker", "ws://127.0.0.1:63578/worker/v1/ws/submit"),
        ("https://flowfile.example/worker", "wss://flowfile.example/worker/v1/ws/submit"),
    ],
    ids=["single-file", "tls"],
)
def test_the_socket_is_dialled_under_the_workers_url_with_the_token_and_version(
    running_worker, monkeypatch, worker_url, socket_url
):
    handshake.ensure_handshake(settings.WORKER_URL)
    result_id = new_result_id()
    dials = dial_into(monkeypatch, FakeSocket([complete_frame(result_id, people())]))

    WorkerBackend(url=worker_url).submit(people(), [], hints(False), result_id, flow_id=-1, node_id=-1)

    headers = {"X-Flowfile-Internal": get_internal_token(), PROTOCOL_VERSION_HEADER: "1"}
    assert dials == [(socket_url, {"additional_headers": headers, "max_size": None, "ssl": None})]
