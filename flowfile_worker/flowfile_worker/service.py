"""The bodies of the worker's ``/v1`` routes: one plain function per route over the wire models.

``protocol_routes.py`` mounts each function as its route, and core's embedded worker adapter calls the same functions
directly when the worker runs in core's own process, so one body serves both the wire and the direct call. Every
function raises the ``HTTPException`` its route answers: 404 for a result or input this worker does not hold, 422 for
what the body refuses before anything runs, and a 500 carrying a child's ``status``, ``error_message`` and
``error_kind`` (``_failure``) when the child did not complete. Nothing here reads a request or writes a response.

``run_submit`` runs the plan in a ``funcs.submit_result`` child and keeps the result in ``results.results``, written
to a file or deferred as ``_materialise`` decides; a deferred submit with no preview runs in the child too, which
resolves the plan's columns and collects nothing. ``sample``, ``count``, ``exists`` and ``release`` read, count, probe
and release what it holds. A submit's job id is its result id, and every submit first drops what has been held for
``CACHE_EXPIRATION_TIME``. The twelve operations run a read, a fuzzy join, a random split, a custom node, a model's
training or its apply in the child its task route spawns, as the job the ``job_id`` names, and hold each output that
child wrote as the materialised result ``<job_id>_<i>``, which the same functions read, count, probe and release. A
training's output is the model it fitted, held as a blob; an apply scores with a model held here, named by its result
id. ``polars_code`` runs a Polars Code node's code, and ``sql_query`` translates a SQL Query node's statement, once in a
pooled child and holds the result as ``<job_id>_0``: deferred, as its plan, when the code answered a LazyFrame over
sources, materialised in its file when it answered a DataFrame or a LazyFrame over rows in memory.
``export`` writes a held result to a URI as parquet in a ``funcs.export_result`` child; ``import_uri`` holds a scan of a
URI as a deferred result, with the columns and row count core sent and nothing read. A credential crosses either as a
stored connection's id and user, resolved where the plan runs; an import whose connection cannot be read from the
catalog answers the 500 a failed export does. A blob, a held file of bytes that is not rows, takes the same two
functions: its export copies the file to a ``file://`` URI or ``PUT``s it to a presigned ``https://`` URL, and an import
with ``kind="blob"`` copies or ``GET``s one into a blob of this worker's own, in this process; a presigned URL carries
its own authorisation, so neither takes a credential. ``cancel_job`` stops a job's child, and takes a job still
``Starting`` too, which then ends as a cancel when it starts. Where a child runs is the spawner's: with
``spawner.in_process`` set, every job here runs in the calling process instead.
"""

import io
import json
import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

import polars as pl
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from flowfile_worker import CACHE_EXPIRATION_TIME, PROCESS_MEMORY_USAGE, models, status_dict, status_dict_lock
from flowfile_worker.configs import logger
from flowfile_worker.create import table_creator_factory_method
from flowfile_worker.create.models import ReceivedTable
from flowfile_worker.custom_node_runner import _output_path
from flowfile_worker.external_sources.google_analytics_source.main import read_google_analytics
from flowfile_worker.external_sources.google_analytics_source.models import GoogleAnalyticsReadSettings
from flowfile_worker.external_sources.kafka_source.main import read_kafka
from flowfile_worker.external_sources.rest_api_source.main import read_rest_api
from flowfile_worker.external_sources.rest_api_source.models import RestApiReadSettings
from flowfile_worker.external_sources.sql_source.main import read_sql_source
from flowfile_worker.external_sources.sql_source.models import DatabaseReadSettings
from flowfile_worker.results import ResultRecord, result_path, results
from flowfile_worker.spawner import (
    process_manager,
    start_apply_model_process,
    start_custom_node_process,
    start_fuzzy_process,
    start_generic_process,
    start_process,
    start_train_model_process,
)
from flowfile_worker.task_errors import describe_exception
from shared.cloud_credential_provider import CredentialResolutionError, credential_ref_kwargs
from shared.compute_protocol import (
    CLOUD_SCHEMES,
    Capabilities,
    ResultRef,
    SubmitHints,
    SubmitResult,
    WireColumn,
    file_uri_path,
    get_blob,
    local_capabilities,
    put_blob,
    schema_to_wire,
)
from shared.kafka.models import KafkaReadSettings

SAMPLE_ROWS_CAP = 10_000
"""The most rows ``sample`` answers; a larger ``n`` is refused, not truncated."""

SAMPLE_ROWS_IN_PARENT = 1_000
"""The most rows of a materialised result a sample reads in this process; more are read in a child."""

OPERATIONS = frozenset(
    {
        "database_read",
        "kafka_read",
        "google_analytics_read",
        "rest_api_read",
        "file_read",
        "fuzzy_join",
        "custom_node",
        "random_split",
        "database_write",
        "cloud_write",
        "file_write",
        "ml_train",
        "ml_apply",
        "delta_write",
        "delta_merge",
        "delta_scd2",
        "viz_session",
        "polars_code",
        "sql_query",
    }
)
"""The operation kinds this worker runs, by ``OperationKind`` value; a kind is named while a route runs it.

Twelve are the ``POST /v1/ops/<kind>`` routes, whose bodies are here: the reads, the fuzzy join, the custom node, the
random split, ML training and apply, Polars code and SQL queries. The other seven are served by the task routes in
``routes.py``: the database, cloud and file writes, the Delta write, merge and SCD2 write, and the visualisation
sessions.
"""


class SubmitMeta(BaseModel):
    """The ``meta`` part of a submit: the hints, the held results the plan reads, and the id to keep its result under.

    An unknown field is refused rather than dropped, so a field core sends that this worker does not
    know fails the submit. ``result_id`` names the result's file, so it holds no path separator. A
    negative ``preview_rows`` is refused, because ``head(-k)`` keeps all but the last k rows.
    """

    model_config = ConfigDict(extra="forbid")

    hints: SubmitHints
    inputs: list[str] = Field(default_factory=list)
    result_id: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    flow_id: int
    node_id: int | str = -1

    @field_validator("hints")
    @classmethod
    def _preview_rows_not_negative(cls, hints: SubmitHints) -> SubmitHints:
        if hints.preview_rows is not None and hints.preview_rows < 0:
            raise ValueError(f"preview_rows must be 0 or more, not {hints.preview_rows}")
        return hints


class OpOutputs(BaseModel):
    """The answer of an operation: the reference of each output, by name, and what it reports besides rows.

    ``metadata`` is empty except for a Kafka read that consumed messages, which reports the ``KafkaReadResult`` its
    child wrote: the offsets core commits once the flow completes, and a training, which reports the model it fitted:
    its ``sha256``, ``size_bytes`` and ``model_type``.
    """

    outputs: dict[str, ResultRef]
    metadata: dict[str, Any] = Field(default_factory=dict)


class RandomSplitInput(models.PolarsOperation):
    """The body of a random split: the rows, already shuffled, and each split as ``(name, percentage)`` in output order.

    An unknown field is refused rather than dropped, because core builds this body by hand.
    """

    model_config = ConfigDict(extra="forbid")

    splits: list[tuple[str, float]]


class MlTrainInput(models.PolarsOperation):
    """The body of a training: the rows to fit on, the model type, the target and feature columns, and the parameters.

    An unknown field is refused rather than dropped, because core builds this body by hand; a path to write the model
    to is one, as the model is held here.
    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model_type: str
    target_column: str
    feature_columns: list[str]
    params: dict[str, Any] = Field(default_factory=dict)


class MlApplyInput(models.PolarsOperation):
    """The body of an apply: the rows to score, the model as the result id of a blob held here, and the column to add.

    An unknown field is refused rather than dropped, because core builds this body by hand; a path to read the model
    from is one, as the model is held here.
    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: str
    output_column: str


class PolarsCodeInput(BaseModel):
    """The body of a Polars Code run: the node's code, its input plans in connection order, and its flow and node.

    An unknown field is refused rather than dropped, because core builds this body by hand.
    """

    model_config = ConfigDict(extra="forbid")

    code: str
    inputs: list[models.Base64Bytes]
    flowfile_flow_id: int
    flowfile_node_id: int | str = -1


class SqlQueryInput(BaseModel):
    """The body of a SQL Query run: the node's statement, its input plans in connection order, and its flow and node.

    An unknown field is refused rather than dropped, because core builds this body by hand.
    """

    model_config = ConfigDict(extra="forbid")

    sql_code: str
    inputs: list[models.Base64Bytes]
    flowfile_flow_id: int
    flowfile_node_id: int | str = -1


class CredentialRefBody(BaseModel):
    """A stored connection as an export or an import names it: its id and the user it runs for, nothing else.

    An unknown field is refused rather than dropped, because core builds this body by hand.
    """

    model_config = ConfigDict(extra="forbid")

    connection_id: int
    user_id: int


class ExportBody(BaseModel):
    """Where ``export`` writes the result, and the stored connection the target needs."""

    model_config = ConfigDict(extra="forbid")

    uri: str
    credential: CredentialRefBody | None = None


class ImportBody(BaseModel):
    """What ``import_uri`` holds: the URI to scan, its connection, and its columns and row count.

    The columns and the row count are core's, never read from the URI. With ``kind="blob"`` the URI's bytes are
    fetched instead, and a blob takes no credential, no columns and no row count.
    """

    model_config = ConfigDict(extra="forbid")

    uri: str
    credential: CredentialRefBody | None = None
    columns: list[WireColumn]
    row_count: int | None = None
    kind: Literal["table", "blob"] = "table"

    @model_validator(mode="after")
    def _a_blob_is_bytes(self) -> "ImportBody":
        if self.kind == "blob" and (self.credential is not None or self.columns or self.row_count is not None):
            raise ValueError("A blob import takes no credential, no columns and no row count")
        return self


def _copy_blob(copy: Callable[[], None]) -> None:
    """Run a blob's copy: a scheme it does not copy is 422, a copy that fails the 500 a failed export answers.

    The messages name no URL query, so a presigned URL's signature is in no answer.
    """
    try:
        copy()
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from None
    except OSError as e:
        raise HTTPException(
            status_code=500, detail={"status": "Error", "error_message": str(e), "error_kind": "task"}
        ) from None


def _materialise(hints: SubmitHints) -> bool:
    """Whether a submit writes its result to a file: when core requires it, or when a wide result is previewed anyway.

    Anything else stays deferred and runs again when rows or a count are asked for. ``fanout`` is not read.
    """
    return hints.must_materialise or (hints.wide and hints.preview_rows is not None)


def _held(result_id: str) -> ResultRecord | None:
    """The record held under *result_id*; ``None`` when there is none or its file is gone."""
    record = results.get(result_id)
    if record is None or (record.state == "materialised" and not os.path.isfile(record.path)):
        return None
    return record


def _not_held(result_id: str) -> HTTPException:
    return HTTPException(status_code=404, detail=f"No result {result_id} is held by this worker")


def _release(result_id: str) -> bool:
    """Forget a held result with its file and its job entry; ``False`` when nothing was held under the id."""
    if results.pop(result_id) is None:
        return False
    with status_dict_lock:
        status_dict.pop(result_id, None)
        PROCESS_MEMORY_USAGE.pop(result_id, None)
    return True


def sweep_expired() -> None:
    """Forget every result and job whose submit ended ``CACHE_EXPIRATION_TIME`` seconds ago or more, failed ones too.

    ``status_dict_lock`` is held across the registry's expiry and the job entries' removal: a resubmit
    under an expired id registers its job only after the old entry is gone, so no running job loses
    its entry. The lock is always taken before the registry's, never after.
    """
    with status_dict_lock:
        for result_id in results.expire(CACHE_EXPIRATION_TIME):
            status_dict.pop(result_id, None)
            PROCESS_MEMORY_USAGE.pop(result_id, None)


def _failure(status: models.Status) -> HTTPException:
    """A 500 saying why a child did not complete, in the fields of the legacy WebSocket error frame."""
    return HTTPException(
        status_code=500,
        detail={"status": status.status, "error_message": status.error_message, "error_kind": status.error_kind},
    )


def _run_job(task_id: str, start: Callable[[], None]) -> models.Status:
    """Register *task_id* as a starting job, call *start*, which returns once its child ended, and return its status."""
    with status_dict_lock:
        status_dict[task_id] = models.Status(
            background_task_id=task_id, status="Starting", file_ref="", result_type="other"
        )
    start()
    with status_dict_lock:
        return status_dict[task_id]


def _run(
    task_id: str,
    plan: bytes,
    operation: str,
    file_ref: str,
    flow_id: int,
    node_id: int | str,
    kwargs: dict | None = None,
) -> models.Status:
    """Run *operation* over *plan* in a child under *task_id*, wait for it, and return its final status."""
    return _run_job(
        task_id,
        lambda: start_process(
            plan,
            task_id=task_id,
            operation=operation,
            file_ref=file_ref,
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
            kwargs=dict(kwargs or {}),
        ),
    )


def _run_child(record: ResultRecord, plan: bytes, operation: str, kwargs: dict | None = None) -> Any:
    """Run *operation* over *plan* as a task of its own under *record*'s flow and node; its result, no entry left."""
    task_id = uuid.uuid4().hex
    try:
        status = _run(task_id, plan, operation, "", record.flow_id, record.node_id, kwargs)
    finally:
        with status_dict_lock:
            status_dict.pop(task_id, None)
    if status.status != "Completed":
        raise _failure(status)
    return status.results


def _op_outputs(job_id: str, count: int) -> list[str]:
    """The ids the *count* outputs of operation *job_id* are held under, ``<job_id>_<i>``, none of them held yet.

    Expired results are swept first. What an earlier run under the job id held is released and the job's end stamp
    dropped, so a rerun replaces its outputs and is not swept while it runs.
    """
    sweep_expired()
    output_ids = [f"{job_id}_{i}" for i in range(count)]
    for result_id in (job_id, *output_ids):
        _release(result_id)
    return output_ids


def _output_file(flow_id: int, result_id: str) -> str:
    """The file an operation writes its output *result_id* to, ``result_path``, with its folder made."""
    target = result_path(flow_id, result_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    return str(target)


def _run_op(job_id: str, start: Callable[[], None], written: list[str]) -> tuple[Any, int | None]:
    """Run an operation's child as job *job_id*; what it reported and the row count it rode along.

    *start* spawns the child and returns once it ended. The job's entry stays, ``Completed`` with its ``results``
    reset once read, and its end is stamped whether or not it completed, so it ages out like a held result. A child
    that did not complete, a cancelled one included, leaves none of *written*, the files it was to write, and the
    caller answers the 500 a failed submit answers.
    """
    try:
        status = _run_job(job_id, start)
    finally:
        results.mark_ended(job_id)
    if status.status != "Completed":
        for path in written:
            Path(path).unlink(missing_ok=True)
        raise _failure(status)
    with status_dict_lock:
        reported, status.results = status.results, None
    return reported, status.number_of_records


def _hold_output(
    result_id: str, path: str, columns: pl.Schema, row_count: int, flow_id: int, node_id: int | str
) -> ResultRef:
    """Hold the file an operation wrote as the materialised result *result_id*, stamped now; its reference."""
    record = ResultRecord(
        result_id=result_id,
        state="materialised",
        columns=schema_to_wire(columns),
        row_count=row_count,
        flow_id=flow_id,
        node_id=node_id,
        path=path,
    )
    results.put(record)
    results.mark_ended(result_id)
    return record.ref()


def _read_into(
    job_id: str,
    result_id: str,
    path: str,
    func: Callable,
    kwargs: dict[str, Any],
    flow_id: int,
    node_id: int | str,
    also_written: tuple[str, ...] = (),
) -> ResultRef:
    """Run the reader *func* in a ``generic_task`` child as job *job_id*, writing *path*, and hold that file.

    The columns are the file's schema, read here as core reads a read's columns on the task routes; the row count is
    the one the child rode along.
    """
    _, row_count = _run_op(
        job_id,
        lambda: start_generic_process(
            func_ref=func,
            task_id=job_id,
            file_ref=path,
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
            kwargs=kwargs,
        ),
        [path, *also_written],
    )
    return _hold_output(result_id, path, pl.scan_ipc(path).collect_schema(), row_count, flow_id, node_id)


def _read(job_id: str, func: Callable, kwargs: dict[str, Any], flow_id: int, node_id: int | str) -> OpOutputs:
    """Run the reader *func* as job *job_id* and answer the file it wrote as the one output, ``result``."""
    (result_id,) = _op_outputs(job_id, 1)
    path = _output_file(flow_id, result_id)
    return OpOutputs(outputs={"result": _read_into(job_id, result_id, path, func, kwargs, flow_id, node_id)})


def worker_capabilities() -> Capabilities:
    """The capabilities of this process with what this worker declares: ``OPERATIONS`` and the schemes it uses.

    It reads and writes object storage and ``https``, a presigned URL a blob is copied to or from, and the local disk
    too when ``FLOWFILE_WORKER_LOCAL_FILES`` is ``1`` at the time of the call: the desktop shell and the single-host
    compose set it for a worker on core's host, which sees core's files. It also reads ``http``: the child that runs a
    node's plan or its ``file_read`` fetches a Read node's ``http`` or ``https`` URL itself, and nothing writes to an
    ``http`` URL.
    """
    schemes = CLOUD_SCHEMES | {"https"}
    if os.environ.get("FLOWFILE_WORKER_LOCAL_FILES") == "1":
        schemes |= {"file"}
    return local_capabilities().model_copy(
        update={"operations": OPERATIONS, "read_schemes": schemes | {"http"}, "write_schemes": schemes}
    )


def run_submit(submit_meta: SubmitMeta, plan: bytes) -> SubmitResult:
    """Run *plan* in a child and hold its result under ``result_id``, written to a file or deferred.

    ``POST /v1/submit``, ``/v1/ws/submit`` and the embedded worker's adapter call this and answer what it returns
    or raises: 404 for an input that is not held, 500 when the child did not complete. Expired results are
    swept first; a result already held under the id is released. The end of the submit is stamped
    whether or not it completed, so its job entry ages out like a held result.

    A deferred submit with no preview runs in the child too, which loads the plan and resolves its
    columns, so its UDF modules and plugin libraries never load here, and collects nothing.
    """
    sweep_expired()
    for input_id in submit_meta.inputs:
        if _held(input_id) is None:
            raise HTTPException(status_code=404, detail=f"Input result {input_id} is not held by this worker")

    result_id = submit_meta.result_id
    _release(result_id)
    materialise = _materialise(submit_meta.hints)
    path = None
    if materialise:
        target = result_path(submit_meta.flow_id, result_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        path = str(target)
    logger.info(f"Submitting result {result_id} ({'materialised' if materialise else 'deferred'})")
    try:
        status = _run(
            result_id,
            plan,
            "submit_result",
            path or "",
            submit_meta.flow_id,
            submit_meta.node_id,
            {"materialise": materialise, "preview_rows": submit_meta.hints.preview_rows},
        )
    finally:
        results.mark_ended(result_id)
    if status.status != "Completed":
        if path:
            Path(path).unlink(missing_ok=True)
        raise _failure(status)

    payload = status.results
    with status_dict_lock:
        status.results = None
    record = ResultRecord(
        result_id=result_id,
        state="materialised" if materialise else "deferred",
        columns=schema_to_wire(payload["columns"]),
        row_count=payload["row_count"],
        flow_id=submit_meta.flow_id,
        node_id=submit_meta.node_id,
        path=path,
        plan=None if materialise else plan,
    )
    results.put(record)
    return SubmitResult(ref=record.ref(), sample=payload["sample"], job_id=result_id)


def sample(result_id: str, n: int) -> bytes:
    """The first *n* rows of a held result as an Arrow IPC stream.

    *n* below 0 or above ``SAMPLE_ROWS_CAP`` is 422 before anything runs; the cap is this worker's rule, checked here
    because no ``Query`` runs on a direct call. Up to ``SAMPLE_ROWS_IN_PARENT`` rows of a materialised result are read
    from its file here; more are collected from ``head(n)`` of a scan of the file in a child, as a deferred result's
    rows are from ``head(n)`` of its plan.
    """
    if not 0 <= n <= SAMPLE_ROWS_CAP:
        raise HTTPException(status_code=422, detail=f"A sample is between 0 and {SAMPLE_ROWS_CAP} rows, not {n}")
    record = _held(result_id)
    if record is None:
        raise _not_held(result_id)
    if record.state == "materialised" and n <= SAMPLE_ROWS_IN_PARENT:
        buffer = io.BytesIO()
        pl.scan_ipc(record.path).head(n).collect().write_ipc_stream(buffer)
        return buffer.getvalue()
    plan = pl.scan_ipc(record.path).serialize() if record.state == "materialised" else record.plan
    return _run_child(record, plan, "submit_result", {"materialise": False, "preview_rows": n})["sample"]


def count(result_id: str) -> int:
    """A held result's row count; a deferred result's plan is counted in a child each time."""
    record = _held(result_id)
    if record is None:
        raise _not_held(result_id)
    if record.row_count is not None:
        return record.row_count
    return int(_run_child(record, record.plan, "calculate_number_of_records"))


def exists(result_id: str) -> bool:
    """Whether the result is held and, when materialised, its file is still there."""
    return _held(result_id) is not None


def release(result_id: str) -> bool:
    """Forget a held result: its file, its record and its job entry; ``False`` when nothing was held under the id."""
    return _release(result_id)


def export(result_id: str, body: ExportBody) -> str:
    """Write a held result to ``body.uri`` as parquet in a child, and answer the URI written.

    The child sinks the result's plan, a scan of its file when materialised and the held plan when deferred, so the
    rows stream to the target without passing through this process. A credential reaches the child as the stored
    connection's id and user, which it resolves against its catalog with its master key. A result that is not held
    is 404; a sink that fails is the 500 a failed submit answers, and the result stays held.

    A blob is not rows: its file is copied here to a ``file://`` URI or ``PUT`` to a presigned ``https://`` URL, with
    no child (``put_blob``). A credential or another scheme is 422, a copy that fails the 500, and the blob stays held.
    """
    record = _held(result_id)
    if record is None:
        raise _not_held(result_id)
    if record.kind == "blob":
        if body.credential is not None:
            raise HTTPException(status_code=422, detail="A blob is copied to a URI that carries its own authorisation")
        _copy_blob(lambda: put_blob(Path(record.path), body.uri))
        return body.uri
    plan = pl.scan_ipc(record.path).serialize() if record.state == "materialised" else record.plan
    credential = body.credential.model_dump() if body.credential is not None else None
    return _run_child(record, plan, "export_result", {"uri": body.uri, "credential": credential})


def import_uri(result_id: str, body: ImportBody) -> ResultRef:
    """Hold a scan of the parquet at ``body.uri`` as the deferred result *result_id*, and answer its reference.

    Nothing of the file is read: the scan is built and serialised here, never run, so the file need not exist yet,
    and the columns and row count are the ones core sent. A credential's connection is read from the catalog here for
    its endpoint and region (``credential_ref_kwargs``); its secrets are resolved by the child that runs the scan. A
    catalog that cannot be read is the 500 an export's child answers for the same error, and nothing is held.
    Expired results are swept first and a result held under the id is released; the record is stamped now, so it ages
    out with the rest. ``count`` answers the given row count without a child, ``sample`` and ``export`` run the scan
    in one, and ``release`` forgets the record and leaves the file, which is not this worker's.

    With ``kind="blob"`` the bytes at ``body.uri`` are copied from a ``file://`` URI or fetched from a presigned
    ``https://`` URL here (``get_blob``) into ``result_path(-1, id)`` with a ``.blob`` suffix, and held as a blob
    record of that file, whose ``release`` removes it. Another scheme is 422 and a fetch that fails the 500; either way
    nothing is held and nothing is left of the file.
    """
    sweep_expired()
    _release(result_id)
    if body.kind == "blob":
        path = result_path(-1, result_id).with_suffix(".blob")
        _copy_blob(lambda: get_blob(body.uri, path))
        record = ResultRecord(
            result_id=result_id,
            state="materialised",
            columns=[],
            row_count=None,
            flow_id=-1,
            node_id=-1,
            path=str(path),
            kind="blob",
        )
        results.put(record)
        results.mark_ended(result_id)
        return record.ref()
    ref = body.credential
    try:
        credential_kwargs = {} if ref is None else credential_ref_kwargs(ref.connection_id, ref.user_id)
    except CredentialResolutionError as e:
        raise HTTPException(
            status_code=500, detail={"status": "Error", "error_message": describe_exception(e), "error_kind": "task"}
        ) from None
    record = ResultRecord(
        result_id=result_id,
        state="deferred",
        columns=body.columns,
        row_count=body.row_count,
        flow_id=-1,
        node_id=-1,
        plan=pl.scan_parquet(file_uri_path(body.uri), **credential_kwargs).serialize(),
    )
    results.put(record)
    results.mark_ended(result_id)
    return record.ref()


def job_status(job_id: str) -> models.Status:
    """The status of a job; a submit's job id is its result id, an operation's the ``job_id`` it was posted with.

    The answer is a copy taken under ``status_dict_lock`` with ``results`` left out: between a child's end and the
    moment its caller reads it, the entry holds what the child reported, a ``pl.Schema`` and sample bytes for a
    submit, which do not serialise.
    """
    with status_dict_lock:
        status = status_dict.get(job_id)
        if status is None:
            raise HTTPException(status_code=404, detail=f"No job {job_id} on this worker")
        return status.model_copy(update={"results": None})


def cancel_job(job_id: str) -> None:
    """Stop a running job's child and mark the job cancelled.

    A job still ``Starting`` has no child yet: its entry is marked ``Cancelled`` under ``status_dict_lock`` and the
    job ends as a cancel when it starts, its child stopped on the monitor's first pass. The check comes first because
    the spawner registers a job's process before its monitor writes ``Processing`` under that lock, so a job seen
    ``Processing`` has one to stop. A job with no child to stop, one that ended or one running in this process, is 404.
    """
    with status_dict_lock:
        status = status_dict.get(job_id)
        if status is not None and status.status == "Starting":
            status.status = "Cancelled"
            return
    if not process_manager.cancel_process(job_id):
        raise HTTPException(status_code=404, detail=f"No job {job_id} is running on this worker")
    with status_dict_lock:
        if job_id in status_dict:
            status_dict[job_id].status = "Cancelled"


def database_read(settings: DatabaseReadSettings, job_id: str) -> OpOutputs:
    """Read a database query in a child as job *job_id* and hold the rows as the one output, ``result``."""
    kwargs = {"database_read_settings": settings}
    return _read(job_id, read_sql_source, kwargs, settings.flowfile_flow_id, settings.flowfile_node_id)


def kafka_read(settings: KafkaReadSettings, job_id: str) -> OpOutputs:
    """Consume a Kafka topic in a child as job *job_id*, without committing, and hold the rows as ``result``.

    The ``metadata`` is the ``KafkaReadResult`` the child wrote beside the rows when it consumed messages, removed
    once read; empty otherwise.
    """
    flow_id, node_id = settings.flowfile_flow_id, settings.flowfile_node_id
    (result_id,) = _op_outputs(job_id, 1)
    path = _output_file(flow_id, result_id)
    sidecar = Path(f"{path}.offsets.json")
    kwargs = {"kafka_read_settings": settings, "sidecar_path": str(sidecar)}
    ref = _read_into(job_id, result_id, path, read_kafka, kwargs, flow_id, node_id, also_written=(str(sidecar),))
    metadata = json.loads(sidecar.read_text()) if sidecar.is_file() else {}
    sidecar.unlink(missing_ok=True)
    return OpOutputs(outputs={"result": ref}, metadata=metadata)


def google_analytics_read(settings: GoogleAnalyticsReadSettings, job_id: str) -> OpOutputs:
    """Fetch a Google Analytics 4 report in a child as job *job_id* and hold its rows as ``result``."""
    kwargs = {"ga_read_settings": settings}
    return _read(job_id, read_google_analytics, kwargs, settings.flowfile_flow_id, settings.flowfile_node_id)


def rest_api_read(settings: RestApiReadSettings, job_id: str) -> OpOutputs:
    """Fetch from a REST API in a child as job *job_id* and hold the rows as ``result``."""
    kwargs = {"settings": settings}
    return _read(job_id, read_rest_api, kwargs, settings.flowfile_flow_id, settings.flowfile_node_id)


def file_read(
    received_table: ReceivedTable, job_id: str, flowfile_flow_id: int = 1, flowfile_node_id: int | str = -1
) -> OpOutputs:
    """Read a file with the reader its ``file_type`` names in a child as job *job_id* and hold the rows as ``result``.

    The job is filed under the flow and node given. A directory scan is 422 before anything runs: no worker reader
    can read a glob.
    """
    if received_table.scan_mode == "directory":
        raise HTTPException(status_code=422, detail="Directory scan mode is not supported by the worker file reader.")
    reader = table_creator_factory_method(received_table.file_type)
    return _read(job_id, reader, {"received_table": received_table}, flowfile_flow_id, flowfile_node_id)


def fuzzy_join(body: models.FuzzyJoinInput, job_id: str) -> OpOutputs:
    """Fuzzy-match two plans in a child as job *job_id* and hold the matched rows as ``result``.

    The child reports no count, so the rows are counted here from the file's metadata.
    """
    flow_id, node_id = body.flowfile_flow_id, body.flowfile_node_id
    (result_id,) = _op_outputs(job_id, 1)
    path = _output_file(flow_id, result_id)
    _run_op(
        job_id,
        lambda: start_fuzzy_process(
            left_serializable_object=body.left_df_operation.polars_serializable_object(),
            right_serializable_object=body.right_df_operation.polars_serializable_object(),
            file_ref=path,
            fuzzy_maps=body.fuzzy_maps,
            task_id=job_id,
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
        ),
        [path],
    )
    rows = pl.scan_ipc(path)
    row_count = rows.select(pl.len()).collect().item()
    return OpOutputs(
        outputs={"result": _hold_output(result_id, path, rows.collect_schema(), row_count, flow_id, node_id)}
    )


def custom_node(body: models.CustomNodeExecuteInput, job_id: str) -> OpOutputs:
    """Run a custom node's ``process`` in a child as job *job_id* and hold each declared output, keyed by its name.

    Output *i* is held as ``<job_id>_<i>`` in the file the child reported for it, so releasing one leaves the others.
    """
    names = body.output_names or ["main"]
    flow_id, node_id = body.flowfile_flow_id, body.flowfile_node_id
    output_ids = _op_outputs(job_id, len(names))
    primary = _output_file(flow_id, output_ids[0])
    written = [_output_path(primary, name, names[0]) for name in names]
    reported, _ = _run_op(
        job_id, lambda: start_custom_node_process(custom_node_input=body, file_ref=primary, task_id=job_id), written
    )
    outputs = json.loads(reported)["outputs"]
    refs = {}
    for result_id, name in zip(output_ids, names, strict=True):
        path = outputs[name]["path"]
        columns = pl.scan_ipc(path).collect_schema()
        refs[name] = _hold_output(result_id, path, columns, outputs[name]["row_count"], flow_id, node_id)
    return OpOutputs(outputs=refs)


def random_split(body: RandomSplitInput, job_id: str) -> OpOutputs:
    """Slice shuffled rows into their splits in a child as job *job_id* and hold each split, keyed by its name.

    Each split is its own file, with the plan's columns and its own row count.
    """
    flow_id, node_id = body.flowfile_flow_id, body.flowfile_node_id
    output_ids = _op_outputs(job_id, len(body.splits))
    paths = [_output_file(flow_id, result_id) for result_id in output_ids]
    kwargs = {"paths": paths, "percentages": [percentage for _, percentage in body.splits]}
    reported, _ = _run_op(
        job_id,
        lambda: start_process(
            body.polars_serializable_object(),
            task_id=job_id,
            operation="split_result",
            file_ref="",
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
            kwargs=kwargs,
        ),
        paths,
    )
    refs = {
        name: _hold_output(result_id, path, reported["columns"], row_count, flow_id, node_id)
        for (name, _), result_id, path, row_count in zip(
            body.splits, output_ids, paths, reported["row_counts"], strict=True
        )
    }
    return OpOutputs(outputs=refs)


def ml_train(body: MlTrainInput, job_id: str) -> OpOutputs:
    """Fit a model to the body's rows in a child as job *job_id* and hold it as the blob ``model``.

    The child writes the model to ``result_path`` of ``<job_id>_0`` with a ``.blob`` suffix, held as a blob record of
    that file; ``metadata`` is what the child reported of it, its ``sha256``, ``size_bytes`` and ``model_type``.
    """
    flow_id, node_id = body.flowfile_flow_id, body.flowfile_node_id
    (model_id,) = _op_outputs(job_id, 1)
    path = str(Path(_output_file(flow_id, model_id)).with_suffix(".blob"))
    reported, _ = _run_op(
        job_id,
        lambda: start_train_model_process(
            polars_serializable_object=body.polars_serializable_object(),
            task_id=job_id,
            file_ref=path,
            model_type=body.model_type,
            target_column=body.target_column,
            feature_columns=body.feature_columns,
            params=body.params,
            staging_path=path,
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
        ),
        [path],
    )
    record = ResultRecord(
        result_id=model_id,
        state="materialised",
        columns=[],
        row_count=None,
        flow_id=flow_id,
        node_id=node_id,
        path=path,
        kind="blob",
    )
    results.put(record)
    results.mark_ended(model_id)
    return OpOutputs(outputs={"model": record.ref()}, metadata=reported)


def ml_apply(body: MlApplyInput, job_id: str) -> OpOutputs:
    """Score the body's rows with a held model in a child as job *job_id* and hold the scored rows as ``result``.

    The model is the blob held under ``body.model``: one that is not held is 404, and a held result that is not a
    blob 422, before anything runs. The child reports no count, so the rows are counted here from the file's metadata.
    """
    flow_id, node_id = body.flowfile_flow_id, body.flowfile_node_id
    (result_id,) = _op_outputs(job_id, 1)
    model = _held(body.model)
    if model is None:
        raise _not_held(body.model)
    if model.kind != "blob":
        raise HTTPException(status_code=422, detail=f"Result {body.model} holds rows, not a model")
    path = _output_file(flow_id, result_id)
    _run_op(
        job_id,
        lambda: start_apply_model_process(
            polars_serializable_object=body.polars_serializable_object(),
            task_id=job_id,
            file_ref=path,
            model_path=model.path,
            output_column=body.output_column,
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
        ),
        [path],
    )
    rows = pl.scan_ipc(path)
    row_count = rows.select(pl.len()).collect().item()
    return OpOutputs(
        outputs={"result": _hold_output(result_id, path, rows.collect_schema(), row_count, flow_id, node_id)}
    )


def polars_code(body: PolarsCodeInput, job_id: str) -> OpOutputs:
    """Run a Polars Code node's code once over its input plans in a child as job *job_id* and hold its result.

    A LazyFrame over sources the code answers is held as the deferred result ``result``, its plan and the columns the
    child resolved for it, nothing collected, so every dtype of the plan reaches its consumers; a DataFrame, or a
    LazyFrame over rows in memory, whose plan would carry them to core, is written to its file by the child and held
    materialised with its height. Either is ``<job_id>_0``.
    """
    flow_id, node_id = body.flowfile_flow_id, body.flowfile_node_id
    (result_id,) = _op_outputs(job_id, 1)
    path = _output_file(flow_id, result_id)
    reported, _ = _run_op(
        job_id,
        lambda: start_process(
            b"",
            task_id=job_id,
            operation="polars_code",
            file_ref=path,
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
            kwargs={"code": body.code, "inputs": list(body.inputs)},
        ),
        [path],
    )
    return OpOutputs(outputs={"result": _hold_user_code_result(result_id, path, reported, flow_id, node_id)})


def sql_query(body: SqlQueryInput, job_id: str) -> OpOutputs:
    """Translate a SQL Query node's statement once over its input plans in a child as job *job_id* and hold its result.

    The child validates the statement itself and registers the inputs as ``input_1``, ``input_2``, …; the result is
    held as a Polars Code result is: deferred as its plan over sources, materialised in its file over rows in memory.
    Either is ``<job_id>_0``.
    """
    flow_id, node_id = body.flowfile_flow_id, body.flowfile_node_id
    (result_id,) = _op_outputs(job_id, 1)
    path = _output_file(flow_id, result_id)
    reported, _ = _run_op(
        job_id,
        lambda: start_process(
            b"",
            task_id=job_id,
            operation="sql_query",
            file_ref=path,
            flowfile_flow_id=flow_id,
            flowfile_node_id=node_id,
            kwargs={"sql_code": body.sql_code, "inputs": list(body.inputs)},
        ),
        [path],
    )
    return OpOutputs(outputs={"result": _hold_user_code_result(result_id, path, reported, flow_id, node_id)})


def _hold_user_code_result(
    result_id: str, path: str, reported: dict[str, Any], flow_id: int, node_id: int | str
) -> ResultRef:
    """Hold what a Polars Code or SQL Query child *reported* as *result_id*: deferred as its plan, else its file."""
    if reported["plan"] is None:
        return _hold_output(result_id, path, reported["columns"], reported["row_count"], flow_id, node_id)
    record = ResultRecord(
        result_id=result_id,
        state="deferred",
        columns=schema_to_wire(reported["columns"]),
        row_count=None,
        flow_id=flow_id,
        node_id=node_id,
        plan=reported["plan"],
    )
    results.put(record)
    results.mark_ended(result_id)
    return record.ref()
