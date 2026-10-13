"""The worker's versioned ``/v1`` routes, which answer a submitted plan or an operation with result references.

Every route needs the internal token, then an ``X-Protocol-Version`` equal to this worker's
``PROTOCOL_VERSION``: a request without one, or with another, is answered 426. ``POST /v1/submit``
runs the plan in a spawned ``funcs.submit_result`` child and keeps the result in ``results.results``,
written to a file or deferred as ``_materialise`` decides; the other routes read, count, probe and
release what it holds. A submit's job id is its result id, and every submit first drops what has
been held for ``CACHE_EXPIRATION_TIME``. ``run_submit`` is the submit itself, which ``streaming.py``'s
``/v1/ws/submit`` runs too. ``POST /v1/ops/<kind>`` runs a read, a fuzzy join, a random split or a
custom node in the child its task route spawns, as the job the request's ``job_id`` names, and holds
each output that child wrote as the materialised result ``<job_id>_<i>``, which the same routes read,
count, probe and release. The legacy routes in ``routes.py`` stay as they are; the two share only
``status_dict``.
"""

import io
import json
import os
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any

import polars as pl
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Response
from fastapi.requests import HTTPConnection
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from flowfile_worker import CACHE_EXPIRATION_TIME, PROCESS_MEMORY_USAGE, models, status_dict, status_dict_lock
from flowfile_worker.auth import verify_internal_token
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
    start_custom_node_process,
    start_fuzzy_process,
    start_generic_process,
    start_process,
)
from shared.compute_protocol import (
    PROTOCOL_VERSION,
    PROTOCOL_VERSION_HEADER,
    Capabilities,
    ResultRef,
    SubmitHints,
    SubmitResult,
    local_capabilities,
    schema_to_wire,
)
from shared.kafka.models import KafkaReadSettings

ARROW_STREAM = "application/vnd.apache.arrow.stream"

# An operation's job id names its output files, so it holds no path separator.
JobId = Annotated[str, Query(pattern=r"^[A-Za-z0-9_-]+$")]


def require_protocol_version(connection: HTTPConnection) -> None:
    """Answer 426 when the request's ``X-Protocol-Version`` is missing or is not this worker's.

    It takes an ``HTTPConnection`` so the WebSocket submit can call it before accepting.
    """
    sent = connection.headers.get(PROTOCOL_VERSION_HEADER)
    if sent != str(PROTOCOL_VERSION):
        raise HTTPException(
            status_code=426,
            detail=f"This worker speaks protocol version {PROTOCOL_VERSION}; "
            f"the request carries {sent if sent is not None else 'none'}",
        )


router = APIRouter(prefix="/v1", dependencies=[Depends(verify_internal_token), Depends(require_protocol_version)])


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
    child wrote: the offsets core commits once the flow completes.
    """

    outputs: dict[str, ResultRef]
    metadata: dict[str, Any] = Field(default_factory=dict)


class RandomSplitInput(models.PolarsOperation):
    """The body of a random split: the rows, already shuffled, and each split as ``(name, percentage)`` in output order.

    An unknown field is refused rather than dropped, because core builds this body by hand.
    """

    model_config = ConfigDict(extra="forbid")

    splits: list[tuple[str, float]]


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


def _run_deferred(record: ResultRecord, operation: str, kwargs: dict | None = None) -> Any:
    """Run *operation* over a deferred result's plan as a task of its own and return its result; no entry is left."""
    task_id = uuid.uuid4().hex
    try:
        status = _run(task_id, record.plan, operation, "", record.flow_id, record.node_id, kwargs)
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
    route answers the 500 a failed submit answers.
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


@router.get("/capabilities", response_model=Capabilities)
async def capabilities() -> Capabilities:
    """What this worker can run, as the legacy ``GET /capabilities`` answers it."""
    return local_capabilities()


def parse_submit_meta(meta: str) -> SubmitMeta:
    """The ``meta`` JSON of a submit as a ``SubmitMeta``; 422 with the validation errors when it is not one."""
    try:
        return SubmitMeta.model_validate_json(meta)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors(include_url=False, include_context=False)) from e


def run_submit(submit_meta: SubmitMeta, plan: bytes) -> SubmitResult:
    """Run *plan* in a child and hold its result under ``result_id``, written to a file or deferred.

    Both ``POST /v1/submit`` and ``/v1/ws/submit`` call this and answer what it returns or raises:
    404 for an input that is not held, 500 when the child did not complete. Expired results are
    swept first; a result already held under the id is released. The end of the submit is stamped
    whether or not it completed, so its job entry ages out like a held result.
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


@router.post("/submit", response_model=SubmitResult)
def submit(plan: bytes = File(), meta: str = Form()) -> SubmitResult:
    """Run *plan* and hold its result under the meta's ``result_id``, written to a file or deferred.

    ``plan`` is a file part, so a plan of any size is spooled rather than refused; ``meta`` is the
    JSON of ``SubmitMeta``. A result already held under the id is released first. The answer
    carries the inline sample when ``preview_rows`` was set.
    """
    return run_submit(parse_submit_meta(meta), plan)


@router.get("/results/{result_id}/sample")
def sample(result_id: str, n: int = Query(ge=0)) -> Response:
    """The first *n* rows of a held result as an Arrow IPC stream.

    A materialised result's rows are read from its file here, bounded by *n*; a deferred one's are
    collected from ``head(n)`` of its plan in a child.
    """
    record = _held(result_id)
    if record is None:
        raise _not_held(result_id)
    if record.state == "materialised":
        buffer = io.BytesIO()
        pl.scan_ipc(record.path).head(n).collect().write_ipc_stream(buffer)
        data = buffer.getvalue()
    else:
        data = _run_deferred(record, "submit_result", {"materialise": False, "preview_rows": n})["sample"]
    return Response(content=data, media_type=ARROW_STREAM)


@router.get("/results/{result_id}/count")
def count(result_id: str) -> dict[str, int]:
    """A held result's row count; a deferred result's plan is counted in a child each time."""
    record = _held(result_id)
    if record is None:
        raise _not_held(result_id)
    if record.row_count is not None:
        return {"row_count": record.row_count}
    return {"row_count": int(_run_deferred(record, "calculate_number_of_records"))}


@router.head("/results/{result_id}")
def exists(result_id: str) -> Response:
    """200 while the result is held and, when materialised, its file is still there; 404 otherwise."""
    if _held(result_id) is None:
        raise _not_held(result_id)
    return Response(status_code=200)


@router.delete("/results/{result_id}", status_code=204)
def release(result_id: str) -> Response:
    """Forget a held result: its file, its record and its job entry."""
    if not _release(result_id):
        raise _not_held(result_id)
    return Response(status_code=204)


@router.get("/jobs/{job_id}", response_model=models.Status)
def job_status(job_id: str) -> models.Status:
    """The status of a job; a submit's job id is its result id, an operation's the ``job_id`` it was posted with."""
    status = status_dict.get(job_id)
    if status is None:
        raise HTTPException(status_code=404, detail=f"No job {job_id} on this worker")
    return status


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> dict[str, str]:
    """Stop a running job's child and mark the job cancelled."""
    if not process_manager.cancel_process(job_id):
        raise HTTPException(status_code=404, detail=f"No job {job_id} is running on this worker")
    with status_dict_lock:
        if job_id in status_dict:
            status_dict[job_id].status = "Cancelled"
    return {"message": f"Job {job_id} has been cancelled."}


@router.post("/ops/database_read", response_model=OpOutputs)
def database_read(settings: DatabaseReadSettings, job_id: JobId) -> OpOutputs:
    """Read a database query in a child as job *job_id* and hold the rows as the one output, ``result``."""
    kwargs = {"database_read_settings": settings}
    return _read(job_id, read_sql_source, kwargs, settings.flowfile_flow_id, settings.flowfile_node_id)


@router.post("/ops/kafka_read", response_model=OpOutputs)
def kafka_read(settings: KafkaReadSettings, job_id: JobId) -> OpOutputs:
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


@router.post("/ops/google_analytics_read", response_model=OpOutputs)
def google_analytics_read(settings: GoogleAnalyticsReadSettings, job_id: JobId) -> OpOutputs:
    """Fetch a Google Analytics 4 report in a child as job *job_id* and hold its rows as ``result``."""
    kwargs = {"ga_read_settings": settings}
    return _read(job_id, read_google_analytics, kwargs, settings.flowfile_flow_id, settings.flowfile_node_id)


@router.post("/ops/rest_api_read", response_model=OpOutputs)
def rest_api_read(settings: RestApiReadSettings, job_id: JobId) -> OpOutputs:
    """Fetch from a REST API in a child as job *job_id* and hold the rows as ``result``."""
    kwargs = {"settings": settings}
    return _read(job_id, read_rest_api, kwargs, settings.flowfile_flow_id, settings.flowfile_node_id)


@router.post("/ops/file_read", response_model=OpOutputs)
def file_read(
    received_table: ReceivedTable, job_id: JobId, flowfile_flow_id: int = 1, flowfile_node_id: int | str = -1
) -> OpOutputs:
    """Read a file with the reader its ``file_type`` names in a child as job *job_id* and hold the rows as ``result``.

    The job is filed under the flow and node the query names. A directory scan is 422 before anything runs: no worker
    reader can read a glob.
    """
    if received_table.scan_mode == "directory":
        raise HTTPException(status_code=422, detail="Directory scan mode is not supported by the worker file reader.")
    reader = table_creator_factory_method(received_table.file_type)
    return _read(job_id, reader, {"received_table": received_table}, flowfile_flow_id, flowfile_node_id)


@router.post("/ops/fuzzy_join", response_model=OpOutputs)
def fuzzy_join(body: models.FuzzyJoinInput, job_id: JobId) -> OpOutputs:
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


@router.post("/ops/custom_node", response_model=OpOutputs)
def custom_node(body: models.CustomNodeExecuteInput, job_id: JobId) -> OpOutputs:
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


@router.post("/ops/random_split", response_model=OpOutputs)
def random_split(body: RandomSplitInput, job_id: JobId) -> OpOutputs:
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
