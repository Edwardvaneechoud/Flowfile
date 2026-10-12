"""The worker's versioned ``/v1`` routes, which answer a submitted plan with a result reference.

Every route needs the internal token, then an ``X-Protocol-Version`` equal to this worker's
``PROTOCOL_VERSION``: a request without one, or with another, is answered 426. ``POST /v1/submit``
runs the plan in a spawned ``funcs.submit_result`` child and keeps the result in ``results.results``,
written to a file or deferred as ``_materialise`` decides; the other routes read, count, probe and
release what it holds. A submit's job id is its result id. The legacy routes in ``routes.py`` stay
as they are; the two share only ``status_dict``.
"""

import io
import os
import uuid
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from flowfile_worker import PROCESS_MEMORY_USAGE, models, status_dict, status_dict_lock
from flowfile_worker.auth import verify_internal_token
from flowfile_worker.configs import logger
from flowfile_worker.results import ResultRecord, result_path, results
from flowfile_worker.spawner import process_manager, start_process
from shared.compute_protocol import (
    PROTOCOL_VERSION,
    PROTOCOL_VERSION_HEADER,
    Capabilities,
    SubmitHints,
    SubmitResult,
    local_capabilities,
    schema_to_wire,
)

ARROW_STREAM = "application/vnd.apache.arrow.stream"


def require_protocol_version(request: Request) -> None:
    """Answer 426 when the request's ``X-Protocol-Version`` is missing or is not this worker's."""
    sent = request.headers.get(PROTOCOL_VERSION_HEADER)
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
    know fails the submit. ``result_id`` names the result's file, so it holds no path separator.
    """

    model_config = ConfigDict(extra="forbid")

    hints: SubmitHints
    inputs: list[str] = Field(default_factory=list)
    result_id: str = Field(pattern=r"^[A-Za-z0-9_-]+$")
    flow_id: int
    node_id: int | str = -1


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


def _failure(status: models.Status) -> HTTPException:
    """A 500 saying why a child did not complete, in the fields of the legacy WebSocket error frame."""
    return HTTPException(
        status_code=500,
        detail={"status": status.status, "error_message": status.error_message, "error_kind": status.error_kind},
    )


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
    with status_dict_lock:
        status_dict[task_id] = models.Status(
            background_task_id=task_id, status="Starting", file_ref="", result_type="other"
        )
    start_process(
        plan,
        task_id=task_id,
        operation=operation,
        file_ref=file_ref,
        flowfile_flow_id=flow_id,
        flowfile_node_id=node_id,
        kwargs=dict(kwargs or {}),
    )
    with status_dict_lock:
        return status_dict[task_id]


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


@router.get("/capabilities", response_model=Capabilities)
async def capabilities() -> Capabilities:
    """What this worker can run, as the legacy ``GET /capabilities`` answers it."""
    return local_capabilities()


@router.post("/submit", response_model=SubmitResult)
def submit(plan: bytes = File(), meta: str = Form()) -> SubmitResult:
    """Run *plan* and hold its result under the meta's ``result_id``, written to a file or deferred.

    ``plan`` is a file part, so a plan of any size is spooled rather than refused; ``meta`` is the
    JSON of ``SubmitMeta``. A result already held under the id is released first. The answer
    carries the inline sample when ``preview_rows`` was set.
    """
    try:
        submit_meta = SubmitMeta.model_validate_json(meta)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors(include_url=False, include_context=False)) from e
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
    status = _run(
        result_id,
        plan,
        "submit_result",
        path or "",
        submit_meta.flow_id,
        submit_meta.node_id,
        {"materialise": materialise, "preview_rows": submit_meta.hints.preview_rows},
    )
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
    """The status of a job; a submit's job id is its result id."""
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
