"""The worker's versioned ``/v1`` routes, which answer a submitted plan or an operation with result references.

Every route needs the internal token, then an ``X-Protocol-Version`` equal to ``PROTOCOL_VERSION`` (426 otherwise).
Each route calls its body in ``service.py`` and answers what it returns, or the ``HTTPException`` it raises. The
submit, the reads and the operations are sync and wait for their child in the threadpool. The short routes (``HEAD``
and ``DELETE`` of a result, a job's status and its cancel) are ``async``, any blocking part on ``_QUICK``, so they
answer while every pool thread waits on a child. The models and helpers importers take from here are re-exported from
``service``.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Response
from fastapi.requests import HTTPConnection
from pydantic import ValidationError

from flowfile_worker import models, service
from flowfile_worker.auth import verify_internal_token
from flowfile_worker.create.models import ReceivedTable
from flowfile_worker.external_sources.google_analytics_source.models import GoogleAnalyticsReadSettings
from flowfile_worker.external_sources.rest_api_source.models import RestApiReadSettings
from flowfile_worker.external_sources.sql_source.models import DatabaseReadSettings
from flowfile_worker.service import (
    OPERATIONS,
    SAMPLE_ROWS_CAP,
    SAMPLE_ROWS_IN_PARENT,
    CredentialRefBody,
    ExportBody,
    ImportBody,
    MlApplyInput,
    MlTrainInput,
    OpOutputs,
    PolarsCodeInput,
    RandomSplitInput,
    SqlQueryInput,
    SubmitMeta,
    _not_held,
    run_submit,
    sweep_expired,
    worker_capabilities,
)
from shared.compute_protocol import (
    PROTOCOL_VERSION,
    PROTOCOL_VERSION_HEADER,
    Capabilities,
    ResultRef,
    SubmitResult,
)
from shared.kafka.models import KafkaReadSettings

__all__ = [
    "ARROW_STREAM",
    "OPERATIONS",
    "SAMPLE_ROWS_CAP",
    "SAMPLE_ROWS_IN_PARENT",
    "CredentialRefBody",
    "ExportBody",
    "ImportBody",
    "JobId",
    "MlApplyInput",
    "MlTrainInput",
    "OpOutputs",
    "PolarsCodeInput",
    "RandomSplitInput",
    "SqlQueryInput",
    "SubmitMeta",
    "parse_submit_meta",
    "require_protocol_version",
    "router",
    "run_submit",
    "sweep_expired",
    "worker_capabilities",
]

ARROW_STREAM = "application/vnd.apache.arrow.stream"

# An operation's job id names its output files, so it holds no path separator.
JobId = Annotated[str, Query(pattern=r"^[A-Za-z0-9_-]+$")]

_QUICK = ThreadPoolExecutor(max_workers=4, thread_name_prefix="v1-quick")
"""The threads the short routes block in: stopping a job's child and removing a file, never waiting on one."""


async def _quick(func, *args):
    """Call *func* with *args* on ``_QUICK`` and answer what it returns."""
    return await asyncio.get_running_loop().run_in_executor(_QUICK, func, *args)


async def require_protocol_version(connection: HTTPConnection) -> None:
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


@router.get("/capabilities", response_model=Capabilities)
async def capabilities() -> Capabilities:
    """What this worker can run, as the legacy ``GET /capabilities`` answers it."""
    return worker_capabilities()


def parse_submit_meta(meta: str) -> SubmitMeta:
    """The ``meta`` JSON of a submit as a ``SubmitMeta``; 422 with the validation errors when it is not one."""
    try:
        return SubmitMeta.model_validate_json(meta)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors(include_url=False, include_context=False)) from e


@router.post("/submit", response_model=SubmitResult)
def submit(plan: bytes = File(), meta: str = Form()) -> SubmitResult:
    """Run *plan* and hold its result under the meta's ``result_id``, written to a file or deferred.

    ``plan`` is a file part, so a plan of any size is spooled rather than refused; ``meta`` is the
    JSON of ``SubmitMeta``. A result already held under the id is released first. The answer
    carries the inline sample when ``preview_rows`` was set.
    """
    return run_submit(parse_submit_meta(meta), plan)


@router.get("/results/{result_id}/sample")
def sample(result_id: str, n: int = Query(ge=0, le=SAMPLE_ROWS_CAP)) -> Response:
    """The first *n* rows of a held result as an Arrow IPC stream; an *n* above ``SAMPLE_ROWS_CAP`` is 422."""
    return Response(content=service.sample(result_id, n), media_type=ARROW_STREAM)


@router.get("/results/{result_id}/count")
def count(result_id: str) -> dict[str, int]:
    """A held result's row count; a deferred result's plan is counted in a child each time."""
    return {"row_count": service.count(result_id)}


@router.head("/results/{result_id}")
async def exists(result_id: str) -> Response:
    """200 while the result is held and, when materialised, its file is still there; 404 otherwise."""
    if not service.exists(result_id):
        raise _not_held(result_id)
    return Response(status_code=200)


@router.delete("/results/{result_id}", status_code=204)
async def release(result_id: str) -> Response:
    """Forget a held result: its file, its record and its job entry; a job still running under the id is stopped."""
    if not await _quick(service.release, result_id):
        raise _not_held(result_id)
    return Response(status_code=204)


@router.post("/results/{result_id}/export")
def export(result_id: str, body: ExportBody) -> dict[str, str]:
    """Write a held result to ``body.uri`` as parquet in a child, or copy a blob there, and answer the URI written."""
    return {"uri": service.export(result_id, body)}


@router.post("/results/{result_id}/import", response_model=ResultRef)
def import_uri(result_id: str, body: ImportBody) -> ResultRef:
    """Hold a scan of the parquet at ``body.uri`` as the deferred result *result_id*, or copy a blob into one."""
    return service.import_uri(result_id, body)


@router.get("/jobs/{job_id}", response_model=models.Status)
async def job_status(job_id: str) -> models.Status:
    """The status of a job; a submit's job id is its result id, an operation's the ``job_id`` it was posted with."""
    return service.job_status(job_id)


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: str) -> dict[str, str]:
    """Stop a running job's child and mark the job cancelled; a job still ``Starting`` ends as a cancel as it starts."""
    await _quick(service.cancel_job, job_id)
    return {"message": f"Job {job_id} has been cancelled."}


@router.post("/ops/file_read", response_model=OpOutputs)
def file_read(
    received_table: ReceivedTable, job_id: JobId, flowfile_flow_id: int = 1, flowfile_node_id: int | str = -1
) -> OpOutputs:
    """Read a file with the reader its ``file_type`` names in a child as job *job_id* and hold the rows as ``result``.

    The job is filed under the flow and node the query names. A directory scan is 422 before anything runs.
    """
    return service.file_read(received_table, job_id, flowfile_flow_id, flowfile_node_id)


_OPS: dict[str, type] = {
    "database_read": DatabaseReadSettings,
    "kafka_read": KafkaReadSettings,
    "google_analytics_read": GoogleAnalyticsReadSettings,
    "rest_api_read": RestApiReadSettings,
    "fuzzy_join": models.FuzzyJoinInput,
    "custom_node": models.CustomNodeExecuteInput,
    "random_split": RandomSplitInput,
    "ml_train": MlTrainInput,
    "ml_apply": MlApplyInput,
    "file_write": models.PolarsScriptWrite,
    "database_write": models.DatabaseScriptWrite,
    "cloud_write": models.CloudStorageScriptWrite,
    "polars_code": PolarsCodeInput,
    "sql_query": SqlQueryInput,
}
"""The ``POST /v1/ops/<kind>`` routes that take one body and the job id, by kind, with the model of their body."""


def _add_op_route(kind: str, body_model: type) -> None:
    """Route ``POST /v1/ops/<kind>`` to ``service.<kind>``, its body *body_model*, named and documented as it is."""
    run = getattr(service, kind)

    def endpoint(body: body_model, job_id: JobId) -> OpOutputs:
        return run(body, job_id)

    endpoint.__doc__ = run.__doc__
    router.add_api_route(f"/ops/{kind}", endpoint, methods=["POST"], response_model=OpOutputs, name=kind)


for _kind, _body_model in _OPS.items():
    _add_op_route(_kind, _body_model)
