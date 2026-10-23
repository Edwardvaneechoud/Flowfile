"""The worker's versioned ``/v1`` routes, which answer a submitted plan or an operation with result references.

Every route needs the internal token, then an ``X-Protocol-Version`` equal to this worker's
``PROTOCOL_VERSION``: a request without one, or with another, is answered 426. The bodies live in
``service.py``, one function per route over the wire models, which core's embedded worker adapter calls
directly when the worker runs in core's own process; each route here reads its request, calls its function
and answers what it returns, or the ``HTTPException`` it raises. ``run_submit`` is the submit itself, which
``streaming.py``'s ``/v1/ws/submit`` runs too. The names importers take from here (``run_submit``,
``worker_capabilities``, ``sweep_expired``, the sample caps, ``OPERATIONS`` and the request and answer models)
are re-exported from ``service``. The legacy routes in ``routes.py`` stay as they are; the two share only
``status_dict``.
"""

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
def exists(result_id: str) -> Response:
    """200 while the result is held and, when materialised, its file is still there; 404 otherwise."""
    if not service.exists(result_id):
        raise _not_held(result_id)
    return Response(status_code=200)


@router.delete("/results/{result_id}", status_code=204)
def release(result_id: str) -> Response:
    """Forget a held result: its file, its record and its job entry."""
    if not service.release(result_id):
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
def job_status(job_id: str) -> models.Status:
    """The status of a job; a submit's job id is its result id, an operation's the ``job_id`` it was posted with."""
    return service.job_status(job_id)


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> dict[str, str]:
    """Stop a running job's child and mark the job cancelled; a job still ``Starting`` ends as a cancel as it starts."""
    service.cancel_job(job_id)
    return {"message": f"Job {job_id} has been cancelled."}


@router.post("/ops/database_read", response_model=OpOutputs)
def database_read(settings: DatabaseReadSettings, job_id: JobId) -> OpOutputs:
    """Read a database query in a child as job *job_id* and hold the rows as the one output, ``result``."""
    return service.database_read(settings, job_id)


@router.post("/ops/kafka_read", response_model=OpOutputs)
def kafka_read(settings: KafkaReadSettings, job_id: JobId) -> OpOutputs:
    """Consume a Kafka topic in a child as job *job_id*, without committing, and hold the rows as ``result``."""
    return service.kafka_read(settings, job_id)


@router.post("/ops/google_analytics_read", response_model=OpOutputs)
def google_analytics_read(settings: GoogleAnalyticsReadSettings, job_id: JobId) -> OpOutputs:
    """Fetch a Google Analytics 4 report in a child as job *job_id* and hold its rows as ``result``."""
    return service.google_analytics_read(settings, job_id)


@router.post("/ops/rest_api_read", response_model=OpOutputs)
def rest_api_read(settings: RestApiReadSettings, job_id: JobId) -> OpOutputs:
    """Fetch from a REST API in a child as job *job_id* and hold the rows as ``result``."""
    return service.rest_api_read(settings, job_id)


@router.post("/ops/file_read", response_model=OpOutputs)
def file_read(
    received_table: ReceivedTable, job_id: JobId, flowfile_flow_id: int = 1, flowfile_node_id: int | str = -1
) -> OpOutputs:
    """Read a file with the reader its ``file_type`` names in a child as job *job_id* and hold the rows as ``result``.

    The job is filed under the flow and node the query names. A directory scan is 422 before anything runs.
    """
    return service.file_read(received_table, job_id, flowfile_flow_id, flowfile_node_id)


@router.post("/ops/fuzzy_join", response_model=OpOutputs)
def fuzzy_join(body: models.FuzzyJoinInput, job_id: JobId) -> OpOutputs:
    """Fuzzy-match two plans in a child as job *job_id* and hold the matched rows as ``result``."""
    return service.fuzzy_join(body, job_id)


@router.post("/ops/custom_node", response_model=OpOutputs)
def custom_node(body: models.CustomNodeExecuteInput, job_id: JobId) -> OpOutputs:
    """Run a custom node's ``process`` in a child as job *job_id* and hold each declared output, keyed by its name."""
    return service.custom_node(body, job_id)


@router.post("/ops/random_split", response_model=OpOutputs)
def random_split(body: RandomSplitInput, job_id: JobId) -> OpOutputs:
    """Slice shuffled rows into their splits in a child as job *job_id* and hold each split, keyed by its name."""
    return service.random_split(body, job_id)


@router.post("/ops/ml_train", response_model=OpOutputs)
def ml_train(body: MlTrainInput, job_id: JobId) -> OpOutputs:
    """Fit a model to the body's rows in a child as job *job_id* and hold it as the blob ``model``."""
    return service.ml_train(body, job_id)


@router.post("/ops/ml_apply", response_model=OpOutputs)
def ml_apply(body: MlApplyInput, job_id: JobId) -> OpOutputs:
    """Score the body's rows with a held model in a child as job *job_id* and hold the scored rows as ``result``."""
    return service.ml_apply(body, job_id)


@router.post("/ops/file_write", response_model=OpOutputs)
def file_write(body: models.PolarsScriptWrite, job_id: JobId) -> OpOutputs:
    """Write the body's plan to the file it names in a child as job *job_id*; nothing is held, so no outputs."""
    return service.file_write(body, job_id)


@router.post("/ops/database_write", response_model=OpOutputs)
def database_write(body: models.DatabaseScriptWrite, job_id: JobId) -> OpOutputs:
    """Write the body's plan to the database table it names in a child as job *job_id*; nothing is held."""
    return service.database_write(body, job_id)


@router.post("/ops/cloud_write", response_model=OpOutputs)
def cloud_write(body: models.CloudStorageScriptWrite, job_id: JobId) -> OpOutputs:
    """Write the body's plan to the object-storage path it names in a child as job *job_id*; nothing is held."""
    return service.cloud_write(body, job_id)


@router.post("/ops/polars_code", response_model=OpOutputs)
def polars_code(body: PolarsCodeInput, job_id: JobId) -> OpOutputs:
    """Run a Polars Code node's code once over its input plans in a child as job *job_id* and hold its result."""
    return service.polars_code(body, job_id)


@router.post("/ops/sql_query", response_model=OpOutputs)
def sql_query(body: SqlQueryInput, job_id: JobId) -> OpOutputs:
    """Translate a SQL Query node's statement once over its input plans in a child as job *job_id*; hold its result."""
    return service.sql_query(body, job_id)
