"""The adapter to the worker embedded in this process: the worker's ``/v1`` bodies called directly, no wire.

``EmbeddedWorkerBackend`` is ``WorkerBackend``'s ``/v1`` half with the wire replaced by a call. Each method builds the
request model its route takes, as the worker's own class, calls the function in ``flowfile_worker.service`` that the
route calls, and reads the answer as ``WorkerBackend`` reads the route's: the reference shape, the job handle, the
keying of an operation's outputs and the decoding of a result's location are the protocol's, unchanged, so a
reference this backend mints is a worker's reference under this process's id. Nothing is serialised but the plan,
which crosses as ``serialize()`` bytes so the one ``funcs._load_plan`` serves every placement, and
``funcs.submit_result`` is the one submit, which makes the worker's materialisation policy this backend's. No token,
no version header, no socket, no port, no handshake gate, no flag read: the handshake runs through ``capabilities()``
and the probe jobs like any backend's and finds this process's own versions.

Where a job runs is the host's choice, not this backend's: with ``flowfile_worker.spawner.in_process`` set, every
job runs in the calling process, as a library host sets it; left unset, the worker's pool or a child runs it, as a
serving host leaves it. Nothing of the worker is imported until the first call (``_service``). Every ``HTTPException``
a body raises is mapped to the typed errors (``_typed``): 404 is ``ResultMissing``, 422 ``PlanRejected``, a 500 that
carries a child's ``status`` goes through ``errors.worker_failure``, and any other code is a ``TaskFailed`` naming it.
The writes, the Delta operations and the visualisation session still run through the worker's task routes and are
neither declared nor run here.
"""

from __future__ import annotations

import io
import uuid
from collections.abc import Callable
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Literal

import polars as pl
from fastapi import HTTPException

from flowfile_core.flowfile.execution.backends.worker import (
    _JOB_STATES,
    _V1_OPERATIONS,
    _custom_node_request,
    _database_read_settings,
    _declared_outputs,
    _file_to_read,
    _flowfile_schema,
    _fuzzy_sides,
    _google_analytics_read_settings,
    _kafka_offsets,
    _kafka_read_settings,
    _rest_api_read_settings,
    _shuffled_rows,
    _wire_columns,
    _wire_credential,
)
from flowfile_core.flowfile.execution.errors import (
    ComputeError,
    ForeignRef,
    OperationUnsupported,
    PlanRejected,
    ResultMissing,
    TaskFailed,
    worker_failure,
)
from flowfile_core.flowfile.execution.payloads import (
    CustomNodePayload,
    DatabaseReadPayload,
    FileReadPayload,
    FuzzyJoinPayload,
    GoogleAnalyticsReadPayload,
    KafkaReadPayload,
    MlApplyPayload,
    MlTrainPayload,
    PolarsCodePayload,
    RandomSplitPayload,
    RestApiReadPayload,
    SqlQueryPayload,
    unpack,
)
from flowfile_core.flowfile.execution.protocol import (
    CredentialRef,
    JobHandle,
    JobStatus,
    OperationKind,
    OpPayload,
    OpResult,
    ResultRef,
    Schema,
    SubmitHints,
    SubmitResult,
)
from flowfile_core.flowfile.execution.registry import LOCAL_BACKEND_ID
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import safe_eval_pl_type
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import FuzzyJoinInput, PolarsOperation
from shared import compute_protocol as wire
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities

if TYPE_CHECKING:
    from flowfile_worker import service as worker_service


def _service() -> worker_service:
    """The worker's service module, imported on the first call, so nothing of the worker loads before the first job."""
    from flowfile_worker import service

    return service


class EmbeddedWorkerBackend:
    """``ComputeBackend`` for the worker embedded in this process, reached by a direct call to its ``/v1`` bodies.

    Its id is ``local``, the id of the compute a process without a worker beside it runs on; a reference or a job
    handle of any other id is refused with ``ForeignRef``. Its jobs run where the host set
    ``flowfile_worker.spawner.in_process``: in this process for a library host, in the worker's pool or a child for a
    serving one. ``capabilities()`` is the embedded worker's own answer with ``operations`` cut to the twelve kinds
    ``run_op`` runs; the writes, the Delta operations and the visualisation session are refused with
    ``OperationUnsupported``, as they still run through the worker's task routes.

    Materialisation policy: the worker's, applied by ``funcs.submit_result``: a plan is written to a file when
    ``hints.must_materialise`` is set, or when it is ``wide`` and a preview is asked for (``preview_rows`` is not
    ``None``); every other plan is held deferred, as the bytes it was sent as. ``fanout`` is not read. With
    ``preview_rows`` set the sample comes inline from the same job. A reference carries the worker's columns, read with
    ``safe_eval_pl_type``, and a ``WorkerLocation`` JSON as ``location``: ``plan_for`` decodes it without a call, a
    ``pl.scan_ipc`` of the file the worker wrote or the deferred plan deserialised, and ``sample``, ``count``,
    ``exists``, ``release`` and ``export`` call the bodies that serve ``/v1/results/{id}``. ``import_uri`` holds a scan
    of the URI as a deferred result under a fresh id, or with ``kind="blob"`` a copy of the bytes at a ``file://``
    URI or a presigned ``https://`` URL. A blob is refused by ``plan_for``, ``sample`` and ``count`` with
    ``OperationUnsupported``, as ``submit`` refuses one among its inputs.

    ``run_op`` runs the five reads, ``fuzzy_join``, ``random_split``, ``custom_node``, ``ml_train``, ``ml_apply``,
    ``polars_code`` and ``sql_query`` through the body of ``POST /v1/ops/<kind>``, the payload's ``job_id`` the job's,
    so ``status`` and ``cancel`` reach it; the refs are keyed as ``WorkerBackend`` keys them, each output the stored
    result ``<job_id>_<i>``, a training's model the blob ``<job_id>_0``, a Kafka read's offsets and a training's
    digest, size and type as ``metadata``. ``cancel`` of a job that already ended, or of one running in this process,
    which nothing stops, is left as it is.
    """

    id = LOCAL_BACKEND_ID
    protocol_version = PROTOCOL_VERSION

    def capabilities(self) -> Capabilities:
        """The embedded worker's capabilities, its ``operations`` cut to the twelve kinds ``run_op`` runs."""
        declared = _service().worker_capabilities()
        return declared.model_copy(update={"operations": frozenset(kind.value for kind in _V1_OPERATIONS)})

    def submit(
        self,
        plan: pl.LazyFrame,
        inputs: list[ResultRef],
        hints: SubmitHints,
        result_id: str,
        *,
        flow_id: int,
        node_id: int | str,
    ) -> SubmitResult:
        """Run ``plan`` through the worker's submit and hold its result under ``result_id``, by the worker's policy.

        The plan crosses as its ``serialize()`` bytes; ``inputs`` must be this backend's results, which the worker
        checks it still holds.

        Raises:
            ForeignRef: An input another backend minted.
            OperationUnsupported: An input is a blob, which a plan cannot read; nothing was run.
            PlanRejected: The worker could not load the plan.
            ResultMissing: The worker no longer holds an input.
            TaskFailed: The plan failed.
        """
        for ref in inputs:
            self._table(ref)
        service = _service()
        meta = service.SubmitMeta(
            hints=wire.SubmitHints.model_validate(asdict(hints)),
            inputs=[ref.result_id for ref in inputs],
            result_id=result_id,
            flow_id=flow_id,
            node_id=node_id,
        )
        return self._from_wire(_call(service.run_submit, meta, plan.serialize()))

    def run_op(self, op: OperationKind, payload: OpPayload) -> OpResult:
        """Run ``op`` through the body of ``POST /v1/ops/{kind}`` and key its refs as ``WorkerBackend`` does.

        The payload is unpacked before anything runs, so an empty one is a ``ValueError`` here too; the body is built
        as the worker's own request model. A training's ``metadata`` is the model's digest, size and type, a Kafka
        read's the offsets to commit.

        Raises:
            ForeignRef: An ``ml_apply`` model another backend minted.
            OperationUnsupported: A kind the embedded worker does not run, or a custom node without a node file.
            PlanRejected: The worker could not load the operation's plan, or the body refused it before running.
            ResultMissing: An ``ml_apply`` model the worker no longer holds.
            TaskFailed: The operation failed.
            ValueError: The worker did not return one of a custom node's declared outputs.
        """
        if op not in _V1_OPERATIONS:
            raise OperationUnsupported(
                f"The embedded worker does not run the {op.value} operation; it stays on the worker's task routes"
            )
        service = _service()
        args: tuple[Any, ...] = ()
        match op:
            case OperationKind.database_read:
                job = unpack(payload, DatabaseReadPayload)
                body = service.DatabaseReadSettings.model_validate(_database_read_settings(job).model_dump(mode="json"))
                run = service.database_read
            case OperationKind.kafka_read:
                job = unpack(payload, KafkaReadPayload)
                body = service.KafkaReadSettings.model_validate(_kafka_read_settings(job).model_dump(mode="json"))
                run = service.kafka_read
            case OperationKind.google_analytics_read:
                job = unpack(payload, GoogleAnalyticsReadPayload)
                body = service.GoogleAnalyticsReadSettings.model_validate(
                    _google_analytics_read_settings(job).model_dump(mode="json")
                )
                run = service.google_analytics_read
            case OperationKind.rest_api_read:
                job = unpack(payload, RestApiReadPayload)
                body = service.RestApiReadSettings.model_validate(_rest_api_read_settings(job).model_dump(mode="json"))
                run = service.rest_api_read
            case OperationKind.file_read:
                job = unpack(payload, FileReadPayload)
                body = service.ReceivedTable.model_validate(_file_to_read(job).model_dump(mode="json"))
                run = service.file_read
                args = (job.flow_id, job.node_id)
            case OperationKind.fuzzy_join:
                job = unpack(payload, FuzzyJoinPayload)
                left, right, fuzzy_maps = _fuzzy_sides(job)
                sides = FuzzyJoinInput(
                    left_df_operation=PolarsOperation(operation=left.serialize()),
                    right_df_operation=PolarsOperation(operation=right.serialize()),
                    fuzzy_maps=fuzzy_maps,
                    task_id=job.job_id,
                    flowfile_flow_id=job.flow_id,
                    flowfile_node_id=job.node_id,
                )
                body = service.models.FuzzyJoinInput.model_validate(sides.model_dump(mode="json"))
                run = service.fuzzy_join
            case OperationKind.custom_node:
                job = unpack(payload, CustomNodePayload)
                body = service.models.CustomNodeExecuteInput.model_validate(
                    _custom_node_request(job).model_dump(mode="json")
                )
                run = service.custom_node
            case OperationKind.ml_train:
                job = unpack(payload, MlTrainPayload)
                body = service.MlTrainInput(
                    operation=job.plan.serialize(),
                    flowfile_flow_id=job.flow_id,
                    flowfile_node_id=job.node_id,
                    model_type=job.model_type,
                    target_column=job.target_column,
                    feature_columns=job.feature_columns,
                    params=job.params,
                )
                run = service.ml_train
            case OperationKind.ml_apply:
                job = unpack(payload, MlApplyPayload)
                self._check(job.model)
                body = service.MlApplyInput(
                    operation=job.plan.serialize(),
                    flowfile_flow_id=job.flow_id,
                    flowfile_node_id=job.node_id,
                    model=job.model.result_id,
                    output_column=job.output_column,
                )
                run = service.ml_apply
            case OperationKind.polars_code:
                job = unpack(payload, PolarsCodePayload)
                body = service.PolarsCodeInput(
                    code=job.code,
                    inputs=[plan.serialize() for plan in job.inputs],
                    flowfile_flow_id=job.flow_id,
                    flowfile_node_id=job.node_id,
                )
                run = service.polars_code
            case OperationKind.sql_query:
                job = unpack(payload, SqlQueryPayload)
                body = service.SqlQueryInput(
                    sql_code=job.sql_code,
                    inputs=[plan.serialize() for plan in job.inputs],
                    flowfile_flow_id=job.flow_id,
                    flowfile_node_id=job.node_id,
                )
                run = service.sql_query
            case _:
                job = unpack(payload, RandomSplitPayload)
                body = service.RandomSplitInput(
                    operation=_shuffled_rows(job).serialize(),
                    flowfile_flow_id=job.flow_id,
                    flowfile_node_id=job.node_id,
                    splits=[(group.name, group.percentage) for group in job.settings.splits],
                )
                run = service.random_split
        answer = _call(run, body, job.job_id, *args)
        refs = {name: self._ref_from_wire(ref) for name, ref in answer.outputs.items()}
        if op is OperationKind.custom_node:
            refs = _declared_outputs(refs, job.output_names)
        elif op is OperationKind.random_split:
            refs = {group.name: refs[group.name] for group in job.settings.splits}
        if op is OperationKind.kafka_read:
            metadata = _kafka_offsets(answer.metadata)
        else:
            metadata = answer.metadata if op is OperationKind.ml_train else {}
        return OpResult(refs=refs, metadata=metadata)

    def sample(self, ref: ResultRef, n: int) -> bytes:
        """The first ``n`` rows of ``ref`` as an Arrow IPC stream, as ``GET /v1/results/{id}/sample`` answers them.

        Raises:
            ForeignRef: Another backend minted ``ref``.
            OperationUnsupported: ``ref`` is a blob.
            PlanRejected: ``n`` is above the worker's sample cap; nothing was run.
            ResultMissing: The worker no longer holds ``ref``.
        """
        self._table(ref)
        return _call(_service().sample, ref.result_id, n)

    def count(self, ref: ResultRef) -> int:
        """The number of rows in ``ref``; a deferred result's plan is counted by a job each time."""
        self._table(ref)
        return _call(_service().count, ref.result_id)

    def exists(self, ref: ResultRef) -> bool:
        """Whether the worker still holds ``ref`` and, when materialised, its file is still there."""
        self._check(ref)
        return _call(_service().exists, ref.result_id)

    def release(self, ref: ResultRef) -> None:
        """Forget ``ref`` on the worker: its file, its record and its job entry; one it no longer holds is left."""
        self._check(ref)
        _call(_service().release, ref.result_id)

    def plan_for(self, ref: ResultRef) -> pl.LazyFrame:
        """The leaf that reads ``ref``, decoded from its location without a call: a scan of the file the worker
        wrote, or the deferred plan it holds.

        Raises:
            ForeignRef: Another backend minted ``ref``.
            OperationUnsupported: ``ref`` is a blob, which has no rows to plan over.
        """
        self._table(ref)
        location = wire.WorkerLocation.model_validate_json(ref.location)
        if location.kind == "file":
            return pl.scan_ipc(location.path)
        if location.kind == "plan":
            return pl.LazyFrame.deserialize(io.BytesIO(location.plan))
        raise OperationUnsupported(f"Result {ref.result_id} is a blob: it has no rows to plan over")

    def export(self, ref: ResultRef, uri: str, credential: CredentialRef | None) -> str:
        """Write ``ref`` to ``uri`` as parquet by the worker's export job, or copy a blob there, and return ``uri``.

        Raises:
            ForeignRef: Another backend minted ``ref``.
            ResultMissing: The worker no longer holds ``ref``.
            TaskFailed: The sink or the copy failed.
            PlanRejected: The worker refused the URI's scheme for a blob.
            ValueError: ``ref`` is a blob and ``credential`` is set; nothing was run.
        """
        self._check(ref)
        if ref.kind == "blob" and credential is not None:
            raise ValueError("A blob is copied to a URI that carries its own authorisation; it takes no credential")
        service = _service()
        body = service.ExportBody(uri=uri, credential=_wire_credential(credential))
        return _call(service.export, ref.result_id, body)

    def import_uri(
        self,
        uri: str,
        schema: Schema | None,
        row_count: int | None,
        credential: CredentialRef | None,
        kind: Literal["table", "blob"] = "table",
    ) -> ResultRef:
        """Hold a scan of the parquet at ``uri`` as a deferred result under a fresh id, with the ``schema`` and
        ``row_count`` given; with ``kind="blob"``, a copy of the bytes at ``uri`` as a blob.

        Raises:
            PlanRejected: The worker refused the URI's scheme for a blob.
            TaskFailed: The blob's copy or fetch failed, or the credential's connection could not be read.
            ValueError: A table without ``schema``, or a blob with a ``schema``, a ``row_count`` or a ``credential``;
                nothing was run.
        """
        result_id = uuid.uuid4().hex
        service = _service()
        if kind == "blob":
            if schema is not None or row_count is not None or credential is not None:
                raise ValueError("A blob import takes no schema, no row count and no credential")
            body = service.ImportBody(uri=uri, credential=None, columns=[], row_count=None, kind="blob")
        else:
            if schema is None:
                raise ValueError("A table import needs its schema: it is the caller's, never read from the file")
            body = service.ImportBody(
                uri=uri, credential=_wire_credential(credential), columns=_wire_columns(schema), row_count=row_count
            )
        return self._ref_from_wire(_call(service.import_uri, result_id, body))

    def status(self, handle: JobHandle) -> JobStatus:
        """The state of the job ``handle`` names, as ``GET /v1/jobs/{id}`` answers it.

        Raises:
            ForeignRef: Another backend's handle.
            LookupError: The worker has no job by that id.
        """
        self._own(handle)
        try:
            status = _service().job_status(handle.job_id)
        except HTTPException as e:
            if e.status_code == 404:
                raise LookupError(f"The embedded worker has no job {handle.job_id!r}") from e
            raise _typed(e) from e
        state = _JOB_STATES[status.status]
        error = (status.error_message or status.status) if state == "failed" else None
        return JobStatus(state=state, progress=status.progress, error=error)

    def cancel(self, handle: JobHandle) -> None:
        """Stop the job ``handle`` names; one that already ended, or runs in this process, is left as it is."""
        self._own(handle)
        try:
            _service().cancel_job(handle.job_id)
        except HTTPException as e:
            if e.status_code != 404:
                raise _typed(e) from e

    def _check(self, ref: ResultRef) -> None:
        if ref.backend_id != self.id:
            raise ForeignRef(f"Result {ref.result_id!r} belongs to backend {ref.backend_id!r}, not {self.id!r}")

    def _table(self, ref: ResultRef) -> None:
        """``_check``, then ``OperationUnsupported`` for a blob, which has no rows to plan over, sample or count."""
        self._check(ref)
        if ref.kind == "blob":
            raise OperationUnsupported(
                f"Result {ref.result_id} is a blob: it has no rows to plan over, sample or count"
            )

    def _own(self, handle: JobHandle) -> None:
        if handle.backend_id != self.id:
            raise ForeignRef(f"Job {handle.job_id!r} belongs to backend {handle.backend_id!r}, not {self.id!r}")

    def _from_wire(self, answer: wire.SubmitResult) -> SubmitResult:
        """Core's ``SubmitResult`` for the worker's answer, its reference through ``_ref_from_wire``."""
        return SubmitResult(
            ref=self._ref_from_wire(answer.ref),
            sample=answer.sample,
            handle=JobHandle(self.id, answer.job_id),
        )

    def _ref_from_wire(self, ref: wire.ResultRef) -> ResultRef:
        """Core's ``ResultRef`` for a worker's reference: this backend's id stamped, the columns read as dtypes."""
        columns = pl.Schema([(column.name, safe_eval_pl_type(column.data_type)) for column in ref.columns])
        return ResultRef(
            backend_id=self.id,
            result_id=ref.result_id,
            schema=_flowfile_schema(columns),
            row_count=ref.row_count,
            state=ref.state,
            location=ref.location,
            kind=ref.kind,
        )


def _call(body: Callable[..., Any], *args: Any) -> Any:
    """Call a ``/v1`` body with ``args`` and answer what it returns; what it raises comes back typed (``_typed``)."""
    try:
        return body(*args)
    except HTTPException as e:
        raise _typed(e) from e


def _typed(exc: HTTPException) -> ComputeError:
    """The typed error for the ``HTTPException`` a ``/v1`` body raised, as ``WorkerBackend`` reads the route's answer.

    404 is a result, an input or a model the worker does not hold; 422 is what a body refuses before anything runs
    (a sample above the cap, a directory scan, a table where a model was expected, a blob's scheme); a 500 carrying a
    child's ``status``, ``error_message`` and ``error_kind`` goes through ``worker_failure`` as a failed task does; any
    other code is a ``TaskFailed`` naming it.
    """
    detail = exc.detail
    if exc.status_code == 404:
        return ResultMissing(str(detail))
    if exc.status_code == 422:
        return PlanRejected(str(detail))
    if exc.status_code == 500 and isinstance(detail, dict) and "status" in detail:
        return worker_failure(detail.get("error_kind"), detail.get("error_message") or detail["status"])
    return TaskFailed(f"The embedded worker answered {exc.status_code}: {detail}")
