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
The four kinds the worker still serves on its task routes, the Delta write, merge and SCD2 write and the
visualisation session, run here through the worker's own job runner (``service._run``) with the operation and the
keyword arguments those routes build, and through its session registry, so every kind the worker declares runs on this
backend too. A custom node whose class has no node file, which exists in this process and
nowhere else, runs its ``process`` here and its outputs are held as the worker holds a child's. The embedded worker is
on core's host by definition, so it reads and writes ``file`` whatever ``FLOWFILE_WORKER_LOCAL_FILES`` says.
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
    _cloud_write_settings,
    _custom_node_request,
    _database_read_settings,
    _database_write_settings,
    _declared_outputs,
    _delta_destination,
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
    CloudWritePayload,
    CustomNodePayload,
    DatabaseReadPayload,
    DatabaseWritePayload,
    DeltaMergePayload,
    DeltaScd2Payload,
    DeltaWritePayload,
    FileReadPayload,
    FileWritePayload,
    FuzzyJoinPayload,
    GoogleAnalyticsReadPayload,
    KafkaReadPayload,
    MlApplyPayload,
    MlTrainPayload,
    PolarsCodePayload,
    RandomSplitPayload,
    RestApiReadPayload,
    SqlQueryPayload,
    VizSessionPayload,
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
from flowfile_core.flowfile.flow_node.multi_output import output_handle
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
    serving one. ``capabilities()`` is the embedded worker's own answer, every kind the worker declares among its
    ``operations``, with ``file`` among its read and write schemes whatever ``FLOWFILE_WORKER_LOCAL_FILES`` says: the
    embedded worker is on core's host.

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
    ``polars_code``, ``sql_query`` and the file, database and cloud writes through the body of ``POST /v1/ops/<kind>``,
    the payload's ``job_id`` the job's, so ``status`` and ``cancel`` reach it; the refs are keyed as ``WorkerBackend``
    keys them, each output the stored result ``<job_id>_<i>``, a training's model the blob ``<job_id>_0``, a Kafka
    read's offsets and a training's digest, size and type as ``metadata``, a write's none. The three Delta kinds run as
    the job the payload's ``job_id`` names through the worker's own job runner, with the operation and the keyword
    arguments their task routes build, no refs and the Delta report as ``metadata``; ``viz_session`` is one request
    to the worker's session registry, its answer the ``metadata`` in the shape the visualisation routes answer. A
    custom node whose class has no node file runs ``process`` in this process and holds each output as a stored result
    ``<job_id>_<i>``. ``cancel`` of a job that already ended, or of one running in this process, which nothing stops,
    is left as it is.
    """

    id = LOCAL_BACKEND_ID
    protocol_version = PROTOCOL_VERSION

    def capabilities(self) -> Capabilities:
        """The embedded worker's capabilities: every kind the worker declares, ``file`` among both scheme sets."""
        declared = _service().worker_capabilities()
        return declared.model_copy(
            update={
                "read_schemes": declared.read_schemes | {"file"},
                "write_schemes": declared.write_schemes | {"file"},
            }
        )

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
        read's the offsets to commit, a Delta write's its report; a write answers no refs. The Delta kinds and
        ``viz_session`` take the worker's job runner and session registry (``_delta``, ``_viz_session``), a custom
        node with no node file runs here (``_custom_node_here``).

        Raises:
            ForeignRef: An ``ml_apply`` model another backend minted.
            PlanRejected: The worker could not load the operation's plan, or the body refused it before running.
            ResultMissing: An ``ml_apply`` model the worker no longer holds.
            TaskFailed: The operation failed.
            RuntimeError: The visualisation session could not answer, as the worker's route fails the request.
            ValueError: A custom node did not return one of its declared outputs.
        """
        service = _service()
        args: tuple[Any, ...] = ()
        match op:
            case OperationKind.file_write:
                job = unpack(payload, FileWritePayload)
                settings = job.settings
                body = service.models.PolarsScriptWrite(
                    operation=job.plan.serialize(),
                    data_type=settings.file_type,
                    path=settings.abs_file_path,
                    write_mode=settings.write_mode,
                    sheet_name=settings.sheet_name,
                    delimiter=settings.delimiter,
                    compression=settings.compression,
                    flowfile_flow_id=job.flow_id,
                    flowfile_node_id=job.node_id,
                )
                run = service.file_write
            case OperationKind.database_write:
                job = unpack(payload, DatabaseWritePayload)
                sent = _database_write_settings(job).model_dump_json()
                body = service.models.DatabaseScriptWrite.model_validate_json(sent)
                run = service.database_write
            case OperationKind.cloud_write:
                job = unpack(payload, CloudWritePayload)
                sent = _cloud_write_settings(job).model_dump_json()
                body = service.models.CloudStorageScriptWrite.model_validate_json(sent)
                run = service.cloud_write
            case OperationKind.delta_write:
                write = unpack(payload, DeltaWritePayload)
                kwargs = {"mode": write.mode, "partition_by": write.partition_by, "enable_cdf": write.enable_cdf}
                return self._delta(write, "write_delta", kwargs)
            case OperationKind.delta_merge:
                write = unpack(payload, DeltaMergePayload)
                kwargs = {
                    "merge_mode": write.merge_mode,
                    "merge_keys": write.merge_keys,
                    "partition_by": write.partition_by,
                    "enable_cdf": write.enable_cdf,
                }
                return self._delta(write, "merge_delta", kwargs)
            case OperationKind.delta_scd2:
                write = unpack(payload, DeltaScd2Payload)
                kwargs = {
                    "run_timestamp": write.run_timestamp,
                    "business_keys": write.business_keys,
                    "compare_columns": write.compare_columns,
                    "full_snapshot": write.full_snapshot,
                    "surrogate_key_column": write.surrogate_key_column,
                    "valid_from_column": write.valid_from_column,
                    "valid_to_column": write.valid_to_column,
                    "is_current_column": write.is_current_column,
                    "partition_by": write.partition_by,
                    "partition_on_current": write.partition_on_current,
                }
                return self._delta(write, "scd2_delta", kwargs)
            case OperationKind.viz_session:
                return self._viz_session(unpack(payload, VizSessionPayload))
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
                if job.node_source is None:
                    return self._custom_node_here(job)
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

    def task_result(self, handle: JobHandle) -> Any:
        """What the job ``handle`` names reported as its result (its status's ``results``); ``None`` until done.

        Not part of the protocol, as on ``WorkerBackend``: the custom-node dry run starts its job on the worker's task
        route and reads the job's result here once ``status`` says it is done.

        Raises:
            ForeignRef: Another backend's handle.
            LookupError: The worker has no job by that id.
        """
        self._own(handle)
        service = _service()
        with service.status_dict_lock:
            status = service.status_dict.get(handle.job_id)
        if status is None:
            raise LookupError(f"The embedded worker has no job {handle.job_id!r}")
        return status.results

    def cancel(self, handle: JobHandle) -> None:
        """Stop the job ``handle`` names; one that already ended, or runs in this process, is left as it is."""
        self._own(handle)
        try:
            _service().cancel_job(handle.job_id)
        except HTTPException as e:
            if e.status_code != 404:
                raise _typed(e) from e

    def _delta(
        self, write: DeltaWritePayload | DeltaMergePayload | DeltaScd2Payload, operation: str, kwargs: dict[str, Any]
    ) -> OpResult:
        """Run a Delta ``operation`` over the payload's plan as the worker's task route would; its report is the
        metadata.

        The destination is ``_delta_destination``'s: a catalog table by identity, resolved under the worker's own
        root, or the payload's path. The job is filed under the cache path the task route gives such a task.
        """
        file_ref = str(_service().result_path(write.flow_id, write.job_id))
        kwargs = {**_delta_destination(write), **kwargs}
        plan = write.plan.serialize()
        reported = self._job(write.job_id, plan, operation, file_ref, write.flow_id, write.node_id, kwargs)
        return OpResult(refs={}, metadata=reported if isinstance(reported, dict) else {})

    def _job(
        self,
        job_id: str,
        plan: bytes,
        operation: str,
        file_ref: str,
        flow_id: int,
        node_id: int | str,
        kwargs: dict[str, Any],
    ) -> Any:
        """Run ``operation`` over ``plan`` as the job ``job_id`` through ``service._run``; what the job reported.

        The job's end is stamped so its entry ages out like a held result's, and its ``results`` are taken out of the
        entry once read. A job that did not complete raises the typed error its 500 maps to.
        """
        service = _service()
        try:
            status = service._run(job_id, plan, operation, file_ref, flow_id, node_id, kwargs)
        finally:
            service.results.mark_ended(job_id)
        if status.status != "Completed":
            raise _typed(service._failure(status))
        with service.status_dict_lock:
            reported, status.results = status.results, None
        return reported

    def _viz_session(self, request: VizSessionPayload) -> OpResult:
        """Answer one Graphic Walker request in the worker's visualisation session over the payload's plan.

        The session registry holds the plan lazily in a child of its own under the session key, as it does for a
        worker's route; the answer is shaped as ``/catalog/visualize_fields`` and ``/catalog/visualize_query`` shape
        theirs, a ``ValueError`` or any other failure of the request as its ``error``. A session that cannot be opened
        or is busy fails the request as the route fails it, the ``RuntimeError`` the analysis routes answer as 502.
        """
        from flowfile_worker.viz_sessions import viz_session_registry

        models = _service().models
        plan = request.plan.serialize()
        source = models.VizWorkerSource(kind="plan", session_key=request.session_key, plan_bytes=plan)
        fields = request.request == "fields"
        try:
            if fields:
                result, cache_hit = viz_session_registry.execute(source, "fields", None, None)
                answer = models.VisualizeFieldsResponse(fields=result["fields"], cache_hit=cache_hit)
            else:
                result, _ = viz_session_registry.execute(source, "execute", request.query, request.max_rows)
                answer = models.VisualizeQueryResponse(**result)
        except HTTPException as e:
            raise RuntimeError(f"Worker visualize_{'fields' if fields else 'query'} failed: {e.detail}") from e
        except Exception as e:
            answer = (models.VisualizeFieldsResponse if fields else models.VisualizeQueryResponse)(error=str(e))
        return OpResult(refs={}, metadata=answer.model_dump())

    def _custom_node_here(self, job: CustomNodePayload) -> OpResult:
        """Run ``process`` of a custom node whose class has no node file in this process and hold its outputs.

        The instance is built from the payload's ``settings_values`` and, when the payload names a user, reads that
        user's secrets through core's secret store as ``process`` accesses them. The refs are ``{"result": ref}`` for
        one frame, one per declared output keyed by its output handle for a dict of them, none otherwise; each frame is
        written to the file the worker gives the output ``<job_id>_<i>`` and held with that file's columns and rows.

        Raises:
            ValueError: ``process`` returned a dict without one of the declared outputs.
        """
        instance = type(job.custom_node).from_settings(job.settings_values)
        if job.user_id is not None:
            instance.set_execution_context(job.user_id)
        output = instance.process(*job.inputs)
        if isinstance(output, dict):
            for name in job.output_names:
                if name not in output:
                    raise ValueError(f"process() did not return declared output '{name}'")
            frames = {output_handle(i): output[name] for i, name in enumerate(job.output_names)}
        elif isinstance(output, pl.LazyFrame | pl.DataFrame):
            frames = {"result": output}
        else:
            return OpResult(refs={})
        service = _service()
        refs = {}
        for (name, frame), result_id in zip(frames.items(), service._op_outputs(job.job_id, len(frames)), strict=True):
            path = service._output_file(job.flow_id, result_id)
            frame.lazy().sink_ipc(path)
            written = pl.scan_ipc(path)
            row_count = written.select(pl.len()).collect().item()
            held = service._hold_output(result_id, path, written.collect_schema(), row_count, job.flow_id, job.node_id)
            refs[name] = self._ref_from_wire(held)
        return OpResult(refs=refs)

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
        """Core's ``ResultRef`` for a worker's reference: this backend's id stamped, the columns read as dtypes.

        A bare class (``"Categorical"``, how Polars writes ``pl.Categorical()``) is made an instance first, since
        ``pl.Schema`` refuses a class whose constructor takes arguments.
        """
        dtypes = [(column.name, safe_eval_pl_type(column.data_type)) for column in ref.columns]
        columns = pl.Schema([(name, dtype() if isinstance(dtype, type) else dtype) for name, dtype in dtypes])
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
