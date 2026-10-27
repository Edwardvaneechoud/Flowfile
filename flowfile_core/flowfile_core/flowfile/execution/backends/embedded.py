"""The adapter to the worker embedded in this process: the worker's ``/v1`` bodies called directly, no wire.

``EmbeddedWorkerBackend`` is ``WorkerBackend``'s ``/v1`` half with the wire replaced by a call into
``flowfile_worker.service``. Each method passes the worker's own request model and reads the answer as
``WorkerBackend`` reads the route's, so its references are a worker's references under this process's id. The plan
crosses as ``serialize()`` bytes, so ``funcs.submit_result`` is the one submit and its materialisation policy is this
backend's. There is no token, socket or handshake gate; the handshake runs through ``capabilities()`` and the probe
jobs like any backend's.

Where a job runs is the host's choice: with ``flowfile_worker.spawner.in_process`` set, in the calling process; unset,
in the worker's pool or a child. Nothing of the worker is imported until the first call (``_service``).

The Delta writes and ``viz_session``, which the worker serves on its task routes, run through ``service._run`` and the
session registry. A custom node whose class has no node file runs ``process`` in this process. The embedded worker is
on core's host, so it reads and writes ``file`` whatever ``FLOWFILE_WORKER_LOCAL_FILES`` says.
"""

from __future__ import annotations

import functools
import io
import uuid
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import polars as pl
from fastapi import HTTPException
from pydantic import ValidationError, validate_call

from flowfile_core.flowfile.execution.backends.worker import (
    _JOB_STATES,
    V1_OPS,
    _delta_destination,
    _v1_error,
    _wire_columns,
    _wire_credential,
    _WorkerReferences,
)
from flowfile_core.flowfile.execution.errors import ComputeError, OperationUnsupported
from flowfile_core.flowfile.execution.payloads import (
    CustomNodePayload,
    DeltaMergePayload,
    DeltaScd2Payload,
    DeltaWritePayload,
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
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from shared import compute_protocol as wire
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities

if TYPE_CHECKING:
    from flowfile_worker import service as worker_service


def _service() -> worker_service:
    """The worker's service module, imported on the first call, so nothing of the worker loads before the first job."""
    from flowfile_worker import service

    return service


class EmbeddedWorkerBackend(_WorkerReferences):
    """``ComputeBackend`` for the worker embedded in this process, reached by a direct call to its ``/v1`` bodies.

    Its id is ``local``; a reference or job handle of another id is refused with ``ForeignRef``. ``capabilities()``
    is the embedded worker's own, with ``file`` added to both scheme sets.

    Materialisation policy is the worker's (``service._materialise``): a plan is written to a file when
    ``hints.must_materialise`` is set, or when it is ``wide`` and either ``preview_rows`` is not ``None`` or ``fanout``
    is above one. Every other plan is held deferred. A reference carries a ``WorkerLocation`` JSON, which ``plan_for``
    decodes without a call. A blob is refused with ``OperationUnsupported`` by ``plan_for``, ``sample``, ``count`` and
    as a ``submit`` input.

    ``run_op`` runs the ``/v1/ops`` and Delta kinds as the job the payload's ``job_id`` names, so ``status`` and
    ``cancel`` reach it. Its refs are keyed as ``WorkerBackend`` keys them: each output the stored result
    ``<job_id>_<i>``, a training's model the blob ``<job_id>_0``. ``metadata`` is a Kafka read's offsets, a training's
    digest, size and type, a Delta write's report or a ``viz_session`` answer; a write answers no refs. ``cancel``
    leaves a job that already ended, or one running in this process, as it is.
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

        The payload is unpacked before anything runs, so an empty one is a ``ValueError``. The Delta kinds and
        ``viz_session`` go to the worker's job runner and session registry (``_delta``, ``_viz_session``). A custom node
        with no node file runs here (``_custom_node_here``).

        Raises:
            ForeignRef: An ``ml_apply`` model another backend minted.
            OperationUnsupported: A kind this backend has no body for; nothing was run.
            PlanRejected: The worker could not load the operation's plan, or the body refused it before running.
            ResultMissing: An ``ml_apply`` model the worker no longer holds.
            TaskFailed: The operation failed.
            RuntimeError: The visualisation session could not answer.
            ValueError: A custom node did not return one of its declared outputs.
        """
        match op:
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
        spec = V1_OPS.get(op)
        if spec is None:
            raise OperationUnsupported(f"The embedded worker backend does not run the {op.value} operation")
        job = unpack(payload, spec.payload)
        if op is OperationKind.custom_node and job.node_source is None:
            return self._custom_node_here(job)
        for ref in spec.held(job):
            self._check(ref)
        answer = _call(_route_body(op), spec.request(job), job.job_id, **spec.params(job))
        refs = {name: self._ref_from_wire(ref) for name, ref in answer.outputs.items()}
        return spec.result(job, refs, answer.metadata)

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
        """The leaf that reads ``ref``, decoded from its location without a call.

        A scan of the file the worker wrote, or the deferred plan it holds.

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
        """Hold a scan of the parquet at ``uri`` as a deferred result under a fresh id, or a blob of its bytes.

        The ``schema`` and ``row_count`` are the caller's; ``kind="blob"`` makes a blob.

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

        Not part of the protocol: the custom-node dry run reads its job's result here once ``status`` says it is done.

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
        """Run a Delta ``operation`` over the payload's plan as its task route would; its report is the metadata.

        The destination is ``_delta_destination``'s: a catalog table by identity, or the payload's path. The job is
        filed under the cache path the task route gives such a task.
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

        The answer is shaped as ``/catalog/visualize_fields`` and ``/catalog/visualize_query`` shape theirs, with a
        failure of the request as its ``error``. A session that cannot be opened or is busy raises ``RuntimeError``,
        which the analysis routes answer as 502.
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

        With a payload user, ``process`` reads that user's secrets through core's secret store. The refs are
        ``{"result": ref}`` for one frame, one per declared output keyed by output handle for a dict, none otherwise.
        Each output is held as the stored result ``<job_id>_<i>``. A failed write releases the outputs held before it
        and removes its file, so a failed node holds nothing. Frames are collected before writing because a failing
        ``sink_ipc`` may create its file after the removal.

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
            try:
                frame.lazy().collect(engine="streaming").write_ipc(path)
                written = pl.scan_ipc(path)
                row_count = written.select(pl.len()).collect().item()
                held = service._hold_output(
                    result_id, path, written.collect_schema(), row_count, job.flow_id, job.node_id
                )
            except BaseException:
                for earlier in refs.values():
                    service.release(earlier.result_id)
                Path(path).unlink(missing_ok=True)
                raise
            refs[name] = self._ref_from_wire(held)
        return OpResult(refs=refs)


def _call(body: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call a ``/v1`` body with ``args`` and answer what it returns; what it raises comes back typed (``_typed``).

    Arguments a body refuses (``_route_body`` validates them as its route does) are ``PlanRejected``, the route's 422.
    """
    try:
        return body(*args, **kwargs)
    except HTTPException as e:
        raise _typed(e) from e
    except ValidationError as e:
        raise _v1_error(422, str(e), answered_by="The embedded worker") from e


@functools.cache
def _route_body(op: OperationKind) -> Callable[..., Any]:
    """The ``/v1/ops/<kind>`` body for ``op``, its arguments validated from its own signature as its route's are."""
    return validate_call(getattr(_service(), op.value))


def _typed(exc: HTTPException) -> ComputeError:
    """The typed error for an ``HTTPException`` a ``/v1`` body raised, mapped by ``_v1_error`` as a route's answer is.

    404 is ``ResultMissing``, 422 ``PlanRejected``, a child's 500 goes through ``worker_failure``, and any other code
    is a ``TaskFailed`` naming it.
    """
    return _v1_error(exc.status_code, exc.detail, answered_by="The embedded worker")
