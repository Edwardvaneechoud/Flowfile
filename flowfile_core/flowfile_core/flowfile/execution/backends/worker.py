"""The worker backend: core's client of ``flowfile_worker`` over the worker's task routes.

It runs everything through the transport in ``flow_data_engine/subprocess_operations``: a plan or an operation
becomes a worker task started by the fetcher core uses for it, the status, clear and cancel routes follow a task,
and a stored result stays in the Arrow IPC file the task wrote. Core and the worker share a disk, so schemas, row
counts and samples of a stored result are read from that file here, not sent over the wire. ``submit`` stores a
plan or keeps it deferred in this process; the class docstring has the policy.
"""

from __future__ import annotations

import base64
import os
import random
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

import polars as pl
import pyarrow as pa
import requests

from flowfile_core.flowfile.execution.errors import (
    ComputeUnavailable,
    ForeignRef,
    OperationUnsupported,
    ResultMissing,
    TaskFailed,
    worker_failure,
)
from flowfile_core.flowfile.execution.op_payloads import (
    CustomNodePayload,
    FuzzyJoinPayload,
    MlApplyPayload,
    MlTrainPayload,
    RandomSplitPayload,
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
from flowfile_core.flowfile.execution.write_payloads import (
    CloudWritePayload,
    DatabaseWritePayload,
    DeltaMergePayload,
    DeltaWritePayload,
    FileWritePayload,
    unpack,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import convert_stats_to_column_info
from flowfile_core.flowfile.flow_data_engine.fuzzy_matching.prepare_for_fuzzy_match import prepare_for_fuzzy_match
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import CustomNodeExecuteInput
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    BaseFetcher,
    ExternalCloudWriter,
    ExternalCreateFetcher,
    ExternalCustomNodeFetcher,
    ExternalDatabaseFetcher,
    ExternalDatabaseWriter,
    ExternalDfFetcher,
    ExternalFuzzyMatchFetcher,
    ExternalGoogleAnalyticsFetcher,
    ExternalKafkaFetcher,
    ExternalOutputWriter,
    ExternalRestApiFetcher,
    ExternalSampler,
    MLApplyFetcher,
    MLTrainFetcher,
    cancel_task,
    clear_task_from_worker,
    get_status,
    get_worker_capabilities,
    results_exists,
)
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from flowfile_core.flowfile.sources.external_sources.sql_source.models import DatabaseExternalWriteSettings
from flowfile_core.flowfile.user_defined.dispatch import resolve_secret_payload
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageWriteSettingsWorkerInterface,
    FullCloudStorageConnectionWorkerInterface,
)
from flowfile_core.schemas.transform_schema import FuzzyMatchInputManager
from flowfile_core.utils.arrow_reader import get_read_top_n
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities

WORKER_BACKEND_ID = "worker"
_OUTPUT_SEPARATOR = "#"
_SPLIT_RANK = "__split_rank__"
_NO_FLOW = -1
_JOB_STATES: dict[str, Literal["running", "done", "failed", "cancelled"]] = {
    "Starting": "running",
    "Processing": "running",
    "Completed": "done",
    "Error": "failed",
    "Unknown Error": "failed",
    "Cancelled": "cancelled",
}


@dataclass
class _DeferredPlan:
    """A deferred result: its plan, where its worker tasks are filed, and the file its last sample task wrote."""

    plan: pl.LazyFrame
    flow_id: int
    node_id: int | str
    sample_file: str | None = None


class WorkerBackend:
    """``ComputeBackend`` for ``flowfile_worker``, built on the worker's existing routes.

    Materialisation policy: ``submit`` stores a plan on the worker when ``hints.must_materialise`` is set, or when
    the plan is ``wide`` and a preview is asked for (``preview_rows`` is not ``None``); every other plan stays
    deferred. A wide plan is stored so its preview and its consumers read rows computed once; with no preview
    asked (Performance mode) nothing reads them before the run's writers do, so it stays a plan. ``fanout`` is not
    read yet.

    - Stored, ``"materialised"``: a worker store task wrote the rows to an Arrow IPC file, which the worker keeps
      until the task is cleared. ``result_id`` is the worker task id, ``location`` the file's path as UTF-8; the
      inline sample, ``sample``, ``count`` and ``plan_for`` (a ``pl.scan_ipc``) read that file. A custom node's
      outputs (``run_op``) add ``#<output name>`` to the task id and share the task, so releasing one releases
      them all.
    - ``"deferred"``: this backend holds the plan object in process, unserialised, and ``location`` is the result
      id as UTF-8. ``plan_for`` returns the plan itself, so a consumer's plan composes on it; the inline sample and
      ``sample(ref, n)`` run a worker sampler task over it, filed under ``result_id``; ``count`` runs the worker's
      count task. Those tasks serialise the plan when they send it. ``exists`` is true while this backend holds
      the plan, and ``release`` drops it and clears its sampler task.

    While ``submit`` waits on a task, ``cancel`` with the handle ``JobHandle("worker", result_id)`` reaches that
    task's fetcher; while a ``run_op`` that takes a payload dataclass waits, so does the handle with its payload's
    ``job_id``.

    Nothing here runs the worker handshake: every worker call goes through the transport's worker session or
    its WebSocket submit, which run it before they reach the worker, so every method that calls the worker is
    gated by it already and can raise its ``VersionMismatch`` or ``ComputeUnavailable``.
    """

    id = WORKER_BACKEND_ID
    protocol_version = PROTOCOL_VERSION

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._deferred: dict[str, _DeferredPlan] = {}
        self._in_flight: dict[str, BaseFetcher] = {}
        self._operations: dict[OperationKind, Callable[[OpPayload], OpResult]] = {
            OperationKind.database_read: self._database_read,
            OperationKind.kafka_read: self._kafka_read,
            OperationKind.google_analytics_read: self._google_analytics_read,
            OperationKind.rest_api_read: self._rest_api_read,
            OperationKind.file_read: self._file_read,
            OperationKind.fuzzy_join: self._fuzzy_join,
            OperationKind.random_split: self._random_split,
            OperationKind.custom_node: self._custom_node,
            OperationKind.ml_train: self._ml_train,
            OperationKind.ml_apply: self._ml_apply,
            OperationKind.database_write: self._database_write,
            OperationKind.cloud_write: self._cloud_write,
            OperationKind.file_write: self._file_write,
            OperationKind.delta_write: self._delta_write,
            OperationKind.delta_merge: self._delta_merge,
            OperationKind.delta_scd2: self._delta_scd2,
        }

    def capabilities(self) -> Capabilities:
        """The worker's answer to ``GET /capabilities``."""
        return get_worker_capabilities()

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
        """Store ``plan`` on the worker or hold it deferred, by the policy in the class docstring.

        A stored plan is serialised once, to send it to the worker's store task; a deferred one is held as the
        object it is. ``inputs`` must be this backend's results; the plan already reads them, so they are only
        checked. The worker files the store or sampler task as ``result_id`` under flow ``flow_id`` and node
        ``node_id``, which is where its log lines go.

        Raises:
            ForeignRef: An input another backend minted.
            PlanRejected: The worker could not load the plan.
            TaskFailed: The plan failed on the worker.
        """
        for ref in inputs:
            self._check(ref)
        if hints.must_materialise or (hints.wide and hints.preview_rows is not None):
            ref = self._store(plan, result_id, flow_id, node_id)
            sample = None if hints.preview_rows is None else self.sample(ref, hints.preview_rows)
        else:
            ref, sample = self._defer(plan, result_id, flow_id, node_id, hints.preview_rows)
        return SubmitResult(ref=ref, sample=sample, handle=JobHandle(self.id, result_id))

    def run_op(self, op: OperationKind, payload: OpPayload) -> OpResult:
        """Run ``op`` as a worker task through the fetcher core uses for it and wait for it to end.

        Reads and the catalog writer's SCD2 write take what their fetcher takes, under these keys (``flow_id`` and
        ``node_id`` name the flow and node the worker logs the task under):

        - ``database_read``: ``settings`` (``DatabaseExternalReadSettings``).
        - ``kafka_read``, ``google_analytics_read``, ``rest_api_read``: ``settings``, the worker read settings.
        - ``file_read``: ``received_table`` (``ReceivedTable`` with its absolute path set; its ``file_type`` picks
          the worker's reader), ``flow_id``, ``node_id``.
        - ``delta_scd2``: ``lf``, ``kwargs`` (the keyword arguments of the worker's ``scd2_delta`` operation),
          ``flow_id``, ``node_id``.

        Every other operation takes one dataclass under ``"payload"``: the writes ``file_write``, ``database_write``,
        ``cloud_write``, ``delta_write`` and ``delta_merge`` their ``write_payloads`` one, and ``fuzzy_join``,
        ``random_split``, ``custom_node``, ``ml_train`` and ``ml_apply`` their ``op_payloads`` one. Its frames are
        serialised here, its credentials sent still encrypted, and the worker task is filed under the payload's
        ``job_id`` (a write's under the id the worker gives it), whose fetcher ``cancel(JobHandle("worker", job_id))``
        reaches while the call waits. ``fuzzy_join`` checks the join and selects and renames each side here, as the
        in-process join does, before it sends the sides; ``custom_node`` resolves the ``$ffsec$`` ciphertext of the
        secrets the node's ``SecretSelector`` fields select here and sends only that.

        A read's and ``fuzzy_join``'s and ``ml_apply``'s refs are ``{"result": ref}``, a stored result. A custom
        node's refs are one stored result per declared output, keyed by its output handle (``output-0``,
        ``output-1``, ...). ``random_split`` stores the shuffled rows once, as the payload's ``job_id`` task, and
        returns one deferred ref per split, keyed by the split's name in the settings' order, whose plan slices that
        task's file and which carries the split's row count; releasing a split leaves the shuffled rows in place. A
        write has no refs. ``ml_train`` and the Delta writes return the worker's result dict as ``metadata``: the
        trained model's ``sha256``, ``size_bytes`` and ``model_type`` (the model itself is the file at
        ``staging_path``, not a table, so there is no ref), or the written table's schema, counts, size and version.

        Raises:
            OperationUnsupported: ``viz_session``, which has no worker task, and a custom node without a node file.
            PlanRejected: The worker could not load the operation's plan.
            TaskFailed: The operation failed on the worker.
            ValueError: The worker did not return one of a custom node's declared outputs.
        """
        operation = self._operations.get(op)
        if operation is None:
            raise OperationUnsupported(f"The worker backend does not run the {op.value} operation")
        return operation(payload)

    def sample(self, ref: ResultRef, n: int) -> bytes:
        """The first ``n`` rows of ``ref``, or all of them when it has fewer, as an Arrow IPC stream.

        A stored result's rows are read from its file; a deferred one's come from a worker sampler task over its
        plan, filed under ``ref.result_id`` like the sample ``submit`` took.
        """
        if ref.state == "deferred":
            held = self._held_plan(ref)
            held.sample_file = self._run_sampler(held, ref.result_id, n)
            return _read_sample(ref, held.sample_file, n)
        return _read_sample(ref, self._file(ref), n)

    def count(self, ref: ResultRef) -> int:
        """The number of rows in ``ref``'s file, or for a deferred result what the worker's count task returns."""
        if ref.state == "deferred":
            held = self._held_plan(ref)
            fetcher = _run(
                lambda: ExternalDfFetcher(
                    flow_id=_NO_FLOW,
                    node_id=_NO_FLOW,
                    lf=held.plan,
                    operation_type="calculate_number_of_records",
                    wait_on_completion=False,
                )
            )
            return fetcher.result
        path = self._file(ref)
        with _held(ref):
            return _row_count(path)

    def exists(self, ref: ResultRef) -> bool:
        """Whether this backend holds a deferred ``ref``, or the worker still holds the task that stored it."""
        if ref.state == "deferred":
            self._check(ref)
            with self._lock:
                return ref.result_id in self._deferred
        self._file(ref)
        return results_exists(_task_id(ref))

    def release(self, ref: ResultRef) -> None:
        """Drop a deferred ``ref`` and clear its sampler task, or clear the task that stored ``ref``.

        Clearing a task deletes the file it registered.
        """
        if ref.state == "deferred":
            self._check(ref)
            with self._lock:
                held = self._deferred.pop(ref.result_id, None)
            if held is not None and held.sample_file is not None:
                clear_task_from_worker(ref.result_id)
            return
        self._file(ref)
        clear_task_from_worker(_task_id(ref))

    def plan_for(self, ref: ResultRef) -> pl.LazyFrame:
        """The plan of a deferred ``ref``, or a scan of a stored ``ref``'s file; valid only in plans this backend runs.

        A stored result is checked on the shared disk, not with the worker: the scan needs the file, and clearing
        the task deletes it.

        Raises:
            ForeignRef: Another backend minted ``ref``.
            ResultMissing: This backend no longer holds the deferred plan, or the stored result's file is gone.
        """
        if ref.state == "deferred":
            return self._held_plan(ref).plan
        path = self._file(ref)
        if not os.path.isfile(path):
            raise ResultMissing(f"The worker no longer holds result {ref.result_id!r}")
        return pl.scan_ipc(path)

    def result_file(self, ref: ResultRef) -> str | None:
        """The Arrow IPC file on the shared disk that holds ``ref``'s rows or its latest sample.

        A stored result's own file, or the file of a deferred result's last sampler task; ``None`` for a deferred
        result no sample was taken of, or one this backend no longer holds. Core reads the first rows of a result
        through it and treats a file that vanished as a result to compute again.
        """
        if ref.state == "deferred":
            self._check(ref)
            with self._lock:
                held = self._deferred.get(ref.result_id)
            return held.sample_file if held is not None else None
        return self._file(ref)

    def export(self, ref: ResultRef, uri: str, credential: CredentialRef | None) -> str:
        """Not supported: the worker backend has no export."""
        raise OperationUnsupported(f"The {self.id} backend does not export results")

    def import_uri(
        self, uri: str, schema: Schema, row_count: int | None, credential: CredentialRef | None
    ) -> ResultRef:
        """Not supported: the worker backend has no import."""
        raise OperationUnsupported(f"The {self.id} backend does not import data from a URI")

    def status(self, handle: JobHandle) -> JobStatus:
        """The worker's ``GET /status`` answer for the task ``handle`` names."""
        self._own(handle)
        try:
            status = get_status(handle.job_id)
        except requests.RequestException as e:
            raise ComputeUnavailable(f"Could not reach the worker: {e}") from e
        state = _JOB_STATES[status.status]
        error = (status.error_message or status.status) if state == "failed" else None
        return JobStatus(state=state, progress=status.progress, error=error)

    def task_result(self, handle: JobHandle) -> Any:
        """What the worker task ``handle`` names reported as its result (the status's ``results``); ``None`` until done.

        Not part of the protocol: a caller that starts a worker task outside ``run_op`` and follows it with ``status``
        (the custom-node dry run) reads the task's result here once ``status`` says it is done.

        Raises:
            ComputeUnavailable: The worker could not be reached.
        """
        self._own(handle)
        try:
            return get_status(handle.job_id).results
        except requests.RequestException as e:
            raise ComputeUnavailable(f"Could not reach the worker: {e}") from e

    def cancel(self, handle: JobHandle) -> None:
        """Cancel the task ``handle`` names: through its fetcher while ``submit`` waits on it, else on the worker.

        A task that already ended is left as it is.
        """
        self._own(handle)
        with self._lock:
            fetcher = self._in_flight.get(handle.job_id)
        if fetcher is not None:
            fetcher.cancel()
        else:
            cancel_task(handle.job_id)

    def _check(self, ref: ResultRef) -> None:
        if ref.backend_id != self.id:
            raise ForeignRef(f"Result {ref.result_id!r} belongs to backend {ref.backend_id!r}, not {self.id!r}")

    def _file(self, ref: ResultRef) -> str:
        self._check(ref)
        return ref.location.decode()

    def _own(self, handle: JobHandle) -> None:
        if handle.backend_id != self.id:
            raise ForeignRef(f"Job {handle.job_id!r} belongs to backend {handle.backend_id!r}, not {self.id!r}")

    def _held_plan(self, ref: ResultRef) -> _DeferredPlan:
        self._check(ref)
        with self._lock:
            held = self._deferred.get(ref.result_id)
        if held is None:
            raise ResultMissing(f"The {self.id} backend no longer holds result {ref.result_id!r}")
        return held

    def _store(self, plan: pl.LazyFrame, result_id: str, flow_id: int, node_id: int | str) -> ResultRef:
        """Run ``plan`` as worker store task ``result_id`` and return the result it wrote."""
        with self._lock:
            self._deferred.pop(result_id, None)
        plan_bytes = plan.serialize()
        fetcher = self._run_tracked(
            result_id,
            lambda: ExternalDfFetcher(
                flow_id=flow_id, node_id=node_id, plan_bytes=plan_bytes, file_ref=result_id, wait_on_completion=False
            ),
        )
        status = fetcher.status
        return self._result_ref(result_id, _written_file(fetcher), status.number_of_records)

    def _defer(
        self,
        plan: pl.LazyFrame,
        result_id: str,
        flow_id: int,
        node_id: int | str,
        preview_rows: int | None,
        *,
        schema: Schema | None = None,
        row_count: int | None = None,
    ) -> tuple[ResultRef, bytes | None]:
        """Hold ``plan`` deferred as ``result_id``, with a sample of ``preview_rows`` rows when that is not ``None``.

        ``schema`` spares resolving the plan's schema and ``row_count`` is carried on the ref, when the caller knows
        them.
        """
        held = _DeferredPlan(plan=plan, flow_id=flow_id, node_id=node_id)
        ref = ResultRef(
            backend_id=self.id,
            result_id=result_id,
            schema=schema if schema is not None else _flowfile_schema(plan.collect_schema()),
            row_count=row_count,
            state="deferred",
            location=result_id.encode(),
        )
        sample = None
        if preview_rows is not None:
            held.sample_file = self._run_sampler(held, result_id, preview_rows)
            sample = _read_sample(ref, held.sample_file, preview_rows)
        with self._lock:
            self._deferred[result_id] = held
        return ref, sample

    def _run_sampler(self, held: _DeferredPlan, task_id: str, n: int) -> str:
        """Run a worker sampler task over ``held``'s plan for ``n`` rows and return the file it wrote."""
        fetcher = self._run_tracked(
            task_id,
            lambda: ExternalSampler(
                lf=held.plan,
                node_id=held.node_id,
                flow_id=held.flow_id,
                file_ref=task_id,
                wait_on_completion=False,
                sample_size=n,
            ),
        )
        return _written_file(fetcher)

    def _run_tracked(self, task_id: str, start: Callable[[], BaseFetcher]) -> BaseFetcher:
        """``_run``, with the fetcher reachable through ``cancel`` under ``task_id`` while it waits."""

        def track(fetcher: BaseFetcher) -> None:
            with self._lock:
                self._in_flight[task_id] = fetcher

        try:
            return _run(start, on_started=track)
        finally:
            with self._lock:
                self._in_flight.pop(task_id, None)

    def _result_ref(self, result_id: str, path: str, row_count: int | None = None) -> ResultRef:
        # Reads the worker's file over the shared disk.
        return ResultRef(
            backend_id=self.id,
            result_id=result_id,
            schema=_flowfile_schema(pl.scan_ipc(path).collect_schema()),
            row_count=row_count if row_count is not None else _row_count(path),
            state="materialised",
            location=path.encode(),
        )

    def _read(self, start: Callable[[], BaseFetcher], job_id: str | None = None) -> OpResult:
        """Run a task that writes one result and return it as ``{"result": ref}``; tracked under ``job_id`` if given."""
        fetcher = _run(start) if job_id is None else self._run_tracked(job_id, start)
        return OpResult(refs={"result": self._result_ref(fetcher.file_ref, _written_file(fetcher))})

    def _database_read(self, payload: OpPayload) -> OpResult:
        return self._read(lambda: ExternalDatabaseFetcher(payload["settings"], wait_on_completion=False))

    def _kafka_read(self, payload: OpPayload) -> OpResult:
        return self._read(lambda: ExternalKafkaFetcher(payload["settings"], wait_on_completion=False))

    def _google_analytics_read(self, payload: OpPayload) -> OpResult:
        return self._read(lambda: ExternalGoogleAnalyticsFetcher(payload["settings"], wait_on_completion=False))

    def _rest_api_read(self, payload: OpPayload) -> OpResult:
        return self._read(lambda: ExternalRestApiFetcher(payload["settings"], wait_on_completion=False))

    def _file_read(self, payload: OpPayload) -> OpResult:
        received_table = payload["received_table"]
        return self._read(
            lambda: ExternalCreateFetcher(
                received_table=received_table,
                node_id=payload["node_id"],
                flow_id=payload["flow_id"],
                file_type=received_table.file_type,
                wait_on_completion=False,
            )
        )

    def _fuzzy_join(self, payload: OpPayload) -> OpResult:
        join = unpack(payload, FuzzyJoinPayload)
        manager = FuzzyMatchInputManager(deepcopy(join.settings.join_input))
        left, right = prepare_for_fuzzy_match(
            left=FlowDataEngine(join.left), right=FlowDataEngine(join.right), fuzzy_match_input_manager=manager
        )
        return self._read(
            lambda: ExternalFuzzyMatchFetcher(
                left_df=left,
                right_df=right,
                fuzzy_maps=manager.fuzzy_maps,
                flow_id=join.flow_id,
                node_id=join.node_id,
                file_ref=join.job_id,
                wait_on_completion=False,
            ),
            job_id=join.job_id,
        )

    def _random_split(self, payload: OpPayload) -> OpResult:
        split = unpack(payload, RandomSplitPayload)
        seed = split.settings.seed if split.settings.seed is not None else random.randint(0, 2**31 - 1)
        shuffled = (
            split.plan.with_columns(pl.int_range(0, pl.len()).shuffle(seed=seed).alias(_SPLIT_RANK))
            .sort(_SPLIT_RANK)
            .drop(_SPLIT_RANK)
        )
        stored = self._store(shuffled, split.job_id, split.flow_id, split.node_id)
        total = stored.row_count
        rows = self.plan_for(stored)
        groups = split.settings.splits
        refs: dict[str, ResultRef] = {}
        offset = 0
        for i, group in enumerate(groups):
            length = total - offset if i == len(groups) - 1 else int(round(total * group.percentage / 100.0))
            length = max(0, length)
            refs[group.name], _ = self._defer(
                rows.slice(offset, length),
                f"{split.job_id}{_OUTPUT_SEPARATOR}{group.name}",
                split.flow_id,
                split.node_id,
                None,
                schema=stored.schema,
                row_count=length,
            )
            offset += length
        return OpResult(refs=refs)

    def _ml_apply(self, payload: OpPayload) -> OpResult:
        apply = unpack(payload, MlApplyPayload)
        return self._read(
            lambda: MLApplyFetcher(
                lf=apply.plan,
                model_path=apply.model_path,
                output_column=apply.output_column,
                flow_id=apply.flow_id,
                node_id=apply.node_id,
                file_ref=apply.job_id,
                wait_on_completion=False,
            ),
            job_id=apply.job_id,
        )

    def _custom_node(self, payload: OpPayload) -> OpResult:
        job = unpack(payload, CustomNodePayload)
        if job.node_source is None:
            raise OperationUnsupported(
                f"The {self.id} backend runs a custom node from its node file; node {job.node_id} has none"
            )
        request = CustomNodeExecuteInput(
            task_id=job.job_id,
            node_source=job.node_source,
            class_name=job.class_name,
            settings_values=job.settings_values,
            secrets=resolve_secret_payload(job.custom_node, job.user_id),
            inputs=[plan.serialize() for plan in job.inputs],
            output_names=job.output_names,
            user_id=job.user_id,
            flowfile_flow_id=job.flow_id,
            flowfile_node_id=job.node_id,
        )
        fetcher = self._run_tracked(job.job_id, lambda: ExternalCustomNodeFetcher(request))
        outputs = fetcher.get_payload()["outputs"]
        refs: dict[str, ResultRef] = {}
        for i, name in enumerate(job.output_names):
            output = outputs.get(name)
            if output is None:
                raise ValueError(f"Worker did not return declared output '{name}'")
            result_id = f"{fetcher.file_ref}{_OUTPUT_SEPARATOR}{name}"
            refs[output_handle(i)] = self._result_ref(result_id, output["path"], output["row_count"])
        return OpResult(refs=refs)

    def _ml_train(self, payload: OpPayload) -> OpResult:
        train = unpack(payload, MlTrainPayload)
        fetcher = self._run_tracked(
            train.job_id,
            lambda: MLTrainFetcher(
                lf=train.plan,
                staging_path=train.staging_path,
                model_type=train.model_type,
                target_column=train.target_column,
                feature_columns=train.feature_columns,
                params=train.params,
                flow_id=train.flow_id,
                node_id=train.node_id,
                file_ref=train.job_id,
                wait_on_completion=False,
            ),
        )
        return OpResult(refs={}, metadata=_reported(fetcher))

    def _database_write(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, DatabaseWritePayload)
        reference = write.connection if write.settings.database_write_settings.connection_mode == "reference" else None
        settings = DatabaseExternalWriteSettings.create_from_from_node_database_writer(
            node_database_writer=write.settings,
            password=write.encrypted_password,
            table_name=write.table_name,
            lf=write.plan,
            database_reference_settings=reference,
        ).model_copy(update={"flowfile_flow_id": write.flow_id, "flowfile_node_id": write.node_id})
        self._run_tracked(write.job_id, lambda: ExternalDatabaseWriter(settings, wait_on_completion=False))
        return OpResult(refs={})

    def _cloud_write(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, CloudWritePayload)
        settings = CloudStorageWriteSettingsWorkerInterface(
            operation=base64.b64encode(write.plan.serialize()).decode(),
            write_settings=write.settings.get_write_setting_worker_interface(),
            connection=write.connection,
            flowfile_flow_id=write.flow_id,
            flowfile_node_id=write.node_id,
        )
        self._run_tracked(write.job_id, lambda: ExternalCloudWriter(settings, wait_on_completion=False))
        return OpResult(refs={})

    def _file_write(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, FileWritePayload)
        settings = write.settings
        self._run_tracked(
            write.job_id,
            lambda: ExternalOutputWriter(
                lf=write.plan,
                data_type=settings.file_type,
                path=settings.abs_file_path,
                write_mode=settings.write_mode,
                flow_id=write.flow_id,
                node_id=write.node_id,
                sheet_name=settings.sheet_name,
                delimiter=settings.delimiter,
                compression=settings.compression,
                wait_on_completion=False,
            ),
        )
        return OpResult(refs={})

    def _delta_write(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, DeltaWritePayload)
        kwargs = {
            "output_path": write.path,
            "mode": write.mode,
            "partition_by": write.partition_by,
            "enable_cdf": write.enable_cdf,
            "storage_payload": _storage_payload(write.connection),
        }
        return self._delta_task(write, "write_delta", kwargs)

    def _delta_merge(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, DeltaMergePayload)
        kwargs = {
            "output_path": write.path,
            "merge_mode": write.merge_mode,
            "merge_keys": write.merge_keys,
            "partition_by": write.partition_by,
            "enable_cdf": write.enable_cdf,
            "storage_payload": _storage_payload(write.connection),
        }
        return self._delta_task(write, "merge_delta", kwargs)

    def _delta_task(
        self, write: DeltaWritePayload | DeltaMergePayload, operation_type: str, kwargs: dict[str, Any]
    ) -> OpResult:
        """Run the worker's ``operation_type`` Delta operation over the payload's plan; its report is the metadata."""
        fetcher = self._run_tracked(
            write.job_id,
            lambda: ExternalDfFetcher(
                flow_id=write.flow_id,
                node_id=write.node_id,
                lf=write.plan,
                operation_type=operation_type,
                kwargs=kwargs,
                wait_on_completion=False,
            ),
        )
        return OpResult(refs={}, metadata=_reported(fetcher))

    def _delta_scd2(self, payload: OpPayload) -> OpResult:
        return _catalog_delta(payload, "scd2_delta")


def _run(start: Callable[[], BaseFetcher], on_started: Callable[[BaseFetcher], None] | None = None) -> BaseFetcher:
    """Start a worker task through ``start``, wait for it to end and return its fetcher.

    ``on_started`` receives the fetcher once the task is submitted, before the wait. A failed task raises a typed
    error: error code -1 goes through ``worker_failure``, whose ``error_kind`` tells a plan the worker could not
    load (``PlanRejected``) from a lost connection and a killed child; every other code is a ``TaskFailed`` with
    the fetcher's description, raised from the fetcher's ``RemoteExecutionError`` so the worker-side class
    stays recoverable from the chain.
    """
    try:
        fetcher = start()
    except requests.RequestException as e:
        raise ComputeUnavailable(f"Could not reach the worker: {e}") from e
    if on_started is not None:
        on_started(fetcher)
    try:
        fetcher.get_result()
    except Exception as e:
        description = fetcher.error_description or str(e)
        if fetcher.error_code == -1:
            raise worker_failure(fetcher.error_kind, description) from e
        raise TaskFailed(description) from e
    return fetcher


def _catalog_delta(payload: OpPayload, operation_type: str) -> OpResult:
    """The catalog writer's SCD2 write: a worker task running ``operation_type`` over the payload's plan."""
    fetcher = _run(
        lambda: ExternalDfFetcher(
            flow_id=payload["flow_id"],
            node_id=payload["node_id"],
            lf=payload["lf"],
            operation_type=operation_type,
            kwargs=payload["kwargs"],
            wait_on_completion=False,
        )
    )
    return OpResult(refs={}, metadata=_reported(fetcher))


def _storage_payload(connection: FullCloudStorageConnectionWorkerInterface | None) -> dict[str, Any] | None:
    """The ``storage_payload`` the worker's Delta operations take; ``None`` for a table on the worker's disk.

    It carries the connection with its secrets still encrypted; the worker decrypts them.
    """
    return None if connection is None else {"connection": connection.model_dump()}


def _reported(fetcher: BaseFetcher) -> dict[str, Any]:
    """The dict a task that writes reports as its result; empty when the worker sent none."""
    result = fetcher.result
    return result if isinstance(result, dict) else {}


def _written_file(fetcher: BaseFetcher) -> str:
    status = fetcher.status
    if status is None or not status.file_ref:
        raise TaskFailed(f"The worker did not say where task {fetcher.file_ref!r} wrote its result")
    return status.file_ref


def _row_count(path: str) -> int:
    return pl.scan_ipc(path).select(pl.len()).collect().item()


def _flowfile_schema(columns: pl.Schema) -> Schema:
    return convert_stats_to_column_info(
        [{"column_name": name, "pl_datatype": dtype, "col_index": i} for i, (name, dtype) in enumerate(columns.items())]
    )


def _read_sample(ref: ResultRef, path: str, n: int) -> bytes:
    """The first ``n`` rows of the Arrow IPC file at ``path`` as an Arrow IPC stream."""
    with _held(ref):
        table = get_read_top_n(path, n)()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _task_id(ref: ResultRef) -> str:
    return ref.result_id.partition(_OUTPUT_SEPARATOR)[0]


@contextmanager
def _held(ref: ResultRef) -> Iterator[None]:
    """Turn a result file that is gone into ``ResultMissing``."""
    try:
        yield
    except FileNotFoundError as e:
        raise ResultMissing(f"The worker no longer holds result {ref.result_id!r}") from e
