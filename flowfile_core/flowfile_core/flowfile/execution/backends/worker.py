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
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
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
from flowfile_core.flowfile.execution.payloads import (
    CloudWritePayload,
    DatabaseReadPayload,
    DatabaseWritePayload,
    DeltaMergePayload,
    DeltaScd2Payload,
    DeltaWritePayload,
    FileReadPayload,
    FileWritePayload,
    GoogleAnalyticsReadPayload,
    KafkaReadPayload,
    OperationPayload,
    RestApiReadPayload,
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
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import convert_stats_to_column_info
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
    fetch_kafka_offsets,
    get_status,
    get_worker_capabilities,
    results_exists,
)
from flowfile_core.flowfile.sources.external_sources.rest_api_source import build_rest_api_worker_settings
from flowfile_core.flowfile.sources.external_sources.sql_source.models import (
    DatabaseExternalReadSettings,
    DatabaseExternalWriteSettings,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import BaseSqlSource
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageWriteSettingsWorkerInterface,
    FullCloudStorageConnectionWorkerInterface,
)
from flowfile_core.utils.arrow_reader import get_read_top_n
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities
from shared.google_analytics.models import GoogleAnalyticsFilter, GoogleAnalyticsOrderBy, GoogleAnalyticsReadSettings
from shared.kafka.models import KafkaReadSettings
from shared.path_utils import DirectoryScanUnsupportedError, refuse_cloud_path
from shared.rest_api.models import RestApiReadSettings

WORKER_BACKEND_ID = "worker"
_OUTPUT_SEPARATOR = "#"
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
    task's fetcher; while a write ``run_op`` waits, so does the handle with its payload's ``job_id``.

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

        ``payload`` holds what that fetcher takes, under these keys (``flow_id`` and ``node_id`` name the flow and
        node the worker logs the task under, ``task_id`` the worker task id, a new one when absent):

        - ``database_read``, ``kafka_read``, ``google_analytics_read``, ``rest_api_read``, ``file_read``: ``payload``,
          the kind's dataclass from ``execution/payloads.py``, turned here into the worker read settings its fetcher
          takes (credentials still encrypted, filed under the payload's ``flow_id`` and ``node_id``). The task runs as
          the payload's ``job_id``, so ``cancel(JobHandle("worker", job_id))`` reaches it while it runs. A
          ``kafka_read`` reports the offsets to commit as ``metadata["offsets"]`` when it consumed messages.
        - ``fuzzy_join``: ``left`` and ``right`` (LazyFrames), ``fuzzy_maps``, ``flow_id``, ``node_id``, ``task_id``.
        - ``custom_node``: ``request`` (``CustomNodeExecuteInput``).
        - ``ml_train``: ``lf``, ``staging_path``, ``model_type``, ``target_column``, ``feature_columns``, ``params``,
          ``flow_id``, ``node_id``, ``task_id``.
        - ``ml_apply``: ``lf``, ``model_path``, ``output_column``, ``flow_id``, ``node_id``, ``task_id``.

        The writes ``file_write``, ``database_write``, ``cloud_write``, ``delta_write``, ``delta_merge`` and
        ``delta_scd2`` take their ``payloads`` dataclass under ``"payload"`` instead; its plan is serialised here,
        its credentials sent still encrypted, and the task runs as the payload's ``job_id``, so
        ``cancel(JobHandle("worker", job_id))`` reaches it while it runs.

        A read's refs are ``{"result": ref}``, a custom node's one reference per output it wrote, and a write has
        none. ``ml_train`` and the Delta writes return the worker's result dict as ``metadata``: the trained
        model's ``sha256``, ``size_bytes`` and ``model_type`` (the model itself is the file at ``staging_path``,
        not a table, so there is no ref), or the written table's schema, counts, size and version.

        Raises:
            OperationUnsupported: ``random_split`` and ``viz_session``, which have no worker task.
            PlanRejected: The worker could not load the operation's plan.
            TaskFailed: The operation failed on the worker.
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
        self, plan: pl.LazyFrame, result_id: str, flow_id: int, node_id: int | str, preview_rows: int | None
    ) -> tuple[ResultRef, bytes | None]:
        """Hold ``plan`` deferred as ``result_id``, with a sample of ``preview_rows`` rows when that is not ``None``."""
        held = _DeferredPlan(plan=plan, flow_id=flow_id, node_id=node_id)
        ref = ResultRef(
            backend_id=self.id,
            result_id=result_id,
            schema=_flowfile_schema(plan.collect_schema()),
            row_count=None,
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

    def _read(self, start: Callable[[], BaseFetcher]) -> OpResult:
        fetcher = _run(start)
        return OpResult(refs={"result": self._result_ref(fetcher.file_ref, _written_file(fetcher))})

    def _read_tracked(self, job_id: str, start: Callable[[], BaseFetcher]) -> tuple[BaseFetcher, ResultRef]:
        """Run a read task as job ``job_id``, which ``cancel`` reaches while it waits; its fetcher and stored result."""
        fetcher = self._run_tracked(job_id, start)
        return fetcher, self._result_ref(fetcher.file_ref, _written_file(fetcher))

    def _database_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, DatabaseReadPayload)
        settings = _database_read_settings(read)
        _, ref = self._read_tracked(read.job_id, lambda: ExternalDatabaseFetcher(settings, wait_on_completion=False))
        return OpResult(refs={"result": ref})

    def _kafka_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, KafkaReadPayload)
        settings = _kafka_read_settings(read)
        fetcher, ref = self._read_tracked(read.job_id, lambda: ExternalKafkaFetcher(settings, wait_on_completion=False))
        consumed = fetch_kafka_offsets(fetcher.file_ref)
        if consumed and consumed.get("messages_consumed", 0) > 0:
            return OpResult(refs={"result": ref}, metadata={"offsets": consumed["new_offsets"]})
        return OpResult(refs={"result": ref})

    def _google_analytics_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, GoogleAnalyticsReadPayload)
        settings = _google_analytics_read_settings(read)
        _, ref = self._read_tracked(
            read.job_id, lambda: ExternalGoogleAnalyticsFetcher(settings, wait_on_completion=False)
        )
        return OpResult(refs={"result": ref})

    def _rest_api_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, RestApiReadPayload)
        settings = _rest_api_read_settings(read)
        _, ref = self._read_tracked(read.job_id, lambda: ExternalRestApiFetcher(settings, wait_on_completion=False))
        return OpResult(refs={"result": ref})

    def _file_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, FileReadPayload)
        received_table = read.settings.received_file
        refuse_cloud_path(received_table.path, received_table.file_type)
        if received_table.scan_mode == "directory":
            raise DirectoryScanUnsupportedError("Directory scan mode cannot be executed by the worker file reader.")
        _, ref = self._read_tracked(
            read.job_id,
            lambda: ExternalCreateFetcher(
                received_table=received_table,
                node_id=read.node_id,
                flow_id=read.flow_id,
                file_type=received_table.file_type,
                wait_on_completion=False,
            ),
        )
        return OpResult(refs={"result": ref})

    def _fuzzy_join(self, payload: OpPayload) -> OpResult:
        return self._read(
            lambda: ExternalFuzzyMatchFetcher(
                left_df=payload["left"],
                right_df=payload["right"],
                fuzzy_maps=payload["fuzzy_maps"],
                flow_id=payload["flow_id"],
                node_id=payload["node_id"],
                file_ref=payload.get("task_id"),
                wait_on_completion=False,
            )
        )

    def _ml_apply(self, payload: OpPayload) -> OpResult:
        return self._read(
            lambda: MLApplyFetcher(
                lf=payload["lf"],
                model_path=payload["model_path"],
                output_column=payload["output_column"],
                flow_id=payload["flow_id"],
                node_id=payload["node_id"],
                file_ref=payload.get("task_id"),
                wait_on_completion=False,
            )
        )

    def _custom_node(self, payload: OpPayload) -> OpResult:
        fetcher = _run(lambda: ExternalCustomNodeFetcher(payload["request"]))
        outputs = fetcher.get_payload()["outputs"]
        return OpResult(
            refs={
                name: self._result_ref(f"{fetcher.file_ref}{_OUTPUT_SEPARATOR}{name}", output["path"])
                for name, output in outputs.items()
            }
        )

    def _ml_train(self, payload: OpPayload) -> OpResult:
        fetcher = _run(
            lambda: MLTrainFetcher(
                lf=payload["lf"],
                staging_path=payload["staging_path"],
                model_type=payload["model_type"],
                target_column=payload["target_column"],
                feature_columns=payload["feature_columns"],
                params=payload["params"],
                flow_id=payload["flow_id"],
                node_id=payload["node_id"],
                file_ref=payload.get("task_id"),
                wait_on_completion=False,
            )
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

    def _delta_scd2(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, DeltaScd2Payload)
        kwargs = {
            "output_path": write.path,
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
            "storage_payload": _storage_payload(write.connection),
        }
        return self._delta_task(write, "scd2_delta", kwargs)

    def _delta_task(
        self,
        write: DeltaWritePayload | DeltaMergePayload | DeltaScd2Payload,
        operation_type: str,
        kwargs: dict[str, Any],
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


def _filed_under(read: OperationPayload) -> dict[str, Any]:
    """The worker read settings' fields that name the flow and node the task is filed under."""
    return {"flowfile_flow_id": read.flow_id, "flowfile_node_id": read.node_id}


def _database_read_settings(read: DatabaseReadPayload) -> DatabaseExternalReadSettings:
    """The worker's database read: the query the node's settings make, over the connection with its password as stored.

    A stored connection travels without its own ``password`` field; the payload's encrypted password replaces it.
    """
    node_settings = read.settings
    database_settings = node_settings.database_settings
    query = BaseSqlSource(
        query=None if database_settings.query_mode == "table" else database_settings.query,
        table_name=database_settings.table_name,
        schema_name=database_settings.schema_name,
        fields=node_settings.fields,
    ).query
    settings = DatabaseExternalReadSettings.create_from_from_node_database_reader(
        node_database_reader=node_settings,
        password=read.encrypted_password,
        query=query,
        database_reference_settings=read.connection if database_settings.connection_mode == "reference" else None,
    )
    return settings.model_copy(update=_filed_under(read))


def _kafka_read_settings(read: KafkaReadPayload) -> KafkaReadSettings:
    return read.settings.model_copy(update=_filed_under(read))


def _google_analytics_read_settings(read: GoogleAnalyticsReadPayload) -> GoogleAnalyticsReadSettings:
    """The worker's report read, signed in with the payload's credential as stored."""
    report = read.settings.google_analytics_settings
    common = dict(
        property_id=report.property_id,
        start_date=report.start_date,
        end_date=report.end_date,
        metrics=report.metrics,
        dimensions=report.dimensions,
        limit=report.limit,
        filters=[
            GoogleAnalyticsFilter(field=f.field, operator=f.operator, value=f.value, case_sensitive=f.case_sensitive)
            for f in report.filters
        ],
        order_bys=[GoogleAnalyticsOrderBy(field=ob.field, descending=ob.descending) for ob in report.order_bys],
        **_filed_under(read),
    )
    if read.auth_method == "service_account":
        return GoogleAnalyticsReadSettings(
            auth_method="service_account", service_account_key_encrypted=read.encrypted_credential, **common
        )
    return GoogleAnalyticsReadSettings(
        auth_method="oauth",
        refresh_token_encrypted=read.encrypted_credential,
        oauth_client_id=read.oauth_client_id,
        oauth_client_secret_encrypted=read.oauth_client_secret_encrypted,
        **common,
    )


def _rest_api_read_settings(read: RestApiReadPayload) -> RestApiReadSettings:
    return build_rest_api_worker_settings(read.settings, read.encrypted_secret).model_copy(update=_filed_under(read))
