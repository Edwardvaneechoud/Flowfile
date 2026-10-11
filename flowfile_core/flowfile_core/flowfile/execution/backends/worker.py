"""The worker backend: core's client of ``flowfile_worker`` over the worker's task routes.

It runs everything through the transport in ``flow_data_engine/subprocess_operations``: a plan or an operation
becomes a worker task started by the fetcher core uses for it, the status, clear and cancel routes follow a task,
and a result stays in the Arrow IPC file the task wrote. A ``ResultRef``'s ``location`` is that file's path. Core
and the worker share a disk, so schemas, row counts and samples are read from the file here, not sent over the wire.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
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
from flowfile_core.flowfile.execution.protocol import (
    CredentialRef,
    JobHandle,
    JobStatus,
    OperationKind,
    OpPayload,
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
    MLApplyFetcher,
    MLTrainFetcher,
    cancel_task,
    clear_task_from_worker,
    get_status,
    get_worker_capabilities,
    results_exists,
)
from flowfile_core.utils.arrow_reader import get_read_top_n
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities

METADATA = "metadata"
"""The ``run_op`` output name under which ``ml_train`` and the Delta writes return the worker's metadata dict."""

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


class WorkerBackend:
    """``ComputeBackend`` for ``flowfile_worker``, built on the worker's existing routes.

    Every result is ``"materialised"``: the worker task that produced it wrote it to an Arrow IPC file, which
    the worker keeps until the task is cleared. ``result_id`` is the worker task id, ``location`` the file's path
    as UTF-8. A custom node's outputs add ``#<output name>`` to the task id and share the task, so releasing
    one releases them all.

    Nothing here runs the worker handshake: every worker call goes through the transport's worker session or
    its WebSocket submit, which run it before they reach the worker, so every method that calls the worker is
    gated by it already and can raise its ``VersionMismatch`` or ``ComputeUnavailable``.
    """

    id = "worker"
    protocol_version = PROTOCOL_VERSION

    def __init__(self) -> None:
        self._operations: dict[OperationKind, Callable[[OpPayload], dict[str, Any]]] = {
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

    def submit(self, plan: bytes, inputs: list[ResultRef], hints: SubmitHints) -> SubmitResult:
        """Run ``plan`` as a worker store task, wait for it to end and return the file it wrote.

        The plan bytes go to the worker as they are, never deserialised here. ``inputs`` must be this backend's
        results; the plan already scans their files, so they are only checked. The plan names no flow or node,
        so the worker files the result under flow ``-1``.

        Raises:
            ForeignRef: An input another backend minted.
            PlanRejected: The worker could not load the plan.
            TaskFailed: The plan failed on the worker.
        """
        for ref in inputs:
            self._location(ref)
        fetcher = _run(
            lambda: ExternalDfFetcher(flow_id=_NO_FLOW, node_id=_NO_FLOW, plan_bytes=plan, wait_on_completion=False)
        )
        ref = self._result_ref(fetcher.file_ref, _written_file(fetcher))
        sample = None if hints.preview_rows is None else self.sample(ref, hints.preview_rows)
        return SubmitResult(ref=ref, sample=sample, handle=JobHandle(self.id, fetcher.file_ref))

    def run_op(self, op: OperationKind, payload: OpPayload) -> dict[str, ResultRef]:
        """Run ``op`` as a worker task through the fetcher core uses for it and wait for it to end.

        ``payload`` holds what that fetcher takes, under these keys (``flow_id`` and ``node_id`` name the flow and
        node the worker logs the task under, ``task_id`` the worker task id, a new one when absent):

        - ``database_read``: ``settings`` (``DatabaseExternalReadSettings``).
        - ``kafka_read``, ``google_analytics_read``, ``rest_api_read``: ``settings``, the worker read settings.
        - ``file_read``: ``received_table`` (``ReceivedTable`` with its absolute path set; its ``file_type`` picks
          the worker's reader), ``flow_id``, ``node_id``.
        - ``fuzzy_join``: ``left`` and ``right`` (LazyFrames), ``fuzzy_maps``, ``flow_id``, ``node_id``, ``task_id``.
        - ``custom_node``: ``request`` (``CustomNodeExecuteInput``).
        - ``ml_train``: ``lf``, ``staging_path``, ``model_type``, ``target_column``, ``feature_columns``, ``params``,
          ``flow_id``, ``node_id``, ``task_id``.
        - ``ml_apply``: ``lf``, ``model_path``, ``output_column``, ``flow_id``, ``node_id``, ``task_id``.
        - ``database_write``: ``settings`` (``DatabaseExternalWriteSettings``).
        - ``cloud_write``: ``settings`` (``CloudStorageWriteSettingsWorkerInterface``).
        - ``file_write``: ``lf``, ``data_type``, ``path``, ``write_mode``, ``flow_id``, ``node_id``, and optionally
          ``sheet_name``, ``delimiter``, ``compression``.
        - ``delta_write``, ``delta_merge``, ``delta_scd2``: ``lf``, ``kwargs`` (the keyword arguments of the
          worker's ``write_delta``, ``merge_delta`` or ``scd2_delta`` operation), ``flow_id``, ``node_id``.

        A read returns ``{"result": ref}``, a custom node one reference per output it wrote, a write ``{}``.
        ``ml_train`` and the Delta writes return the worker's result dict under ``METADATA`` instead of a
        reference: the trained model's hash and size, or the written table's schema, counts and version, are
        what the caller records, and a ``ResultRef`` has no field to carry them.

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
        """The first ``n`` rows of ``ref``'s file, or all of them when it has fewer, as an Arrow IPC stream."""
        path = self._location(ref)
        with _held(ref):
            table = get_read_top_n(path, n)()
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return sink.getvalue().to_pybytes()

    def count(self, ref: ResultRef) -> int:
        """The number of rows in ``ref``'s file."""
        path = self._location(ref)
        with _held(ref):
            return _row_count(path)

    def exists(self, ref: ResultRef) -> bool:
        """Whether the worker still holds the task that produced ``ref`` (``results_exists``)."""
        self._location(ref)
        return results_exists(_task_id(ref))

    def release(self, ref: ResultRef) -> None:
        """Clear the task that produced ``ref`` from the worker, which deletes the file the task registered."""
        self._location(ref)
        clear_task_from_worker(_task_id(ref))

    def plan_for(self, ref: ResultRef) -> pl.LazyFrame:
        """A scan of ``ref``'s file, valid only in plans this backend runs.

        Raises:
            ForeignRef: Another backend minted ``ref``.
            ResultMissing: The worker no longer holds the task that produced it.
        """
        path = self._location(ref)
        if not results_exists(_task_id(ref)):
            raise ResultMissing(f"The worker no longer holds result {ref.result_id!r}")
        return pl.scan_ipc(path)

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
        """Ask the worker to cancel the task ``handle`` names; a task that already ended is left as it is."""
        self._own(handle)
        cancel_task(handle.job_id)

    def _location(self, ref: ResultRef) -> str:
        if ref.backend_id != self.id:
            raise ForeignRef(f"Result {ref.result_id!r} belongs to backend {ref.backend_id!r}, not {self.id!r}")
        return ref.location.decode()

    def _own(self, handle: JobHandle) -> None:
        if handle.backend_id != self.id:
            raise ForeignRef(f"Job {handle.job_id!r} belongs to backend {handle.backend_id!r}, not {self.id!r}")

    def _result_ref(self, result_id: str, path: str) -> ResultRef:
        # Reads the worker's file over the shared disk.
        columns = pl.scan_ipc(path).collect_schema()
        schema = convert_stats_to_column_info(
            [
                {"column_name": name, "pl_datatype": dtype, "col_index": i}
                for i, (name, dtype) in enumerate(columns.items())
            ]
        )
        return ResultRef(
            backend_id=self.id,
            result_id=result_id,
            schema=schema,
            row_count=_row_count(path),
            state="materialised",
            location=path.encode(),
        )

    def _read(self, start: Callable[[], BaseFetcher]) -> dict[str, ResultRef]:
        fetcher = _run(start)
        return {"result": self._result_ref(fetcher.file_ref, _written_file(fetcher))}

    def _database_read(self, payload: OpPayload) -> dict[str, ResultRef]:
        return self._read(lambda: ExternalDatabaseFetcher(payload["settings"], wait_on_completion=False))

    def _kafka_read(self, payload: OpPayload) -> dict[str, ResultRef]:
        return self._read(lambda: ExternalKafkaFetcher(payload["settings"], wait_on_completion=False))

    def _google_analytics_read(self, payload: OpPayload) -> dict[str, ResultRef]:
        return self._read(lambda: ExternalGoogleAnalyticsFetcher(payload["settings"], wait_on_completion=False))

    def _rest_api_read(self, payload: OpPayload) -> dict[str, ResultRef]:
        return self._read(lambda: ExternalRestApiFetcher(payload["settings"], wait_on_completion=False))

    def _file_read(self, payload: OpPayload) -> dict[str, ResultRef]:
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

    def _fuzzy_join(self, payload: OpPayload) -> dict[str, ResultRef]:
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

    def _ml_apply(self, payload: OpPayload) -> dict[str, ResultRef]:
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

    def _custom_node(self, payload: OpPayload) -> dict[str, ResultRef]:
        fetcher = _run(lambda: ExternalCustomNodeFetcher(payload["request"]))
        outputs = fetcher.get_payload()["outputs"]
        return {
            name: self._result_ref(f"{fetcher.file_ref}{_OUTPUT_SEPARATOR}{name}", output["path"])
            for name, output in outputs.items()
        }

    def _ml_train(self, payload: OpPayload) -> dict[str, Any]:
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
        return {METADATA: fetcher.result}

    def _database_write(self, payload: OpPayload) -> dict[str, ResultRef]:
        _run(lambda: ExternalDatabaseWriter(payload["settings"], wait_on_completion=False))
        return {}

    def _cloud_write(self, payload: OpPayload) -> dict[str, ResultRef]:
        _run(lambda: ExternalCloudWriter(payload["settings"], wait_on_completion=False))
        return {}

    def _file_write(self, payload: OpPayload) -> dict[str, ResultRef]:
        _run(
            lambda: ExternalOutputWriter(
                lf=payload["lf"],
                data_type=payload["data_type"],
                path=payload["path"],
                write_mode=payload["write_mode"],
                flow_id=payload["flow_id"],
                node_id=payload["node_id"],
                sheet_name=payload.get("sheet_name"),
                delimiter=payload.get("delimiter"),
                compression=payload.get("compression"),
                wait_on_completion=False,
            )
        )
        return {}

    def _delta_write(self, payload: OpPayload) -> dict[str, Any]:
        return _catalog_delta(payload, "write_delta")

    def _delta_merge(self, payload: OpPayload) -> dict[str, Any]:
        return _catalog_delta(payload, "merge_delta")

    def _delta_scd2(self, payload: OpPayload) -> dict[str, Any]:
        return _catalog_delta(payload, "scd2_delta")


def _run(start: Callable[[], BaseFetcher]) -> BaseFetcher:
    """Start a worker task through ``start``, wait for it to end and return its fetcher.

    A failed task raises what ``FlowNode._do_execute_remote`` raises for it: error code -1 goes through
    ``worker_failure``, whose ``error_kind`` tells a plan the worker could not load (``PlanRejected``) from a
    lost connection and a killed child; every other code is a ``TaskFailed`` with the fetcher's description.
    """
    try:
        fetcher = start()
    except requests.RequestException as e:
        raise ComputeUnavailable(f"Could not reach the worker: {e}") from e
    try:
        fetcher.get_result()
    except Exception as e:
        description = fetcher.error_description or str(e)
        if fetcher.error_code == -1:
            raise worker_failure(fetcher.error_kind, description) from e
        raise TaskFailed(description) from e
    return fetcher


def _catalog_delta(payload: OpPayload, operation_type: str) -> dict[str, Any]:
    """The catalog writer's Delta write: a worker task running ``operation_type`` over the payload's plan."""
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
    return {METADATA: fetcher.result}


def _written_file(fetcher: BaseFetcher) -> str:
    status = fetcher.status
    if status is None or not status.file_ref:
        raise TaskFailed(f"The worker did not say where task {fetcher.file_ref!r} wrote its result")
    return status.file_ref


def _row_count(path: str) -> int:
    return pl.scan_ipc(path).select(pl.len()).collect().item()


def _task_id(ref: ResultRef) -> str:
    return ref.result_id.partition(_OUTPUT_SEPARATOR)[0]


@contextmanager
def _held(ref: ResultRef) -> Iterator[None]:
    """Turn a result file that is gone into ``ResultMissing``."""
    try:
        yield
    except FileNotFoundError as e:
        raise ResultMissing(f"The worker no longer holds result {ref.result_id!r}") from e
