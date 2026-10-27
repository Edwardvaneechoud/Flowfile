"""The worker backend: core's client of ``flowfile_worker``.

It speaks one of two route families, chosen by ``settings.FLOWFILE_COMPUTE_PROTOCOL`` (read on each call):

- Task routes (flag off): each plan or operation is a worker task started by the fetcher core uses for it
  (``flow_data_engine/subprocess_operations``). A stored result stays in the Arrow IPC file the task wrote, and core
  reads its schema, row count and samples from that file over the disk it shares with the worker.
- ``/v1`` routes (flag on): the worker holds every result, stored or deferred, and answers its schema, row count and
  samples over the wire. ``submit`` goes over ``WS /v1/ws/submit``, else ``POST /v1/submit``, and most operations
  over ``POST /v1/ops/{kind}``.

``polars_code`` and ``sql_query`` have no task route and use ``/v1`` in either flag state. The Delta writes and
``viz_session`` always use the task routes. A result is read and exported over the routes that minted it, whatever
the flag says when it is read. See docs/for-developers/compute-protocol.md.
"""

from __future__ import annotations

import base64
import io
import json
import os
import random
import re
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass
from pathlib import Path
from time import monotonic
from typing import Any, Literal

import polars as pl
import pyarrow as pa
import requests
from pl_fuzzy_frame_match.models import FuzzyMapping
from websockets.exceptions import ConnectionClosed, InvalidStatus
from websockets.sync.client import ClientConnection, connect

from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.configs import logger, settings
from flowfile_core.flowfile.execution.errors import (
    ComputeError,
    ComputeUnavailable,
    ForeignRef,
    OperationUnsupported,
    PlanRejected,
    ResultMissing,
    TaskFailed,
    VersionMismatch,
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
    OperationPayload,
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
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import convert_stats_to_column_info
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import safe_eval_pl_type
from flowfile_core.flowfile.flow_data_engine.fuzzy_matching.prepare_for_fuzzy_match import prepare_for_fuzzy_match
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.handshake import (
    Verdict,
    cached_verdict,
    capabilities_from_answer,
    ensure_handshake,
    owns,
    reset_handshake,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import (
    CustomNodeExecuteInput,
    FuzzyJoinInput,
    PolarsOperation,
    Status,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.streaming import (
    _WS_INACTIVITY_TIMEOUT,
    _WS_RECV_POLL_SECONDS,
    _get_ws_url,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.subprocess_operations import (
    _POLL_DEADLINE_SECONDS,
    _WORKER_TIMEOUT,
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
    _worker_session,
    _WorkerAuth,
    cancel_task,
    clear_task_from_worker,
    fetch_kafka_offsets,
    get_status,
    results_exists,
    ssl_context_for,
    trigger_visualize_fields,
    trigger_visualize_query,
    trust_ca,
)
from flowfile_core.flowfile.flow_node.multi_output import output_handle
from flowfile_core.flowfile.sources.external_sources.rest_api_source import build_rest_api_worker_settings
from flowfile_core.flowfile.sources.external_sources.sql_source.models import (
    DatabaseExternalReadSettings,
    DatabaseExternalWriteSettings,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.sql_source import BaseSqlSource
from flowfile_core.flowfile.user_defined.dispatch import resolve_secret_payload
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageWriteSettingsWorkerInterface,
    FullCloudStorageConnectionWorkerInterface,
)
from flowfile_core.schemas.input_schema import ReceivedTable
from flowfile_core.schemas.transform_schema import FuzzyMatchInputManager
from flowfile_core.utils.arrow_reader import get_read_top_n
from shared import compute_protocol as wire
from shared.cloud_credential_provider import credential_ref_kwargs
from shared.compute_protocol import PROTOCOL_VERSION, Capabilities
from shared.google_analytics.models import GoogleAnalyticsFilter, GoogleAnalyticsOrderBy, GoogleAnalyticsReadSettings
from shared.kafka.models import KafkaReadSettings
from shared.path_utils import DirectoryScanUnsupportedError, refuse_cloud_path
from shared.rest_api.models import RestApiReadSettings
from shared.storage_config import storage

WORKER_BACKEND_ID = "worker"
TOKEN_RULE = (
    "a worker token is sent as an HTTP header value, so it cannot hold a CR, a LF, any other ASCII control character "
    "but a tab, or a character beyond Latin-1"
)
_HEADER_VALUE = re.compile(r"[\t\x20-\x7e\x80-\xff]*")
_OUTPUT_SEPARATOR = "#"
_SPLIT_RANK = "__split_rank__"
_NO_FLOW = -1
# A proxy may hold a refused upgrade's connection open; websockets would wait 10 s for it to close
_SOCKET_CLOSE_TIMEOUT = 1.0
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


class _WorkerReferences:
    """Minting and checking of a worker's ``/v1`` references and job handles.

    Shared by ``WorkerBackend`` and ``EmbeddedWorkerBackend``. Each subclass sets ``id``, which they carry and are
    checked against.
    """

    id: str

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


class WorkerBackend(_WorkerReferences):
    """``ComputeBackend`` for ``flowfile_worker``; every call goes to the worker at ``url``.

    Constructor arguments:

    - ``backend_id``: the id it answers to, ``"worker"`` built in or ``worker:<name>`` configured. Its refs and
      handles carry it, and it refuses those of any other id.
    - ``token``: the worker's own token, sent as ``X-Flowfile-Internal`` by ``capabilities``, every ``/v1`` request
      and the ``/v1`` submit socket. ``None`` signs with the process's internal token, as the task routes always do.
      A token that is no header value (``is_header_value``) fails each call as a ``TaskFailed`` naming
      ``TOKEN_RULE`` before any client is handed it, so no message or log line carries it.
    - ``ca_cert``: the PEM of the CA to verify an ``https://`` worker against (``trust_ca``). ``None`` uses the public
      CAs and drops a CA an earlier backend on ``url`` was given.
    - ``shares_core_disk``: the worker sees core's files, so ``capabilities`` adds ``file`` to its schemes.

    Materialisation policy: ``submit`` stores a plan when ``hints.must_materialise`` is set, or when it is ``wide``
    and either a preview is asked for (``preview_rows`` not ``None``) or more than one consumer reads it (``fanout``),
    so the preview and the consumers share one computation. Every other plan stays deferred: with no preview
    (Performance mode) nothing reads a wide plan with one consumer before the writer does.

    The handshake gates every worker call but ``capabilities``, which the check itself reads, so any other call can
    raise ``VersionMismatch`` or ``ComputeUnavailable``. Making a backend registers it with that gate for ``url``
    (``handshake.owns``). An absent plugin refuses no call here; placement refuses the nodes that need it. Wire
    details: docs/for-developers/compute-protocol.md.
    """

    id = WORKER_BACKEND_ID
    protocol_version = PROTOCOL_VERSION

    def __init__(
        self,
        url: str,
        *,
        backend_id: str = WORKER_BACKEND_ID,
        token: str | None = None,
        ca_cert: str | None = None,
        shares_core_disk: bool = False,
    ) -> None:
        self.url = url
        self.id = backend_id
        self.token = token
        self.shares_core_disk = shares_core_disk
        trust_ca(url, ca_cert)
        owns(self)
        self._auth = _TokenAuth(token, backend_id) if token is not None else None
        self._lock = threading.Lock()
        self._socket_refused_under: Verdict | None = None
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
            OperationKind.polars_code: self._polars_code,
            OperationKind.sql_query: self._sql_query,
            OperationKind.database_write: self._database_write,
            OperationKind.cloud_write: self._cloud_write,
            OperationKind.file_write: self._file_write,
            OperationKind.delta_write: self._delta_write,
            OperationKind.delta_merge: self._delta_merge,
            OperationKind.delta_scd2: self._delta_scd2,
            OperationKind.viz_session: self._viz_session,
        }

    def capabilities(self) -> Capabilities:
        """The worker's answer to ``GET /capabilities`` (``GET /v1/capabilities`` with the protocol flag on).

        One request around the handshake gate (``gated=False``), signed with this backend's token and carrying its
        ``X-Protocol-Version``. Its ``operations`` are the kinds ``run_op`` sends the worker. With
        ``shares_core_disk``, ``file`` is added to both scheme sets: core knows that worker sees its files, which the
        worker cannot know by itself.

        Raises:
            ComputeUnavailable: The worker could not be reached, or the URL answers as no Flowfile worker
                (``capabilities_from_answer``).
            VersionMismatch: A 404 (the worker predates core), a refused protocol version (426), or an answer of
                another release.
            TaskFailed: This backend's token is no header value; nothing was sent.
            ComputeError: Any other failed answer (``_v1_failure``); a refused token is a ``TaskFailed`` naming 401.
        """
        route = f"{'/v1' if settings.FLOWFILE_COMPUTE_PROTOCOL else ''}/capabilities"
        try:
            response = _worker_session.get(
                route,
                worker_url=self.url,
                gated=False,
                auth=self._auth or _WorkerAuth(),
                headers={wire.PROTOCOL_VERSION_HEADER: str(self.protocol_version)},
                timeout=_WORKER_TIMEOUT,
            )
        except requests.RequestException as e:
            raise ComputeUnavailable(f"Could not reach the worker at {self.url}: {e}", backend_id=self.id) from e
        if response.status_code == 404:
            raise VersionMismatch(
                f"The worker at {self.url} does not report its capabilities (GET {route} answered 404), "
                "so it is an older release than core"
            )
        if not response.ok:
            raise _v1_failure(response.status_code, response.content, response.reason)
        declared = capabilities_from_answer(self.url, response, backend_id=self.id)
        if not self.shares_core_disk:
            return declared
        return declared.model_copy(
            update={
                "read_schemes": frozenset(declared.read_schemes) | {"file"},
                "write_schemes": frozenset(declared.write_schemes) | {"file"},
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
        """Store ``plan`` on the worker or hold it deferred, by the policy in the class docstring.

        ``inputs`` must be this backend's results; the plan already reads them, so they are only checked. The worker
        files the task as ``result_id`` under ``flow_id`` and ``node_id``, which is where its log lines go. Over the
        task routes a stored plan is written to an Arrow IPC file by a worker store task, and a deferred one is held in
        this process as the object it is.

        With the protocol flag on, the plan and the ids of its ``/v1`` inputs go over ``WS /v1/ws/submit``, else
        ``POST /v1/submit`` (``_v1_socket_submit``). Once the plan is sent over the socket nothing is resent.

        Raises:
            ComputeUnavailable: Over ``/v1``, the worker could not be reached, or its socket closed after the plan was
                sent.
            ForeignRef: An input another backend minted.
            OperationUnsupported: An input is a blob; nothing was sent.
            PlanRejected: The worker could not load the plan.
            ResultMissing: Over ``/v1``, the worker no longer holds an input.
            TaskFailed: The plan failed on the worker, or over ``/v1`` the socket went silent past
                ``FLOWFILE_WORKER_WS_TIMEOUT``.
            VersionMismatch: Over ``/v1``, the worker refused this backend's protocol version.
        """
        for ref in inputs:
            self._table(ref)
        if settings.FLOWFILE_COMPUTE_PROTOCOL:
            with self._lock:
                self._deferred.pop(result_id, None)
            meta = json.dumps(
                {
                    "hints": wire.SubmitHints.model_validate(asdict(hints)).model_dump(),
                    "inputs": [ref.result_id for ref in inputs if self._is_v1(ref)],
                    "result_id": result_id,
                    "flow_id": flow_id,
                    "node_id": node_id,
                }
            )
            plan_bytes = plan.serialize()
            answer = self._v1_socket_submit(meta, plan_bytes, result_id)
            if answer is None:
                response = self._v1(
                    "POST",
                    "/submit",
                    waits=True,
                    job_id=result_id,
                    files={"plan": ("plan", plan_bytes, "application/octet-stream")},
                    data={"meta": meta},
                )
                answer = wire.SubmitResult.model_validate_json(response.content)
            return self._from_wire(answer)
        if hints.must_materialise or (hints.wide and (hints.preview_rows is not None or hints.fanout > 1)):
            ref = self._store(plan, result_id, flow_id, node_id)
            sample = None if hints.preview_rows is None else self.sample(ref, hints.preview_rows)
        else:
            ref, sample = self._defer(plan, result_id, flow_id, node_id, hints.preview_rows)
        return SubmitResult(ref=ref, sample=sample, handle=JobHandle(self.id, result_id))

    def run_op(self, op: OperationKind, payload: OpPayload) -> OpResult:
        """Run ``op`` on the worker and wait for it to end.

        Each kind takes its frozen dataclass from ``execution/payloads.py`` under ``"payload"``. Frames are serialised
        here and credentials sent still encrypted. ``cancel(JobHandle(self.id, job_id))`` with the payload's
        ``job_id`` reaches the job while the call waits.

        With the protocol flag on, the kinds in ``V1_OPS`` go to ``POST /v1/ops/{kind}``, and ``polars_code``
        and ``sql_query`` do in either flag state. Their refs are keyed as on the task routes, each output a ``/v1``
        result ``<job_id>_<i>`` of its own. The Delta writes and ``viz_session`` always use the task routes.

        Per kind, its payload and what differs from refs ``{"result": ref}`` and empty ``metadata``:

        - ``database_read``: ``DatabaseReadPayload``.
        - ``kafka_read``: ``KafkaReadPayload``; ``metadata["offsets"]`` to commit when it consumed messages.
        - ``google_analytics_read``: ``GoogleAnalyticsReadPayload``.
        - ``rest_api_read``: ``RestApiReadPayload``.
        - ``file_read``: ``FileReadPayload``.
        - ``fuzzy_join``: ``FuzzyJoinPayload``, its sides prepared here over their payload schemas (``_fuzzy_sides``).
        - ``random_split``: ``RandomSplitPayload``, its rows shuffled once; one stored ref per split, keyed by split
          name. Over the task routes each split is stored as task ``<job_id>_<i>`` and the shuffle task released.
        - ``custom_node``: ``CustomNodePayload``, only the ``$ffsec$`` ciphertext of its selected secrets sent; one
          stored ref per declared output, keyed ``output-0``, ``output-1``, ... Over the task routes the outputs share
          one task (``<task id>#<name>``), so releasing one releases them all.
        - ``ml_train``: ``MlTrainPayload``; refs ``{"model": ref}``, the blob ``<job_id>_0`` (over the task routes, a
          file in the flow's cache directory), and ``metadata`` its ``sha256``, ``size_bytes`` and ``model_type``.
        - ``ml_apply``: ``MlApplyPayload``, scoring with the blob ``model``.
        - ``polars_code``: ``PolarsCodePayload``; the ref is deferred when the code returned a LazyFrame over sources,
          stored for a DataFrame or a LazyFrame over rows in memory.
        - ``sql_query``: ``SqlQueryPayload``; held as a ``polars_code`` result is.
        - ``file_write``: ``FileWritePayload``; no refs.
        - ``database_write``: ``DatabaseWritePayload``; no refs.
        - ``cloud_write``: ``CloudWritePayload``; no refs.
        - ``delta_write``: ``DeltaWritePayload``; no refs, ``metadata`` the worker's report (keys in the ``payloads``
          module docstring).
        - ``delta_merge``: ``DeltaMergePayload``; as ``delta_write``.
        - ``delta_scd2``: ``DeltaScd2Payload``; as ``delta_write``.
        - ``viz_session``: ``VizSessionPayload``; no task, the plan sent to the visualisation session ``session_key``
          names; no refs, ``metadata`` the session's answer.

        Raises:
            ComputeUnavailable: The worker could not be reached.
            ForeignRef: An ``ml_apply`` model another backend minted; nothing was sent.
            OperationUnsupported: A custom node without a node file.
            PlanRejected: The worker could not load the operation's plan.
            ResultMissing: An ``ml_apply`` model this backend no longer holds; over the task routes, found before any
                task.
            RuntimeError: The worker answered a ``viz_session`` request with an error status.
            TaskFailed: The operation failed on the worker.
            ValueError: The worker did not return one of a custom node's declared outputs.
        """
        operation = self._operations.get(op)
        if operation is None:
            raise OperationUnsupported(f"The worker backend does not run the {op.value} operation")
        if settings.FLOWFILE_COMPUTE_PROTOCOL and op in V1_OPS:
            return self._v1_op(op, payload)
        return operation(payload)

    def sample(self, ref: ResultRef, n: int) -> bytes:
        """The first ``n`` rows of ``ref``, or all of them when it has fewer, as an Arrow IPC stream.

        A stored result's rows are read from its file; a deferred one's come from a worker sampler task over its
        plan, filed under ``ref.result_id`` like the sample ``submit`` took. A ``/v1`` result's are the worker's
        answer to ``GET /v1/results/{id}/sample``. A blob is refused with ``OperationUnsupported`` before any call.
        """
        self._table(ref)
        if self._is_v1(ref):
            return self._v1("GET", f"/results/{ref.result_id}/sample", waits=True, params={"n": n}).content
        if ref.state == "deferred":
            held = self._held_plan(ref)
            held.sample_file = self._run_sampler(held, ref.result_id, n)
            return _read_sample(ref, held.sample_file, n)
        return _read_sample(ref, self._file(ref), n)

    def count(self, ref: ResultRef) -> int:
        """The number of rows in ``ref``'s file, or for a deferred result what the worker's count task returns.

        A ``/v1`` result's count is the worker's answer, asked even when the reference carries one. A blob is refused
        with ``OperationUnsupported`` before any call.
        """
        self._table(ref)
        if self._is_v1(ref):
            return self._v1("GET", f"/results/{ref.result_id}/count", waits=True).json()["row_count"]
        if ref.state == "deferred":
            held = self._held_plan(ref)
            fetcher = _run(
                lambda: ExternalDfFetcher(
                    flow_id=_NO_FLOW,
                    node_id=_NO_FLOW,
                    lf=held.plan,
                    operation_type="calculate_number_of_records",
                    wait_on_completion=False,
                    worker_url=self.url,
                ),
                backend_id=self.id,
            )
            return fetcher.result
        path = self._file(ref)
        with _held(ref):
            return _row_count(path)

    def exists(self, ref: ResultRef) -> bool:
        """Whether this backend holds a deferred ``ref``, or the worker still holds the task that stored it.

        For a ``/v1`` result, whether the worker still holds it (``HEAD /v1/results/{id}``). A task-route blob is held
        while its file is there.
        """
        if self._is_v1(ref):
            return self._v1("HEAD", f"/results/{ref.result_id}", missing_ok=True).status_code == 200
        if ref.state == "deferred":
            self._check(ref)
            with self._lock:
                return ref.result_id in self._deferred
        path = self._file(ref)
        if ref.kind == "blob":
            return os.path.isfile(path)
        return results_exists(_task_id(ref), worker_url=self.url)

    def release(self, ref: ResultRef) -> None:
        """Drop a deferred ``ref`` and clear its sampler task, or clear the task that stored ``ref``.

        Clearing a task deletes the file it registered. A ``/v1`` result is released on the worker
        (``DELETE /v1/results/{id}``); one it no longer holds is left as it is.
        """
        if self._is_v1(ref):
            self._v1("DELETE", f"/results/{ref.result_id}", missing_ok=True)
            return
        if ref.state == "deferred":
            self._check(ref)
            with self._lock:
                held = self._deferred.pop(ref.result_id, None)
            if held is not None and held.sample_file is not None:
                clear_task_from_worker(ref.result_id, worker_url=self.url)
            return
        self._file(ref)
        clear_task_from_worker(_task_id(ref), worker_url=self.url)

    def plan_for(self, ref: ResultRef) -> pl.LazyFrame:
        """The plan of a deferred ``ref``, or a scan of a stored ``ref``'s file; valid only in plans this backend runs.

        A task-route stored result is checked on the shared disk, not with the worker, since the scan needs the file.
        A ``/v1`` result's location is decoded without a call: a scan of the worker's path, which core never opens, or
        the deferred plan. A plan over a ``/v1`` result the worker has released fails when it is submitted.

        Raises:
            ForeignRef: Another backend minted ``ref``.
            OperationUnsupported: ``ref`` is a blob.
            ResultMissing: This backend no longer holds the deferred plan, or the stored result's file is gone.
        """
        self._table(ref)
        location = self._v1_location(ref)
        if location is not None:
            if location.kind == "file":
                return pl.scan_ipc(location.path)
            return pl.LazyFrame.deserialize(io.BytesIO(location.plan))
        if ref.state == "deferred":
            return self._held_plan(ref).plan
        path = self._file(ref)
        if not os.path.isfile(path):
            raise ResultMissing(f"The worker no longer holds result {ref.result_id!r}")
        return pl.scan_ipc(path)

    def result_file(self, ref: ResultRef) -> str | None:
        """The Arrow IPC file on the shared disk that holds ``ref``'s rows or its latest sample.

        A stored result's own file, or a deferred result's last sample file. ``None`` when no sample was taken, when
        this backend no longer holds the plan, or for a ``/v1`` result, whose file core never opens. Core reads first
        rows through it and treats a vanished file as a result to compute again.
        """
        if self._is_v1(ref):
            return None
        if ref.state == "deferred":
            self._check(ref)
            with self._lock:
                held = self._deferred.get(ref.result_id)
            return held.sample_file if held is not None else None
        return self._file(ref)

    def export(self, ref: ResultRef, uri: str, credential: CredentialRef | None) -> str:
        """Write ``ref``'s rows to ``uri`` as parquet where they are held, and return ``uri``.

        A result is exported over the routes that minted it, whatever the flag says: a ``/v1`` result by a worker child
        (``POST /v1/results/{id}/export``), a task-route result by a worker task running that child over
        ``plan_for(ref)``. Nothing of it runs in this process. ``credential`` crosses as the connection's id and its
        user's, resolved in the child.

        A blob takes no credential, as a presigned URL carries its own authorisation. The worker copies a ``/v1`` blob
        to a ``file://`` URI or ``PUT``s it to an ``https://`` URL. Core copies a task-route blob, a file on the shared
        disk, to a ``file://`` URI only.

        Raises:
            ComputeUnavailable: The worker could not be reached.
            ForeignRef: Another backend minted ``ref``.
            OperationUnsupported: A task-route blob and a ``uri`` that is not ``file://``; nothing was sent.
            OSError: The copy of a task-route blob failed.
            PlanRejected: The worker refused the URI's scheme for a blob (422).
            ResultMissing: The worker, or this backend, no longer holds ``ref``.
            TaskFailed: The worker's sink or copy failed.
            ValueError: A blob with a ``credential``; nothing was sent.
        """
        v1 = self._is_v1(ref)
        if ref.kind == "blob" and credential is not None:
            raise ValueError("A blob is copied to a URI that carries its own authorisation; it takes no credential")
        if v1:
            body = {"uri": uri, "credential": _wire_credential(credential)}
            return self._v1("POST", f"/results/{ref.result_id}/export", waits=True, json=body).json()["uri"]
        if ref.kind == "blob":
            path = self._file(ref)
            if not uri.startswith("file://"):
                raise OperationUnsupported(
                    f"Result {ref.result_id} is a file the task routes hold, copied to a file:// URI only; a presigned "
                    "URL needs the /v1 routes, with the protocol flag on"
                )
            if not os.path.isfile(path):
                raise ResultMissing(f"The worker no longer holds result {ref.result_id!r}")
            wire.put_blob(Path(path), uri)
            return uri
        plan = self.plan_for(ref)
        _run(
            lambda: ExternalDfFetcher(
                flow_id=_NO_FLOW,
                node_id=_NO_FLOW,
                lf=plan,
                operation_type="export_result",
                kwargs={"uri": uri, "credential": _wire_credential(credential)},
                wait_on_completion=False,
                worker_url=self.url,
            ),
            backend_id=self.id,
        )
        return uri

    def import_uri(
        self,
        uri: str,
        schema: Schema | None,
        row_count: int | None,
        credential: CredentialRef | None,
        kind: Literal["table", "blob"] = "table",
    ) -> ResultRef:
        """Hold a scan of the parquet at ``uri`` as a deferred result with the caller's ``schema`` and ``row_count``.

        Nothing is read, and ``credential`` crosses as the connection's id and its user's. With the protocol flag on
        the worker holds the scan (``POST /v1/results/{id}/import``); with it off this process holds it, a ``file:``
        URI scanned as its local path. ``release`` leaves the file.

        A ``kind="blob"`` reference is materialised, with no columns and no row count. With the flag on the worker
        copies a ``file://`` URI or fetches a presigned ``https://`` URL into a blob it holds, which ``release``
        removes. With it off only a ``file://`` URI is taken: the reference names its local path, nothing is copied,
        and ``release`` leaves the file.

        Raises:
            ComputeUnavailable: Over ``/v1``, the worker could not be reached.
            OperationUnsupported: A blob with the flag off whose URI is not ``file://``; nothing was sent.
            PlanRejected: The worker refused the URI's scheme for a blob (422).
            TaskFailed: The worker's copy or fetch failed.
            ValueError: A table without ``schema``, or a blob with a ``schema``, a ``row_count`` or a ``credential``;
                nothing was sent.
        """
        result_id = uuid.uuid4().hex
        if kind == "blob":
            if schema is not None or row_count is not None or credential is not None:
                raise ValueError("A blob import takes no schema, no row count and no credential")
            if not settings.FLOWFILE_COMPUTE_PROTOCOL:
                if not uri.startswith("file://"):
                    raise OperationUnsupported(
                        "The worker backend's task routes hold a blob from a file:// URI only; a presigned URL needs "
                        "the /v1 routes, with the protocol flag on"
                    )
                return ResultRef(
                    backend_id=self.id,
                    result_id=result_id,
                    schema=[],
                    row_count=None,
                    state="materialised",
                    location=wire.file_uri_path(uri).encode(),
                    kind="blob",
                )
            body = {"uri": uri, "credential": None, "columns": [], "row_count": None, "kind": "blob"}
            response = self._v1("POST", f"/results/{result_id}/import", waits=True, json=body)
            return self._ref_from_wire(wire.ResultRef.model_validate_json(response.content))
        if schema is None:
            raise ValueError("A table import needs its schema: it is the caller's, never read from the file")
        if settings.FLOWFILE_COMPUTE_PROTOCOL:
            body = {
                "uri": uri,
                "credential": _wire_credential(credential),
                "columns": _wire_columns(schema),
                "row_count": row_count,
            }
            response = self._v1("POST", f"/results/{result_id}/import", json=body)
            return self._ref_from_wire(wire.ResultRef.model_validate_json(response.content))
        scan = pl.scan_parquet(wire.file_uri_path(uri), **_credential_kwargs(credential))
        ref, _ = self._defer(scan, result_id, _NO_FLOW, _NO_FLOW, None, schema=schema, row_count=row_count)
        return ref

    def status(self, handle: JobHandle) -> JobStatus:
        """The worker's ``GET /status`` answer for the task ``handle`` names.

        With the protocol flag on, its ``GET /v1/jobs/{id}`` answer, which covers task-route jobs too.

        Raises:
            LookupError: Over ``/v1``, the worker has no job by that id.
        """
        self._own(handle)
        if settings.FLOWFILE_COMPUTE_PROTOCOL:
            response = self._v1("GET", f"/jobs/{handle.job_id}", missing_ok=True)
            if response.status_code == 404:
                raise LookupError(f"The worker at {self.url} has no job {handle.job_id!r}")
            status = Status.model_validate(response.json())
            state = _JOB_STATES[status.status]
            error = (status.error_message or status.status) if state == "failed" else None
            return JobStatus(state=state, progress=status.progress, error=error)
        try:
            status = get_status(handle.job_id, worker_url=self.url)
        except requests.RequestException as e:
            raise ComputeUnavailable(f"Could not reach the worker: {e}", backend_id=self.id) from e
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
            return get_status(handle.job_id, worker_url=self.url).results
        except requests.RequestException as e:
            raise ComputeUnavailable(f"Could not reach the worker: {e}", backend_id=self.id) from e

    def cancel(self, handle: JobHandle) -> None:
        """Cancel the task ``handle`` names: via its fetcher while ``submit`` or ``run_op`` waits, else on the worker.

        A task that already ended is left as it is. With the protocol flag on, a job no fetcher waits on is
        cancelled through ``POST /v1/jobs/{id}/cancel``; a ``/v1`` submit waiting on it then fails with
        ``TaskFailed``.
        """
        self._own(handle)
        with self._lock:
            fetcher = self._in_flight.get(handle.job_id)
        if fetcher is not None:
            fetcher.cancel()
        elif settings.FLOWFILE_COMPUTE_PROTOCOL:
            self._v1("POST", f"/jobs/{handle.job_id}/cancel", missing_ok=True)
        else:
            cancel_task(handle.job_id, worker_url=self.url)

    def _file(self, ref: ResultRef) -> str:
        self._check(ref)
        return ref.location.decode()

    def _held_plan(self, ref: ResultRef) -> _DeferredPlan:
        self._check(ref)
        with self._lock:
            held = self._deferred.get(ref.result_id)
        if held is None:
            raise ResultMissing(f"The {self.id} backend no longer holds result {ref.result_id!r}")
        return held

    def _v1(
        self,
        method: str,
        path: str,
        *,
        missing_ok: bool = False,
        waits: bool = False,
        job_id: str | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Send one request to the worker's ``/v1`` route ``path`` with this backend's protocol version and token.

        ``waits`` marks a call that waits on a worker child (a submit, a sample, a count): its read timeout is the poll
        deadline (``FLOWFILE_WORKER_POLL_TIMEOUT``), and past it the job ``job_id`` names is cancelled. A 2xx answer is
        returned, a 404 too with ``missing_ok``. The worker session's handshake gates the request.

        Raises:
            ComputeUnavailable: The worker could not be reached.
            TaskFailed: This backend's token is no header value; nothing was sent. Or a waiting call had no answer
                within the poll deadline; the handshake stands.
            ComputeError: The worker answered anything else (``_v1_failure``).
        """
        timeout = (_WORKER_TIMEOUT[0], _POLL_DEADLINE_SECONDS or None) if waits else _WORKER_TIMEOUT
        try:
            response = _worker_session.request(
                method,
                f"/v1{path}",
                worker_url=self.url,
                backend_id=self.id,
                headers={wire.PROTOCOL_VERSION_HEADER: str(self.protocol_version)},
                timeout=timeout,
                auth=self._auth,
                **kwargs,
            )
        except requests.ReadTimeout as e:
            if not waits:
                raise ComputeUnavailable(f"Could not reach the worker at {self.url}: {e}", backend_id=self.id) from e
            if job_id is not None:
                self._give_up(job_id)
            raise TaskFailed(
                f"The {self.id} backend gave no answer to {method} /v1{path} within the {_POLL_DEADLINE_SECONDS:.0f}s "
                "poll deadline (FLOWFILE_WORKER_POLL_TIMEOUT); giving up"
            ) from e
        except requests.RequestException as e:
            raise ComputeUnavailable(f"Could not reach the worker at {self.url}: {e}", backend_id=self.id) from e
        if response.ok or (missing_ok and response.status_code == 404):
            return response
        raise _v1_failure(response.status_code, response.content, response.reason)

    def _give_up(self, job_id: str) -> None:
        """Cancel the ``/v1`` job ``job_id`` core stopped waiting on, so it holds no child or output; best effort."""
        try:
            self._v1("POST", f"/jobs/{job_id}/cancel", missing_ok=True)
        except ComputeError as e:
            logger.warning(f"Could not cancel job {job_id!r} on the {self.id} backend after giving up on it: {e}")

    def _v1_socket_submit(self, meta: str, plan: bytes, result_id: str) -> wire.SubmitResult | None:
        """The worker's answer to a submit over ``WS /v1/ws/submit``, or ``None`` for ``submit`` to post it instead.

        ``None`` means the socket was not dialled, could not be opened or the plan not sent. The worker reads both
        frames before it runs anything and drops a socket that closed before the plan, so the post never runs a plan
        twice. The socket is not dialled when the cached verdict declares no submit socket, or its upgrade was refused
        under that same verdict, so a refusal is retried only after the backend is checked again. The dial has no
        frame-size cap, since a deferred result's ``complete`` frame carries its plan. Once the plan is sent nothing is
        resent.

        Raises:
            ComputeUnavailable: The socket closed after the plan was sent; the handshake of the backends on ``url`` is
                re-armed.
            TaskFailed: No frame for longer than ``FLOWFILE_WORKER_WS_TIMEOUT``, and the job is cancelled; or this
                backend's token is no header value, found before the dial.
            VersionMismatch: The worker refused the upgrade with 426.
            ComputeError: The handshake failed, or the worker's ``error`` frame (``_v1_error``).
        """
        ensure_handshake(self.url, backend_id=self.id)
        verdict = cached_verdict(self)
        if verdict is not None and (verdict is self._socket_refused_under or not _takes_submit_socket(verdict)):
            return None
        token = self._internal_token()
        try:
            ws = connect(
                _get_ws_url(self.url) + "/v1/ws/submit",
                additional_headers={
                    "X-Flowfile-Internal": token,
                    wire.PROTOCOL_VERSION_HEADER: str(self.protocol_version),
                },
                max_size=None,
                ssl=ssl_context_for(self.url),
                close_timeout=_SOCKET_CLOSE_TIMEOUT,
            )
        except OSError as e:
            # Re-arming here would re-enter a running check; the POST's own connection error re-arms an outage.
            self._socket_refused_under = verdict
            logger.debug(f"The /v1 submit socket to {self.url} could not be opened ({e}); posting the plan instead")
            return None
        except Exception as e:
            if isinstance(e, InvalidStatus) and e.response.status_code == 426:
                raise _v1_failure(426, bytes(e.response.body), e.response.reason_phrase) from e
            self._socket_refused_under = verdict
            logger.debug(f"The /v1 submit socket to {self.url} was refused ({e}); posting the plan instead")
            return None
        try:
            ws.send(meta)
            ws.send(plan)
        except Exception as e:
            ws.close()
            logger.debug(f"The plan could not be sent over the /v1 submit socket ({e}); posting it instead")
            return None
        try:
            frame = _final_frame(ws, self.url, result_id, backend_id=self.id)
        except TaskFailed:
            self._give_up(result_id)
            raise
        finally:
            ws.close()
        if frame["type"] == "error":
            raise _v1_error(frame["status_code"], frame["detail"])
        return wire.SubmitResult.model_validate(frame["result"])

    def _internal_token(self) -> str:
        """The token this backend signs the worker's ``/v1`` calls with: its own, else the process's internal token.

        Raises:
            TaskFailed: Its own token is no header value (``_sendable_token``).
        """
        return _sendable_token(self.token, self.id) if self.token is not None else get_internal_token()

    def _is_v1(self, ref: ResultRef) -> bool:
        """Whether ``/v1`` minted ``ref``: its location is a ``WorkerLocation`` JSON, which is not decoded here.

        A task-route result's location is a file path or its result id, never JSON.

        Raises:
            ForeignRef: Another backend minted ``ref``.
        """
        self._check(ref)
        return ref.location.startswith(b"{")

    def _v1_location(self, ref: ResultRef) -> wire.WorkerLocation | None:
        """The ``WorkerLocation`` of a result ``/v1`` minted, decoded (a deferred one's carries its plan); ``None`` for
        a task-route result.

        Raises:
            ForeignRef: Another backend minted ``ref``.
        """
        if not self._is_v1(ref):
            return None
        return wire.WorkerLocation.model_validate_json(ref.location)

    def _v1_op(self, op: OperationKind, payload: OpPayload) -> OpResult:
        """Run ``op`` over the worker's ``POST /v1/ops/{kind}`` and key its refs as the task-route method does.

        The payload is unpacked before anything is sent, so an empty one is a ``ValueError`` here too. A training's
        answer ``metadata`` is the model's digest, size and type.

        Raises:
            ForeignRef: An ``ml_apply`` model another backend minted.
            OperationUnsupported: A custom node without a node file.
            ValueError: The worker did not return one of a custom node's declared outputs.
        """
        spec = V1_OPS.get(op)
        if spec is None:
            raise OperationUnsupported(f"The worker backend does not run the {op.value} operation over /v1")
        job = unpack(payload, spec.payload)
        for ref in spec.held(job):
            self._check(ref)
        answer = self._v1(
            "POST",
            f"/ops/{op.value}",
            waits=True,
            job_id=job.job_id,
            params={"job_id": job.job_id, **spec.params(job)},
            json=_json_body(spec.request(job)),
        ).json()
        refs = {
            name: self._ref_from_wire(wire.ResultRef.model_validate(raw)) for name, raw in answer["outputs"].items()
        }
        return spec.result(job, refs, answer["metadata"])

    def _store(
        self, plan: pl.LazyFrame, result_id: str, flow_id: int, node_id: int | str, *, tracked_as: str | None = None
    ) -> ResultRef:
        """Run ``plan`` as worker store task ``result_id`` and return the result it wrote.

        ``tracked_as`` is the job id ``cancel`` reaches the task under, ``result_id`` by default.
        """
        with self._lock:
            self._deferred.pop(result_id, None)
        plan_bytes = plan.serialize()
        fetcher = self._run_tracked(
            tracked_as or result_id,
            lambda: ExternalDfFetcher(
                flow_id=flow_id,
                node_id=node_id,
                plan_bytes=plan_bytes,
                file_ref=result_id,
                wait_on_completion=False,
                worker_url=self.url,
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
                worker_url=self.url,
            ),
        )
        return _written_file(fetcher)

    def _run_tracked(self, task_id: str, start: Callable[[], BaseFetcher]) -> BaseFetcher:
        """``_run``, with the fetcher reachable through ``cancel`` under ``task_id`` while it waits."""

        def track(fetcher: BaseFetcher) -> None:
            with self._lock:
                self._in_flight[task_id] = fetcher

        try:
            return _run(start, on_started=track, backend_id=self.id)
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
        fetcher = _run(start, backend_id=self.id) if job_id is None else self._run_tracked(job_id, start)
        return OpResult(refs={"result": self._result_ref(fetcher.file_ref, _written_file(fetcher))})

    def _read_tracked(self, job_id: str, start: Callable[[], BaseFetcher]) -> tuple[BaseFetcher, ResultRef]:
        """Run a read task as job ``job_id``, which ``cancel`` reaches while it waits; its fetcher and stored result."""
        fetcher = self._run_tracked(job_id, start)
        return fetcher, self._result_ref(fetcher.file_ref, _written_file(fetcher))

    def _database_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, DatabaseReadPayload)
        settings = _database_read_settings(read)
        _, ref = self._read_tracked(
            read.job_id, lambda: ExternalDatabaseFetcher(settings, wait_on_completion=False, worker_url=self.url)
        )
        return OpResult(refs={"result": ref})

    def _kafka_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, KafkaReadPayload)
        settings = _kafka_read_settings(read)
        fetcher, ref = self._read_tracked(
            read.job_id, lambda: ExternalKafkaFetcher(settings, wait_on_completion=False, worker_url=self.url)
        )
        consumed = fetch_kafka_offsets(fetcher.file_ref, worker_url=self.url)
        return OpResult(refs={"result": ref}, metadata=_kafka_offsets(consumed))

    def _google_analytics_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, GoogleAnalyticsReadPayload)
        settings = _google_analytics_read_settings(read)
        _, ref = self._read_tracked(
            read.job_id, lambda: ExternalGoogleAnalyticsFetcher(settings, wait_on_completion=False, worker_url=self.url)
        )
        return OpResult(refs={"result": ref})

    def _rest_api_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, RestApiReadPayload)
        settings = _rest_api_read_settings(read)
        _, ref = self._read_tracked(
            read.job_id, lambda: ExternalRestApiFetcher(settings, wait_on_completion=False, worker_url=self.url)
        )
        return OpResult(refs={"result": ref})

    def _file_read(self, payload: OpPayload) -> OpResult:
        read = unpack(payload, FileReadPayload)
        received_table = _file_to_read(read)
        _, ref = self._read_tracked(
            read.job_id,
            lambda: ExternalCreateFetcher(
                received_table=received_table,
                node_id=read.node_id,
                flow_id=read.flow_id,
                file_type=received_table.file_type,
                wait_on_completion=False,
                worker_url=self.url,
            ),
        )
        return OpResult(refs={"result": ref})

    def _fuzzy_join(self, payload: OpPayload) -> OpResult:
        join = unpack(payload, FuzzyJoinPayload)
        left, right, fuzzy_maps = _fuzzy_sides(join)
        return self._read(
            lambda: ExternalFuzzyMatchFetcher(
                left_df=left,
                right_df=right,
                fuzzy_maps=fuzzy_maps,
                flow_id=join.flow_id,
                node_id=join.node_id,
                file_ref=join.job_id,
                wait_on_completion=False,
                worker_url=self.url,
            ),
            job_id=join.job_id,
        )

    def _random_split(self, payload: OpPayload) -> OpResult:
        split = unpack(payload, RandomSplitPayload)
        stored = self._store(_shuffled_rows(split), split.job_id, split.flow_id, split.node_id)
        total = stored.row_count
        rows = self.plan_for(stored)
        groups = split.settings.splits
        refs: dict[str, ResultRef] = {}
        offset = 0
        try:
            for i, group in enumerate(groups):
                length = total - offset if i == len(groups) - 1 else int(round(total * group.percentage / 100.0))
                length = max(0, length)
                refs[group.name] = self._store(
                    rows.slice(offset, length),
                    f"{split.job_id}_{i}",
                    split.flow_id,
                    split.node_id,
                    tracked_as=split.job_id,
                )
                offset += length
        except BaseException:
            for ref in refs.values():
                self.release(ref)
            raise
        finally:
            # Each split is stored on its own, as the /v1 operation stores them, so the shuffle is no one's.
            self.release(stored)
        return OpResult(refs=refs)

    def _ml_apply(self, payload: OpPayload) -> OpResult:
        apply = unpack(payload, MlApplyPayload)
        model = apply.model
        if self._is_v1(model) or not os.path.isfile(model.location.decode()):
            raise ResultMissing(f"The worker's task routes do not hold model {model.result_id!r}")
        return self._read(
            lambda: MLApplyFetcher(
                lf=apply.plan,
                model_path=model.location.decode(),
                output_column=apply.output_column,
                flow_id=apply.flow_id,
                node_id=apply.node_id,
                file_ref=apply.job_id,
                wait_on_completion=False,
                worker_url=self.url,
            ),
            job_id=apply.job_id,
        )

    def _custom_node(self, payload: OpPayload) -> OpResult:
        job = unpack(payload, CustomNodePayload)
        request = _custom_node_request(job)
        fetcher = self._run_tracked(job.job_id, lambda: ExternalCustomNodeFetcher(request, worker_url=self.url))
        outputs = fetcher.get_payload()["outputs"]
        refs: dict[str, ResultRef] = {}
        for i, name in enumerate(job.output_names):
            output = outputs.get(name)
            if output is None:
                raise ValueError(f"Worker did not return declared output '{name}'")
            result_id = f"{fetcher.file_ref}{_OUTPUT_SEPARATOR}{name}"
            refs[output_handle(i)] = self._result_ref(result_id, output["path"], output["row_count"])
        return OpResult(refs=refs)

    def _polars_code(self, payload: OpPayload) -> OpResult:
        """Run the code over ``POST /v1/ops/polars_code`` whatever the flag says, as no task route runs it."""
        return self._v1_op(OperationKind.polars_code, payload)

    def _sql_query(self, payload: OpPayload) -> OpResult:
        """Send the statement to ``POST /v1/ops/sql_query`` whatever the flag says, as no task route runs it."""
        return self._v1_op(OperationKind.sql_query, payload)

    def _ml_train(self, payload: OpPayload) -> OpResult:
        """Stage the model in the flow's cache directory as worker task ``<job_id>_0``, apart from the node's task."""
        train = unpack(payload, MlTrainPayload)
        model_id = f"{train.job_id}_0"
        staged = str(storage.get_flow_cache_directory(train.flow_id) / "ml_models" / f"{model_id}.json")
        fetcher = self._run_tracked(
            train.job_id,
            lambda: MLTrainFetcher(
                lf=train.plan,
                staging_path=staged,
                model_type=train.model_type,
                target_column=train.target_column,
                feature_columns=train.feature_columns,
                params=train.params,
                flow_id=train.flow_id,
                node_id=train.node_id,
                file_ref=model_id,
                wait_on_completion=False,
                worker_url=self.url,
            ),
        )
        model = ResultRef(
            backend_id=self.id,
            result_id=model_id,
            schema=[],
            row_count=None,
            state="materialised",
            location=staged.encode(),
            kind="blob",
        )
        return OpResult(refs={"model": model}, metadata=_reported(fetcher))

    def _database_write(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, DatabaseWritePayload)
        settings = _database_write_settings(write)
        self._run_tracked(
            write.job_id, lambda: ExternalDatabaseWriter(settings, wait_on_completion=False, worker_url=self.url)
        )
        return OpResult(refs={})

    def _cloud_write(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, CloudWritePayload)
        settings = _cloud_write_settings(write)
        self._run_tracked(
            write.job_id, lambda: ExternalCloudWriter(settings, wait_on_completion=False, worker_url=self.url)
        )
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
                worker_url=self.url,
            ),
        )
        return OpResult(refs={})

    def _delta_write(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, DeltaWritePayload)
        kwargs = {
            **_delta_destination(write),
            "mode": write.mode,
            "partition_by": write.partition_by,
            "enable_cdf": write.enable_cdf,
        }
        return self._delta_task(write, "write_delta", kwargs)

    def _delta_merge(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, DeltaMergePayload)
        kwargs = {
            **_delta_destination(write),
            "merge_mode": write.merge_mode,
            "merge_keys": write.merge_keys,
            "partition_by": write.partition_by,
            "enable_cdf": write.enable_cdf,
        }
        return self._delta_task(write, "merge_delta", kwargs)

    def _delta_scd2(self, payload: OpPayload) -> OpResult:
        write = unpack(payload, DeltaScd2Payload)
        kwargs = {
            **_delta_destination(write),
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
        return self._delta_task(write, "scd2_delta", kwargs)

    def _viz_session(self, payload: OpPayload) -> OpResult:
        """Answer one Graphic Walker request in the worker's visualisation session over the payload's plan."""
        request = unpack(payload, VizSessionPayload)
        source = {
            "kind": "plan",
            "session_key": request.session_key,
            "plan_bytes": base64.b64encode(request.plan.serialize()).decode("ascii"),
        }
        try:
            if request.request == "fields":
                answer = trigger_visualize_fields(source, worker_url=self.url)
            else:
                answer = trigger_visualize_query(source, request.query, request.max_rows, worker_url=self.url)
        except requests.RequestException as e:
            raise ComputeUnavailable(f"Could not reach the worker: {e}", backend_id=self.id) from e
        return OpResult(refs={}, metadata=answer)

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
                worker_url=self.url,
            ),
        )
        return OpResult(refs={}, metadata=_reported(fetcher))


def _run(
    start: Callable[[], BaseFetcher], on_started: Callable[[BaseFetcher], None] | None = None, *, backend_id: str
) -> BaseFetcher:
    """Start a worker task through ``start``, wait for it to end and return its fetcher.

    ``on_started`` receives the fetcher once the task is submitted, before the wait. A ``requests`` error while
    starting is a ``ComputeUnavailable`` naming ``backend_id``. A failed task with error code -1 goes through
    ``worker_failure``, a lost connection naming ``backend_id``. Any other code is a ``TaskFailed`` raised from the
    fetcher's ``RemoteExecutionError``, so the worker-side class stays recoverable from the chain.
    """
    try:
        fetcher = start()
    except requests.RequestException as e:
        raise ComputeUnavailable(f"Could not reach the worker: {e}", backend_id=backend_id) from e
    if on_started is not None:
        on_started(fetcher)
    try:
        fetcher.get_result()
    except Exception as e:
        description = fetcher.error_description or str(e)
        if fetcher.error_code == -1:
            failure = worker_failure(fetcher.error_kind, description)
            if isinstance(failure, ComputeUnavailable):
                failure.backend_id = backend_id
            raise failure from e
        raise TaskFailed(description) from e
    return fetcher


def _takes_submit_socket(verdict: Verdict) -> bool:
    """Whether ``verdict``'s backend declared the ``/v1`` submit socket; true when its capabilities were not read."""
    return verdict.backend is None or verdict.backend.submit_socket


def _final_frame(ws: ClientConnection, worker_url: str, result_id: str, *, backend_id: str) -> dict[str, Any]:
    """The first ``complete`` or ``error`` frame the worker sends on the ``/v1`` submit socket ``ws``.

    Progress frames are read past, and each frame resets the silence clock (``FLOWFILE_WORKER_WS_TIMEOUT``, 0
    disables).

    Raises:
        ComputeUnavailable: The socket closed before a final frame. It names ``backend_id``, and the backends on
            ``worker_url`` are checked again on their next use, since the worker that comes back may be another one.
        TaskFailed: The worker went silent past the timeout.
    """
    last_frame_at = monotonic()
    while True:
        try:
            message = ws.recv(timeout=_WS_RECV_POLL_SECONDS)
        except TimeoutError:
            silence = monotonic() - last_frame_at
            if _WS_INACTIVITY_TIMEOUT and silence > _WS_INACTIVITY_TIMEOUT:
                raise TaskFailed(
                    f"No data from the worker at {worker_url} for {silence:.0f}s (submit {result_id}); worker presumed "
                    "unresponsive. Tune or disable via FLOWFILE_WORKER_WS_TIMEOUT."
                ) from None
            continue
        except ConnectionClosed as e:
            reset_handshake(worker_url)
            raise ComputeUnavailable(
                f"The worker at {worker_url} closed the connection before it answered submit {result_id}: {e}",
                backend_id=backend_id,
            ) from e
        last_frame_at = monotonic()
        frame = json.loads(message)
        if frame.get("type") in ("complete", "error"):
            return frame


def is_header_value(value: str) -> bool:
    """Whether ``value`` can be sent as an HTTP header value by ``requests`` and by the ``/v1`` submit socket.

    Tabs, spaces, printable ASCII and the Latin-1 characters above it. ``http.client`` refuses a CR or a LF and a
    character beyond Latin-1 with an error that quotes the value, and ``websockets`` refuses any other ASCII control
    character but a tab too, logging the value before it raises.
    """
    return _HEADER_VALUE.fullmatch(value) is not None


def _sendable_token(token: str, backend_id: str) -> str:
    """``token``, once it is known to be a header value, so no client is handed one it would quote in an error.

    Raises:
        TaskFailed: It is no header value; the message names the backend and ``TOKEN_RULE``, never the token.
    """
    if not is_header_value(token):
        raise TaskFailed(f"The token configured for the {backend_id} backend cannot be sent: {TOKEN_RULE}")
    return token


class _TokenAuth(requests.auth.AuthBase):
    """Signs a worker request with one configured backend's own token in place of the process's internal token.

    The check of the token (``_sendable_token``) runs as the request is prepared, before ``http.client`` sees it, and
    its ``TaskFailed`` leaves ``requests`` as it is raised.
    """

    def __init__(self, token: str, backend_id: str) -> None:
        self._token = token
        self._backend_id = backend_id

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        request.headers["X-Flowfile-Internal"] = _sendable_token(self._token, self._backend_id)
        return request


def _v1_error(status_code: int, detail: Any, *, answered_by: str = "The worker") -> ComputeError:
    """The typed error for a ``/v1`` failure the worker answered with ``status_code`` and ``detail``.

    REST answers, refused socket upgrades, ``error`` frames and an ``HTTPException`` a ``/v1`` body raised in this
    process (``EmbeddedWorkerBackend``) all map here, so both backends type a failure alike:

    - 426: ``VersionMismatch``, a protocol version the worker refuses.
    - 404: ``ResultMissing``, a result or input it does not hold.
    - 422: ``PlanRejected``, what a body refuses before anything runs.
    - 500 carrying a child's ``status``: through ``worker_failure``, as a failed task.
    - Any other code: a ``TaskFailed`` naming the code and ``answered_by``.
    """
    if status_code == 426:
        return VersionMismatch(str(detail))
    if status_code == 404:
        return ResultMissing(str(detail))
    if status_code == 422:
        return PlanRejected(str(detail))
    if status_code == 500 and isinstance(detail, dict) and "status" in detail:
        return worker_failure(detail.get("error_kind"), detail.get("error_message") or detail["status"])
    return TaskFailed(f"{answered_by} answered {status_code}: {detail}")


def _v1_failure(status_code: int, body: bytes, reason: str) -> ComputeError:
    """The typed error for an HTTP ``/v1`` answer that is no success, through ``_v1_error``.

    The detail is the ``detail`` of the JSON ``body``, else the body as text, else the code and its ``reason``.
    """
    try:
        answer = json.loads(body)
    except ValueError:
        answer = None
    detail = answer.get("detail") if isinstance(answer, dict) else None
    if detail is None:
        detail = body.decode(errors="replace") or f"{status_code} {reason}"
    return _v1_error(status_code, detail)


def _storage_payload(connection: FullCloudStorageConnectionWorkerInterface | None) -> dict[str, Any] | None:
    """The ``storage_payload`` the worker's Delta operations take; ``None`` for a table on the worker's disk.

    It carries the connection with its secrets still encrypted; the worker decrypts them.
    """
    return None if connection is None else {"connection": connection.model_dump()}


def _delta_destination(write: DeltaWritePayload | DeltaMergePayload | DeltaScd2Payload) -> dict[str, Any]:
    """Where the worker's Delta operation writes, as its keyword arguments.

    A catalog table named by identity goes as ``table_id``, ``table_name`` and a ``storage_payload`` carrying the
    catalog's ``base_uri`` beside the connection (``None`` for the worker's catalog tables directory), so no path
    crosses and the worker resolves the table under its own root; any other write goes as its ``output_path``.
    """
    storage_payload = _storage_payload(write.connection)
    if write.table is None:
        return {"output_path": write.path, "storage_payload": storage_payload}
    if write.table.base_uri is not None:
        storage_payload = {"base_uri": write.table.base_uri, **(storage_payload or {})}
    return {"table_id": write.table.table_id, "table_name": write.table.table_name, "storage_payload": storage_payload}


def _credential_kwargs(credential: CredentialRef | None) -> dict[str, Any]:
    """What a scan of a URI takes for ``credential``: its provider and its connection's plain options.

    The provider carries the stored connection's id and user only, and resolves it where the plan runs. The options
    (``credential_ref_kwargs``, no secret among them) carry the endpoint and region, so an S3-compatible store is
    reached.
    """
    if credential is None:
        return {}
    return credential_ref_kwargs(credential.connection_id, credential.user_id)


def _wire_credential(credential: CredentialRef | None) -> dict[str, int] | None:
    """A credential as the export and import bodies and the export task carry it: the connection's id and its user's."""
    return None if credential is None else asdict(credential)


def _wire_columns(schema: Schema) -> list[dict[str, str]]:
    """A schema in the ``WireColumn`` spelling the worker answers and ``_ref_from_wire`` reads back."""
    return [{"name": column.column_name, "data_type": column.data_type} for column in schema]


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


def _database_write_settings(write: DatabaseWritePayload) -> DatabaseExternalWriteSettings:
    """The worker's database write: the plan, the table and the connection with its password as stored."""
    reference = write.connection if write.settings.database_write_settings.connection_mode == "reference" else None
    settings = DatabaseExternalWriteSettings.create_from_from_node_database_writer(
        node_database_writer=write.settings,
        password=write.encrypted_password,
        table_name=write.table_name,
        lf=write.plan,
        database_reference_settings=reference,
    )
    return settings.model_copy(update=_filed_under(write))


def _cloud_write_settings(write: CloudWritePayload) -> CloudStorageWriteSettingsWorkerInterface:
    """The worker's cloud write: the plan, the write settings and the connection with its secrets still encrypted."""
    return CloudStorageWriteSettingsWorkerInterface(
        operation=base64.b64encode(write.plan.serialize()).decode(),
        write_settings=write.settings.get_write_setting_worker_interface(),
        connection=write.connection,
        **_filed_under(write),
    )


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


def _file_to_read(read: FileReadPayload) -> ReceivedTable:
    """The file a file read names, refused here when it is a cloud path or a directory, which no worker reader reads."""
    received_table = read.settings.received_file
    refuse_cloud_path(received_table.path, received_table.file_type)
    if received_table.scan_mode == "directory":
        raise DirectoryScanUnsupportedError("Directory scan mode cannot be executed by the worker file reader.")
    return received_table


def _fuzzy_sides(join: FuzzyJoinPayload) -> tuple[pl.LazyFrame, pl.LazyFrame, list[FuzzyMapping]]:
    """Both sides of a fuzzy join selected and renamed as the in-process join does, and the mappings to match on.

    Each side is prepared over the schema the payload carries for it, so no side's plan, which may scan a result the
    worker holds, is resolved here; a side whose schema is ``None`` is.
    """
    manager = FuzzyMatchInputManager(deepcopy(join.settings.join_input))
    left, right = prepare_for_fuzzy_match(
        left=FlowDataEngine(join.left, schema=join.left_schema),
        right=FlowDataEngine(join.right, schema=join.right_schema),
        fuzzy_match_input_manager=manager,
    )
    return left, right, manager.fuzzy_maps


def _shuffled_rows(split: RandomSplitPayload) -> pl.LazyFrame:
    """A random split's rows in random order, by its seed or a fresh one; every split slices this one order."""
    seed = split.settings.seed if split.settings.seed is not None else random.randint(0, 2**31 - 1)
    return (
        split.plan.with_columns(pl.int_range(0, pl.len()).shuffle(seed=seed).alias(_SPLIT_RANK))
        .sort(_SPLIT_RANK)
        .drop(_SPLIT_RANK)
    )


def _custom_node_request(job: CustomNodePayload) -> CustomNodeExecuteInput:
    """What the worker runs a custom node from: its source, settings, selected secrets as ciphertext and inputs.

    Raises:
        OperationUnsupported: The node has no node file, so the worker has no source to run.
    """
    if job.node_source is None:
        raise OperationUnsupported(
            f"The {WORKER_BACKEND_ID} backend runs a custom node from its node file; node {job.node_id} has none"
        )
    return CustomNodeExecuteInput(
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


def _kafka_offsets(consumed: dict[str, Any] | None) -> dict[str, Any]:
    """A Kafka read's ``metadata``: the offsets to commit when it consumed messages, else nothing."""
    if consumed and consumed.get("messages_consumed", 0) > 0:
        return {"offsets": consumed["new_offsets"]}
    return {}


def _declared_outputs(refs: dict[str, ResultRef], output_names: list[str]) -> dict[str, ResultRef]:
    """A custom node's refs by declared output name, keyed by output handle in declaration order.

    Raises:
        ValueError: The worker did not return one of the declared outputs.
    """
    handles: dict[str, ResultRef] = {}
    for i, name in enumerate(output_names):
        ref = refs.get(name)
        if ref is None:
            raise ValueError(f"Worker did not return declared output '{name}'")
        handles[output_handle(i)] = ref
    return handles


def _json_body(body: Any) -> Any:
    """``body`` with every ``bytes`` value base64-encoded, as the worker's models read bytes from JSON."""
    if isinstance(body, bytes):
        return base64.b64encode(body).decode("ascii")
    if isinstance(body, dict):
        return {key: _json_body(value) for key, value in body.items()}
    if isinstance(body, list | tuple):
        return [_json_body(value) for value in body]
    return body


def _no_params(job: Any) -> dict[str, Any]:
    return {}


def _no_refs(job: Any) -> tuple[ResultRef, ...]:
    return ()


def _all_outputs(job: Any, refs: dict[str, ResultRef]) -> dict[str, ResultRef]:
    return refs


def _no_metadata(answered: dict[str, Any]) -> dict[str, Any]:
    return {}


def _all_metadata(answered: dict[str, Any]) -> dict[str, Any]:
    return answered


@dataclass(frozen=True)
class V1Op:
    """How one kind runs over ``POST /v1/ops/<kind>``, the same on ``WorkerBackend`` and ``EmbeddedWorkerBackend``.

    ``request`` is the route's body as plain data (``bytes`` left raw: the worker's models read raw bytes or base64,
    and ``_json_body`` encodes them for the wire), ``params`` the query parameters besides ``job_id``, ``held`` the
    references the payload names that must be the running backend's, ``outputs`` the refs keyed as the caller reads
    them and ``metadata`` what the caller keeps of the answer's ``metadata``.
    """

    payload: type
    request: Callable[[Any], dict[str, Any]]
    params: Callable[[Any], dict[str, Any]] = _no_params
    held: Callable[[Any], tuple[ResultRef, ...]] = _no_refs
    outputs: Callable[[Any, dict[str, ResultRef]], dict[str, ResultRef]] = _all_outputs
    metadata: Callable[[dict[str, Any]], dict[str, Any]] = _no_metadata

    def result(self, job: Any, refs: dict[str, ResultRef], metadata: dict[str, Any]) -> OpResult:
        """The ``OpResult`` for an answer's refs, already the backend's, and its ``metadata``."""
        return OpResult(refs=self.outputs(job, refs), metadata=self.metadata(metadata))


def _fuzzy_join_body(job: FuzzyJoinPayload) -> dict[str, Any]:
    left, right, fuzzy_maps = _fuzzy_sides(job)
    return FuzzyJoinInput(
        left_df_operation=PolarsOperation(operation=left.serialize()),
        right_df_operation=PolarsOperation(operation=right.serialize()),
        fuzzy_maps=fuzzy_maps,
        task_id=job.job_id,
        flowfile_flow_id=job.flow_id,
        flowfile_node_id=job.node_id,
    ).model_dump(mode="json")


def _ml_train_body(job: MlTrainPayload) -> dict[str, Any]:
    return {
        "operation": job.plan.serialize(),
        "flowfile_flow_id": job.flow_id,
        "flowfile_node_id": job.node_id,
        "model_type": job.model_type,
        "target_column": job.target_column,
        "feature_columns": job.feature_columns,
        "params": job.params,
    }


def _ml_apply_body(job: MlApplyPayload) -> dict[str, Any]:
    return {
        "operation": job.plan.serialize(),
        "flowfile_flow_id": job.flow_id,
        "flowfile_node_id": job.node_id,
        "model": job.model.result_id,
        "output_column": job.output_column,
    }


def _polars_code_body(job: PolarsCodePayload) -> dict[str, Any]:
    return {
        "code": job.code,
        "inputs": [plan.serialize() for plan in job.inputs],
        "flowfile_flow_id": job.flow_id,
        "flowfile_node_id": job.node_id,
    }


def _sql_query_body(job: SqlQueryPayload) -> dict[str, Any]:
    return {
        "sql_code": job.sql_code,
        "inputs": [plan.serialize() for plan in job.inputs],
        "flowfile_flow_id": job.flow_id,
        "flowfile_node_id": job.node_id,
    }


def _file_write_body(job: FileWritePayload) -> dict[str, Any]:
    settings = job.settings
    return {
        "operation": job.plan.serialize(),
        "data_type": settings.file_type,
        "path": settings.abs_file_path,
        "write_mode": settings.write_mode,
        "sheet_name": settings.sheet_name,
        "delimiter": settings.delimiter,
        "compression": settings.compression,
        "flowfile_flow_id": job.flow_id,
        "flowfile_node_id": job.node_id,
    }


def _random_split_body(job: RandomSplitPayload) -> dict[str, Any]:
    return {
        "operation": _shuffled_rows(job).serialize(),
        "flowfile_flow_id": job.flow_id,
        "flowfile_node_id": job.node_id,
        "splits": [[group.name, group.percentage] for group in job.settings.splits],
    }


V1_OPS: dict[OperationKind, V1Op] = {
    OperationKind.database_read: V1Op(
        DatabaseReadPayload, lambda job: _database_read_settings(job).model_dump(mode="json")
    ),
    OperationKind.kafka_read: V1Op(
        KafkaReadPayload, lambda job: _kafka_read_settings(job).model_dump(mode="json"), metadata=_kafka_offsets
    ),
    OperationKind.google_analytics_read: V1Op(
        GoogleAnalyticsReadPayload, lambda job: _google_analytics_read_settings(job).model_dump(mode="json")
    ),
    OperationKind.rest_api_read: V1Op(
        RestApiReadPayload, lambda job: _rest_api_read_settings(job).model_dump(mode="json")
    ),
    OperationKind.file_read: V1Op(
        FileReadPayload,
        lambda job: _file_to_read(job).model_dump(mode="json"),
        params=lambda job: {"flowfile_flow_id": job.flow_id, "flowfile_node_id": job.node_id},
    ),
    OperationKind.fuzzy_join: V1Op(FuzzyJoinPayload, _fuzzy_join_body),
    OperationKind.random_split: V1Op(
        RandomSplitPayload,
        _random_split_body,
        outputs=lambda job, refs: {group.name: refs[group.name] for group in job.settings.splits},
    ),
    OperationKind.custom_node: V1Op(
        CustomNodePayload,
        lambda job: _custom_node_request(job).model_dump(mode="json"),
        outputs=lambda job, refs: _declared_outputs(refs, job.output_names),
    ),
    OperationKind.ml_train: V1Op(MlTrainPayload, _ml_train_body, metadata=_all_metadata),
    OperationKind.ml_apply: V1Op(MlApplyPayload, _ml_apply_body, held=lambda job: (job.model,)),
    OperationKind.polars_code: V1Op(PolarsCodePayload, _polars_code_body),
    OperationKind.sql_query: V1Op(SqlQueryPayload, _sql_query_body),
    OperationKind.file_write: V1Op(FileWritePayload, _file_write_body),
    OperationKind.database_write: V1Op(
        DatabaseWritePayload, lambda job: json.loads(_database_write_settings(job).model_dump_json())
    ),
    OperationKind.cloud_write: V1Op(
        CloudWritePayload, lambda job: json.loads(_cloud_write_settings(job).model_dump_json())
    ),
}
