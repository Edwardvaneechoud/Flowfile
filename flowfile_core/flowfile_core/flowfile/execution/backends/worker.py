"""The worker backend: core's client of ``flowfile_worker`` over the worker's task routes.

It runs everything through the transport in ``flow_data_engine/subprocess_operations``, against the worker at its
``url``: a plan or an operation becomes a worker task started by the fetcher core uses for it, the status, clear and
cancel routes follow a task, and a stored result stays in the Arrow IPC file the task wrote. Core and the worker
share a disk, so schemas, row counts and samples of a stored result are read from that file here, not sent over the
wire. ``submit`` stores a plan or keeps it deferred in this process; the class docstring has the policy.

With ``settings.FLOWFILE_COMPUTE_PROTOCOL`` on, ``submit``, ``status``, ``cancel`` and ``capabilities`` speak the
worker's versioned ``/v1`` routes instead, and the worker holds what ``submit`` returns, whether stored or deferred:
its schema, row count and samples come over the wire, never from a file core opens. ``submit`` goes over
``WS /v1/ws/submit``, and to ``POST /v1/submit`` only when that socket cannot be opened or the plan not sent. ``run_op``
runs the reads, ``fuzzy_join``, ``random_split``, ``custom_node``, ``ml_train`` and ``ml_apply`` over
``POST /v1/ops/{kind}``, whose outputs the worker holds as it holds a stored submit, a trained model as a blob; the
writes and ``viz_session`` stay on the task routes. ``import_uri`` asks the worker to hold a scan of the URI
(``POST /v1/results/{id}/import``). A result is read and exported over the routes that minted it, whatever the flag
says when it is read: a ``/v1`` result is exported by a child on the worker (``POST /v1/results/{id}/export``), a
task-route result by a worker task running the same child.
A blob, a file of bytes that is not rows, is held by the worker over ``/v1``: ``import_uri(kind="blob")`` has the
worker copy or fetch it and ``export`` has it copy or ``PUT`` it. Over the task routes a blob is a file on the shared
disk, a model a training staged in the flow's cache directory or the path a ``file://`` import names, which ``export``
copies to a ``file://`` URI in core. ``plan_for``, ``sample`` and ``count`` refuse a blob before any call.
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
    RandomSplitPayload,
    RestApiReadPayload,
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
    capabilities_from_answer,
    ensure_handshake,
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
_V1_OPERATIONS = frozenset(
    {
        OperationKind.database_read,
        OperationKind.kafka_read,
        OperationKind.google_analytics_read,
        OperationKind.rest_api_read,
        OperationKind.file_read,
        OperationKind.fuzzy_join,
        OperationKind.random_split,
        OperationKind.custom_node,
        OperationKind.ml_train,
        OperationKind.ml_apply,
    }
)
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

    ``url`` is the base URL of the worker this backend runs on; every transport call it makes goes there. The
    registry's built-in worker backend is made with the URL core's worker settings configure, and a backend made on
    another URL reaches that worker instead. The version handshake is once per process, not per backend: the first
    worker called is checked, and a later call to another one is not.

    ``backend_id`` is the id the backend answers to, ``"worker"`` for the built-in one and ``worker:<name>`` for a
    configured one: the references and job handles it makes carry it, and it refuses those of any other id.
    ``token`` is the worker's own bearer token, which every ``/v1`` request and the ``/v1`` submit socket's upgrade
    carry as ``X-Flowfile-Internal``, and ``capabilities`` too in both flag states; ``None`` (the built-in backend)
    signs them with the process's internal token. The task routes, and the handshake gate in front of them, always
    sign with the process's token. A token that is no header value (``is_header_value``) fails each call it would
    sign as a ``TaskFailed`` naming ``TOKEN_RULE``, before any HTTP or socket client is handed it, so no message, log
    line or traceback carries it.

    ``ca_cert`` is the PEM of the CA to verify an ``https://`` worker's certificate against when it is not public (a
    self-signed worker certificate is its own); the backend hands it to the transport (``trust_ca``) for its ``url``,
    so the task routes, the handshake gate, the ``/v1`` requests, ``capabilities`` and both submit sockets trust it,
    and a worker whose certificate it does not verify is unreachable. ``None`` verifies against the public CAs and
    drops a CA an earlier backend on ``url`` was given, so a worker removed and added again without one is not
    verified against the certificate it was added with before.

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
    task's fetcher; while a ``run_op`` that runs a worker task waits, so does the handle with its payload's
    ``job_id``.

    The worker handshake runs before any call reaches the worker: every worker call goes through the transport's
    worker session or its WebSocket submit, which run it, and the ``/v1`` submit's socket calls ``ensure_handshake``
    before it dials, as the transport's WebSocket submit does. So every method that calls the worker is gated by it
    and can raise its ``VersionMismatch`` or ``ComputeUnavailable``, except ``capabilities``: it is the one read that
    goes around the gate, so a caller can read a worker's versions and compare them with core's itself. The ``/v1``
    requests and the ``/v1`` submit's socket give the gate this backend's ``id``, so its ``ComputeUnavailable`` names
    the backend, as every one this backend makes for a worker it cannot reach does.

    With ``settings.FLOWFILE_COMPUTE_PROTOCOL`` on (read on each call), ``submit`` sends a meta JSON (hints, inputs,
    ids) and the plan over the worker's ``WS /v1/ws/submit``, and posts them to ``POST /v1/submit`` only when the socket
    cannot be opened or the plan not sent. A 426 on the socket's upgrade is ``VersionMismatch`` and nothing is posted;
    once the plan is sent nothing is resent: silence past ``FLOWFILE_WORKER_WS_TIMEOUT`` (each frame resets it) is
    ``TaskFailed`` and a closed socket ``ComputeUnavailable``. The worker applies the same policy and holds the result
    itself, in its own file or as the plan it was sent. The reference carries the worker's columns, read here with
    ``safe_eval_pl_type``, and a ``WorkerLocation`` JSON as ``location``. For such a reference ``sample``, ``count``,
    ``exists`` and ``release`` go to ``/v1/results/{id}`` and ``export`` to ``POST /v1/results/{id}/export``,
    ``plan_for`` decodes the location without a call (a ``pl.scan_ipc`` of the worker's path, or the deferred plan)
    and ``result_file`` names no file. ``import_uri`` posts to ``POST /v1/results/{id}/import``, so the worker holds
    the scan of the URI as a deferred ``/v1`` result, and with ``kind="blob"`` copies a ``file://`` URI or fetches a
    presigned ``https://`` URL into a blob it holds. A reference is read over the routes that minted it, whatever the
    flag says when it is read, and only ``/v1`` references go as a submit's ``inputs``. ``run_op`` runs the reads,
    ``fuzzy_join``, ``random_split``, ``custom_node``, ``ml_train`` and ``ml_apply`` over ``POST /v1/ops/{kind}``
    (``run_op`` has the details), whose refs are ``/v1`` references like a stored submit's; every other operation and
    its results stay on the task routes. ``status`` and ``cancel`` of a job no fetcher waits on use ``/v1/jobs/{id}``.
    Every ``/v1`` request and the socket's upgrade carry ``X-Protocol-Version``. A REST answer, a refused upgrade and
    an ``error`` frame map alike (``_v1_error``): 426 is ``VersionMismatch``, a 404 ``ResultMissing``, and a failed
    child's 500 goes through ``worker_failure``.

    A blob (``ResultRef.kind == "blob"``) has no rows: ``plan_for``, ``sample`` and ``count`` raise
    ``OperationUnsupported`` for one, as ``submit`` does for one among its inputs, before the worker is called, while
    ``exists``, ``release`` and ``export`` take a ``/v1`` one like any ``/v1`` result. A task-route blob is a file on
    the shared disk: a training over the task routes stages its model in the flow's cache directory as worker task
    ``<job_id>_0``, and with the flag off ``import_uri(kind="blob")`` of a ``file://`` URI answers a reference over the
    path, nothing copied. Such a blob ``exists`` while its file is there, ``release`` clears its task (a training's
    file goes, an import's stays), and ``export`` copies the file to a ``file://`` URI in core; a presigned
    ``https://`` URL needs the ``/v1`` routes.
    """

    id = WORKER_BACKEND_ID
    protocol_version = PROTOCOL_VERSION

    def __init__(
        self, url: str, *, backend_id: str = WORKER_BACKEND_ID, token: str | None = None, ca_cert: str | None = None
    ) -> None:
        self.url = url
        self.id = backend_id
        self.token = token
        trust_ca(url, ca_cert)
        self._auth = _TokenAuth(token, backend_id) if token is not None else None
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
            OperationKind.viz_session: self._viz_session,
        }

    def capabilities(self) -> Capabilities:
        """The answer of the worker at ``url`` to ``GET /capabilities``, as the worker declares it.

        With the protocol flag on, the answer to ``GET /v1/capabilities``. Either is one request through the worker
        session around the handshake gate (``gated=False``), signed with this backend's token and carrying its
        ``X-Protocol-Version``. Its ``operations`` are the kinds the worker runs, the ones ``run_op`` sends it.

        Raises:
            ComputeUnavailable: The worker could not be reached, or it answered with no ``Capabilities`` and no
                ``app_version``, so the URL is no Flowfile worker (``capabilities_from_answer``, naming this backend).
            VersionMismatch: With the flag off, the worker has no ``/capabilities`` route, so it predates core; with
                it on, the worker refused this protocol version (426); in either, its answer carries an
                ``app_version`` but is no ``Capabilities``, so it is of another release.
            TaskFailed: This backend's token is no header value (``is_header_value``); nothing was sent.
            ComputeError: Any other answer that is no success (``_v1_failure``): a refused token is a ``TaskFailed``
                naming 401.
        """
        protocol = bool(settings.FLOWFILE_COMPUTE_PROTOCOL)
        try:
            response = _worker_session.get(
                f"{'/v1' if protocol else ''}/capabilities",
                worker_url=self.url,
                gated=False,
                auth=self._auth or _WorkerAuth(),
                headers={wire.PROTOCOL_VERSION_HEADER: str(self.protocol_version)},
                timeout=_WORKER_TIMEOUT,
            )
        except requests.RequestException as e:
            raise ComputeUnavailable(f"Could not reach the worker at {self.url}: {e}", backend_id=self.id) from e
        if response.status_code == 404 and not protocol:
            raise VersionMismatch(
                f"The worker at {self.url} does not report its capabilities (GET /capabilities answered 404), "
                "so it is an older release than core"
            )
        if not response.ok:
            raise _v1_failure(response.status_code, response.content, response.reason)
        return capabilities_from_answer(self.url, response, backend_id=self.id)

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

        With the protocol flag on, the plan goes with the ids of the ``/v1`` results among ``inputs``, which the worker
        checks it still holds, over ``WS /v1/ws/submit``, or to ``POST /v1/submit`` when the socket cannot be opened or
        the plan not sent, and the worker's answer is the result. Once the plan is sent over the socket nothing is
        resent.

        Raises:
            ComputeUnavailable: Over ``/v1``, the worker could not be reached, or its socket closed after the plan was
                sent.
            ForeignRef: An input another backend minted.
            OperationUnsupported: An input is a blob, which a plan cannot read; nothing was sent.
            PlanRejected: The worker could not load the plan.
            ResultMissing: Over ``/v1``, the worker no longer holds an input.
            TaskFailed: The plan failed on the worker, or over ``/v1`` the socket went silent past
                ``FLOWFILE_WORKER_WS_TIMEOUT``.
            VersionMismatch: Over ``/v1``, the worker refused this backend's protocol version, on the socket's upgrade
                or on the REST route.
        """
        for ref in inputs:
            self._table(ref)
        if settings.FLOWFILE_COMPUTE_PROTOCOL:
            with self._lock:
                self._deferred.pop(result_id, None)
            meta = json.dumps(
                {
                    "hints": wire.SubmitHints.model_validate(asdict(hints)).model_dump(),
                    "inputs": [ref.result_id for ref in inputs if self._v1_location(ref) is not None],
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
                    files={"plan": ("plan", plan_bytes, "application/octet-stream")},
                    data={"meta": meta},
                )
                answer = wire.SubmitResult.model_validate_json(response.content)
            return self._from_wire(answer)
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

        - ``database_read``, ``kafka_read``, ``google_analytics_read``, ``rest_api_read``, ``file_read``: ``payload``,
          the kind's dataclass from ``execution/payloads.py``, turned here into the worker read settings its fetcher
          takes (credentials still encrypted, filed under the payload's ``flow_id`` and ``node_id``). The task runs as
          the payload's ``job_id``, so ``cancel(JobHandle("worker", job_id))`` reaches it while it runs. A
          ``kafka_read`` reports the offsets to commit as ``metadata["offsets"]`` when it consumed messages.

        Every other operation takes one dataclass under ``"payload"`` too: the writes ``file_write``,
        ``database_write``, ``cloud_write``, ``delta_write``, ``delta_merge`` and ``delta_scd2``, and ``fuzzy_join``,
        ``random_split``, ``custom_node``, ``ml_train`` and ``ml_apply``. Its frames are serialised here, its
        credentials sent still encrypted, and the worker task is filed under the payload's ``job_id`` (a write's under
        the id the worker gives it), whose fetcher ``cancel(JobHandle("worker", job_id))`` reaches while the call
        waits. ``fuzzy_join`` checks the join and selects and renames each side here, over the sides' schemas its
        payload carries, as the in-process join does, before it sends the sides; ``custom_node`` resolves the
        ``$ffsec$`` ciphertext of the secrets the node's ``SecretSelector`` fields select here and sends only that.

        A read's and ``fuzzy_join``'s and ``ml_apply``'s refs are ``{"result": ref}``, a stored result. A custom
        node's refs are one stored result per declared output, keyed by its output handle (``output-0``,
        ``output-1``, ...). ``random_split`` stores the shuffled rows once, as the payload's ``job_id`` task, and
        returns one deferred ref per split, keyed by the split's name in the settings' order, whose plan slices that
        task's file and which carries the split's row count; releasing a split leaves the shuffled rows in place. A
        write has no refs. ``ml_train``'s refs are ``{"model": ref}``, a blob: the worker task ``<job_id>_0`` stages
        the model at ``ml_models/<job_id>_0.json`` in the flow's cache directory, and ``release`` clears that task and
        its file. ``ml_train`` and the Delta writes return the worker's result dict as ``metadata``: the trained
        model's ``sha256``, ``size_bytes`` and ``model_type``, or the written table's schema, counts, size and version.
        ``ml_apply`` scores with its model's file; a model the task routes do not hold (a ``/v1`` one, or one whose
        file is gone) is ``ResultMissing`` before any task.

        ``viz_session`` takes a ``VizSessionPayload`` and runs no task: it sends the payload's plan, serialised, as a
        ``kind="plan"`` source to the worker's visualisation session named by the payload's ``session_key``, which
        holds the plan lazily in a child and answers each Graphic Walker request over it (``trigger_visualize_fields``
        or ``trigger_visualize_query``), and returns the session's answer as ``metadata`` with no refs.

        With ``settings.FLOWFILE_COMPUTE_PROTOCOL`` on, the five reads, ``fuzzy_join``, ``random_split``,
        ``custom_node``, ``ml_train`` and ``ml_apply`` go to the worker's ``POST /v1/ops/{kind}`` instead: the body is
        the request the kind's fetcher sends (the read settings, the file to read with the flow and node as query
        parameters, the fuzzy join's prepared sides, the custom node's request, a random split's shuffled rows with
        its splits, a training's rows with the model type, the columns and the parameters, an apply's rows with the
        result id of its model and the output column), the payload's ``job_id`` the query parameter the job runs
        under, so ``cancel(JobHandle("worker", job_id))`` reaches it through ``POST /v1/jobs/{job_id}/cancel`` and
        ``status`` follows it. The refs are keyed as on the task routes, but every one is a stored ``/v1`` result
        ``<job_id>_<i>``, read over ``/v1/results/{id}`` like a stored submit's, its columns and row count as the worker
        answered them: a random split's splits are each a file of their own, releasing one output of a custom node or
        a split leaves the others, and a training's model is the blob ``<job_id>_0``. A ``kafka_read`` takes its
        offsets, and an ``ml_train`` the model's digest, size and type, from the answer's ``metadata``. A failure maps
        as every ``/v1`` answer does: an ``ml_apply`` whose model the worker no longer holds is ``ResultMissing``.

        Raises:
            ComputeUnavailable: The worker could not be reached for a ``viz_session`` request.
            ForeignRef: An ``ml_apply`` model another backend minted; nothing was sent.
            OperationUnsupported: A custom node without a node file.
            PlanRejected: The worker could not load the operation's plan.
            ResultMissing: An ``ml_apply`` model this backend no longer holds.
            RuntimeError: The worker answered a ``viz_session`` request with an error status.
            TaskFailed: The operation failed on the worker.
            ValueError: The worker did not return one of a custom node's declared outputs.
        """
        operation = self._operations.get(op)
        if operation is None:
            raise OperationUnsupported(f"The worker backend does not run the {op.value} operation")
        if settings.FLOWFILE_COMPUTE_PROTOCOL and op in _V1_OPERATIONS:
            return self._v1_op(op, payload)
        return operation(payload)

    def sample(self, ref: ResultRef, n: int) -> bytes:
        """The first ``n`` rows of ``ref``, or all of them when it has fewer, as an Arrow IPC stream.

        A stored result's rows are read from its file; a deferred one's come from a worker sampler task over its
        plan, filed under ``ref.result_id`` like the sample ``submit`` took. A ``/v1`` result's are the worker's
        answer to ``GET /v1/results/{id}/sample``. A blob is refused with ``OperationUnsupported`` before any call.
        """
        self._table(ref)
        if self._v1_location(ref) is not None:
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
        if self._v1_location(ref) is not None:
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
        if self._v1_location(ref) is not None:
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
        if self._v1_location(ref) is not None:
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

        A stored result is checked on the shared disk, not with the worker: the scan needs the file, and clearing
        the task deletes it.

        A ``/v1`` result's location is decoded without a call: a scan of the path the worker wrote, which core does
        not open, or the deferred plan the worker was sent. A plan composed on a result the worker has released fails
        when it is submitted, which names it among its inputs.

        Raises:
            ForeignRef: Another backend minted ``ref``.
            OperationUnsupported: ``ref`` is a blob, which has no rows to plan over.
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

        A stored result's own file, or the file of a deferred result's last sampler task; ``None`` for a deferred
        result no sample was taken of, or one this backend no longer holds. Core reads the first rows of a result
        through it and treats a file that vanished as a result to compute again. ``None`` for a ``/v1`` result, whose
        file core never opens.
        """
        if self._v1_location(ref) is not None:
            return None
        if ref.state == "deferred":
            self._check(ref)
            with self._lock:
                held = self._deferred.get(ref.result_id)
            return held.sample_file if held is not None else None
        return self._file(ref)

    def export(self, ref: ResultRef, uri: str, credential: CredentialRef | None) -> str:
        """Write ``ref``'s rows to ``uri`` as parquet where they are held, and return ``uri``.

        A ``/v1`` result is exported by the worker: ``POST /v1/results/{id}/export`` runs a child that sinks the
        result's plan to ``uri``. A task-route result is exported by a worker task running that child,
        ``export_result``, over ``plan_for(ref)``, the scan of its file or the plan this backend holds, as ``sample``
        and ``count`` send a deferred plan to a task; nothing of it runs in this process. Either way ``credential``
        crosses as the connection's id and its user's, resolved in the child. A result is exported over the routes
        that minted it, whatever the flag says.

        A ``/v1`` blob goes to the same ``/v1`` route, where the worker copies its file to a ``file://`` URI or
        ``PUT``s it to a presigned ``https://`` URL; such a URL carries its own authorisation, so a blob takes no
        credential. A task-route blob is a file on the shared disk, which core copies to a ``file://`` URI
        (``put_blob``); a presigned URL needs the ``/v1`` routes.

        Raises:
            ComputeUnavailable: The worker could not be reached.
            ForeignRef: Another backend minted ``ref``.
            OperationUnsupported: ``ref`` is a task-route blob and ``uri`` is not ``file://``; nothing was sent.
            OSError: The copy of a task-route blob failed.
            ResultMissing: The worker, or this backend, no longer holds ``ref``; for a task-route blob, its file is
                gone, found before the copy.
            TaskFailed: The worker's sink or copy failed, or it refused the URI's scheme for a blob (422).
            ValueError: ``ref`` is a blob and ``credential`` is set; nothing was sent.
        """
        location = self._v1_location(ref)
        if ref.kind == "blob" and credential is not None:
            raise ValueError("A blob is copied to a URI that carries its own authorisation; it takes no credential")
        if location is not None:
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
        """Hold a scan of the parquet at ``uri`` as a deferred result with the ``schema`` and ``row_count`` given.

        Nothing is read: the schema and the count are the caller's. With the protocol flag on the worker builds and
        holds the scan under a fresh id (``POST /v1/results/{id}/import``), ``credential`` crossing as the
        connection's id and its user's, and the answer is a ``/v1`` reference whose ``plan_for`` decodes the scan
        without a call. With it off the scan is held in this process like every deferred plan, a ``file:`` URI
        scanned as the local path it names. ``release`` forgets the scan and leaves the file.

        With ``kind="blob"`` and the flag on, the same route has the worker copy the bytes at a ``file://`` URI or
        fetch a presigned ``https://`` URL into a blob it holds, before it answers; the reference is a materialised
        blob with no columns and no row count, and ``release`` removes the worker's copy. With the flag off a
        ``file://`` URI answers such a reference over the local path it names, as ``file_uri_path`` reads it:
        nothing is copied and no call is made, and ``release`` leaves the file.

        Raises:
            ComputeUnavailable: Over ``/v1``, the worker could not be reached.
            OperationUnsupported: A blob with the protocol flag off whose URI is not ``file://``; nothing was sent.
            TaskFailed: The worker refused the URI's scheme for a blob (422), or its copy or fetch failed.
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
        """Cancel the task ``handle`` names: through its fetcher while ``submit`` waits on it, else on the worker.

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

    def _v1(
        self, method: str, path: str, *, missing_ok: bool = False, waits: bool = False, **kwargs: Any
    ) -> requests.Response:
        """Send one request to the worker's ``/v1`` route ``path`` with this backend's protocol version and token.

        ``waits`` is for a call that waits on a worker child (a submit, a sample, a count): its read timeout is the
        deadline the task routes' poll honours. A 2xx answer is returned, a 404 too with ``missing_ok``. The request
        goes through the worker session, so the handshake gates it; this backend's token, when it has one, signs it
        in place of the session's process token.

        Raises:
            ComputeUnavailable: The worker could not be reached.
            TaskFailed: This backend's token is no header value (``is_header_value``); nothing was sent.
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
        except requests.RequestException as e:
            raise ComputeUnavailable(f"Could not reach the worker at {self.url}: {e}", backend_id=self.id) from e
        if response.ok or (missing_ok and response.status_code == 404):
            return response
        raise _v1_failure(response.status_code, response.content, response.reason)

    def _v1_socket_submit(self, meta: str, plan: bytes, result_id: str) -> wire.SubmitResult | None:
        """The worker's answer to a submit over ``WS /v1/ws/submit``, or ``None`` for ``submit`` to post it instead.

        ``None`` means the socket could not be opened or the plan not sent. The worker reads both frames before it runs
        anything and drops a socket that closed before the plan, so the post never runs a plan twice. The socket is
        dialled after ``ensure_handshake``, with this backend's token (``_internal_token``), its ``X-Protocol-Version``,
        no frame-size cap (a deferred result's ``complete`` frame carries its plan) and the CA the transport trusts for
        ``url`` (``ssl_context_for``; ``None`` verifies a ``wss://`` worker against the public CAs); a connect that
        fails at the network re-arms the handshake, as the transport's WebSocket submit does. Once the plan is sent the
        answer is the final frame's, and nothing is resent.

        Raises:
            ComputeUnavailable: The socket closed after the plan was sent.
            TaskFailed: The worker sent nothing for longer than ``FLOWFILE_WORKER_WS_TIMEOUT``, or this backend's
                token is no header value (``is_header_value``), found before the socket is dialled.
            VersionMismatch: The worker refused the upgrade with 426.
            ComputeError: The handshake failed, or the worker's ``error`` frame (``_v1_error``).
        """
        ensure_handshake(self.url, backend_id=self.id)
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
            )
        except OSError as e:
            reset_handshake()
            logger.debug(f"The /v1 submit socket to {self.url} could not be opened ({e}); posting the plan instead")
            return None
        except Exception as e:
            if isinstance(e, InvalidStatus) and e.response.status_code == 426:
                raise _v1_failure(426, bytes(e.response.body), e.response.reason_phrase) from e
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
            frame = _final_frame(ws, self.url, result_id)
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

    def _v1_location(self, ref: ResultRef) -> wire.WorkerLocation | None:
        """The ``WorkerLocation`` of a result ``/v1`` minted; ``None`` for a task-route result.

        A task-route result's location is a file path or its result id, never JSON.

        Raises:
            ForeignRef: Another backend minted ``ref``.
        """
        self._check(ref)
        if not ref.location.startswith(b"{"):
            return None
        return wire.WorkerLocation.model_validate_json(ref.location)

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

    def _v1_op(self, op: OperationKind, payload: OpPayload) -> OpResult:
        """Run ``op`` over the worker's ``POST /v1/ops/{kind}`` and key its refs as the task-route method does.

        The payload is unpacked before anything is sent, so an empty one is a ``ValueError`` here too. A training's
        answer ``metadata`` is the model's digest, size and type.

        Raises:
            ForeignRef: An ``ml_apply`` model another backend minted.
            OperationUnsupported: A custom node without a node file.
            ValueError: The worker did not return one of a custom node's declared outputs.
        """
        params: dict[str, Any] = {}
        match op:
            case OperationKind.database_read:
                job = unpack(payload, DatabaseReadPayload)
                body = _database_read_settings(job).model_dump(mode="json")
            case OperationKind.kafka_read:
                job = unpack(payload, KafkaReadPayload)
                body = _kafka_read_settings(job).model_dump(mode="json")
            case OperationKind.google_analytics_read:
                job = unpack(payload, GoogleAnalyticsReadPayload)
                body = _google_analytics_read_settings(job).model_dump(mode="json")
            case OperationKind.rest_api_read:
                job = unpack(payload, RestApiReadPayload)
                body = _rest_api_read_settings(job).model_dump(mode="json")
            case OperationKind.file_read:
                job = unpack(payload, FileReadPayload)
                body = _file_to_read(job).model_dump(mode="json")
                params = {"flowfile_flow_id": job.flow_id, "flowfile_node_id": job.node_id}
            case OperationKind.fuzzy_join:
                job = unpack(payload, FuzzyJoinPayload)
                left, right, fuzzy_maps = _fuzzy_sides(job)
                body = FuzzyJoinInput(
                    left_df_operation=PolarsOperation(operation=left.serialize()),
                    right_df_operation=PolarsOperation(operation=right.serialize()),
                    fuzzy_maps=fuzzy_maps,
                    task_id=job.job_id,
                    flowfile_flow_id=job.flow_id,
                    flowfile_node_id=job.node_id,
                ).model_dump(mode="json")
            case OperationKind.custom_node:
                job = unpack(payload, CustomNodePayload)
                body = _custom_node_request(job).model_dump(mode="json")
            case OperationKind.ml_train:
                job = unpack(payload, MlTrainPayload)
                body = {
                    "operation": base64.b64encode(job.plan.serialize()).decode("ascii"),
                    "flowfile_flow_id": job.flow_id,
                    "flowfile_node_id": job.node_id,
                    "model_type": job.model_type,
                    "target_column": job.target_column,
                    "feature_columns": job.feature_columns,
                    "params": job.params,
                }
            case OperationKind.ml_apply:
                job = unpack(payload, MlApplyPayload)
                self._check(job.model)
                body = {
                    "operation": base64.b64encode(job.plan.serialize()).decode("ascii"),
                    "flowfile_flow_id": job.flow_id,
                    "flowfile_node_id": job.node_id,
                    "model": job.model.result_id,
                    "output_column": job.output_column,
                }
            case _:
                job = unpack(payload, RandomSplitPayload)
                body = {
                    "operation": base64.b64encode(_shuffled_rows(job).serialize()).decode("ascii"),
                    "flowfile_flow_id": job.flow_id,
                    "flowfile_node_id": job.node_id,
                    "splits": [[group.name, group.percentage] for group in job.settings.splits],
                }
        answer = self._v1(
            "POST", f"/ops/{op.value}", waits=True, params={"job_id": job.job_id, **params}, json=body
        ).json()
        refs = {
            name: self._ref_from_wire(wire.ResultRef.model_validate(raw)) for name, raw in answer["outputs"].items()
        }
        if op is OperationKind.custom_node:
            refs = _declared_outputs(refs, job.output_names)
        elif op is OperationKind.random_split:
            refs = {group.name: refs[group.name] for group in job.settings.splits}
        if op is OperationKind.kafka_read:
            metadata = _kafka_offsets(answer["metadata"])
        else:
            metadata = answer["metadata"] if op is OperationKind.ml_train else {}
        return OpResult(refs=refs, metadata=metadata)

    def _store(self, plan: pl.LazyFrame, result_id: str, flow_id: int, node_id: int | str) -> ResultRef:
        """Run ``plan`` as worker store task ``result_id`` and return the result it wrote."""
        with self._lock:
            self._deferred.pop(result_id, None)
        plan_bytes = plan.serialize()
        fetcher = self._run_tracked(
            result_id,
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
        model = apply.model
        if self._v1_location(model) is not None or not os.path.isfile(model.location.decode()):
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
        reference = write.connection if write.settings.database_write_settings.connection_mode == "reference" else None
        settings = DatabaseExternalWriteSettings.create_from_from_node_database_writer(
            node_database_writer=write.settings,
            password=write.encrypted_password,
            table_name=write.table_name,
            lf=write.plan,
            database_reference_settings=reference,
        ).model_copy(update={"flowfile_flow_id": write.flow_id, "flowfile_node_id": write.node_id})
        self._run_tracked(
            write.job_id, lambda: ExternalDatabaseWriter(settings, wait_on_completion=False, worker_url=self.url)
        )
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

    ``on_started`` receives the fetcher once the task is submitted, before the wait. A worker that cannot be reached
    while the task is started is a ``ComputeUnavailable`` naming ``backend_id``, except one the handshake gate
    raised for ``start``'s call, which carries a URL only and so names none. A failed task raises a typed
    error: error code -1 goes through ``worker_failure``, whose ``error_kind`` tells a plan the worker could not
    load (``PlanRejected``) from a lost connection and a killed child; every other code is a ``TaskFailed`` with
    the fetcher's description, raised from the fetcher's ``RemoteExecutionError`` so the worker-side class
    stays recoverable from the chain.
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
            raise worker_failure(fetcher.error_kind, description) from e
        raise TaskFailed(description) from e
    return fetcher


def _final_frame(ws: ClientConnection, worker_url: str, result_id: str) -> dict[str, Any]:
    """The first ``complete`` or ``error`` frame the worker sends on the ``/v1`` submit socket ``ws``.

    Progress frames are read past. Every frame resets the silence clock that the task routes' socket keeps too:
    nothing received for longer than ``FLOWFILE_WORKER_WS_TIMEOUT`` (0 disables) means the worker is presumed wedged.

    Raises:
        ComputeUnavailable: The socket closed before a final frame.
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
            raise ComputeUnavailable(
                f"The worker at {worker_url} closed the connection before it answered submit {result_id}: {e}"
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


def _v1_error(status_code: int, detail: Any) -> ComputeError:
    """The typed error for a ``/v1`` failure the worker answered with ``status_code`` and ``detail``.

    A REST answer, a refused socket upgrade and an ``error`` frame on the socket all come here. 426 is a protocol
    version the worker refuses, 404 a result or input it does not hold, and a 500 carrying a child's ``status``,
    ``error_message`` and ``error_kind`` goes through ``worker_failure`` as a failed task does. Any other code (a token
    the worker refuses, a meta it cannot read) is a ``TaskFailed`` naming the code.
    """
    if status_code == 426:
        return VersionMismatch(str(detail))
    if status_code == 404:
        return ResultMissing(str(detail))
    if status_code == 500 and isinstance(detail, dict) and "status" in detail:
        return worker_failure(detail.get("error_kind"), detail.get("error_message") or detail["status"])
    return TaskFailed(f"The worker answered {status_code}: {detail}")


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

    The provider carries the stored connection's id and user only, and resolves it where the plan runs; the options
    (``credential_ref_kwargs``, read from core's catalog, no secret among them) carry the connection's endpoint and
    region, so an S3-compatible store is reached. Without a credential Polars resolves the target as it does for any
    scan.
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
