"""Core's transport to a worker: the REST calls, the task fetchers and the worker session they go through.

Nothing here knows which worker to call. Every function and fetcher takes ``worker_url``, the worker's base URL,
from its caller: ``WorkerBackend`` passes its own ``url``, and a caller outside it that sends work to the worker
asks the backend registry for it (``registry.worker_url()``). Whether to call the worker at all is the caller's
decision too.

A worker whose TLS certificate is not public is trusted through ``trust_ca``, which the backend made for it calls with
its configured CA certificate: the worker session then verifies that worker's URL against it, and both WebSocket clients
take it from ``ssl_context_for``. Every other worker is verified against the public CAs.
"""

import io
import json
import os
import ssl
import threading
from base64 import b64decode
from time import monotonic
from typing import Any, Literal
from uuid import uuid4

import polars as pl
import requests
from pl_fuzzy_frame_match.models import FuzzyMapping
from requests.adapters import HTTPAdapter
from websockets.exceptions import ConnectionClosed

from flowfile_core.auth.jwt import get_internal_token
from flowfile_core.configs import logger
from flowfile_core.flowfile.execution.errors import ComputeError, ComputeUnavailable
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.handshake import (
    ensure_handshake,
    fetch_worker_capabilities,
    reset_handshake,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import (
    ApplyModelInput,
    CustomNodeExecuteInput,
    FuzzyJoinInput,
    OperationType,
    PolarsOperation,
    Status,
    TrainModelInput,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.streaming import (
    WorkerStreamInterrupted,
    WorkerTaskError,
    error_code_for_kind,
    streaming_receive,
    streaming_start,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.models import (
    DatabaseExternalReadSettings,
    DatabaseExternalWriteSettings,
)
from flowfile_core.schemas.catalog_schema import CatalogTablePreview, DeltaTableHistory
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageWriteSettingsWorkerInterface
from flowfile_core.schemas.input_schema import ReceivedTable
from shared.compute_protocol import Capabilities
from shared.viz_protocol import HTTP_TIMEOUT_SECONDS


class WorkerAuthUnavailable(requests.RequestException):
    """No internal token can be resolved, so the worker is unreachable for this process.

    Raised as a ``RequestException`` so every caller that already degrades on an
    unreachable worker (cache probes, catalog helpers, local-execution runs in a
    mode that mints no token) treats it the same way instead of crashing.
    """


class _WorkerAuth(requests.auth.AuthBase):
    """Signs every worker call with the shared internal token, resolved per
    request so nothing mints at import time."""

    def __call__(self, r):
        try:
            r.headers["X-Flowfile-Internal"] = get_internal_token()
        except ValueError as exc:
            raise WorkerAuthUnavailable(str(exc), request=r) from exc
        return r


class _WorkerSession(requests.Session):
    """The session every REST call from core to a worker goes through.

    A call names the worker it is for with the keyword ``worker_url``, the worker's base URL, and the route on it
    as its path (``_worker_session.get("/status/abc", worker_url=...)``); the module never holds a worker URL of
    its own, its callers pass the one of the backend they serve. Each call first passes ``ensure_handshake``, which
    is once per process, not per worker: the first call's worker is refused before it is sent anything when its
    versions differ from core's, and later calls pass whichever worker they name. A call that cannot reach the
    worker re-arms that check, because the worker that comes back may be another one. A caller that knows which
    backend the worker is names it with the keyword ``backend_id``, which the check's ``ComputeUnavailable`` names.
    ``gated=False`` sends a call around the check, neither passing nor re-arming it: the gate's own
    ``GET /capabilities`` and ``WorkerBackend.capabilities``, which read a worker's versions to compare them.
    """

    def request(
        self, method, path, *args, worker_url: str, backend_id: str | None = None, gated: bool = True, **kwargs
    ):
        if gated:
            ensure_handshake(worker_url, backend_id=backend_id)
        try:
            return super().request(method, f"{worker_url}{path}", *args, **kwargs)
        except requests.ConnectionError:
            if gated:
                reset_handshake()
            raise


class _TrustedCa(HTTPAdapter):
    """Verifies a worker's certificate against the CA core was given for it, in place of the public CAs."""

    def __init__(self, context: ssl.SSLContext) -> None:
        self._context = context
        super().__init__()

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self._context
        return super().init_poolmanager(*args, **kwargs)


_worker_session = _WorkerSession()
_worker_session.auth = _WorkerAuth()

_trusted_cas: dict[str, ssl.SSLContext] = {}


def trust_ca(worker_url: str, ca_cert: str) -> None:
    """Verify the worker at ``worker_url`` against ``ca_cert``, a PEM, from now on, in place of the public CAs.

    The PEM is the CA that signed the worker's certificate, or a self-signed certificate itself. The worker session
    uses it for every call under ``worker_url``, and ``ssl_context_for`` hands it to both WebSocket clients; a later
    call for the same URL replaces it. Nothing is written to disk.

    Raises:
        ssl.SSLError: ``ca_cert`` holds no PEM certificate.
    """
    context = ssl.create_default_context(cadata=ca_cert)
    _trusted_cas[worker_url] = context
    _worker_session.mount(f"{worker_url}/", _TrustedCa(context))


def ssl_context_for(worker_url: str) -> ssl.SSLContext | None:
    """The context ``trust_ca`` made for ``worker_url``; ``None`` for a worker verified against the public CAs."""
    return _trusted_cas.get(worker_url)


_JSON_HEADERS = {"Content-Type": "application/json"}

# (connect, read) timeout for the short worker control calls so a dead/wedged
# worker surfaces promptly instead of hanging a request thread indefinitely.
_WORKER_TIMEOUT = (5, 30)

# Generous so large offloads aren't killed; 0 disables. Env-tunable.
_POLL_DEADLINE_SECONDS = float(os.getenv("FLOWFILE_WORKER_POLL_TIMEOUT", "3600"))


def trigger_df_operation(
    flow_id: int,
    node_id: int | str,
    plan_bytes: bytes,
    file_ref: str,
    operation_type: OperationType = "store",
    kwargs: dict | None = None,
    *,
    worker_url: str,
) -> Status:
    """Submit task ``file_ref`` running ``plan_bytes``, a ``pl.LazyFrame.serialize()`` output, sent as is."""
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Task-Id": file_ref,
        "X-Operation-Type": operation_type,
        "X-Flow-Id": str(flow_id),
        "X-Node-Id": str(node_id),
    }
    if kwargs:
        headers["X-Kwargs"] = json.dumps(kwargs)
    v = _worker_session.post(
        "/submit_query/", worker_url=worker_url, data=plan_bytes, headers=headers, timeout=_WORKER_TIMEOUT
    )
    if not v.ok:
        raise Exception(f"trigger_df_operation: Could not cache the data, {v.text}")
    return Status(**v.json())


def trigger_sample_operation(
    lf: pl.LazyFrame, file_ref: str, flow_id: int, node_id: str | int, sample_size: int = 100, *, worker_url: str
) -> Status:
    # Send raw bytes directly - no base64 encoding overhead
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Task-Id": file_ref,
        "X-Operation-Type": "store_sample",
        "X-Sample-Size": str(sample_size),
        "X-Flow-Id": str(flow_id),
        "X-Node-Id": str(node_id),
    }
    v = _worker_session.post(
        "/store_sample/", worker_url=worker_url, data=lf.serialize(), headers=headers, timeout=_WORKER_TIMEOUT
    )
    if not v.ok:
        raise Exception(f"trigger_sample_operation: Could not cache the data, {v.text}")
    return Status(**v.json())


def trigger_fuzzy_match_operation(
    left_df: pl.LazyFrame,
    right_df: pl.LazyFrame,
    fuzzy_maps: list[FuzzyMapping],
    file_ref: str,
    flow_id: int,
    node_id: int | str,
    *,
    worker_url: str,
) -> Status:
    # Use raw bytes - Pydantic will handle single base64 encoding for JSON transport
    left_serializable_object = PolarsOperation(operation=left_df.serialize())
    right_serializable_object = PolarsOperation(operation=right_df.serialize())
    fuzzy_join_input = FuzzyJoinInput(
        left_df_operation=left_serializable_object,
        right_df_operation=right_serializable_object,
        fuzzy_maps=fuzzy_maps,
        task_id=file_ref,
        flowfile_flow_id=flow_id,
        flowfile_node_id=node_id,
    )
    v = _worker_session.post(
        "/add_fuzzy_join", worker_url=worker_url, data=fuzzy_join_input.model_dump_json(), headers=_JSON_HEADERS
    )
    if not v.ok:
        raise Exception(f"trigger_fuzzy_match_operation: Could not cache the data, {v.text}")
    return Status(**v.json())


def trigger_custom_node_operation(request: CustomNodeExecuteInput, *, worker_url: str) -> Status:
    v = _worker_session.post(
        "/execute_custom_node",
        worker_url=worker_url,
        data=request.model_dump_json(),
        headers=_JSON_HEADERS,
    )
    if not v.ok:
        raise Exception(f"trigger_custom_node_operation: Could not start the custom node, {v.text}")
    return Status(**v.json())


def trigger_train_model_operation(
    lf: pl.LazyFrame,
    staging_path: str,
    model_type: str,
    target_column: str,
    feature_columns: list[str],
    params: dict[str, Any],
    file_ref: str,
    flow_id: int,
    node_id: int | str,
    *,
    worker_url: str,
) -> Status:
    """Submit a training job to the worker.

    The worker writes the model bytes to *staging_path*; the resulting Status's
    ``results`` field carries ``{sha256, size_bytes, model_type}`` once complete.
    """
    payload = TrainModelInput(
        df_operation=PolarsOperation(operation=lf.serialize()),
        model_type=model_type,
        target_column=target_column,
        feature_columns=feature_columns,
        params=params,
        staging_path=staging_path,
        task_id=file_ref,
        flowfile_flow_id=flow_id,
        flowfile_node_id=node_id,
    )
    v = _worker_session.post(
        "/train_ml_model", worker_url=worker_url, data=payload.model_dump_json(), headers=_JSON_HEADERS
    )
    if not v.ok:
        raise Exception(f"trigger_train_model_operation: Could not start training, {v.text}")
    return Status(**v.json())


def trigger_apply_model_operation(
    lf: pl.LazyFrame,
    model_path: str,
    output_column: str,
    file_ref: str,
    flow_id: int,
    node_id: int | str,
    *,
    worker_url: str,
) -> Status:
    """Submit an apply-model job to the worker."""
    payload = ApplyModelInput(
        df_operation=PolarsOperation(operation=lf.serialize()),
        model_path=model_path,
        output_column=output_column,
        task_id=file_ref,
        flowfile_flow_id=flow_id,
        flowfile_node_id=node_id,
    )
    v = _worker_session.post(
        "/apply_ml_model", worker_url=worker_url, data=payload.model_dump_json(), headers=_JSON_HEADERS
    )
    if not v.ok:
        raise Exception(f"trigger_apply_model_operation: Could not start scoring, {v.text}")
    return Status(**v.json())


def trigger_create_operation(
    flow_id: int,
    node_id: int | str,
    received_table: ReceivedTable,
    file_type: str = Literal["csv", "parquet", "json", "excel", "ipc", "ndjson", "avro", "ipc_stream"],
    *,
    worker_url: str,
):
    f = _worker_session.post(
        f"/create_table/{file_type}",
        worker_url=worker_url,
        data=received_table.model_dump_json(),
        headers=_JSON_HEADERS,
        params={"flowfile_flow_id": flow_id, "flowfile_node_id": node_id},
    )
    if not f.ok:
        raise Exception(f"trigger_create_operation: Could not cache the data, {f.text}")
    return Status(**f.json())


def trigger_database_read_collector(database_external_read_settings: DatabaseExternalReadSettings, *, worker_url: str):
    f = _worker_session.post(
        "/store_database_read_result",
        worker_url=worker_url,
        data=database_external_read_settings.model_dump_json(),
        headers=_JSON_HEADERS,
    )
    if not f.ok:
        raise Exception(f"trigger_database_read_collector: Could not cache the data, {f.text}")
    return Status(**f.json())


def trigger_kafka_read(kafka_read_settings, *, worker_url: str) -> Status:
    """Send a Kafka read request to the worker service."""
    f = _worker_session.post(
        "/store_kafka_read_result",
        worker_url=worker_url,
        data=kafka_read_settings.model_dump_json(),
        headers=_JSON_HEADERS,
    )
    if not f.ok:
        raise Exception(f"trigger_kafka_read: Could not read from Kafka, {f.text}")
    return Status(**f.json())


def fetch_kafka_offsets(task_id: str, *, worker_url: str) -> dict | None:
    """Fetch deferred Kafka offset data from the worker for a completed task.

    Returns a dict with ``new_offsets``, ``messages_consumed``, etc. from the
    KafkaReadResult that was saved as a sidecar file, or ``None`` if no
    offsets were recorded (e.g. empty topic).
    """
    f = _worker_session.get(f"/kafka_offsets/{task_id}", worker_url=worker_url)
    if not f.ok:
        logger.warning("Failed to fetch Kafka offsets for task %s: %s", task_id, f.text)
        return None
    data = f.json()
    return data


def trigger_google_analytics_read(ga_read_settings, *, worker_url: str) -> Status:
    """Send a Google Analytics 4 read request to the worker service."""
    f = _worker_session.post(
        "/store_google_analytics_read_result",
        worker_url=worker_url,
        data=ga_read_settings.model_dump_json(),
        headers=_JSON_HEADERS,
    )
    if not f.ok:
        raise Exception(f"trigger_google_analytics_read: Could not read from GA, {f.text}")
    return Status(**f.json())


def trigger_rest_api_read(settings, *, worker_url: str) -> Status:
    """Send a REST API read request to the worker service."""
    f = _worker_session.post(
        "/store_rest_api_read_result",
        worker_url=worker_url,
        data=settings.model_dump_json(),
        headers=_JSON_HEADERS,
        timeout=_WORKER_TIMEOUT,
    )
    if not f.ok:
        raise Exception(f"trigger_rest_api_read: Could not read from the REST API, {f.text}")
    return Status(**f.json())


def trigger_database_write(database_external_write_settings: DatabaseExternalWriteSettings, *, worker_url: str):
    f = _worker_session.post(
        "/store_database_write_result",
        worker_url=worker_url,
        data=database_external_write_settings.model_dump_json(),
        headers=_JSON_HEADERS,
    )
    if not f.ok:
        raise Exception(f"trigger_database_write: Could not cache the data, {f.text}")
    return Status(**f.json())


def trigger_cloud_storage_write(
    database_external_write_settings: CloudStorageWriteSettingsWorkerInterface, *, worker_url: str
):
    f = _worker_session.post(
        "/write_data_to_cloud",
        worker_url=worker_url,
        data=database_external_write_settings.model_dump_json(),
        headers=_JSON_HEADERS,
    )
    if not f.ok:
        raise Exception(f"trigger_cloud_storage_write: Could not cache the data, {f.text}")
    return Status(**f.json())


def trigger_write_output(
    lf: pl.LazyFrame,
    data_type: str,
    path: str,
    write_mode: str,
    flow_id: int,
    node_id: int | str,
    sheet_name: str | None = None,
    delimiter: str | None = None,
    compression: str | None = None,
    *,
    worker_url: str,
) -> Status:
    from base64 import encodebytes

    serializable_df = lf.serialize()
    r = _worker_session.post(
        "/write_results/",
        worker_url=worker_url,
        json={
            "operation": encodebytes(serializable_df).decode(),
            "data_type": data_type,
            "path": path,
            "write_mode": write_mode,
            "sheet_name": sheet_name,
            "delimiter": delimiter,
            "compression": compression,
            "flowfile_node_id": node_id,
            "flowfile_flow_id": flow_id,
        },
    )
    if not r.ok:
        raise Exception(f"trigger_write_output: Could not write the data, {r.text}")
    return Status(**r.json())


def trigger_catalog_materialize(
    source_file_path: str,
    table_name: str | None = None,
    storage: dict | None = None,
    *,
    worker_url: str,
):
    payload = {
        "source_file_path": source_file_path,
        "table_name": table_name,
    }
    if storage is not None:
        payload["storage"] = storage
    response = _worker_session.post("/catalog/materialize", worker_url=worker_url, json=payload)
    return response


def trigger_resolve_virtual_table(
    table_id: int,
    plan_bytes: bytes,
    source_versions_hash: str,
    target: str = "virtual_results",
    *,
    worker_url: str,
) -> dict:
    """Ask the worker to materialise a flow-virtual table to its IPC cache.

    Ships *plan_bytes* (output of ``pl.LazyFrame.serialize()``); the worker
    deserialises in a spawned child, collects, and writes IPC. Idempotent on
    ``(table_id, source_versions_hash)``. *target* selects the destination
    directory: ``"virtual_results"`` (worker-private cache, the default) or
    ``"kernel_shared"`` (kernel-mounted shared volume).
    """
    from base64 import b64encode

    payload = {
        "table_id": table_id,
        "plan_bytes": b64encode(plan_bytes).decode("ascii"),
        "source_versions_hash": source_versions_hash,
        "target": target,
    }
    response = _worker_session.post("/flow/resolve_virtual_table", worker_url=worker_url, json=payload, timeout=300)
    if not response.ok:
        raise RuntimeError(f"Worker resolve_virtual_table failed: {response.text}")
    return response.json()


def trigger_sql_query(
    query: str,
    tables: dict[str, str],
    max_rows: int = 10_000,
    virtual_refs: dict[str, str] | None = None,
    storage: dict | None = None,
    *,
    worker_url: str,
) -> dict:
    """Ask the worker to execute a SQL query against catalog tables.

    *tables* is a mapping of logical table name -> directory name.
    *virtual_refs* is an optional mapping of virtual table name -> bare IPC
    filename under the worker's catalog_virtual_results directory.
    *storage* (when set) routes the physical-table scans to object storage.
    Returns the parsed JSON response dict.
    """
    payload: dict = {"query": query, "tables": tables, "max_rows": max_rows}
    if virtual_refs:
        payload["virtual_refs"] = virtual_refs
    if storage is not None:
        payload["storage"] = storage
    response = _worker_session.post("/catalog/sql_query", worker_url=worker_url, json=payload)
    if not response.ok:
        raise RuntimeError(f"Worker SQL query execution failed: {response.text}")
    return response.json()


def trigger_visualize_query(worker_source: dict, payload: dict, max_rows: int, *, worker_url: str) -> dict:
    """Ask the worker to compute a Graphic Walker chart payload.

    *worker_source* is a dict matching the worker's ``VizWorkerSource`` model.
    The worker maintains a per-source LazyFrame cache keyed on
    ``worker_source["session_key"]`` so successive calls on the same source
    skip the load step.
    """
    session_key = worker_source.get("session_key")
    logger.info(
        "[viz] -> worker /catalog/visualize_query session_key=%s kind=%s payload_keys=%s max_rows=%d",
        session_key,
        worker_source.get("kind"),
        list(payload.keys()),
        max_rows,
    )
    body = {"source": worker_source, "payload": payload, "max_rows": max_rows}
    response = _worker_session.post(
        "/catalog/visualize_query", worker_url=worker_url, json=body, timeout=HTTP_TIMEOUT_SECONDS
    )
    if not response.ok:
        logger.warning(
            "[viz] <- worker /catalog/visualize_query session_key=%s status=%d body=%s",
            session_key,
            response.status_code,
            response.text[:300],
        )
        raise RuntimeError(f"Worker visualize_query failed: {response.text}")
    data = response.json()
    logger.info(
        "[viz] <- worker /catalog/visualize_query session_key=%s status=%d cache_hit=%s rows=%d elapsed_ms=%s",
        session_key,
        response.status_code,
        data.get("cache_hit"),
        len(data.get("rows", [])),
        data.get("elapsed_ms"),
    )
    return data


def trigger_visualize_fields(worker_source: dict, *, worker_url: str) -> dict:
    """Ask the worker for the Graphic Walker field schema of a source."""
    session_key = worker_source.get("session_key")
    logger.info(
        "[viz] -> worker /catalog/visualize_fields session_key=%s kind=%s",
        session_key,
        worker_source.get("kind"),
    )
    body = {"source": worker_source}
    response = _worker_session.post("/catalog/visualize_fields", worker_url=worker_url, json=body, timeout=30)
    if not response.ok:
        logger.warning(
            "[viz] <- worker /catalog/visualize_fields session_key=%s status=%d body=%s",
            session_key,
            response.status_code,
            response.text[:300],
        )
        raise RuntimeError(f"Worker visualize_fields failed: {response.text}")
    data = response.json()
    logger.info(
        "[viz] <- worker /catalog/visualize_fields session_key=%s status=%d cache_hit=%s field_count=%d",
        session_key,
        response.status_code,
        data.get("cache_hit"),
        len(data.get("fields", [])),
    )
    return data


def trigger_visualize_column_stats(worker_source: dict, column: str, limit: int, *, worker_url: str) -> dict:
    """Ask the worker for distinct values + min/max of a single column."""
    session_key = worker_source.get("session_key")
    logger.info(
        "[viz] -> worker /catalog/visualize_column_stats session_key=%s kind=%s column=%s limit=%d",
        session_key,
        worker_source.get("kind"),
        column,
        limit,
    )
    body = {"source": worker_source, "column": column, "limit": limit}
    response = _worker_session.post(
        "/catalog/visualize_column_stats", worker_url=worker_url, json=body, timeout=HTTP_TIMEOUT_SECONDS
    )
    if not response.ok:
        logger.warning(
            "[viz] <- worker /catalog/visualize_column_stats session_key=%s status=%d body=%s",
            session_key,
            response.status_code,
            response.text[:300],
        )
        raise RuntimeError(f"Worker visualize_column_stats failed: {response.text}")
    data = response.json()
    logger.info(
        "[viz] <- worker /catalog/visualize_column_stats session_key=%s status=%d "
        "cache_hit=%s value_count=%d truncated=%s",
        session_key,
        response.status_code,
        data.get("cache_hit"),
        len(data.get("values") or []),
        data.get("truncated"),
    )
    return data


def trigger_read_table_metadata(table_name: str, storage: dict | None = None, *, worker_url: str) -> dict:
    """Ask the worker to read table metadata (schema, row_count, size_bytes).

    *table_name* is the bare directory name inside the catalog tables
    directory (no path separators). *storage* (when set) reads from object storage.

    Returns the parsed JSON dict on success, raises on failure.
    """
    payload = {"table_path": table_name}
    if storage is not None:
        payload["storage"] = storage
    response = _worker_session.post("/catalog/table_metadata", worker_url=worker_url, json=payload)
    if not response.ok:
        raise RuntimeError(f"Worker table metadata read failed: {response.text}")
    return response.json()


def trigger_delta_history(
    table_name: str,
    limit: int | None = None,
    storage: dict | None = None,
    *,
    worker_url: str,
) -> DeltaTableHistory:
    """Ask the worker to read Delta table version history.

    *table_name* is the bare directory name inside the catalog tables directory.
    *storage* (when set) reads from object storage.
    """
    payload = {"table_path": table_name, "limit": limit}
    if storage is not None:
        payload["storage"] = storage
    response = _worker_session.post("/catalog/delta_history", worker_url=worker_url, json=payload)
    if not response.ok:
        raise RuntimeError(f"Worker delta history read failed: {response.text}")
    return DeltaTableHistory.model_validate(response.json())


def trigger_delta_version_preview(
    table_name: str,
    version: int,
    n_rows: int = 100,
    storage: dict | None = None,
    *,
    worker_url: str,
) -> CatalogTablePreview:
    """Ask the worker to preview a Delta table at a specific version.

    *table_name* is the bare directory name inside the catalog tables directory.
    *storage* (when set) reads from object storage.
    """
    payload = {"table_path": table_name, "version": version, "n_rows": n_rows}
    if storage is not None:
        payload["storage"] = storage
    response = _worker_session.post("/catalog/delta_version_preview", worker_url=worker_url, json=payload)
    if response.status_code == 404:
        from flowfile_core.catalog.exceptions import TableVersionUnavailableError

        raise TableVersionUnavailableError(version=version)
    if not response.ok:
        raise RuntimeError(f"Worker delta version preview failed: {response.text}")
    return CatalogTablePreview.model_validate(response.json())


def trigger_delta_preview(
    table_name: str,
    n_rows: int = 100,
    storage: dict | None = None,
    *,
    worker_url: str,
) -> CatalogTablePreview:
    """Ask the worker to preview a Delta table (latest version).

    *table_name* is the bare directory name inside the catalog tables directory.
    *storage* (when set) reads from object storage.
    """
    payload = {"table_path": table_name, "n_rows": n_rows}
    if storage is not None:
        payload["storage"] = storage
    response = _worker_session.post("/catalog/delta_preview", worker_url=worker_url, json=payload)
    if not response.ok:
        raise RuntimeError(f"Worker delta preview failed: {response.text}")
    return CatalogTablePreview.model_validate(response.json())


def trigger_optimize_catalog_table(
    table_name: str,
    z_order_columns: list[str] | None = None,
    storage: dict | None = None,
    *,
    worker_url: str,
) -> dict:
    """Ask the worker to optimize (compact / Z-order) a Delta catalog table.

    *table_name* is the bare directory name inside the catalog tables directory.
    *storage* (when set) optimizes the table in object storage.
    """
    payload = {"table_path": table_name, "z_order_columns": z_order_columns}
    if storage is not None:
        payload["storage"] = storage
    response = _worker_session.post("/catalog/optimize", worker_url=worker_url, json=payload, timeout=600)
    if not response.ok:
        raise RuntimeError(f"Worker optimize failed: {response.text}")
    return response.json()


def trigger_vacuum_catalog_table(
    table_name: str,
    retention_hours: int = 168,
    dry_run: bool = True,
    storage: dict | None = None,
    *,
    worker_url: str,
) -> dict:
    """Ask the worker to vacuum tombstoned files from a Delta catalog table.

    *table_name* is the bare directory name inside the catalog tables directory.
    *storage* (when set) vacuums the table in object storage.
    """
    payload = {"table_path": table_name, "retention_hours": retention_hours, "dry_run": dry_run}
    if storage is not None:
        payload["storage"] = storage
    response = _worker_session.post("/catalog/vacuum", worker_url=worker_url, json=payload, timeout=600)
    if not response.ok:
        raise RuntimeError(f"Worker vacuum failed: {response.text}")
    return response.json()


def _raise_for_edit_response(response, table_id: int) -> None:
    """Translate the worker edit routes' typed failures into domain exceptions."""
    if response.status_code == 409:
        from flowfile_core.catalog.exceptions import StaleWriteError

        detail = {}
        try:
            detail = response.json().get("detail", {}) or {}
        except ValueError:
            pass
        raise StaleWriteError("catalog_table", table_id, expected=detail.get("expected"), current=detail.get("current"))
    if response.status_code == 422:
        try:
            detail = response.json().get("detail", {}) or {}
            message = detail.get("message") if isinstance(detail, dict) else str(detail)
        except ValueError:
            message = response.text
        raise ValueError(message or "Edit payload was rejected")


def trigger_apply_table_edits(
    table_name: str,
    table_id: int,
    edits: dict,
    storage: dict | None = None,
    *,
    worker_url: str,
) -> dict:
    """Ask the worker to apply keyed row edits to a Delta catalog table.

    *table_name* is the bare directory name inside the catalog tables directory;
    *edits* carries key_columns / expected_version / upserts / deletes / new columns.
    *table_id* is only used to construct typed errors (409 → ``StaleWriteError``).
    """
    payload = {"table_path": table_name, **edits}
    if storage is not None:
        payload["storage"] = storage
    try:
        response = _worker_session.post("/catalog/apply_edits", worker_url=worker_url, json=payload, timeout=600)
    except (requests.RequestException, ComputeError) as exc:
        from flowfile_core.catalog.exceptions import WorkerUnavailableError

        raise WorkerUnavailableError(f"worker unavailable while applying table edits: {exc}") from exc
    _raise_for_edit_response(response, table_id)
    if not response.ok:
        raise RuntimeError(f"Worker apply_edits failed: {response.text}")
    return response.json()


def trigger_add_key_column(
    table_name: str,
    table_id: int,
    column_name: str,
    expected_version: int | None = None,
    storage: dict | None = None,
    *,
    worker_url: str,
) -> dict:
    """Ask the worker to rewrite a Delta catalog table with a sequential row-id column."""
    payload = {"table_path": table_name, "column_name": column_name, "expected_version": expected_version}
    if storage is not None:
        payload["storage"] = storage
    try:
        response = _worker_session.post("/catalog/add_key_column", worker_url=worker_url, json=payload, timeout=600)
    except (requests.RequestException, ComputeError) as exc:
        from flowfile_core.catalog.exceptions import WorkerUnavailableError

        raise WorkerUnavailableError(f"worker unavailable while adding a key column: {exc}") from exc
    _raise_for_edit_response(response, table_id)
    if not response.ok:
        raise RuntimeError(f"Worker add_key_column failed: {response.text}")
    return response.json()


def get_results(file_ref: str, *, worker_url: str) -> Status | None:
    f = _worker_session.get(f"/status/{file_ref}", worker_url=worker_url, timeout=_WORKER_TIMEOUT)
    if f.status_code == 200:
        return Status(**f.json())
    else:
        raise Exception(f"get_results: Could not fetch the data, {f.text}")


def results_exists(file_ref: str, *, worker_url: str) -> bool:
    """Whether the worker at ``worker_url`` holds task ``file_ref`` as completed.

    ``False`` means only that the worker answered and the task is not there (or not finished); the caller decides
    whether to ask at all.

    Raises:
        ComputeUnavailable: The worker could not be reached; the message names ``worker_url``.
    """
    try:
        f = _worker_session.get(f"/status/{file_ref}", worker_url=worker_url, timeout=_WORKER_TIMEOUT)
        if f.status_code == 200:
            if f.json()["status"] == "Completed":
                return True
        return False
    except requests.RequestException as e:
        raise ComputeUnavailable(f"Could not reach the worker at {worker_url}: {e}") from e


def clear_task_from_worker(file_ref: str, *, worker_url: str) -> bool:
    """
    Clears a task from the worker service by making a DELETE request. It also removes associated cached files.
    Args:
        file_ref (str): The unique identifier of the task to clear.
        worker_url (str): The base URL of the worker that holds the task.

    Returns:
        bool: True if the task was successfully cleared, False otherwise. Best-effort cleanup:
        a worker this core cannot use (``ComputeError``) is logged and never fails the caller.
    """
    try:
        f = _worker_session.delete(f"/clear_task/{file_ref}", worker_url=worker_url, timeout=_WORKER_TIMEOUT)
        if f.status_code == 200:
            return True
        return False
    except requests.RequestException as e:
        logger.error(f"Failed to remove results: {str(e)}")
        return False
    except ComputeError as e:
        logger.warning(f"Did not remove results for {file_ref} from the worker: {e}")
        return False


def get_df_result(result_b64: str) -> pl.LazyFrame:
    # Results are base64-encoded string from JSON response, decode once
    return pl.LazyFrame.deserialize(io.BytesIO(b64decode(result_b64)))


def get_external_df_result(file_ref: str, *, worker_url: str) -> pl.LazyFrame | None:
    status = get_results(file_ref, worker_url=worker_url)
    if status.status != "Completed":
        raise Exception(f"Status is not completed, {status.status}")
    if status.result_type == "polars":
        return get_df_result(status.results)
    else:
        raise Exception(f"Result type is not polars, {status.result_type}")


def get_status(file_ref: str, *, worker_url: str) -> Status:
    status_response = _worker_session.get(f"/status/{file_ref}", worker_url=worker_url, timeout=_WORKER_TIMEOUT)
    if status_response.status_code == 200:
        return Status(**status_response.json())
    else:
        raise Exception(f"Could not fetch the status, {status_response.text}")


def get_worker_capabilities(*, worker_url: str) -> Capabilities:
    """What the worker at ``worker_url`` reports from ``GET /capabilities``, without the handshake's comparison."""
    return fetch_worker_capabilities(worker_url)


def cancel_task(file_ref: str, *, worker_url: str) -> bool:
    """
    Cancels a running task by making a request to the worker service.

    Args:
        file_ref: The unique identifier of the task to cancel
        worker_url: The base URL of the worker that runs the task

    Returns:
        bool: True if cancellation was successful, False otherwise; also False, logged, when the
        worker is one this core cannot use (``ComputeError``)

    Raises:
        Exception: If there's an error communicating with the worker service
    """
    try:
        response = _worker_session.post(f"/cancel_task/{file_ref}", worker_url=worker_url, timeout=_WORKER_TIMEOUT)
        if response.ok:
            return True
        return False
    except requests.RequestException as e:
        raise Exception(f"Failed to cancel task: {str(e)}") from e
    except ComputeError as e:
        logger.warning(f"Did not cancel task {file_ref} on the worker: {e}")
        return False


class BaseFetcher:
    """
    Thread-safe fetcher for polling worker status and retrieving results.

    Every fetcher follows its task on the worker at ``worker_url``, the base URL its caller passes: the status
    polls, the WebSocket submit and the cancel all go there.
    """

    status: Status | None = None

    def __init__(self, file_ref: str = None, *, worker_url: str):
        self.file_ref = file_ref if file_ref else str(uuid4())
        self.worker_url = worker_url

        # Thread synchronization
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._stop_event = threading.Event()
        self._thread = None

        # WebSocket connection for non-blocking streaming mode
        self._ws = None

        # State variables - use properties for thread-safe access
        self._result: Any | None = None
        self._started: bool = False
        self._running: bool = False
        self._error_code: int = 0
        self._error_description: str | None = None
        self._error_kind: str | None = None

    # Public properties for compatibility with subclasses
    @property
    def result(self) -> Any | None:
        with self._lock:
            return self._result

    @property
    def started(self) -> bool:
        with self._lock:
            return self._started

    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    @running.setter
    def running(self, value: bool):
        """Allow subclasses to set running status and auto-start if needed."""
        with self._lock:
            self._running = value
            # If subclass sets running=True, auto-start the thread
            if value and not self._started:
                self._start_thread()

    @property
    def error_code(self) -> int:
        with self._lock:
            return self._error_code

    @property
    def error_description(self) -> str | None:
        with self._lock:
            return self._error_description

    @property
    def error_kind(self) -> str | None:
        """The worker's failure kind ("environment", "task"), "connection" for a socket lost mid-task, or None."""
        with self._lock:
            return self._error_kind

    def _start_thread(self):
        """Internal method to start thread (must be called under lock)."""
        if not self._started:
            self._thread = threading.Thread(target=self._fetch_cached_df, daemon=True)
            self._thread.start()
            self._started = True

    def _fetch_cached_df(self):
        """Background thread that polls for results."""
        sleep_time = 0.5
        start = monotonic()

        # Don't check _running here - subclasses already set it
        try:
            while not self._stop_event.is_set():
                try:
                    r = _worker_session.get(f"/status/{self.file_ref}", worker_url=self.worker_url, timeout=10)

                    if r.status_code == 200:
                        status = Status(**r.json())

                        if status.status == "Completed":
                            self._handle_completion(status)
                            return
                        elif status.status == "Error":
                            self._handle_error(
                                error_code_for_kind(status.error_kind), status.error_message, status.error_kind
                            )
                            return
                        elif status.status == "Unknown Error":
                            self._handle_error(
                                -1,
                                "There was an unknown error with the process, "
                                "and the process got killed by the server",
                            )
                            return
                    else:
                        self._handle_error(2, f"HTTP {r.status_code}: {r.text}")
                        return

                except (requests.RequestException, ComputeError) as e:
                    self._handle_error(2, f"Request failed: {e}")
                    return

                if _POLL_DEADLINE_SECONDS and (monotonic() - start) > _POLL_DEADLINE_SECONDS:
                    self._handle_error(
                        2,
                        f"Worker task exceeded the {_POLL_DEADLINE_SECONDS:.0f}s poll deadline "
                        "while still reporting 'Processing'; giving up.",
                    )
                    return

                # Sleep without holding the lock
                if not self._stop_event.wait(timeout=sleep_time):
                    continue
                else:
                    break

            # Only reached if stop_event was set
            self._handle_cancellation()

        except Exception as e:
            logger.exception("Unexpected error in fetch thread")
            self._handle_error(-1, f"Unexpected error: {e}")

    def _handle_completion(self, status):
        """Handle successful completion. Must be called from fetch thread."""
        with self._condition:
            self.status = status
            try:
                if status.result_type == "polars":
                    self._result = get_df_result(status.results)
                else:
                    self._result = status.results
            except Exception as e:
                logger.exception("Error processing result")
                self._error_code = -1
                self._error_description = f"Error processing result: {e}"
            finally:
                self._running = False
                self._condition.notify_all()

    def _handle_error(self, code: int, description: str, kind: str | None = None):
        """Handle error state. Must be called from fetch thread."""
        with self._condition:
            self._error_code = code
            self._error_description = description
            self._error_kind = kind
            self._running = False
            self._condition.notify_all()

    def _handle_cancellation(self):
        """Handle cancellation. Must be called from fetch thread."""
        with self._condition:
            if self._error_description is None:
                self._error_description = "Task cancelled"
            logger.warning(f"Fetch operation cancelled: {self._error_description}")
            self._running = False
            self._condition.notify_all()

    def start(self):
        """Start the background fetch thread."""
        with self._lock:
            if self._started:
                logger.info("Fetcher already started")
                return
            if self._running:
                logger.info("Already running the fetching")
                return

            self._running = True
            self._start_thread()

    def cancel(self):
        """
        Cancels the current task both locally and on the worker service.
        Also cleans up any resources being used.
        """
        logger.warning("Cancelling the operation")

        # Mark cancel intent BEFORE tearing down the socket: the receive path
        # classifies the resulting ConnectionClosed as a user cancel only if
        # the stop event is already set when the close reaches it.
        self._stop_event.set()

        # Close WebSocket if streaming (causes recv thread to exit)
        with self._lock:
            ws = self._ws
            self._ws = None
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

        try:
            cancel_task(self.file_ref, worker_url=self.worker_url)
        except Exception as e:
            logger.error(f"Failed to cancel task on worker: {str(e)}")

        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                logger.warning("Fetch thread did not stop within timeout")

    def get_result(self) -> Any | None:
        """
        Get the result, blocking until it's available.

        Returns:
            The fetched result.

        Raises:
            RemoteExecutionError: If an error occurred during fetching. It carries the
                worker-side exception class recovered from the description, so callers
                that never wrap the raise themselves still report the real class.
        """
        # imported here: flow_node imports this module, so a top-level import cycles
        from flowfile_core.flowfile.flow_node.models import RemoteExecutionError, recover_error_class

        # Start if not already started (for manual usage)
        with self._lock:
            if not self._started:
                if not self._running:
                    self._running = True
                self._start_thread()

        with self._condition:
            while self._running:
                self._condition.wait()

        with self._lock:
            if self._error_description is not None:
                description = self._error_description
                raise RemoteExecutionError(
                    description, recover_error_class(None, description), error_kind=self._error_kind
                )
            return self._result

    @property
    def is_running(self) -> bool:
        """Check if the fetcher is currently running."""
        with self._lock:
            return self._running

    @property
    def has_error(self) -> bool:
        """Check if the fetcher encountered an error."""
        with self._lock:
            return self._error_description is not None

    @property
    def error_info(self) -> tuple[int, str | None]:
        """Get error code and description."""
        with self._lock:
            return self._error_code, self._error_description

    def _execute_streaming(
        self,
        operation_type: str,
        flow_id: int,
        node_id: int | str,
        lf_bytes: bytes,
        kwargs: dict | None = None,
        blocking: bool = True,
    ) -> None:
        """Execute via WebSocket streaming - no polling, binary result transfer.

        Args:
            blocking: If True (default), blocks until the result is available
                and sets self._result directly.  If False, opens the
                connection, sends the task, and hands off to a background
                thread that will set self._result when done.

        Raises on connection or send error so the caller can fall back to REST.
        """
        if blocking:
            ws = streaming_start(
                task_id=self.file_ref,
                operation_type=operation_type,
                flow_id=flow_id,
                node_id=node_id,
                lf_bytes=lf_bytes,
                kwargs=kwargs,
                worker_url=self.worker_url,
            )
            # Store the socket so cancel() can close it mid-receive, and pass the
            # stop event so the bounded receive loop observes cancellation too.
            with self._lock:
                self._ws = ws
                self._running = True
                self._started = True
            try:
                result, status = streaming_receive(ws, self.file_ref, should_abort=self._stop_event.is_set)
            except WorkerTaskError as e:
                self._record_task_failure(str(e), e.error_code, e.kind)
                return
            except Exception:
                # Reset to pristine: the caller's REST fallback (generic errors
                # only) relies on `running = True` auto-starting its poll thread,
                # which requires _started to be False again.
                with self._lock:
                    self._ws = None
                    self._running = False
                    self._started = False
                raise
            with self._lock:
                self._ws = None
                self._result = result
                self._running = False
            self.status = status
        else:
            ws = streaming_start(
                task_id=self.file_ref,
                operation_type=operation_type,
                flow_id=flow_id,
                node_id=node_id,
                lf_bytes=lf_bytes,
                kwargs=kwargs,
                worker_url=self.worker_url,
            )
            with self._lock:
                self._ws = ws
                self._running = True
                self._started = True
                self._thread = threading.Thread(
                    target=self._ws_receive_thread,
                    args=(ws,),
                    daemon=True,
                )
                self._thread.start()

    def _record_task_failure(self, description: str, error_code: int, kind: str | None) -> None:
        """Record a task the worker ran and reported failed, with the code the REST path would give it."""
        with self._lock:
            self._ws = None
            self._running = False
            self._error_code = error_code
            self._error_description = description
            self._error_kind = kind

    def _raise_recorded_failure(self) -> None:
        """Raise a recorded failure the way the REST path's blocking ``get_result()`` does."""
        if self.has_error:
            self.get_result()

    def _ws_receive_thread(self, ws) -> None:
        """Background thread that receives results over an open WebSocket."""
        try:
            result, status = streaming_receive(ws, self.file_ref, should_abort=self._stop_event.is_set)
            with self._condition:
                self._result = result
                self._running = False
                self._ws = None
                self.status = status
                self._condition.notify_all()
        except Exception as e:
            if isinstance(e, WorkerTaskError):
                logger.warning("Worker task %s failed (%s): %s", self.file_ref, e.kind or "task", e)
            else:
                logger.exception("Error in WebSocket receive thread")
            with self._condition:
                # -2: stalled, or canceled (a cancel can tear down with any error type); -1: error_kind says why.
                interrupted = isinstance(e, WorkerStreamInterrupted) or self._stop_event.is_set()
                if interrupted:
                    self._error_code = -2
                elif isinstance(e, WorkerTaskError):
                    self._error_code = e.error_code
                    self._error_kind = e.kind
                else:
                    self._error_code = -1
                    self._error_kind = "connection" if isinstance(e, ConnectionClosed) else None
                self._error_description = str(e)
                self._running = False
                self._ws = None
                self._condition.notify_all()


class ExternalDfFetcher(BaseFetcher):
    def __init__(
        self,
        flow_id: int,
        node_id: int | str,
        lf: pl.LazyFrame | pl.DataFrame | None = None,
        file_ref: str = None,
        wait_on_completion: bool = True,
        operation_type: OperationType = "store",
        offload_to_worker: bool = True,
        kwargs: dict | None = None,
        plan_bytes: bytes | None = None,
        *,
        worker_url: str,
    ):
        """Run a plan on the worker at ``worker_url`` as task ``file_ref`` (a new id when omitted).

        The plan is ``lf``, serialised here, or ``plan_bytes``, the output of ``pl.LazyFrame.serialize()``
        sent as is so the caller never has to deserialise it; pass exactly one of the two.
        """
        super().__init__(file_ref=file_ref, worker_url=worker_url)
        if (lf is None) == (plan_bytes is None):
            raise ValueError("ExternalDfFetcher takes exactly one of lf and plan_bytes")
        if plan_bytes is None:
            plan_bytes = (lf.lazy() if isinstance(lf, pl.DataFrame) else lf).serialize()

        try:
            self._execute_streaming(
                operation_type=operation_type,
                flow_id=flow_id,
                node_id=node_id,
                lf_bytes=plan_bytes,
                kwargs=kwargs,
                blocking=wait_on_completion,
            )
        except WorkerStreamInterrupted:
            # The task already reached the worker; re-submitting via REST would
            # duplicate work against a wedged worker or a cancelled run.
            raise
        except ComputeError:
            # The handshake refused this worker; the REST path would refuse it again.
            raise
        except Exception as e:
            logger.debug(f"WebSocket streaming unavailable ({e}), falling back to REST")
        else:
            if wait_on_completion:
                self._raise_recorded_failure()
            return

        # REST fallback (original behavior)
        r = trigger_df_operation(
            plan_bytes=plan_bytes,
            file_ref=self.file_ref,
            operation_type=operation_type,
            node_id=node_id,
            flow_id=flow_id,
            kwargs=kwargs,
            worker_url=worker_url,
        )
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()
        self.status = get_status(self.file_ref, worker_url=worker_url)


class ExternalSampler(BaseFetcher):
    def __init__(
        self,
        lf: pl.LazyFrame | pl.DataFrame,
        node_id: str | int,
        flow_id: int,
        file_ref: str = None,
        wait_on_completion: bool = True,
        sample_size: int = 100,
        *,
        worker_url: str,
    ):
        super().__init__(file_ref=file_ref, worker_url=worker_url)
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf

        try:
            self._execute_streaming(
                operation_type="store_sample",
                flow_id=flow_id,
                node_id=node_id,
                lf_bytes=lf.serialize(),
                kwargs={"sample_size": sample_size},
                blocking=wait_on_completion,
            )
        except (WorkerStreamInterrupted, ComputeError):
            # See ExternalDfFetcher: never re-submit after a successful submit or a refused handshake.
            raise
        except Exception as e:
            logger.debug(f"WebSocket streaming unavailable ({e}), falling back to REST")
        else:
            if wait_on_completion:
                self._raise_recorded_failure()
            return

        # REST fallback (original behavior)
        r = trigger_sample_operation(
            lf=lf,
            file_ref=self.file_ref,
            sample_size=sample_size,
            node_id=node_id,
            flow_id=flow_id,
            worker_url=worker_url,
        )
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()
        self.status = get_status(self.file_ref, worker_url=worker_url)


class ExternalFuzzyMatchFetcher(BaseFetcher):
    def __init__(
        self,
        left_df: pl.LazyFrame,
        right_df: pl.LazyFrame,
        fuzzy_maps: list[Any],
        flow_id: int,
        node_id: int | str,
        file_ref: str = None,
        wait_on_completion: bool = True,
        *,
        worker_url: str,
    ):
        super().__init__(file_ref=file_ref, worker_url=worker_url)

        r = trigger_fuzzy_match_operation(
            left_df=left_df,
            right_df=right_df,
            fuzzy_maps=fuzzy_maps,
            file_ref=file_ref,
            flow_id=flow_id,
            node_id=node_id,
            worker_url=worker_url,
        )
        self.file_ref = r.background_task_id
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class MLTrainFetcher(BaseFetcher):
    """Fetches the training-completion result, which is a metadata dict
    (``{sha256, size_bytes, model_type}``) rather than a LazyFrame.

    ``BaseFetcher._handle_completion`` already routes non-polars results
    straight through ``status.results``, so no overrides are needed.
    """

    def __init__(
        self,
        lf: pl.LazyFrame | pl.DataFrame,
        staging_path: str,
        model_type: str,
        target_column: str,
        feature_columns: list[str],
        params: dict[str, Any],
        flow_id: int,
        node_id: int | str,
        file_ref: str,
        wait_on_completion: bool = True,
        *,
        worker_url: str,
    ):
        super().__init__(file_ref=file_ref, worker_url=worker_url)
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf
        r = trigger_train_model_operation(
            lf=lf,
            staging_path=staging_path,
            model_type=model_type,
            target_column=target_column,
            feature_columns=feature_columns,
            params=params,
            file_ref=file_ref,
            flow_id=flow_id,
            node_id=node_id,
            worker_url=worker_url,
        )
        self.file_ref = r.background_task_id
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class MLApplyFetcher(BaseFetcher):
    """Fetches the scored LazyFrame produced by :func:`trigger_apply_model_operation`."""

    def __init__(
        self,
        lf: pl.LazyFrame | pl.DataFrame,
        model_path: str,
        output_column: str,
        flow_id: int,
        node_id: int | str,
        file_ref: str,
        wait_on_completion: bool = True,
        *,
        worker_url: str,
    ):
        super().__init__(file_ref=file_ref, worker_url=worker_url)
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf
        r = trigger_apply_model_operation(
            lf=lf,
            model_path=model_path,
            output_column=output_column,
            file_ref=file_ref,
            flow_id=flow_id,
            node_id=node_id,
            worker_url=worker_url,
        )
        self.file_ref = r.background_task_id
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalCustomNodeFetcher(BaseFetcher):
    """Runs a custom node's process() in the worker; result is the JSON payload
    with per-output IPC paths and row counts (result_type="other")."""

    def __init__(self, request: CustomNodeExecuteInput, wait_on_completion: bool = False, *, worker_url: str):
        super().__init__(file_ref=request.task_id, worker_url=worker_url)
        request.task_id = self.file_ref
        r = trigger_custom_node_operation(request, worker_url=worker_url)
        self.file_ref = r.background_task_id
        self.running = r.status in ("Processing", "Starting")
        if wait_on_completion:
            _ = self.get_result()

    def get_payload(self) -> dict:
        result = self.get_result()
        return json.loads(result) if isinstance(result, str) else result


class ExternalCreateFetcher(BaseFetcher):
    def __init__(
        self,
        received_table: ReceivedTable,
        node_id: int,
        flow_id: int,
        file_type: str = "csv",
        wait_on_completion: bool = True,
        *,
        worker_url: str,
    ):
        r = trigger_create_operation(
            received_table=received_table, file_type=file_type, node_id=node_id, flow_id=flow_id, worker_url=worker_url
        )
        super().__init__(file_ref=r.background_task_id, worker_url=worker_url)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalDatabaseFetcher(BaseFetcher):
    def __init__(
        self,
        database_external_read_settings: DatabaseExternalReadSettings,
        wait_on_completion: bool = True,
        *,
        worker_url: str,
    ):
        r = trigger_database_read_collector(
            database_external_read_settings=database_external_read_settings, worker_url=worker_url
        )
        super().__init__(file_ref=r.background_task_id, worker_url=worker_url)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalKafkaFetcher(BaseFetcher):
    """Fetches data from Kafka via the worker service. Same pattern as ExternalDatabaseFetcher."""

    def __init__(self, kafka_read_settings, wait_on_completion: bool = True, *, worker_url: str):
        r = trigger_kafka_read(kafka_read_settings=kafka_read_settings, worker_url=worker_url)
        super().__init__(file_ref=r.background_task_id, worker_url=worker_url)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalGoogleAnalyticsFetcher(BaseFetcher):
    """Fetches GA4 data via the worker service. Same pattern as ExternalDatabaseFetcher."""

    def __init__(self, ga_read_settings, wait_on_completion: bool = True, *, worker_url: str):
        r = trigger_google_analytics_read(ga_read_settings=ga_read_settings, worker_url=worker_url)
        super().__init__(file_ref=r.background_task_id, worker_url=worker_url)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalRestApiFetcher(BaseFetcher):
    """Fetches REST API data via the worker service. Same pattern as ExternalDatabaseFetcher."""

    def __init__(self, settings, wait_on_completion: bool = True, *, worker_url: str):
        r = trigger_rest_api_read(settings=settings, worker_url=worker_url)
        super().__init__(file_ref=r.background_task_id, worker_url=worker_url)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalDatabaseWriter(BaseFetcher):
    def __init__(
        self,
        database_external_write_settings: DatabaseExternalWriteSettings,
        wait_on_completion: bool = True,
        *,
        worker_url: str,
    ):
        r = trigger_database_write(
            database_external_write_settings=database_external_write_settings, worker_url=worker_url
        )
        super().__init__(file_ref=r.background_task_id, worker_url=worker_url)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalCloudWriter(BaseFetcher):
    def __init__(
        self,
        cloud_storage_write_settings: CloudStorageWriteSettingsWorkerInterface,
        wait_on_completion: bool = True,
        *,
        worker_url: str,
    ):
        r = trigger_cloud_storage_write(
            database_external_write_settings=cloud_storage_write_settings, worker_url=worker_url
        )
        super().__init__(file_ref=r.background_task_id, worker_url=worker_url)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalOutputWriter(BaseFetcher):
    """Writes output to disk via the worker's /write_results/ endpoint.

    Inherits BaseFetcher.cancel(), so a flow cancel propagates to the worker
    as a POST /cancel_task/{task_id} and terminates the running sink process.
    """

    def __init__(
        self,
        lf: pl.LazyFrame | pl.DataFrame,
        data_type: str,
        path: str,
        write_mode: str,
        flow_id: int,
        node_id: int | str,
        sheet_name: str | None = None,
        delimiter: str | None = None,
        compression: str | None = None,
        wait_on_completion: bool = True,
        *,
        worker_url: str,
    ):
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf
        r = trigger_write_output(
            lf=lf,
            data_type=data_type,
            path=path,
            write_mode=write_mode,
            sheet_name=sheet_name,
            delimiter=delimiter,
            compression=compression,
            flow_id=flow_id,
            node_id=node_id,
            worker_url=worker_url,
        )
        super().__init__(file_ref=r.background_task_id, worker_url=worker_url)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()
