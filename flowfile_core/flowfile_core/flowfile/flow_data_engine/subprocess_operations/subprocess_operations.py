# Standard library imports
import io
import json
import threading
from base64 import b64decode
from typing import Any, Literal
from uuid import uuid4

import polars as pl
import requests
from pl_fuzzy_frame_match.models import FuzzyMapping

from flowfile_core.configs import logger
from flowfile_core.configs.settings import OFFLOAD_TO_WORKER, WORKER_URL
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import (
    ApplyModelInput,
    FuzzyJoinInput,
    OperationType,
    PolarsOperation,
    Status,
    TrainModelInput,
)
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.streaming import (
    streaming_receive,
    streaming_start,
    streaming_submit,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.models import (
    DatabaseExternalReadSettings,
    DatabaseExternalWriteSettings,
)
from flowfile_core.schemas.catalog_schema import CatalogTablePreview, DeltaTableHistory
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageWriteSettingsWorkerInterface
from flowfile_core.schemas.input_schema import ReceivedTable
from flowfile_core.utils.arrow_reader import read
from shared.viz_protocol import HTTP_TIMEOUT_SECONDS


def trigger_df_operation(
    flow_id: int,
    node_id: int | str,
    lf: pl.LazyFrame,
    file_ref: str,
    operation_type: OperationType = "store",
    kwargs: dict | None = None,
) -> Status:
    # Send raw bytes directly - no base64 encoding overhead
    headers = {
        "Content-Type": "application/octet-stream",
        "X-Task-Id": file_ref,
        "X-Operation-Type": operation_type,
        "X-Flow-Id": str(flow_id),
        "X-Node-Id": str(node_id),
    }
    if kwargs:
        headers["X-Kwargs"] = json.dumps(kwargs)
    v = requests.post(url=f"{WORKER_URL}/submit_query/", data=lf.serialize(), headers=headers)
    if not v.ok:
        raise Exception(f"trigger_df_operation: Could not cache the data, {v.text}")
    return Status(**v.json())


def trigger_sample_operation(
    lf: pl.LazyFrame, file_ref: str, flow_id: int, node_id: str | int, sample_size: int = 100
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
    v = requests.post(url=f"{WORKER_URL}/store_sample/", data=lf.serialize(), headers=headers)
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
    v = requests.post(f"{WORKER_URL}/add_fuzzy_join", data=fuzzy_join_input.model_dump_json())
    if not v.ok:
        raise Exception(f"trigger_fuzzy_match_operation: Could not cache the data, {v.text}")
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
    v = requests.post(f"{WORKER_URL}/train_ml_model", data=payload.model_dump_json())
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
    v = requests.post(f"{WORKER_URL}/apply_ml_model", data=payload.model_dump_json())
    if not v.ok:
        raise Exception(f"trigger_apply_model_operation: Could not start scoring, {v.text}")
    return Status(**v.json())


def trigger_create_operation(
    flow_id: int,
    node_id: int | str,
    received_table: ReceivedTable,
    file_type: str = Literal["csv", "parquet", "json", "excel"],
):
    f = requests.post(
        url=f"{WORKER_URL}/create_table/{file_type}",
        data=received_table.model_dump_json(),
        params={"flowfile_flow_id": flow_id, "flowfile_node_id": node_id},
    )
    if not f.ok:
        raise Exception(f"trigger_create_operation: Could not cache the data, {f.text}")
    return Status(**f.json())


def trigger_database_read_collector(database_external_read_settings: DatabaseExternalReadSettings):
    f = requests.post(
        url=f"{WORKER_URL}/store_database_read_result", data=database_external_read_settings.model_dump_json()
    )
    if not f.ok:
        raise Exception(f"trigger_database_read_collector: Could not cache the data, {f.text}")
    return Status(**f.json())


def trigger_kafka_read(kafka_read_settings) -> Status:
    """Send a Kafka read request to the worker service."""
    f = requests.post(url=f"{WORKER_URL}/store_kafka_read_result", data=kafka_read_settings.model_dump_json())
    if not f.ok:
        raise Exception(f"trigger_kafka_read: Could not read from Kafka, {f.text}")
    return Status(**f.json())


def fetch_kafka_offsets(task_id: str) -> dict | None:
    """Fetch deferred Kafka offset data from the worker for a completed task.

    Returns a dict with ``new_offsets``, ``messages_consumed``, etc. from the
    KafkaReadResult that was saved as a sidecar file, or ``None`` if no
    offsets were recorded (e.g. empty topic).
    """
    f = requests.get(f"{WORKER_URL}/kafka_offsets/{task_id}")
    if not f.ok:
        logger.warning("Failed to fetch Kafka offsets for task %s: %s", task_id, f.text)
        return None
    data = f.json()
    return data


def trigger_google_analytics_read(ga_read_settings) -> Status:
    """Send a Google Analytics 4 read request to the worker service."""
    f = requests.post(url=f"{WORKER_URL}/store_google_analytics_read_result", data=ga_read_settings.model_dump_json())
    if not f.ok:
        raise Exception(f"trigger_google_analytics_read: Could not read from GA, {f.text}")
    return Status(**f.json())


def trigger_rest_api_read(settings) -> Status:
    """Send a REST API read request to the worker service."""
    f = requests.post(url=f"{WORKER_URL}/store_rest_api_read_result", data=settings.model_dump_json())
    if not f.ok:
        raise Exception(f"trigger_rest_api_read: Could not read from the REST API, {f.text}")
    return Status(**f.json())


def trigger_database_write(database_external_write_settings: DatabaseExternalWriteSettings):
    f = requests.post(
        url=f"{WORKER_URL}/store_database_write_result", data=database_external_write_settings.model_dump_json()
    )
    if not f.ok:
        raise Exception(f"trigger_database_write: Could not cache the data, {f.text}")
    return Status(**f.json())


def trigger_cloud_storage_write(database_external_write_settings: CloudStorageWriteSettingsWorkerInterface):
    f = requests.post(url=f"{WORKER_URL}/write_data_to_cloud", data=database_external_write_settings.model_dump_json())
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
) -> Status:
    from base64 import encodebytes

    serializable_df = lf.serialize()
    r = requests.post(
        f"{WORKER_URL}/write_results/",
        json={
            "operation": encodebytes(serializable_df).decode(),
            "data_type": data_type,
            "path": path,
            "write_mode": write_mode,
            "sheet_name": sheet_name,
            "delimiter": delimiter,
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
):
    payload = {
        "source_file_path": source_file_path,
        "table_name": table_name,
    }
    response = requests.post(f"{WORKER_URL}/catalog/materialize", json=payload)
    return response


def trigger_resolve_virtual_table(
    table_id: int,
    plan_bytes: bytes,
    source_versions_hash: str,
) -> dict:
    """Ask the worker to materialise a flow-virtual table to its IPC cache.

    Ships *plan_bytes* (output of ``pl.LazyFrame.serialize()``); the worker
    deserialises in a spawned child, collects, and writes IPC. Idempotent on
    ``(table_id, source_versions_hash)``.
    """
    from base64 import b64encode

    payload = {
        "table_id": table_id,
        "plan_bytes": b64encode(plan_bytes).decode("ascii"),
        "source_versions_hash": source_versions_hash,
    }
    response = requests.post(f"{WORKER_URL}/flow/resolve_virtual_table", json=payload, timeout=300)
    if not response.ok:
        raise RuntimeError(f"Worker resolve_virtual_table failed: {response.text}")
    return response.json()


def trigger_sql_query(
    query: str,
    tables: dict[str, str],
    max_rows: int = 10_000,
    virtual_refs: dict[str, str] | None = None,
) -> dict:
    """Ask the worker to execute a SQL query against catalog tables.

    *tables* is a mapping of logical table name -> directory name.
    *virtual_refs* is an optional mapping of virtual table name -> bare IPC
    filename under the worker's catalog_virtual_results directory.
    Returns the parsed JSON response dict.
    """
    payload: dict = {"query": query, "tables": tables, "max_rows": max_rows}
    if virtual_refs:
        payload["virtual_refs"] = virtual_refs
    response = requests.post(f"{WORKER_URL}/catalog/sql_query", json=payload)
    if not response.ok:
        raise RuntimeError(f"Worker SQL query execution failed: {response.text}")
    return response.json()


def trigger_visualize_query(worker_source: dict, payload: dict, max_rows: int) -> dict:
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
    response = requests.post(f"{WORKER_URL}/catalog/visualize_query", json=body, timeout=HTTP_TIMEOUT_SECONDS)
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


def trigger_visualize_fields(worker_source: dict) -> dict:
    """Ask the worker for the Graphic Walker field schema of a source."""
    session_key = worker_source.get("session_key")
    logger.info(
        "[viz] -> worker /catalog/visualize_fields session_key=%s kind=%s",
        session_key,
        worker_source.get("kind"),
    )
    body = {"source": worker_source}
    response = requests.post(f"{WORKER_URL}/catalog/visualize_fields", json=body, timeout=30)
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


def trigger_visualize_column_stats(worker_source: dict, column: str, limit: int) -> dict:
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
    response = requests.post(f"{WORKER_URL}/catalog/visualize_column_stats", json=body, timeout=HTTP_TIMEOUT_SECONDS)
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
        "[viz] <- worker /catalog/visualize_column_stats session_key=%s status=%d cache_hit=%s value_count=%d truncated=%s",
        session_key,
        response.status_code,
        data.get("cache_hit"),
        len(data.get("values") or []),
        data.get("truncated"),
    )
    return data


def trigger_visualize_evict(session_key: str) -> None:
    """Ask the worker to drop a cached viz session (e.g. after a table update)."""
    logger.info("[viz] -> worker /catalog/visualize_evict session_key=%s", session_key)
    response = requests.post(
        f"{WORKER_URL}/catalog/visualize_evict",
        params={"session_key": session_key},
        timeout=10,
    )
    if not response.ok:
        raise RuntimeError(f"Worker visualize_evict failed: {response.text}")


def trigger_read_table_metadata(table_name: str) -> dict:
    """Ask the worker to read table metadata (schema, row_count, size_bytes).

    *table_name* is the bare directory name inside the catalog tables
    directory (no path separators).

    Returns the parsed JSON dict on success, raises on failure.
    """
    payload = {"table_path": table_name}
    response = requests.post(f"{WORKER_URL}/catalog/table_metadata", json=payload)
    if not response.ok:
        raise RuntimeError(f"Worker table metadata read failed: {response.text}")
    return response.json()


def trigger_delta_history(
    table_name: str,
    limit: int | None = None,
) -> DeltaTableHistory:
    """Ask the worker to read Delta table version history.

    *table_name* is the bare directory name inside the catalog tables directory.
    """
    payload = {"table_path": table_name, "limit": limit}
    response = requests.post(f"{WORKER_URL}/catalog/delta_history", json=payload)
    if not response.ok:
        raise RuntimeError(f"Worker delta history read failed: {response.text}")
    return DeltaTableHistory.model_validate(response.json())


def trigger_delta_version_preview(
    table_name: str,
    version: int,
    n_rows: int = 100,
) -> CatalogTablePreview:
    """Ask the worker to preview a Delta table at a specific version.

    *table_name* is the bare directory name inside the catalog tables directory.
    """
    payload = {"table_path": table_name, "version": version, "n_rows": n_rows}
    response = requests.post(f"{WORKER_URL}/catalog/delta_version_preview", json=payload)
    if not response.ok:
        raise RuntimeError(f"Worker delta version preview failed: {response.text}")
    return CatalogTablePreview.model_validate(response.json())


def get_results(file_ref: str) -> Status | None:
    f = requests.get(f"{WORKER_URL}/status/{file_ref}")
    if f.status_code == 200:
        return Status(**f.json())
    else:
        raise Exception(f"get_results: Could not fetch the data, {f.text}")


def results_exists(file_ref: str):
    if not OFFLOAD_TO_WORKER:
        return False

    try:
        f = requests.get(f"{WORKER_URL}/status/{file_ref}")
        if f.status_code == 200:
            if f.json()["status"] == "Completed":
                return True
        return False
    except requests.RequestException as e:
        logger.error(f"Failed to check results existence: {str(e)}")
        if "Connection refused" in str(e):
            logger.info("")
        return False


def clear_task_from_worker(file_ref: str) -> bool:
    """
    Clears a task from the worker service by making a DELETE request. It also removes associated cached files.
    Args:
        file_ref (str): The unique identifier of the task to clear.

    Returns:
        bool: True if the task was successfully cleared, False otherwise.
    """
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    if not OFFLOAD_TO_WORKER:
        return False

    try:
        f = requests.delete(f"{WORKER_URL}/clear_task/{file_ref}")
        if f.status_code == 200:
            return True
        return False
    except requests.RequestException as e:
        logger.error(f"Failed to remove results: {str(e)}")
        return False


def get_df_result(result_b64: str) -> pl.LazyFrame:
    # Results are base64-encoded string from JSON response, decode once
    return pl.LazyFrame.deserialize(io.BytesIO(b64decode(result_b64)))


def get_external_df_result(file_ref: str) -> pl.LazyFrame | None:
    status = get_results(file_ref)
    if status.status != "Completed":
        raise Exception(f"Status is not completed, {status.status}")
    if status.result_type == "polars":
        return get_df_result(status.results)
    else:
        raise Exception(f"Result type is not polars, {status.result_type}")


def get_status(file_ref: str) -> Status:
    status_response = requests.get(f"{WORKER_URL}/status/{file_ref}")
    if status_response.status_code == 200:
        return Status(**status_response.json())
    else:
        raise Exception(f"Could not fetch the status, {status_response.text}")


def cancel_task(file_ref: str) -> bool:
    """
    Cancels a running task by making a request to the worker service.

    Args:
        file_ref: The unique identifier of the task to cancel

    Returns:
        bool: True if cancellation was successful, False otherwise

    Raises:
        Exception: If there's an error communicating with the worker service
    """
    try:
        response = requests.post(f"{WORKER_URL}/cancel_task/{file_ref}")
        if response.ok:
            return True
        return False
    except requests.RequestException as e:
        raise Exception(f"Failed to cancel task: {str(e)}") from e


class BaseFetcher:
    """
    Thread-safe fetcher for polling worker status and retrieving results.
    """

    def __init__(self, file_ref: str = None):
        self.file_ref = file_ref if file_ref else str(uuid4())

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

    def _start_thread(self):
        """Internal method to start thread (must be called under lock)."""
        if not self._started:
            self._thread = threading.Thread(target=self._fetch_cached_df, daemon=True)
            self._thread.start()
            self._started = True

    def _fetch_cached_df(self):
        """Background thread that polls for results."""
        sleep_time = 0.5

        # Don't check _running here - subclasses already set it
        try:
            while not self._stop_event.is_set():
                try:
                    r = requests.get(f"{WORKER_URL}/status/{self.file_ref}", timeout=10)

                    if r.status_code == 200:
                        status = Status(**r.json())

                        if status.status == "Completed":
                            self._handle_completion(status)
                            return
                        elif status.status == "Error":
                            self._handle_error(1, status.error_message)
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

                except requests.RequestException as e:
                    self._handle_error(2, f"Request failed: {e}")
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

    def _handle_error(self, code: int, description: str):
        """Handle error state. Must be called from fetch thread."""
        with self._condition:
            self._error_code = code
            self._error_description = description
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
            cancel_task(self.file_ref)
        except Exception as e:
            logger.error(f"Failed to cancel task on worker: {str(e)}")

        self._stop_event.set()

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
            Exception: If an error occurred during fetching.
        """
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
                raise Exception(self._error_description)
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
            result, status = streaming_submit(
                task_id=self.file_ref,
                operation_type=operation_type,
                flow_id=flow_id,
                node_id=node_id,
                lf_bytes=lf_bytes,
                kwargs=kwargs,
            )
            with self._lock:
                self._result = result
                self._running = False
                self._started = True
            self.status = status
        else:
            ws = streaming_start(
                task_id=self.file_ref,
                operation_type=operation_type,
                flow_id=flow_id,
                node_id=node_id,
                lf_bytes=lf_bytes,
                kwargs=kwargs,
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

    def _ws_receive_thread(self, ws) -> None:
        """Background thread that receives results over an open WebSocket."""
        try:
            result, status = streaming_receive(ws, self.file_ref)
            with self._condition:
                self._result = result
                self._running = False
                self._ws = None
                self.status = status
                self._condition.notify_all()
        except Exception as e:
            logger.exception("Error in WebSocket receive thread")
            with self._condition:
                self._error_code = -1
                self._error_description = str(e)
                self._running = False
                self._ws = None
                self._condition.notify_all()


class ExternalDfFetcher(BaseFetcher):
    status: Status | None = None

    def __init__(
        self,
        flow_id: int,
        node_id: int | str,
        lf: pl.LazyFrame | pl.DataFrame,
        file_ref: str = None,
        wait_on_completion: bool = True,
        operation_type: OperationType = "store",
        offload_to_worker: bool = True,
        kwargs: dict | None = None,
    ):
        super().__init__(file_ref=file_ref)
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf

        try:
            self._execute_streaming(
                operation_type=operation_type,
                flow_id=flow_id,
                node_id=node_id,
                lf_bytes=lf.serialize(),
                kwargs=kwargs,
                blocking=wait_on_completion,
            )
            return
        except Exception as e:
            logger.debug(f"WebSocket streaming unavailable ({e}), falling back to REST")

        # REST fallback (original behavior)
        r = trigger_df_operation(
            lf=lf,
            file_ref=self.file_ref,
            operation_type=operation_type,
            node_id=node_id,
            flow_id=flow_id,
            kwargs=kwargs,
        )
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()
        self.status = get_status(self.file_ref)


class ExternalSampler(BaseFetcher):
    status: Status | None = None

    def __init__(
        self,
        lf: pl.LazyFrame | pl.DataFrame,
        node_id: str | int,
        flow_id: int,
        file_ref: str = None,
        wait_on_completion: bool = True,
        sample_size: int = 100,
    ):
        super().__init__(file_ref=file_ref)
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
            return
        except Exception as e:
            logger.debug(f"WebSocket streaming unavailable ({e}), falling back to REST")

        # REST fallback (original behavior)
        r = trigger_sample_operation(
            lf=lf, file_ref=file_ref, sample_size=sample_size, node_id=node_id, flow_id=flow_id
        )
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()
        self.status = get_status(self.file_ref)


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
    ):
        super().__init__(file_ref=file_ref)

        r = trigger_fuzzy_match_operation(
            left_df=left_df,
            right_df=right_df,
            fuzzy_maps=fuzzy_maps,
            file_ref=file_ref,
            flow_id=flow_id,
            node_id=node_id,
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
    ):
        super().__init__(file_ref=file_ref)
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
    ):
        super().__init__(file_ref=file_ref)
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf
        r = trigger_apply_model_operation(
            lf=lf,
            model_path=model_path,
            output_column=output_column,
            file_ref=file_ref,
            flow_id=flow_id,
            node_id=node_id,
        )
        self.file_ref = r.background_task_id
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalCreateFetcher(BaseFetcher):
    def __init__(
        self,
        received_table: ReceivedTable,
        node_id: int,
        flow_id: int,
        file_type: str = "csv",
        wait_on_completion: bool = True,
    ):
        r = trigger_create_operation(
            received_table=received_table, file_type=file_type, node_id=node_id, flow_id=flow_id
        )
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalDatabaseFetcher(BaseFetcher):
    def __init__(self, database_external_read_settings: DatabaseExternalReadSettings, wait_on_completion: bool = True):
        r = trigger_database_read_collector(database_external_read_settings=database_external_read_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalKafkaFetcher(BaseFetcher):
    """Fetches data from Kafka via the worker service. Same pattern as ExternalDatabaseFetcher."""

    def __init__(self, kafka_read_settings, wait_on_completion: bool = True):
        r = trigger_kafka_read(kafka_read_settings=kafka_read_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalGoogleAnalyticsFetcher(BaseFetcher):
    """Fetches GA4 data via the worker service. Same pattern as ExternalDatabaseFetcher."""

    def __init__(self, ga_read_settings, wait_on_completion: bool = True):
        r = trigger_google_analytics_read(ga_read_settings=ga_read_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalRestApiFetcher(BaseFetcher):
    """Fetches REST API data via the worker service. Same pattern as ExternalDatabaseFetcher."""

    def __init__(self, settings, wait_on_completion: bool = True):
        r = trigger_rest_api_read(settings=settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalDatabaseWriter(BaseFetcher):
    def __init__(
        self, database_external_write_settings: DatabaseExternalWriteSettings, wait_on_completion: bool = True
    ):
        r = trigger_database_write(database_external_write_settings=database_external_write_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


class ExternalCloudWriter(BaseFetcher):
    def __init__(
        self, cloud_storage_write_settings: CloudStorageWriteSettingsWorkerInterface, wait_on_completion: bool = True
    ):
        r = trigger_cloud_storage_write(database_external_write_settings=cloud_storage_write_settings)
        super().__init__(file_ref=r.background_task_id)
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
        wait_on_completion: bool = True,
    ):
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf
        r = trigger_write_output(
            lf=lf,
            data_type=data_type,
            path=path,
            write_mode=write_mode,
            sheet_name=sheet_name,
            delimiter=delimiter,
            flow_id=flow_id,
            node_id=node_id,
        )
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == "Processing"
        if wait_on_completion:
            _ = self.get_result()


def fetch_unique_values(lf: pl.LazyFrame) -> list[str]:
    """
    Fetches unique values from a specified column in a LazyFrame, attempting first via an external fetcher
    and falling back to direct LazyFrame computation if that fails.

    Args:
        lf: A Polars LazyFrame containing the data
        column: Name of the column to extract unique values from

    Returns:
        List[str]: List of unique values from the specified column cast to strings

    Raises:
        ValueError: If no unique values are found or if the fetch operation fails

    Example:
        >>> lf = pl.LazyFrame({'category': ['A', 'B', 'A', 'C']})
        >>> unique_vals = fetch_unique_values(lf)
        >>> print(unique_vals)
        ['A', 'B', 'C']
    """
    try:
        try:
            external_df_fetcher = ExternalDfFetcher(lf=lf, flow_id=1, node_id=-1)
            if external_df_fetcher.status.status == "Completed":
                unique_values = read(external_df_fetcher.status.file_ref).column(0).to_pylist()
                if logger:
                    logger.info(f"Got {len(unique_values)} unique values from external source")
                return unique_values
        except Exception as e:
            if logger:
                logger.debug(f"Failed reading external file: {str(e)}")

        unique_values = lf.unique().collect(engine="streaming")[:, 0].to_list()

        if not unique_values:
            raise ValueError("No unique values found in lazyframe")

        return unique_values

    except Exception as e:
        error_msg = f"Failed to fetch unique values: {str(e)}"
        if logger:
            logger.error(error_msg)
        raise ValueError(error_msg) from e
