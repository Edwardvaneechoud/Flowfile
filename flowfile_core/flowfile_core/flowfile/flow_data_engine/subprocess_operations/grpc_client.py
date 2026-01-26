# flowfile_core/flowfile_core/flowfile/flow_data_engine/subprocess_operations/grpc_client.py
"""
gRPC client for communicating with the Flowfile worker service.
This replaces the REST-based communication in subprocess_operations.py.
"""

import io
import threading
from time import sleep
from typing import Any
from uuid import uuid4

import grpc
import polars as pl
from pl_fuzzy_frame_match.models import FuzzyMapping

from flowfile_core.configs import logger
from flowfile_core.configs.settings import WORKER_GRPC_HOST, WORKER_GRPC_PORT
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import (
    OperationType,
    Status,
)
from flowfile_core.flowfile.sources.external_sources.sql_source.models import (
    DatabaseExternalReadSettings,
    DatabaseExternalWriteSettings,
)
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageWriteSettingsWorkerInterface
from flowfile_core.schemas.input_schema import ReceivedTable
from flowfile_core.utils.arrow_reader import read
from shared.grpc_protos import (
    CloudStorageConnection,
    CloudStorageWriteRequest,
    CloudStorageWriteSettings,
    CreateTableRequest,
    DatabaseConnection,
    DatabaseReadRequest,
    DatabaseWriteRequest,
    Empty,
    FuzzyJoinRequest,
    PolarsOperation,
    StatusResponse,
    StoreSampleRequest,
    SubmitQueryRequest,
    TaskIdRequest,
    WorkerServiceStub,
    WriteResultsRequest,
)
from shared.grpc_protos import FuzzyMapping as GrpcFuzzyMapping

# Global channel and stub (reusable connection)
_channel: grpc.Channel | None = None
_stub: WorkerServiceStub | None = None
_channel_lock = threading.Lock()


def get_grpc_channel() -> grpc.Channel:
    """Get or create a gRPC channel to the worker."""
    global _channel
    with _channel_lock:
        if _channel is None:
            target = f"{WORKER_GRPC_HOST}:{WORKER_GRPC_PORT}"
            _channel = grpc.insecure_channel(
                target,
                options=[
                    ("grpc.max_send_message_length", 512 * 1024 * 1024),  # 512MB
                    ("grpc.max_receive_message_length", 512 * 1024 * 1024),  # 512MB
                    ("grpc.keepalive_time_ms", 30000),  # 30 seconds
                    ("grpc.keepalive_timeout_ms", 10000),  # 10 seconds
                ],
            )
            logger.info(f"Created gRPC channel to {target}")
        return _channel


def get_worker_stub() -> WorkerServiceStub:
    """Get or create a gRPC stub for the worker service."""
    global _stub
    with _channel_lock:
        if _stub is None:
            _stub = WorkerServiceStub(get_grpc_channel())
        return _stub


def close_grpc_channel():
    """Close the gRPC channel."""
    global _channel, _stub
    with _channel_lock:
        if _channel is not None:
            _channel.close()
            _channel = None
            _stub = None
            logger.info("Closed gRPC channel")


def grpc_status_to_model(response: StatusResponse) -> Status:
    """Convert a gRPC StatusResponse to a models.Status."""
    return Status(
        background_task_id=response.task_id,
        status=response.status,
        file_ref=response.file_ref,
        progress=response.progress,
        error_message=response.error_message if response.error_message else None,
        results=response.results if response.results else None,
        result_type=response.result_type,
    )


def trigger_df_operation(
    flow_id: int, node_id: int | str, lf: pl.LazyFrame, file_ref: str, operation_type: OperationType = "store"
) -> Status:
    """Submit a dataframe query for processing via gRPC."""
    stub = get_worker_stub()
    request = SubmitQueryRequest(
        polars_data=lf.serialize(),
        task_id=file_ref,
        operation_type=operation_type,
        flow_id=flow_id,
        node_id=str(node_id),
    )
    try:
        response = stub.SubmitQuery(request)
        return grpc_status_to_model(response)
    except grpc.RpcError as e:
        raise Exception(f"trigger_df_operation: gRPC error - {e.code()}: {e.details()}")


def trigger_sample_operation(
    lf: pl.LazyFrame, file_ref: str, flow_id: int, node_id: str | int, sample_size: int = 100
) -> Status:
    """Submit a sample operation via gRPC."""
    stub = get_worker_stub()
    request = StoreSampleRequest(
        polars_data=lf.serialize(),
        task_id=file_ref,
        sample_size=sample_size,
        flow_id=flow_id,
        node_id=str(node_id),
    )
    try:
        response = stub.StoreSample(request)
        return grpc_status_to_model(response)
    except grpc.RpcError as e:
        raise Exception(f"trigger_sample_operation: gRPC error - {e.code()}: {e.details()}")


def trigger_fuzzy_match_operation(
    left_df: pl.LazyFrame,
    right_df: pl.LazyFrame,
    fuzzy_maps: list[FuzzyMapping],
    file_ref: str,
    flow_id: int,
    node_id: int | str,
) -> Status:
    """Submit a fuzzy join operation via gRPC."""
    stub = get_worker_stub()

    # Convert FuzzyMapping to gRPC FuzzyMapping
    grpc_fuzzy_maps = []
    for fm in fuzzy_maps:
        grpc_fuzzy_maps.append(
            GrpcFuzzyMapping(
                left_col=fm.left_col,
                right_col=fm.right_col,
                threshold=fm.threshold,
                algorithm=fm.algorithm,
                limit=fm.limit,
            )
        )

    request = FuzzyJoinRequest(
        left_df_operation=PolarsOperation(
            operation=left_df.serialize(),
            flow_id=flow_id,
            node_id=str(node_id),
        ),
        right_df_operation=PolarsOperation(
            operation=right_df.serialize(),
            flow_id=flow_id,
            node_id=str(node_id),
        ),
        fuzzy_maps=grpc_fuzzy_maps,
        task_id=file_ref,
        flow_id=flow_id,
        node_id=str(node_id),
    )
    try:
        response = stub.AddFuzzyJoin(request)
        return grpc_status_to_model(response)
    except grpc.RpcError as e:
        raise Exception(f"trigger_fuzzy_match_operation: gRPC error - {e.code()}: {e.details()}")


def trigger_create_operation(
    flow_id: int,
    node_id: int | str,
    received_table: ReceivedTable,
    file_type: str = "csv",
) -> Status:
    """Submit a table creation operation via gRPC."""
    stub = get_worker_stub()
    request = CreateTableRequest(
        file_type=file_type,
        table_data=received_table.model_dump_json().encode(),
        flow_id=flow_id,
        node_id=str(node_id),
    )
    try:
        response = stub.CreateTable(request)
        return grpc_status_to_model(response)
    except grpc.RpcError as e:
        raise Exception(f"trigger_create_operation: gRPC error - {e.code()}: {e.details()}")


def trigger_database_read_collector(database_external_read_settings: DatabaseExternalReadSettings) -> Status:
    """Submit a database read operation via gRPC."""
    stub = get_worker_stub()
    request = DatabaseReadRequest(
        connection=DatabaseConnection(
            driver=database_external_read_settings.connection.driver,
            host=database_external_read_settings.connection.host,
            port=database_external_read_settings.connection.port,
            username=database_external_read_settings.connection.username,
            password=database_external_read_settings.connection.password,
            database=database_external_read_settings.connection.database,
            schema=database_external_read_settings.connection.schema or "",
        ),
        query=database_external_read_settings.query,
        flow_id=database_external_read_settings.flowfile_flow_id,
        node_id=str(database_external_read_settings.flowfile_node_id),
    )
    try:
        response = stub.StoreDatabaseReadResult(request)
        return grpc_status_to_model(response)
    except grpc.RpcError as e:
        raise Exception(f"trigger_database_read_collector: gRPC error - {e.code()}: {e.details()}")


def trigger_database_write(database_external_write_settings: DatabaseExternalWriteSettings) -> Status:
    """Submit a database write operation via gRPC."""
    stub = get_worker_stub()
    request = DatabaseWriteRequest(
        polars_data=database_external_write_settings.operation,
        connection=DatabaseConnection(
            driver=database_external_write_settings.connection.driver,
            host=database_external_write_settings.connection.host,
            port=database_external_write_settings.connection.port,
            username=database_external_write_settings.connection.username,
            password=database_external_write_settings.connection.password,
            database=database_external_write_settings.connection.database,
            schema=database_external_write_settings.connection.schema or "",
        ),
        table_name=database_external_write_settings.table_name,
        if_exists=database_external_write_settings.if_exists,
        flow_id=database_external_write_settings.flowfile_flow_id,
        node_id=str(database_external_write_settings.flowfile_node_id),
    )
    try:
        response = stub.StoreDatabaseWriteResult(request)
        return grpc_status_to_model(response)
    except grpc.RpcError as e:
        raise Exception(f"trigger_database_write: gRPC error - {e.code()}: {e.details()}")


def trigger_cloud_storage_write(cloud_storage_write_settings: CloudStorageWriteSettingsWorkerInterface) -> Status:
    """Submit a cloud storage write operation via gRPC."""
    stub = get_worker_stub()
    request = CloudStorageWriteRequest(
        polars_data=cloud_storage_write_settings.operation,
        connection=CloudStorageConnection(
            provider=cloud_storage_write_settings.connection.provider,
            bucket=cloud_storage_write_settings.connection.bucket,
            access_key=cloud_storage_write_settings.connection.access_key,
            secret_key=cloud_storage_write_settings.connection.secret_key,
            region=cloud_storage_write_settings.connection.region or "",
            endpoint_url=cloud_storage_write_settings.connection.endpoint_url or "",
        ),
        write_settings=CloudStorageWriteSettings(
            path=cloud_storage_write_settings.write_settings.path,
            file_type=cloud_storage_write_settings.write_settings.file_type,
            write_mode=cloud_storage_write_settings.write_settings.write_mode,
        ),
        flow_id=cloud_storage_write_settings.flowfile_flow_id,
        node_id=str(cloud_storage_write_settings.flowfile_node_id),
    )
    try:
        response = stub.WriteDataToCloud(request)
        return grpc_status_to_model(response)
    except grpc.RpcError as e:
        raise Exception(f"trigger_cloud_storage_write: gRPC error - {e.code()}: {e.details()}")


def trigger_write_results(
    lf: pl.LazyFrame,
    flow_id: int,
    node_id: int | str,
    data_type: str,
    path: str,
    write_mode: str,
    sheet_name: str | None = None,
    delimiter: str | None = None,
) -> Status:
    """Submit a write results operation via gRPC."""
    stub = get_worker_stub()
    request = WriteResultsRequest(
        polars_data=lf.serialize(),
        data_type=data_type,
        path=path,
        write_mode=write_mode,
        flow_id=flow_id,
        node_id=str(node_id),
    )
    if sheet_name:
        request.sheet_name = sheet_name
    if delimiter:
        request.delimiter = delimiter

    try:
        response = stub.WriteResults(request)
        return grpc_status_to_model(response)
    except grpc.RpcError as e:
        raise Exception(f"trigger_write_results: gRPC error - {e.code()}: {e.details()}")


def get_results(file_ref: str) -> Status:
    """Get task status via gRPC."""
    stub = get_worker_stub()
    request = TaskIdRequest(task_id=file_ref)
    try:
        response = stub.GetStatus(request)
        return grpc_status_to_model(response)
    except grpc.RpcError as e:
        raise Exception(f"get_results: gRPC error - {e.code()}: {e.details()}")


def results_exists(file_ref: str) -> bool:
    """Check if results exist for a task via gRPC."""
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    if not OFFLOAD_TO_WORKER:
        return False

    try:
        status = get_results(file_ref)
        return status.status == "Completed"
    except Exception as e:
        logger.error(f"Failed to check results existence: {str(e)}")
        return False


def clear_task_from_worker(file_ref: str) -> bool:
    """Clear a task from the worker via gRPC."""
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER

    if not OFFLOAD_TO_WORKER:
        return False

    stub = get_worker_stub()
    request = TaskIdRequest(task_id=file_ref)
    try:
        response = stub.ClearTask(request)
        return response.success
    except grpc.RpcError as e:
        logger.error(f"Failed to clear task: {e.code()}: {e.details()}")
        return False


def get_df_result(result_bytes: bytes) -> pl.LazyFrame:
    """Deserialize result bytes to a LazyFrame."""
    return pl.LazyFrame.deserialize(io.BytesIO(result_bytes))


def get_external_df_result(file_ref: str) -> pl.LazyFrame | None:
    """Get the external dataframe result."""
    status = get_results(file_ref)
    if status.status != "Completed":
        raise Exception(f"Status is not completed, {status.status}")
    if status.result_type == "polars":
        return get_df_result(status.results)
    else:
        raise Exception(f"Result type is not polars, {status.result_type}")


def get_status(file_ref: str) -> Status:
    """Get the status of a task."""
    return get_results(file_ref)


def cancel_task(file_ref: str) -> bool:
    """Cancel a running task via gRPC."""
    stub = get_worker_stub()
    request = TaskIdRequest(task_id=file_ref)
    try:
        response = stub.CancelTask(request)
        return response.success
    except grpc.RpcError as e:
        raise Exception(f"Failed to cancel task: {e.code()}: {e.details()}")


class BaseFetcher:
    """
    Thread-safe fetcher for polling worker status and retrieving results via gRPC.
    Uses server streaming for efficient status updates.
    """

    def __init__(self, file_ref: str = None):
        self.file_ref = file_ref if file_ref else str(uuid4())

        # Thread synchronization
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._stop_event = threading.Event()
        self._thread = None

        # State variables
        self._result: Any | None = None
        self._started: bool = False
        self._running: bool = False
        self._error_code: int = 0
        self._error_description: str | None = None

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
        with self._lock:
            self._running = value
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
            self._thread = threading.Thread(target=self._fetch_via_grpc, daemon=True)
            self._thread.start()
            self._started = True

    def _fetch_via_grpc(self):
        """Background thread that fetches results via gRPC streaming."""
        try:
            stub = get_worker_stub()
            request = TaskIdRequest(task_id=self.file_ref)

            # Use streaming for real-time status updates
            for status_response in stub.StreamStatus(request):
                if self._stop_event.is_set():
                    self._handle_cancellation()
                    return

                if status_response.status == "Completed":
                    self._handle_completion(status_response)
                    return
                elif status_response.status == "Error":
                    self._handle_error(1, status_response.error_message)
                    return
                elif status_response.status == "Unknown Error":
                    self._handle_error(
                        -1,
                        "There was an unknown error with the process, and the process got killed by the server",
                    )
                    return
                elif status_response.status == "Cancelled":
                    self._handle_cancellation()
                    return

        except grpc.RpcError as e:
            # Fall back to polling if streaming fails
            self._fetch_via_polling()
        except Exception as e:
            logger.exception("Unexpected error in gRPC fetch thread")
            self._handle_error(-1, f"Unexpected error: {e}")

    def _fetch_via_polling(self):
        """Fallback to polling if streaming is not available."""
        sleep_time = 0.5
        stub = get_worker_stub()
        request = TaskIdRequest(task_id=self.file_ref)

        while not self._stop_event.is_set():
            try:
                response = stub.GetStatus(request)

                if response.status == "Completed":
                    self._handle_completion(response)
                    return
                elif response.status == "Error":
                    self._handle_error(1, response.error_message)
                    return
                elif response.status == "Unknown Error":
                    self._handle_error(
                        -1,
                        "There was an unknown error with the process, and the process got killed by the server",
                    )
                    return

            except grpc.RpcError as e:
                self._handle_error(2, f"gRPC error: {e.code()}: {e.details()}")
                return

            if not self._stop_event.wait(timeout=sleep_time):
                continue
            else:
                break

        self._handle_cancellation()

    def _handle_completion(self, status: StatusResponse):
        """Handle successful completion."""
        with self._condition:
            try:
                if status.result_type == "polars" and status.results:
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
        """Handle error state."""
        with self._condition:
            self._error_code = code
            self._error_description = description
            self._running = False
            self._condition.notify_all()

    def _handle_cancellation(self):
        """Handle cancellation."""
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
        """Cancel the current task both locally and on the worker service."""
        logger.warning("Cancelling the operation")

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
        """Get the result, blocking until it's available."""
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
        with self._lock:
            return self._running

    @property
    def has_error(self) -> bool:
        with self._lock:
            return self._error_description is not None

    @property
    def error_info(self) -> tuple[int, str | None]:
        with self._lock:
            return self._error_code, self._error_description


class ExternalDfFetcher(BaseFetcher):
    """Fetcher for external dataframe operations."""
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
    ):
        super().__init__(file_ref=file_ref)
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf
        r = trigger_df_operation(
            lf=lf, file_ref=self.file_ref, operation_type=operation_type, node_id=node_id, flow_id=flow_id
        )
        self.running = r.status in ("Processing", "Starting")
        if wait_on_completion:
            _ = self.get_result()
        self.status = get_status(self.file_ref)


class ExternalSampler(BaseFetcher):
    """Fetcher for sampling operations."""
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
        r = trigger_sample_operation(
            lf=lf, file_ref=file_ref, sample_size=sample_size, node_id=node_id, flow_id=flow_id
        )
        self.running = r.status in ("Processing", "Starting")
        if wait_on_completion:
            _ = self.get_result()
        self.status = get_status(self.file_ref)


class ExternalFuzzyMatchFetcher(BaseFetcher):
    """Fetcher for fuzzy match operations."""

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
        self.running = r.status in ("Processing", "Starting")
        if wait_on_completion:
            _ = self.get_result()


class ExternalCreateFetcher(BaseFetcher):
    """Fetcher for table creation operations."""

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
        self.running = r.status in ("Processing", "Starting")
        if wait_on_completion:
            _ = self.get_result()


class ExternalDatabaseFetcher(BaseFetcher):
    """Fetcher for database read operations."""

    def __init__(self, database_external_read_settings: DatabaseExternalReadSettings, wait_on_completion: bool = True):
        r = trigger_database_read_collector(database_external_read_settings=database_external_read_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status in ("Processing", "Starting")
        if wait_on_completion:
            _ = self.get_result()


class ExternalDatabaseWriter(BaseFetcher):
    """Fetcher for database write operations."""

    def __init__(
        self, database_external_write_settings: DatabaseExternalWriteSettings, wait_on_completion: bool = True
    ):
        r = trigger_database_write(database_external_write_settings=database_external_write_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status in ("Processing", "Starting")
        if wait_on_completion:
            _ = self.get_result()


class ExternalCloudWriter(BaseFetcher):
    """Fetcher for cloud storage write operations."""

    def __init__(
        self, cloud_storage_write_settings: CloudStorageWriteSettingsWorkerInterface, wait_on_completion: bool = True
    ):
        r = trigger_cloud_storage_write(database_external_write_settings=cloud_storage_write_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status in ("Processing", "Starting")
        if wait_on_completion:
            _ = self.get_result()


class ExternalExecutorTracker:
    """Tracker for external executor operations."""
    result: pl.LazyFrame | None
    started: bool = False
    running: bool = False
    error_code: int = 0
    error_description: str | None = None
    file_ref: str = None

    def __init__(self, initial_response: Status, wait_on_completion: bool = True):
        self.file_ref = initial_response.background_task_id
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._fetch_via_grpc)
        self.result = None
        self.error_description = None
        self.running = initial_response.status in ("Processing", "Starting")
        self.condition = threading.Condition()
        if wait_on_completion:
            _ = self.get_result()

    def _fetch_via_grpc(self):
        with self.condition:
            if self.running:
                logger.info("Already running the fetching")
                return
            sleep_time = 1
            self.running = True

            stub = get_worker_stub()
            request = TaskIdRequest(task_id=self.file_ref)

            while not self.stop_event.is_set():
                try:
                    response = stub.GetStatus(request)

                    if response.status == "Completed":
                        self.running = False
                        self.condition.notify_all()
                        if response.result_type == "polars" and response.results:
                            self.result = get_df_result(response.results)
                        else:
                            self.result = response.results
                        return
                    elif response.status == "Error":
                        self.error_code = 1
                        self.error_description = response.error_message
                        break
                    elif response.status == "Unknown Error":
                        self.error_code = -1
                        self.error_description = (
                            "There was an unknown error with the process, and the process got killed by the server"
                        )
                        break
                except grpc.RpcError as e:
                    self.error_code = 2
                    self.error_description = f"gRPC error: {e.code()}: {e.details()}"
                    break

                sleep(sleep_time)

            logger.warning("Fetch operation cancelled")
            if self.error_description is not None:
                self.running = False
                logger.warning(self.error_description)
                self.condition.notify_all()

    def start(self):
        self.started = True
        if self.running:
            logger.info("Already running the fetching")
            return
        self.thread.start()

    def cancel(self):
        logger.warning("Cancelling the operation")
        self.thread.join()
        self.running = False

    def get_result(self) -> pl.LazyFrame | Any | None:
        if not self.started:
            self.start()
        with self.condition:
            while self.running and self.result is None:
                self.condition.wait()
        if self.error_description is not None:
            raise Exception(self.error_description)
        return self.result


def fetch_unique_values(lf: pl.LazyFrame) -> list[str]:
    """
    Fetches unique values from a LazyFrame.
    """
    try:
        try:
            external_df_fetcher = ExternalDfFetcher(lf=lf, flow_id=1, node_id=-1)
            if external_df_fetcher.status.status == "Completed":
                unique_values = read(external_df_fetcher.status.file_ref).column(0).to_pylist()
                logger.info(f"Got {len(unique_values)} unique values from external source")
                return unique_values
        except Exception as e:
            logger.debug(f"Failed reading external file: {str(e)}")

        unique_values = lf.unique().collect(engine="streaming")[:, 0].to_list()

        if not unique_values:
            raise ValueError("No unique values found in lazyframe")

        return unique_values

    except Exception as e:
        error_msg = f"Failed to fetch unique values: {str(e)}"
        logger.error(error_msg)
        raise ValueError(error_msg) from e
