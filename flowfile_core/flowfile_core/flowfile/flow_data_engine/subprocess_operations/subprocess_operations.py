# Standard library imports
from base64 import decodebytes, encodebytes
import io
import threading
from time import sleep
from typing import Any, List, Literal, Optional
from uuid import uuid4

import polars as pl
import requests

from pl_fuzzy_frame_match.models import FuzzyMapping

from flowfile_core.configs import logger
from flowfile_core.configs.settings import WORKER_URL
from flowfile_core.flowfile.flow_data_engine.subprocess_operations.models import (
    FuzzyJoinInput,
    OperationType,
    PolarsOperation,
    Status
)
from flowfile_core.flowfile.sources.external_sources.sql_source.models import (DatabaseExternalReadSettings,
                                                                               DatabaseExternalWriteSettings)
from flowfile_core.schemas.cloud_storage_schemas import CloudStorageWriteSettingsWorkerInterface
from flowfile_core.schemas.input_schema import (
    ReceivedTable
)
from flowfile_core.utils.arrow_reader import read


def trigger_df_operation(flow_id: int, node_id: int | str, lf: pl.LazyFrame, file_ref: str, operation_type: OperationType = 'store') -> Status:
    encoded_operation = encodebytes(lf.serialize()).decode()
    _json = {'task_id': file_ref, 'operation': encoded_operation, 'operation_type': operation_type,
             'flowfile_flow_id': flow_id, 'flowfile_node_id': node_id}
    v = requests.post(url=f'{WORKER_URL}/submit_query/', json=_json)
    if not v.ok:
        raise Exception(f'trigger_df_operation: Could not cache the data, {v.text}')
    return Status(**v.json())


def trigger_sample_operation(lf: pl.LazyFrame, file_ref: str, flow_id: int, node_id: str | int, sample_size: int = 100) -> Status:
    encoded_operation = encodebytes(lf.serialize()).decode()
    _json = {'task_id': file_ref, 'operation': encoded_operation, 'operation_type': 'store_sample',
             'sample_size': sample_size, 'flowfile_flow_id': flow_id, 'flowfile_node_id': node_id}
    v = requests.post(url=f'{WORKER_URL}/store_sample/', json=_json)
    if not v.ok:
        raise Exception(f'trigger_sample_operation: Could not cache the data, {v.text}')
    return Status(**v.json())


def trigger_fuzzy_match_operation(left_df: pl.LazyFrame, right_df: pl.LazyFrame,
                                  fuzzy_maps: List[FuzzyMapping],
                                  file_ref: str,
                                  flow_id: int,
                                  node_id: int | str) -> Status:
    left_serializable_object = PolarsOperation(operation=encodebytes(left_df.serialize()))
    right_serializable_object = PolarsOperation(operation=encodebytes(right_df.serialize()))
    fuzzy_join_input = FuzzyJoinInput(left_df_operation=left_serializable_object,
                                      right_df_operation=right_serializable_object,
                                      fuzzy_maps=fuzzy_maps,
                                      task_id=file_ref,
                                      flowfile_flow_id=flow_id,
                                      flowfile_node_id=node_id
                                      )
    print("fuzzy join input", fuzzy_join_input)
    v = requests.post(f'{WORKER_URL}/add_fuzzy_join', data=fuzzy_join_input.model_dump_json())
    if not v.ok:
        raise Exception(f'trigger_fuzzy_match_operation: Could not cache the data, {v.text}')
    return Status(**v.json())


def trigger_create_operation(flow_id: int, node_id: int | str, received_table: ReceivedTable,
                             file_type: str = Literal['csv', 'parquet', 'json', 'excel']):
    f = requests.post(url=f'{WORKER_URL}/create_table/{file_type}', data=received_table.model_dump_json(),
                      params={'flowfile_flow_id': flow_id, 'flowfile_node_id': node_id})
    if not f.ok:
        raise Exception(f'trigger_create_operation: Could not cache the data, {f.text}')
    return Status(**f.json())


def trigger_database_read_collector(database_external_read_settings: DatabaseExternalReadSettings):
    f = requests.post(url=f'{WORKER_URL}/store_database_read_result',
                      data=database_external_read_settings.model_dump_json())
    if not f.ok:
        raise Exception(f'trigger_database_read_collector: Could not cache the data, {f.text}')
    return Status(**f.json())


def trigger_database_write(database_external_write_settings: DatabaseExternalWriteSettings):
    f = requests.post(url=f'{WORKER_URL}/store_database_write_result',
                      data=database_external_write_settings.model_dump_json())
    if not f.ok:
        raise Exception(f'trigger_database_write: Could not cache the data, {f.text}')
    return Status(**f.json())


def trigger_cloud_storage_write(database_external_write_settings: CloudStorageWriteSettingsWorkerInterface):
    f = requests.post(url=f'{WORKER_URL}/write_data_to_cloud',
                      data=database_external_write_settings.model_dump_json())
    if not f.ok:
        raise Exception(f'trigger_cloud_storage_write: Could not cache the data, {f.text}')
    return Status(**f.json())


def get_results(file_ref: str) -> Status | None:
    f = requests.get(f'{WORKER_URL}/status/{file_ref}')
    if f.status_code == 200:
        return Status(**f.json())
    else:
        raise Exception(f'get_results: Could not fetch the data, {f.text}')


def results_exists(file_ref: str):
    try:
        f = requests.get(f'{WORKER_URL}/status/{file_ref}')
        if f.status_code == 200:
            if f.json()['status'] == 'Completed':
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
    try:
        f = requests.delete(f'{WORKER_URL}/clear_task/{file_ref}')
        if f.status_code == 200:
            return True
        return False
    except requests.RequestException as e:
        logger.error(f"Failed to remove results: {str(e)}")
        return False


def get_df_result(encoded_df: str) -> pl.LazyFrame:
    r = decodebytes(encoded_df.encode())
    return pl.LazyFrame.deserialize(io.BytesIO(r))


def get_external_df_result(file_ref: str) -> pl.LazyFrame | None:
    status = get_results(file_ref)
    if status.status != 'Completed':
        raise Exception(f"Status is not completed, {status.status}")
    if status.result_type == 'polars':
        return get_df_result(status.results)
    else:
        raise Exception(f"Result type is not polars, {status.result_type}")


def get_status(file_ref: str) -> Status:
    status_response = requests.get(f'{WORKER_URL}/status/{file_ref}')
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
        response = requests.post(f'{WORKER_URL}/cancel_task/{file_ref}')
        if response.ok:
            return True
        return False
    except requests.RequestException as e:
        raise Exception(f'Failed to cancel task: {str(e)}')


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

        # State variables - use properties for thread-safe access
        self._result: Optional[Any] = None
        self._started: bool = False
        self._running: bool = False
        self._error_code: int = 0
        self._error_description: Optional[str] = None

    # Public properties for compatibility with subclasses
    @property
    def result(self) -> Optional[Any]:
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
    def error_description(self) -> Optional[str]:
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
                    r = requests.get(f'{WORKER_URL}/status/{self.file_ref}', timeout=10)

                    if r.status_code == 200:
                        status = Status(**r.json())

                        if status.status == 'Completed':
                            self._handle_completion(status)
                            return
                        elif status.status == 'Error':
                            self._handle_error(1, status.error_message)
                            return
                        elif status.status == 'Unknown Error':
                            self._handle_error(
                                -1,
                                'There was an unknown error with the process, '
                                'and the process got killed by the server'
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
            # Catch any unexpected errors
            logger.exception("Unexpected error in fetch thread")
            self._handle_error(-1, f"Unexpected error: {e}")

    def _handle_completion(self, status):
        """Handle successful completion. Must be called from fetch thread."""
        with self._condition:
            try:
                if status.result_type == 'polars':
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
                logger.info('Fetcher already started')
                return
            if self._running:
                logger.info('Already running the fetching')
                return

            self._running = True
            self._start_thread()

    def cancel(self):
        """
        Cancels the current task both locally and on the worker service.
        Also cleans up any resources being used.
        """
        logger.warning('Cancelling the operation')

        # Cancel on the worker side
        try:
            cancel_task(self.file_ref)
        except Exception as e:
            logger.error(f'Failed to cancel task on worker: {str(e)}')

        # Signal the thread to stop
        self._stop_event.set()

        # Wait for thread to finish
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                logger.warning("Fetch thread did not stop within timeout")

    def get_result(self) -> Optional[Any]:
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

        # Wait for completion
        with self._condition:
            while self._running:
                self._condition.wait()

        # Check for errors
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
    def error_info(self) -> tuple[int, Optional[str]]:
        """Get error code and description."""
        with self._lock:
            return self._error_code, self._error_description


class ExternalDfFetcher(BaseFetcher):
    status: Optional[Status] = None

    def __init__(self, flow_id: int, node_id: int | str, lf: pl.LazyFrame | pl.DataFrame, file_ref: str = None,
                 wait_on_completion: bool = True,
                 operation_type: OperationType = 'store', offload_to_worker: bool = True):
        super().__init__(file_ref=file_ref)
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf
        r = trigger_df_operation(lf=lf, file_ref=self.file_ref, operation_type=operation_type,
                                 node_id=node_id, flow_id=flow_id)
        self.running = r.status == 'Processing'
        if wait_on_completion:
            _ = self.get_result()
        self.status = get_status(self.file_ref)


class ExternalSampler(BaseFetcher):
    status: Optional[Status] = None

    def __init__(self, lf: pl.LazyFrame | pl.DataFrame, node_id: str | int, flow_id: int, file_ref: str = None, wait_on_completion: bool = True,
                 sample_size: int = 100):
        super().__init__(file_ref=file_ref)
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf
        r = trigger_sample_operation(lf=lf, file_ref=file_ref, sample_size=sample_size, node_id=node_id, flow_id=flow_id)
        self.running = r.status == 'Processing'
        if wait_on_completion:
            _ = self.get_result()
        self.status = get_status(self.file_ref)


class ExternalFuzzyMatchFetcher(BaseFetcher):
    def __init__(self, left_df: pl.LazyFrame, right_df: pl.LazyFrame, fuzzy_maps: List[Any], flow_id: int,
                 node_id: int | str,
                 file_ref: str = None,
                 wait_on_completion: bool = True):
        super().__init__(file_ref=file_ref)

        r = trigger_fuzzy_match_operation(left_df=left_df, right_df=right_df, fuzzy_maps=fuzzy_maps,
                                          file_ref=file_ref, flow_id=flow_id, node_id=node_id)
        self.file_ref = r.background_task_id
        self.running = r.status == 'Processing'
        if wait_on_completion:
            _ = self.get_result()


class ExternalCreateFetcher(BaseFetcher):
    def __init__(self, received_table: ReceivedTable, node_id: int, flow_id: int,
                 file_type: str = 'csv', wait_on_completion: bool = True):
        r = trigger_create_operation(received_table=received_table, file_type=file_type,
                                     node_id=node_id, flow_id=flow_id)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == 'Processing'
        if wait_on_completion:
            _ = self.get_result()


class ExternalDatabaseFetcher(BaseFetcher):
    def __init__(self, database_external_read_settings: DatabaseExternalReadSettings,
                 wait_on_completion: bool = True):
        r = trigger_database_read_collector(database_external_read_settings=database_external_read_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == 'Processing'
        if wait_on_completion:
            _ = self.get_result()


class ExternalDatabaseWriter(BaseFetcher):
    def __init__(self, database_external_write_settings: DatabaseExternalWriteSettings,
                 wait_on_completion: bool = True):
        r = trigger_database_write(database_external_write_settings=database_external_write_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == 'Processing'
        if wait_on_completion:
            _ = self.get_result()


class ExternalCloudWriter(BaseFetcher):

    def __init__(self, cloud_storage_write_settings: CloudStorageWriteSettingsWorkerInterface,
                 wait_on_completion: bool = True):
        r = trigger_cloud_storage_write(database_external_write_settings=cloud_storage_write_settings)
        super().__init__(file_ref=r.background_task_id)
        self.running = r.status == 'Processing'
        if wait_on_completion:
            _ = self.get_result()


class ExternalExecutorTracker:
    result: Optional[pl.LazyFrame]
    started: bool = False
    running: bool = False
    error_code: int = 0
    error_description: Optional[str] = None
    file_ref: str = None

    def __init__(self, initial_response: Status, wait_on_completion: bool = True):
        self.file_ref = initial_response.background_task_id
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._fetch_cached_df)
        self.result = None
        self.error_description = None
        self.running = initial_response.status == 'Processing'
        self.condition = threading.Condition()
        if wait_on_completion:
            _ = self.get_result()

    def _fetch_cached_df(self):
        with self.condition:
            if self.running:
                logger.info('Already running the fetching')
                return
            sleep_time = 1
            self.running = True
            while not self.stop_event.is_set():
                try:
                    r = requests.get(f'{WORKER_URL}/status/{self.file_ref}')
                    if r.status_code == 200:
                        status = Status(**r.json())
                        if status.status == 'Completed':
                            self.running = False
                            self.condition.notify_all()  # Notify all waiting threads
                            if status.result_type == 'polars':
                                self.result = get_df_result(status.results)
                            else:
                                self.result = status.results
                            return
                        elif status.status == 'Error':
                            self.error_code = 1
                            self.error_description = status.error_message
                            break
                        elif status.status == 'Unknown Error':
                            self.error_code = -1
                            self.error_description = 'There was an unknown error with the process, and the process got killed by the server'
                            break
                    else:
                        self.error_description = r.text
                        self.error_code = 2
                        break
                except requests.RequestException as e:
                    self.error_code = 2
                    self.error_description = f"Request failed: {e}"
                    break

                sleep(sleep_time)
                # logger.info('Fetching the data')

            logger.warning("Fetch operation cancelled")
            if self.error_description is not None:
                self.running = False
                logger.warning(self.error_description)
                self.condition.notify_all()
                return

    def start(self):
        self.started = True
        if self.running:
            logger.info('Already running the fetching')
            return
        self.thread.start()

    def cancel(self):
        logger.warning('Cancelling the operation')
        self.thread.join()

        self.running = False

    def get_result(self) -> pl.LazyFrame | Any | None:
        if not self.started:
            self.start()
        with self.condition:
            while self.running and self.result is None:
                self.condition.wait()  # Wait until notified
        if self.error_description is not None:
            raise Exception(self.error_description)
        return self.result


def fetch_unique_values(lf: pl.LazyFrame) -> List[str]:
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
        # Try external source first if lf is provided
        try:
            external_df_fetcher = ExternalDfFetcher(lf=lf, flow_id=1, node_id=-1)
            if external_df_fetcher.status.status == 'Completed':

                unique_values = read(external_df_fetcher.status.file_ref).column(0).to_pylist()
                if logger:
                    logger.info(f"Got {len(unique_values)} unique values from external source")
                return unique_values
        except Exception as e:
            if logger:
                logger.debug(f"Failed reading external file: {str(e)}")

        unique_values = (lf.unique().collect(engine="streaming")[:, 0].to_list())

        if not unique_values:
            raise ValueError(f"No unique values found in lazyframe")

        return unique_values

    except Exception as e:
        error_msg = f"Failed to fetch unique values: {str(e)}"
        if logger:
            logger.error(error_msg)
        raise ValueError(error_msg) from e
