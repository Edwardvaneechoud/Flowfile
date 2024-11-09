from typing import Optional, Literal, Any, List
import polars as pl
import io
from flowfile_core.configs import logger
from flowfile_core.configs.settings import WORKER_URL
import threading
from time import sleep
import requests
from uuid import uuid4
from base64 import decodebytes, encodebytes
from flowfile_core.flowfile.flowfile_table.subprocess_operations.models import (OperationType, Status, FuzzyJoinInput,
                                                                                FuzzyMap,
                                                                                PolarsOperation)
from flowfile_core.schemas.input_schema import ReceivedCsvTable, ReceivedParquetTable, ReceivedJsonTable, \
    ReceivedExcelTable

ReceivedTableCollection = ReceivedCsvTable | ReceivedParquetTable | ReceivedJsonTable | ReceivedExcelTable


def trigger_df_operation(lf: pl.LazyFrame, file_ref: str, operation_type: OperationType = 'store') -> Status:
    encoded_operation = encodebytes(lf.serialize()).decode()
    v = requests.post(url=f'{WORKER_URL}/submit_query/', json={'task_id': file_ref,
                                                               'operation': encoded_operation,
                                                               'operation_type': operation_type})
    if not v.ok:
        raise Exception(f'Could not cache the data, {v.text}')
    return Status(**v.json())


def trigger_fuzzy_match_operation(left_df: pl.LazyFrame, right_df: pl.LazyFrame,
                                  fuzzy_maps: List[FuzzyMap],
                                  file_ref: str) -> Status:
    left_serializable_object = PolarsOperation(operation=encodebytes(left_df.serialize()))
    right_serializable_object = PolarsOperation(operation=encodebytes(right_df.serialize()))
    fuzzy_join_input = FuzzyJoinInput(left_df_operation=left_serializable_object,
                                      right_df_operation=right_serializable_object,
                                      fuzzy_maps=fuzzy_maps,
                                      task_id=file_ref
                                      )
    v = requests.post(f'{WORKER_URL}/add_fuzzy_join', data=fuzzy_join_input.json())
    if not v.ok:
        raise Exception(f'Could not cache the data, {v.text}')
    return Status(**v.json())


def trigger_create_operation(received_table: ReceivedTableCollection,
                             file_type: str = Literal['csv', 'parquet', 'json', 'excel']):
    f = requests.post(url=f'{WORKER_URL}/create_table/{file_type}', data=received_table.json())
    if not f.ok:
        raise Exception(f'Could not cache the data, {f.text}')
    return Status(**f.json())


def get_results(file_ref: str) -> Status | None:
    f = requests.get(f'{WORKER_URL}/status/{file_ref}')
    if f.status_code == 200:
        return Status(**f.json())
    else:
        raise Exception(f'Could not fetch the data, {f.text}')


def results_exists(file_ref: str):
    f = requests.get(f'{WORKER_URL}/status/{file_ref}')
    if f.status_code == 200:
        if f.json()['status'] == 'Completed':
            return True
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


class BaseFetcher:
    result: Optional[Any] = None
    started: bool = False
    running: bool = False
    error_code: int = 0
    error_description: Optional[str] = None
    file_ref: Optional[str] = None

    def __init__(self, file_ref: str = None):
        self.file_ref = file_ref if file_ref else str(uuid4())
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._fetch_cached_df)
        self.result = None
        self.error_description = None
        self.running = False
        self.started = False
        self.condition = threading.Condition()
        self.error_code = 0

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
                            self._handle_completion(status)
                            return
                        elif status.status == 'Error':
                            self._handle_error(1, status.error_message)
                            break
                        elif status.status == 'Unknown Error':
                            self._handle_error(-1,
                                               'There was an unknown error with the process, and the process got killed by the server')
                            break
                    else:
                        self._handle_error(2, r.text)
                        break
                except requests.RequestException as e:
                    self._handle_error(2, f"Request failed: {e}")
                    break

                sleep(sleep_time)

            self._handle_cancellation()

    def _handle_completion(self, status):
        self.running = False
        self.condition.notify_all()
        if status.result_type == 'polars':
            self.result = get_df_result(status.results)
        else:
            self.result = status.results

    def _handle_error(self, code, description):
        self.error_code = code
        self.error_description = description
        self.running = False
        self.condition.notify_all()

    def _handle_cancellation(self):
        logger.warning("Fetch operation cancelled")
        if self.error_description is not None:
            logger.warning(self.error_description)
        self.running = False
        self.condition.notify_all()

    def start(self):
        if self.running:
            logger.info('Already running the fetching')
            return
        if not self.started:
            self.thread.start()
            self.started = True

    def cancel(self):
        logger.warning('Cancelling the operation')
        self.stop_event.set()
        self.thread.join()

    def get_result(self) -> Optional[Any]:
        if not self.started:
            self.start()
        with self.condition:
            while self.running and self.result is None:
                self.condition.wait()  # Wait until notified
        if self.error_description is not None:
            raise Exception(self.error_description)
        return self.result


class ExternalDfFetcher(BaseFetcher):
    def __init__(self, lf: pl.LazyFrame | pl.DataFrame, file_ref: str = None, wait_on_completion: bool = True,
                 operation_type: OperationType = 'store'):
        super().__init__(file_ref=file_ref)
        lf = lf.lazy() if isinstance(lf, pl.DataFrame) else lf
        r = trigger_df_operation(lf=lf, file_ref=self.file_ref, operation_type=operation_type)
        self.running = r.status == 'Processing'
        if wait_on_completion:
            _ = self.get_result()


class ExternalFuzzyMatchFetcher(BaseFetcher):
    def __init__(self, left_df: pl.LazyFrame, right_df: pl.LazyFrame, fuzzy_maps: List[Any], file_ref: str = None,
                 wait_on_completion: bool = True):
        super().__init__(file_ref=file_ref)

        r = trigger_fuzzy_match_operation(left_df=left_df, right_df=right_df, fuzzy_maps=fuzzy_maps, file_ref=file_ref)
        self.file_ref = r.background_task_id
        self.running = r.status == 'Processing'
        if wait_on_completion:
            _ = self.get_result()


class ExternalCreateFetcher(BaseFetcher):
    def __init__(self, received_table: ReceivedTableCollection, file_type: str = 'csv',
                 wait_on_completion: bool = True):
        r = trigger_create_operation(received_table=received_table, file_type=file_type)
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
