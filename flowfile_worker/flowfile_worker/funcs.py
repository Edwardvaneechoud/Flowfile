import polars as pl
import io
from typing import List, Dict, Callable
from multiprocessing import Array, Value, Queue
from flowfile_worker.polars_fuzzy_match.matcher import fuzzy_match_dfs
from flowfile_worker.polars_fuzzy_match.models import FuzzyMapping
from flowfile_worker.flow_logger import get_worker_logger
from flowfile_worker.external_sources.sql_source.models import DatabaseWriteSettings
from flowfile_worker.external_sources.sql_source.main import write_serialized_df_to_database, write_df_to_database
from base64 import encodebytes
from logging import Logger
import logging
import os
from flowfile_worker.utils import collect_lazy_frame, collect_lazy_frame_and_get_streaming_info


# 'store', 'calculate_schema', 'calculate_number_of_records', 'write_output', 'fuzzy', 'store_sample']

logging.basicConfig(format='%(asctime)s: %(message)s')
logger = logging.getLogger('Spawner')
logger.setLevel(logging.INFO)


def fuzzy_join_task(left_serializable_object: bytes, right_serializable_object: bytes,
                    fuzzy_maps: List[FuzzyMapping], error_message: Array, file_path: str,
                    progress: Value,
                    queue: Queue, flowfile_flow_id: int, flowfile_node_id: int | str,
                    ):
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    try:
        flowfile_logger.info("Starting fuzzy join operation")
        left_df = pl.LazyFrame.deserialize(io.BytesIO(left_serializable_object))
        right_df = pl.LazyFrame.deserialize(io.BytesIO(right_serializable_object))
        fuzzy_match_result = fuzzy_match_dfs(left_df, right_df, fuzzy_maps, flowfile_logger)
        flowfile_logger.info("Fuzzy join operation completed successfully")
        fuzzy_match_result.write_ipc(file_path)
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:256]
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1
        flowfile_logger.error(f'Error during fuzzy join operation: {str(e)}')
    lf = pl.scan_ipc(file_path)
    number_of_records = collect_lazy_frame(lf.select(pl.len()))[0, 0]
    flowfile_logger.info(f'Number of records after fuzzy match: {number_of_records}')
    queue.put(encodebytes(lf.serialize()))


def process_and_cache(polars_serializable_object: io.BytesIO, progress: Value, error_message: Array,
                      file_path: str, flowfile_logger: Logger) -> bytes:
    try:
        lf = pl.LazyFrame.deserialize(polars_serializable_object)
        collect_lazy_frame(lf).write_ipc(file_path)
        flowfile_logger.info("Process operation completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:1024]  # Limit error message length
        flowfile_logger.error(f'Error during process and cache operation: {str(e)}')
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error
        return error_msg


def store_sample(polars_serializable_object: bytes,
                 progress: Value,
                 error_message: Array,
                 queue: Queue,
                 file_path: str,
                 sample_size: int,
                 flowfile_flow_id: int,
                 flowfile_node_id: int | str
                 ):
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info("Starting store sample operation")
    try:
        lf = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
        collect_lazy_frame(lf.limit(sample_size)).write_ipc(file_path)
        flowfile_logger.info("Store sample operation completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        flowfile_logger.error(f'Error during store sample operation: {str(e)}')
        error_msg = str(e).encode()[:1024]  # Limit error message length
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error
        return error_msg


def store(polars_serializable_object: bytes, progress: Value, error_message: Array, queue: Queue,  file_path: str,
          flowfile_flow_id: int, flowfile_node_id: int | str):
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info("Starting store operation")
    polars_serializable_object_io = io.BytesIO(polars_serializable_object)
    process_and_cache(polars_serializable_object_io, progress, error_message, file_path, flowfile_logger)
    lf = pl.scan_ipc(file_path)
    number_of_records = collect_lazy_frame(lf.select(pl.len()))[0, 0]
    flowfile_logger.info(f'Number of records processed: {number_of_records}')
    queue.put(encodebytes(lf.serialize()))


def calculate_schema_logic(df: pl.LazyFrame, optimize_memory: bool = True, flowfile_logger: Logger = None) -> List[Dict]:
    if flowfile_logger is None:
        raise ValueError('flowfile_logger is required')
    schema = df.collect_schema()
    schema_stats = [dict(column_name=k, pl_datatype=str(v), col_index=i) for i, (k, v) in
                    enumerate(schema.items())]
    flowfile_logger.info('Starting to calculate the number of records')
    collected_streaming_info = collect_lazy_frame_and_get_streaming_info(df.select(pl.len()))
    n_records = collected_streaming_info.df[0, 0]
    if n_records < 10_000:
        flowfile_logger.info('Collecting the whole dataset')
        df = collect_lazy_frame(df).lazy()
    if optimize_memory and n_records > 1_000_000:
        df = df.head(1_000_000)
    null_cols = [col for col, data_type in schema.items() if data_type is pl.Null]
    if not (n_records == 0 and df.width == 0):
        if len(null_cols) == 0:
            pl_stats = df.describe()
        else:
            df = df.drop(null_cols)
            pl_stats = df.describe()
        n_unique_per_cols = list(df.select(pl.all().approx_n_unique()).collect(
            engine="streaming" if collected_streaming_info.streaming_collect_available else "auto").to_dicts()[0].values()
                                 )
        stats_headers = pl_stats.drop_in_place('statistic').to_list()
        stats = {v['column_name']: v for v in pl_stats.transpose(include_header=True, header_name='column_name',
                                                                 column_names=stats_headers).to_dicts()}
        for i, (col_stat, n_unique_values) in enumerate(zip(stats.values(), n_unique_per_cols)):
            col_stat['n_unique'] = n_unique_values
            col_stat['examples'] = ', '.join({str(col_stat['min']), str(col_stat['max'])})
            col_stat['null_count'] = int(float(col_stat['null_count']))
            col_stat['count'] = int(float(col_stat['count']))

        for schema_stat in schema_stats:
            deep_stat = stats.get(schema_stat['column_name'])
            if deep_stat:
                schema_stat.update(deep_stat)
        del df
    else:
        schema_stats = []
    return schema_stats


def calculate_schema(polars_serializable_object: bytes, progress: Value, error_message: Array, queue: Queue,
                     flowfile_flow_id: int, flowfile_node_id: int | str, *args, **kwargs):
    polars_serializable_object_io = io.BytesIO(polars_serializable_object)
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info("Starting schema calculation")
    try:
        lf = pl.LazyFrame.deserialize(polars_serializable_object_io)
        schema_stats = calculate_schema_logic(lf, flowfile_logger=flowfile_logger)
        flowfile_logger.info('schema_stats', schema_stats)
        queue.put(schema_stats)
        flowfile_logger.info("Schema calculation completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:256]  # Limit error message length
        flowfile_logger.error('error', e)
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error


def calculate_number_of_records(polars_serializable_object: bytes, progress: Value, error_message: Array,
                                queue: Queue, flowfile_flow_id: int, *args, **kwargs):
    flowfile_logger = get_worker_logger(flowfile_flow_id, -1)
    flowfile_logger.info("Starting number of records calculation")
    polars_serializable_object_io = io.BytesIO(polars_serializable_object)
    try:
        lf = pl.LazyFrame.deserialize(polars_serializable_object_io)
        n_records = collect_lazy_frame(lf.select(pl.len()))[0, 0]
        queue.put(n_records)
        flowfile_logger.debug("Number of records calculation completed successfully")
        flowfile_logger.debug(f'n_records {n_records}')
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        flowfile_logger.error('error', e)
        error_msg = str(e).encode()[:256]  # Limit error message length
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error
        return b'error'


def execute_write_method(write_method: Callable, path: str, data_type: str = None, sheet_name: str = None,
                         delimiter: str = None,
                         write_mode: str = 'create', flowfile_logger: Logger = None):
    flowfile_logger.info('executing write method')
    if data_type == 'excel':
        logger.info('Writing as excel file')
        write_method(path, worksheet=sheet_name)
    elif data_type == 'csv':
        logger.info('Writing as csv file')
        if write_mode == 'append':
            with open(path, 'ab') as f:
                write_method(file=f, separator=delimiter, quote_style='always')
        else:
            write_method(file=path, separator=delimiter, quote_style='always')
    elif data_type == 'parquet':
        logger.info('Writing as parquet file')
        write_method(path)


def write_to_database(polars_serializable_object: bytes,
                      progress: Value,
                      error_message: Array,
                      queue: Queue,
                      file_path: str,
                      database_write_settings: DatabaseWriteSettings,
                      flowfile_flow_id: int = -1,
                      flowfile_node_id: int | str = -1
                      ):
    """
    Writes a Polars DataFrame to a SQL database.
    """
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting write operation to: {database_write_settings.table_name}")
    df = collect_lazy_frame(pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object)))
    flowfile_logger.info(f"Starting to write {len(df)} records")
    try:
        write_df_to_database(df, database_write_settings)
        flowfile_logger.info("Write operation completed successfully")
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:1024]
        flowfile_logger.error(f'Error during write operation: {str(e)}')
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1


def write_output(polars_serializable_object: bytes,
                 progress: Value,
                 error_message: Array,
                 queue: Queue,
                 file_path: str,
                 data_type: str,
                 path: str,
                 write_mode: str,
                 sheet_name: str = None,
                 delimiter: str = None,
                 flowfile_flow_id: int = -1,
                 flowfile_node_id: int | str = -1
                 ):
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info(f"Starting write operation to: {path}")
    try:
        df = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
        if isinstance(df, pl.LazyFrame):
            flowfile_logger.info(f'Execution plan explanation:\n{df.explain(format="plain")}')
        flowfile_logger.info("Successfully deserialized dataframe")
        is_lazy = False
        sink_method_str = 'sink_'+data_type
        write_method_str = 'write_'+data_type
        has_sink_method = hasattr(df, sink_method_str)
        write_method = None
        if os.path.exists(path) and write_mode == 'create':
            raise Exception('File already exists')
        if has_sink_method and is_lazy:
            write_method = getattr(df, 'sink_' + data_type)
        elif not is_lazy or not has_sink_method:
            if isinstance(df, pl.LazyFrame):
                df = collect_lazy_frame(df)
            write_method = getattr(df, write_method_str)
        if write_method is not None:
            execute_write_method(write_method, path=path, data_type=data_type, sheet_name=sheet_name,
                                 delimiter=delimiter, write_mode=write_mode, flowfile_logger=flowfile_logger)
            number_of_records_written = (collect_lazy_frame(df.select(pl.len()))[0, 0]
                                         if isinstance(df, pl.LazyFrame) else df.height)
            flowfile_logger.info(f'Number of records written: {number_of_records_written}')
        else:
            raise Exception('Write method not found')
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        logger.info(f'Error during write operation: {str(e)}')
        error_message[:len(str(e))] = str(e).encode()


def generic_task(func: Callable,
                 progress: Value,
                 error_message: Array,
                 queue: Queue,
                 file_path: str,
                 flowfile_flow_id: int,
                 flowfile_node_id: int | str,
                 *args, **kwargs):
    print(kwargs)
    flowfile_logger = get_worker_logger(flowfile_flow_id, flowfile_node_id)
    flowfile_logger.info("Starting generic task")
    try:
        df = func(*args, **kwargs)
        if isinstance(df, pl.LazyFrame):
            collect_lazy_frame(df).write_ipc(file_path)
        elif isinstance(df, pl.DataFrame):
            df.write_ipc(file_path)
        else:
            raise Exception('Returned object is not a DataFrame or LazyFrame')
        with progress.get_lock():
            progress.value = 100
        flowfile_logger.info("Task completed successfully")
    except Exception as e:
        flowfile_logger.error(f'Error during task execution: {str(e)}')
        error_msg = str(e).encode()[:1024]
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1

    lf = pl.scan_ipc(file_path)
    number_of_records = collect_lazy_frame(lf.select(pl.len()))[0, 0]
    flowfile_logger.info(f'Number of records processed: {number_of_records}')
    queue.put(encodebytes(lf.serialize()))
