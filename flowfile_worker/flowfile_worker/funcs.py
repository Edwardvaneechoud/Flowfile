import polars as pl
import io
from typing import List, Dict, Callable
from multiprocessing import Array, Value, Queue
from flowfile_worker.polars_fuzzy_match.matcher import fuzzy_match_dfs
from flowfile_worker.polars_fuzzy_match.models import FuzzyMapping
from base64 import encodebytes
import logging
import os


logging.basicConfig(format='%(asctime)s: %(message)s')
logger = logging.getLogger('Spawner')
logger.setLevel(logging.INFO)


def fuzzy_join_task(left_serializable_object: bytes, right_serializable_object: bytes,
                    fuzzy_maps: List[FuzzyMapping], error_message: Array, file_path: str,
                    progress: Value,
                    queue: Queue
                    ):
    try:
        left_df = pl.LazyFrame.deserialize(io.BytesIO(left_serializable_object))
        right_df = pl.LazyFrame.deserialize(io.BytesIO(right_serializable_object))
        fuzzy_match_result = fuzzy_match_dfs(left_df, right_df, fuzzy_maps)
        fuzzy_match_result.write_ipc(file_path)
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:256]
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1
    lf = pl.scan_ipc(file_path)
    queue.put(encodebytes(lf.serialize()))


def process_and_cache(polars_serializable_object: io.BytesIO, progress: Value, error_message: Array,
                      file_path: str) -> bytes:
    try:
        lf = pl.LazyFrame.deserialize(polars_serializable_object)
        # try:
        #     lf.sink_ipc(file_path)
        # except:
        lf.collect(streaming=True).write_ipc(file_path)
        # Simulate progress update
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:256]  # Limit error message length
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error
        return b'error'


def store(polars_serializable_object: bytes, progress: Value, error_message: Array, queue: Queue,  file_path: str):
    polars_serializable_object_io = io.BytesIO(polars_serializable_object)
    process_and_cache(polars_serializable_object_io, progress, error_message, file_path)
    lf = pl.scan_ipc(file_path)
    queue.put(encodebytes(lf.serialize()))


def calculate_schema_logic(df: pl.LazyFrame, optimize_memory: bool = True) -> List[Dict]:
    schema = df.collect_schema()
    schema_stats = [dict(column_name=k, pl_datatype=str(v), col_index=i) for i, (k, v) in
                    enumerate(schema.items())]
    print('Starting to calculate the number of records')
    try:
        n_records = df.select(pl.len()).collect(streaming=True)[0, 0]
        streaming = True
    except:
        n_records = df.select(pl.len()).collect(streaming=False)[0, 0]
        streaming = False
    if n_records < 10_000:
        print('Collecting the whole dataset')
        df = df.collect(streaming=True).lazy()
    if optimize_memory and n_records > 1_000_000:
        df = df.head(1_000_000)
    null_cols = [col for col, data_type in schema.items() if data_type is pl.Null]
    if not (n_records == 0 and df.width == 0):
        if len(null_cols) == 0:
            pl_stats = df.describe()
        else:
            df = df.drop(null_cols)
            pl_stats = df.describe()
        n_unique_per_cols = list(df.select(pl.all().approx_n_unique()).collect(streaming=streaming).to_dicts()[0].values())
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


def calculate_schema(polars_serializable_object: bytes, progress: Value, error_message: Array, queue: Queue, *args, **kwargs):
    polars_serializable_object_io = io.BytesIO(polars_serializable_object)
    try:
        lf = pl.LazyFrame.deserialize(polars_serializable_object_io)
        schema_stats = calculate_schema_logic(lf)
        print('schema_stats', schema_stats)
        queue.put(schema_stats)
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:256]  # Limit error message length
        print('error', e)
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error


def calculate_number_of_records(polars_serializable_object: bytes, progress: Value, error_message: Array, queue: Queue, *args, **kwargs):
    polars_serializable_object_io = io.BytesIO(polars_serializable_object)
    try:
        lf = pl.LazyFrame.deserialize(polars_serializable_object_io)
        n_records = lf.select(pl.len()).collect(streaming=True)[0, 0]
        queue.put(n_records)
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:256]  # Limit error message length
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1  # Indicate error
        return b'error'


def execute_write_method(write_method: Callable, path: str, data_type: str = None, sheet_name: str = None,
                         delimiter: str = None,
                         write_mode: str = 'create'):
    print('executing write method')
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


def write_output(polars_serializable_object: bytes,
                 progress: Value,
                 error_message: Array,
                 q: Queue,
                 file_ref: str,
                 data_type: str,
                 path: str,
                 write_mode: str,
                 sheet_name: str = None,
                 delimiter: str = None):
    try:
        df = pl.LazyFrame.deserialize(io.BytesIO(polars_serializable_object))
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
                df = df.collect(streaming=True)
            write_method = getattr(df, write_method_str)
        if write_method is not None:
            execute_write_method(write_method, path=path, data_type=data_type, sheet_name=sheet_name,
                                 delimiter=delimiter, write_mode=write_mode)
        else:
            raise Exception('Write method not found')
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_message[:len(str(e))] = str(e).encode()


def generic_task(func: Callable,
                 progress: Value,
                 error_message: Array,
                 queue: Queue,
                 file_path: str,
                 *args, **kwargs):
    try:
        df = func(*args, **kwargs)
        if isinstance(df, pl.LazyFrame):
            df.collect(streaming=True).write_ipc(file_path)
        elif isinstance(df, pl.DataFrame):
            df.write_ipc(file_path)
        else:
            raise Exception('Returned object is not a DataFrame or LazyFrame')
        with progress.get_lock():
            progress.value = 100
    except Exception as e:
        error_msg = str(e).encode()[:256]
        with error_message.get_lock():
            error_message[:len(error_msg)] = error_msg
        with progress.get_lock():
            progress.value = -1
    lf = pl.scan_ipc(file_path)
    queue.put(encodebytes(lf.serialize()))
