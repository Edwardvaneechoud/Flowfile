import polars as pl
from flowfile_core.configs.settings import AVAILABLE_RAM, WORKER_URL
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import ExternalDfFetcher
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import Status
import os
from typing import List, Dict, Iterable, Callable, Any
from itertools import chain
import requests
from base64 import encodebytes


def convert_to_string(v):
    try:
        return str(v)
    except:
        return None


def standardize_col_dtype(vals):
    types = set(type(val) for val in vals)
    if len(types) == 1:
        return vals
    elif int in types and float in types:
        return vals
    else:
        return [convert_to_string(v) for v in vals]


def get_data_type(vals: Iterable[Any]):
    types = set(type(val) for val in vals)
    if len(types) == 1:
        return types.pop().__name__
    elif types == {float, int}:
        return 'float'
    else:
        return 'str'


def ensure_similarity_dicts(datas: List[Dict], respect_order: bool = True):
    all_cols = (data.keys() for data in datas)
    if not respect_order:
        unique_cols = set(chain(*all_cols))
    else:
        col_store = set()
        unique_cols = list()
        for row in all_cols:
            for col in row:
                if col not in col_store:
                    unique_cols.append(col)
                    col_store.update((col,))
    output = []
    for data in datas:
        new_record = dict()
        for col in unique_cols:
            val = data.get(col)
            new_record[col] = val
        output.append(new_record)
    return output


def calculate_schema(lf: pl.LazyFrame) -> List[Dict]:
    r = ExternalDfFetcher(lf=lf, operation_type='calculate_schema', wait_on_completion=False, flow_id=-1, node_id=-1)
    schema_stats: List[Dict] = r.get_result()
    for schema_stat in schema_stats:
        schema_stat['pl_datatype'] = getattr(pl.datatypes, schema_stat['pl_datatype'])
    return schema_stats


def write_polars_frame(_df: pl.LazyFrame | pl.DataFrame, path: str, data_type: str = 'parquet',
                       estimated_size: int = 0):
    is_lazy = isinstance(_df, pl.LazyFrame)
    logger.info('Caching data frame')
    if is_lazy:
        if estimated_size > 0:
            fit_memory = estimated_size / 1024 / 1000 / 1000 < AVAILABLE_RAM
            if fit_memory:
                _df = _df.collect()
                is_lazy = False

        if is_lazy:
            logger.info("Writing in memory efficient mode")
            write_method = getattr(_df, 'sink_' + data_type)
            try:
                write_method(path)
                return True
            except Exception as e:
                pass
        if is_lazy:
            _df = _df.collect()
    try:
        write_method = getattr(_df, 'write_' + data_type)
        write_method(path)
        return True
    except:
        return False


def collect(df: pl.LazyFrame, streamable: bool = True):
    try:
        return df.collect(engine="streaming" if streamable else "auto")
    except:
        return df.collect(engine="auto")


def cache_polars_frame_to_temp(_df: pl.LazyFrame | pl.DataFrame, tempdir: str = None) -> pl.LazyFrame:
    path = f'{tempdir}\\fl_file_{id(_df)}'
    result = write_polars_frame(_df, path)
    if result:
        df = pl.read_parquet(path)
        return df.lazy()
    else:
        raise Exception('Could not cache the data')


def define_pl_col_transformation(col_name: str, col_type: pl.DataType) -> pl.Expr:
    if col_type == pl.Datetime:
        return pl.col(col_name).str.to_datetime(strict=False)
    elif col_type == pl.Date:
        return pl.col(col_name).str.to_date(strict=False)
    else:
        return pl.col(col_name).cast(col_type, strict=False)


def execute_write_method(write_method: Callable, path: str, data_type: str = None, sheet_name: str = None,
                         delimiter: str = None,
                         write_mode: str = 'create'):
    if data_type == 'excel':
        logger.info('Writing as excel file')
        write_method(path, worksheet=sheet_name)
    elif data_type == 'csv':
        logger.info('Writing as csv file')
        if write_mode == 'append':
            with open(path, 'ab') as f:
                write_method(path=f, separator=delimiter, quote_style='always')
        else:
            write_method(path=path, separator=delimiter, quote_style='always')
    elif data_type == 'parquet':
        logger.info('Writing as parquet file')
        write_method(path)


def write_output(_df: pl.LazyFrame,
                 data_type: str, path: str, write_mode: str, sheet_name: str = None,
                 delimiter: str = None, flow_id: int = -1, node_id: int | str = -1) -> Status:
    serializable_df = _df.serialize()
    r = requests.post(f'{WORKER_URL}/write_results/',
                      json={'operation': encodebytes(serializable_df).decode(),
                            'data_type': data_type,
                            'path': path,
                            'write_mode': write_mode,
                            'sheet_name': sheet_name,
                            'delimiter': delimiter,
                            'flowfile_node_id': node_id,
                            'flowfile_flow_id': flow_id})
    if r.ok:
        return Status(**r.json())
    else:
        raise Exception(f'Could not cache the data, {r.text}')


def local_write_output(_df: pl.LazyFrame | pl.DataFrame, data_type: str, path: str, write_mode: str,
                       sheet_name: str = None, delimiter: str = None, flow_id: int = -1, node_id: int | str = -1):
    is_lazy = isinstance(_df, pl.LazyFrame)
    sink_method_str = 'sink_' + data_type
    write_method_str = 'write_' + data_type
    has_sink_method = hasattr(_df, sink_method_str)
    write_method = None
    if os.path.exists(path) and write_mode == 'create':
        return None
    if has_sink_method and is_lazy:
        write_method = getattr(_df, 'sink_' + data_type)
    elif not is_lazy or not has_sink_method:
        if is_lazy:
            _df = _df.collect()
        write_method = getattr(_df, write_method_str)
    if write_method is not None:
        execute_write_method(write_method, path=path, data_type=data_type, sheet_name=sheet_name,
                             delimiter=delimiter, write_mode=write_mode)


def create_pl_df_type_save(raw_data: Iterable[Iterable], orient: str = 'row') -> pl.DataFrame:
    """
        orient : {'col', 'row'}, default None
        Whether to interpret two-dimensional data as columns or as rows. If None,
        the orientation is inferred by matching the columns and data dimensions. If
        this does not yield conclusive results, column orientation is used.
    :param raw_data: iterables with values
    :param orient:
    :return: polars dataframe
    """
    if orient == 'row':
        raw_data = zip(*raw_data)
    raw_data = [standardize_col_dtype(values) for values in raw_data]
    return pl.DataFrame(raw_data, orient='col')


def find_first_positions(lst: List[str]) -> Dict[str, int]:
    first_positions: Dict[str, int] = {}
    for i, value in enumerate(lst):
        if value not in first_positions:
            first_positions[value] = i
    return first_positions


def match_order(l: List[str], ref: List[str]) -> List[str]:
    ref_order = find_first_positions(ref)
    order = []
    for v in l:
        org_order = ref_order.get(v, float('inf'))
        order.append(org_order)
    return [v for _, v in sorted(zip(order, l))]
