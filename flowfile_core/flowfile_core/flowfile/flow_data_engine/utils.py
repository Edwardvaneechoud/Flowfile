import os
from collections.abc import Callable, Iterable
from typing import Any

import polars as pl

from flowfile_core.configs import logger
from flowfile_core.configs.settings import AVAILABLE_RAM
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import ExternalDfFetcher
from flowfile_core.utils.utils import standardize_col_dtype


def get_data_type(vals: Iterable[Any]):
    types = set(type(val) for val in vals)
    if len(types) == 1:
        return types.pop().__name__
    elif types == {float, int}:
        return "float"
    else:
        return "str"


def calculate_schema(lf: pl.LazyFrame) -> list[dict]:
    r = ExternalDfFetcher(lf=lf, operation_type="calculate_schema", wait_on_completion=False, flow_id=-1, node_id=-1)
    schema_stats: list[dict] = r.get_result()

    for schema_stat in schema_stats:
        schema_stat["pl_datatype"] = getattr(pl.datatypes, schema_stat["pl_datatype"])

    return schema_stats


def write_polars_frame(
    _df: pl.LazyFrame | pl.DataFrame, path: str, data_type: str = "parquet", estimated_size: int = 0
):
    is_lazy = isinstance(_df, pl.LazyFrame)
    logger.info("Caching data frame")
    if is_lazy:
        if estimated_size > 0:
            fit_memory = estimated_size / 1024 / 1000 / 1000 < AVAILABLE_RAM
            if fit_memory:
                _df = _df.collect()
                is_lazy = False

        if is_lazy:
            logger.info("Writing in memory efficient mode")
            write_method = getattr(_df, "sink_" + data_type)
            try:
                write_method(path)
                return True
            except Exception:
                pass
        if is_lazy:
            _df = _df.collect()
    try:
        write_method = getattr(_df, "write_" + data_type)
        write_method(path)
        return True
    except Exception:
        return False


def collect(df: pl.LazyFrame, streamable: bool = True):
    try:
        return df.collect(engine="streaming" if streamable else "auto")
    except Exception:
        return df.collect(engine="auto")


def cache_polars_frame_to_temp(_df: pl.LazyFrame | pl.DataFrame, tempdir: str = None) -> pl.LazyFrame:
    path = f"{tempdir}\\fl_file_{id(_df)}"
    result = write_polars_frame(_df, path)
    if result:
        df = pl.read_parquet(path)
        return df.lazy()
    else:
        raise Exception("Could not cache the data")


def define_pl_col_transformation(col_name: str, col_type: pl.DataType) -> pl.Expr:
    if col_type == pl.Datetime:
        return pl.col(col_name).str.to_datetime(strict=False)
    elif col_type == pl.Date:
        return pl.col(col_name).str.to_date(strict=False)
    else:
        return pl.col(col_name).cast(col_type, strict=False)


def execute_write_method(
    write_method: Callable,
    path: str,
    data_type: str = None,
    sheet_name: str = None,
    delimiter: str = None,
    write_mode: str = "create",
    compression: str = None,
):
    if data_type == "excel":
        logger.info("Writing as excel file")
        write_method(path, worksheet=sheet_name)
    elif data_type == "csv":
        logger.info("Writing as csv file")
        if write_mode == "append":
            with open(path, "ab") as f:
                write_method(f, separator=delimiter, quote_style="always")
        else:
            write_method(path, separator=delimiter, quote_style="always")
    elif data_type == "parquet":
        logger.info("Writing as parquet file")
        write_method(path, compression=compression) if compression else write_method(path)
    elif data_type == "ndjson":
        logger.info("Writing as ndjson file")
        # check_extension=False: compressed ndjson otherwise requires a .gz/.zst suffix.
        write_method(path, compression=compression, check_extension=False) if compression else write_method(path)
    elif data_type in ("ipc", "avro"):
        logger.info(f"Writing as {data_type} file")
        write_method(path, compression=compression) if compression else write_method(path)


def local_write_output(
    _df: pl.LazyFrame | pl.DataFrame,
    data_type: str,
    path: str,
    write_mode: str,
    sheet_name: str = None,
    delimiter: str = None,
    compression: str = None,
    flow_id: int = -1,
    node_id: int | str = -1,
):
    is_lazy = isinstance(_df, pl.LazyFrame)
    sink_method_str = "sink_" + data_type
    write_method_str = "write_" + data_type
    has_sink_method = hasattr(_df, sink_method_str)
    write_method = None
    if os.path.exists(path) and write_mode == "create":
        return None
    if has_sink_method and is_lazy:
        write_method = getattr(_df, "sink_" + data_type)
    elif not is_lazy or not has_sink_method:
        if is_lazy:
            _df = _df.collect()
        write_method = getattr(_df, write_method_str)
    if write_method is not None:
        execute_write_method(
            write_method,
            path=path,
            data_type=data_type,
            sheet_name=sheet_name,
            delimiter=delimiter,
            write_mode=write_mode,
            compression=compression,
        )


def create_pl_df_type_save(raw_data: Iterable[Iterable], orient: str = "row") -> pl.DataFrame:
    """
        orient : {'col', 'row'}, default None
        Whether to interpret two-dimensional data as columns or as rows. If None,
        the orientation is inferred by matching the columns and data dimensions. If
        this does not yield conclusive results, column orientation is used.
    :param raw_data: iterables with values
    :param orient:
    :return: polars dataframe
    """
    if orient == "row":
        raw_data = zip(*raw_data, strict=False)
    raw_data = [standardize_col_dtype(values) for values in raw_data]
    return pl.DataFrame(raw_data, orient="col")


def find_first_positions(lst: list[str]) -> dict[str, int]:
    first_positions: dict[str, int] = {}
    for i, value in enumerate(lst):
        if value not in first_positions:
            first_positions[value] = i
    return first_positions


def match_order(lst: list[str], ref: list[str]) -> list[str]:
    ref_order = find_first_positions(ref)
    order = []
    for v in lst:
        org_order = ref_order.get(v, float("inf"))
        order.append(org_order)
    return [v for _, v in sorted(zip(order, lst, strict=False))]
