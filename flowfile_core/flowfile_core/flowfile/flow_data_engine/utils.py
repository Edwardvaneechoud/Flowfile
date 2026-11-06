from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

import polars as pl

from flowfile_core.configs import logger
from flowfile_core.configs.settings import AVAILABLE_RAM
from flowfile_core.flowfile.flow_data_engine.bounded import single_row
from shared.excel_writer import write_excel_output

if TYPE_CHECKING:
    from flowfile_core.flowfile.execution.protocol import ComputeBackend

SCHEMA_STATS_MAX_ROWS = 1_000_000
"""The rows ``calculate_schema`` takes its statistics over: the first million, all of a smaller frame."""


def get_data_type(vals: Iterable[Any]):
    types = set(type(val) for val in vals)
    if len(types) == 1:
        return types.pop().__name__
    elif types == {float, int}:
        return "float"
    else:
        return "str"


def calculate_schema(
    lf: pl.LazyFrame | pl.DataFrame, backend: ComputeBackend, schema: pl.Schema | None = None
) -> list[dict]:
    """The schema of ``lf`` with each column's statistics, as the ``PlType`` fields a ``FlowfileColumn`` reads.

    One aggregate over the first ``SCHEMA_STATS_MAX_ROWS`` rows yields every column's statistics as a single row,
    which ``backend`` computes (``bounded.single_row``), so only that row reaches core. The statistics follow Polars'
    ``describe``: ``mean``, ``min`` and ``max`` are text, ``n_unique`` is approximate, and a statistic a dtype has none
    of is ``None``. Pass ``lf``'s ``schema`` so it is not resolved in core.

    Raises:
        ComputeError: ``backend`` could not compute the statistics.
    """
    lazy = lf.lazy()
    schema = lazy.collect_schema() if schema is None else schema
    if not schema:
        return []
    exprs = [expr for index, (name, dtype) in enumerate(schema.items()) for expr in _stat_exprs(index, name, dtype)]
    row = single_row(
        backend,
        lazy.head(SCHEMA_STATS_MAX_ROWS).select(exprs),
        empty_message="The schema statistics aggregation returned no row.",
    )
    return [_column_stats(index, name, dtype, row) for index, (name, dtype) in enumerate(schema.items())]


_STATS = ("count", "null_count", "mean", "std", "min", "max", "n_unique")
_NO_MIN_MAX = (pl.Categorical, pl.Enum, pl.Null, pl.Object, pl.Unknown)


def _stat_exprs(index: int, name: str, dtype: pl.DataType) -> list[pl.Expr]:
    """One column's statistics, named ``<index>:<statistic>`` so no column name can collide."""
    column = pl.col(name)
    is_numeric = dtype.is_numeric()
    has_mean = is_numeric or dtype.is_temporal() or dtype == pl.Boolean
    skip_min_max = dtype.is_nested() or dtype.is_extension() or dtype in _NO_MIN_MAX
    stats = {
        "count": column.count(),
        "null_count": column.null_count(),
        "mean": column.mean() if has_mean else pl.lit(None),
        "std": column.std() if is_numeric else pl.lit(None),
        "min": pl.lit(None) if skip_min_max else column.min(),
        "max": pl.lit(None) if skip_min_max else column.max(),
        "n_unique": pl.lit(None) if dtype.is_nested() or dtype == pl.Object else column.approx_n_unique(),
    }
    if _states_floats(dtype):
        for stat in ("mean", "min", "max"):
            stats[stat] = stats[stat].cast(pl.Float64).cast(pl.String)
    return [stats[stat].alias(f"{index}:{stat}") for stat in _STATS]


def _states_floats(dtype: pl.DataType) -> bool:
    """Whether a column states its mean, minimum and maximum as float text, as ``describe`` does."""
    return dtype.is_numeric() or dtype.is_nested() or dtype in (pl.Null, pl.Boolean)


def _column_stats(index: int, name: str, dtype: pl.DataType, row: dict[str, Any]) -> dict:
    """The ``PlType`` fields of one column from the statistics row."""
    stat = {stat: row[f"{index}:{stat}"] for stat in _STATS}
    as_text = {key: None if stat[key] is None else str(stat[key]) for key in ("mean", "min", "max")}
    return {
        "column_name": name,
        "col_index": index,
        "pl_datatype": dtype,
        "count": int(stat["count"]),
        "null_count": int(stat["null_count"]),
        "std": None if stat["std"] is None else float(stat["std"]),
        "n_unique": None if stat["n_unique"] is None else int(stat["n_unique"]),
        "examples": ", ".join(dict.fromkeys([str(as_text["min"]), str(as_text["max"])])),
        **as_text,
    }


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


def cache_polars_frame_to_temp(_df: pl.LazyFrame | pl.DataFrame, tempdir: str = None) -> pl.LazyFrame:
    path = f"{tempdir}\\fl_file_{id(_df)}"
    result = write_polars_frame(_df, path)
    if result:
        df = pl.read_parquet(path)
        return df.lazy()
    else:
        raise Exception("Could not cache the data")


def define_pl_col_transformation(
    col_name: str, col_type: pl.DataType, source_type: pl.DataType | None = None
) -> pl.Expr:
    """Cast one column, parsing dates out of text but casting everything else directly.

    ``str.to_date`` / ``str.to_datetime`` raise on a non-string input, so they are only used
    when the source column is text (or its type is unknown, which keeps the historical
    behaviour for callers that do not pass ``source_type``).
    """
    from_text = source_type is None or source_type == pl.String
    if col_type == pl.Datetime and from_text:
        return pl.col(col_name).str.to_datetime(strict=False)
    if col_type == pl.Date and from_text:
        return pl.col(col_name).str.to_date(strict=False)
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
    if data_type == "csv":
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
    if data_type == "excel":
        frame = _df.collect() if is_lazy else _df
        write_excel_output(frame, path=path, sheet_name=sheet_name, write_mode=write_mode)
        return None
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
