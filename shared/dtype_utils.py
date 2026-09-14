"""Row-oriented value coercion shared by core, the worker, and the excel reader.

These three helpers existed as byte-identical copies in ``flowfile_core.utils.utils``,
``flowfile_core.flowfile.flow_data_engine.utils`` and ``flowfile_worker.create.utils``.
They live here so ``shared.excel_reader`` can build frames without either package, and the
original modules re-export them so existing import paths keep working.
"""

from collections.abc import Iterable

import polars as pl


def convert_to_string(v):
    try:
        return str(v)
    except Exception:
        return None


def standardize_col_dtype(vals):
    """Stringify a genuinely mixed-type column; nulls don't count as a type, so [1, None] stays nullable Int."""
    types = set(type(val) for val in vals if val is not None)
    if len(types) <= 1:
        return vals
    elif int in types and float in types:
        return vals
    else:
        return [convert_to_string(v) for v in vals]


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
