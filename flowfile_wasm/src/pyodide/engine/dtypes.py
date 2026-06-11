from typing import Any

import polars as pl

_DTYPE_STR_MAP: dict[str, Any] = {}


for _dtname in (
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "Int128",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Float32",
    "Float64",
    "Boolean",
    "String",
    "Utf8",
    "Categorical",
    "Date",
    "Time",
    "Null",
):
    _dt = getattr(pl, _dtname, None)
    if _dt is not None:
        _DTYPE_STR_MAP[_dtname] = _dt


_DTYPE_STR_MAP["Bool"] = _DTYPE_STR_MAP.get("Boolean", pl.Boolean)


def _str_to_dtype(s: str):
    """Resolve a dtype string (e.g. 'Int64', 'Datetime(...)') to a Polars dtype."""
    if s in _DTYPE_STR_MAP:
        return _DTYPE_STR_MAP[s]
    base = s.split("(")[0]
    if base in _DTYPE_STR_MAP:
        return _DTYPE_STR_MAP[base]
    dt = getattr(pl, base, None)
    return dt if dt is not None else pl.String


def build_empty_lf_from_schema(schema_list: list[dict[str, str]]) -> pl.LazyFrame:
    """Build an empty (0-row) LazyFrame carrying only the given schema.

    The flowfile_core equivalent is FlowDataEngine.create_from_schema.
    """
    pl_schema = {}
    for col in schema_list:
        pl_schema[col["name"]] = _str_to_dtype(col.get("data_type", "String"))
    return pl.LazyFrame(schema=pl_schema)
