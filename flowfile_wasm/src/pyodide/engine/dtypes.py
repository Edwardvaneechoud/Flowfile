import datetime
from decimal import Decimal
from typing import Any

import polars as pl

_DTYPE_STR_MAP: dict[str, Any] = {}


def to_json_safe_value(v: Any) -> Any:
    """Coerce one Polars .rows()/.to_dicts() cell to a JSON/Pyodide-safe primitive.

    Polars temporal/duration/decimal/bytes values cross Pyodide's toJs() bridge as
    PyProxies that render as '{}' in the grid; coerce them to strings/numbers.
    Scalar only — nested List/Struct temporal cells are not recursed (matches the
    explore path).
    """
    if v is None:
        return None
    if isinstance(v, datetime.datetime | datetime.date | datetime.time):
        return v.isoformat()
    if isinstance(v, datetime.timedelta):
        return v.total_seconds()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, bytes | bytearray):
        try:
            return v.decode("utf-8", errors="replace")
        except Exception:
            return str(v)
    return v


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


_NUMERIC_DTYPE_BASES = frozenset(
    {
        "fixed_decimal", "decimal", "float", "integer", "boolean", "double",
        "Int8", "Int16", "Int32", "Int64", "Int128",
        "Float16", "Float32", "Float64", "Decimal", "Binary", "Boolean",
        "Uint8", "Uint16", "Uint32", "Uint64",
        "UInt8", "UInt16", "UInt32", "UInt64", "UInt128",
    }
)


def readable_data_type_group(data_type: str) -> str:
    """Map a Polars dtype string to a readable group for dynamic-rename selection.

    Mirrors flowfile_core's FlowfileColumn.get_readable_datatype_group so a
    `selected_data_type` value picks the same columns in both apps — note Boolean
    and Binary deliberately fold into "Numeric" there. Parametric dtypes (e.g.
    'Datetime(...)') are matched on their base name.
    """
    base = data_type.split("(")[0]
    if base in ("Utf8", "VARCHAR", "CHAR", "NVARCHAR", "String"):
        return "String"
    if base in _NUMERIC_DTYPE_BASES:
        return "Numeric"
    if base in ("datetime", "date", "Date", "Datetime", "Time"):
        return "Date"
    return "Other"


def build_empty_lf_from_schema(schema_list: list[dict[str, str]]) -> pl.LazyFrame:
    """Build an empty (0-row) LazyFrame carrying only the given schema.

    The flowfile_core equivalent is FlowDataEngine.create_from_schema.
    """
    pl_schema = {}
    for col in schema_list:
        pl_schema[col["name"]] = _str_to_dtype(col.get("data_type", "String"))
    return pl.LazyFrame(schema=pl_schema)
