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


_CAST_NAME_ALIASES: dict[str, Any] = {
    # flowfile_core's dtype_to_pl plus SelectInput.polars_type's friendly names.
    "int": pl.Int64,
    "integer": pl.Int64,
    "char": pl.String,
    "fixed decimal": pl.Float32,
    "double": pl.Float64,
    "float": pl.Float64,
    "bool": pl.Boolean,
    "byte": pl.UInt8,
    "bit": pl.Binary,
    "date": pl.Date,
    "datetime": pl.Datetime,
    "string": pl.String,
    "str": pl.String,
    "time": pl.Time,
}

for _dtname in (
    "Int8", "Int16", "Int32", "Int64", "Int128",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Float32", "Float64",
    "Boolean", "String", "Utf8", "Binary",
    "Date", "Time", "Datetime", "Duration", "Categorical", "Null",
):
    _dt = getattr(pl, _dtname, None)
    if _dt is not None:
        _CAST_NAME_ALIASES.setdefault(_dtname.lower(), _dt)


def polars_type_from_name(name: str):
    """Resolve a declared data-type name to a Polars dtype, or None if unknown.

    Mirrors flowfile_core's SelectInput.polars_type + get_polars_type: both the
    friendly names ("integer", "double", "string") and the Polars ones resolve,
    and a parametrised name ("Datetime(time_unit='us')") matches on its base.
    The one deliberate divergence is the fallback: core turns an unrecognised
    name into String, which would let a stale marker silently stringify a
    column, so an unknown name here means "no type was asked for".
    """
    if not name:
        return None
    base = str(name).split("(")[0].strip().lower()
    return _CAST_NAME_ALIASES.get(base)


def select_cast_dtype(declared: str | None, current_dtype) -> Any | None:
    """The dtype a select entry asks a column to become, or None for no change.

    flowfile_core's change_column_types drops any transform whose target already
    equals the column's base dtype, which is what makes the data type the select
    panel records for every column (changed or not) a no-op.
    """
    target = polars_type_from_name(declared)
    if target is None:
        return None
    if current_dtype is not None and target == current_dtype.base_type():
        return None
    return target


def build_empty_lf_from_schema(schema_list: list[dict[str, str]]) -> pl.LazyFrame:
    """Build an empty (0-row) LazyFrame carrying only the given schema.

    The flowfile_core equivalent is FlowDataEngine.create_from_schema.
    """
    pl_schema = {}
    for col in schema_list:
        pl_schema[col["name"]] = _str_to_dtype(col.get("data_type", "String"))
    return pl.LazyFrame(schema=pl_schema)
