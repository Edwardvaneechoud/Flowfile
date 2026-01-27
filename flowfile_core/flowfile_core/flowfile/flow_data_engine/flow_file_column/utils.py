import polars as pl

dtype_to_pl = {
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


def safe_eval_pl_type(type_string: str):
    """
    Safely evaluate a Polars type string with restricted namespace.
    Supports both formats:
      - With pl. prefix: pl.List(pl.Int64)
      - Without pl. prefix: List(Int64)
    """
    # Define allowed names in the evaluation namespace
    safe_dict = {
        # Keep pl module for backwards compatibility with pl.X format
        "pl": pl,

        # Polars types directly available (without pl. prefix)
        "List": pl.List,
        "Array": pl.Array,
        "Struct": pl.Struct,
        "Field": pl.Field,
        "Decimal": pl.Decimal,

        # Integer types
        "Int8": pl.Int8,
        "Int16": pl.Int16,
        "Int32": pl.Int32,
        "Int64": pl.Int64,
        "Int128": pl.Int128,
        "UInt8": pl.UInt8,
        "UInt16": pl.UInt16,
        "UInt32": pl.UInt32,
        "UInt64": pl.UInt64,

        # Float types
        "Float32": pl.Float32,
        "Float64": pl.Float64,

        # Other types
        "Boolean": pl.Boolean,
        "String": pl.String,
        "Utf8": pl.Utf8,
        "Binary": pl.Binary,
        "Date": pl.Date,
        "Time": pl.Time,
        "Datetime": pl.Datetime,
        "Duration": pl.Duration,
        "Categorical": pl.Categorical,
        "Enum": pl.Enum,
        "Null": pl.Null,
        "Object": pl.Object,

        # Disable dangerous built-ins
        "__builtins__": {},
    }

    try:
        return eval(type_string, safe_dict, {})
    except Exception as e:
        raise ValueError(f"Failed to safely evaluate type string '{type_string}': {e}")


dtype_to_pl_str = {k: v.__name__ for k, v in dtype_to_pl.items()}


def get_polars_type(dtype: str):
    if "pl." in dtype:
        try:
            return safe_eval_pl_type(dtype)
        except Exception:
            return pl.String
    pl_datetype = dtype_to_pl.get(dtype.lower())
    if pl_datetype is not None:
        return pl_datetype
    try:
        return safe_eval_pl_type(dtype)
    except Exception:
        return pl.String  # Fallback to String if evaluation fails


def cast_str_to_polars_type(dtype: str) -> pl.DataType:
    pl_type = get_polars_type(dtype)
    if callable(pl_type):
        return pl_type()
    else:
        return pl_type
