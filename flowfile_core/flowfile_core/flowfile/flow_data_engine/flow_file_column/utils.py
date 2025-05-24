import polars as pl


dtype_to_pl = {
    'int': pl.Int64,
    'integer': pl.Int64,
    'char': pl.String,
    'fixed decimal': pl.Float32,
    'double': pl.Float64,
    'float': pl.Float64,
    'bool': pl.Boolean,
    'byte': pl.UInt8,
    'bit': pl.Binary,
    'date': pl.Date,
    'datetime': pl.Datetime,
    'string': pl.String,
    'str': pl.String,
    'time': pl.Time,
}


def safe_eval_pl_type(type_string: str):
    """
    Safely evaluate a Polars type string with restricted namespace.
    Only allows Polars types and basic Python literals.
    """
    # Define allowed names in the evaluation namespace
    safe_dict = {
        # Polars module and types
        'pl': pl,

        # Basic Python built-ins for literals
        'int': int,
        'str': str,
        'float': float,
        'bool': bool,
        'list': list,
        'dict': dict,
        'tuple': tuple,

        # Disable dangerous built-ins
        '__builtins__': {},
    }

    try:
        return eval(type_string, safe_dict, {})
    except Exception as e:
        raise ValueError(f"Failed to safely evaluate type string '{type_string}': {e}")


dtype_to_pl_str = {k: v.__name__ for k, v in dtype_to_pl.items()}


def get_polars_type(dtype: str):
    if 'pl.' in dtype:
        try:
            return safe_eval_pl_type(dtype)
        except Exception as e:
            return pl.String
    pl_datetype = dtype_to_pl.get(dtype.lower())
    if pl_datetype is not None:
        return pl_datetype
    elif hasattr(pl, dtype):
        return getattr(pl, dtype)
    else:
        return pl.String


def cast_str_to_polars_type(dtype: str) -> pl.DataType:
    pl_type = get_polars_type(dtype)
    if hasattr(pl_type, '__call__'):
        return pl_type()
    else:
        return pl_type

