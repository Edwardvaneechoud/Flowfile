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

dtype_to_pl_str = {k: v.__name__ for k, v in dtype_to_pl.items()}


def type_to_polars(dtype: str):
    pl_datetype = dtype_to_pl.get(dtype.lower())
    if pl_datetype is not None:
        return pl_datetype
    elif hasattr(pl, dtype):
        return getattr(pl, dtype)
    else:
        return pl.String


def type_to_polars_str(dtype: str) -> pl.DataType:
    return type_to_polars(dtype)()
