import io

import polars as pl

_NO_ARROW_FORM = (pl.Int128, pl.UInt128)


def _arrow_readable(dtype: pl.DataType) -> pl.DataType:
    """*dtype* with every 128-bit integer in it, nested ones included, as text."""
    if dtype in _NO_ARROW_FORM:
        return pl.String()
    if isinstance(dtype, pl.List):
        return pl.List(_arrow_readable(dtype.inner))
    if isinstance(dtype, pl.Array):
        return pl.Array(_arrow_readable(dtype.inner), dtype.shape)
    if isinstance(dtype, pl.Struct):
        return pl.Struct([pl.Field(field.name, _arrow_readable(field.dtype)) for field in dtype.fields])
    return dtype


def sample_stream(df: pl.DataFrame) -> bytes:
    """*df* as the Arrow IPC stream a sample travels in, its 128-bit integers as text.

    Polars 2 types signed-with-UInt64 arithmetic as ``Int128``, which pyarrow cannot read, and core reads a
    sample with pyarrow. A sample is a preview; the result itself keeps its dtype.
    """
    casts = {}
    for name, dtype in df.schema.items():
        readable = _arrow_readable(dtype)
        if readable != dtype:
            casts[name] = readable
    if casts:
        df = df.cast(casts)
    buffer = io.BytesIO()
    df.write_ipc_stream(buffer)
    return buffer.getvalue()
