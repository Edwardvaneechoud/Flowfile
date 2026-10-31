import io

import polars as pl

_NO_ARROW_FORM = (pl.Int128, pl.UInt128)


def sample_stream(df: pl.DataFrame) -> bytes:
    """*df* as the Arrow IPC stream a sample travels in, its 128-bit integers as text.

    Polars 2 types signed-with-UInt64 arithmetic as ``Int128``, which pyarrow cannot read, and core reads a
    sample with pyarrow. A sample is a preview; the result itself keeps its dtype.
    """
    wide = [name for name, dtype in df.schema.items() if dtype in _NO_ARROW_FORM]
    if wide:
        df = df.with_columns(pl.col(wide).cast(pl.String))
    buffer = io.BytesIO()
    df.write_ipc_stream(buffer)
    return buffer.getvalue()
