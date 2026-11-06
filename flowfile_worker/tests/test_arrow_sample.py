"""A sample's 128-bit integers travel as text, at any depth, so pyarrow can read it."""

import io

import polars as pl
import pyarrow as pa
import pyarrow.ipc as ipc

from flowfile_worker.arrow_sample import sample_stream


def test_nested_128_bit_integers_become_text():
    df = pl.DataFrame({"a": [1], "items": [[1, 2]], "pair": [{"x": 1, "y": "q"}], "fixed": [[1, 2]]}).with_columns(
        pl.col("a").cast(pl.Int128),
        pl.col("items").cast(pl.List(pl.Int128)),
        pl.col("pair").cast(pl.Struct({"x": pl.UInt128, "y": pl.String})),
        pl.col("fixed").cast(pl.Array(pl.Int128, 2)),
    )

    table = ipc.open_stream(io.BytesIO(sample_stream(df))).read_all()

    assert table.column("a").to_pylist() == ["1"]
    assert table.column("items").to_pylist() == [["1", "2"]]
    assert table.column("pair").to_pylist() == [{"x": "1", "y": "q"}]
    assert table.column("fixed").to_pylist() == [["1", "2"]]


def test_a_frame_without_128_bit_integers_keeps_its_types():
    df = pl.DataFrame({"a": [1], "items": [[1]]})

    table = ipc.open_stream(io.BytesIO(sample_stream(df))).read_all()

    assert table.schema.field("a").type == pa.int64()
