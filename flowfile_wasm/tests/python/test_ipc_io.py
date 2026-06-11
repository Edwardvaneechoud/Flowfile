"""Arrow IPC read path (execute_read_ipc) and the parquet-output IPC staging.

The browser decodes Parquet to Arrow IPC stream bytes in JS (parquet-wasm) —
the engine never sees parquet. These tests build IPC bytes with
pl.write_ipc_stream, exactly the byte format the bridge delivers. Zero new
dependencies. The memoryview cases mirror the Pyodide bridge
(`_temp_bytes.to_py()` yields a memoryview, not bytes).
"""
import io

import polars as pl
import pytest

import engine


@pytest.fixture
def ipc_bytes() -> bytes:
    df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "score": [1.5, 2.5, None]})
    bio = io.BytesIO()
    df.write_ipc_stream(bio)
    return bio.getvalue()


def test_read_ipc_happy_path(ipc_bytes):
    r = engine.execute_read_ipc(1, ipc_bytes, {})
    assert r["success"] is True
    assert [c["name"] for c in r["schema"]] == ["id", "name", "score"]
    df = engine.get_lazyframe(1).collect()
    assert df["id"].to_list() == [1, 2, 3]
    assert df["score"].dtype == pl.Float64


def test_read_ipc_accepts_memoryview(ipc_bytes):
    r = engine.execute_read_ipc(1, memoryview(ipc_bytes), {})
    assert r["success"] is True


def test_read_ipc_corrupt_bytes_reports_error():
    r = engine.execute_read_ipc(1, b"definitely not arrow", {})
    assert r["success"] is False
    assert "node #1" in r["error"]


def test_ipc_round_trip_preserves_dtypes(ipc_bytes):
    engine.execute_read_ipc(1, ipc_bytes, {})
    out = engine.execute_output(
        2, 1, {"output_settings": {"name": "x.parquet", "file_type": "parquet"}}
    )
    assert out["success"] is True

    staged = engine.take_output_binary(2)
    df = pl.read_ipc_stream(io.BytesIO(staged))
    assert df.schema == {"id": pl.Int64, "name": pl.String, "score": pl.Float64}
    assert len(df) == 3


def test_get_node_arrow_round_trip(ipc_bytes):
    engine.execute_read_ipc(1, ipc_bytes, {})

    arrow = engine.get_node_arrow(1)
    assert isinstance(arrow, bytes)
    df = pl.read_ipc_stream(io.BytesIO(arrow))
    assert df.schema == {"id": pl.Int64, "name": pl.String, "score": pl.Float64}
    assert df["id"].to_list() == [1, 2, 3]


def test_get_node_arrow_unknown_node_returns_none():
    assert engine.get_node_arrow(404) is None


def test_categorical_columns_survive_export():
    # Dictionary-encoded IPC inputs arrive as Categorical; the export cleaner
    # must rebuild them (their rev-map carries the same string buffers that
    # panic the wasm view->classic conversion) while preserving dtype + values.
    df = pl.DataFrame({"cat": ["x", "y", "x"], "n": [1, 2, 3]}).cast({"cat": pl.Categorical})
    bio = io.BytesIO()
    df.write_ipc_stream(bio, compat_level=pl.CompatLevel.oldest())
    engine.execute_read_ipc(1, bio.getvalue(), {})
    assert engine.get_lazyframe(1).collect_schema()["cat"] == pl.Categorical

    out = engine.execute_output(2, 1, {"output_settings": {"name": "x.parquet", "file_type": "parquet"}})
    assert out["success"] is True
    staged = pl.read_ipc_stream(io.BytesIO(engine.take_output_binary(2)))
    assert staged["cat"].to_list() == ["x", "y", "x"]
    assert staged["cat"].dtype == pl.Categorical

    arrow = engine.get_node_arrow(1)
    assert pl.read_ipc_stream(io.BytesIO(arrow))["cat"].to_list() == ["x", "y", "x"]
