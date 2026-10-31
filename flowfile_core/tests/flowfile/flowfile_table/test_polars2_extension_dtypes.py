"""Polars 2 extension-dtype defaults Flowfile relies on.

Flowfile marks a column as geometry only from a declared GeoArrow extension dtype
(``FlowfileColumn.semantic_type``). Polars 2 keeps an *unregistered* extension
dtype when a frame crosses IPC or Parquet — the worker hop — and when a column is
cast to it; Polars 1.x dropped it to its storage type unless
``POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR`` was ``load_as_extension``.

The hop probes run in a subprocess with that variable scrubbed, because polars
reads it once at import: each test observes polars' real default no matter what
the parent process or CI exported.
"""

import os
import subprocess
import sys

import polars as pl

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

ENV_VAR = "POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR"
WKT = pl.Extension("geoarrow.wkt", pl.String, "{}")

# Prints KEPT/DROPPED for one hop; runs under the parent's interpreter with the override scrubbed.
_PROBE = """
import io, sys, warnings
warnings.simplefilter("ignore")
import polars as pl
ext = pl.Extension("geoarrow.wkt", pl.String, "{}")
df = pl.DataFrame({"g": pl.Series(["POINT (4.9 52.3)"], dtype=ext)})
if sys.argv[1] == "ipc":
    buf = io.BytesIO(); df.write_ipc(buf); buf.seek(0); dtype = pl.read_ipc(buf).schema["g"]
else:
    df.write_parquet(sys.argv[2]); dtype = pl.scan_parquet(sys.argv[2]).collect_schema()["g"]
print("KEPT" if dtype == ext else "DROPPED")
"""


def _default_hop_behaviour(kind: str, tmp_path) -> str:
    env = {k: v for k, v in os.environ.items() if k != ENV_VAR}
    proc = subprocess.run(
        [sys.executable, "-c", _PROBE, kind, str(tmp_path / "g.parquet")],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip().splitlines()[-1]


def test_ipc_keeps_extension_dtype_by_default(tmp_path):
    assert _default_hop_behaviour("ipc", tmp_path) == "KEPT"


def test_parquet_scan_keeps_extension_dtype_by_default(tmp_path):
    assert _default_hop_behaviour("parquet", tmp_path) == "KEPT"


def test_cast_attaches_extension_dtype():
    out = pl.DataFrame({"g": ["POINT (1 2)"]}).with_columns(pl.col("g").cast(WKT))
    assert out.schema["g"] == WKT


def test_engine_schema_flags_geometry_after_worker_hop(tmp_path):
    path = tmp_path / "g.parquet"
    pl.DataFrame({"g": pl.Series(["POINT (4.9 52.3)"], dtype=WKT)}).write_parquet(path)
    column = next(iter(FlowDataEngine(pl.scan_parquet(path)).schema))
    assert column.semantic_type == "geometry"
    assert column.data_type_group == "String"
