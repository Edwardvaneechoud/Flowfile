"""Sentinels for the Polars 2.0 extension-dtype defaults.

Flowfile marks a column as geometry only from a declared GeoArrow extension dtype
(``FlowfileColumn.semantic_type``). Under Polars 1.x an *unregistered* extension
dtype is dropped to its storage type whenever a frame crosses IPC or Parquet —
which is the worker hop — unless ``POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR`` is
``load_as_extension``. Polars 2.0 flips that default (polars says so in the
UserWarning it emits today).

The default-behaviour probes run in a subprocess with that variable scrubbed,
because polars reads it once at import: each sentinel observes polars' real
default no matter what the parent process or CI exported. Every sentinel is an
unconditional strict xfail, so the first polars release that changes the
behaviour turns it into a hard XPASS failure. That failure is the to-do list:

1. delete the xfail markers here (the probe bodies stay as regression tests);
2. drop any ``POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR`` workaround core/worker set;
3. re-verify the geometry label end to end (preview header + cells, stats panel,
   Select node marker, formula badges) with a producer that emits ``geoarrow.*``.

``test_engine_schema_flags_geometry_after_worker_hop`` is not a sentinel: it is
the end-to-end contract, run wherever the dtype can survive the hop (polars >= 2,
or the override exported into this process) and skipped elsewhere.
"""

import os
import subprocess
import sys

import polars as pl
import pytest

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

POLARS_MAJOR = int(pl.__version__.split(".")[0])
ENV_VAR = "POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR"
WKT = pl.Extension("geoarrow.wkt", pl.String, "{}")

xfail_until_polars_keeps_extensions = pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "polars 1.x loads an unregistered extension dtype as its storage type unless "
        f"{ENV_VAR}=load_as_extension; 2.0 makes load_as_extension the default. "
        "An XPASS here means polars changed: remove the markers and the env-var workaround."
    ),
)

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


@xfail_until_polars_keeps_extensions
def test_ipc_keeps_extension_dtype_by_default(tmp_path):
    assert _default_hop_behaviour("ipc", tmp_path) == "KEPT"


@xfail_until_polars_keeps_extensions
def test_parquet_scan_keeps_extension_dtype_by_default(tmp_path):
    assert _default_hop_behaviour("parquet", tmp_path) == "KEPT"


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "polars 1.43 silently returns the storage dtype when casting to an Extension, so a "
        "producer must build the series with dtype=Extension(...) instead. An XPASS means cast "
        "now attaches the extension: producers can switch to a plain cast."
    ),
)
def test_cast_attaches_extension_dtype():
    out = pl.DataFrame({"g": ["POINT (1 2)"]}).with_columns(pl.col("g").cast(WKT))
    assert out.schema["g"] == WKT


@pytest.mark.skipif(
    POLARS_MAJOR < 2 and os.environ.get(ENV_VAR, "").lower() != "load_as_extension",
    reason=f"needs polars >= 2 or {ENV_VAR}=load_as_extension for the dtype to survive the hop",
)
def test_engine_schema_flags_geometry_after_worker_hop(tmp_path):
    path = tmp_path / "g.parquet"
    pl.DataFrame({"g": pl.Series(["POINT (4.9 52.3)"], dtype=WKT)}).write_parquet(path)
    column = next(iter(FlowDataEngine(pl.scan_parquet(path)).schema))
    assert column.semantic_type == "geometry"
    assert column.data_type_group == "String"
