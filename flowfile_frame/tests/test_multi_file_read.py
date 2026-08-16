"""Directory / multi-file read coverage for the flowfile_frame readers.

Everything here works on real files under ``tmp_path``: these readers build and resolve their
graph node eagerly, so simply constructing a FlowFrame already exercises the engine's scan path.
"""

import inspect
import os
from pathlib import Path

import polars as pl
import pytest

import flowfile_frame as ff
from flowfile_core.schemas.input_schema import NodePolarsCode
from shared.path_utils import ensure_glob_pattern

# Helpers


def _csv_dir(tmp_path: Path) -> Path:
    """Two csv files sharing one column set, in a directory of their own.

    Per-format directories keep bare-directory reads unambiguous — polars refuses a folder
    holding mixed file extensions.
    """
    directory = tmp_path / "csvs"
    directory.mkdir()
    pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_csv(directory / "one.csv")
    pl.DataFrame({"a": [3], "b": ["z"]}).write_csv(directory / "two.csv")
    return directory


def _parquet_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "parquets"
    directory.mkdir()
    pl.DataFrame({"a": [1, 2]}).write_parquet(directory / "one.parquet")
    pl.DataFrame({"a": [3]}).write_parquet(directory / "two.parquet")
    return directory


def _ipc_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "arrows"
    directory.mkdir()
    pl.DataFrame({"a": [1, 2]}).write_ipc(directory / "one.arrow")
    pl.DataFrame({"a": [3]}).write_ipc(directory / "two.arrow")
    return directory


def _received(frame: ff.FlowFrame):
    """The ReceivedTable the frame's read node was configured with."""
    return frame.flow_graph.get_node(frame.node_id).setting_input.received_file


def _polars_code(frame: ff.FlowFrame) -> str:
    """The generated polars source of a fallback (non-native) read node."""
    settings = frame.flow_graph.get_node(frame.node_id).setting_input
    assert isinstance(settings, NodePolarsCode), f"expected a polars code node, got {type(settings).__name__}"
    return settings.polars_code_input.polars_code


# Inference


def test_read_csv_glob_pattern_matches_polars(tmp_path):
    """A glob source needs no extra kwargs: it reads every match as one table, matches the plain
    polars result, and reaches the node settings as a directory scan with the pattern intact."""
    pattern = str(_csv_dir(tmp_path) / "*.csv")

    frame = ff.read_csv(pattern)

    assert frame.collect().sort("a").equals(pl.scan_csv(pattern).collect().sort("a"))
    received = _received(frame)
    assert received.scan_mode == "directory"
    assert received.path.endswith(f"csvs{os.sep}*.csv")


@pytest.mark.parametrize("suffix", ["", os.sep], ids=["bare_dir", "trailing_sep"])
def test_directory_source_infers_directory_mode(tmp_path, suffix):
    """A folder — with or without a trailing separator — infers directory mode and is turned into
    a recursive extension-scoped glob, so the user never has to write the pattern."""
    frame = ff.read_csv(str(_csv_dir(tmp_path)) + suffix)

    received = _received(frame)
    assert received.scan_mode == "directory"
    assert received.abs_file_path.endswith(f"**{os.sep}*.csv")
    assert frame.collect().height == 3


def test_single_file_source_is_unchanged(tmp_path):
    """Regression guard: an ordinary single-file read keeps its single-file settings and adds no
    source-path column."""
    frame = ff.read_csv(str(_csv_dir(tmp_path) / "one.csv"))

    received = _received(frame)
    assert received.scan_mode == "single_file"
    assert received.include_file_paths is None
    assert frame.collect().columns == ["a", "b"]


def test_explicit_scan_mode_overrides_inference(tmp_path):
    """An explicit scan_mode wins over inference: the glob is stored verbatim and read as one
    literal (nonexistent) file, which is exactly why building the frame raises."""
    graph = ff.create_flow_graph()
    pattern = str(_csv_dir(tmp_path) / "*.csv")

    with pytest.raises(OSError):
        ff.read_csv(pattern, scan_mode="single_file", flow_graph=graph)

    received = graph.nodes[-1].setting_input.received_file
    assert received.scan_mode == "single_file"
    assert received.path.endswith("*.csv")


# include_file_paths


def test_include_file_paths_adds_source_column(tmp_path):
    """The named column carries each row's real source path, one distinct value per input file."""
    frame = ff.read_csv(str(_csv_dir(tmp_path) / "*.csv"), include_file_paths="src")

    df = frame.collect()
    assert "src" in df.columns
    assert {Path(p).name for p in df["src"]} == {"one.csv", "two.csv"}
    assert df.height == 3


def test_blank_include_file_paths_normalizes_to_none(tmp_path):
    """A blank column name means 'no source-path column', not a column named with whitespace."""
    frame = ff.read_csv(str(_csv_dir(tmp_path) / "*.csv"), include_file_paths="   ")

    assert _received(frame).include_file_paths is None
    assert frame.collect().columns == ["a", "b"]


# glob interplay (csv only)


def test_glob_false_routes_to_polars_code(tmp_path):
    """glob=False is not expressible on the native read node, so it keeps today's fallback route
    and emits the flag into generated code; nothing new is added to that string."""
    frame = ff.read_csv(str(_csv_dir(tmp_path) / "one.csv"), glob=False)

    code = _polars_code(frame)
    assert "glob=False" in code
    assert "include_file_paths" not in code
    assert frame.collect().height == 2


def test_directory_scan_mode_requires_glob(tmp_path):
    """The only new error: directory mode is meaningless with glob expansion switched off."""
    with pytest.raises(ValueError, match="requires glob=True"):
        ff.read_csv(str(_csv_dir(tmp_path)), scan_mode="directory", glob=False)


def test_directory_fallback_emits_glob_and_file_paths(tmp_path):
    """A non-native option (n_rows) routes to generated polars code, which must still get a real
    glob for a bare directory plus the include_file_paths kwarg."""
    directory = str(_csv_dir(tmp_path))
    frame = ff.read_csv(directory, n_rows=5, include_file_paths="src")

    code = _polars_code(frame)
    # repr() because that is how the builder embeds the source (doubles Windows backslashes).
    assert repr(ensure_glob_pattern(directory, "csv")) in code
    assert "include_file_paths='src'" in code
    assert frame.collect().columns == ["a", "b", "src"]


# Other formats and aliases


def test_read_parquet_directory(tmp_path):
    """Parquet infers directory mode from a glob and reads every match as one table."""
    frame = ff.read_parquet(str(_parquet_dir(tmp_path) / "*.parquet"))

    assert _received(frame).scan_mode == "directory"
    assert frame.collect().height == 3


def test_read_ipc_directory_with_file_paths(tmp_path):
    """IPC infers directory mode from a bare folder and honours include_file_paths."""
    frame = ff.read_ipc(str(_ipc_dir(tmp_path)), include_file_paths="src")

    received = _received(frame)
    assert received.scan_mode == "directory"
    assert received.abs_file_path.endswith(f"**{os.sep}*.arrow")
    assert frame.collect().columns == ["a", "src"]


def test_scan_aliases_forward_directory_mode(tmp_path):
    """The scan_* aliases forward the new kwargs by name instead of dropping them into **options."""
    csv_frame = ff.scan_csv(str(_csv_dir(tmp_path)), include_file_paths="src")
    parquet_frame = ff.scan_parquet(str(_parquet_dir(tmp_path)), include_file_paths="src")
    ipc_frame = ff.scan_ipc(str(_ipc_dir(tmp_path)), include_file_paths="src")

    for frame in (csv_frame, parquet_frame, ipc_frame):
        received = _received(frame)
        assert received.scan_mode == "directory"
        assert received.include_file_paths == "src"
        assert "src" in frame.collect().columns


# Signature pins


def test_directory_capable_readers_expose_both_kwargs():
    """Every directory-capable reader and alias must expose scan_mode and include_file_paths as
    real named parameters — **options silently swallows them otherwise.

    Mirrors test_flowfile_frame.py's output_field_config signature pin.
    """
    for fn in (ff.read_csv, ff.read_parquet, ff.read_ipc, ff.scan_csv, ff.scan_parquet, ff.scan_ipc):
        params = inspect.signature(fn).parameters
        assert "scan_mode" in params, f"{fn.__name__} missing scan_mode kwarg"
        assert "include_file_paths" in params, f"{fn.__name__} missing include_file_paths kwarg"


def test_non_directory_readers_do_not_expose_scan_mode():
    """ndjson/avro/excel cannot be scanned from a file list by the engine, so they must not
    advertise a directory mode they would silently ignore."""
    for fn in (ff.read_ndjson, ff.read_avro, ff.read_excel):
        params = inspect.signature(fn).parameters
        assert "scan_mode" not in params, f"{fn.__name__} should not advertise scan_mode"
        assert "include_file_paths" not in params, f"{fn.__name__} should not advertise include_file_paths"


if __name__ == "__main__":
    pytest.main([__file__])
