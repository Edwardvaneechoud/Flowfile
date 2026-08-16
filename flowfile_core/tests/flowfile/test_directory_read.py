"""Directory / glob read-node behaviour (slice 1 of the multi-file Read feature).

Everything here works on real files under ``tmp_path`` and runs real graphs — the only
patched symbol is a call counter that proves a function was *not* invoked.
"""

import os
from pathlib import Path

import polars as pl
import pytest
from openpyxl import Workbook

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import FlowParameter
from shared.path_utils import DirectoryScanUnsupportedError, NoFilesMatchedError, expand_glob_pattern
from tests.flowfile.test_flowfile import add_node_promise_on_type, create_graph, handle_run_info

# Helpers


def _add_read(graph: FlowGraph, received_table: input_schema.ReceivedTable, node_id: int = 1):
    """Place a read node and push its settings, returning the live node."""
    add_node_promise_on_type(graph, "read", node_id)
    graph.add_read(input_schema.NodeRead(flow_id=1, node_id=node_id, received_file=received_table))
    return graph.get_node(node_id)


def _collect(node) -> pl.DataFrame:
    """Materialise a node's result. ``FlowDataEngine.collect`` already covers lazy and eager frames."""
    return node.get_resulting_data().collect()


def _directory_table(path, file_type: str = "csv", **kwargs) -> input_schema.ReceivedTable:
    """A directory-mode ReceivedTable. ``name`` is deliberately set: in directory mode it is a node
    label and must never be appended to the path."""
    return input_schema.ReceivedTable(
        name="folder",
        path=str(path),
        file_type=file_type,
        scan_mode="directory",
        **kwargs,
    )


def _write_csv(path, rows: list[dict]) -> None:
    pl.DataFrame(rows).write_csv(path)


def _rewrite_atomic(path: Path, content: str) -> None:
    """write_text truncates under polars' live mmap (SIGBUS); write-then-replace is atomic.

    The same window exists in production (the read node's schema callback mmaps on a background
    thread while an external writer truncates the file); it predates this feature and is filed
    separately.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content)
    os.replace(tmp, path)


def _write_xlsx(path, rows: list[dict], sheet_name: str = "Sheet1") -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = sheet_name
    sheet.append(list(rows[0]))
    for row in rows:
        sheet.append(list(row.values()))
    workbook.save(path)


def _three_csvs(directory) -> None:
    """Three files written in non-sorted order plus a decoy the csv glob must not pick up."""
    _write_csv(directory / "part_c.csv", [{"id": 3, "val": "c"}])
    _write_csv(directory / "part_a.csv", [{"id": 1, "val": "a"}])
    _write_csv(directory / "part_b.csv", [{"id": 2, "val": "b"}])
    (directory / "skip_me.txt").write_text("not a csv\n")


# Reading a directory


def test_directory_read_csv(tmp_path, execution_location):
    """A bare directory path reads every csv under it as one frame, ignoring non-csv files."""
    _three_csvs(tmp_path)

    graph = create_graph(execution_location=execution_location)
    node = _add_read(graph, _directory_table(tmp_path))
    handle_run_info(graph.run_graph())

    df = _collect(node)
    assert df.height == 3
    assert sorted(df["id"].to_list()) == [1, 2, 3]


def test_directory_read_parquet(tmp_path, execution_location):
    """Parquet directories go through the same single native scan as csv."""
    for i, name in enumerate(["part_c", "part_a", "part_b"], start=1):
        pl.DataFrame([{"id": i, "val": name}]).write_parquet(tmp_path / f"{name}.parquet")
    (tmp_path / "skip_me.txt").write_text("not parquet\n")

    graph = create_graph(execution_location=execution_location)
    node = _add_read(graph, _directory_table(tmp_path, file_type="parquet"))
    handle_run_info(graph.run_graph())

    assert _collect(node).height == 3


def test_directory_read_ipc(tmp_path):
    """IPC directories synthesise ``*.arrow`` (the on-disk extension), not ``*.ipc``."""
    for i, name in enumerate(["part_c", "part_a", "part_b"], start=1):
        pl.DataFrame([{"id": i, "val": name}]).write_ipc(tmp_path / f"{name}.arrow")

    graph = create_graph(execution_location="local")
    node = _add_read(graph, _directory_table(tmp_path, file_type="ipc"))
    handle_run_info(graph.run_graph())

    assert _collect(node).height == 3


def test_explicit_glob_pattern_is_used_verbatim(tmp_path, execution_location):
    """A pattern the user typed is authoritative — it is never wrapped in another /**/*.ext."""
    _three_csvs(tmp_path)
    _write_csv(tmp_path / "other_d.csv", [{"id": 4, "val": "d"}])

    graph = create_graph(execution_location=execution_location)
    node = _add_read(graph, _directory_table(tmp_path / "part_*.csv"))
    handle_run_info(graph.run_graph())

    df = _collect(node)
    assert sorted(df["id"].to_list()) == [1, 2, 3]
    assert 4 not in df["id"].to_list()


def test_directory_read_is_recursive(tmp_path, execution_location):
    """A bare directory means everything underneath it, however deeply nested."""
    _three_csvs(tmp_path)
    nested = tmp_path / "2024" / "01"
    nested.mkdir(parents=True)
    _write_csv(nested / "part_d.csv", [{"id": 4, "val": "d"}])

    graph = create_graph(execution_location=execution_location)
    node = _add_read(graph, _directory_table(tmp_path))
    handle_run_info(graph.run_graph())

    assert sorted(_collect(node)["id"].to_list()) == [1, 2, 3, 4]


# Schema divergence across files


def test_schema_divergence_raises_clear_error(tmp_path):
    """Strict native semantics: an extra column in a later file fails the run rather than
    silently null-filling. Slice 1 ships no tolerance switch, so the failure is the contract."""
    pl.DataFrame([{"id": 1, "val": "a"}]).write_parquet(tmp_path / "part_a.parquet")
    pl.DataFrame([{"id": 2, "val": "b", "extra": 9}]).write_parquet(tmp_path / "part_b.parquet")

    graph = create_graph(execution_location="local")
    node = _add_read(graph, _directory_table(tmp_path, file_type="parquet"))
    run_info = graph.run_graph()

    assert run_info.success is False
    error = str(node.results.errors)
    assert "extra" in error or "part_b.parquet" in error, f"divergence must be attributable, got: {error}"


def test_csv_column_count_divergence_fails_the_run(tmp_path):
    """Same contract for csv: a file with a different column count fails instead of being coerced."""
    (tmp_path / "part_a.csv").write_text("id,val\n1,a\n")
    (tmp_path / "part_b.csv").write_text("id,val,extra\n2,b,9\n")

    graph = create_graph(execution_location="local")
    _add_read(graph, _directory_table(tmp_path))
    run_info = graph.run_graph()

    assert run_info.success is False


def test_csv_dtype_widening_is_not_predicted(tmp_path):
    """Accepted slice-1 gap, made visible: polars silently widens a csv column to String when a
    later file holds text, but the predicted schema probes only the first match and says Int64."""
    (tmp_path / "a_ints.csv").write_text("id,val\n1,10\n2,20\n")
    (tmp_path / "b_mixed.csv").write_text("id,val\n3,abc\n")

    graph = create_graph(execution_location="local")
    node = _add_read(graph, _directory_table(tmp_path))
    predicted = {column.name: column.data_type for column in node.schema}

    handle_run_info(graph.run_graph())

    assert _collect(node).schema["val"] == pl.String, "polars unifies divergent csv dtypes to String"
    assert predicted.get("val") != "String", (
        "The first-match probe predicted the widened column correctly — the R2 prediction gap is "
        f"closed and this pin should be updated (predicted: {predicted.get('val')})"
    )


# Zero matches


def test_zero_matches_fails_the_run(tmp_path):
    """A pattern that matches nothing is an error at run time, not an empty frame."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    graph = create_graph(execution_location="local")
    node = _add_read(graph, _directory_table(empty_dir))
    run_info = graph.run_graph()

    assert run_info.success is False
    assert "No files matched" in str(node.results.errors)


def test_zero_matches_raises_from_create_from_path(tmp_path):
    """The engine raises the typed error itself, so every caller (core, worker, probe) agrees."""
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    with pytest.raises(NoFilesMatchedError):
        FlowDataEngine.create_from_path(_directory_table(empty_dir))


def test_zero_matches_is_tolerated_at_settings_save(tmp_path, monkeypatch):
    """Saving settings for a directory that is still empty must not raise — the schema is simply
    unknown until files land. Saving must also not touch the read path at all.

    Note: reading ``node.schema`` afterwards *can* reach the exec fallback tier in
    ``get_predicted_schema`` (an empty callback result falls through to running the node's own
    function, whose zero-match error is then swallowed), so the no-read pin is scoped to the save.
    """
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()

    scanned_paths: list[str] = []
    original_create_from_path = FlowDataEngine.create_from_path

    def _counting_create_from_path(received_table):
        scanned_paths.append(received_table.abs_file_path)
        return original_create_from_path(received_table)

    monkeypatch.setattr(FlowDataEngine, "create_from_path", staticmethod(_counting_create_from_path))

    graph = create_graph(execution_location="local")
    table = _directory_table(empty_dir)
    node = _add_read(graph, table)

    assert scanned_paths == [], "saving settings must never attempt a read"
    assert node.schema in ([], None)


# The source-path column


def test_source_file_column_values(tmp_path, execution_location):
    """include_file_paths adds one column carrying the absolute path each row came from."""
    _three_csvs(tmp_path)

    graph = create_graph(execution_location=execution_location)
    node = _add_read(graph, _directory_table(tmp_path, include_file_paths="src"))
    handle_run_info(graph.run_graph())

    df = _collect(node)
    sources = df["src"].to_list()
    assert sorted(os.path.basename(p) for p in sources) == ["part_a.csv", "part_b.csv", "part_c.csv"]
    assert all(os.path.isabs(p) for p in sources)


def test_source_file_column_appears_in_predicted_schema(tmp_path):
    """The canvas must show the extra column before the flow is ever run."""
    _three_csvs(tmp_path)

    graph = create_graph(execution_location="local")
    node = _add_read(graph, _directory_table(tmp_path, include_file_paths="src"))

    schema = node.schema
    assert "src" in [column.name for column in schema]
    assert next(column for column in schema if column.name == "src").data_type == "String"


def test_blank_include_file_paths_normalizes_to_none(tmp_path):
    """An empty box in the UI arrives as whitespace and must mean 'no extra column'."""
    _three_csvs(tmp_path)

    table = _directory_table(tmp_path, include_file_paths="  ")
    assert table.include_file_paths is None

    graph = create_graph(execution_location="local")
    node = _add_read(graph, table)
    handle_run_info(graph.run_graph())

    assert _collect(node).columns == ["id", "val"]


def test_deterministic_ordering(tmp_path, execution_location):
    """Rows arrive in the sorted order of the expansion: our sort is lexicographic (not numeric)
    and the native scan preserves the order of the list we hand it."""
    names = ["z_0", "b_2", "a_1", "10", "2"]
    for i, name in enumerate(names, start=1):
        _write_csv(tmp_path / f"{name}.csv", [{"id": i, "val": name}])

    graph = create_graph(execution_location=execution_location)
    node = _add_read(graph, _directory_table(tmp_path, include_file_paths="src"))
    handle_run_info(graph.run_graph())

    sources = _collect(node)["src"].to_list()
    assert [os.path.basename(p) for p in sources] == ["10.csv", "2.csv", "a_1.csv", "b_2.csv", "z_0.csv"]
    assert sources == expand_glob_pattern(node.setting_input.received_file.abs_file_path)


# Single-file mode must be untouched


@pytest.mark.parametrize("file_type,ext", [("csv", "csv"), ("parquet", "parquet"), ("ipc", "arrow")])
def test_single_file_mode_unchanged(tmp_path, file_type, ext, execution_location):
    """Regression: the default scan_mode still reads exactly the named file, even with a sibling
    of the same type sitting next to it."""
    named = pl.DataFrame([{"id": 1}, {"id": 2}])
    sibling = pl.DataFrame([{"id": 99}])
    for frame, stem in ((named, "named"), (sibling, "sibling")):
        target = tmp_path / f"{stem}.{ext}"
        if file_type == "csv":
            frame.write_csv(target)
        elif file_type == "parquet":
            frame.write_parquet(target)
        else:
            frame.write_ipc(target)

    graph = create_graph(execution_location=execution_location)
    table = input_schema.ReceivedTable(name=f"named.{ext}", path=str(tmp_path), file_type=file_type)
    assert table.scan_mode == "single_file"
    node = _add_read(graph, table)
    handle_run_info(graph.run_graph())

    assert sorted(_collect(node)["id"].to_list()) == [1, 2]


def test_set_absolute_filepath_directory_no_name_append():
    """In directory mode ``name`` is a node label; appending it would turn a pattern into a
    non-existent file path. Single-file mode keeps appending, exactly as before."""
    directory_table = input_schema.ReceivedTable(
        name="sales.csv", path="/data/*.csv", file_type="csv", scan_mode="directory"
    )
    assert not directory_table.abs_file_path.endswith("sales.csv")
    assert directory_table.abs_file_path.endswith("*.csv")

    single_file_table = input_schema.ReceivedTable(name="sales.csv", path="/data", file_type="csv")
    assert single_file_table.abs_file_path.endswith("sales.csv")


# Refused combinations


@pytest.mark.parametrize(
    "file_type,table_settings",
    [
        ("excel", input_schema.InputExcelTable(sheet_name="Sheet1")),
        ("avro", None),
        ("json", None),
        ("ndjson", None),
        ("csv", input_schema.InputCsvTable(encoding="latin1")),
    ],
)
def test_directory_refusals(tmp_path, file_type, table_settings):
    """Types polars cannot scan from a file list (and non-UTF-8 csv, which routes to the worker's
    separate reader) are refused when the settings are saved, not halfway through a run."""
    _write_xlsx(tmp_path / "book_a.xlsx", [{"id": 1, "val": "a"}])
    _write_xlsx(tmp_path / "book_b.xlsx", [{"id": 2, "val": "b"}])
    _three_csvs(tmp_path)

    graph = create_graph(execution_location="local")
    add_node_promise_on_type(graph, "read", 1)
    table = _directory_table(tmp_path, file_type=file_type, table_settings=table_settings)

    with pytest.raises(DirectoryScanUnsupportedError):
        graph.add_read(input_schema.NodeRead(flow_id=1, node_id=1, received_file=table))


def test_directory_refusal_backstop_in_create_from_path(tmp_path):
    """The engine refuses too, so a hand-built or older saved flow can never reach the reader."""
    _three_csvs(tmp_path)
    table = _directory_table(tmp_path, table_settings=input_schema.InputCsvTable(encoding="latin1"))

    with pytest.raises(DirectoryScanUnsupportedError):
        FlowDataEngine.create_from_path(table)


def test_single_file_excel_still_reads(tmp_path, execution_location):
    """Regression: refusing excel *directories* must not touch single-file excel reads."""
    _write_xlsx(tmp_path / "book.xlsx", [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}])

    graph = create_graph(execution_location=execution_location)
    table = input_schema.ReceivedTable(
        name="book.xlsx",
        path=str(tmp_path),
        file_type="excel",
        table_settings=input_schema.InputExcelTable(sheet_name="Sheet1"),
    )
    node = _add_read(graph, table)
    handle_run_info(graph.run_graph())

    assert _collect(node).height == 2


# Change detection across runs


def test_change_detection_notices_new_file(tmp_path):
    """A new file in the directory changes the data without changing any setting, so the second
    run must re-read instead of serving the first run's result."""
    _three_csvs(tmp_path)

    graph = create_graph(execution_location="local")
    node = _add_read(graph, _directory_table(tmp_path))
    handle_run_info(graph.run_graph())
    assert _collect(node).height == 3

    _write_csv(tmp_path / "part_d.csv", [{"id": 4, "val": "d"}])
    handle_run_info(graph.run_graph())

    assert _collect(node).height == 4


def test_change_detection_notices_edited_file(tmp_path):
    """Editing one file in place changes the aggregate fingerprint (size and mtime)."""
    _three_csvs(tmp_path)

    graph = create_graph(execution_location="local")
    node = _add_read(graph, _directory_table(tmp_path))
    handle_run_info(graph.run_graph())
    assert sorted(_collect(node)["val"].to_list()) == ["a", "b", "c"]

    edited = tmp_path / "part_a.csv"
    _rewrite_atomic(edited, "id,val\n1,edited_value\n")
    stat = os.stat(edited)
    os.utime(edited, (stat.st_atime + 10, stat.st_mtime + 10))

    handle_run_info(graph.run_graph())

    assert sorted(_collect(node)["val"].to_list()) == ["b", "c", "edited_value"]


def test_change_detection_notices_deleted_file(tmp_path):
    """A removed file lowers the count, which the fingerprint has to catch."""
    _three_csvs(tmp_path)

    graph = create_graph(execution_location="local")
    node = _add_read(graph, _directory_table(tmp_path))
    handle_run_info(graph.run_graph())
    assert _collect(node).height == 3

    (tmp_path / "part_c.csv").unlink()
    handle_run_info(graph.run_graph())

    assert _collect(node).height == 2


def test_param_pattern_run_skips_when_unchanged(tmp_path):
    """A ${dir} directory read stores the SUBSTITUTED pattern in its fingerprint. If it stored the
    raw ``${dir}`` text instead, re-expanding it would match nothing and every run would look
    changed, so this pins that an untouched directory is recognised as unchanged."""
    data_dir = tmp_path / "landing"
    data_dir.mkdir()
    _three_csvs(data_dir)

    graph = create_graph(execution_location="local")
    graph.flow_settings.parameters = [FlowParameter(name="dir", default_value=str(data_dir))]
    node = _add_read(graph, _directory_table("${dir}"))

    handle_run_info(graph.run_graph())
    assert _collect(node).height == 3

    decision = node.executor._decide_execution(
        state=node._execution_state,
        run_location="local",
        performance_mode=False,
        force_refresh=False,
    )
    assert decision.should_run is False, f"unchanged directory must not re-run (reason: {decision.reason})"

    handle_run_info(graph.run_graph())
    assert _collect(node).height == 3
