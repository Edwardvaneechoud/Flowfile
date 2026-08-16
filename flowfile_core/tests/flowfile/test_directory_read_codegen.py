"""Code-generator coverage for read nodes in ``scan_mode="directory"``.

Both exporters must produce a script that reads the same file set the flow itself reads:
the Polars export repeats the engine's sorted glob expansion, the FlowFrame export hands
the path back to the reader with ``scan_mode="directory"`` and lets it re-derive the glob.
"""

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_core.flowfile.code_generator.code_generator import (
    UnsupportedNodeError,
    export_flow_to_flowframe,
    export_flow_to_polars,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema

EXPORTERS = pytest.mark.parametrize(
    "export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"]
)


def create_flow(flow_id: int = 1) -> FlowGraph:
    return FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=flow_id, execution_mode="Performance", execution_location="local", path="/tmp/test_flow"
        ),
        name="directory_read_flow",
    )


def csv_settings(encoding: str = "utf-8") -> input_schema.InputCsvTable:
    return input_schema.InputCsvTable(delimiter=",", has_headers=True, encoding=encoding)


def add_read(
    flow: FlowGraph,
    path: str,
    file_type: str,
    table_settings,
    scan_mode: str = "directory",
    include_file_paths: str | None = None,
    node_id: int = 1,
) -> FlowGraph:
    received = input_schema.ReceivedTable(
        name=Path(path).name,
        path=path,
        file_type=file_type,
        scan_mode=scan_mode,
        include_file_paths=include_file_paths,
        table_settings=table_settings,
    )
    flow.add_read(input_schema.NodeRead(flow_id=flow.flow_id, node_id=node_id, received_file=received))
    return flow


def run_generated(code: str):
    exec_globals = {}
    exec(code, exec_globals)
    return exec_globals["run_etl_pipeline"]()


def normalize(result) -> pl.DataFrame:
    return result.collect() if hasattr(result, "collect") else result


def assert_matches_flow(code: str, flow: FlowGraph, node_id: int = 1, sort_by: str = "a") -> pl.DataFrame:
    """Execute the generated code and compare it with what the flow's own node produced."""
    result = normalize(run_generated(code)).sort(sort_by)
    expected = normalize(flow.get_node(node_id).get_resulting_data().data_frame).sort(sort_by)
    assert_frame_equal(result, expected, check_column_order=False)
    return result


def make_csv_dir(tmp_path: Path) -> Path:
    """A directory whose files a directory scan must combine: two files, one nested, one dotfile.

    The dotfile is the ordering/visibility trap: python's glob skips it, polars' own glob
    does not, so it proves the export expands the pattern the way the engine does.
    """
    directory = tmp_path / "csv_data"
    (directory / "nested").mkdir(parents=True)
    pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_csv(directory / "one.csv")
    pl.DataFrame({"a": [3], "b": ["z"]}).write_csv(directory / "two.csv")
    pl.DataFrame({"a": [4], "b": ["w"]}).write_csv(directory / "nested" / "three.csv")
    (directory / ".hidden.csv").write_text("a,b\n999,dot\n")
    return directory


def make_parquet_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "parquet_data"
    directory.mkdir()
    pl.DataFrame({"a": [1, 2]}).write_parquet(directory / "one.parquet")
    pl.DataFrame({"a": [3]}).write_parquet(directory / "two.parquet")
    return directory


def make_ipc_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "ipc_data"
    directory.mkdir()
    pl.DataFrame({"a": [1, 2]}).write_ipc(directory / "one.arrow")
    pl.DataFrame({"a": [3]}).write_ipc(directory / "two.arrow")
    return directory


@EXPORTERS
def test_directory_csv_read_round_trip(tmp_path, export_func):
    """A csv directory read exports to a script that reads the same rows the flow read."""
    directory = make_csv_dir(tmp_path)
    flow = add_read(create_flow(), str(directory), "csv", csv_settings())

    code = export_func(flow)
    result = assert_matches_flow(code, flow)

    assert result.height == 4, "expected the two top-level files plus the nested one"
    assert 999 not in result["a"].to_list(), "dotfiles must stay out of the scan"


@EXPORTERS
def test_directory_parquet_read_round_trip(tmp_path, export_func):
    directory = make_parquet_dir(tmp_path)
    flow = add_read(create_flow(), str(directory), "parquet", input_schema.InputParquetTable())

    result = assert_matches_flow(export_func(flow), flow)
    assert result["a"].to_list() == [1, 2, 3]


@EXPORTERS
def test_directory_ipc_read_round_trip(tmp_path, export_func):
    directory = make_ipc_dir(tmp_path)
    flow = add_read(create_flow(), str(directory), "ipc", input_schema.InputIpcTable())

    result = assert_matches_flow(export_func(flow), flow)
    assert result["a"].to_list() == [1, 2, 3]


@EXPORTERS
def test_directory_read_fuses_with_downstream_node(tmp_path, export_func):
    """The multi-line directory emission is still a single assignment, so chain fusion holds."""
    directory = make_csv_dir(tmp_path)
    flow = add_read(create_flow(), str(directory), "csv", csv_settings())
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[a]>1"),
        )
    )
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(1, 2))

    result = assert_matches_flow(export_func(flow), flow, node_id=2)
    assert result["a"].to_list() == [2, 3, 4]


@EXPORTERS
def test_explicit_pattern_is_emitted_verbatim(tmp_path, export_func):
    """A user-written pattern is honoured as-is — no recursive rewrite behind their back."""
    directory = make_csv_dir(tmp_path)
    pattern = str(directory / "*.csv")
    flow = add_read(create_flow(), pattern, "csv", csv_settings())

    code = export_func(flow)
    # Only the leading /var vs /private/var spelling can differ (the Polars export emits the
    # resolved path, the FlowFrame export the node's own), so match on the pattern's tail.
    assert f"{directory.name}/*.csv" in code
    result = assert_matches_flow(code, flow)
    assert result["a"].to_list() == [1, 2, 3], "a non-recursive pattern must not pull in the nested file"


@EXPORTERS
def test_include_file_paths_emitted_and_present_in_result(tmp_path, export_func):
    directory = make_parquet_dir(tmp_path)
    flow = add_read(
        create_flow(), str(directory), "parquet", input_schema.InputParquetTable(), include_file_paths="source_file"
    )

    code = export_func(flow)
    assert 'include_file_paths="source_file"' in code
    result = assert_matches_flow(code, flow)
    assert "source_file" in result.columns
    assert all(path.endswith(".parquet") for path in result["source_file"].to_list())


@EXPORTERS
def test_include_file_paths_omitted_when_unset(tmp_path, export_func):
    directory = make_parquet_dir(tmp_path)
    flow = add_read(create_flow(), str(directory), "parquet", input_schema.InputParquetTable())

    # The kwarg form, not the bare word: pytest's tmp dir is named after this test and
    # would otherwise match inside the emitted path.
    assert "include_file_paths=" not in export_func(flow)


def test_polars_export_expands_the_pattern_itself(tmp_path):
    """The Polars script must not lean on polars' globbing; it repeats the engine's expansion."""
    directory = make_csv_dir(tmp_path)
    flow = add_read(create_flow(), str(directory), "csv", csv_settings())

    code = export_flow_to_polars(flow)
    assert "import glob" in code
    assert "import os" in code
    assert "sorted(p for p in glob.glob(" in code
    assert "if os.path.isfile(p)" in code


def test_flowframe_export_keeps_the_read_node_shape(tmp_path):
    """The FlowFrame script stays a reader call, so re-importing it yields the same read node."""
    directory = make_csv_dir(tmp_path)
    flow = add_read(create_flow(), str(directory), "csv", csv_settings())

    code = export_flow_to_flowframe(flow)
    assert 'scan_mode="directory"' in code
    assert "glob.glob(" not in code
    assert str(directory) in code


@EXPORTERS
@pytest.mark.parametrize("file_type", ["csv", "parquet"])
def test_single_file_emission_is_unchanged(tmp_path, export_func, file_type):
    """Directory mode is strictly additive: a single-file read emits exactly what it always did."""
    if file_type == "csv":
        path = tmp_path / "single.csv"
        pl.DataFrame({"a": [1, 2]}).write_csv(path)
        settings = csv_settings()
    else:
        path = tmp_path / "single.parquet"
        pl.DataFrame({"a": [1, 2]}).write_parquet(path)
        settings = input_schema.InputParquetTable()
    flow = add_read(create_flow(), str(path), file_type, settings, scan_mode="single_file")

    code = export_func(flow)
    assert "scan_mode" not in code
    assert "include_file_paths=" not in code
    assert "glob.glob(" not in code
    assert_matches_flow(code, flow)


@EXPORTERS
def test_single_file_ipc_read_is_generated(tmp_path, export_func):
    """ipc used to fall through the read chain and emit nothing, leaving an unbound variable."""
    path = tmp_path / "single.arrow"
    pl.DataFrame({"a": [1, 2]}).write_ipc(path)
    flow = add_read(create_flow(), str(path), "ipc", input_schema.InputIpcTable(), scan_mode="single_file")

    code = export_func(flow)
    assert "scan_ipc(" in code
    assert_matches_flow(code, flow)


def force_directory_settings(flow: FlowGraph, received: input_schema.ReceivedTable, node_id: int = 1) -> None:
    """Put unsupported directory settings on an existing read node.

    ``add_read`` refuses these combinations at build time, so they can only reach the code
    generator from a flow that was saved with them; the exporters must still refuse rather
    than emit a script that dies on the first read.
    """
    flow.get_node(node_id).setting_input.received_file = received


@EXPORTERS
@pytest.mark.parametrize(
    "file_type,table_settings,expected",
    [
        ("excel", input_schema.InputExcelTable(sheet_name="Sheet1"), "not supported for file type 'excel'"),
        ("csv", input_schema.InputCsvTable(encoding="latin1"), "requires a UTF-8 encoding"),
    ],
    ids=["excel", "latin1_csv"],
)
def test_unsupported_directory_read_is_refused(tmp_path, export_func, file_type, table_settings, expected):
    directory = make_csv_dir(tmp_path)
    single = tmp_path / "single.csv"
    pl.DataFrame({"a": [1]}).write_csv(single)
    flow = add_read(create_flow(), str(single), "csv", csv_settings(), scan_mode="single_file")
    force_directory_settings(
        flow,
        input_schema.ReceivedTable(
            name=None,
            path=str(directory),
            file_type=file_type,
            scan_mode="directory",
            table_settings=table_settings,
        ),
    )

    with pytest.raises(UnsupportedNodeError) as exc_info:
        export_func(flow)
    assert expected in str(exc_info.value)
