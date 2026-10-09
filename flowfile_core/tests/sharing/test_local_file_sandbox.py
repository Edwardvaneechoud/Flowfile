"""Local read/write nodes in multi-user (docker) mode are confined to the user-data directory.

The conftest's autouse fixture puts the process in docker mode with ``tmp_path`` as the user-data
directory; ``outside`` is a sibling folder the nodes must not reach. Electron keeps the whole machine.
"""

import os

import polars as pl
import pytest
from fastapi import HTTPException

from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import (
    FlowGraph,
    add_connection,
    get_xlsx_schema_callback,
    scan_directory_to_frame,
)
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas

DENIED = "is outside the allowed directory"
LOCATIONS = pytest.mark.parametrize("location", ["local", "remote"])


@pytest.fixture
def user_data(tmp_path):
    return tmp_path


@pytest.fixture
def outside(tmp_path_factory):
    folder = tmp_path_factory.mktemp("outside_user_data")
    pl.DataFrame({"secret_col": [1, 2]}).write_parquet(folder / "secret.parquet")
    pl.DataFrame({"secret_col": [1, 2]}).write_csv(folder / "secret.csv")
    return folder


def _graph(location: str = "local") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=1, name="sandbox", path=".", execution_mode="Development", execution_location=location
        )
    )
    return handler.get_flow(1)


def _read_graph(path, location: str = "local", **received) -> FlowGraph:
    graph = _graph(location)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="read"))
    file_type = received.pop("file_type", "parquet")
    received_file = input_schema.ReceivedTable.create_from_path(str(path), file_type=file_type)
    for field, value in received.items():
        setattr(received_file, field, value)
    graph.add_read(input_schema.NodeRead(flow_id=1, node_id=1, received_file=received_file))
    return graph


def _write_graph(directory, location: str = "local") -> FlowGraph:
    graph = _graph(location)
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}, {"a": 2}])
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="output"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    output_settings = input_schema.OutputSettings(
        name="out.csv", directory=str(directory), file_type="csv", table_settings=input_schema.OutputCsvTable()
    )
    graph.add_output(input_schema.NodeOutput(flow_id=1, node_id=2, output_settings=output_settings))
    return graph


def _node_error(run_info, node_id: int) -> str:
    [result] = [result for result in run_info.node_step_result if result.node_id == node_id]
    assert result.success is False, f"node {node_id} was expected to fail"
    return result.error


class TestReadNode:
    def test_a_file_under_the_user_data_directory_runs(self, user_data):
        pl.DataFrame({"a": [1, 2, 3]}).write_parquet(user_data / "data.parquet")
        graph = _read_graph(user_data / "data.parquet")
        run_info = graph.run_graph()
        assert run_info.success, [r.error for r in run_info.node_step_result]
        assert graph.get_node(1).get_resulting_data().collect()["a"].to_list() == [1, 2, 3]

    @LOCATIONS
    def test_a_file_outside_it_fails_naming_the_allowed_directory(self, outside, user_data, location):
        run_info = _read_graph(outside / "secret.parquet", location).run_graph()
        assert not run_info.success
        error = _node_error(run_info, 1)
        assert DENIED in error
        assert str(user_data.resolve()) in error

    def test_dot_dot_cannot_climb_out(self, outside, user_data):
        climbing = user_data / ".." / outside.name / "secret.parquet"
        assert DENIED in _node_error(_read_graph(climbing).run_graph(), 1)

    def test_a_symlink_inside_pointing_outside_is_refused(self, outside, user_data):
        (user_data / "link.parquet").symlink_to(outside / "secret.parquet")
        assert DENIED in _node_error(_read_graph(user_data / "link.parquet").run_graph(), 1)

    def test_schema_prediction_does_not_read_an_outside_file(self, outside):
        graph = _read_graph(outside / "secret.csv", file_type="csv")
        predicted = graph.get_node(1).get_predicted_schema() or []
        assert "secret_col" not in [column.column_name for column in predicted]


class TestDirectoryScan:
    def test_a_folder_under_the_user_data_directory_runs(self, user_data):
        folder = user_data / "parts"
        folder.mkdir()
        pl.DataFrame({"a": [1]}).write_parquet(folder / "one.parquet")
        pl.DataFrame({"a": [2]}).write_parquet(folder / "two.parquet")
        graph = _read_graph(folder, scan_mode="directory")
        run_info = graph.run_graph()
        assert run_info.success, [r.error for r in run_info.node_step_result]
        assert sorted(graph.get_node(1).get_resulting_data().collect()["a"].to_list()) == [1, 2]

    @LOCATIONS
    def test_a_folder_outside_it_fails(self, outside, location):
        run_info = _read_graph(outside, location, scan_mode="directory").run_graph()
        assert DENIED in _node_error(run_info, 1)

    def test_a_symlink_among_the_matches_is_refused(self, outside, user_data):
        folder = user_data / "parts"
        folder.mkdir()
        pl.DataFrame({"secret_col": [0]}).write_parquet(folder / "own.parquet")
        (folder / "link.parquet").symlink_to(outside / "secret.parquet")
        run_info = _read_graph(folder, scan_mode="directory").run_graph()
        assert "link.parquet" in _node_error(run_info, 1)

    def test_schema_prediction_does_not_list_an_outside_folder(self, outside):
        graph = _read_graph(outside, scan_mode="directory")
        predicted = graph.get_node(1).get_predicted_schema() or []
        assert "secret_col" not in [column.column_name for column in predicted]


class TestOutputNode:
    def test_a_folder_under_the_user_data_directory_is_written(self, user_data):
        run_info = _write_graph(user_data).run_graph()
        assert run_info.success, [r.error for r in run_info.node_step_result]
        assert pl.read_csv(user_data / "out.csv")["a"].to_list() == [1, 2]

    def test_a_folder_outside_it_fails_and_writes_nothing(self, outside):
        run_info = _write_graph(outside).run_graph()
        assert DENIED in _node_error(run_info, 2)
        assert not (outside / "out.csv").exists()

    def test_the_worker_branch_refuses_before_shipping_the_write(self, outside):
        """Called directly: a docker-mode run of the upstream node would need the worker's token."""
        node = _write_graph(outside, "remote").get_node(2)
        with pytest.raises(PermissionError, match=DENIED):
            node.function(FlowDataEngine(pl.LazyFrame({"a": [1]})))
        assert not (outside / "out.csv").exists()


def test_the_excel_schema_probe_refuses_an_outside_file(outside):
    probe = get_xlsx_schema_callback(
        engine="openpyxl",
        file_path=str(outside / "secret.xlsx"),
        sheet_name="Sheet1",
        start_row=0,
        start_column=0,
        end_row=0,
        end_column=0,
        has_headers=True,
    )
    with pytest.raises(PermissionError, match=DENIED):
        probe()


def test_list_files_follows_the_live_mode(outside, monkeypatch):
    settings = input_schema.NodeListFiles(flow_id=1, node_id=1, path=str(outside))
    with pytest.raises(HTTPException) as refused:
        scan_directory_to_frame(settings)
    assert refused.value.status_code == 403
    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    assert "secret.csv" in scan_directory_to_frame(settings)["file_name"].to_list()


class TestElectronIsUnaffected:
    @pytest.fixture(autouse=True)
    def electron(self, monkeypatch):
        monkeypatch.setenv("FLOWFILE_MODE", "electron")

    def test_reads_anywhere(self, outside):
        graph = _read_graph(outside / "secret.parquet")
        run_info = graph.run_graph()
        assert run_info.success, [r.error for r in run_info.node_step_result]
        assert graph.get_node(1).get_resulting_data().collect()["secret_col"].to_list() == [1, 2]

    def test_scans_a_folder_anywhere(self, outside):
        run_info = _read_graph(outside, scan_mode="directory").run_graph()
        assert run_info.success, [r.error for r in run_info.node_step_result]

    def test_writes_anywhere(self, outside):
        run_info = _write_graph(outside).run_graph()
        assert run_info.success, [r.error for r in run_info.node_step_result]
        assert os.path.exists(outside / "out.csv")
