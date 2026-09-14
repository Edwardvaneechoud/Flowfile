"""
Tests for the list_files source node.

Run with:
    pytest flowfile_core/tests/flowfile/test_list_files.py -v
"""
from pathlib import Path

import pytest
from fastapi import HTTPException

from flowfile_core.flowfile.flow_graph import (
    FlowGraph,
    add_connection,
    list_files_schema,
    scan_directory_to_frame,
)
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema, schemas


EXPECTED_COLUMNS = [
    "file_name",
    "file_path",
    "directory",
    "relative_path",
    "file_type",
    "size_bytes",
    "last_modified",
    "created_date",
    "is_directory",
]


@pytest.fixture
def sample_tree(tmp_path: Path) -> Path:
    (tmp_path / "a.csv").write_text("x,y\n1,2\n")
    (tmp_path / "b.parquet").write_bytes(b"PAR1" + b"0" * 20)
    (tmp_path / "notes.txt").write_text("hello")
    (tmp_path / ".hidden.csv").write_text("secret")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "c.csv").write_text("z\n3\n")
    return tmp_path


def create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="test_flow", path=".", execution_mode="Development")
    )
    return handler.get_flow(flow_id)


def settings(path: Path, node_id: int = 1, flow_id: int = 1, **kwargs) -> input_schema.NodeListFiles:
    return input_schema.NodeListFiles(flow_id=flow_id, node_id=node_id, path=str(path), **kwargs)


class TestScan:
    def test_lists_files_only_by_default(self, sample_tree):
        df = scan_directory_to_frame(settings(sample_tree))
        assert sorted(df["file_name"].to_list()) == ["a.csv", "b.parquet", "notes.txt"]
        assert df["is_directory"].to_list() == [False] * 3

    def test_output_schema_is_fixed(self, sample_tree, tmp_path):
        assert scan_directory_to_frame(settings(sample_tree)).columns == EXPECTED_COLUMNS
        empty = tmp_path / "empty"
        empty.mkdir()
        empty_df = scan_directory_to_frame(settings(empty))
        assert empty_df.columns == EXPECTED_COLUMNS
        assert empty_df.height == 0

    def test_schema_callback_matches_scan(self, sample_tree):
        df = scan_directory_to_frame(settings(sample_tree))
        assert [c.column_name for c in list_files_schema()] == df.columns

    def test_file_type_filter_is_normalized(self, sample_tree):
        for spec in (["csv"], [".csv"], ["CSV"], [".CSV"]):
            df = scan_directory_to_frame(settings(sample_tree, file_types=spec))
            assert df["file_name"].to_list() == ["a.csv"], spec

    def test_recursive_walks_subfolders(self, sample_tree):
        flat = scan_directory_to_frame(settings(sample_tree, file_types=["csv"]))
        deep = scan_directory_to_frame(settings(sample_tree, file_types=["csv"], recursive=True))
        assert flat["file_name"].to_list() == ["a.csv"]
        assert sorted(deep["file_name"].to_list()) == ["a.csv", "c.csv"]

    def test_relative_path_is_relative_to_the_scanned_root(self, sample_tree):
        df = scan_directory_to_frame(settings(sample_tree, file_types=["csv"], recursive=True))
        rels = dict(zip(df["file_name"].to_list(), df["relative_path"].to_list()))
        assert rels["a.csv"] == "a.csv"
        assert Path(rels["c.csv"]) == Path("sub/c.csv")

    def test_file_path_is_absolute_and_readable(self, sample_tree):
        df = scan_directory_to_frame(settings(sample_tree, file_types=["csv"]))
        path = Path(df["file_path"][0])
        assert path.is_absolute()
        assert path.read_text() == "x,y\n1,2\n"

    def test_hidden_files_are_opt_in(self, sample_tree):
        assert ".hidden.csv" not in scan_directory_to_frame(settings(sample_tree))["file_name"].to_list()
        visible = scan_directory_to_frame(settings(sample_tree, include_hidden=True))["file_name"].to_list()
        assert ".hidden.csv" in visible

    def test_directories_are_opt_in(self, sample_tree):
        df = scan_directory_to_frame(settings(sample_tree, include_directories=True))
        rows = dict(zip(df["file_name"].to_list(), df["is_directory"].to_list()))
        assert rows["sub"] is True
        assert rows["a.csv"] is False

    def test_directories_only(self, sample_tree):
        df = scan_directory_to_frame(settings(sample_tree, include_files=False, include_directories=True))
        assert df["file_name"].to_list() == ["sub"]

    def test_max_files_caps_rows(self, sample_tree):
        assert scan_directory_to_frame(settings(sample_tree, max_files=2)).height == 2

    def test_size_and_timestamps_are_populated(self, sample_tree):
        df = scan_directory_to_frame(settings(sample_tree, file_types=["csv"]))
        assert df["size_bytes"][0] == len("x,y\n1,2\n")
        assert df["last_modified"][0] is not None
        assert df["created_date"][0] is not None

    def test_missing_folder_raises_a_readable_error(self, tmp_path):
        with pytest.raises(HTTPException) as e:
            scan_directory_to_frame(settings(tmp_path / "nope"))
        assert e.value.status_code == 400
        assert "does not exist" in e.value.detail

    def test_empty_path_raises(self):
        with pytest.raises(HTTPException) as e:
            scan_directory_to_frame(input_schema.NodeListFiles(flow_id=1, node_id=1, path=""))
        assert e.value.status_code == 400
        assert "No folder selected" in e.value.detail

    def test_a_file_is_not_a_folder(self, sample_tree):
        with pytest.raises(HTTPException) as e:
            scan_directory_to_frame(settings(sample_tree / "a.csv"))
        assert e.value.status_code == 400


class TestSettingsModel:
    def test_requires_files_or_directories(self):
        with pytest.raises(ValueError):
            input_schema.NodeListFiles(flow_id=1, node_id=1, path="/tmp", include_files=False)

    def test_file_types_accept_a_comma_string(self):
        s = input_schema.NodeListFiles(flow_id=1, node_id=1, path="/tmp", file_types=".csv, .Parquet")
        assert s.file_types == ["csv", "parquet"]

    def test_default_description_summarizes_the_folder(self, sample_tree):
        s = settings(sample_tree, file_types=["csv"], recursive=True)
        desc = s.get_default_description()
        assert sample_tree.name in desc
        assert ".csv" in desc
        assert "recursive" in desc

    def test_settings_class_name_matches_the_route_convention(self):
        """add_generic_settings resolves 'node' + node_type without underscores."""
        assert input_schema.NodeListFiles.__name__.lower() == "node" + "list_files".replace("_", "")


class TestGraph:
    def test_node_runs_and_produces_rows(self, sample_tree):
        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(settings(sample_tree, file_types=["csv"]))
        run_info = graph.run_graph()
        assert run_info.success, run_info.errors
        data = graph.get_node(1).get_resulting_data().data_frame.collect()
        assert data["file_name"].to_list() == ["a.csv"]

    def test_schema_is_predicted_without_touching_the_disk(self, tmp_path):
        """The folder need not exist for the UI to know the output columns."""
        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(settings(tmp_path / "does-not-exist"))
        predicted = graph.get_node(1).get_predicted_schema()
        assert [c.column_name for c in predicted] == EXPECTED_COLUMNS

    def test_graph_method_name_matches_the_route_convention(self):
        assert hasattr(FlowGraph, "add_" + "list_files")

    def test_registered_in_the_settings_class_registry(self):
        assert schemas.NODE_TYPE_TO_SETTINGS_CLASS["list_files"] is input_schema.NodeListFiles

    def test_template_is_a_source_node(self):
        from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

        _, node_dict, _ = get_all_standard_nodes()
        tmpl = node_dict["list_files"]
        assert (tmpl.input, tmpl.output, tmpl.node_type, tmpl.node_group) == (0, 1, "input", "input")

    def test_feeds_a_downstream_node(self, sample_tree):
        """The point of the node: file_path drives what comes next."""
        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(settings(sample_tree, file_types=["csv"], recursive=True))
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="record_count"))
        add_connection(
            graph,
            input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2),
        )
        graph.add_record_count(input_schema.NodeRecordCount(flow_id=1, node_id=2, depending_on_id=1))
        run_info = graph.run_graph()
        assert run_info.success, run_info.errors
        counted = graph.get_node(2).get_resulting_data().data_frame.collect()
        assert counted["number_of_records"][0] == 2


class TestPersistence:
    def test_survives_a_save_reload_round_trip(self, sample_tree, tmp_path):
        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(
            settings(sample_tree, file_types=["csv"], recursive=True, include_hidden=True, max_files=5)
        )

        yaml_path = tmp_path / "list_files_flow.yaml"
        graph.save_flow(str(yaml_path))
        loaded = open_flow(yaml_path)

        reloaded = loaded.get_node(1).setting_input
        assert isinstance(reloaded, input_schema.NodeListFiles)
        assert reloaded.path == str(sample_tree)
        assert reloaded.file_types == ["csv"]
        assert (reloaded.recursive, reloaded.include_hidden, reloaded.max_files) == (True, True, 5)

        run_info = loaded.run_graph()
        assert run_info.success, run_info.errors
        names = loaded.get_node(1).get_resulting_data().collect()["file_name"].to_list()
        assert sorted(names) == [".hidden.csv", "a.csv", "c.csv"]


class TestCodeExport:
    def _graph(self, sample_tree):
        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(settings(sample_tree, file_types=["csv"], recursive=True, max_files=10))
        return graph

    def test_exports_to_flowframe_code(self, sample_tree):
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_flowframe

        code = export_flow_to_flowframe(self._graph(sample_tree))
        assert "ff.list_files(" in code
        assert "recursive=True" in code
        assert "max_files=10" in code

    def test_exports_to_polars_code(self, sample_tree):
        from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

        code = export_flow_to_polars(self._graph(sample_tree))
        assert "ff.list_files(" in code
        assert ").data" in code

    def test_export_refuses_an_unconfigured_node(self):
        from flowfile_core.flowfile.code_generator.code_generator import (
            UnsupportedNodeError,
            export_flow_to_flowframe,
        )

        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(input_schema.NodeListFiles(flow_id=1, node_id=1, path=""))
        with pytest.raises(UnsupportedNodeError):
            export_flow_to_flowframe(graph)
