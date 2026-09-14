"""
Tests for the list_files source node.

Run with:
    pytest flowfile_core/tests/flowfile/test_list_files.py -v
"""
from pathlib import Path

import pytest
from fastapi import HTTPException

from flowfile_core.fileExplorer import funcs as file_explorer_funcs
from flowfile_core.fileExplorer.funcs import DirectoryScanCancelledError
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


class TestCancellation:
    """The walk runs in core, so it must poll for cancellation itself."""

    @pytest.fixture
    def many_files(self, tmp_path: Path) -> Path:
        for i in range(40):
            (tmp_path / f"f{i:03d}.csv").write_text("v\n1\n")
        return tmp_path

    def test_cancel_check_aborts_the_walk(self, many_files):
        with pytest.raises(DirectoryScanCancelledError):
            scan_directory_to_frame(settings(many_files), cancel_check=lambda: True)

    def test_cancel_check_is_polled_per_entry(self, many_files):
        """Polling granularity is one entry, so a huge folder still aborts promptly."""
        calls = {"n": 0}

        def cancel_after_five() -> bool:
            calls["n"] += 1
            return calls["n"] >= 5

        with pytest.raises(DirectoryScanCancelledError):
            scan_directory_to_frame(settings(many_files), cancel_check=cancel_after_five)
        assert calls["n"] == 5, "walk kept going past the cancel"

    def test_no_cancel_check_is_unaffected(self, many_files):
        assert scan_directory_to_frame(settings(many_files)).height == 40
        assert scan_directory_to_frame(settings(many_files), cancel_check=lambda: False).height == 40

    def test_cancelling_a_run_stops_the_node_mid_walk(self, many_files, monkeypatch):
        """Regression: the scan used to run to completion after cancel was requested."""
        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(settings(many_files))

        original = file_explorer_funcs.FileInfo.from_path
        seen = {"n": 0}

        def cancel_partway(cls_path, *args, **kwargs):
            seen["n"] += 1
            if seen["n"] == 5:
                graph.cancel()
            return original(cls_path, *args, **kwargs)

        monkeypatch.setattr(file_explorer_funcs.FileInfo, "from_path", cancel_partway)

        run_info = graph.run_graph()
        assert not run_info.success
        assert graph.get_node(1).node_stats.is_canceled
        assert seen["n"] < 40, f"walk visited {seen['n']}/40 entries after cancel"
        # A cancelled node reports success=None (cancelled), never False (failed).
        assert [r.success for r in run_info.node_step_result] == [None]


class TestCancellationHardening:
    """Regressions found reviewing the first cut of the cancel fix."""

    @pytest.fixture
    def empty_dirs(self, tmp_path: Path) -> Path:
        """A tree that is all directories and no files: the per-entry poll never fires."""
        for i in range(200):
            (tmp_path / f"d{i:03d}").mkdir()
        return tmp_path

    def test_queue_loop_polls_on_a_tree_of_empty_directories(self, empty_dirs):
        """Without a poll in the ``while dirs_to_process`` loop this ran to completion."""
        calls = {"n": 0}

        def cancel_after(n: int):
            def check() -> bool:
                calls["n"] += 1
                return calls["n"] > n

            return check

        with pytest.raises(DirectoryScanCancelledError):
            scan_directory_to_frame(
                settings(empty_dirs, recursive=True, include_directories=True),
                cancel_check=cancel_after(210),
            )
        assert calls["n"] < 500, "queue loop kept walking after the cancel"

    def test_a_cancelled_run_does_not_poison_the_next_preview(self, sample_tree):
        """The graph flag is only cleared at the *next* run, so it must be ignored when idle."""
        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(settings(sample_tree, file_types=["csv"]))

        graph.flow_settings.is_running = True
        graph.cancel()
        graph.flow_settings.is_running = False
        graph.get_node(1)._execution_state.is_canceled = False

        # Materialising outside a run (preview / fetch) must still work.
        data = graph.get_node(1).get_resulting_data().collect()
        assert data["file_name"].to_list() == ["a.csv"]

    def test_graph_cancel_is_mirrored_onto_the_node(self, sample_tree, monkeypatch):
        """FlowGraph.cancel sets the graph flag first; the node flag is what reclassifies."""
        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(settings(sample_tree))
        node = graph.get_node(1)

        # Simulate observing the graph flag before FlowNode.cancel() reached this node.
        graph.flow_settings.is_running = True
        graph.flow_settings.is_canceled = True
        node._execution_state.is_canceled = False

        with pytest.raises(DirectoryScanCancelledError):
            node.get_resulting_data()
        assert node._execution_state.is_canceled, "node flag not mirrored; executor would log a failure"

    def test_cancelled_node_is_not_stamped_with_an_invalid_graph_error(self, many_files_for_error, monkeypatch):
        graph = create_graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="list_files"))
        graph.add_list_files(settings(many_files_for_error))

        original = file_explorer_funcs.FileInfo.from_path
        seen = {"n": 0}

        def cancel_partway(cls_path, *args, **kwargs):
            seen["n"] += 1
            if seen["n"] == 5:
                graph.cancel()
            return original(cls_path, *args, **kwargs)

        monkeypatch.setattr(file_explorer_funcs.FileInfo, "from_path", cancel_partway)
        graph.run_graph()

        errors = graph.get_node(1).results.errors
        assert errors is None or "invalid graph" not in errors, f"misleading error on a cancel: {errors!r}"

    @pytest.fixture
    def many_files_for_error(self, tmp_path: Path) -> Path:
        for i in range(40):
            (tmp_path / f"f{i:03d}.csv").write_text("v\n1\n")
        return tmp_path

    def test_symlink_cycle_does_not_expand(self, tmp_path):
        """A symlink to an ancestor yields a fresh Path per level, so the visited
        set must key on the real path or the cycle is walked once per depth level."""
        data = tmp_path / "data"
        data.mkdir()
        (data / "a.csv").write_text("v\n1\n")
        (data / "backup").symlink_to(data, target_is_directory=True)

        df = scan_directory_to_frame(settings(data, recursive=True, max_depth=12))
        assert df["file_name"].to_list() == ["a.csv"], "symlink cycle expanded"

    def test_cancellation_error_is_telemetry_classified(self):
        """The allowlist already names the DB read's twin; an unlisted class reports OtherError."""
        from flowfile_core.telemetry import ERROR_CLASS_ALLOWLIST

        assert "DirectoryScanCancelledError" in ERROR_CLASS_ALLOWLIST


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
