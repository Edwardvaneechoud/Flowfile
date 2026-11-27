"""On a server a flow's file nodes use the running person's workspace and the shared folder, nobody else's workspace.

The conftest puts the process in docker mode with ``tmp_path`` as the user data folder. A run from the editor is
made for its person (``acting_for``); a scheduled or headless run has none, and its nodes carry the run's user.
"""

import os

import polars as pl
import pytest

from flowfile_core.fileExplorer import workspace
from flowfile_core.flowfile.execution import registry
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection, scan_directory_to_frame
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas

REFUSED = "is not in your workspace or the shared folder, so this flow cannot use it."


@pytest.fixture
def alice_file(users):
    folder = workspace.workspace_of(users["alice"].id)
    pl.DataFrame({"secret_col": [1, 2]}).write_csv(folder / "private.csv")
    pl.DataFrame({"secret_col": [1, 2]}).write_parquet(folder / "private.parquet")
    return folder / "private.csv"


def _graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id, name="workspace", path=".", execution_mode="Development", execution_location="local"
        )
    )
    return handler.get_flow(flow_id)


def _read_graph(path, user_id: int, file_type: str = "csv", **received) -> FlowGraph:
    graph = _graph()
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="read"))
    received_file = input_schema.ReceivedTable.create_from_path(str(path), file_type=file_type)
    for field, value in received.items():
        setattr(received_file, field, value)
    graph.add_read(input_schema.NodeRead(flow_id=1, node_id=1, received_file=received_file, user_id=user_id))
    return graph


def _write_graph(directory, user_id: int, name: str = "out.csv") -> FlowGraph:
    graph = _graph()
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}]), user_id=user_id
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type="output"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    output_settings = input_schema.OutputSettings(
        name=name, directory=str(directory), file_type="csv", table_settings=input_schema.OutputCsvTable()
    )
    graph.add_output(input_schema.NodeOutput(flow_id=1, node_id=2, output_settings=output_settings, user_id=user_id))
    return graph


def _error(run_info, node_id: int) -> str:
    [result] = [r for r in run_info.node_step_result if r.node_id == node_id]
    assert result.success is False, f"node {node_id} was expected to fail"
    return result.error


def _ran(run_info) -> None:
    assert run_info.success, [r.error for r in run_info.node_step_result]


class TestReadNode:
    def test_a_person_reads_a_file_in_their_own_workspace(self, users):
        own = workspace.workspace_of(users["bob"].id) / "mine.csv"
        pl.DataFrame({"a": [1, 2, 3]}).write_csv(own)
        _ran(_read_graph(own, users["bob"].id).run_graph())

    def test_a_file_in_the_shared_folder_reads_for_everyone(self, users, tmp_path):
        pl.DataFrame({"a": [1]}).write_csv(tmp_path / "team.csv")
        for key in ("alice", "bob"):
            _ran(_read_graph(tmp_path / "team.csv", users[key].id).run_graph())

    def test_another_persons_file_is_refused_in_one_sentence_from_the_editor(self, users, alice_file):
        graph = _read_graph(alice_file, users["bob"].id)
        with registry.acting_for(users["bob"].id):
            error = _error(graph.run_graph(), 1)
        assert error.endswith(REFUSED) and str(alice_file) in error

    def test_on_a_run_with_no_person_the_nodes_user_decides(self, users, alice_file):
        assert REFUSED in _error(_read_graph(alice_file, users["bob"].id).run_graph(), 1)
        _ran(_read_graph(alice_file, users["alice"].id).run_graph())

    def test_the_running_person_counts_over_whoever_set_the_node(self, users, alice_file):
        graph = _read_graph(alice_file, users["alice"].id)
        with registry.acting_for(users["bob"].id):
            assert REFUSED in _error(graph.run_graph(), 1)

    def test_dot_dot_a_symlink_and_a_glob_cannot_reach_it(self, users, alice_file, tmp_path):
        mine = workspace.workspace_of(users["bob"].id)
        (mine / "link.csv").symlink_to(alice_file)
        for path in (
            mine / ".." / alice_file.parent.name / alice_file.name,
            mine / "link.csv",
            tmp_path / "workspaces" / "*" / "private.csv",
        ):
            assert REFUSED in _error(_read_graph(path, users["bob"].id).run_graph(), 1), path

    def test_a_directory_scan_of_another_persons_folder_is_refused(self, users, alice_file, tmp_path):
        for folder in (alice_file.parent, tmp_path / "workspaces"):
            graph = _read_graph(folder, users["bob"].id, file_type="parquet", scan_mode="directory")
            assert REFUSED in _error(graph.run_graph(), 1), folder

    def test_schema_prediction_does_not_read_another_persons_file(self, users, alice_file):
        graph = _read_graph(alice_file, users["bob"].id)
        predicted = graph.get_node(1).get_predicted_schema() or []
        assert "secret_col" not in [column.column_name for column in predicted]


class TestOutputNode:
    def test_an_output_node_on_another_persons_folder_says_why_before_a_run(self, client_for, users):
        bob = client_for("bob")
        own = workspace.workspace_of(users["bob"].id)
        theirs = workspace.workspace_of(users["alice"].id)
        flow_id = bob.post(
            "/editor/create_flow/", params={"flow_path": str(own / "writes.yaml"), "register_in_catalog": False}
        ).json()
        bob.post("/editor/add_node/", params={"flow_id": flow_id, "node_id": 1, "node_type": "output"})

        def shown(directory) -> str:
            body = {
                "flow_id": flow_id,
                "node_id": 1,
                "output_settings": {
                    "name": "out.csv",
                    "directory": str(directory),
                    "file_type": "csv",
                    "table_settings": {"file_type": "csv"},
                },
            }
            saved = bob.post("/update_settings/", params={"node_type": "output"}, json=body)
            assert saved.status_code == 200, saved.text
            return bob.get("/node", params={"flow_id": flow_id, "node_id": 1}).json().get("prediction_warning") or ""

        assert REFUSED in shown(theirs)
        assert shown(own) == ""
        assert shown("${folder}") == ""

    def test_on_a_desktop_the_path_set_with_the_node_is_the_path_written(self, monkeypatch, tmp_path):
        monkeypatch.setenv("FLOWFILE_MODE", "electron")
        graph = _write_graph(tmp_path / "first", 1)
        settings = graph.get_node(2).setting_input.output_settings
        (tmp_path / "second").mkdir()
        settings.abs_file_path = str(tmp_path / "second" / "out.csv")
        _ran(graph.run_graph())
        assert (tmp_path / "second" / "out.csv").exists()
        assert settings.abs_file_path == str(tmp_path / "second" / "out.csv")

    def test_a_person_writes_into_their_own_workspace(self, users):
        own = workspace.workspace_of(users["bob"].id)
        _ran(_write_graph(own, users["bob"].id).run_graph())
        assert (own / "out.csv").exists()

    def test_writing_into_another_persons_workspace_is_refused(self, users):
        theirs = workspace.workspace_of(users["alice"].id)
        assert REFUSED in _error(_write_graph(theirs, users["bob"].id).run_graph(), 2)
        assert not (theirs / "out.csv").exists()


class TestTheAppsOwnFolders:
    def test_nothing_is_written_into_the_custom_nodes_folder(self, users):
        from shared.storage_config import storage

        storage.user_defined_nodes_directory.mkdir(parents=True, exist_ok=True)
        graph = _write_graph(storage.user_defined_nodes_directory, users["bob"].id, name="node.py")
        assert REFUSED in _error(graph.run_graph(), 2)
        assert not (storage.user_defined_nodes_directory / "node.py").exists()

    def test_a_flow_file_in_the_flows_folder_is_not_written_or_read_by_a_file_node(self, users):
        from shared.storage_config import storage

        storage.flows_directory.mkdir(parents=True, exist_ok=True)
        kept = storage.flows_directory / "7_alice_flow.yaml"
        kept.write_text("alice: original\n")
        graph = _write_graph(storage.flows_directory, users["bob"].id, name="7_alice_flow.yaml")
        assert REFUSED in _error(graph.run_graph(), 2)
        assert kept.read_text() == "alice: original\n"
        (storage.flows_directory / "data.csv").write_text("a\n1\n")
        assert REFUSED in _error(_read_graph(storage.flows_directory / "data.csv", users["bob"].id).run_graph(), 1)

    def test_the_uploads_and_outputs_folders_are_read_and_written(self, users):
        from shared.storage_config import storage

        for folder in (storage.uploads_directory, storage.outputs_directory):
            folder.mkdir(parents=True, exist_ok=True)
            _ran(_write_graph(folder, users["bob"].id).run_graph())
            _ran(_read_graph(folder / "out.csv", users["bob"].id).run_graph())


class TestTemplateData:
    @pytest.fixture
    def template_file(self, tmp_path_factory, monkeypatch):
        from shared.storage_config import storage

        monkeypatch.setattr(storage, "_base_dir", tmp_path_factory.mktemp("internal"))
        storage.template_data_directory.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"part": ["a", "b"]}).write_csv(storage.template_data_directory / "parts.csv")
        return storage.template_data_directory / "parts.csv"

    def test_a_flow_reads_the_data_of_a_template(self, users, template_file):
        _ran(_read_graph(template_file, users["bob"].id).run_graph())
        assert workspace.data_place(str(template_file), None) is not None

    def test_nothing_is_written_into_the_template_data(self, users, template_file):
        assert REFUSED in _error(_write_graph(template_file.parent, users["bob"].id).run_graph(), 2)
        assert not (template_file.parent / "out.csv").exists()

    def test_a_flow_made_from_a_template_predicts_its_columns(self, client_for, users):
        from flowfile_core import flow_file_handler
        from shared.storage_config import storage

        bob = client_for("bob")
        storage.flows_directory.mkdir(parents=True, exist_ok=True)
        made = bob.post("/templates/bill_of_materials_explosion/create")
        assert made.status_code == 200, made.text
        flow = flow_file_handler.get_flow(made.json())
        try:
            reads = [node for node in flow.nodes if node.node_type == "read"]
            assert reads
            for read in reads:
                shown = bob.get("/node", params={"flow_id": made.json(), "node_id": read.node_id}).json()
                assert not shown.get("prediction_warning"), shown.get("prediction_warning")
                assert shown["main_output"]["columns"]
        finally:
            flow_file_handler.delete_flow(made.json())


class TestAFolderInSingleFileMode:
    def test_a_folder_that_holds_other_peoples_files_is_refused(self, users, tmp_path):
        theirs = workspace.workspace_of(users["alice"].id)
        pl.DataFrame({"secret_col": [41, 42]}).write_parquet(theirs / "private.parquet")
        graph = _read_graph(tmp_path, users["bob"].id, file_type="parquet")
        assert graph.get_node(1).setting_input.received_file.scan_mode == "single_file"
        predicted = graph.get_node(1).get_predicted_schema() or []
        assert "secret_col" not in [column.column_name for column in predicted]
        error = _error(graph.run_graph(), 1)
        assert "also matches files in other people's workspaces" in error
        assert "private.parquet" not in error

    def test_a_folder_of_ones_own_files_is_read(self, users):
        own = workspace.workspace_of(users["bob"].id) / "parts"
        own.mkdir()
        pl.DataFrame({"a": [1]}).write_parquet(own / "one.parquet")
        pl.DataFrame({"a": [2]}).write_parquet(own / "two.parquet")
        graph = _read_graph(own, users["bob"].id, file_type="parquet")
        _ran(graph.run_graph())


class TestListFiles:
    def _settings(self, path, user_id: int) -> input_schema.NodeListFiles:
        return input_schema.NodeListFiles(flow_id=1, node_id=1, path=str(path), user_id=user_id)

    def test_another_persons_folder_is_refused(self, users, alice_file):
        with pytest.raises(Exception) as refused:
            scan_directory_to_frame(self._settings(alice_file.parent, users["bob"].id))
        assert REFUSED in str(getattr(refused.value, "detail", refused.value))

    def test_the_shared_folder_lists_no_workspace(self, users, alice_file, tmp_path):
        frame = scan_directory_to_frame(self._settings(tmp_path, users["bob"].id))
        assert "workspaces" not in frame["file_name"].to_list()
        own = scan_directory_to_frame(self._settings(alice_file.parent, users["alice"].id))
        assert "private.csv" in own["file_name"].to_list()


class TestExcelSheetNames:
    def test_another_persons_workbook_is_refused(self, client_for, users):
        theirs = workspace.workspace_of(users["alice"].id) / "book.xlsx"
        pl.DataFrame({"a": [1]}).write_excel(theirs)
        response = client_for("bob").get("/api/get_xlsx_sheet_names", params={"path": str(theirs)})
        assert response.status_code == 403
        assert client_for("alice").get("/api/get_xlsx_sheet_names", params={"path": str(theirs)}).status_code == 200


class TestDataSourceNode:
    def test_another_persons_file_is_refused(self, users, alice_file):
        graph = _graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
        graph.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=1, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
            )
        )
        graph.add_datasource(
            input_schema.NodeDatasource(
                flow_id=1, node_id=1, file_ref=str(alice_file.with_suffix(".parquet")), user_id=users["bob"].id
            )
        )
        assert REFUSED in _error(graph.run_graph(), 1)

    @pytest.mark.skipif(os.name == "nt", reason="Windows allows no * in a file name, so no such file can exist")
    def test_a_file_whose_name_is_a_pattern_that_reaches_another_persons_file_is_refused(
        self, users, alice_file, tmp_path
    ):
        """The engine opens the file of that very name first and then scans the same text, which expands."""
        theirs = alice_file.with_suffix(".parquet")
        named = tmp_path / "workspace*" / theirs.parent.name / theirs.name
        named.parent.mkdir(parents=True)
        pl.DataFrame({"mine": [1]}).write_parquet(named)
        graph = _graph()
        graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type="manual_input"))
        graph.add_datasource(
            input_schema.NodeDatasource(flow_id=1, node_id=1, file_ref=str(named), user_id=users["bob"].id)
        )
        assert "other people's workspaces" in _error(graph.run_graph(), 1)


class TestPatterns:
    def test_a_pattern_reaching_a_hidden_folder_is_refused(self, users, tmp_path):
        theirs = workspace.workspace_of(users["alice"].id)
        (theirs / ".keep").mkdir()
        pl.DataFrame({"secret_col": [1, 2]}).write_ipc(theirs / ".keep" / "private.arrow")
        graph = _read_graph(tmp_path / "*" / "*" / "*" / "private.arrow", users["bob"].id, file_type="ipc")
        error = _error(graph.run_graph(), 1)
        assert "other people's workspaces" in error

    def test_a_scan_of_the_shared_folder_says_why_it_is_refused(self, users, tmp_path):
        theirs = workspace.workspace_of(users["alice"].id)
        pl.DataFrame({"x": [1]}).write_parquet(theirs / "a.parquet")
        pl.DataFrame({"x": [1]}).write_parquet(tmp_path / "team.parquet")
        graph = _read_graph(tmp_path, users["bob"].id, file_type="parquet", scan_mode="directory")
        error = _error(graph.run_graph(), 1)
        assert f'"{tmp_path}" also matches files in other people\'s workspaces' in error
        assert "is not in your workspace or the shared folder" not in error


class TestAHeadlessRun:
    def test_a_saved_flow_opened_for_a_person_uses_that_persons_workspace(self, users, alice_file):
        from flowfile_core.flowfile.manage.io_flowfile import open_flow

        path = workspace.workspace_of(users["bob"].id) / "reads_alice.yaml"
        _read_graph(alice_file, users["bob"].id).save_flow(str(path))
        as_bob = open_flow(path, user_id=users["bob"].id)
        assert REFUSED in _error(as_bob.run_graph(), 1)
        as_alice = open_flow(path, user_id=users["alice"].id)
        _ran(as_alice.run_graph())


class TestTheEditor:
    def _flow_with_a_read_node(self, client, users, path):
        own = workspace.workspace_of(users["bob"].id)
        created = client.post(
            "/editor/create_flow/", params={"flow_path": str(own / "mine.yaml"), "register_in_catalog": False}
        )
        flow_id = created.json()
        client.post(
            "/editor/add_node/",
            params={"flow_id": flow_id, "node_id": 1, "node_type": "read", "pos_x": 0, "pos_y": 0},
        )
        body = {
            "flow_id": flow_id,
            "node_id": 1,
            "received_file": {"path": str(path), "name": path.name, "file_type": "csv"},
        }
        saved = client.post("/update_settings/", params={"node_type": "read"}, json=body)
        assert saved.status_code == 200, saved.text
        return flow_id

    def test_a_read_node_on_another_persons_file_says_why_before_a_run(self, client_for, users, alice_file):
        bob = client_for("bob")
        flow_id = self._flow_with_a_read_node(bob, users, alice_file)
        node = bob.get("/node", params={"flow_id": flow_id, "node_id": 1})
        assert node.status_code == 200
        assert "secret_col" not in node.text
        assert REFUSED in (node.json().get("prediction_warning") or "")

    def test_a_run_and_a_fetch_from_the_editor_are_refused(self, client_for, users, alice_file):
        import time

        bob = client_for("bob")
        flow_id = self._flow_with_a_read_node(bob, users, alice_file)
        assert bob.post("/flow/run/", params={"flow_id": flow_id}).status_code == 200
        for _ in range(120):
            status = bob.get("/flow/run_status/", params={"flow_id": flow_id}).json()
            if not status.get("is_running") and status.get("node_step_result"):
                break
            time.sleep(0.25)
        [step] = [r for r in status["node_step_result"] if r["node_id"] == 1]
        assert step["success"] is False and REFUSED in step["error"]
        bob.post("/node/trigger_fetch_data", params={"flow_id": flow_id, "node_id": 1})
        time.sleep(1)
        data = bob.get("/node/data", params={"flow_id": flow_id, "node_id": 1})
        assert "secret_col" not in data.text


class TestTheCatalogStillWorks:
    def test_a_flow_writes_a_catalog_table_and_reads_it_back(self, users):
        from tests.flowfile.conftest import (
            add_test_catalog_writer,
            add_test_manual_input,
            catalog_cleanup,
            create_test_graph,
            create_test_namespace,
            run_test_graph,
        )

        catalog_cleanup()
        try:
            namespace_id = create_test_namespace()
            writer = create_test_graph(flow_id=71)
            add_test_manual_input(writer, [{"a": 1}, {"a": 2}], node_id=1, user_id=1)
            add_test_catalog_writer(writer, 2, 1, "in_catalog", namespace_id, user_id=1)
            run_test_graph(writer)
            reader = create_test_graph(flow_id=72)
            reader.add_node_promise(input_schema.NodePromise(flow_id=72, node_id=1, node_type="catalog_reader"))
            reader.add_catalog_reader(
                input_schema.NodeCatalogReader(
                    flow_id=72, node_id=1, catalog_table_name="in_catalog", catalog_namespace_id=namespace_id, user_id=1
                )
            )
            run_test_graph(reader)
            assert reader.get_node(1).get_resulting_data().collect()["a"].to_list() == [1, 2]
        finally:
            catalog_cleanup()
