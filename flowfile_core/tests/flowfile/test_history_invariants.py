"""Graph-level invariants of the undo/redo history (I1 round-trip, I4 dirty) plus targeted regressions.

I1: every graph builder path (restore_from_snapshot, open_flow) reproduces the in-scope snapshot exactly:
``snapshot(build(s)) == s``.
"""

import asyncio
import threading
from pathlib import Path

import pytest
from fastapi import HTTPException

from flowfile_core.flowfile.flow_graph import delete_connection, insert_node_on_edge
from flowfile_core.flowfile.history_manager import HistoryManager
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import history_schema, input_schema, schemas, transform_schema
from tests.flowfile.history_graphs import (
    basic_filter,
    build_rich_graph,
    connect,
    edge_bookkeeping_errors,
    in_scope,
    make_graph,
    manual_input,
    promise,
)

REPO_ROOT = Path(__file__).resolve().parents[3]

FIXTURE_FLOW_GLOBS = (
    "data/templates/flows/*.yaml",
    "docs/assets/flows/*.yaml",
    "flowfile_core/flowfile_core/catalog/demo_flows/*.yaml",
    "flowfile_frontend/tests/fixtures/*.yaml",
    "flowfile_core/tests/support_files/flows/*.flowfile",
)


def _fixture_flows() -> list[Path]:
    paths: list[Path] = []
    for pattern in FIXTURE_FLOW_GLOBS:
        paths.extend(sorted(REPO_ROOT.glob(pattern)))
    return paths


def _snapshot(flow) -> schemas.FlowfileData:
    return schemas.FlowfileData.model_validate(flow.get_flowfile_data().model_dump())


def _node_edges(flow) -> set[tuple[int, int, str]]:
    edges = set()
    for node in in_scope(flow)["nodes"]:
        for target, handle in zip(node["outputs"] or [], node["output_handles"] or [], strict=False):
            edges.add((node["id"], target, handle))
    return edges


class TestRoundTrip:
    def test_restore_on_separate_graph_reproduces_snapshot(self):
        source = build_rich_graph(8101)
        expected = in_scope(source)

        target = make_graph(8102)
        target.restore_from_snapshot(_snapshot(source))

        assert in_scope(target) == expected

    def test_restore_on_same_graph_reproduces_snapshot(self):
        graph = build_rich_graph(8103)
        snapshot = _snapshot(graph)
        expected = in_scope(graph)

        promise(graph, "sort", 99)
        graph.delete_node(9)
        graph.restore_from_snapshot(snapshot)

        assert in_scope(graph) == expected

    def test_open_flow_reproduces_saved_graph(self, tmp_path):
        path = tmp_path / "rich.yaml"
        graph = build_rich_graph(8104, path=str(path))
        graph.save_flow(str(path))

        reopened = open_flow(path)

        assert in_scope(reopened) == in_scope(graph)

    def test_else_and_split_handles_survive_restore(self):
        graph = build_rich_graph(8105)
        snapshot = _snapshot(graph)
        edges = _node_edges(graph)
        assert (3, 5, "output-1") in edges
        assert (6, 7, "output-1") in edges
        assert (14, 15, "output-1") in edges

        graph.restore_from_snapshot(snapshot)

        assert _node_edges(graph) == edges
        assert graph.get_node(5)._input_output_handles[3] == "output-1"

    def test_multi_input_order_survives_restore(self):
        graph = build_rich_graph(8106)
        graph.restore_from_snapshot(_snapshot(graph))

        assert [n.node_id for n in graph.get_node(9).main_input] == [5, 4, 2]
        assert [n.node_id for n in graph.get_node(10).main_input] == [8, 1]

    def test_promise_metadata_survives_restore(self):
        graph = build_rich_graph(8107)
        group_of_13 = graph.get_node(13).setting_input.group_id
        graph.restore_from_snapshot(_snapshot(graph))

        restored = graph.get_node(13).setting_input
        assert restored.group_id == group_of_13 is not None
        assert restored.description == "Promise note"
        assert restored.node_reference == "pending_filter"

    @pytest.mark.parametrize("flow_path", _fixture_flows(), ids=lambda p: str(p.relative_to(REPO_ROOT)))
    def test_fixture_flow_round_trips(self, flow_path, tmp_path):
        try:
            opened = open_flow(flow_path)
        except Exception as exc:  # noqa: BLE001 - some fixtures need services absent in tests
            pytest.skip(f"fixture does not open in this environment: {exc}")
        if flow_path.suffix == ".flowfile":
            # Legacy pickles unpickle old settings objects; one rebuild normalizes them to the current models.
            legacy = opened
            opened = make_graph(8198)
            opened.restore_from_snapshot(_snapshot(legacy))
        expected = in_scope(opened)

        rebuilt = make_graph(8199)
        rebuilt.restore_from_snapshot(_snapshot(opened))
        assert in_scope(rebuilt) == expected

        resaved = tmp_path / "resaved.yaml"
        opened.save_flow(str(resaved))
        assert in_scope(open_flow(resaved)) == expected


class TestOpenFlowHistory:
    def test_open_flow_starts_with_empty_history(self, tmp_path):
        path = tmp_path / "opened.yaml"
        graph = build_rich_graph(8110, path=str(path))
        graph.save_flow(str(path))

        reopened = open_flow(path)

        state = reopened.get_history_state()
        assert state.undo_count == 0
        assert state.redo_count == 0
        assert reopened.has_unsaved_changes() is False


class TestRestoreKeepsFlowSettings:
    def test_undo_keeps_live_flow_settings(self):
        graph = make_graph(8120)
        manual_input(graph, 1, [{"a": 1}])
        graph.flow_settings.show_edge_labels = True
        graph.flow_settings.source_registration_id = 4242
        promise(graph, "filter", 2)

        assert graph.undo().success is True

        assert graph.flow_settings.show_edge_labels is True
        assert graph.flow_settings.source_registration_id == 4242
        assert graph.flow_settings.track_history is True
        assert graph.get_node(2) is None

    def test_redo_keeps_live_flow_settings(self):
        graph = make_graph(8121)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 2)
        assert graph.undo().success is True
        graph.flow_settings.show_edge_labels = True

        assert graph.redo().success is True

        assert graph.flow_settings.show_edge_labels is True
        assert graph.get_node(2) is not None


class TestLosslessFields:
    def test_auto_description_stays_auto_after_undo(self):
        graph = make_graph(8130)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 2)
        graph.add_filter(
            input_schema.NodeFilter(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                filter_input=transform_schema.FilterInput(
                    mode="basic",
                    basic_filter=transform_schema.BasicFilter(field="a", operator="equals", value="1"),
                ),
            )
        )
        _, auto_before = graph.get_node(2).resolve_description()
        assert auto_before is True
        promise(graph, "sort", 3)

        assert graph.undo().success is True

        description, is_auto = graph.get_node(2).resolve_description()
        assert is_auto is True, f"auto description frozen into user text: {description!r}"
        assert graph.get_node(2).setting_input.description == ""

    def test_typed_description_equal_to_the_auto_text_survives_undo(self):
        graph = make_graph(8132)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 2)
        graph.add_filter(
            input_schema.NodeFilter(flow_id=graph.flow_id, node_id=2, depending_on_id=1, filter_input=basic_filter())
        )
        auto_text, _ = graph.get_node(2).resolve_description()
        graph.get_node(2).setting_input.description = auto_text
        promise(graph, "sort", 3)

        assert graph.undo().success is True

        assert graph.get_node(2).resolve_description() == (auto_text, False)

    def test_description_provenance_stays_out_of_saved_files(self, tmp_path):
        graph = build_rich_graph(8133, path=str(tmp_path / "rich.yaml"))
        in_memory = graph.get_flowfile_data().model_dump()["nodes"]
        assert {node["description_is_auto_generated"] for node in in_memory} == {True, False}
        as_json = graph.get_flowfile_data().model_dump(mode="json")["nodes"]
        assert all("description_is_auto_generated" not in node for node in as_json)

        for suffix in (".yaml", ".json"):
            path = tmp_path / f"saved{suffix}"
            graph.save_flow(str(path))
            assert "description_is_auto_generated" not in path.read_text(encoding="utf-8")

    def test_select_data_type_change_survives_undo(self):
        graph = make_graph(8131)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "select", 2)
        graph.add_select(
            input_schema.NodeSelect(
                flow_id=graph.flow_id,
                node_id=2,
                depending_on_id=1,
                select_input=[transform_schema.SelectInput(old_name="a", data_type="Int64", data_type_change=False)],
            )
        )
        promise(graph, "sort", 3)

        assert graph.undo().success is True

        column = graph.get_node(2).setting_input.select_input[0]
        assert column.data_type == "Int64"
        assert column.data_type_change is False


class TestDirtyFlag:
    def test_undo_after_save_is_dirty_and_redo_back_is_clean(self, tmp_path):
        path = tmp_path / "dirty.yaml"
        graph = make_graph(8140, path=str(path))
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 2)
        graph.save_flow(str(path))
        assert graph.has_unsaved_changes() is False

        assert graph.undo().success is True
        assert graph.has_unsaved_changes() is True

        assert graph.redo().success is True
        assert graph.has_unsaved_changes() is False

    def test_edit_then_undo_back_to_saved_is_clean(self, tmp_path):
        path = tmp_path / "dirty2.yaml"
        graph = make_graph(8141, path=str(path))
        manual_input(graph, 1, [{"a": 1}])
        graph.save_flow(str(path))

        promise(graph, "filter", 2)
        assert graph.has_unsaved_changes() is True

        assert graph.undo().success is True
        assert graph.has_unsaved_changes() is False

    def test_legacy_capture_leaves_dirty_to_the_formula(self, tmp_path):
        path = tmp_path / "legacy_capture.yaml"
        graph = make_graph(8142, path=str(path), track_history=False)
        graph._history_manager.config = history_schema.HistoryConfig(enabled=True)
        manual_input(graph, 1, [{"a": 1}])
        graph.save_flow(str(path))

        graph.capture_history_snapshot(history_schema.HistoryActionType.ADD_NODE, "nothing follows")
        assert graph.has_unsaved_changes() is False

        graph.capture_history_snapshot(history_schema.HistoryActionType.ADD_NODE, "add filter")
        promise(graph, "filter", 2)
        assert graph.has_unsaved_changes() is True


class TestFailedRestoreKeepsStacks:
    def test_undo_failure_leaves_stacks_and_graph_untouched(self, monkeypatch):
        graph = make_graph(8150)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 2)
        before = in_scope(graph)
        state_before = graph.get_history_state()

        original = graph.restore_from_snapshot
        calls = {"n": 0}

        def failing_restore(snapshot):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            return original(snapshot)

        monkeypatch.setattr(graph, "restore_from_snapshot", failing_restore)
        result = graph.undo()

        assert result.success is False
        state_after = graph.get_history_state()
        assert (state_after.undo_count, state_after.redo_count) == (state_before.undo_count, state_before.redo_count)
        assert in_scope(graph) == before


class TestTransactionContract:
    def test_transaction_never_waits_for_the_lock_on_the_event_loop(self):
        graph = make_graph(8160)
        holding, release = threading.Event(), threading.Event()

        def hold_lock():
            with graph.edit_lock():
                holding.set()
                release.wait(5)

        async def edit_on_loop():
            with graph.transaction("edit"):
                pass

        holder = threading.Thread(target=hold_lock)
        holder.start()
        try:
            assert holding.wait(5)
            with pytest.raises(RuntimeError, match="event-loop"):
                asyncio.run(edit_on_loop())
        finally:
            release.set()
            holder.join()
        asyncio.run(edit_on_loop())  # uncontended: never blocks, so it is allowed

    def test_nested_transactions_record_one_step(self):
        graph = make_graph(8161)
        manual_input(graph, 1, [{"a": 1}])
        before = graph.get_history_state().undo_count

        with graph.transaction("Outer") as txn:
            promise(graph, "filter", 2)
            promise(graph, "sort", 3)

        state = graph.get_history_state()
        assert state.undo_count == before + 1
        assert state.undo_description == "Outer"
        assert txn.entry is not None

    def test_failed_transaction_rolls_back_and_keeps_redo(self):
        graph = make_graph(8162)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 2)
        assert graph.undo().success is True
        before, state_before = in_scope(graph), graph.get_history_state()

        with pytest.raises(ValueError, match="boom"):
            with graph.transaction("Failing"):
                promise(graph, "sort", 3)
                raise ValueError("boom")

        assert in_scope(graph) == before
        state = graph.get_history_state()
        assert (state.undo_count, state.redo_count) == (state_before.undo_count, state_before.redo_count)

    def test_failed_history_record_rolls_back_and_keeps_both_stacks(self, monkeypatch):
        graph = make_graph(8165)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 2)
        assert graph.undo().success is True
        before, state_before = in_scope(graph), graph.get_history_state()
        dirty_before = graph.has_unsaved_changes()
        assert state_before.undo_count > 0 and state_before.redo_count > 0

        original_record, failed = HistoryManager.record, []

        def record_fails_once(self, *args, **kwargs):
            if not failed:
                failed.append(True)
                raise RuntimeError("record failed")
            return original_record(self, *args, **kwargs)

        monkeypatch.setattr(HistoryManager, "record", record_fails_once)
        with pytest.raises(RuntimeError, match="record failed"):
            with graph.transaction("Add sort") as txn:
                promise(graph, "sort", 3)

        assert in_scope(graph) == before
        assert graph.get_history_state() == state_before
        assert graph.has_unsaved_changes() is dirty_before
        assert (txn.entry, graph._transaction_depth) == (None, 0)

        with graph.transaction("Add sort") as txn:
            promise(graph, "sort", 3)
        state = graph.get_history_state()
        assert (state.undo_count, state.redo_count) == (state_before.undo_count + 1, 0)
        assert state.undo_description == "Add sort" and txn.entry is not None

    def test_failure_after_recording_discards_the_step_and_rolls_back(self, monkeypatch):
        graph = make_graph(8166)
        manual_input(graph, 1, [{"a": 1}])
        before, state_before = in_scope(graph), graph.get_history_state()
        original_state, failed = graph.get_history_state, []

        def state_fails_once_recorded():
            state = original_state()
            if state.undo_count > state_before.undo_count and not failed:
                failed.append(True)
                raise RuntimeError("state failed")
            return state

        monkeypatch.setattr(graph, "get_history_state", state_fails_once_recorded)
        with pytest.raises(RuntimeError, match="state failed"):
            with graph.transaction("Add sort") as txn:
                promise(graph, "sort", 3)

        assert failed and in_scope(graph) == before
        assert original_state() == state_before
        assert (txn.entry, graph._transaction_depth) == (None, 0)

    def test_revert_if_top_retracts_only_the_latest_step(self):
        graph = make_graph(8163)
        manual_input(graph, 1, [{"a": 1}])
        before, undo_before = in_scope(graph), graph.get_history_state().undo_count
        with graph.transaction("AI step") as txn:
            promise(graph, "sort", 3)

        assert graph.revert_if_top(txn.entry) is True

        assert in_scope(graph) == before
        state = graph.get_history_state()
        assert (state.undo_count, state.redo_count) == (undo_before, 0)
        assert graph.revert_if_top(txn.entry) is False

    def test_revert_if_top_refuses_a_step_that_is_no_longer_latest(self):
        graph = make_graph(8164)
        manual_input(graph, 1, [{"a": 1}])
        with graph.transaction("AI step") as txn:
            promise(graph, "sort", 3)
        promise(graph, "filter", 4)

        assert graph.revert_if_top(txn.entry) is False
        assert graph.get_node(3) is not None


def _join(graph, node_id: int, left_id: int, right_id: int):
    graph.add_join(
        input_schema.NodeJoin(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_ids=[left_id, right_id],
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("a", "a")], left_select=[], right_select=[]
            ),
        )
    )


class TestEdgeBookkeeping:
    def test_rich_graph_keeps_one_edge_record_per_slot(self):
        graph = build_rich_graph(8170)
        assert edge_bookkeeping_errors(graph) == []

        graph.restore_from_snapshot(_snapshot(graph))
        assert edge_bookkeeping_errors(graph) == []

    def test_connecting_into_an_occupied_input_detaches_the_old_source(self):
        graph = make_graph(8171)
        manual_input(graph, 1, [{"a": 1}])
        manual_input(graph, 2, [{"a": 2}])
        promise(graph, "filter", 3)
        connect(graph, 1, 3)

        connect(graph, 2, 3)

        assert edge_bookkeeping_errors(graph) == []
        assert graph.get_node(1).get_node_information().outputs == []
        rebuilt = make_graph(8172)
        rebuilt.restore_from_snapshot(_snapshot(graph))
        assert in_scope(rebuilt) == in_scope(graph)

    @pytest.mark.parametrize("input_type", ["main", "right"])
    def test_reconnecting_a_single_source_input_is_a_no_op(self, input_type):
        graph = make_graph(8173)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "join" if input_type == "right" else "filter", 2)
        connect(graph, 1, 2, input_type=input_type)
        before, undo_before = in_scope(graph), graph.get_history_state().undo_count

        connect(graph, 1, 2, input_type=input_type)

        assert in_scope(graph) == before
        assert graph.get_history_state().undo_count == undo_before
        assert edge_bookkeeping_errors(graph) == []

    def test_a_multi_input_node_keeps_a_repeated_source_as_positional_inputs(self):
        graph = make_graph(8177)
        manual_input(graph, 1, [{"a": 1}])
        manual_input(graph, 2, [{"a": 2}])
        promise(graph, "polars_code", 3)
        for source in (1, 2, 1):
            connect(graph, source, 3)
        assert [n.node_id for n in graph.get_node(3).main_input] == [1, 2, 1]
        assert edge_bookkeeping_errors(graph) == []

        graph.restore_from_snapshot(_snapshot(graph))
        assert [n.node_id for n in graph.get_node(3).main_input] == [1, 2, 1]

        delete_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 3))
        assert [n.node_id for n in graph.get_node(3).main_input] == [2, 1]
        assert graph.get_node(1).get_node_information().outputs == [3]
        assert edge_bookkeeping_errors(graph) == []

    def test_deleting_one_side_of_a_self_join_keeps_the_other(self):
        graph = make_graph(8174)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "join", 2)
        connect(graph, 1, 2)
        connect(graph, 1, 2, input_type="right")
        _join(graph, 2, 1, 1)

        connection = input_schema.NodeConnection.create_from_simple_input(1, 2, input_type="right")
        delete_connection(graph, connection)

        join = graph.get_node(2)
        assert [n.node_id for n in join.all_inputs] == [1]
        assert join._input_output_handles == {1: "output-0"}
        assert edge_bookkeeping_errors(graph) == []

    def test_deleting_the_join_clears_both_self_join_leads(self):
        graph = make_graph(8175)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "join", 2)
        connect(graph, 1, 2)
        connect(graph, 1, 2, input_type="right")

        graph.delete_node(2)

        assert graph.get_node(1).leads_to_nodes == []
        assert edge_bookkeeping_errors(graph) == []

    def test_snapshot_edges_come_from_the_targets_inputs(self):
        graph = make_graph(8176)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 2)
        promise(graph, "sort", 3)
        connect(graph, 1, 2)
        expected = in_scope(graph)

        # A stale lead (what the old replace path left behind) must not surface as an edge.
        graph.get_node(1).leads_to_nodes.append(graph.get_node(3))

        assert in_scope(graph) == expected


class TestInsertOnEdge:
    @staticmethod
    def _splice(graph, node_id: int, from_id: int, to_id: int, input_type: str = "main", handle: str = "output-0"):
        promise(graph, "sample", node_id)
        connection = input_schema.NodeConnection.create_from_simple_input(
            from_id, to_id, input_type=input_type, output_handle=handle
        )
        insert_node_on_edge(graph, node_id, connection)

    def test_positional_inputs_keep_their_order(self):
        graph = build_rich_graph(8200)

        self._splice(graph, 50, 8, 10)
        self._splice(graph, 51, 4, 9)

        assert [n.node_id for n in graph.get_node(10).main_input] == [50, 1]
        assert [n.node_id for n in graph.get_node(9).main_input] == [5, 51, 2]
        assert [n.node_id for n in graph.get_node(50).main_input] == [8]
        assert edge_bookkeeping_errors(graph) == []
        expected = in_scope(graph)
        graph.restore_from_snapshot(_snapshot(graph))
        assert in_scope(graph) == expected

    def test_join_sides_and_the_source_handle_are_kept(self):
        graph = build_rich_graph(8201)

        self._splice(graph, 52, 2, 11, input_type="right")
        self._splice(graph, 53, 2, 12, input_type="right")
        self._splice(graph, 54, 3, 5, handle="output-1")

        join, self_join = graph.get_node(11), graph.get_node(12)
        assert ([n.node_id for n in join.main_input], join.right_input.node_id) == ([1], 52)
        assert ([n.node_id for n in self_join.main_input], self_join.right_input.node_id) == ([2], 53)
        assert {(3, 54, "output-1"), (54, 5, "output-0")} <= _node_edges(graph)
        assert (3, 5, "output-1") not in _node_edges(graph)
        assert edge_bookkeeping_errors(graph) == []

    def test_a_keyed_input_keeps_its_handle(self):
        graph = build_rich_graph(8202)
        promise(graph, "sample", 55)
        connection = input_schema.NodeConnection.create_from_simple_input(4, 18)
        connection.input_connection.connection_class = "input-1"

        insert_node_on_edge(graph, 55, connection)

        keyed = graph.get_node(18).node_inputs.keyed_inputs
        assert (keyed["input-1"].node_id, keyed["input-2"].node_id) == (55, 5)
        assert edge_bookkeeping_errors(graph) == []

    @pytest.mark.parametrize(
        "from_id, to_id, input_type, handle",
        [(1, 10, "main", "output-1"), (2, 10, "main", "output-0"), (1, 11, "right", "output-0")],
        ids=["wrong-handle", "no-edge", "wrong-slot"],
    )
    def test_a_missing_edge_is_refused_without_changes(self, from_id, to_id, input_type, handle):
        graph = build_rich_graph(8203)
        promise(graph, "sample", 56)
        before = in_scope(graph)

        with pytest.raises(HTTPException) as refused:
            self._splice(graph, 56, from_id, to_id, input_type=input_type, handle=handle)

        assert refused.value.status_code == 422
        assert in_scope(graph) == before


class TestStartNodes:
    def test_a_reused_id_does_not_inherit_the_start_flag(self):
        graph = make_graph(8180)
        manual_input(graph, 7, [{"a": 1}])
        graph.delete_node(7)
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 7)
        connect(graph, 1, 7)

        starts = {node.id: node.is_start_node for node in graph.get_flowfile_data().nodes}
        assert starts == {1: True, 7: False}

        promise(graph, "sort", 9)
        assert graph.undo().success is True
        assert [node.node_id for node in graph._flow_starts] == [1]

    def test_open_ignores_a_stale_start_flag_in_the_file(self, tmp_path):
        path = tmp_path / "stale_start.yaml"
        graph = make_graph(8181, path=str(path))
        manual_input(graph, 1, [{"a": 1}])
        promise(graph, "filter", 2)
        connect(graph, 1, 2)
        graph.save_flow(str(path))
        path.write_text(path.read_text().replace("is_start_node: false", "is_start_node: true"))

        reopened = open_flow(path)

        assert [node.node_id for node in reopened._flow_starts] == [1]


class TestReadsAndRunsDoNotWrite:
    def test_running_a_basic_filter_leaves_its_settings_and_dirty_flag_alone(self, tmp_path):
        path = tmp_path / "run_filter.yaml"
        graph = make_graph(8190, path=str(path))
        manual_input(graph, 1, [{"a": 1}, {"a": 2}])
        promise(graph, "filter", 2)
        connect(graph, 1, 2)
        graph.add_filter(
            input_schema.NodeFilter(flow_id=graph.flow_id, node_id=2, depending_on_id=1, filter_input=basic_filter())
        )
        graph.save_flow(str(path))
        before = in_scope(graph)

        assert graph.run_graph().success is True

        assert in_scope(graph) == before
        assert graph.get_node(2).setting_input.filter_input.advanced_filter == ""
        assert graph.has_unsaved_changes() is False

    def test_reading_node_data_leaves_join_settings_alone(self, tmp_path):
        path = tmp_path / "read_join.yaml"
        graph = make_graph(8191, path=str(path))
        manual_input(graph, 1, [{"a": 1, "x": 2}])
        manual_input(graph, 2, [{"a": 1, "y": 3}])
        promise(graph, "join", 3)
        connect(graph, 1, 3)
        connect(graph, 2, 3, input_type="right")
        _join(graph, 3, 1, 2)
        graph.save_flow(str(path))
        before = in_scope(graph)

        shown = graph.get_node(3).get_node_data(graph.flow_id, include_output=False)

        assert {r.old_name for r in shown.setting_input.join_input.left_select.renames} == {"a", "x"}
        assert in_scope(graph) == before
        assert graph.get_node(3).setting_input.join_input.left_select.renames == []
        assert graph.has_unsaved_changes() is False

    def test_a_basic_filter_shows_its_run_expression_without_writing_it(self):
        graph = make_graph(8192)
        manual_input(graph, 1, [{"a": 1, "b": "1"}])
        promise(graph, "filter", 2)
        connect(graph, 1, 2)
        stale = transform_schema.FilterInput(
            mode="basic",
            basic_filter=transform_schema.BasicFilter(field="b", operator="equals", value="1"),
            advanced_filter="[a] > 100",
        )
        graph.add_filter(
            input_schema.NodeFilter(flow_id=graph.flow_id, node_id=2, depending_on_id=1, filter_input=stale)
        )
        before = in_scope(graph)

        shown = graph.get_node(2).get_node_data(graph.flow_id, include_output=False)

        assert shown.setting_input.filter_input.advanced_filter == '[b]="1"'
        assert graph.get_node(2).setting_input.filter_input.advanced_filter == "[a] > 100"
        assert in_scope(graph) == before

    def test_running_select_and_cross_join_leaves_their_settings_alone(self, tmp_path):
        path = tmp_path / "run_select.yaml"
        graph = make_graph(8193, path=str(path))
        manual_input(graph, 1, [{"a": 1, "b": "x"}])
        manual_input(graph, 2, [{"c": 2}])
        promise(graph, "select", 3)
        connect(graph, 1, 3)
        graph.add_select(
            input_schema.NodeSelect(
                flow_id=graph.flow_id,
                node_id=3,
                depending_on_id=1,
                select_input=[transform_schema.SelectInput("a"), transform_schema.SelectInput("gone", keep=False)],
            )
        )
        promise(graph, "cross_join", 4)
        connect(graph, 1, 4)
        connect(graph, 2, 4, input_type="right")
        graph.add_cross_join(
            input_schema.NodeCrossJoin(
                flow_id=graph.flow_id,
                node_id=4,
                depending_on_ids=[1, 2],
                cross_join_input=transform_schema.CrossJoinInput(
                    left_select=[transform_schema.SelectInput("a")], right_select=[transform_schema.SelectInput("c")]
                ),
            )
        )
        graph.save_flow(str(path))
        before = in_scope(graph)
        live_cross_join = graph.get_node(4).setting_input.cross_join_input.model_dump()

        assert graph.run_graph().success is True

        assert in_scope(graph) == before
        assert graph.has_unsaved_changes() is False
        assert graph.get_node(4).setting_input.cross_join_input.model_dump() == live_cross_join
        shown = graph.get_node(3).get_node_data(graph.flow_id, include_output=False).setting_input.select_input
        assert {s.old_name: s.is_available for s in shown} == {"a": True, "gone": False}


class TestSnapshotCost:
    def test_a_transaction_serializes_and_hashes_the_graph_once_on_each_side(self, monkeypatch):
        graph = make_graph(8195)
        manual_input(graph, 1, [{"a": i} for i in range(50)])
        undo_before = graph.get_history_state().undo_count
        serialized, hashed = [], []
        original_data, original_hash = graph.get_flowfile_data, history_schema.in_scope_hash
        monkeypatch.setattr(graph, "get_flowfile_data", lambda: serialized.append(1) or original_data())
        monkeypatch.setattr(history_schema, "in_scope_hash", lambda d: hashed.append(1) or original_hash(d))

        with graph.transaction("Move"):
            graph.set_node_positions([schemas.NodePositionUpdate(node_id=1, pos_x=5, pos_y=5)])

        assert graph.get_history_state().undo_count == undo_before + 1
        assert (len(serialized), len(hashed)) == (2, 2)

    def test_graph_snapshot_hash_sees_secret_values_and_payload_is_detached(self):
        base = {"nodes": [{"id": 1, "setting_input": {"password": history_schema.SecretStr("a")}}]}
        changed = {"nodes": [{"id": 1, "setting_input": {"password": history_schema.SecretStr("b")}}]}
        snapshot = history_schema.HistorySnapshot(base)

        assert snapshot.graph_hash != history_schema.HistorySnapshot(changed, keep_payload=False).graph_hash
        base["nodes"].append({"id": 2})
        assert len(snapshot.load()["nodes"]) == 1

    def test_non_string_dict_keys_hash_deterministically(self):
        mixed = {"nodes": [{"id": 1, "setting_input": {"lookup": {1: "a", "b": 2, (3, 4): "c"}}}]}
        assert history_schema.in_scope_hash(mixed) == history_schema.in_scope_hash(mixed)
