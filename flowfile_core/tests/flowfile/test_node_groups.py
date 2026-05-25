"""
Tests for visual node groups (organizational containers; no execution impact).

Run with:
    pytest flowfile_core/tests/flowfile/test_node_groups.py -v

Covers: membership on nodes, serialization round-trip (incl. that group_id is not
duplicated inside setting_input), backward compatibility, apply_layout bounds refit,
empty-group pruning/auto-delete, and undo/redo of group operations.
"""
import tempfile
from pathlib import Path

import pytest

from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema, schemas


def create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="test_flow", path=".", execution_mode="Development")
    )
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data, node_id: int = 1) -> FlowGraph:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(data)
        )
    )
    return graph


def build_two_node_graph(flow_id: int = 1) -> FlowGraph:
    graph = create_graph(flow_id)
    add_manual_input(graph, [{"a": 1, "b": 2}], node_id=1)
    add_manual_input(graph, [{"a": 3, "b": 4}], node_id=2)
    return graph


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


def test_create_group_assigns_membership():
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2], color="blue")

    assert group.id in graph._groups
    assert graph.get_node(1).setting_input.group_id == group.id
    assert graph.get_node(2).setting_input.group_id == group.id
    assert sorted(graph._member_node_ids(group.id)) == [1, 2]
    # Bounds were computed from members.
    assert group.width > 0 and group.height > 0


def test_serialization_keeps_group_id_at_top_level_not_in_setting_input():
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2])

    data = graph.get_flowfile_data()
    assert len(data.groups) == 1
    assert data.groups[0].id == group.id

    dumped = data.model_dump()
    for node in dumped["nodes"]:
        assert node["group_id"] == group.id  # top-level
        # group_id must NOT leak into the nested settings payload
        assert "group_id" not in (node["setting_input"] or {})


def test_yaml_round_trip_preserves_groups(temp_dir):
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2], color="green")
    group_bounds = (group.x_position, group.y_position, group.width, group.height)

    path = temp_dir / "flow.yaml"
    graph.save_flow(str(path))
    reloaded = open_flow(path)

    assert len(reloaded._groups) == 1
    restored = next(iter(reloaded._groups.values()))
    assert restored.name == "Cleaning"
    assert restored.color == "green"
    assert (restored.x_position, restored.y_position, restored.width, restored.height) == pytest.approx(group_bounds)
    assert reloaded.get_node(1).setting_input.group_id == restored.id
    assert reloaded.get_node(2).setting_input.group_id == restored.id


def test_backward_compat_flow_without_groups(temp_dir):
    graph = build_two_node_graph()  # no groups created
    path = temp_dir / "flow.yaml"
    graph.save_flow(str(path))
    reloaded = open_flow(path)

    assert reloaded._groups == {}
    assert reloaded.get_node(1).setting_input.group_id is None
    assert reloaded.get_node(2).setting_input.group_id is None


def test_apply_layout_refits_group_bounds():
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2])
    graph.apply_layout()

    members = [graph.get_node(1), graph.get_node(2)]
    refit = graph._groups[group.id]
    # Box encloses every member's top-left after the reflow.
    for node in members:
        assert refit.x_position <= node.setting_input.pos_x
        assert refit.y_position <= node.setting_input.pos_y


def test_delete_last_member_prunes_group():
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2])
    graph.delete_node(1)
    assert group.id in graph._groups  # node 2 still a member
    graph.delete_node(2)
    assert group.id not in graph._groups


def test_remove_nodes_from_group_prunes_when_empty():
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2])
    graph.remove_nodes_from_group([1, 2])
    assert group.id not in graph._groups
    assert graph.get_node(1).setting_input.group_id is None


def test_empty_group_not_serialized():
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1])
    graph._set_node_group(1, None)  # orphan the group without going through prune paths
    data = graph.get_flowfile_data()
    assert all(g.id != group.id for g in data.groups)


def test_assign_node_to_named_group_find_or_create():
    graph = build_two_node_graph()
    first = graph.assign_node_to_named_group(1, "Shared")
    second = graph.assign_node_to_named_group(2, "Shared")
    assert first.id == second.id
    assert sorted(graph._member_node_ids(first.id)) == [1, 2]


def test_undo_redo_create_group():
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2])
    assert group.id in graph._groups

    graph.undo()
    assert graph._groups == {}
    assert graph.get_node(1).setting_input.group_id is None

    graph.redo()
    assert len(graph._groups) == 1
    assert graph.get_node(1).setting_input.group_id is not None


def test_set_node_positions_persists():
    graph = build_two_node_graph()
    graph.set_node_positions(
        [schemas.NodePositionUpdate(node_id=1, pos_x=321.0, pos_y=654.0)]
    )
    assert graph.get_node(1).setting_input.pos_x == 321.0
    assert graph.get_node(1).setting_input.pos_y == 654.0


def test_collapsed_flag_round_trips(temp_dir):
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2])
    assert graph._groups[group.id].collapsed is False  # default
    graph.update_group(group.id, collapsed=True)
    assert graph._groups[group.id].collapsed is True

    path = temp_dir / "flow.yaml"
    graph.save_flow(str(path))
    reloaded = open_flow(path)
    assert next(iter(reloaded._groups.values())).collapsed is True


def test_collapse_toggle_is_undoable():
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2])
    graph.update_group(group.id, collapsed=True)
    graph.undo()
    assert graph._groups[group.id].collapsed is False
