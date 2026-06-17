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
    assert group.width > 0 and group.height > 0


def test_serialization_keeps_group_id_at_top_level_not_in_setting_input():
    graph = build_two_node_graph()
    group = graph.create_group("Cleaning", [1, 2])

    data = graph.get_flowfile_data()
    assert len(data.groups) == 1
    assert data.groups[0].id == group.id

    dumped = data.model_dump()
    for node in dumped["nodes"]:
        assert node["group_id"] == group.id
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


# Nested (embedded) groups


def test_nested_create_sets_parent_group_id():
    graph = build_two_node_graph()
    add_manual_input(graph, [{"a": 5}], node_id=3)
    outer = graph.create_group("Outer", [1, 2, 3])
    inner = graph.create_group("Inner", [1, 2], parent_group_id=outer.id)

    assert graph._groups[inner.id].parent_group_id == outer.id
    assert graph._child_group_ids(outer.id) == [inner.id]
    # Nodes 1,2 moved to the sub-group; node 3 stays in the outer group (no stealing).
    assert graph.get_node(1).setting_input.group_id == inner.id
    assert graph.get_node(2).setting_input.group_id == inner.id
    assert graph.get_node(3).setting_input.group_id == outer.id


def test_create_group_wraps_existing_groups():
    graph = build_two_node_graph()
    a = graph.create_group("A", [1])
    b = graph.create_group("B", [2])
    wrapper = graph.create_group("Wrapper", [], child_group_ids=[a.id, b.id])

    assert graph._groups[a.id].parent_group_id == wrapper.id
    assert graph._groups[b.id].parent_group_id == wrapper.id
    assert sorted(graph._child_group_ids(wrapper.id)) == sorted([a.id, b.id])


def test_delete_group_lifts_members_one_level():
    graph = build_two_node_graph()
    outer = graph.create_group("Outer", [])
    inner = graph.create_group("Inner", [1, 2], parent_group_id=outer.id)
    graph.delete_group(inner.id)

    assert inner.id not in graph._groups
    assert graph.get_node(1).setting_input.group_id == outer.id
    assert graph.get_node(2).setting_input.group_id == outer.id


def test_delete_top_level_group_ungroups_to_none():
    graph = build_two_node_graph()
    g = graph.create_group("G", [1, 2])
    graph.delete_group(g.id)
    assert g.id not in graph._groups
    assert graph.get_node(1).setting_input.group_id is None


def test_group_with_child_groups_not_pruned_when_last_node_removed():
    graph = build_two_node_graph()
    add_manual_input(graph, [{"a": 5}], node_id=3)
    outer = graph.create_group("Outer", [3])
    inner = graph.create_group("Inner", [1, 2], parent_group_id=outer.id)
    graph.remove_nodes_from_group([3])  # outer loses its only node but still holds a sub-group

    assert outer.id in graph._groups
    assert graph._child_group_ids(outer.id) == [inner.id]


def test_bounds_wrap_child_groups():
    graph = build_two_node_graph()
    add_manual_input(graph, [{"a": 5}], node_id=3)
    graph.set_node_positions(
        [
            schemas.NodePositionUpdate(node_id=1, pos_x=0.0, pos_y=0.0),
            schemas.NodePositionUpdate(node_id=2, pos_x=120.0, pos_y=120.0),
            schemas.NodePositionUpdate(node_id=3, pos_x=600.0, pos_y=400.0),
        ]
    )
    inner = graph.create_group("Inner", [1, 2])
    outer = graph.create_group("Outer", [3], child_group_ids=[inner.id])

    o, i = graph._groups[outer.id], graph._groups[inner.id]
    assert o.x_position <= i.x_position
    assert o.y_position <= i.y_position
    assert o.x_position + o.width >= i.x_position + i.width
    assert o.y_position + o.height >= i.y_position + i.height


def test_nested_groups_round_trip(temp_dir):
    graph = build_two_node_graph()
    outer = graph.create_group("Outer", [])
    graph.create_group("Inner", [1, 2], parent_group_id=outer.id)

    path = temp_dir / "flow.yaml"
    graph.save_flow(str(path))
    reloaded = open_flow(path)

    by_name = {g.name: g for g in reloaded._groups.values()}
    assert by_name["Inner"].parent_group_id == by_name["Outer"].id


# Group id allocation


def test_next_group_id_does_not_reuse_freed_id():
    graph = build_two_node_graph()
    add_manual_input(graph, [{"a": 5}], node_id=3)
    g1 = graph.create_group("A", [1])
    g2 = graph.create_group("B", [2])
    g3 = graph.create_group("C", [3])
    assert [g1.id, g2.id, g3.id] == [1, 2, 3]

    graph.delete_group(g3.id)  # free the highest id
    g4 = graph.create_group("D", [3])
    assert g4.id == 4  # monotonic; the freed id 3 is not reused
    assert g4.id not in {g1.id, g2.id}


def test_restore_groups_resyncs_id_counter(temp_dir):
    graph = build_two_node_graph()
    add_manual_input(graph, [{"a": 5}], node_id=3)
    graph.create_group("A", [1])
    graph.create_group("B", [2])
    graph.create_group("C", [3])  # ids 1, 2, 3

    path = temp_dir / "flow.yaml"
    graph.save_flow(str(path))
    reloaded = open_flow(path)

    assert reloaded._group_id_seq == 3  # counter resumes above the highest restored id
    regrouped = reloaded.create_group("D", [1])
    assert regrouped.id == 4
