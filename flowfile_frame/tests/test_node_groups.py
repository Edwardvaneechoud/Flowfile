"""
Tests for visual node grouping in the flowfile_frame API.

Run with:
    pytest flowfile_frame/tests/test_node_groups.py -v

Visual groups are organizational only and unrelated to ``group_by`` (aggregation).
"""

import pytest

import flowfile_frame as ff


def _df() -> ff.FlowFrame:
    return ff.from_dict({"a": [1, 2, 3], "b": [4, 5, 6]})


def test_context_manager_groups_block_nodes():
    df = _df()
    source_node = df.node_id
    with df.group("Cleaning", color="blue"):
        df = df.filter(ff.col("a") > 1)
        df = df.select(["a", "b"])
    last_node = df.node_id

    graph = df.flow_graph
    assert len(graph._groups) == 1
    group = next(iter(graph._groups.values()))
    assert group.name == "Cleaning"
    assert group.color == "blue"

    members = set(graph._member_node_ids(group.id))
    assert last_node in members
    assert source_node not in members  # created before the block


def test_nodes_outside_block_not_grouped():
    df = _df()
    with df.group("Inside"):
        df = df.filter(ff.col("a") > 1)
    grouped_node = df.node_id
    df = df.select(["a"])  # outside the block
    outside_node = df.node_id

    group = next(iter(df.flow_graph._groups.values()))
    members = set(df.flow_graph._member_node_ids(group.id))
    assert grouped_node in members
    assert outside_node not in members


def test_set_group_find_or_create_reuses_group_by_name():
    df = _df()
    df = df.filter(ff.col("a") > 1).set_group("Shared")
    first_node = df.node_id
    df = df.select(["a"]).set_group("Shared")
    second_node = df.node_id

    graph = df.flow_graph
    assert len(graph._groups) == 1  # same name -> same group
    group = next(iter(graph._groups.values()))
    assert set(graph._member_node_ids(group.id)) == {first_node, second_node}


def test_group_and_group_by_coexist():
    df = _df()
    with df.group("Prep"):
        df = df.filter(ff.col("a") > 0)
    aggregated = df.group_by("a").agg(ff.col("b").sum())  # aggregation; not a visual group

    data = aggregated.flow_graph.get_flowfile_data()
    assert len(data.groups) == 1  # exactly the visual group


def test_round_trip_through_save_open(tmp_path):
    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    df = _df()
    with df.group("Cleaning"):
        df = df.filter(ff.col("a") > 1)
    grouped_node = df.node_id

    path = tmp_path / "flow.yaml"
    df.save_graph(str(path))
    reloaded = open_flow(path)

    assert len(reloaded._groups) == 1
    assert reloaded.get_node(grouped_node).setting_input.group_id is not None


# ff.FlowGroup: a first-class group object, nested with parent_group, joined with add_to_group


def _groups(graph) -> dict[str, tuple]:
    """Group name -> (color, parent name, sorted member node ids)."""
    by_id = {g.id: g for g in graph._groups.values()}
    return {
        g.name: (
            g.color,
            by_id[g.parent_group_id].name if g.parent_group_id else None,
            sorted(graph._member_node_ids(g.id)),
        )
        for g in by_id.values()
    }


def test_flow_group_is_placed_on_first_add_and_nests_parents_first():
    outer = ff.FlowGroup("Outer", color="blue")
    inner = ff.FlowGroup("Inner", parent_group=outer)
    assert outer.id is None and outer.flow_graph is None and inner.node_ids == []

    df = _df()
    filtered = df.filter(ff.col("a") > 1).add_to_group(inner)
    assert isinstance(filtered, ff.FlowFrame)
    graph = filtered.flow_graph
    assert outer.flow_graph is graph and inner.flow_graph is graph
    assert graph._groups[inner.id].parent_group_id == outer.id
    assert _groups(graph) == {"Outer": ("blue", None, []), "Inner": (None, "Outer", [filtered.node_id])}
    assert inner.node_ids == [filtered.node_id]

    selected = filtered.select(["a"]).add_to_group(outer)
    assert _groups(graph)["Outer"] == ("blue", None, [selected.node_id])
    assert repr(inner) == f"FlowGroup('Inner', id={inner.id}, parent_group=FlowGroup('Outer', id={outer.id}))"


def test_native_nodes_join_a_group_too():
    group = ff.FlowGroup("Gated")
    df = _df()
    gate = ff.Gate(df, formula="[a] > 1").add_to_group(group)
    assert isinstance(gate, ff.Gate)
    assert gate.node.setting_input.group_id == group.id
    assert set(group.node_ids) == {gate.node_id}


def test_a_group_follows_its_nodes_across_a_join_of_two_graphs():
    group = ff.FlowGroup("Prep")
    left = ff.from_dict({"id": [1, 2], "a": [1, 2]}).add_to_group(group)
    right = ff.from_dict({"id": [1, 2], "b": [3, 4]})
    assert left.flow_graph is not right.flow_graph
    joined = left.join(right, on="id").add_to_group(group)
    graph = joined.flow_graph
    assert group.flow_graph is graph
    assert sorted(graph._member_node_ids(group.id)) == sorted([left.node_id, joined.node_id])
    assert joined.collect().to_dict(as_series=False) == {"id": [1, 2], "a": [1, 2], "b": [3, 4]}


def test_a_group_holds_nodes_of_one_graph():
    group = ff.FlowGroup("One")
    _df().add_to_group(group)
    with pytest.raises(ff.NativeNodeError, match="belongs to another graph"):
        _df().add_to_group(group)
    with pytest.raises(ff.NativeNodeError, match="takes a ff.FlowGroup"):
        _df().add_to_group("One")


def test_flow_group_arguments_are_checked():
    with pytest.raises(ff.NativeNodeError, match="non-empty name"):
        ff.FlowGroup("")
    with pytest.raises(ff.NativeNodeError, match="color must be one of"):
        ff.FlowGroup("x", color="pink")
    with pytest.raises(ff.NativeNodeError, match="parent_group must be a FlowGroup"):
        ff.FlowGroup("x", parent_group="Outer")


def test_nested_groups_round_trip_through_save_open(tmp_path):
    from flowfile_core.flowfile.manage.io_flowfile import open_flow

    outer, inner = ff.FlowGroup("Outer"), None
    inner = ff.FlowGroup("Inner", color="green", parent_group=outer)
    df = _df().add_to_group(outer)
    df = df.filter(ff.col("a") > 1).add_to_group(inner)
    path = tmp_path / "flow.yaml"
    df.save_graph(str(path))
    reloaded = open_flow(path)
    assert _groups(reloaded) == _groups(df.flow_graph)
    assert _groups(reloaded)["Inner"] == ("green", "Outer", [df.node_id])
