"""
Tests for visual node grouping in the flowfile_frame API.

Run with:
    pytest flowfile_frame/tests/test_node_groups.py -v

Visual groups are organizational only and unrelated to ``group_by`` (aggregation).
"""
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
