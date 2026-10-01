from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.schemas import input_schema, schemas, transform_schema

try:
    import os

    from tests.flowfile_core_test_utils import ensure_password_is_available, is_docker_available
    from tests.utils import ensure_cloud_storage_connection_is_available_and_get_connection, get_cloud_connection
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/utils.py")))
    # noinspection PyUnresolvedReferences


def create_flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    """Create basic flow settings for tests"""
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_flow"
    )


def create_basic_flow(flow_id: int = 1, name: str = "test_flow") -> FlowGraph:
    """Create a basic flow graph for testing"""
    return FlowGraph(flow_settings=create_flow_settings(flow_id), name=name)


def test_graph_tree(capsys):
    """ Test made to the FlowGraph's print_tree method"""
    flow = create_basic_flow()

    manual_input_data_1 = [
    [1, 2, 3],
    ["North", "South", "East"],
    [10, 5, 8],
    [150, 300, 200]
    ]

    manual_input_data_2 = [
    [4, 5, 6],
    ["West", "North", "South"],
    [15, 12, 7],
    [100, 200, 250]
    ]

    manual_input_data_3 = [
    [7, 8, 9],
    ["East", "West", "North"],
    [10, 6, 3],
    [180, 220, 350]
    ]

    manual_input_data_4 = [
    [10, 11, 12],
    ["South", "East", "West"],
    [9, 4, 7],
    [280, 260, 230]
    ]

    for i, j in zip([manual_input_data_1, manual_input_data_2, manual_input_data_3, manual_input_data_4], range(1, 5), strict=False):
        data = input_schema.NodeManualInput(
            flow_id=1,
            node_id=j,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="region", data_type="String"),
                    input_schema.MinimalFieldInfo(name="quantity", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="price", data_type="Integer")
                ],
                data=i
            )
        )
        flow.add_manual_input(data)

    union_node = input_schema.NodeUnion(
        flow_id=1,
        node_id=5,
        depending_on_ids=[1, 2, 3, 4],
        union_input=transform_schema.UnionInput(mode="relaxed")
    )
    flow.add_union(union_node)
    for i in range(1, 5):
        connection = input_schema.NodeConnection.create_from_simple_input(i, 5, 'main')
        add_connection(flow, connection)

    groupby_node = input_schema.NodeGroupBy(
        flow_id=1,
        node_id=6,
        depending_on_id=1,
        groupby_input=transform_schema.GroupByInput(
            agg_cols=[
                transform_schema.AggColl("region", "groupby"),
                transform_schema.AggColl("quantity", "sum", "total_quantity"),
                transform_schema.AggColl("price", "mean", "avg_price"),
                transform_schema.AggColl("quantity", "count", "num_transactions")
            ]
        )
    )
    flow.add_group_by(groupby_node)
    add_connection(flow, node_connection=input_schema.NodeConnection.create_from_simple_input(5, 6, 'main'))

    flow.print_tree()

    assert capsys.readouterr().out == (
        "● Manual input (1)\n"
        "│ ● Manual input (2)\n"
        "│ │ ● Manual input (3)\n"
        "│ │ │ ● Manual input (4)\n"
        "├─┴─┴─┘\n"
        "● Union data (5)\n"
        "│\n"
        "● Group by (6)\n"
    )


def test_print_tree_draws_branches_merges_and_named_exits(capsys):
    """Branches open and merge back, a crossing lane stays visible, nodes in one column are linked, and Gate exits are named."""
    import flowfile as ff

    source = ff.from_dict({"x": [1, 5]}, description="Orders")
    kept = source.filter(ff.col("x") > 0)
    picked = source.select("x")
    checked = kept.select("x")
    stacked = ff.concat([kept, picked], how="diagonal_relaxed")
    joined = stacked.join(checked, on="x")
    gate = ff.Gate(frame=joined, formula=ff.col("x") > 3)
    small = gate.otherwise.select("x")

    small.flow_graph.print_tree()

    assert capsys.readouterr().out == (
        f"● Manual input ({source.node_id})  Orders\n"
        "├─┐\n"
        f"● │ Filter data ({kept.node_id})\n"
        "├─┼─┐\n"
        f"│ ● │ Select data ({picked.node_id})\n"
        f"● │ │ Select data ({checked.node_id})\n"
        "│ ├─┘\n"
        f"│ ● Union data ({stacked.node_id})\n"
        "├─┘\n"
        f"● Join ({joined.node_id})\n"
        "│\n"
        f"● Gate ({gate.node_id})\n"
        "│\n"
        f"● Select data ({small.node_id})  [else]\n"
    )


def test_print_tree_closes_a_dead_end_branch_before_the_main_line(capsys):
    """A dead end added last prints under the node it leaves; the branch with more below it keeps the column."""
    import flowfile as ff

    source = ff.from_dict({"x": [1, 5]})
    kept = source.filter(ff.col("x") > 0)
    shown = kept.select("x")
    side = source.select("x")

    source.flow_graph.print_tree()

    assert capsys.readouterr().out == (
        f"● Manual input ({source.node_id})\n"
        "├─┐\n"
        f"│ ● Select data ({side.node_id})\n"
        f"● Filter data ({kept.node_id})\n"
        "│\n"
        f"● Select data ({shown.node_id})\n"
    )
