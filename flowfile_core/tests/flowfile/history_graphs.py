"""Graph builders shared by the undo/redo invariant tests.

``build_rich_graph`` covers every wiring shape the history builder must reproduce:
multi-output handles (gate else, filter split, random split), multi-input order
(union / polars_code wired out of creation order), join left/right, a self-join,
unconfigured promises with metadata inside nested groups, typed Select columns,
user/auto descriptions, node references, comments and a dynamic-input node.
"""

from collections import Counter

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.history_schema import IN_SCOPE_KEYS
from flowfile_core.schemas.schemas import FlowParameter


def make_graph(flow_id: int, path: str = ".", track_history: bool = True) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name=f"history_{flow_id}",
            path=path,
            execution_mode="Development",
            execution_location="local",
            track_history=track_history,
        )
    )
    return handler.get_flow(flow_id)


def in_scope(flow: FlowGraph) -> dict:
    data = flow.get_flowfile_data().model_dump()
    return {key: data[key] for key in IN_SCOPE_KEYS}


def edge_bookkeeping_errors(flow: FlowGraph) -> list[str]:
    """Where the live graph's two edge records disagree.

    Every connected input slot of a target must be matched by exactly one ``leads_to_nodes``
    entry on its source, and a target keeps a source's output handle only while that source
    still feeds one of its slots. A snapshot reads edges from the targets, so a stale
    ``leads_to`` entry would be invisible to snapshot comparisons; this checks it directly.
    """
    errors = []
    expected: Counter = Counter()
    for target in flow.nodes:
        feeding = {source.node_id for source, _ in target._incoming_edges()}
        for source, _ in target._incoming_edges():
            expected[(source.node_id, target.node_id)] += 1
        stale_handles = set(target._input_output_handles) - feeding
        if stale_handles:
            errors.append(f"node {target.node_id} keeps output handles of {sorted(stale_handles)}")
    actual = Counter((node.node_id, lead.node_id) for node in flow.nodes for lead in node.leads_to_nodes)
    if actual != expected:
        errors.append(f"leads_to extra {dict(actual - expected)}, missing {dict(expected - actual)}")
    return errors


def promise(graph: FlowGraph, node_type: str, node_id: int, pos_x: float = 0, pos_y: float = 0) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type, pos_x=pos_x, pos_y=pos_y)
    )


def connect(graph: FlowGraph, from_id: int, to_id: int, input_type: str = "main", output_handle: str = "output-0"):
    add_connection(
        graph,
        input_schema.NodeConnection.create_from_simple_input(
            from_id, to_id, input_type=input_type, output_handle=output_handle
        ),
    )


def connect_keyed(graph: FlowGraph, from_id: int, to_id: int, handle: str, output_handle: str = "output-0"):
    connection = input_schema.NodeConnection.create_from_simple_input(from_id, to_id, output_handle=output_handle)
    connection.input_connection.connection_class = handle
    add_connection(graph, connection)


def manual_input(graph: FlowGraph, node_id: int, rows: list[dict], pos_x: float = 0, pos_y: float = 0) -> None:
    promise(graph, "manual_input", node_id, pos_x, pos_y)
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            pos_x=pos_x,
            pos_y=pos_y,
            raw_data_format=input_schema.RawData.from_pylist(rows),
        )
    )


def basic_filter(field: str = "a", value: str = "1") -> transform_schema.FilterInput:
    return transform_schema.FilterInput(
        mode="basic", basic_filter=transform_schema.BasicFilter(field=field, operator="equals", value=value)
    )


def build_rich_graph(flow_id: int, path: str = ".") -> FlowGraph:
    graph = make_graph(flow_id, path=path)
    graph.flow_settings.parameters.append(FlowParameter(name="env", default_value="prod", type="string"))
    fid = graph.flow_id

    manual_input(graph, 1, [{"a": 1, "b": "x", "c": 1.5}], 10, 10)
    manual_input(graph, 2, [{"a": 2, "b": "y", "c": 2.5}], 10, 200)

    promise(graph, "gate", 3, 200, 10)
    connect(graph, 1, 3)
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=fid,
            node_id=3,
            depending_on_id=1,
            pos_x=200,
            pos_y=10,
            else_output=True,
            gate_input=transform_schema.GateInput(parameter="env", operator="equals", value="prod"),
        )
    )

    promise(graph, "select", 4, 400, 10)
    connect(graph, 3, 4, output_handle="output-0")
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=fid,
            node_id=4,
            depending_on_id=3,
            pos_x=400,
            pos_y=10,
            select_input=[
                transform_schema.SelectInput(old_name="a", data_type="Int64", data_type_change=False),
                transform_schema.SelectInput(old_name="b", new_name="bb", data_type="String"),
                transform_schema.SelectInput(old_name="c", data_type="String", data_type_change=True),
            ],
        )
    )

    promise(graph, "select", 5, 400, 120)
    connect(graph, 3, 5, output_handle="output-1")
    graph.add_select(
        input_schema.NodeSelect(
            flow_id=fid,
            node_id=5,
            depending_on_id=3,
            pos_x=400,
            pos_y=120,
            select_input=[transform_schema.SelectInput(old_name="a", data_type="Int64")],
        )
    )

    promise(graph, "random_split", 6, 200, 200)
    connect(graph, 2, 6)
    graph.add_random_split(
        input_schema.NodeRandomSplit(flow_id=fid, node_id=6, depending_on_id=2, pos_x=200, pos_y=200, seed=7)
    )
    for node_id, handle, pos_y in ((7, "output-1", 200), (8, "output-0", 300)):
        promise(graph, "sample", node_id, 400, pos_y)
        connect(graph, 6, node_id, output_handle=handle)
        graph.add_sample(
            input_schema.NodeSample(flow_id=fid, node_id=node_id, depending_on_id=6, pos_x=400, pos_y=pos_y)
        )

    promise(graph, "union", 9, 600, 50)
    for source in (5, 4, 2):
        connect(graph, source, 9)
    graph.add_union(
        input_schema.NodeUnion(
            flow_id=fid,
            node_id=9,
            depending_on_ids=[5, 4, 2],
            pos_x=600,
            pos_y=50,
            union_input=transform_schema.UnionInput(mode="relaxed"),
        )
    )

    promise(graph, "polars_code", 10, 600, 250)
    for source in (8, 1):
        connect(graph, source, 10)
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=fid,
            node_id=10,
            depending_on_ids=[8, 1],
            pos_x=600,
            pos_y=250,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code="input_df_1"),
        )
    )

    join_input = transform_schema.JoinInput(
        join_mapping=[transform_schema.JoinMap("a", "a")],
        left_select=[transform_schema.SelectInput("a"), transform_schema.SelectInput("b")],
        right_select=[transform_schema.SelectInput("a"), transform_schema.SelectInput("c", "c_right")],
    )
    promise(graph, "join", 11, 800, 10)
    connect(graph, 1, 11)
    connect(graph, 2, 11, input_type="right")
    graph.add_join(
        input_schema.NodeJoin(
            flow_id=fid, node_id=11, depending_on_ids=[1, 2], pos_x=800, pos_y=10, join_input=join_input
        )
    )

    promise(graph, "join", 12, 800, 200)
    connect(graph, 2, 12)
    connect(graph, 2, 12, input_type="right")
    graph.add_join(
        input_schema.NodeJoin(
            flow_id=fid,
            node_id=12,
            depending_on_ids=[2, 2],
            pos_x=800,
            pos_y=200,
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("a", "a")],
                left_select=[transform_schema.SelectInput("a")],
                right_select=[transform_schema.SelectInput("a", "a_right")],
            ),
        )
    )

    promise(graph, "filter", 13, 1000, 10)
    connect(graph, 11, 13)
    graph.get_node(13).setting_input.description = "Promise note"
    graph.get_node(13).setting_input.node_reference = "pending_filter"

    promise(graph, "filter", 14, 1000, 150)
    connect(graph, 11, 14)
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=fid,
            node_id=14,
            depending_on_id=11,
            pos_x=1000,
            pos_y=150,
            description="User description",
            node_reference="user_ref",
            split_mode=True,
            filter_input=basic_filter(),
        )
    )
    promise(graph, "sample", 15, 1200, 100)
    connect(graph, 14, 15, output_handle="output-1")
    graph.add_sample(input_schema.NodeSample(flow_id=fid, node_id=15, depending_on_id=14, pos_x=1200, pos_y=100))

    promise(graph, "filter", 16, 1000, 300)
    connect(graph, 12, 16)
    graph.add_filter(
        input_schema.NodeFilter(
            flow_id=fid, node_id=16, depending_on_id=12, pos_x=1000, pos_y=300, filter_input=basic_filter("a", "2")
        )
    )

    promise(graph, "sort", 17, 1200, 300)

    outer = graph.create_group("Outer", [13, 14])
    inner = graph.create_group("Inner", [17], parent_group_id=outer.id)
    graph.update_group(inner.id, collapsed=True)

    graph.create_comment("First note", 20, 400, width=200, height=90)
    graph.create_comment("Second note", 300, 400)

    promise(graph, "run_flow", 18, 800, 400)
    graph.add_run_flow(
        input_schema.NodeRunFlow(
            flow_id=fid,
            node_id=18,
            pos_x=800,
            pos_y=400,
            flow_reference=input_schema.SubflowReference(registration_id=987654),
            input_slots=["first", "second"],
            output_slots=["out"],
        )
    )
    connect_keyed(graph, 5, 18, "input-2")
    connect_keyed(graph, 4, 18, "input-1")
    return graph
