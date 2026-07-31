"""Geometric assertions for the layered layout, against real FlowGraph objects.

Run with:
    pytest flowfile_core/tests/flowfile/test_calculate_layout_geometry.py -v

The sibling test_calculate_layout.py pins the X/stage contract with mock graphs.
This suite pins what the mocks cannot reach: crossing counts, straightness,
port order, component banding and determinism.
"""
import time

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.util.calculate_layout import calculate_layered_layout
from flowfile_core.schemas import input_schema, schemas

X_SPACING = 200
Y_SPACING = 150


def create_graph(flow_id: int = 1) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="layout_flow", path=".", execution_mode="Development")
    )
    return handler.get_flow(flow_id)


def build(node_types: dict[int, str], connections: list[tuple], flow_id: int = 1) -> FlowGraph:
    """A graph of promised nodes; connections are (source, target) or (source, target, input_type)."""
    graph = create_graph(flow_id)
    for node_id, node_type in node_types.items():
        graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type=node_type))
    for connection in connections:
        source, target = connection[0], connection[1]
        input_type = connection[2] if len(connection) > 2 else "main"
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(source, target, input_type))
    return graph


def layout(graph: FlowGraph) -> dict[int, tuple[int, int]]:
    return calculate_layered_layout(graph, x_spacing=X_SPACING, y_spacing=Y_SPACING, initial_y=100)


def count_crossings(positions: dict[int, tuple[int, int]], edges: list[tuple[int, int]]) -> int:
    crossings = 0
    spans = [(positions[source], positions[target]) for source, target in edges]
    for index, (source_a, target_a) in enumerate(spans):
        for source_b, target_b in spans[index + 1 :]:
            if source_a[0] != source_b[0] or target_a[0] != target_b[0]:
                continue
            if (source_a[1] - source_b[1]) * (target_a[1] - target_b[1]) < 0:
                crossings += 1
    return crossings


def test_reported_case_has_no_crossings():
    """Three readers feeding three transforms, wired so node ids interleave.

    This is the reported bug: ordering a layer by node id put reader 1 above
    reader 2 above reader 3 while their targets sat in the opposite order.
    """
    edges = [(1, 5), (2, 6), (3, 4), (4, 7), (5, 7), (7, 8), (6, 8)]
    graph = build(
        {1: "manual_input", 2: "manual_input", 3: "manual_input", 4: "select", 5: "select", 6: "select",
         7: "join", 8: "join"},
        [(1, 5), (2, 6), (3, 4), (4, 7), (5, 7, "right"), (7, 8), (6, 8, "right")],
    )
    positions = layout(graph)

    assert len(positions) == 8
    assert count_crossings(positions, edges) == 0


def test_chain_is_perfectly_straight():
    graph = build({1: "manual_input", 2: "select", 3: "sort", 4: "output"}, [(1, 2), (2, 3), (3, 4)])
    positions = layout(graph)

    assert len({positions[node_id][1] for node_id in (1, 2, 3, 4)}) == 1
    assert [positions[node_id][0] for node_id in (1, 2, 3, 4)] == [0, X_SPACING, 2 * X_SPACING, 3 * X_SPACING]


def test_diamond_is_symmetric():
    graph = build(
        {1: "manual_input", 2: "select", 3: "select", 4: "join"},
        [(1, 2), (1, 3), (2, 4), (3, 4, "right")],
    )
    positions = layout(graph)

    assert positions[1][1] == positions[4][1]
    centre = positions[1][1]
    assert abs(positions[2][1] - centre) == abs(positions[3][1] - centre)
    assert positions[2][1] != positions[3][1]


def test_join_feeders_follow_port_order():
    """The node wired to input-0 sits above the one wired to input-1, either way round."""
    main_first = layout(build({1: "manual_input", 2: "manual_input", 3: "join"}, [(1, 3), (2, 3, "right")]))
    assert main_first[1][1] < main_first[2][1]

    right_first = layout(build({1: "manual_input", 2: "manual_input", 3: "join"}, [(2, 3), (1, 3, "right")]))
    assert right_first[2][1] < right_first[1][1]


def test_unconnected_pipelines_are_banded():
    """A->B and C->D lay out as two rows, each starting at the left edge."""
    graph = build(
        {1: "manual_input", 2: "select", 3: "manual_input", 4: "select"},
        [(1, 2), (3, 4)],
    )
    positions = layout(graph)

    assert positions[1][0] == positions[3][0] == 0
    assert positions[2][0] == positions[4][0] == X_SPACING
    assert positions[1][1] == positions[2][1]
    assert positions[3][1] == positions[4][1]
    assert positions[1][1] < positions[3][1]


def test_lone_nodes_stay_compact():
    """Unwired nodes advance one slot at a time, not a whole pipeline gutter."""
    graph = build({node_id: "manual_input" for node_id in range(1, 6)}, [])
    positions = layout(graph)

    ys = sorted(position[1] for position in positions.values())
    assert {position[0] for position in positions.values()} == {0}
    assert ys == [100 + index * Y_SPACING for index in range(5)]


def test_is_deterministic():
    node_types = {1: "manual_input", 2: "manual_input", 3: "select", 4: "select", 5: "join"}
    connections = [(1, 3), (2, 4), (3, 5), (4, 5, "right")]
    first = layout(build(node_types, connections, flow_id=1))
    second = layout(build(node_types, connections, flow_id=2))

    assert first == second


def test_every_node_is_positioned_with_integer_coordinates():
    graph = build(
        {1: "manual_input", 2: "select", 3: "select", 4: "join", 5: "output"},
        [(1, 2), (1, 3), (2, 4), (3, 4, "right"), (4, 5)],
    )
    positions = layout(graph)

    assert set(positions) == {1, 2, 3, 4, 5}
    for pos_x, pos_y in positions.values():
        assert isinstance(pos_x, int) and isinstance(pos_y, int)
        assert pos_x >= 0 and pos_y >= 0


def test_long_edge_does_not_break_the_layout():
    graph = build(
        {1: "manual_input", 2: "select", 3: "sort", 4: "join"},
        [(1, 2), (2, 3), (3, 4), (1, 4, "right")],
    )
    positions = layout(graph)

    assert len(positions) == 4
    assert positions[1][0] < positions[2][0] < positions[3][0] < positions[4][0]


def test_every_pipeline_starts_at_the_left_edge():
    """Two pipelines of different depth both begin in column zero."""
    graph = build(
        {1: "manual_input", 2: "select", 3: "sort", 4: "manual_input", 5: "select"},
        [(1, 2), (2, 3), (4, 5)],
    )
    positions = layout(graph)

    assert positions[1][0] == positions[4][0] == 0
    assert positions[3][0] == 2 * X_SPACING


def test_large_graph_is_fast():
    node_types = {node_id: "select" for node_id in range(1, 301)}
    node_types[1] = "manual_input"
    connections = [(node_id - 1, node_id) for node_id in range(2, 301) if node_id % 10]
    graph = build(node_types, connections)

    started = time.perf_counter()
    positions = layout(graph)
    elapsed = time.perf_counter() - started

    assert len(positions) == 300
    assert elapsed < 0.5
