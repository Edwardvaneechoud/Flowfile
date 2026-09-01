"""The explicit ``connections`` array has to reproduce the canvas exactly.

Without it the browser derives edges and hardcodes ``output-0``, which silently
drops a gate's else branch and mis-routes multi-input nodes.
"""

from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.share.transform import build_connections, build_share_envelope
from flowfile_core.schemas import input_schema

from tests.flowfile.share.conftest import add_manual_input, make_graph


def _connect(graph, from_id: int, to_id: int, target_handle: str = "input-0", source_handle: str = "output-0"):
    add_connection(
        graph,
        input_schema.NodeConnection(
            input_connection=input_schema.NodeInputConnection(node_id=to_id, connection_class=target_handle),
            output_connection=input_schema.NodeOutputConnection(node_id=from_id, connection_class=source_handle),
        ),
    )


def _promise(graph, node_id: int, node_type: str):
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def _edges(envelope: dict) -> set[tuple[int, int, str, str]]:
    return {
        (edge["from_node"], edge["to_node"], edge["from_handle"], edge["to_handle"])
        for edge in envelope["flow"]["connections"]
    }


def test_connections_are_always_emitted():
    graph = make_graph(name="linear")
    add_manual_input(graph, 1)
    _promise(graph, 2, "sort")
    _connect(graph, 1, 2)
    graph.add_sort(input_schema.NodeSort(flow_id=1, node_id=2, depending_on_id=1, sort_input=[]))

    envelope = build_share_envelope(graph).envelope
    assert _edges(envelope) == {(1, 2, "output-0", "input-0")}


def test_gate_else_branch_keeps_its_second_output_handle():
    graph = make_graph(name="gate_else")
    add_manual_input(graph, 1)
    _promise(graph, 2, "gate")
    _connect(graph, 1, 2)
    graph.add_gate(input_schema.NodeGate(flow_id=1, node_id=2, depending_on_id=1, else_output=True))
    for node_id, source_handle in ((3, "output-0"), (4, "output-1")):
        _promise(graph, node_id, "sort")
        _connect(graph, 2, node_id, source_handle=source_handle)
        graph.add_sort(input_schema.NodeSort(flow_id=1, node_id=node_id, depending_on_id=2, sort_input=[]))

    result = build_share_envelope(graph)
    assert _edges(result.envelope) == {
        (1, 2, "output-0", "input-0"),
        (2, 3, "output-0", "input-0"),
        (2, 4, "output-1", "input-0"),
    }
    assert any("secondary output handles" in warning for warning in result.warnings)


def test_placeholder_nodes_keep_their_edges():
    """A gate is a full-app node, so it travels as a placeholder — with its wiring."""
    graph = make_graph(name="gate_placeholder")
    add_manual_input(graph, 1)
    _promise(graph, 2, "gate")
    _connect(graph, 1, 2)
    graph.add_gate(input_schema.NodeGate(flow_id=1, node_id=2, depending_on_id=1, else_output=True))
    _promise(graph, 3, "sort")
    _connect(graph, 2, 3, source_handle="output-1")
    graph.add_sort(input_schema.NodeSort(flow_id=1, node_id=3, depending_on_id=2, sort_input=[]))

    envelope = build_share_envelope(graph).envelope
    gate = next(node for node in envelope["flow"]["nodes"] if node["id"] == 2)
    assert gate["type"] == "gate__unsupported"
    assert gate["setting_input"]["inputs"] >= 1
    assert gate["setting_input"]["outputs"] == 2
    assert (2, 3, "output-1", "input-0") in _edges(envelope)


def test_one_source_feeding_both_join_inputs_keeps_both_edges():
    graph = make_graph(name="self_join")
    add_manual_input(graph, 1)
    _promise(graph, 2, "cross_join")
    _connect(graph, 1, 2, target_handle="input-0")
    _connect(graph, 1, 2, target_handle="input-1")

    envelope = build_share_envelope(graph).envelope
    assert _edges(envelope) == {
        (1, 2, "output-0", "input-0"),
        (1, 2, "output-0", "input-1"),
    }


def test_multi_input_union_keeps_every_branch():
    graph = make_graph(name="union")
    add_manual_input(graph, 1, [{"a": 1}])
    add_manual_input(graph, 2, [{"a": 2}])
    add_manual_input(graph, 3, [{"a": 3}])
    _promise(graph, 4, "union")
    for source_id in (1, 2, 3):
        _connect(graph, source_id, 4)
    graph.add_union(input_schema.NodeUnion(flow_id=1, node_id=4, depending_on_ids=[1, 2, 3]))

    envelope = build_share_envelope(graph).envelope
    assert _edges(envelope) == {
        (1, 4, "output-0", "input-0"),
        (2, 4, "output-0", "input-0"),
        (3, 4, "output-0", "input-0"),
    }


def test_keyed_inputs_win_over_positional_derivation():
    """Dynamic-input nodes (run_flow) address their inputs by handle name."""
    nodes = [
        {"id": 1, "outputs": [3], "output_handles": ["output-0"]},
        {"id": 2, "outputs": [3], "output_handles": ["output-1"]},
        {
            "id": 3,
            "input_ids": [1],
            "outputs": [],
            "input_connections": [
                {"from_id": 1, "input_handle": "input-1", "source_handle": "output-0"},
                {"from_id": 2, "input_handle": "input-2", "source_handle": "output-1"},
            ],
        },
    ]
    assert build_connections(nodes) == [
        {"from_node": 1, "to_node": 3, "from_handle": "output-0", "to_handle": "input-1"},
        {"from_node": 2, "to_node": 3, "from_handle": "output-1", "to_handle": "input-2"},
    ]


def test_left_and_right_inputs_map_to_their_own_handles():
    nodes = [
        {"id": 1, "outputs": [3], "output_handles": ["output-0"]},
        {"id": 2, "outputs": [3], "output_handles": ["output-0"]},
        {"id": 3, "left_input_id": 1, "right_input_id": 2, "input_ids": [], "outputs": []},
    ]
    assert build_connections(nodes) == [
        {"from_node": 1, "to_node": 3, "from_handle": "output-0", "to_handle": "input-0"},
        {"from_node": 2, "to_node": 3, "from_handle": "output-0", "to_handle": "input-1"},
    ]


def test_missing_output_handles_default_to_output_zero():
    nodes = [
        {"id": 1, "outputs": [2]},
        {"id": 2, "input_ids": [1], "outputs": []},
    ]
    assert build_connections(nodes) == [
        {"from_node": 1, "to_node": 2, "from_handle": "output-0", "to_handle": "input-0"}
    ]
