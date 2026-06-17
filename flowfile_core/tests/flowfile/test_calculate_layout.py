"""Tests for flowfile/util/calculate_layout module."""

from unittest.mock import MagicMock, PropertyMock

from flowfile_core.flowfile.util.calculate_layout import calculate_layered_layout


def _make_mock_node(node_id, leads_to=None):
    """Helper to create a mock node."""
    node = MagicMock()
    node.node_id = node_id
    if leads_to is None:
        leads_to = []
    child_nodes = []
    for child_id in leads_to:
        child = MagicMock()
        child.node_id = child_id
        child_nodes.append(child)
    node.leads_to_nodes = child_nodes
    return node


def _make_mock_graph(nodes_data, connections):
    """
    Helper to create a mock FlowGraph.
    nodes_data: list of dicts with 'id' and optionally 'leads_to'
    connections: list of (from_id, to_id) tuples
    """
    graph = MagicMock()
    nodes = [_make_mock_node(d["id"], d.get("leads_to", [])) for d in nodes_data]
    graph.nodes = nodes
    graph.node_connections = connections
    # _node_db should be truthy when nodes exist
    graph._node_db = {n["id"]: True for n in nodes_data} if nodes_data else {}
    return graph


class TestCalculateLayeredLayout:
    """Test calculate_layered_layout function."""

    def test_empty_graph(self):
        graph = MagicMock()
        graph._node_db = {}
        result = calculate_layered_layout(graph)
        assert result == {}

    def test_single_node(self):
        graph = _make_mock_graph([{"id": 1}], [])
        result = calculate_layered_layout(graph)
        assert 1 in result
        pos_x, pos_y = result[1]
        assert pos_x == 0  # First stage

    def test_linear_chain(self):
        """Test a simple A -> B -> C chain."""
        graph = _make_mock_graph(
            [{"id": 1}, {"id": 2}, {"id": 3}],
            [(1, 2), (2, 3)],
        )
        result = calculate_layered_layout(graph)
        assert len(result) == 3
        # Node 1 should be in stage 0, node 2 in stage 1, node 3 in stage 2
        assert result[1][0] < result[2][0] < result[3][0]

    def test_parallel_nodes(self):
        """Test nodes with no dependencies at the same stage."""
        graph = _make_mock_graph(
            [{"id": 1}, {"id": 2}, {"id": 3}],
            [],  # No connections
        )
        result = calculate_layered_layout(graph)
        assert len(result) == 3
        # All should be in stage 0 (same X position)
        assert result[1][0] == result[2][0] == result[3][0]

    def test_fan_out(self):
        """Test A -> B, A -> C pattern."""
        graph = _make_mock_graph(
            [{"id": 1}, {"id": 2}, {"id": 3}],
            [(1, 2), (1, 3)],
        )
        result = calculate_layered_layout(graph)
        assert len(result) == 3
        # Node 1 should be in earlier stage
        assert result[1][0] < result[2][0]
        assert result[1][0] < result[3][0]
        # Nodes 2 and 3 should be in the same stage
        assert result[2][0] == result[3][0]

    def test_fan_in(self):
        """Test A -> C, B -> C pattern."""
        graph = _make_mock_graph(
            [{"id": 1}, {"id": 2}, {"id": 3}],
            [(1, 3), (2, 3)],
        )
        result = calculate_layered_layout(graph)
        assert len(result) == 3
        # Nodes 1 and 2 should be at the same stage
        assert result[1][0] == result[2][0]
        # Node 3 should be at a later stage
        assert result[3][0] > result[1][0]

    def test_custom_spacing(self):
        """Test custom x_spacing and y_spacing."""
        graph = _make_mock_graph(
            [{"id": 1}, {"id": 2}],
            [(1, 2)],
        )
        result = calculate_layered_layout(graph, x_spacing=500, y_spacing=200)
        assert len(result) == 2
        # X spacing should reflect custom spacing
        assert result[2][0] - result[1][0] == 500

    def test_positions_are_integers(self):
        """Test that positions are integer tuples."""
        graph = _make_mock_graph(
            [{"id": 1}, {"id": 2}, {"id": 3}],
            [(1, 2), (1, 3)],
        )
        result = calculate_layered_layout(graph)
        for node_id, (x, y) in result.items():
            assert isinstance(x, int)
            assert isinstance(y, int)

    def test_diamond_pattern(self):
        """Test A -> B, A -> C, B -> D, C -> D pattern."""
        graph = _make_mock_graph(
            [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}],
            [(1, 2), (1, 3), (2, 4), (3, 4)],
        )
        result = calculate_layered_layout(graph)
        assert len(result) == 4
        # A first, B and C middle, D last
        assert result[1][0] < result[2][0]
        assert result[2][0] == result[3][0]
        assert result[4][0] > result[2][0]

    def test_fallback_on_connection_error(self):
        """Test that fallback graph building works when node_connections fails."""
        graph = _make_mock_graph(
            [{"id": 1, "leads_to": [2]}, {"id": 2}],
            [],
        )
        # Make node_connections raise an exception to trigger fallback
        type(graph).node_connections = PropertyMock(side_effect=Exception("connection error"))
        result = calculate_layered_layout(graph)
        assert len(result) == 2
