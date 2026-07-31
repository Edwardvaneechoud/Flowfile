"""Tests for flowfile/util/calculate_layout module.

These pin the X/stage contract with mock graphs; the sibling
test_calculate_layout_geometry.py covers real FlowGraph behaviour.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, PropertyMock

from flowfile_core.flowfile.util.calculate_layout import calculate_layered_layout


def _make_mock_node(node_id):
    """A node double with no canvas edges of its own."""
    node = MagicMock()
    node.node_id = node_id
    node.get_edge_input.return_value = []
    return node


def _make_mock_graph(node_ids, connections):
    """A graph double whose edges come from node_connections only."""
    graph = MagicMock()
    graph.nodes = [_make_mock_node(node_id) for node_id in node_ids]
    graph.node_connections = connections
    return graph


def _wire(source, target, source_handle="output-0", target_handle="input-0"):
    return SimpleNamespace(source=source, target=target, sourceHandle=source_handle, targetHandle=target_handle)


class TestCalculateLayeredLayout:
    """Test calculate_layered_layout function."""

    def test_empty_graph(self):
        graph = _make_mock_graph([], [])
        result = calculate_layered_layout(graph)
        assert result == {}

    def test_single_node(self):
        graph = _make_mock_graph([1], [])
        result = calculate_layered_layout(graph)
        assert 1 in result
        pos_x, pos_y = result[1]
        assert pos_x == 0  # First stage

    def test_linear_chain(self):
        """Test a simple A -> B -> C chain."""
        graph = _make_mock_graph([1, 2, 3], [(1, 2), (2, 3)])
        result = calculate_layered_layout(graph)
        assert len(result) == 3
        # Node 1 should be in stage 0, node 2 in stage 1, node 3 in stage 2
        assert result[1][0] < result[2][0] < result[3][0]

    def test_parallel_nodes(self):
        """Test nodes with no dependencies at the same stage."""
        graph = _make_mock_graph([1, 2, 3], [])
        result = calculate_layered_layout(graph)
        assert len(result) == 3
        # All should be in stage 0 (same X position)
        assert result[1][0] == result[2][0] == result[3][0]

    def test_fan_out(self):
        """Test A -> B, A -> C pattern."""
        graph = _make_mock_graph([1, 2, 3], [(1, 2), (1, 3)])
        result = calculate_layered_layout(graph)
        assert len(result) == 3
        # Node 1 should be in earlier stage
        assert result[1][0] < result[2][0]
        assert result[1][0] < result[3][0]
        # Nodes 2 and 3 should be in the same stage
        assert result[2][0] == result[3][0]

    def test_fan_in(self):
        """Test A -> C, B -> C pattern."""
        graph = _make_mock_graph([1, 2, 3], [(1, 3), (2, 3)])
        result = calculate_layered_layout(graph)
        assert len(result) == 3
        # Nodes 1 and 2 should be at the same stage
        assert result[1][0] == result[2][0]
        # Node 3 should be at a later stage
        assert result[3][0] > result[1][0]

    def test_custom_spacing(self):
        """Test custom x_spacing and y_spacing."""
        graph = _make_mock_graph([1, 2], [(1, 2)])
        result = calculate_layered_layout(graph, x_spacing=500, y_spacing=200)
        assert len(result) == 2
        # X spacing should reflect custom spacing
        assert result[2][0] - result[1][0] == 500

    def test_positions_are_integers(self):
        """Test that positions are integer tuples."""
        graph = _make_mock_graph([1, 2, 3], [(1, 2), (1, 3)])
        result = calculate_layered_layout(graph)
        for node_id, (x, y) in result.items():
            assert isinstance(x, int)
            assert isinstance(y, int)

    def test_diamond_pattern(self):
        """Test A -> B, A -> C, B -> D, C -> D pattern."""
        graph = _make_mock_graph([1, 2, 3, 4], [(1, 2), (1, 3), (2, 4), (3, 4)])
        result = calculate_layered_layout(graph)
        assert len(result) == 4
        # A first, B and C middle, D last
        assert result[1][0] < result[2][0]
        assert result[2][0] == result[3][0]
        assert result[4][0] > result[2][0]

    def test_edges_survive_connection_error(self):
        """node_connections raising leaves the canvas edge set in charge."""
        graph = _make_mock_graph([1, 2], [])
        graph.nodes[1].get_edge_input.return_value = [_wire(1, 2)]
        type(graph).node_connections = PropertyMock(side_effect=Exception("connection error"))
        result = calculate_layered_layout(graph)
        assert len(result) == 2
        assert result[1][0] < result[2][0]

    def test_edges_survive_edge_input_error(self):
        """A node whose get_edge_input raises is backfilled from node_connections."""
        graph = _make_mock_graph([1, 2], [(1, 2)])
        graph.nodes[1].get_edge_input.side_effect = Exception("broken node")
        result = calculate_layered_layout(graph)
        assert len(result) == 2
        assert result[1][0] < result[2][0]

    def test_malformed_source_handle_does_not_break_layout(self):
        """An unparseable port degrades to port 0 for that edge, not a failed layout."""
        graph = _make_mock_graph([1, 2], [])
        graph.nodes[1].get_edge_input.return_value = [_wire(1, 2, source_handle="output--1")]
        result = calculate_layered_layout(graph)
        assert len(result) == 2
        assert result[1][0] < result[2][0]
