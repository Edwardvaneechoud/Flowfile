"""``FlowGraph.node_id_ceiling``: the canvas keeps its own ceiling, so a push numbers above every id it has held."""

from tests.flowfile.history_graphs import make_graph, promise


def test_the_ceiling_follows_the_live_ids():
    graph = make_graph(9320)
    assert graph.node_id_ceiling == 0
    promise(graph, "manual_input", 1)
    promise(graph, "filter", 5)
    assert graph.node_id_ceiling == 5


def test_a_deleted_id_stays_under_the_ceiling():
    graph = make_graph(9321)
    promise(graph, "manual_input", 1)
    promise(graph, "filter", 5)
    graph.delete_node(5)
    assert [n.node_id for n in graph.nodes] == [1]
    assert graph.node_id_ceiling == 5
    assert graph.next_node_id() == 6
    assert graph.next_node_id() == 7


def test_an_undone_placement_stays_under_the_ceiling():
    graph = make_graph(9322)
    promise(graph, "manual_input", 1)
    with graph.transaction("place"):
        promise(graph, "filter", 4)
    assert graph.undo().success is True
    assert [n.node_id for n in graph.nodes] == [1]
    assert graph.node_id_ceiling == 4
    assert graph.redo().success is True
    assert graph.node_id_ceiling == 4


def test_the_ceiling_ignores_string_keyed_entries():
    graph = make_graph(9323)
    promise(graph, "manual_input", 2)
    graph._node_db["group-1"] = graph._node_db[2]
    assert graph.node_id_ceiling == 2
    graph._node_db.pop("group-1")
