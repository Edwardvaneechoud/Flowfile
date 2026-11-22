"""The core check of a flow document: what a clean but broken merge would only show at its first run.

The flows are built through the graph API and saved by core, so the documents are real saves; the two sides of a
merge are edits made on a real graph of each side, saved to disk.
"""

import copy

import yaml

from flowfile_core.flowfile.manage.document_check import check_document
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.flowfile.manage.ephemeral_graph import free_flow_id
from flowfile_core.schemas import input_schema, transform_schema
from shared.flow_merge.canonical import canonical, emit
from shared.flow_merge.edges import RIGHT
from shared.flow_merge.merge import merge
from tests.flowfile.history_graphs import basic_filter, connect, make_graph, manual_input, promise


def _base(tmp_path, flow_id: int) -> dict:
    """``customers (1) + orders (2) -> join (3) -> filter on a (4) -> sample (5)``, saved by core."""
    graph = make_graph(flow_id, track_history=False)
    manual_input(graph, 1, [{"a": 1, "b": "x"}], 0, 0)
    manual_input(graph, 2, [{"a": 1, "c": 2.5}], 0, 200)
    promise(graph, "join", 3, 250, 100)
    connect(graph, 1, 3)
    connect(graph, 2, 3, input_type="right")
    graph.add_join(
        input_schema.NodeJoin(
            flow_id=flow_id,
            node_id=3,
            depending_on_ids=[1, 2],
            pos_x=250,
            pos_y=100,
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap("a", "a")],
                left_select=[transform_schema.SelectInput("a"), transform_schema.SelectInput("b")],
                right_select=[transform_schema.SelectInput("a", "a_right"), transform_schema.SelectInput("c")],
            ),
        )
    )
    promise(graph, "filter", 4, 500, 100)
    connect(graph, 3, 4)
    graph.add_filter(
        input_schema.NodeFilter(flow_id=flow_id, node_id=4, depending_on_id=3, pos_x=500, pos_y=100, filter_input=basic_filter())
    )
    promise(graph, "sample", 5, 750, 100)
    connect(graph, 4, 5)
    graph.add_sample(input_schema.NodeSample(flow_id=flow_id, node_id=5, depending_on_id=4, pos_x=750, pos_y=100))
    path = tmp_path / f"base_{flow_id}.yaml"
    graph.save_flow(str(path))
    graph.close_flow()
    return yaml.safe_load(path.read_text())


def _edit(tmp_path, document: dict, name: str, change) -> dict:
    """Open ``document`` as a real graph, apply ``change(graph)``, and return what core saves."""
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False))
    graph = open_flow(path)
    try:
        change(graph)
        graph.save_flow(str(path))
    finally:
        graph.close_flow()
    return yaml.safe_load(path.read_text())


def _add_sample(graph, node_id: int, after: int, size: int, x: float) -> None:
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="sample", pos_x=x, pos_y=400)
    )
    connect(graph, after, node_id)
    graph.add_sample(
        input_schema.NodeSample(
            flow_id=graph.flow_id, node_id=node_id, depending_on_id=after, sample_size=size, pos_x=x, pos_y=400
        )
    )


def test_clean_merge_of_real_flows_passes(tmp_path):
    base = _base(tmp_path, free_flow_id())

    def ours(graph):
        node = graph.get_node(5).setting_input.model_copy(deep=True)
        node.sample_size = 20
        graph.add_sample(node)
        _add_sample(graph, 6, 4, 3, 1000)

    def theirs(graph):
        _add_sample(graph, 6, 3, 7, 1200)

    result = merge(base, _edit(tmp_path, base, "ours", ours), _edit(tmp_path, base, "theirs", theirs))
    assert result.clean
    assert result.id_map.nodes == {6: 7}
    check = check_document(result.document)
    assert check.ok, check.problems()


def test_join_left_with_one_input_is_reported(tmp_path):
    flow, _ = canonical(_base(tmp_path, free_flow_id()))
    del flow.inputs[3][RIGHT]
    check = check_document(emit(flow))
    assert check.not_wired == ["Step 3 join does not have the inputs a join needs."]
    assert not check.ok


def test_edge_lost_on_load_is_reported(tmp_path):
    """One source feeding both sides of a join through two outputs: core keeps one output per source."""
    flow, _ = canonical(_base(tmp_path, free_flow_id()))
    flow.inputs[3] = {"input-0": ((1, "output-0"),), RIGHT: ((1, "output-1"),)}
    check = check_document(emit(flow))
    assert any("step 3 join" in line and "inputs" in line for line in check.lost_on_load), check.problems()


def test_settings_check_flags_renamed_column(tmp_path):
    """Ours renames the column the filter reads out of the join; theirs keeps filtering on it: merged cleanly."""
    base = _base(tmp_path, free_flow_id())
    ours = copy.deepcopy(base)
    join = next(n for n in ours["nodes"] if n["id"] == 3)
    join["setting_input"]["join_input"]["left_select"]["select"][0]["new_name"] = "key"
    result = merge(base, ours, copy.deepcopy(base))
    assert result.clean
    check = check_document(result.document)
    assert any(line.startswith("Step 4:") and "a" in line for line in check.settings), check.problems()


def test_check_leaves_no_flow_or_log_file(tmp_path, monkeypatch):
    from flowfile_core.configs.flow_logger import FlowLogger, get_flow_log_file
    from flowfile_core.flowfile.manage import ephemeral_graph

    base = _base(tmp_path, free_flow_id())
    used = []

    def tracked() -> int:
        used.append(free_flow_id())
        return used[-1]

    monkeypatch.setattr(ephemeral_graph, "free_flow_id", tracked)
    assert check_document(base).ok
    assert len(used) == 1
    assert FlowLogger.get_instance(used[0]) is None
    assert not get_flow_log_file(used[0]).exists()
