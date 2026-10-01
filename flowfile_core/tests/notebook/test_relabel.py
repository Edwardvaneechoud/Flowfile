"""Tests for the push relabel step: pure payload rewrite, provenance matching, and a frame round trip."""

import json

import polars as pl

import flowfile_frame as ff
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.notebook.relabel import provenance_mapping, relabel


def _node(node_id, **fields):
    base = {"id": node_id, "type": "filter", "input_ids": [], "outputs": [], "setting_input": None}
    base.update(fields)
    return base


def _payload():
    return {
        "flowfile_version": "x",
        "flowfile_id": 1,
        "flowfile_name": "f",
        "flowfile_settings": {},
        "nodes": [
            _node(1, type="manual_input", outputs=[3, 4], output_handles=["output-0", "output-0"]),
            _node(2, type="manual_input", outputs=[3]),
            _node(3, type="join", left_input_id=1, right_input_id=2, outputs=[5]),
            _node(
                4,
                input_ids=[1],
                outputs=[5, 6],
                output_handles=["output-0", "output-1"],
                setting_input={"node_id": 4, "depending_on_id": 1, "nested": {"upstream_node_id": 1}},
            ),
            _node(
                5,
                type="union",
                input_ids=[3, 4],
                setting_input={"depending_on_ids": [3, 4], "upstream_train_node_id": 3},
            ),
            _node(6, input_ids=[4], input_connections=[{"from_id": 4, "input_handle": "a", "source_handle": "output-1"}]),
        ],
    }


def test_relabel_rewrites_every_id_bearing_field():
    original = _payload()
    snapshot = json.dumps(original, sort_keys=True)
    out = relabel(original, {1: 10, 3: 30, 4: 40})
    assert json.dumps(original, sort_keys=True) == snapshot
    by_id = {n["id"]: n for n in out["nodes"]}
    assert set(by_id) == {10, 2, 30, 40, 5, 6}
    assert by_id[10]["outputs"] == [30, 40]
    assert by_id[2]["outputs"] == [30]
    assert (by_id[30]["left_input_id"], by_id[30]["right_input_id"]) == (10, 2)
    assert by_id[40]["input_ids"] == [10]
    assert by_id[40]["output_handles"] == ["output-0", "output-1"]
    assert by_id[40]["setting_input"] == {"node_id": 40, "depending_on_id": 10, "nested": {"upstream_node_id": 10}}
    assert by_id[5]["setting_input"] == {"depending_on_ids": [30, 40], "upstream_train_node_id": 30}
    assert by_id[5]["input_ids"] == [30, 40]
    assert by_id[6]["input_connections"][0]["from_id"] == 40


def test_relabel_permutation_swaps_ids():
    out = relabel(_payload(), {1: 2, 2: 1})
    by_id = {n["id"]: n for n in out["nodes"]}
    assert by_id[2]["type"] == by_id[1]["type"] == "manual_input"
    assert (by_id[3]["left_input_id"], by_id[3]["right_input_id"]) == (2, 1)
    assert by_id[4]["input_ids"] == [2]


def test_provenance_mapping_matches_by_type_in_creation_order():
    created = [
        ("c1", "manual_input", 1),
        ("c2", "filter", 2),
        ("c2", "select", 3),
        ("c2", "filter", 4),
        ("c3", "join", 5),
    ]
    provenance = {"c1": [("manual_input", 7)], "c2": [("filter", 12), ("filter", 9)]}
    mapping = provenance_mapping(created, provenance, ceiling=20)
    assert mapping == {1: 7, 2: 12, 4: 9, 3: 21, 5: 22}


def test_provenance_mapping_fresh_ids_clear_provenance_above_ceiling():
    mapping = provenance_mapping([("c1", "filter", 1), ("c1", "filter", 2)], {"c1": [("filter", 50)]}, ceiling=10)
    assert mapping == {1: 50, 2: 51}


def test_provenance_mapping_cells_do_not_cross_match():
    mapping = provenance_mapping([("c2", "filter", 1)], {"c1": [("filter", 5)]}, ceiling=5)
    assert mapping == {1: 6}


def test_round_trip_through_open_flow(tmp_path):
    left = ff.from_dict({"id": [1, 2, 3], "v": [10, 20, 30]})
    right = ff.from_dict({"id": [1, 2, 3], "w": ["a", "b", "c"]})
    joined = left.join(right, on="id")
    kept, dropped = joined.filter(ff.col("v") > 5).filter_split(ff.col("v") > 15)
    final = kept.select("id", "w")
    dropped.select("id")
    graph = final.flow_graph
    ids_before = sorted(n.node_id for n in graph.nodes)
    data = graph.get_flowfile_data().model_dump(mode="json")

    permutation = dict(zip(ids_before, [i + 100 for i in reversed(ids_before)], strict=True))
    relabelled = relabel(data, permutation)
    path = tmp_path / "relabelled.json"
    path.write_text(json.dumps(relabelled))
    reopened = open_flow(path)

    def edges(g, remap=None):
        remap = remap or {}
        out = set()
        for node in g.nodes:
            info = node.get_node_information()
            for target, handle in zip(info.outputs, info.output_handles or ["output-0"] * len(info.outputs), strict=True):
                out.add((remap.get(node.node_id, node.node_id), remap.get(target, target), handle))
        return out

    assert sorted(n.node_id for n in reopened.nodes) == sorted(permutation.values())
    assert edges(reopened) == edges(graph, permutation)
    assert any(h == "output-1" for *_, h in edges(reopened))
    types_before = {permutation[n.node_id]: n.node_type for n in graph.nodes}
    assert {n.node_id: n.node_type for n in reopened.nodes} == types_before
    join_id = next(n.node_id for n in graph.nodes if n.node_type == "join")
    join_after = reopened.get_node(permutation[join_id]).get_node_information()
    join_before = graph.get_node(join_id).get_node_information()
    assert join_after.input_ids == [permutation[i] for i in join_before.input_ids]
    assert join_after.right_input_id == permutation[join_before.right_input_id]
    final_id = permutation[final.node_id]
    result = reopened.get_node(final_id).get_resulting_data().data_frame
    if isinstance(result, pl.LazyFrame):
        result = result.collect()
    assert result.sort("id").to_dict(as_series=False) == {"id": [2, 3], "w": ["b", "c"]}
