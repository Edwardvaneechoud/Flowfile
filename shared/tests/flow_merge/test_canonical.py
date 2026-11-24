"""``shared.flow_merge.canonical``: a flow document read once from the target side, and written back as a save does."""

import copy

import pytest

from shared.flow_merge.canonical import canonical, emit
from shared.flow_merge.edges import MAIN, RIGHT

from .fixtures import fixture_id, flow_files, load

FLOW_FILES = flow_files()


def _node(node_id, node_type="filter", **fields):
    return {"id": node_id, "type": node_type, "setting_input": {"x": node_id}, **fields}


def test_there_are_fixtures():
    assert len(FLOW_FILES) >= 25


@pytest.mark.parametrize("path", FLOW_FILES, ids=fixture_id)
def test_round_trip_every_fixture_flow(path):
    document = load(path)
    flow, issues = canonical(document)
    assert issues == []
    assert emit(flow) == document


@pytest.mark.parametrize("path", FLOW_FILES, ids=fixture_id)
def test_reading_does_not_change_the_document(path):
    document = load(path)
    before = copy.deepcopy(document)
    flow, _ = canonical(document)
    emit(flow)
    assert document == before


def test_edges_read_once_from_both_ends():
    """The target is what a load wires: an edge the source forgot to list is still an edge, and is written back."""
    document = {
        "nodes": [
            _node(1, "manual_input", input_ids=None, outputs=[], output_handles=[]),
            _node(2, input_ids=[1], outputs=[], output_handles=[]),
        ]
    }
    flow, issues = canonical(document)
    assert flow.inputs == {2: {MAIN: ((1, "output-0"),)}}
    assert issues == []
    assert emit(flow)["nodes"][0]["outputs"] == [2]


def test_source_handle_is_read_from_the_source():
    document = {
        "nodes": [
            _node(1, "random_split", outputs=[2, 3], output_handles=["output-0", "output-1"]),
            _node(2, input_ids=[1], outputs=[]),
            _node(3, "join", input_ids=[2], right_input_id=1, outputs=[]),
        ]
    }
    flow, _ = canonical(document)
    assert flow.inputs[2] == {MAIN: ((1, "output-0"),)}
    assert flow.inputs[3] == {MAIN: ((2, "output-0"),), RIGHT: ((1, "output-1"),)}


def test_source_only_edge_becomes_main_input():
    """A legacy file may list an edge on its source only; a load wires it as main into an input-less target."""
    document = {
        "nodes": [
            _node(1, "manual_input", outputs=[2, 3]),
            _node(2, input_ids=None, outputs=[]),
            _node(3, input_ids=[2], outputs=[]),
        ]
    }
    flow, issues = canonical(document)
    assert flow.inputs[2] == {MAIN: ((1, "output-0"),)}
    assert flow.inputs[3] == {MAIN: ((2, "output-0"),)}, "a target with inputs keeps them; the extra edge is dropped"
    assert [(i.code, i.ids) for i in issues] == [("source_only_edge", (1, 2)), ("source_only_edge", (1, 3))]


def test_an_edge_from_a_missing_step_is_dropped():
    flow, issues = canonical({"nodes": [_node(2, input_ids=[9], outputs=[])]})
    assert flow.inputs == {}
    assert [(i.code, i.ids) for i in issues] == [("dangling_edge", (2, 9))]
    assert emit(flow)["nodes"][0]["input_ids"] is None


def test_duplicate_step_ids_keep_the_first():
    flow, issues = canonical({"nodes": [_node(1, "a"), _node(1, "b")]})
    assert flow.steps[1]["type"] == "a"
    assert [i.code for i in issues] == ["duplicate_step"]


def test_absent_keys_equal_defaults():
    """``input_ids: []`` and no ``output_handles`` read as the save's spelling and are written as the file had them."""
    short = {"nodes": [_node(1, "manual_input", input_ids=[], outputs=[2]), _node(2, input_ids=[1], outputs=[])]}
    full = {
        "nodes": [
            _node(1, "manual_input", input_ids=None, outputs=[2], output_handles=["output-0"]),
            _node(2, input_ids=[1], outputs=[], output_handles=[]),
        ]
    }
    short_flow, _ = canonical(short)
    full_flow, _ = canonical(full)
    assert short_flow.inputs == full_flow.inputs
    assert emit(short_flow) == short
    assert emit(full_flow) == full


def test_keyed_inputs_round_trip():
    """A step with keyed inputs (Run Flow) keeps them in ``input_connections``, each with its own source handle."""
    connections = [
        {"from_id": 1, "input_handle": "input-1", "source_handle": "output-0"},
        {"from_id": 2, "input_handle": "input-2", "source_handle": "output-1"},
    ]
    document = {
        "nodes": [
            _node(1, "manual_input", outputs=[3], output_handles=["output-0"], input_connections=None),
            _node(2, "random_split", outputs=[3], output_handles=["output-1"], input_connections=None),
            _node(3, "run_flow", input_ids=None, outputs=[], output_handles=[], input_connections=connections),
        ]
    }
    flow, issues = canonical(document)
    assert issues == []
    assert flow.inputs[3] == {"input-1": ((1, "output-0"),), "input-2": ((2, "output-1"),)}
    assert flow.keyed == {3}
    assert emit(flow) == document


def test_outputs_are_written_sorted_by_target_and_handle():
    document = {
        "nodes": [
            _node(1, "random_split", outputs=[], output_handles=[]),
            _node(3, input_ids=[1], outputs=[]),
            _node(2, "join", input_ids=[1], right_input_id=1, outputs=[]),
        ]
    }
    flow, _ = canonical(document)
    flow.inputs[2] = {MAIN: ((1, "output-1"),), RIGHT: ((1, "output-0"),)}
    out = emit(flow)["nodes"][0]
    assert (out["outputs"], out["output_handles"]) == ([2, 2, 3], ["output-0", "output-1", "output-0"])


def test_parameters_are_read_by_name_and_written_in_place():
    document = {
        "flowfile_settings": {"description": None, "parameters": [{"name": "a", "default_value": "1"}], "auto_save": False},
        "nodes": [],
    }
    flow, _ = canonical(document)
    assert list(flow.parameters) == ["a"]
    flow.parameters["b"] = {"name": "b", "default_value": "2"}
    settings = emit(flow)["flowfile_settings"]
    assert list(settings) == ["description", "parameters", "auto_save"]
    assert [p["name"] for p in settings["parameters"]] == ["a", "b"]
