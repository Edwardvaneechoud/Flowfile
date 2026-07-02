"""Unit tests for keyed dynamic-input handle plumbing (input_handles + NodeStepInputs)."""

import pytest

from flowfile_core.flowfile.flow_node.input_handles import (
    PARAM_INPUT_HANDLE,
    input_handle,
    input_handle_index,
)
from flowfile_core.flowfile.flow_node.models import NodeStepInputs


class _StubNode:
    def __init__(self, node_id: int):
        self.node_id = node_id

    def __repr__(self):
        return f"_StubNode({self.node_id})"


def test_input_handle_roundtrip():
    assert input_handle(0) == PARAM_INPUT_HANDLE
    assert input_handle(3) == "input-3"
    assert input_handle_index("input-0") == 0
    assert input_handle_index("input-12") == 12


@pytest.mark.parametrize("bad", ["output-0", "input-", "input-x", "input--1", "main"])
def test_input_handle_index_rejects_invalid(bad):
    with pytest.raises(ValueError):
        input_handle_index(bad)


def test_keyed_connection_exists():
    inputs = NodeStepInputs()
    assert not inputs.keyed_connection_exists("input-1", 5)
    inputs.keyed_inputs = {"input-1": _StubNode(5)}
    assert inputs.keyed_connection_exists("input-1", 5)
    assert not inputs.keyed_connection_exists("input-1", 6)
    assert not inputs.keyed_connection_exists("input-2", 5)


def test_slot_items_ordered_by_handle_index():
    inputs = NodeStepInputs()
    inputs.keyed_inputs = {
        "input-10": _StubNode(3),
        "input-0": _StubNode(1),
        "input-2": _StubNode(2),
    }
    assert [h for h, _ in inputs.slot_items()] == ["input-0", "input-2", "input-10"]


def test_rebuild_keyed_projection_is_slot_ordered():
    inputs = NodeStepInputs()
    a, b = _StubNode(1), _StubNode(2)
    inputs.keyed_inputs = {"input-2": b, "input-1": a}
    inputs.rebuild_keyed_projection()
    assert inputs.main_inputs == [a, b]
    # same source on two handles appears twice in the projection
    inputs.keyed_inputs["input-3"] = a
    inputs.rebuild_keyed_projection()
    assert inputs.main_inputs == [a, b, a]
