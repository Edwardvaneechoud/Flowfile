"""Declarative ports serialize from existing template definitions."""

import pytest

from flowfile_core.configs.node_store.nodes import get_all_standard_nodes
from flowfile_core.schemas.schemas import NodeInput, NodeTemplate


@pytest.fixture
def standard_nodes():
    return get_all_standard_nodes()[1]


def test_effective_ports_preserve_union_and_optional_control_contracts(standard_nodes):
    union = standard_nodes["union"]
    assert [(p.id, p.multiple, p.required) for p in union.inputs] == [("input-0", True, True)]
    gate = standard_nodes["gate"]
    assert [(p.kind, p.required) for p in gate.inputs] == [("data", True), ("parameter", False)]
    assert [p.name for p in gate.inputs] == gate.input_labels


def test_dynamic_instance_serialization_recomputes_without_mutating_palette(standard_nodes):
    palette = standard_nodes["run_flow"]
    assert [(p.id, p.hidden) for p in palette.inputs] == [("input-0", True)]
    assert palette.outputs == []
    node = NodeInput(**palette.__dict__, id=7, pos_x=0, pos_y=0, input_names=["", "Orders"])
    node.output_names = ["Result", "Rejected"]
    data = node.model_dump(mode="json")
    assert data["inputs"] == [
        {"id": "input-0", "name": "", "kind": "parameter", "required": False, "multiple": False, "hidden": True},
        {"id": "input-1", "name": "Orders", "kind": "data", "required": False, "multiple": False, "hidden": False},
    ]
    assert [p["id"] for p in data["outputs"]] == ["output-0", "output-1"]
    copied = node.model_copy(update={"input_names": ["Parameters", "Customers"]})
    assert [(p.id, p.kind) for p in copied.inputs] == [("input-0", "parameter"), ("input-1", "data")]
    assert [(p.id, p.hidden) for p in palette.inputs] == [("input-0", True)]
    assert palette.outputs == []


def test_standard_definitions_are_not_mutated_by_port_serialization(standard_nodes):
    from copy import deepcopy

    for node in standard_nodes.values():
        before = deepcopy(node.__dict__)
        data = node.model_dump(mode="json")
        assert node.__dict__ == before, node.item
        assert [p["id"] for p in data["inputs"]] == (
            ["input-0"] if node.dynamic_inputs else [f"input-{i}" for i in range(1 if node.multi else node.input)]
        ), node.item
        assert [p["id"] for p in data["outputs"]] == (
            []
            if node.dynamic_inputs
            else [f"output-{i}" for i in range(max(node.output, len(node.output_names or [])))]
        ), node.item
        # Stale or client-supplied descriptors cannot override the original definition.
        restored = NodeTemplate.model_validate(data | {"inputs": [], "outputs": []})
        assert restored.inputs == node.inputs
        assert restored.outputs == node.outputs
