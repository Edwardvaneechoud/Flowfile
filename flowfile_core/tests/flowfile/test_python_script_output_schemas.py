"""Declared per-output schemas on python_script nodes: validation, persistence, prediction, hashing.

Nothing here runs a kernel: prediction must come from the declaration alone.
"""

import pytest
from pydantic import ValidationError

from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema

MAIN_COLUMNS = [
    input_schema.MinimalFieldInfo(name="id", data_type="Int64"),
    input_schema.MinimalFieldInfo(name="score", data_type="Float64"),
]
EXTRA_COLUMNS = [input_schema.MinimalFieldInfo(name="label", data_type="String")]


def _graph_with_script(output_names=("main",), output_schemas=None, kernel_id="k1") -> FlowGraph:
    graph = FlowGraph()
    flow_id = graph.flow_id
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist([{"a": 1, "b": "x"}]),
        )
    )
    graph.add_python_script(
        input_schema.NodePythonScript(
            flow_id=flow_id,
            node_id=2,
            depending_on_ids=[1],
            python_script_input=input_schema.PythonScriptInput(code="pass", kernel_id=kernel_id),
            output_names=list(output_names),
            output_schemas=output_schemas,
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return graph


def _names(columns) -> list[str]:
    return [c.column_name for c in columns]


def test_output_schemas_defaults_to_none_for_legacy_settings():
    settings = input_schema.NodePythonScript.model_validate({"flow_id": 1, "node_id": 2})
    assert settings.output_schemas is None


def test_output_schemas_rejects_unknown_output():
    with pytest.raises(ValidationError, match="unknown outputs"):
        input_schema.NodePythonScript(flow_id=1, node_id=2, output_names=["main"], output_schemas={"other": []})


def test_output_schemas_rejects_duplicate_columns():
    with pytest.raises(ValidationError, match="duplicate column names"):
        input_schema.NodePythonScript(flow_id=1, node_id=2, output_schemas={"main": [MAIN_COLUMNS[0], MAIN_COLUMNS[0]]})


def test_output_schemas_round_trip_through_yaml(tmp_path):
    declared = {"main": MAIN_COLUMNS, "extra": EXTRA_COLUMNS}
    graph = _graph_with_script(output_names=("main", "extra"), output_schemas=declared)
    path = tmp_path / "script_flow.yaml"
    graph.save_flow(str(path))

    reopened = open_flow(path)
    settings = reopened.get_node(2).setting_input
    assert isinstance(settings, input_schema.NodePythonScript)
    assert settings.output_schemas == declared
    assert settings.output_names == ["main", "extra"]


def test_legacy_yaml_without_output_schemas_opens(tmp_path):
    graph = _graph_with_script()
    path = tmp_path / "legacy_flow.yaml"
    graph.save_flow(str(path))
    text = path.read_text()
    path.write_text("\n".join(line for line in text.splitlines() if "output_schemas" not in line))

    reopened = open_flow(path)
    assert reopened.get_node(2).setting_input.output_schemas is None


def test_prediction_uses_declared_schemas_per_output():
    graph = _graph_with_script(
        output_names=("main", "extra", "rest"), output_schemas={"main": MAIN_COLUMNS, "extra": EXTRA_COLUMNS}
    )
    node = graph.get_node(2)

    assert _names(node.schema) == ["id", "score"]
    assert [c.data_type for c in node.schema] == ["Int64", "Float64"]
    assert _names(node.schema_for_handle("output-1")) == ["label"]
    # An undeclared output keeps the pass-through default.
    assert _names(node.schema_for_handle("output-2")) == ["a", "b"]


def test_prediction_without_declaration_passes_input_through():
    node = _graph_with_script().get_node(2)
    assert _names(node.schema) == ["a", "b"]


def test_editing_output_schemas_refreshes_prediction_downstream():
    graph = _graph_with_script(output_schemas={"main": MAIN_COLUMNS})
    graph.add_select(
        input_schema.NodeSelect(flow_id=graph.flow_id, node_id=3, depending_on_id=2, select_input=[], keep_missing=True)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    assert _names(graph.get_node(3).schema) == ["id", "score"]

    settings = graph.get_node(2).setting_input.model_copy(update={"output_schemas": {"main": EXTRA_COLUMNS}})
    graph.add_python_script(settings)

    assert _names(graph.get_node(2).schema) == ["label"]
    assert _names(graph.get_node(3).schema) == ["label"]


def test_output_schemas_do_not_change_the_node_hash():
    graph = _graph_with_script()
    node = graph.get_node(2)
    before = node.hash

    declared = node.setting_input.model_copy(update={"output_schemas": {"main": MAIN_COLUMNS}})
    assert node.calculate_hash(declared) == before
    graph.add_python_script(declared)
    assert graph.get_node(2).hash == before

    # A real settings change still moves the hash.
    changed_code = declared.model_copy(
        update={"python_script_input": input_schema.PythonScriptInput(code="x = 1", kernel_id="k1")}
    )
    assert node.calculate_hash(changed_code) != before


def test_hash_of_settings_without_exclusions_is_unchanged():
    from flowfile_core.flowfile.flow_node.flow_node import _settings_for_hash

    settings = input_schema.NodeSelect(flow_id=1, node_id=3, select_input=[])
    assert _settings_for_hash(settings) is settings
