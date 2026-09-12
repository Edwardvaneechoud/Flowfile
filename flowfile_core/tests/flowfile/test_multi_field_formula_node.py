"""Graph-level integration tests for the multi_field_formula node.

Run with:
    pytest flowfile_core/tests/flowfile/test_multi_field_formula_node.py -v
"""
from typing import Literal

from flowfile_core.configs.node_store import node_dict
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.routes.routes import get_node_model
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.output_model import RunInformation
from flowfile_core.schemas.schemas import NODE_TYPE_TO_SETTINGS_CLASS


def create_graph(flow_id: int = 1, execution_mode: Literal["Development", "Performance"] = "Development") -> FlowGraph:
    """Create a new FlowGraph for testing."""
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(flow_id=flow_id, name="test_flow", path=".", execution_mode=execution_mode)
    )
    return handler.get_flow(flow_id)


def make_raw_data(schema: dict[str, str], columns: dict[str, list]) -> input_schema.RawData:
    """Build RawData with explicit dtypes.

    `RawData.from_pylist` stringifies any column mixing nulls with numbers, which would
    hide the dtype-driven selection modes under test, so the dtypes are pinned here instead.
    """
    return input_schema.RawData(
        columns=[input_schema.MinimalFieldInfo(name=name, data_type=dtype) for name, dtype in schema.items()],
        data=[columns[name] for name in schema],
    )


def add_manual_input(graph: FlowGraph, raw_data: input_schema.RawData, node_id: int = 1) -> FlowGraph:
    """Add a manual input node with data."""
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input")
    )
    graph.add_manual_input(
        input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=node_id, raw_data_format=raw_data)
    )
    return graph


def add_node_promise(graph: FlowGraph, node_type: str, node_id: int) -> None:
    """Add a node promise."""
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type=node_type))


def handle_run_info(run_info: RunInformation) -> None:
    """Check run info for errors."""
    if run_info is None:
        raise ValueError("Run info is None")
    if not run_info.success:
        errors = "errors:"
        for node_step in run_info.node_step_result:
            if not node_step.success:
                errors += f"\n node_id:{node_step.node_id}, error: {node_step.error}"
        raise ValueError(f"Graph should run successfully:\n{errors}")


MONTH_SCHEMA = {"jan": "Float64", "feb": "Float64", "Total": "Float64"}
MONTH_COLUMNS = {"jan": [10.0, 20.0], "feb": [30.0, 40.0], "Total": [100.0, 200.0]}


def month_raw_data() -> input_schema.RawData:
    return make_raw_data(MONTH_SCHEMA, MONTH_COLUMNS)


def build_multi_field_graph(raw_data: input_schema.RawData, **formula_kwargs) -> FlowGraph:
    """Wire manual_input -> multi_field_formula and configure the formula node."""
    graph = create_graph()
    add_manual_input(graph, raw_data, node_id=1)
    add_node_promise(graph, "multi_field_formula", node_id=2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_multi_field_formula(
        input_schema.NodeMultiFieldFormula(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            multi_field_formula_input=transform_schema.MultiFieldFormulaInput(**formula_kwargs),
        )
    )
    return graph


def run_and_collect(graph: FlowGraph, node_id: int = 2) -> list[dict]:
    run_info = graph.run_graph()
    handle_run_info(run_info)
    return graph.get_node(node_id).get_resulting_data().collect().to_dicts()


# Registry wiring


def test_node_template_is_registered_in_the_palette():
    template = node_dict["multi_field_formula"]
    assert template.name == "Multi-field formula"
    assert template.item == "multi_field_formula"
    assert (template.input, template.output) == (1, 1)
    assert template.node_group == "transform"
    assert template.node_type == "process"
    assert template.transform_type == "narrow"
    assert template.image == "multi_field_formula.svg"
    assert template.laziness == "lazy"
    assert template.drawer_title == "Multi-Field Formula"
    assert template.drawer_intro == "Apply one expression to many columns at once"
    assert template.multi is False


def test_generic_settings_dispatch_names_resolve():
    """`/update_settings/?node_type=multi_field_formula` resolves by string convention."""
    graph = create_graph()
    node_type = "multi_field_formula"
    assert callable(getattr(graph, f"add_{node_type}"))
    assert get_node_model("node" + node_type.replace("_", "")) is input_schema.NodeMultiFieldFormula
    assert NODE_TYPE_TO_SETTINGS_CLASS[node_type] is input_schema.NodeMultiFieldFormula


# Execution


def test_replace_mode_runs_end_to_end():
    # `Total` is processed FIRST on purpose: a per-column loop would divide by the rewritten value.
    graph = build_multi_field_graph(
        month_raw_data(),
        formula="[_CurrentField_] / [Total] * 100",
        selection_mode="list",
        selected_columns=["Total", "jan", "feb"],
    )
    assert run_and_collect(graph) == [
        {"jan": 10.0, "feb": 30.0, "Total": 100.0},
        {"jan": 10.0, "feb": 20.0, "Total": 100.0},
    ]


def test_new_mode_with_suffix_runs_end_to_end():
    graph = build_multi_field_graph(
        month_raw_data(),
        formula="[_CurrentField_] / [Total] * 100",
        selection_mode="list",
        selected_columns=["jan", "feb"],
        output_mode="new",
        output_suffix=" % Total",
        output_data_type="Float64",
    )
    assert run_and_collect(graph) == [
        {"jan": 10.0, "feb": 30.0, "Total": 100.0, "jan % Total": 10.0, "feb % Total": 30.0},
        {"jan": 20.0, "feb": 40.0, "Total": 200.0, "jan % Total": 10.0, "feb % Total": 20.0},
    ]


def test_data_type_mode_with_placeholders_runs_end_to_end():
    graph = build_multi_field_graph(
        make_raw_data(
            {"code": "String", "label": "String", "qty": "Int64"},
            {"code": ["a"], "label": ["b"], "qty": [7]},
        ),
        formula='uppercase([_CurrentField_]) + "|" + [_CurrentFieldName_] + "|" + [_CurrentFieldType_]',
        selection_mode="data_type",
        selected_data_type="String",
        output_mode="new",
        output_prefix="meta_",
    )
    assert run_and_collect(graph) == [
        {
            "code": "a",
            "label": "b",
            "qty": 7,
            "meta_code": "A|code|String",
            "meta_label": "B|label|String",
        }
    ]


# Schema prediction


def test_predicted_schema_shows_new_and_cast_columns_before_running():
    graph = build_multi_field_graph(
        month_raw_data(),
        formula="[_CurrentField_] / [Total] * 100",
        selection_mode="list",
        selected_columns=["jan", "feb"],
        output_mode="new",
        output_suffix="_pct",
        output_data_type="String",
    )
    predicted = graph.get_node(2).get_predicted_schema()
    assert [(c.column_name, c.data_type) for c in predicted] == [
        ("jan", "Float64"),
        ("feb", "Float64"),
        ("Total", "Float64"),
        ("jan_pct", "String"),
        ("feb_pct", "String"),
    ]


def test_predicted_schema_for_replace_mode_keeps_the_column_order():
    graph = build_multi_field_graph(
        month_raw_data(),
        formula="[_CurrentField_]",
        selection_mode="all",
        output_data_type="Int64",
    )
    predicted = graph.get_node(2).get_predicted_schema()
    assert [(c.column_name, c.data_type) for c in predicted] == [
        ("jan", "Int64"),
        ("feb", "Int64"),
        ("Total", "Int64"),
    ]


# Settings persistence


def test_node_settings_round_trip_and_description():
    graph = build_multi_field_graph(
        month_raw_data(),
        formula="[_CurrentField_] / [Total] * 100",
        selection_mode="list",
        selected_columns=["jan", "feb"],
        output_mode="new",
        output_suffix=" % Total",
    )
    settings = graph.get_node(2).setting_input
    assert isinstance(settings, input_schema.NodeMultiFieldFormula)
    assert settings.multi_field_formula_input.selected_columns == ["jan", "feb"]
    assert settings.get_default_description() == (
        "[_CurrentField_] / [Total] * 100 on 2 column(s) → * % Total"
    )


def test_multi_field_formula_yaml_roundtrip(tmp_path):
    graph = build_multi_field_graph(
        month_raw_data(),
        formula="[_CurrentField_] / [Total] * 100",
        selection_mode="data_type",
        selected_data_type="Numeric",
        output_mode="new",
        output_prefix="pct_",
        output_data_type="Float64",
    )
    yaml_path = tmp_path / "multi_field_formula.yaml"
    graph.save_flow(str(yaml_path))

    loaded = open_flow(yaml_path)
    loaded_settings = loaded.get_node(2).setting_input
    assert isinstance(loaded_settings, input_schema.NodeMultiFieldFormula)
    assert loaded_settings.multi_field_formula_input == graph.get_node(2).setting_input.multi_field_formula_input
    assert run_and_collect(loaded) == [
        {"jan": 10.0, "feb": 30.0, "Total": 100.0, "pct_jan": 10.0, "pct_feb": 30.0, "pct_Total": 100.0},
        {"jan": 20.0, "feb": 40.0, "Total": 200.0, "pct_jan": 10.0, "pct_feb": 20.0, "pct_Total": 100.0},
    ]


# Chaining


def test_chained_with_dynamic_rename():
    """A rename upstream changes which columns the formula's list selection matches."""
    graph = create_graph()
    add_manual_input(graph, month_raw_data(), node_id=1)
    add_node_promise(graph, "dynamic_rename", node_id=2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_dynamic_rename(
        input_schema.NodeDynamicRename(
            flow_id=graph.flow_id,
            node_id=2,
            depending_on_id=1,
            dynamic_rename_input=transform_schema.DynamicRenameInput(rename_mode="prefix", prefix="m_"),
        )
    )
    add_node_promise(graph, "multi_field_formula", node_id=3)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    graph.add_multi_field_formula(
        input_schema.NodeMultiFieldFormula(
            flow_id=graph.flow_id,
            node_id=3,
            depending_on_id=2,
            multi_field_formula_input=transform_schema.MultiFieldFormulaInput(
                formula="[_CurrentField_] / [m_Total] * 100",
                selection_mode="list",
                selected_columns=["m_jan", "m_feb", "jan"],
                output_mode="new",
                output_suffix="_pct",
            ),
        )
    )
    # "jan" no longer exists after the rename and is silently skipped.
    assert run_and_collect(graph, node_id=3) == [
        {"m_jan": 10.0, "m_feb": 30.0, "m_Total": 100.0, "m_jan_pct": 10.0, "m_feb_pct": 30.0},
        {"m_jan": 20.0, "m_feb": 40.0, "m_Total": 200.0, "m_jan_pct": 10.0, "m_feb_pct": 20.0},
    ]
