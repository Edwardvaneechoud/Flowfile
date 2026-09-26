"""Flow parameters in custom node settings reach ``process()`` resolved, typed by the component.

The in-core path builds its instance from the settings as they are when the node executes, so a
``${name}`` reference is substituted by the run loop first; a ``NumericInput`` or ``ToggleSwitch``
then turns the substituted text into a number or a boolean.
"""

from copy import deepcopy

import polars as pl
import pytest

from flowfile_core.configs import node_store
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.node_designer import (
    CustomNodeBase,
    NodeSettings,
    NumericInput,
    Section,
    SliderInput,
    TextInput,
    ToggleSwitch,
)
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.schemas import FlowParameter

SEEN: list[tuple] = []


class ParamProbeNode(CustomNodeBase):
    node_name: str = "Param Probe Node"
    node_category: str = "Testing"
    settings_schema: NodeSettings = NodeSettings(
        main=Section(
            title="Main",
            column=TextInput(label="Column", default="name"),
            factor=NumericInput(label="Factor", default=1),
            upper=ToggleSwitch(label="Upper"),
        ),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        main = self.settings_schema.main
        SEEN.append((main.column.value, main.factor.value, main.upper.value))
        text = pl.col(main.column.value)
        if main.upper.value is True:
            text = text.str.to_uppercase()
        return inputs[0].with_columns(text, (pl.col("amount") * main.factor.value).alias("scaled"))


SETTINGS = {"main": {"column": "${col}", "factor": "${factor}", "upper": "${upper}"}}


class ParamHookNode(CustomNodeBase):
    node_name: str = "Param Hook Node"
    node_category: str = "Testing"
    settings_schema: NodeSettings = NodeSettings(
        main=Section(title="Main", output=TextInput(label="Output column", default="result")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.col("amount").alias(self.settings_schema.main.output.value))

    def predict_output_schema(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.lit(0).alias(self.settings_schema.main.output.value))


HOOK_SETTINGS = {"main": {"output": "${out}"}}


@pytest.fixture(autouse=True)
def registered_probe():
    SEEN.clear()
    node_store.add_to_custom_node_store(ParamProbeNode)
    node_store.add_to_custom_node_store(ParamHookNode)
    yield
    node_store.remove_from_custom_node_store(ParamProbeNode().item)
    node_store.remove_from_custom_node_store(ParamHookNode().item)


def _graph(
    flow_id: int,
    parameters: list[FlowParameter],
    node_class: type[CustomNodeBase] = ParamProbeNode,
    settings: dict = SETTINGS,
) -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id, name="udn_params", path=".", execution_mode="Development", execution_location="local"
        )
    )
    graph = handler.get_flow(flow_id)
    graph.flow_settings.parameters = parameters
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(
                [{"name": "ann", "amount": 2}, {"name": "bob", "amount": 5}]
            ),
        )
    )
    node_type = node_class().item
    graph.add_node_promise(
        input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type=node_type, is_user_defined=True)
    )
    graph.add_user_defined_node(
        custom_node=node_class.from_settings(settings),
        user_defined_node_settings=input_schema.UserDefinedNode(
            flow_id=flow_id, node_id=2, settings=deepcopy(settings), is_user_defined=True, depending_on_ids=[1]
        ),
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return graph


def _run(graph: FlowGraph) -> pl.DataFrame:
    result = graph.run_graph()
    assert result.success, [r.error for r in result.node_step_result if not r.success]
    return graph.get_node(2).get_resulting_data().collect()


def test_string_parameters_reach_process_resolved_and_typed_by_the_component():
    graph = _graph(
        8101,
        [
            FlowParameter(name="col", default_value="name"),
            FlowParameter(name="factor", default_value="3"),
            FlowParameter(name="upper", default_value="true"),
        ],
    )

    out = _run(graph)

    assert SEEN[-1] == ("name", 3, True)
    assert out["name"].to_list() == ["ANN", "BOB"]
    assert out["scaled"].to_list() == [6, 15]
    assert graph.get_node(2).setting_input.settings == SETTINGS


def test_typed_parameters_and_a_changed_value_are_read_when_the_node_runs():
    graph = _graph(
        8102,
        [
            FlowParameter(name="col", default_value="name"),
            FlowParameter(name="factor", default_value="2.5", type="float"),
            FlowParameter(name="upper", default_value="false", type="boolean"),
        ],
    )
    assert _run(graph)["scaled"].to_list() == [5.0, 12.5]
    assert SEEN[-1] == ("name", 2.5, False)

    next(p for p in graph.flow_settings.parameters if p.name == "factor").default_value = "4"
    graph.get_node(2).reset(deep=True)
    out = _run(graph)

    assert SEEN[-1] == ("name", 4.0, False)
    assert out["scaled"].to_list() == [8.0, 20.0]
    assert out["name"].to_list() == ["ann", "bob"]


def test_the_schema_hook_predicts_with_parameters_resolved():
    graph = _graph(8103, [FlowParameter(name="out", default_value="total")], ParamHookNode, HOOK_SETTINGS)
    node = graph.get_node(2)

    assert [column.column_name for column in node.get_predicted_schema()] == ["name", "amount", "total"]
    assert node.setting_input.settings == HOOK_SETTINGS
    assert _run(graph).columns == ["name", "amount", "total"]


@pytest.mark.parametrize(
    "component, raw, expected",
    [
        (NumericInput(label="n"), "3", 3),
        (NumericInput(label="n"), " 2.5 ", 2.5),
        (NumericInput(label="n"), "${factor}", "${factor}"),
        (NumericInput(label="n"), "", ""),
        (NumericInput(label="n"), 7, 7),
        (SliderInput(label="s"), "40", 40),
        (ToggleSwitch(label="t"), "TRUE", True),
        (ToggleSwitch(label="t"), "false", False),
        (ToggleSwitch(label="t"), "${upper}", "${upper}"),
        (ToggleSwitch(label="t"), True, True),
    ],
)
def test_number_and_toggle_components_coerce_text(component, raw, expected):
    assert component.set_value(raw).value == expected
    assert type(component.value) is type(expected)
