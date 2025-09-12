import polars as pl
import pytest

from flowfile_core.configs.node_store import register_custom_node, CUSTOM_NODE_STORE, nodes_list
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.node_designer.custom_node import (
    CustomNodeBase,
    NodeSettings
)

from flowfile_core.flowfile.node_designer.ui_components import (
    TextInput,
    Section
)
from flowfile_core.schemas import input_schema, schemas


def create_flowfile_handler():
    from flowfile_core.flowfile.handler import FlowfileHandler
    handler = FlowfileHandler()
    assert handler._flows == {}, 'Flow should be empty'
    return handler


def create_graph(flow_id: int = 1, execution_mode: str = 'Development') -> FlowGraph:
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name=f'flow_{flow_id}', path='.',
                                               execution_mode=execution_mode))
    graph = handler.get_flow(flow_id)
    return graph


def add_manual_input(graph: FlowGraph, data, node_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=graph.flow_id, node_id=node_id,
                                              raw_data_format=input_schema.RawData.from_pylist(data))
    graph.add_manual_input(input_file)
    return graph


def add_node_promise_on_type(graph: FlowGraph, node_type: str, node_id: int, flow_id: int = None):
    if flow_id is None:
        flow_id = graph.flow_id
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type=node_type)
    graph.add_node_promise(node_promise)


@pytest.fixture
def UserDefinedNode():

    class FixedColumn(CustomNodeBase):
        """
        A custom node that adds a new column with a fixed, user-defined value.
        """
        # --- Node Metadata ---
        node_name: str = "Fixed Column"
        node_group: str = "custom"
        intro: str = "Adds a new column with a fixed value you provide."
        title: str = "Fixed Column"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

        # --- UI Definition ---
        # The UI is defined declaratively using the custom Section and NodeSettings classes.
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(
                title="Configuration",
                standard_input=TextInput(
                    label="Fixed Value",
                    placeholder="Enter the value to set..."
                ),
                column_name=TextInput(
                    label="New Column Name",
                    placeholder="Enter the output column name"
                )
            ),
        )

        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            """
            The core processing logic for the node.
            """
            if not inputs:
                return pl.DataFrame()

            input_df = inputs[0]

            # Access settings in a type-safe way
            fixed_value = self.settings_schema.main_section.standard_input.value
            new_col_name = self.settings_schema.main_section.column_name.value

            # Ensure both values are set before proceeding
            if fixed_value is None or not new_col_name:
                return input_df

            return input_df.with_columns(
                pl.lit(fixed_value).alias(new_col_name)
            )
    return FixedColumn


def test_custom_node_registration(UserDefinedNode):
    register_custom_node(UserDefinedNode().to_node_template())
    assert UserDefinedNode().item in CUSTOM_NODE_STORE
    assert any(n.name == UserDefinedNode().node_name for n in nodes_list)


def test_custom_node_in_graph(UserDefinedNode):
    settings = {
            "main_section": {
                "standard_input": "hello from test",
                "column_name": "new_col"
            }
        }
    register_custom_node(UserDefinedNode().to_node_template())
    graph = create_graph()
    add_manual_input(graph, [{"A": 1}, {"A": 2}], node_id=1)
    add_node_promise_on_type(graph, UserDefinedNode().item, node_id=2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    user_defined_node = UserDefinedNode.from_settings(settings)

    node_settings = input_schema.UserDefinedNode(flow_id=graph.flow_id, node_id=2, settings=settings, is_user_defined=True)

    graph.add_user_defined_node(custom_node=user_defined_node, user_defined_node_settings=node_settings)

    run_result = graph.run_graph()
    assert run_result.success
    expected_data = FlowDataEngine({"A": [1, 2], "new_col": ["hello from test", "hello from test"]})
    expected_data.assert_equal(graph.get_node(2).get_resulting_data())
