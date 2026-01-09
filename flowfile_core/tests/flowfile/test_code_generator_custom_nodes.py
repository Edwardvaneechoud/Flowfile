"""
Tests for code generation of custom/user-defined nodes.
Tests that generated code includes proper imports, settings classes, and correct process invocations.
"""

import polars as pl
import pytest

from flowfile_core.configs.node_store import CUSTOM_NODE_STORE, add_to_custom_node_store
from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars, FlowGraphToPolarsConverter
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.node_designer import (
    ColumnSelector,
    CustomNodeBase,
    NodeSettings,
    Section,
    TextInput,
)
from flowfile_core.schemas import input_schema, schemas


def create_flowfile_handler() -> FlowfileHandler:
    handler = FlowfileHandler()
    return handler


def create_graph(flow_id: int = 1, execution_mode: str = 'Development') -> FlowGraph:
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(
        flow_id=flow_id,
        name=f'flow_{flow_id}',
        path='.',
        execution_mode=execution_mode
    ))
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1) -> FlowGraph:
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type='manual_input'
    )
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(data)
    )
    graph.add_manual_input(input_file)
    return graph


def add_custom_node_to_graph(
        graph: FlowGraph,
        custom_node_class: type,
        node_id: int,
        settings: dict
) -> FlowGraph:
    """Helper to add a custom node to a graph with settings."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type=custom_node_class().item,
        is_user_defined=True
    )
    graph.add_node_promise(node_promise)
    user_defined_node = custom_node_class.from_settings(settings)
    node_settings = input_schema.UserDefinedNode(
        flow_id=graph.flow_id,
        node_id=node_id,
        settings=settings,
        is_user_defined=True
    )
    graph.add_user_defined_node(
        custom_node=user_defined_node,
        user_defined_node_settings=node_settings
    )
    return graph


@pytest.fixture
def AddColumnNode():
    """A custom node that adds a column with a fixed value."""

    class AddColumnSettings(NodeSettings):
        config: Section = Section(
            title="Configuration",
            column_name=TextInput(
                label="New Column Name",
                default="new_column",
            ),
            fixed_value=TextInput(
                label="Fixed Value",
                default="default_value",
            ),
        )

    class AddColumn(CustomNodeBase):
        node_name: str = "Add Column"
        node_category: str = "Transform"
        title: str = "Add Fixed Column"
        intro: str = "Adds a new column with a fixed value"
        number_of_inputs: int = 1
        number_of_outputs: int = 1
        settings_schema: AddColumnSettings = AddColumnSettings()

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            lf = inputs[0]
            col_name = self.settings_schema.config.column_name.value
            value = self.settings_schema.config.fixed_value.value
            return lf.with_columns(pl.lit(value).alias(col_name))

    return AddColumn


class TestCustomNodeCodeGeneration:
    """Integration tests for generating code with custom nodes."""

    def test_generated_code_includes_custom_node_imports(self, AddColumnNode):
        """Test that generated code includes necessary imports."""
        add_to_custom_node_store(AddColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)

        settings = {"config": {"column_name": "new_col", "fixed_value": "hello"}}
        add_custom_node_to_graph(graph, AddColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)

        assert "from flowfile_core.flowfile.node_designer import" in code
        assert "CustomNodeBase" in code

    def test_generated_code_no_unnecessary_collect_lazy(self, AddColumnNode):
        """Test that LazyFrame-based process methods don't have unnecessary collect/lazy."""
        add_to_custom_node_store(AddColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)

        settings = {"config": {"column_name": "new_col", "fixed_value": "hello"}}
        add_custom_node_to_graph(graph, AddColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)

        # Since AddColumn.process accepts LazyFrame and returns LazyFrame,
        # there should be no .collect() or .lazy() in the process call
        lines = code.split('\n')
        process_lines = [l for l in lines if '_custom_node_' in l and '.process(' in l]

        for line in process_lines:
            assert '.collect()' not in line, f"Should not have .collect() for LazyFrame process: {line}"
            assert '.lazy()' not in line, f"Should not have .lazy() for LazyFrame process: {line}"


class TestProcessMethodSignatureDetection:
    """Tests for detecting process method signatures."""

    def test_detects_lazyframe_input(self, AddColumnNode):
        """Test that LazyFrame input types are detected."""
        add_to_custom_node_store(AddColumnNode)

        graph = create_graph()
        converter = FlowGraphToPolarsConverter(graph)

        needs_collect, needs_lazy = converter._check_process_method_signature(AddColumnNode)

        # AddColumn.process accepts *inputs: pl.LazyFrame and returns pl.LazyFrame
        assert needs_collect is False, "Should detect LazyFrame input"
        assert needs_lazy is False, "Should detect LazyFrame output"

    def test_detects_dataframe_input(self):
        """Test that DataFrame input types are detected (default behavior)."""

        class DataFrameNode(CustomNodeBase):
            node_name: str = "DataFrame Node"
            node_category: str = "Test"
            number_of_inputs: int = 1
            number_of_outputs: int = 1

            def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
                return inputs[0]

        add_to_custom_node_store(DataFrameNode)

        graph = create_graph()
        converter = FlowGraphToPolarsConverter(graph)

        needs_collect, needs_lazy = converter._check_process_method_signature(DataFrameNode)

        # Should need collect and lazy for DataFrame-based process
        assert needs_collect is True, "Should need collect for DataFrame input"
        assert needs_lazy is True, "Should need lazy for DataFrame output"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
