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
    IncomingColumns,
    NodeSettings,
    NumericInput,
    Section,
    SingleSelect,
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


# =============================================================================
# Test Fixtures - Custom Nodes
# =============================================================================

@pytest.fixture
def FilterColumnNode():
    """A simple custom node that filters to a single column."""

    class FilterColumnSettings(NodeSettings):
        main_section: Section = Section(
            title="Column Selection",
            select_column=ColumnSelector(
                label="Select Column",
                required=True,
                multiple=False
            )
        )

    class FilterColumn(CustomNodeBase):
        node_name: str = "Filter column"
        node_category: str = "Transform"
        node_icon: str = "filter-svgrepo-com.svg"
        title: str = "Select one column"
        intro: str = "Select one column"
        number_of_inputs: int = 1
        number_of_outputs: int = 1
        settings_schema: FilterColumnSettings = FilterColumnSettings()

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            select_column: str = self.settings_schema.main_section.select_column.value
            lf = inputs[0]
            return lf.select(pl.col(select_column))

    return FilterColumn


@pytest.fixture
def AddColumnNode():
    """A custom node that adds a column with a fixed value."""

    class AddColumnSettings(NodeSettings):
        config: Section = Section(
            title="Configuration",
            column_name=TextInput(
                label="New Column Name",
                default="new_column",
                placeholder="Enter column name"
            ),
            fixed_value=TextInput(
                label="Fixed Value",
                default="default_value",
                placeholder="Enter value"
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


# =============================================================================
# Unit Tests for Settings Schema Extraction
# =============================================================================

class TestSettingsSchemaExtraction:
    """Tests for extracting/generating settings schema class definitions."""

    def test_extract_settings_schema_class_returns_code(self, FilterColumnNode):
        """Test that _extract_settings_schema_class returns valid code."""
        add_to_custom_node_store(FilterColumnNode)

        graph = create_graph()
        converter = FlowGraphToPolarsConverter(graph)

        result = converter._extract_settings_schema_class(FilterColumnNode)

        assert result is not None, "Should return settings schema code"
        assert "class" in result, "Should contain a class definition"
        # Either extracted source or generated code should have the class name
        assert "FilterColumnSettings" in result or "NodeSettings" in result

    def test_generate_settings_schema_code_basic(self, FilterColumnNode):
        """Test generating settings schema code from runtime structure."""
        add_to_custom_node_store(FilterColumnNode)

        graph = create_graph()
        converter = FlowGraphToPolarsConverter(graph)

        settings_schema = FilterColumnNode().settings_schema
        class_name = type(settings_schema).__name__

        result = converter._generate_settings_schema_code(settings_schema, class_name)

        assert result is not None
        assert f"class {class_name}(NodeSettings):" in result
        assert "main_section" in result or "Section" in result

    def test_generate_component_code(self, FilterColumnNode):
        """Test generating code for individual UI components."""
        add_to_custom_node_store(FilterColumnNode)

        graph = create_graph()
        converter = FlowGraphToPolarsConverter(graph)

        settings_schema = FilterColumnNode().settings_schema
        section = settings_schema.main_section
        component = section.select_column

        result = converter._generate_component_code(component)

        assert "ColumnSelector" in result
        assert "label=" in result

    def test_generate_section_code(self, FilterColumnNode):
        """Test generating code for Section objects."""
        add_to_custom_node_store(FilterColumnNode)

        graph = create_graph()
        converter = FlowGraphToPolarsConverter(graph)

        settings_schema = FilterColumnNode().settings_schema
        section = settings_schema.main_section

        result = converter._generate_section_code(section, "main_section")

        assert len(result) > 0
        assert "main_section = Section(" in result[0]


# =============================================================================
# Integration Tests for Code Generation
# =============================================================================

class TestCustomNodeCodeGeneration:
    """Integration tests for generating code with custom nodes."""

    def test_generated_code_includes_settings_class(self, FilterColumnNode):
        """Test that generated code includes the settings class definition."""
        add_to_custom_node_store(FilterColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}, {"Column 1": "test2"}], node_id=1)

        settings = {"main_section": {"select_column": "Column 1"}}
        add_custom_node_to_graph(graph, FilterColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)

        # Check that the settings class is defined
        assert "FilterColumnSettings" in code, f"Settings class should be in generated code. Got:\n{code}"
        assert "class FilterColumnSettings" in code or "FilterColumnSettings(NodeSettings)" in code

    def test_generated_code_includes_custom_node_imports(self, FilterColumnNode):
        """Test that generated code includes necessary imports."""
        add_to_custom_node_store(FilterColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)

        settings = {"main_section": {"select_column": "Column 1"}}
        add_custom_node_to_graph(graph, FilterColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)

        assert "from flowfile_core.flowfile.node_designer.custom_node import CustomNodeBase" in code
        assert "NodeSettings" in code

    def test_generated_code_no_unnecessary_collect_lazy(self, FilterColumnNode):
        """Test that LazyFrame-based process methods don't have unnecessary collect/lazy."""
        add_to_custom_node_store(FilterColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)

        settings = {"main_section": {"select_column": "Column 1"}}
        add_custom_node_to_graph(graph, FilterColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)

        # Since FilterColumn.process accepts LazyFrame and returns LazyFrame,
        # there should be no .collect() or .lazy() in the process call
        lines = code.split('\n')
        process_lines = [l for l in lines if '_custom_node_' in l and '.process(' in l]

        for line in process_lines:
            assert '.collect()' not in line, f"Should not have .collect() for LazyFrame process: {line}"
            assert '.lazy()' not in line, f"Should not have .lazy() for LazyFrame process: {line}"

    def test_generated_code_settings_class_before_node_class(self, FilterColumnNode):
        """Test that settings class appears before the node class that uses it."""
        add_to_custom_node_store(FilterColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)

        settings = {"main_section": {"select_column": "Column 1"}}
        add_custom_node_to_graph(graph, FilterColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)

        # Find positions of class definitions
        settings_class_match = code.find("FilterColumnSettings")
        node_class_match = code.find("class FilterColumn(")

        if settings_class_match != -1 and node_class_match != -1:
            assert settings_class_match < node_class_match, \
                "Settings class should appear before node class"

    def test_generated_code_is_executable(self, AddColumnNode):
        """Test that generated code can be executed."""
        add_to_custom_node_store(AddColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"existing": 1}, {"existing": 2}], node_id=1)

        settings = {"config": {"column_name": "new_col", "fixed_value": "hello"}}
        add_custom_node_to_graph(graph, AddColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)

        # Print code for debugging if test fails
        print("Generated code:")
        print(code)

        # Execute the generated code
        exec_globals = {}
        try:
            exec(code, exec_globals)
            result = exec_globals['run_etl_pipeline']()

            # Verify result
            if hasattr(result, 'collect'):
                result_df = result.collect()
            else:
                result_df = result

            assert "new_col" in result_df.columns
            assert result_df["new_col"].to_list() == ["hello", "hello"]

        except Exception as e:
            pytest.fail(f"Generated code should be executable. Error: {e}\n\nCode:\n{code}")


class TestProcessMethodSignatureDetection:
    """Tests for detecting process method signatures."""

    def test_detects_lazyframe_input(self, FilterColumnNode):
        """Test that LazyFrame input types are detected."""
        add_to_custom_node_store(FilterColumnNode)

        graph = create_graph()
        converter = FlowGraphToPolarsConverter(graph)

        needs_collect, needs_lazy = converter._check_process_method_signature(FilterColumnNode)

        # FilterColumn.process accepts *inputs: pl.LazyFrame and returns pl.LazyFrame
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
