"""
Tests for the custom node code generator.

This module tests the functionality of generating executable Python code
from CustomNodeBase instances.
"""

import ast
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_core.flowfile.node_designer import (
    CustomNodeBase,
    CustomNodeCodeGenerator,
    NodeSettings,
    Section,
    generate_node_code,
)
from flowfile_core.flowfile.node_designer.ui_components import (
    ColumnSelector,
    IncomingColumns,
    MultiSelect,
    NumericInput,
    SingleSelect,
    SliderInput,
    TextInput,
    ToggleSwitch,
)
from flowfile_core.types import Types


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def simple_node_class():
    """A simple custom node with basic settings."""

    class FixedColumn(CustomNodeBase):
        """A custom node that adds a new column with a fixed value."""

        node_name: str = "Fixed Column"
        node_category: str = "Transform"
        node_group: str = "custom"
        intro: str = "Adds a new column with a fixed value you provide."
        title: str = "Fixed Column"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

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
            if not inputs:
                return pl.DataFrame()

            input_df = inputs[0]
            fixed_value = self.settings_schema.main_section.standard_input.value
            new_col_name = self.settings_schema.main_section.column_name.value

            if fixed_value is None or not new_col_name:
                return input_df

            return input_df.with_columns(
                pl.lit(fixed_value).alias(new_col_name)
            )

    return FixedColumn


@pytest.fixture
def node_with_all_components():
    """A node that uses all UI component types."""

    class AllComponentsNode(CustomNodeBase):
        """A node demonstrating all available UI components."""

        node_name: str = "All Components"
        node_category: str = "Demo"
        intro: str = "Demonstrates all available UI components."
        title: str = "All Components Demo"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            basic_section=Section(
                title="Basic Inputs",
                text_field=TextInput(label="Text Input", default="default"),
                number_field=NumericInput(label="Number", default=42, min_value=0, max_value=100),
                slider_field=SliderInput(label="Slider", min_value=0, max_value=100, step=5, default=50),
                toggle_field=ToggleSwitch(label="Enable", default=True),
            ),
            select_section=Section(
                title="Selection Inputs",
                single_choice=SingleSelect(label="Mode", options=["A", "B", "C"], default="A"),
                multi_choice=MultiSelect(label="Tags", options=["Tag1", "Tag2", "Tag3"]),
                column_choice=SingleSelect(label="Column", options=IncomingColumns),
            ),
        )

        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            if not inputs:
                return pl.DataFrame()
            return inputs[0]

    return AllComponentsNode


@pytest.fixture
def source_node_class():
    """A source node with 0 inputs."""

    class DataGenerator(CustomNodeBase):
        """A source node that generates sample data."""

        node_name: str = "Data Generator"
        node_category: str = "Source"
        intro: str = "Generates sample data for testing."
        title: str = "Generate Data"
        number_of_inputs: int = 0
        number_of_outputs: int = 1

        settings_schema: NodeSettings = NodeSettings(
            config=Section(
                title="Configuration",
                row_count=NumericInput(label="Number of Rows", default=10, min_value=1),
            ),
        )

        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            row_count = self.settings_schema.config.row_count.value or 10
            return pl.DataFrame({
                "id": list(range(1, int(row_count) + 1)),
            })

    return DataGenerator


@pytest.fixture
def node_without_settings():
    """A node with no settings schema."""

    class PassthroughNode(CustomNodeBase):
        """A node that passes data through unchanged."""

        node_name: str = "Passthrough"
        node_category: str = "Utility"
        intro: str = "Passes data through without modification."
        title: str = "Passthrough"
        number_of_inputs: int = 1
        number_of_outputs: int = 1
        settings_schema: NodeSettings | None = None

        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            if not inputs:
                return pl.DataFrame()
            return inputs[0]

    return PassthroughNode


# =============================================================================
# Test: Code Generator Initialization
# =============================================================================


class TestCodeGeneratorInit:
    """Tests for CustomNodeCodeGenerator initialization."""

    def test_init_with_node_only(self, simple_node_class):
        """Test initializing generator with just a node."""
        node = simple_node_class()
        generator = CustomNodeCodeGenerator(node)

        assert generator.node is node
        assert generator.settings_values == {}  # No values set yet
        assert generator.source_file_path is None

    def test_init_with_configured_node(self, simple_node_class):
        """Test initializing generator with a configured node."""
        settings = {
            "main_section": {
                "standard_input": "Hello",
                "column_name": "greeting"
            }
        }
        node = simple_node_class.from_settings(settings)
        generator = CustomNodeCodeGenerator(node)

        assert generator.settings_values == settings

    def test_init_with_explicit_settings(self, simple_node_class):
        """Test that explicit settings override node settings."""
        node = simple_node_class()
        explicit_settings = {"main_section": {"standard_input": "Override"}}

        generator = CustomNodeCodeGenerator(node, settings_values=explicit_settings)

        assert generator.settings_values == explicit_settings


# =============================================================================
# Test: Import Generation
# =============================================================================


class TestImportGeneration:
    """Tests for import statement generation."""

    def test_basic_imports(self, simple_node_class):
        """Test that basic imports are always included."""
        node = simple_node_class()
        generator = CustomNodeCodeGenerator(node)
        code = generator.generate(include_example=False)

        assert "import polars as pl" in code
        assert "from flowfile_core.flowfile.node_designer import" in code
        assert "CustomNodeBase" in code
        assert "NodeSettings" in code
        assert "Section" in code

    def test_component_imports(self, simple_node_class):
        """Test that used UI components are imported."""
        node = simple_node_class()
        generator = CustomNodeCodeGenerator(node)
        code = generator.generate(include_example=False)

        assert "TextInput" in code

    def test_all_component_imports(self, node_with_all_components):
        """Test that all used component types are imported."""
        node = node_with_all_components()
        generator = CustomNodeCodeGenerator(node)
        code = generator.generate(include_example=False)

        assert "TextInput" in code
        assert "NumericInput" in code
        assert "SliderInput" in code
        assert "ToggleSwitch" in code
        assert "SingleSelect" in code
        assert "MultiSelect" in code

    def test_incoming_columns_import(self, node_with_all_components):
        """Test that IncomingColumns marker is imported when used."""
        node = node_with_all_components()
        generator = CustomNodeCodeGenerator(node)
        code = generator.generate(include_example=False)

        assert "IncomingColumns" in code


# =============================================================================
# Test: Class Definition Generation
# =============================================================================


class TestClassDefinitionGeneration:
    """Tests for class definition generation."""

    def test_class_name_in_output(self, simple_node_class):
        """Test that the correct class name appears in output."""
        node = simple_node_class()
        code = node.to_code(include_example=False)

        assert "class FixedColumn(CustomNodeBase):" in code

    def test_node_metadata_in_output(self, simple_node_class):
        """Test that node metadata is preserved."""
        node = simple_node_class()
        code = node.to_code(include_example=False)

        assert 'node_name' in code
        assert '"Fixed Column"' in code or "'Fixed Column'" in code
        assert 'number_of_inputs' in code
        assert 'number_of_outputs' in code

    def test_process_method_extracted(self, simple_node_class):
        """Test that the process method is included."""
        node = simple_node_class()
        code = node.to_code(include_example=False)

        assert "def process(self, *inputs" in code
        assert "pl.lit" in code


# =============================================================================
# Test: Settings Serialization
# =============================================================================


class TestSettingsSerialization:
    """Tests for settings value serialization."""

    def test_empty_settings(self, node_without_settings):
        """Test handling of nodes without settings."""
        node = node_without_settings()
        generator = CustomNodeCodeGenerator(node)

        assert generator.settings_values == {}

    def test_settings_values_extracted(self, simple_node_class):
        """Test that settings values are correctly extracted."""
        settings = {
            "main_section": {
                "standard_input": "Test Value",
                "column_name": "test_col"
            }
        }
        node = simple_node_class.from_settings(settings)
        generator = CustomNodeCodeGenerator(node)

        assert generator.settings_values == settings

    def test_settings_in_example_code(self, simple_node_class):
        """Test that settings appear in example code."""
        settings = {
            "main_section": {
                "standard_input": "Hello World",
                "column_name": "greeting"
            }
        }
        node = simple_node_class.from_settings(settings)
        code = node.to_code(include_example=True)

        assert "Hello World" in code
        assert "greeting" in code
        assert "settings" in code


# =============================================================================
# Test: Example Usage Generation
# =============================================================================


class TestExampleGeneration:
    """Tests for example usage generation."""

    def test_example_included_when_requested(self, simple_node_class):
        """Test that example is included when include_example=True."""
        node = simple_node_class()
        code = node.to_code(include_example=True)

        assert 'if __name__ == "__main__":' in code
        assert "Example Usage" in code

    def test_example_excluded_when_requested(self, simple_node_class):
        """Test that example is excluded when include_example=False."""
        node = simple_node_class()
        code = node.to_code(include_example=False)

        assert 'if __name__ == "__main__":' not in code

    def test_example_has_sample_data(self, simple_node_class):
        """Test that example includes sample input data."""
        node = simple_node_class()
        code = node.to_code(include_example=True)

        assert "input_df = pl.DataFrame" in code
        assert "node.process" in code

    def test_source_node_example_no_input(self, source_node_class):
        """Test that source node example doesn't have input data."""
        node = source_node_class()
        code = node.to_code(include_example=True)

        # Should have process call without input
        assert "node.process()" in code

    def test_example_shows_output(self, simple_node_class):
        """Test that example shows how to view output."""
        node = simple_node_class()
        code = node.to_code(include_example=True)

        assert "print" in code
        assert "result" in code


# =============================================================================
# Test: Generated Code Validity
# =============================================================================


class TestCodeValidity:
    """Tests that generated code is valid Python."""

    def test_code_parses_without_errors(self, simple_node_class):
        """Test that generated code is valid Python syntax."""
        node = simple_node_class()
        code = node.to_code(include_example=True)

        # This will raise SyntaxError if invalid
        ast.parse(code)

    def test_code_parses_without_example(self, simple_node_class):
        """Test that code without example is valid Python."""
        node = simple_node_class()
        code = node.to_code(include_example=False)

        ast.parse(code)

    def test_all_component_node_parses(self, node_with_all_components):
        """Test that node with all components generates valid code."""
        node = node_with_all_components()
        code = node.to_code(include_example=True)

        ast.parse(code)

    def test_source_node_code_parses(self, source_node_class):
        """Test that source node generates valid code."""
        node = source_node_class()
        code = node.to_code(include_example=True)

        ast.parse(code)

    def test_no_settings_node_parses(self, node_without_settings):
        """Test that node without settings generates valid code."""
        node = node_without_settings()
        code = node.to_code(include_example=True)

        ast.parse(code)


# =============================================================================
# Test: Module Docstring
# =============================================================================


class TestModuleDocstring:
    """Tests for module docstring generation."""

    def test_docstring_has_node_name(self, simple_node_class):
        """Test that docstring includes the node name."""
        node = simple_node_class()
        code = node.to_code(include_example=False)

        assert 'Custom Node: Fixed Column' in code

    def test_docstring_has_category(self, simple_node_class):
        """Test that docstring includes the category."""
        node = simple_node_class()
        code = node.to_code(include_example=False)

        assert 'Category: Transform' in code

    def test_docstring_has_description(self, simple_node_class):
        """Test that docstring includes the description."""
        node = simple_node_class()
        code = node.to_code(include_example=False)

        assert 'Description:' in code
        assert 'fixed value' in code.lower()


# =============================================================================
# Test: Convenience Function
# =============================================================================


class TestGenerateNodeCodeFunction:
    """Tests for the generate_node_code convenience function."""

    def test_generate_node_code_basic(self, simple_node_class):
        """Test the convenience function with basic usage."""
        node = simple_node_class()
        code = generate_node_code(node)

        assert "class FixedColumn" in code
        assert 'if __name__ == "__main__":' in code

    def test_generate_node_code_no_example(self, simple_node_class):
        """Test the convenience function without example."""
        node = simple_node_class()
        code = generate_node_code(node, include_example=False)

        assert "class FixedColumn" in code
        assert 'if __name__ == "__main__":' not in code

    def test_generate_node_code_with_settings(self, simple_node_class):
        """Test the convenience function with explicit settings."""
        node = simple_node_class()
        settings = {"main_section": {"standard_input": "Custom"}}
        code = generate_node_code(node, settings_values=settings)

        assert "Custom" in code


# =============================================================================
# Test: to_code Method on CustomNodeBase
# =============================================================================


class TestToCodeMethod:
    """Tests for the to_code method on CustomNodeBase."""

    def test_to_code_exists(self, simple_node_class):
        """Test that to_code method exists on CustomNodeBase."""
        node = simple_node_class()
        assert hasattr(node, 'to_code')
        assert callable(node.to_code)

    def test_to_code_returns_string(self, simple_node_class):
        """Test that to_code returns a string."""
        node = simple_node_class()
        result = node.to_code()
        assert isinstance(result, str)

    def test_to_code_with_settings_values(self, simple_node_class):
        """Test to_code with explicit settings values."""
        node = simple_node_class()
        settings = {"main_section": {"standard_input": "Test"}}
        code = node.to_code(settings_values=settings)

        assert "Test" in code


# =============================================================================
# Test: Integration
# =============================================================================


class TestIntegration:
    """Integration tests for the code generator."""

    def test_configured_node_full_workflow(self, simple_node_class):
        """Test the full workflow with a configured node."""
        # Configure the node
        settings = {
            "main_section": {
                "standard_input": "Hello World",
                "column_name": "greeting"
            }
        }
        node = simple_node_class.from_settings(settings)

        # Generate code
        code = node.to_code(include_example=True)

        # Verify code structure
        assert "class FixedColumn" in code
        assert "Hello World" in code
        assert "greeting" in code
        assert 'if __name__ == "__main__":' in code

        # Verify it's valid Python
        ast.parse(code)

    def test_node_output_matches_expectation(self, simple_node_class):
        """Test that the original node produces expected output."""
        settings = {
            "main_section": {
                "standard_input": "Test",
                "column_name": "result"
            }
        }
        node = simple_node_class.from_settings(settings)

        input_df = pl.DataFrame({"id": [1, 2, 3]})
        result = node.process(input_df)

        expected = pl.DataFrame({
            "id": [1, 2, 3],
            "result": ["Test", "Test", "Test"]
        })

        assert_frame_equal(result, expected)


# =============================================================================
# Test: Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_intro(self, simple_node_class):
        """Test handling of node with empty intro."""
        class NoIntroNode(CustomNodeBase):
            node_name: str = "No Intro"
            intro: str = ""
            number_of_inputs: int = 1
            number_of_outputs: int = 1

            def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
                return inputs[0] if inputs else pl.DataFrame()

        node = NoIntroNode()
        code = node.to_code()

        # Should still generate valid code
        ast.parse(code)

    def test_special_characters_in_values(self, simple_node_class):
        """Test handling of special characters in settings."""
        settings = {
            "main_section": {
                "standard_input": 'Hello "World"',
                "column_name": "test_col"
            }
        }
        node = simple_node_class.from_settings(settings)
        code = node.to_code()

        # Should generate valid Python
        ast.parse(code)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
