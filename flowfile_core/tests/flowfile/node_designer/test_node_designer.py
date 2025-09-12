import pytest
from typing import Dict, Any
import polars as pl
from polars.testing import assert_frame_equal
# Assuming these files are in a structure that allows direct import.
# You may need to adjust your PYTHONPATH for this to work.
from flowfile_core.flowfile.node_designer.ui_components import (
    TextInput,
    NumericInput,
    ToggleSwitch,
    SingleSelect,
    MultiSelect,
    Section,
    IncomingColumns
)
from flowfile_core.flowfile.node_designer.custom_node import (
    CustomNodeBase,
    NodeSettings,
    to_frontend_schema
)


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


# Fixtures for reusable test components and schemas
@pytest.fixture
def sample_components() -> Dict[str, Any]:
    """Provides a dictionary of sample UI components for testing."""
    return {
        "text_input": TextInput(label="Name", default="Default Name", placeholder="Enter name"),
        "num_input": NumericInput(label="Count", default=5),
        "toggle": ToggleSwitch(label="Enable", default=True),
        "single_select": SingleSelect(label="Mode", options=["A", "B", "C"], default="A"),
        "multi_select": MultiSelect(label="Tags", options=["T1", "T2"], default=["T1"]),
        "col_select": SingleSelect(label="Column", options=IncomingColumns)
    }


@pytest.fixture
def sample_section(sample_components: Dict[str, Any]) -> Section:
    """Provides a sample Section containing various components."""
    return Section(
        title="Main Config",
        description="Main configuration options.",
        **sample_components
    )

@pytest.fixture
def sample_node_settings(sample_section: Section) -> NodeSettings:
    """Provides a sample NodeSettings containing a section."""
    return NodeSettings(config=sample_section)

@pytest.fixture
def sample_custom_node(sample_node_settings: NodeSettings) -> CustomNodeBase:
    """Provides a sample CustomNodeBase instance for testing."""
    class MyTestNode(CustomNodeBase):
        node_name: str = "Test Node"
        settings_schema: NodeSettings = sample_node_settings

    return MyTestNode()

# --- Tests for individual UI Components ---

def test_text_input_initialization():
    """Tests TextInput initialization and default value handling."""
    comp = TextInput(default="hello", placeholder="world")
    assert comp.value == "hello"
    comp.set_value("new")
    assert comp.value == "new"

def test_numeric_input_initialization():
    """Tests NumericInput initialization and default value handling."""
    comp = NumericInput(default=10, min_value=0)
    assert comp.value == 10
    comp.set_value(25)
    assert comp.value == 25

def test_toggle_switch_initialization_and_bool():
    """Tests ToggleSwitch default value, updates, and boolean representation."""
    comp = ToggleSwitch(default=True)
    assert comp.value is True
    assert bool(comp) is True
    comp.set_value(False)
    assert comp.value is False
    assert bool(comp) is False

def test_multi_select_default_is_empty_list():
    """Tests that MultiSelect defaults to an empty list if no default is provided."""
    comp = MultiSelect(options=["a", "b"])
    assert comp.value == []
    comp.set_value(["a"])
    assert comp.value == ["a"]


# --- Tests for Section and NodeSettings ---

def test_section_get_components(sample_section: Section, sample_components: Dict[str, Any]):
    """Tests that a Section can correctly find all its component children."""
    found_components = sample_section.get_components()
    assert len(found_components) == len(sample_components)
    assert "text_input" in found_components
    assert found_components["text_input"].label == "Name"

def test_node_settings_populate_values(sample_node_settings: NodeSettings):
    """Tests that component values are correctly updated from a dictionary."""
    # Define new values as if they came from the frontend
    new_values = {
        "config": {
            "text_input": "Updated Name",
            "num_input": 99,
            "toggle": False,
            "multi_select": ["T1", "T2"]
        }
    }

    # Populate the values
    sample_node_settings.populate_values(new_values)

    # Check that the component values have been updated
    section = getattr(sample_node_settings, 'config')
    components = section.get_components()
    assert components["text_input"].value == "Updated Name"
    assert components["num_input"].value == 99
    assert components["toggle"].value is False
    assert components["multi_select"].value == ["T1", "T2"]
    # Check that a value not in the input dict remains its default
    assert components["single_select"].value == "A"


# --- Tests for Frontend Schema Conversion ---

def test_to_frontend_schema_structure(sample_node_settings: NodeSettings):
    """Tests the structure of the JSON-serializable schema for the frontend."""
    schema = to_frontend_schema(sample_node_settings)
    # Check top-level section
    assert "config" in schema
    config_section = schema["config"]
    assert config_section["component_type"] == "Section"
    assert config_section["title"] == "Main Config"
    assert "components" in config_section

    # Check nested components
    components = config_section["components"]
    assert "text_input" in components
    assert components["text_input"]["component_type"] == "TextInput"
    assert components["text_input"]["label"] == "Name"
    assert components["text_input"]["value"] == "Default Name"


def test_to_frontend_schema_incoming_columns(sample_node_settings: NodeSettings):
    """Tests that IncomingColumns is correctly serialized."""
    schema = to_frontend_schema(sample_node_settings)
    col_select_schema = schema["config"]["components"]["col_select"]

    assert "options" in col_select_schema
    assert col_select_schema["options"] == {"__type__": "IncomingColumns"}


# --- Tests for CustomNodeBase ---

def test_custom_node_initialization_with_values(sample_node_settings: NodeSettings):
    """Tests initializing a custom node with starting values."""
    class MyNode(CustomNodeBase):
        node_name: str = "Init Node"
        settings_schema: NodeSettings = sample_node_settings

    initial_values = {
        "config": {
            "text_input": "Initial Name"
        }
    }
    node = MyNode(initial_values=initial_values)

    schema = node.get_frontend_schema()
    component_value = schema["settings_schema"]["config"]["components"]["text_input"]["value"]
    assert component_value == "Initial Name"

def test_custom_node_update_settings(sample_custom_node: CustomNodeBase):
    """Tests the update_settings method."""
    new_values = {
        "config": {
            "num_input": 123
        }
    }
    sample_custom_node.update_settings(new_values)

    schema = sample_custom_node.get_frontend_schema()
    component_value = schema["settings_schema"]["config"]["components"]["num_input"]["value"]
    assert component_value == 123


def test_custom_node_from_settings(sample_node_settings: NodeSettings):
    """Tests creating a node instance from just a settings dictionary."""
    class MyNode(CustomNodeBase):
        node_name: str = "From Settings Node"
        settings_schema: NodeSettings = sample_node_settings

    settings_values = {
        "config": {
            "toggle": False
        }
    }
    node = MyNode.from_settings(settings_values)

    schema = node.get_frontend_schema()
    component_value = schema["settings_schema"]["config"]["components"]["toggle"]["value"]
    assert component_value is False


class TestUserDefinedNode:
    """A dedicated test class for the node defined in the UserDefinedNode fixture."""

    @pytest.fixture
    def settings_dict(self) -> Dict[str, Any]:
        """Provides a sample settings dictionary for the FixedColumn node."""
        return {
            "main_section": {
                "standard_input": "hello from test",
                "column_name": "new_col"
            }
        }

    @pytest.fixture
    def configured_node(self, UserDefinedNode, settings_dict: Dict[str, Any]):
        """Provides a configured instance of the FixedColumn node."""
        return UserDefinedNode.from_settings(settings_dict)

    def test_build_from_settings(self, configured_node):
        """Tests that the node correctly populates its values from a dict."""
        section = getattr(configured_node.settings_schema, 'main_section')
        components = section.get_components()
        assert components["standard_input"].value == "hello from test"
        assert components["column_name"].value == "new_col"

    def test_export_schema_with_values(self, configured_node):
        """Tests that the exported schema contains the configured values."""
        schema = configured_node.get_frontend_schema()
        components = schema["settings_schema"]["main_section"]["components"]
        assert components["standard_input"]["value"] == "hello from test"
        assert components["column_name"]["value"] == "new_col"

    def test_process_logic(self, configured_node):
        """Tests the data transformation logic of the FixedColumn node."""
        input_df = pl.DataFrame({"a": [1, 2, 3]})

        result_df = configured_node.process(input_df)

        expected_df = pl.DataFrame({
            "a": [1, 2, 3],
            "new_col": ["hello from test", "hello from test", "hello from test"]
        })

        assert_frame_equal(result_df, expected_df)


if __name__ == "__main__":
    pytest.main([__file__])
