import ast
from typing import Any

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from flowfile_core.flowfile.node_designer import (
    Artifact,
    ColumnSelector,
    CustomNodeBase,
    NodeSettings,
    Section,
    VisibleWhen,
)
from flowfile_core.flowfile.node_designer.custom_node import (
    to_frontend_schema,
)
from flowfile_core.flowfile.node_designer.ui_components import (
    AvailableArtifacts,
    IncomingColumns,
    MultiSelect,
    NumericInput,
    SingleSelect,
    SliderInput,
    TextInput,
    ToggleSwitch,
)
from flowfile_core.types import Types


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
        settings_schema: NodeSettings = NodeSettings(
            main_section=Section(
                title="Configuration",
                standard_input=TextInput(label="Fixed Value", placeholder="Enter the value to set..."),
                column_name=TextInput(label="New Column Name", placeholder="Enter the output column name"),
            ),
        )

        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            """
            The core processing logic for the node.
            """
            if not inputs:
                return pl.DataFrame()

            input_df = inputs[0]

            fixed_value = self.settings_schema.main_section.standard_input.value
            new_col_name = self.settings_schema.main_section.column_name.value

            if fixed_value is None or not new_col_name:
                return input_df

            return input_df.with_columns(pl.lit(fixed_value).alias(new_col_name))

    return FixedColumn


@pytest.fixture
def node_settings_with_multi_column_selector():
    """Provides a NodeSettings instance with a MultiSelect ColumnSelector for testing."""
    input_value = Section(
        title="input",
        numeric_columns=ColumnSelector(
            label="Select numeric columns",
            required=True,
            multiple=True,
            data_types="Numeric",
        ),
    )

    class ColumnsToStringSettings(NodeSettings):
        input: Section = input_value

    class ColumnsToString(CustomNodeBase):
        node_name: str = "Columns to String"
        node_category: str = "Convert"
        title: str = "Convert columns to String"
        intro: str = "Select columns that are numeric and convert them to string"
        number_of_inputs: int = 1
        number_of_outputs: int = 1
        settings_schema: ColumnsToStringSettings = ColumnsToStringSettings()

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            lf = inputs[0]
            expr = [
                pl.col(col).cast(pl.Utf8) if col else pl.col(col) in self.settings_schema.input.numeric_columns.value
                for col in lf.columns
            ]

            return lf.select(expr)

    return ColumnsToString


# Fixtures for reusable test components and schemas
@pytest.fixture
def sample_components() -> dict[str, Any]:
    """Provides a dictionary of sample UI components for testing."""
    return {
        "text_input": TextInput(label="Name", default="Default Name", placeholder="Enter name"),
        "num_input": NumericInput(label="Count", default=5),
        "slider_input": SliderInput(label="Threshold", min_value=0, max_value=100, default=50),
        "toggle": ToggleSwitch(label="Enable", default=True),
        "single_select": SingleSelect(label="Mode", options=["A", "B", "C"], default="A"),
        "multi_select": MultiSelect(label="Tags", options=["T1", "T2"], default=["T1"]),
        "col_select": SingleSelect(label="Column", options=IncomingColumns),
    }


@pytest.fixture
def sample_section(sample_components: dict[str, Any]) -> Section:
    """Provides a sample Section containing various components."""
    return Section(title="Main Config", description="Main configuration options.", **sample_components)


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


def test_slider_input_initialization():
    """Tests SliderInput initialization and default value handling."""
    comp = SliderInput(min_value=0, max_value=100, step=5, default=50)
    assert comp.value == 50
    assert comp.min_value == 0
    assert comp.max_value == 100
    assert comp.step == 5
    comp.set_value(75)
    assert comp.value == 75


def test_slider_input_defaults_to_min_value():
    """Tests that SliderInput defaults to min_value when no default is provided."""
    comp = SliderInput(min_value=10, max_value=200)
    assert comp.value == 10


def test_slider_input_with_default():
    """Tests SliderInput with explicit default value."""
    comp = SliderInput(min_value=0, max_value=100, default=25)
    assert comp.value == 25


def test_slider_input_component_type():
    """Tests that SliderInput has the correct component_type."""
    comp = SliderInput()
    assert comp.component_type == "SliderInput"
    assert comp.input_type == "number"


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


def test_section_get_components(sample_section: Section, sample_components: dict[str, Any]):
    """Tests that a Section can correctly find all its component children."""
    found_components = sample_section.get_components()
    assert len(found_components) == len(sample_components)
    assert "text_input" in found_components
    assert found_components["text_input"].label == "Name"


def test_node_settings_populate_values(sample_node_settings: NodeSettings):
    """Tests that component values are correctly updated from a dictionary."""
    new_values = {
        "config": {"text_input": "Updated Name", "num_input": 99, "toggle": False, "multi_select": ["T1", "T2"]}
    }

    sample_node_settings.populate_values(new_values)

    section = sample_node_settings.config
    components = section.get_components()
    assert components["text_input"].value == "Updated Name"
    assert components["num_input"].value == 99
    assert components["toggle"].value is False
    assert components["multi_select"].value == ["T1", "T2"]
    assert components["single_select"].value == "A"


# --- Tests for Frontend Schema Conversion ---


def test_to_frontend_schema_structure(sample_node_settings: NodeSettings):
    """Tests the structure of the JSON-serializable schema for the frontend."""
    schema = to_frontend_schema(sample_node_settings)
    assert "config" in schema
    config_section = schema["config"]
    assert config_section["component_type"] == "Section"
    assert config_section["title"] == "Main Config"
    assert config_section["layout"] == "vertical"
    assert "components" in config_section

    components = config_section["components"]
    assert "text_input" in components
    assert components["text_input"]["component_type"] == "TextInput"
    assert components["text_input"]["label"] == "Name"
    assert components["text_input"]["value"] == "Default Name"


def test_to_frontend_schema_horizontal_layout():
    """A section built with layout='horizontal' carries that into the frontend schema."""
    settings = NodeSettings(config=Section(title="Row", layout="horizontal", note=TextInput(label="Note")))
    schema = to_frontend_schema(settings)
    assert schema["config"]["layout"] == "horizontal"


def test_to_frontend_schema_visible_when():
    """VisibleWhen serializes to the {field, equals} wire dict; sections without it omit the key."""
    settings = NodeSettings(
        opts=Section(title="Opts", show_advanced=ToggleSwitch(label="Show advanced")),
        advanced=Section(
            title="Advanced",
            visible_when=VisibleWhen(field="opts.show_advanced"),
            note=TextInput(label="Note"),
        ),
    )
    schema = to_frontend_schema(settings)
    assert schema["advanced"]["visible_when"] == {"field": "opts.show_advanced", "equals": True}
    assert "visible_when" not in schema["opts"]


def test_section_visible_when_accepts_dict_and_coerces():
    """A plain dict is coerced to VisibleWhen at the Section boundary (lenient authoring)."""
    section = Section(title="Advanced", visible_when={"field": "opts.flag", "equals": False})
    assert isinstance(section.visible_when, VisibleWhen)
    assert section.visible_when.field == "opts.flag"
    assert section.visible_when.equals is False


def test_to_frontend_schema_incoming_columns(sample_node_settings: NodeSettings):
    """Tests that IncomingColumns is correctly serialized."""
    schema = to_frontend_schema(sample_node_settings)
    col_select_schema = schema["config"]["components"]["col_select"]

    assert "options" in col_select_schema
    assert col_select_schema["options"] == {"__type__": "IncomingColumns"}


def test_to_frontend_schema_available_artifacts():
    """Tests that AvailableArtifacts is correctly serialized."""

    class ArtifactSettings(NodeSettings):
        config: Section = Section(
            title="Artifacts",
            artifact=SingleSelect(label="Artifact", options=AvailableArtifacts),
            artifacts=MultiSelect(label="Artifacts", options=AvailableArtifacts),
        )

    schema = to_frontend_schema(ArtifactSettings())
    single_schema = schema["config"]["components"]["artifact"]
    multi_schema = schema["config"]["components"]["artifacts"]

    assert single_schema["options"] == {"__type__": "AvailableArtifacts"}
    assert multi_schema["options"] == {"__type__": "AvailableArtifacts"}


def test_available_artifacts_default_instance_matches_bare_class():
    """A default AvailableArtifacts() instance serializes byte-identically to the bare class."""

    class BareSettings(NodeSettings):
        config: Section = Section(sel=SingleSelect(label="A", options=AvailableArtifacts))

    class InstanceSettings(NodeSettings):
        config: Section = Section(sel=SingleSelect(label="A", options=AvailableArtifacts()))

    bare = to_frontend_schema(BareSettings())["config"]["components"]["sel"]["options"]
    instance = to_frontend_schema(InstanceSettings())["config"]["components"]["sel"]["options"]
    assert bare == instance == {"__type__": "AvailableArtifacts"}


def test_available_artifacts_scope_and_type_filter_serialized():
    """scope=='global' and a type filter ride along on the wire marker."""

    class ScopedSettings(NodeSettings):
        config: Section = Section(
            sel=SingleSelect(label="A", options=AvailableArtifacts(scope="global", type="xgboost.*"))
        )

    options = to_frontend_schema(ScopedSettings())["config"]["components"]["sel"]["options"]
    assert options == {"__type__": "AvailableArtifacts", "scope": "global", "type_filter": ["xgboost.*"]}


def test_available_artifacts_scope_all_serialized():
    """scope=='all' (upstream ∪ global) rides along on the wire marker."""

    class AllScopeSettings(NodeSettings):
        config: Section = Section(sel=SingleSelect(label="A", options=AvailableArtifacts(scope="all")))

    options = to_frontend_schema(AllScopeSettings())["config"]["components"]["sel"]["options"]
    assert options == {"__type__": "AvailableArtifacts", "scope": "all"}


def test_available_artifacts_type_list_normalizes_sorted_unique():
    """A list type filter is de-duplicated and sorted (scope default omitted)."""
    marker = AvailableArtifacts(type=["b.*", "a.*", "a.*"])
    assert marker.type_filter == ["a.*", "b.*"]

    class ListSettings(NodeSettings):
        config: Section = Section(sel=MultiSelect(label="A", options=marker))

    options = to_frontend_schema(ListSettings())["config"]["components"]["sel"]["options"]
    assert options == {"__type__": "AvailableArtifacts", "type_filter": ["a.*", "b.*"]}


def test_available_artifacts_rejects_bad_scope():
    with pytest.raises(ValueError):
        AvailableArtifacts(scope="sideways")
    with pytest.raises(ValueError):
        AvailableArtifacts(scope="everything")


def test_custom_node_publishes_validates_and_templates():
    """CustomNodeBase carries publishes; to_node_template surfaces them (None when empty)."""

    class PublishingNode(CustomNodeBase):
        node_name: str = "Publisher"
        publishes: list[Artifact] = [Artifact("model"), Artifact("scaler", type="sklearn.X")]

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            return inputs[0]

    node = PublishingNode()
    assert [(a.name, a.type) for a in node.publishes] == [("model", None), ("scaler", "sklearn.X")]
    template = node.to_node_template()
    assert [p.model_dump() for p in template.publishes] == [
        {"name": "model", "type": None},
        {"name": "scaler", "type": "sklearn.X"},
    ]

    class NoPublishNode(CustomNodeBase):
        node_name: str = "No Publish"

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            return inputs[0]

    assert NoPublishNode().to_node_template().publishes is None


def test_custom_node_publishes_coerces_dicts():
    """Dicts passed to the publishes field coerce to the Artifact dataclass (pydantic v2)."""

    class DictNode(CustomNodeBase):
        node_name: str = "Dict Publisher"

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            return inputs[0]

    node = DictNode(publishes=[{"name": "m"}, {"name": "s", "type": "t"}])
    assert all(isinstance(a, Artifact) for a in node.publishes)
    assert [(a.name, a.type) for a in node.publishes] == [("m", None), ("s", "t")]


def test_custom_node_publishes_string_form_default_templates():
    """A `publishes` class-attribute default may hold plain strings (pydantic v2
    doesn't coerce defaults); to_node_template must not crash on them."""

    class StrPublishingNode(CustomNodeBase):
        node_name: str = "Str Publisher"
        publishes: list[Artifact] = ["model", Artifact("m2", type="x.Y")]

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            return inputs[0]

    node = StrPublishingNode()
    # the string default is not coerced by pydantic v2 -> stays a plain str
    assert node.publishes[0] == "model"
    template = node.to_node_template()
    assert [p.model_dump() for p in template.publishes] == [
        {"name": "model", "type": None},
        {"name": "m2", "type": "x.Y"},
    ]


def test_string_form_publishes_ast_template_matches_exec_template():
    """AST (exec-free) and exec templates must agree for string-form publishes.

    Codegen always canonicalizes string publishes to nd.Artifact(...), so this
    case can't live in the parity corpus; assert parity directly instead.
    """
    from flowfile_core.flowfile.node_designer.parsing import scan_node_source
    from flowfile_core.flowfile.user_defined.registry import compute_node_key
    from flowfile_core.flowfile.user_defined.templates import manifest_to_template
    from shared.node_designer.loading import find_custom_node_class, load_node_module

    source = (
        "import polars as pl\n"
        "from flowfile import node_designer as nd\n\n\n"
        "class StrPublisher(nd.CustomNodeBase):\n"
        '    node_name: str = "Str Publisher"\n'
        '    publishes: list[nd.Artifact] = ["model", nd.Artifact("m2", type="x.Y")]\n\n'
        "    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n"
        "        return inputs[0]\n"
    )
    manifest = scan_node_source(source)
    ast_template = manifest_to_template(manifest, compute_node_key(manifest.node_name))

    module = load_node_module(source=source, module_name="parity_str_publisher")
    node_class = find_custom_node_class(module, class_name=manifest.class_name)
    exec_template = node_class().to_node_template()

    assert ast_template.model_dump() == exec_template.model_dump()
    assert [p.model_dump() for p in exec_template.publishes] == [
        {"name": "model", "type": None},
        {"name": "m2", "type": "x.Y"},
    ]


# --- Tests for CustomNodeBase ---


def test_custom_node_initialization_with_values(sample_node_settings: NodeSettings):
    """Tests initializing a custom node with starting values."""

    class MyNode(CustomNodeBase):
        node_name: str = "Init Node"
        settings_schema: NodeSettings = sample_node_settings

    initial_values = {"config": {"text_input": "Initial Name"}}
    node = MyNode(initial_values=initial_values)

    schema = node.get_frontend_schema()
    component_value = schema["settings_schema"]["config"]["components"]["text_input"]["value"]
    assert component_value == "Initial Name"


def test_custom_node_update_settings(sample_custom_node: CustomNodeBase):
    """Tests the update_settings method."""
    new_values = {"config": {"num_input": 123}}
    sample_custom_node.update_settings(new_values)

    schema = sample_custom_node.get_frontend_schema()
    component_value = schema["settings_schema"]["config"]["components"]["num_input"]["value"]
    assert component_value == 123


def test_generate_kernel_code_is_valid_python():
    class KernelNode(CustomNodeBase):
        node_name: str = "Kernel Node"

        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            if not inputs:
                return pl.DataFrame()
            return inputs[0]

    node = KernelNode()
    code = node.generate_kernel_code()
    ast.parse(code)


def test_generate_kernel_code_includes_output_names():
    class MultiOutputNode(CustomNodeBase):
        node_name: str = "Multi Output"
        output_names: list[str] = ["main", "secondary"]

        def process(self, *inputs: pl.DataFrame) -> dict[str, pl.DataFrame]:
            if not inputs:
                return {"main": pl.DataFrame(), "secondary": pl.DataFrame()}
            return {"main": inputs[0], "secondary": inputs[0]}

    node = MultiOutputNode()
    code = node.generate_kernel_code()
    assert 'flowfile_ctx.publish_output(result["main"], name="main")' in code
    assert 'flowfile_ctx.publish_output(result["secondary"], name="secondary")' in code


def test_custom_node_from_settings(sample_node_settings: NodeSettings):
    """Tests creating a node instance from just a settings dictionary."""

    class MyNode(CustomNodeBase):
        node_name: str = "From Settings Node"
        settings_schema: NodeSettings = sample_node_settings

    settings_values = {"config": {"toggle": False}}
    node = MyNode.from_settings(settings_values)

    schema = node.get_frontend_schema()
    component_value = schema["settings_schema"]["config"]["components"]["toggle"]["value"]
    assert component_value is False


class TestUserDefinedNode:
    """A dedicated test class for the node defined in the UserDefinedNode fixture."""

    @pytest.fixture
    def settings_dict(self) -> dict[str, Any]:
        """Provides a sample settings dictionary for the FixedColumn node."""
        return {"main_section": {"standard_input": "hello from test", "column_name": "new_col"}}

    @pytest.fixture
    def configured_node(self, UserDefinedNode, settings_dict: dict[str, Any]):
        """Provides a configured instance of the FixedColumn node."""
        return UserDefinedNode.from_settings(settings_dict)

    def test_build_from_settings(self, configured_node):
        """Tests that the node correctly populates its values from a dict."""
        section = configured_node.settings_schema.main_section
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

        expected_df = pl.DataFrame(
            {"a": [1, 2, 3], "new_col": ["hello from test", "hello from test", "hello from test"]}
        )

        assert_frame_equal(result_df, expected_df)


class TestColumnSelectorInNodeSettings:
    """Tests for ColumnSelector integration within NodeSettings and CustomNodeBase."""

    @pytest.fixture
    def numeric_column_selector_node(self):
        """Creates a node with a ColumnSelector filtered to numeric types."""
        from flowfile_core.flowfile.node_designer import ColumnSelector, CustomNodeBase, NodeSettings, Section, Types

        input_section = Section(
            title="input",
            numeric_columns=ColumnSelector(
                label="Select numeric columns",
                required=True,
                multiple=True,
                data_types=Types.Numeric,
            ),
        )

        class ColumnsToStringSettings(NodeSettings):
            input: Section = input_section

        class ColumnsToString(CustomNodeBase):
            node_name: str = "Columns to String"
            node_category: str = "Convert"
            title: str = "Convert columns to String"
            intro: str = "Select columns that are numeric and convert them to string"
            number_of_inputs: int = 1
            number_of_outputs: int = 1
            settings_schema: ColumnsToStringSettings = ColumnsToStringSettings()

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                lf = inputs[0]
                selected_cols = self.settings_schema.input.numeric_columns.value or []
                expr = [
                    pl.col(col).cast(pl.Utf8) if col in selected_cols else pl.col(col)
                    for col in lf.collect_schema().names()
                ]
                return lf.select(expr)

        return ColumnsToString

    def test_column_selector_has_all_numeric_types(self, numeric_column_selector_node):
        """Verify that Types.Numeric is preserved as the 'Numeric' group token."""
        node = numeric_column_selector_node()
        selector = node.settings_schema.input.numeric_columns

        assert selector.data_types_filter == ["Numeric"]

    def test_column_selector_frontend_schema(self, numeric_column_selector_node):
        """Test that the frontend schema contains correct data_types."""
        node = numeric_column_selector_node()
        schema = node.get_frontend_schema()

        selector_schema = schema["settings_schema"]["input"]["components"]["numeric_columns"]

        assert selector_schema["component_type"] == "ColumnSelector"
        assert selector_schema["multiple"] is True
        assert selector_schema["required"] is True
        assert selector_schema["data_types"] == ["Numeric"]

    def test_column_selector_populate_values(self, numeric_column_selector_node):
        """Test that column selections can be populated from settings dict."""
        settings = {"input": {"numeric_columns": ["col_a", "col_b"]}}
        node = numeric_column_selector_node.from_settings(settings)

        assert node.settings_schema.input.numeric_columns.value == ["col_a", "col_b"]

    def test_column_selector_process_converts_selected_columns(self, numeric_column_selector_node):
        """Test that the process method correctly converts selected numeric columns to string."""
        settings = {"input": {"numeric_columns": ["int_col", "float_col"]}}
        node = numeric_column_selector_node.from_settings(settings)

        input_df = pl.DataFrame({"int_col": [1, 2, 3], "float_col": [1.5, 2.5, 3.5], "str_col": ["a", "b", "c"]}).lazy()

        result = node.process(input_df).collect()

        assert result["int_col"].dtype == pl.Utf8
        assert result["float_col"].dtype == pl.Utf8
        assert result["str_col"].dtype == pl.Utf8
        assert result["int_col"].to_list() == ["1", "2", "3"]
        assert result["float_col"].to_list() == ["1.5", "2.5", "3.5"]

    def test_column_selector_process_with_no_selection(self, numeric_column_selector_node):
        """Test that process returns unchanged data when no columns are selected."""
        node = numeric_column_selector_node()

        input_df = pl.DataFrame({"int_col": [1, 2, 3], "str_col": ["a", "b", "c"]}).lazy()

        result = node.process(input_df).collect()

        assert result["int_col"].dtype == pl.Int64
        assert result["str_col"].dtype == pl.Utf8


class TestNumericGroupNormalization:
    """Both the 'numeric' alias and Types.Numeric normalize to the canonical 'Numeric'
    group token — groups are preserved, not expanded to their member types."""

    def test_numeric_string_alias_maps_to_group(self):
        selector = ColumnSelector(data_types="numeric")
        assert selector.data_types_filter == ["Numeric"]

    def test_types_numeric_maps_to_group(self):
        selector = ColumnSelector(data_types=Types.Numeric)
        assert selector.data_types_filter == ["Numeric"]


if __name__ == "__main__":
    pytest.main([__file__])
