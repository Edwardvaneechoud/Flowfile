"""Corpus-driven tests for the pure-AST designer parser.

Every corpus file asserts its expected mode + issue codes; in-subset files
assert the lifted DesignerState field-by-field for the load-bearing parts.
"""

import textwrap
from pathlib import Path

from flowfile_core.flowfile.node_designer.parsing import (
    ParseIssueCode,
    extract_manifest,
    parse_file,
    parse_source,
)
from flowfile_core.flowfile.node_designer.state import (
    ColumnSelectorState,
    NumericInputState,
    SelectState,
    SliderInputState,
    TextInputState,
    ToggleSwitchState,
)

# Fixtures that are deliberately invalid Python use .pytxt so IDEs don't flag them.
CORPUS = Path(__file__).parent / "corpus"


def load(name: str) -> str:
    return (CORPUS / name).read_text(encoding="utf-8")


def parse(name: str):
    return parse_source(load(name), file_name=name)


def codes(result) -> set[str]:
    return {issue.code for issue in result.issues}


def warning_codes(result) -> set[str]:
    return {issue.code for issue in result.issues if issue.severity == "warning"}


def error_codes(result) -> set[str]:
    return {issue.code for issue in result.issues if issue.severity == "error"}


INLINE_SOURCES = {
    "non_literal_attr": """
        import polars as pl

        from flowfile import node_designer as nd

        NAME = "Configurable"


        class ConfigurableNode(nd.CustomNodeBase):
            node_name: str = NAME

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """,
    "unresolved_section_ref": """
        import polars as pl

        from flowfile import node_designer as nd


        class LostSettings(nd.NodeSettings):
            main: nd.Section = missing_section


        class LostNode(nd.CustomNodeBase):
            node_name: str = "Lost"
            settings_schema: LostSettings = LostSettings()

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """,
    "dynamic_settings": """
        import polars as pl

        from flowfile import node_designer as nd

        SECTIONS = {"main": nd.Section(title="Main")}


        class SplatNode(nd.CustomNodeBase):
            node_name: str = "Splat"
            settings_schema = nd.NodeSettings(**SECTIONS)

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """,
    "invalid_example_input": """
        import polars as pl

        from flowfile import node_designer as nd


        class BadExampleNode(nd.CustomNodeBase):
            node_name: str = "Bad Example"
            example_inputs: list = [{"a": range(3)}]

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """,
    "invalid_output_names": """
        import polars as pl

        from flowfile import node_designer as nd


        class DupNode(nd.CustomNodeBase):
            node_name: str = "Dup"
            number_of_outputs: int = 2
            output_names: list[str] = ["main", "main"]

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """,
    "process_signature_nonstandard": """
        import polars as pl

        from flowfile import node_designer as nd


        class SingleInputNode(nd.CustomNodeBase):
            node_name: str = "Single Input"

            def process(self, df):
                return df
        """,
}


def parse_inline(name: str):
    return parse_source(textwrap.dedent(INLINE_SOURCES[name]))


def test_docs_example_designer_state():
    result = parse("insubset_docs_example.py")
    assert result.mode == "designer"
    assert codes(result) == {ParseIssueCode.DATAFRAME_HINT.value}
    assert warning_codes(result) == {ParseIssueCode.DATAFRAME_HINT.value}

    state = result.designer_state
    assert state is not None
    assert state.schema_version == 1
    assert state.class_name == "GreetingNode"
    assert state.settings_class_name == "GreetingSettings"
    assert state.node_name == "Greeting Generator"
    assert state.node_category == "Text Processing"
    assert state.node_group == "custom"
    assert state.node_icon == "user-defined-icon.png"
    assert state.title == "Add greetings"
    assert state.intro == "Prefix a name column with a greeting."
    assert state.number_of_inputs == 1
    assert state.number_of_outputs == 1
    assert state.output_names == ["main"]
    assert state.environment.kind == "local"
    assert state.environment.dependencies == []
    assert state.environment.default_kernel_id is None
    assert state.example_inputs is None
    assert state.example_settings is None
    assert state.extra_imports == []
    assert state.module_extra == []
    assert state.class_extra == []

    assert len(state.sections) == 1
    section = state.sections[0]
    assert section.name == "main_config"
    assert section.title == "Greeting Configuration"
    assert section.description == "Configure how to greet each row"
    assert section.hidden is False
    assert section.layout == "vertical"

    name_column, greeting = section.components
    assert isinstance(name_column, ColumnSelectorState)
    assert name_column.name == "name_column"
    assert name_column.label == "Name Column"
    assert name_column.required is True
    assert name_column.multiple is False
    assert name_column.data_types == ["String"]  # Types.String preserved as the String group token

    assert isinstance(greeting, SelectState)
    assert greeting.component_type == "SingleSelect"
    assert greeting.name == "greeting"
    assert greeting.options_source == "static"
    assert [(o.value, o.label) for o in greeting.options] == [("formal", "Hello"), ("casual", "Hey")]
    assert greeting.default == "casual"

    expected_process = textwrap.dedent(
        """\
        def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
            df = inputs[0]
            name_col = self.settings_schema.main_config.name_column.value
            style = self.settings_schema.main_config.greeting.value
            word = "Hello" if style == "formal" else "Hey"
            return df.with_columns(
                pl.concat_str([pl.lit(f"{word}, "), pl.col(name_col)]).alias("greeting")
            )"""
    )
    assert state.process_code == expected_process


def test_docs_generated_sample_module_level_section():
    result = parse("insubset_docs_generated.py")
    assert result.mode == "designer"
    assert codes(result) == {ParseIssueCode.LEGACY_IMPORT_PATH.value, ParseIssueCode.DATAFRAME_HINT.value}
    assert error_codes(result) == set()

    state = result.designer_state
    assert state.class_name == "Prefixer"
    assert state.settings_class_name == "PrefixerSettings"
    assert state.node_name == "Prefixer"
    assert state.node_category == "Custom"
    assert state.node_icon == "ruler-plus.svg"
    assert state.number_of_inputs == 1
    assert state.number_of_outputs == 1

    assert len(state.sections) == 1
    section = state.sections[0]
    assert section.name == "main_section"
    assert section.title == "Section 1"

    prefix_text, columns_to_change = section.components
    assert isinstance(prefix_text, TextInputState)
    assert prefix_text.name == "prefix_text"
    assert prefix_text.label == "PrefixText"
    assert prefix_text.default is None

    assert isinstance(columns_to_change, ColumnSelectorState)
    assert columns_to_change.label == "Columns To Prefix"
    assert columns_to_change.required is True
    assert columns_to_change.multiple is True
    assert columns_to_change.data_types == "ALL"

    # the consumed module-level section and recognized imports leave nothing behind
    assert state.module_extra == []
    assert state.extra_imports == []
    assert "def process" in state.process_code
    assert "# Get the first input DataFrame" in state.process_code


def test_create_section_and_create_node_settings_forms():
    result = parse("insubset_create_section.py")
    assert result.mode == "designer"
    assert codes(result) == set()

    state = result.designer_state
    assert state.class_name == "ScalerNode"
    assert state.settings_class_name == "ScalerNodeSettings"  # synthesized: anonymous settings instance
    assert len(state.sections) == 1
    section = state.sections[0]
    assert section.name == "main"
    assert section.title == "Main"

    (factor,) = section.components
    assert isinstance(factor, NumericInputState)
    assert factor.name == "factor"
    assert factor.default == 2.0
    assert factor.min_value == 0.0
    assert factor.max_value == 10.0
    assert state.module_extra == []


def test_nd_attribute_style_components():
    result = parse("insubset_nd_attribute.py")
    assert result.mode == "designer"
    assert codes(result) == set()

    state = result.designer_state
    assert [s.name for s in state.sections] == ["columns", "tuning"]

    target_columns, method = state.sections[0].components
    assert isinstance(target_columns, SelectState)
    assert target_columns.component_type == "MultiSelect"
    assert target_columns.options_source == "incoming_columns"
    assert target_columns.options == []

    assert isinstance(method, SelectState)
    assert method.component_type == "SingleSelect"
    assert method.options_source == "static"
    assert [(o.value, o.label) for o in method.options] == [("drop", None), ("fill", None)]
    assert method.default == "drop"

    threshold, strict = state.sections[1].components
    assert isinstance(threshold, SliderInputState)
    assert threshold.min_value == 0.0
    assert threshold.max_value == 1.0
    assert threshold.step == 0.1
    assert threshold.default == 0.5

    assert isinstance(strict, ToggleSwitchState)
    assert strict.default is True
    assert strict.description == "Fail on unknown columns"


def _layout_source(layout_kwarg: str) -> str:
    return textwrap.dedent(
        f"""
        import polars as pl

        from flowfile import node_designer as nd


        class LayoutSettings(nd.NodeSettings):
            main: nd.Section = nd.Section(
                title="Main",
                {layout_kwarg}
                threshold=nd.NumericInput(label="Threshold"),
            )


        class LayoutNode(nd.CustomNodeBase):
            node_name: str = "Layout"
            settings_schema: LayoutSettings = LayoutSettings()

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """
    )


def test_section_layout_horizontal_lifts_and_round_trips():
    result = parse_source(_layout_source('layout="horizontal",'))
    assert result.mode == "designer"
    assert error_codes(result) == set()
    section = result.designer_state.sections[0]
    assert section.layout == "horizontal"

    from flowfile_core.flowfile.node_designer.codegen import generate_source

    generated = generate_source(result.designer_state)
    assert 'layout="horizontal"' in generated
    reparsed = parse_source(generated)
    assert reparsed.designer_state == result.designer_state


def test_section_layout_defaults_to_vertical_and_emits_no_kwarg():
    result = parse_source(_layout_source(""))
    assert result.mode == "designer"
    section = result.designer_state.sections[0]
    assert section.layout == "vertical"

    from flowfile_core.flowfile.node_designer.codegen import generate_source

    assert "layout=" not in generate_source(result.designer_state)


def test_section_invalid_layout_is_code_only():
    result = parse_source(_layout_source('layout="sideways",'))
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_COMPONENT_KWARG.value in error_codes(result)


def test_section_visible_when_lifts_and_round_trips():
    result = parse("insubset_section_visible_when.py")
    assert result.mode == "designer"
    assert codes(result) == set()
    sections = {s.name: s for s in result.designer_state.sections}
    assert sections["main"].visible_when is None
    assert sections["advanced"].visible_when.field == "main.show_advanced"
    assert sections["advanced"].visible_when.equals is True
    assert sections["inverted"].visible_when.field == "main.show_advanced"
    assert sections["inverted"].visible_when.equals is False

    from flowfile_core.flowfile.node_designer.codegen import generate_source

    generated = generate_source(result.designer_state)
    assert "visible_when=nd.VisibleWhen(" in generated
    assert 'field="main.show_advanced"' in generated
    # equals=True (advanced) is the default and omitted; only equals=False (inverted) is emitted.
    assert generated.count("equals=") == 1
    assert "equals=False" in generated
    reparsed = parse_source(generated)
    assert reparsed.designer_state == result.designer_state


def test_section_visible_when_dict_form_lifts_and_normalizes():
    """A hand-written dict is accepted and normalized to the canonical nd.VisibleWhen(...) on save."""
    result = parse_source(_layout_source('visible_when={"field": "main.flag"},'))
    assert result.mode == "designer"
    assert error_codes(result) == set()
    section = result.designer_state.sections[0]
    assert section.visible_when.field == "main.flag"
    assert section.visible_when.equals is True

    from flowfile_core.flowfile.node_designer.codegen import generate_source

    generated = generate_source(result.designer_state)
    assert "visible_when=nd.VisibleWhen(" in generated
    assert 'field="main.flag"' in generated


def test_section_visible_when_non_dict_is_code_only():
    result = parse_source(_layout_source('visible_when="main.flag",'))
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_COMPONENT_KWARG.value in error_codes(result)


def test_multi_input_node_without_settings():
    result = parse("insubset_multi_input.py")
    assert result.mode == "designer"
    assert codes(result) == set()

    state = result.designer_state
    assert state.class_name == "StackNode"
    assert state.settings_class_name == "StackNodeSettings"  # synthesized: no settings_schema
    assert state.number_of_inputs == 2
    assert state.number_of_outputs == 1
    assert state.sections == []


def test_multi_output_with_output_names():
    result = parse("insubset_multi_output.py")
    assert result.mode == "designer"
    assert codes(result) == set()

    state = result.designer_state
    assert state.number_of_outputs == 2
    assert state.output_names == ["pass", "fail"]
    (split_column,) = state.sections[0].components
    assert split_column.data_types == ["Boolean"]


def test_example_inputs_and_example_settings_round_trip():
    result = parse("insubset_examples.py")
    assert result.mode == "designer"
    assert codes(result) == set()

    state = result.designer_state
    assert state.example_inputs is not None
    assert len(state.example_inputs) == 1
    assert state.example_inputs[0].data == {"name": ["Alice", "Bob"], "age": [30, 40]}
    assert state.example_settings == {"main": {"name_column": "name", "greeting": "Hi"}}


def test_verbatim_escape_hatches_preserved_in_order():
    result = parse("insubset_verbatim.py")
    assert result.mode == "designer"
    assert codes(result) == set()

    state = result.designer_state
    assert state.extra_imports == ["import datetime", "import math"]

    assert len(state.module_extra) == 3
    assert state.module_extra[0] == "EPOCH = datetime.date(1970, 1, 1)"
    assert state.module_extra[1] == "DEFAULT_PRECISION = 4"
    assert state.module_extra[2].startswith("def _round_half_up(value: float, digits: int) -> float:")

    assert len(state.class_extra) == 2
    assert state.class_extra[0].startswith("def _apply(self, value: float, digits: int) -> float:")
    assert state.class_extra[1].startswith("@staticmethod\ndef days_since_epoch")

    assert state.process_code.startswith("def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:")
    assert "DEFAULT_PRECISION" in state.process_code


def test_legacy_kernel_fields_normalized():
    result = parse("insubset_legacy_kernel.py")
    assert result.mode == "designer"
    assert codes(result) == {ParseIssueCode.LEGACY_KERNEL_FIELDS.value}
    assert warning_codes(result) == {ParseIssueCode.LEGACY_KERNEL_FIELDS.value}

    state = result.designer_state
    assert state.environment.kind == "kernel"
    assert state.environment.default_kernel_id == "kernel-abc"
    assert state.environment.dependencies == []
    assert state.number_of_outputs == 2
    assert state.output_names == ["main", "metrics"]


def test_new_style_environment_and_dependencies():
    result = parse("insubset_environment_kernel.py")
    assert result.mode == "designer"
    assert codes(result) == set()

    state = result.designer_state
    assert state.environment.kind == "kernel"
    assert state.environment.dependencies == ["scikit-learn>=1.4", "xgboost"]
    assert state.environment.default_kernel_id is None


def test_shared_sdk_import_is_not_legacy():
    result = parse("insubset_shared_import.py")
    assert result.mode == "designer"
    assert codes(result) == set()

    state = result.designer_state
    assert state.class_name == "TrimNode"
    (column,) = state.sections[0].components
    assert column.data_types == ["String"]


def test_publish_metadata_fields_lift():
    result = parse("insubset_publish_metadata.py")
    assert result.mode == "designer"
    assert codes(result) == set()

    state = result.designer_state
    assert state.author == "Jane Doe"
    assert state.version == "1.2.0"
    assert state.tags == ["text", "demo"]


def test_publish_metadata_tags_must_be_list_of_strings():
    source = (
        "import polars as pl\n"
        "from flowfile import node_designer as nd\n\n\n"
        "class BadTags(nd.CustomNodeBase):\n"
        '    node_name: str = "Bad Tags"\n'
        '    tags: str = "not-a-list"\n\n'
        "    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n"
        "        return inputs[0]\n"
    )
    result = parse_source(source)
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_ATTR.value in error_codes(result)


def test_section_builder_is_code_only():
    result = parse("codeonly_section_builder.py")
    assert result.mode == "code_only"
    assert result.designer_state is None
    assert ParseIssueCode.UNSUPPORTED_CONSTRUCT.value in error_codes(result)


def test_dynamic_kwargs_are_code_only():
    result = parse("codeonly_dynamic_kwargs.py")
    assert result.mode == "code_only"
    assert result.designer_state is None
    assert error_codes(result) == {ParseIssueCode.NON_LITERAL_COMPONENT_KWARG.value}
    non_literal = [i for i in result.issues if i.code == ParseIssueCode.NON_LITERAL_COMPONENT_KWARG.value]
    assert len(non_literal) == 2  # f-string title + name-reference default


def test_two_node_classes_are_code_only():
    result = parse("codeonly_two_classes.py")
    assert result.mode == "code_only"
    assert result.designer_state is None
    assert ParseIssueCode.MULTIPLE_NODE_CLASSES.value in error_codes(result)


def test_zero_node_classes_are_code_only():
    result = parse("codeonly_no_class.py")
    assert result.mode == "code_only"
    assert result.designer_state is None
    assert error_codes(result) == {ParseIssueCode.NO_NODE_CLASS.value}


def test_syntax_error_is_code_only_with_line():
    result = parse("codeonly_syntax_error.pytxt")
    assert result.mode == "code_only"
    assert result.designer_state is None
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert issue.code == ParseIssueCode.SYNTAX_ERROR.value
    assert issue.severity == "error"
    assert issue.line is not None


def test_unknown_component_type_is_code_only():
    result = parse("codeonly_unknown_component.py")
    assert result.mode == "code_only"
    assert error_codes(result) == {ParseIssueCode.UNKNOWN_COMPONENT_TYPE.value}
    (issue,) = [i for i in result.issues if i.severity == "error"]
    assert "HoloDeck" in issue.message


def test_unknown_component_kwarg_is_code_only():
    result = parse("codeonly_unknown_kwarg.py")
    assert result.mode == "code_only"
    assert error_codes(result) == {ParseIssueCode.UNKNOWN_COMPONENT_KWARG.value}
    (issue,) = [i for i in result.issues if i.severity == "error"]
    assert "wobble" in issue.message


def test_non_literal_recognized_attr():
    result = parse_inline("non_literal_attr")
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_ATTR.value in error_codes(result)


def test_unresolved_section_ref():
    result = parse_inline("unresolved_section_ref")
    assert result.mode == "code_only"
    assert ParseIssueCode.UNRESOLVED_SECTION_REF.value in error_codes(result)


def test_dynamic_settings_via_kwargs_unpacking():
    result = parse_inline("dynamic_settings")
    assert result.mode == "code_only"
    assert ParseIssueCode.DYNAMIC_SETTINGS.value in error_codes(result)


def test_invalid_example_input():
    result = parse_inline("invalid_example_input")
    assert result.mode == "code_only"
    assert ParseIssueCode.INVALID_EXAMPLE_INPUT.value in error_codes(result)


def test_invalid_output_names():
    result = parse_inline("invalid_output_names")
    assert result.mode == "code_only"
    assert ParseIssueCode.INVALID_OUTPUT_NAMES.value in error_codes(result)


def test_output_names_count_mismatch():
    source = textwrap.dedent(
        """
        import polars as pl

        from flowfile import node_designer as nd


        class MismatchNode(nd.CustomNodeBase):
            node_name: str = "Mismatch"
            number_of_outputs: int = 3
            output_names: list[str] = ["a", "b"]

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """
    )
    result = parse_source(source)
    assert result.mode == "code_only"
    assert ParseIssueCode.INVALID_OUTPUT_NAMES.value in error_codes(result)


def test_process_signature_warning_stays_designer_mode():
    result = parse_inline("process_signature_nonstandard")
    assert result.mode == "designer"
    assert result.designer_state is not None
    assert codes(result) == {ParseIssueCode.PROCESS_SIGNATURE_NONSTANDARD.value}
    assert warning_codes(result) == {ParseIssueCode.PROCESS_SIGNATURE_NONSTANDARD.value}


def test_every_issue_code_is_exercised():
    observed: set[str] = set()
    for path in sorted([*CORPUS.glob("*.py"), *CORPUS.glob("*.pytxt")]):
        observed |= codes(parse_file(path))
    for name in INLINE_SOURCES:
        observed |= codes(parse_inline(name))
    all_codes = {code.value for code in ParseIssueCode}
    assert observed == all_codes


def test_designer_state_presence_matches_mode():
    for path in sorted([*CORPUS.glob("*.py"), *CORPUS.glob("*.pytxt")]):
        result = parse_file(path)
        assert (result.designer_state is None) == (result.mode == "code_only"), path.name


def test_parse_file_matches_parse_source():
    path = CORPUS / "insubset_docs_example.py"
    from_file = parse_file(path)
    from_source = parse_source(path.read_text(encoding="utf-8"), file_name=path.name)
    assert from_file == from_source


def test_extract_manifest_docs_example():
    manifest = extract_manifest(load("insubset_docs_example.py"))
    assert manifest.class_name == "GreetingNode"
    assert manifest.node_name == "Greeting Generator"
    assert manifest.node_category == "Text Processing"
    assert manifest.title == "Add greetings"
    assert manifest.intro == "Prefix a name column with a greeting."
    assert manifest.environment.kind == "local"


def test_extract_manifest_never_fails_hard():
    manifest = extract_manifest(load("codeonly_syntax_error.pytxt"))
    assert manifest.node_name is None
    assert manifest.class_name is None

    manifest = extract_manifest(load("codeonly_no_class.py"))
    assert manifest.node_name is None

    manifest = extract_manifest("")
    assert manifest.node_name is None


def test_extract_manifest_publish_metadata():
    manifest = extract_manifest(load("insubset_publish_metadata.py"))
    assert manifest.author == "Jane Doe"
    assert manifest.version == "1.2.0"
    assert manifest.tags == ["text", "demo"]

    # absent fields stay at their defaults
    manifest = extract_manifest(load("insubset_docs_example.py"))
    assert manifest.author == ""
    assert manifest.version == ""
    assert manifest.tags == []


def test_extract_manifest_environment_normalization():
    legacy = extract_manifest(load("insubset_legacy_kernel.py"))
    assert legacy.environment.kind == "kernel"
    assert legacy.environment.default_kernel_id == "kernel-abc"
    assert legacy.number_of_outputs == 2
    assert legacy.output_names == ["main", "metrics"]

    new_style = extract_manifest(load("insubset_environment_kernel.py"))
    assert new_style.environment.kind == "kernel"
    assert new_style.environment.dependencies == ["scikit-learn>=1.4", "xgboost"]

    # dynamic attrs are skipped, not fatal
    manifest = extract_manifest(load("codeonly_dynamic_kwargs.py"))
    assert manifest.class_name == "DynamicNode"
    assert manifest.node_name == "Dynamic Node"


# -- AvailableArtifacts marker call parsing ------------------------------------


def _select_source(options_expr: str) -> str:
    return textwrap.dedent(
        f"""
        import polars as pl

        from flowfile import node_designer as nd


        class ArtifactSettings(nd.NodeSettings):
            main: nd.Section = nd.Section(
                title="Main",
                artifact=nd.SingleSelect(label="Artifact", options={options_expr}),
            )


        class ArtifactNode(nd.CustomNodeBase):
            node_name: str = "Artifact Node"
            settings_schema: ArtifactSettings = ArtifactSettings()

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """
    )


def _artifact_select(options_expr: str) -> SelectState:
    result = parse_source(_select_source(options_expr))
    assert result.mode == "designer", [i.message for i in result.issues]
    (component,) = result.designer_state.sections[0].components
    assert isinstance(component, SelectState)
    return component


def test_available_artifacts_bare_marker():
    comp = _artifact_select("nd.AvailableArtifacts")
    assert comp.options_source == "available_artifacts"
    assert comp.artifact_scope == "upstream"
    assert comp.artifact_type_filter == []


def test_available_artifacts_kwarg_form():
    comp = _artifact_select('nd.AvailableArtifacts(scope="global", type=["a.*", "b.*"])')
    assert comp.options_source == "available_artifacts"
    assert comp.artifact_scope == "global"
    assert comp.artifact_type_filter == ["a.*", "b.*"]


def test_available_artifacts_scope_all():
    comp = _artifact_select('nd.AvailableArtifacts(scope="all")')
    assert comp.options_source == "available_artifacts"
    assert comp.artifact_scope == "all"
    assert comp.artifact_type_filter == []


def test_available_artifacts_positional_scope():
    comp = _artifact_select('nd.AvailableArtifacts("global")')
    assert comp.artifact_scope == "global"
    assert comp.artifact_type_filter == []


def test_available_artifacts_type_as_string():
    comp = _artifact_select('nd.AvailableArtifacts(type="sklearn.*")')
    assert comp.artifact_scope == "upstream"
    assert comp.artifact_type_filter == ["sklearn.*"]


def test_available_artifacts_type_list_normalizes_sorted_unique():
    comp = _artifact_select('nd.AvailableArtifacts(type=["b.*", "a.*", "a.*"])')
    assert comp.artifact_type_filter == ["a.*", "b.*"]


def test_available_artifacts_bad_scope_is_code_only():
    result = parse_source(_select_source('nd.AvailableArtifacts(scope="sideways")'))
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_COMPONENT_KWARG.value in error_codes(result)


def test_available_artifacts_bad_scope_everywhere_is_code_only():
    result = parse_source(_select_source('nd.AvailableArtifacts(scope="everywhere")'))
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_COMPONENT_KWARG.value in error_codes(result)


def test_incoming_columns_call_is_still_rejected():
    """IncomingColumns/AvailableSecrets stay bare-only — calling them is not a designer literal."""
    result = parse_source(_select_source("nd.IncomingColumns()"))
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_COMPONENT_KWARG.value in error_codes(result)


def _select_default_source(default_expr: str) -> str:
    return textwrap.dedent(
        f"""
        import polars as pl

        from flowfile import node_designer as nd


        class ArtifactSettings(nd.NodeSettings):
            main: nd.Section = nd.Section(
                title="Main",
                artifact=nd.SingleSelect(label="Artifact", options=["a"], default={default_expr}),
            )


        class ArtifactNode(nd.CustomNodeBase):
            node_name: str = "Artifact Node"
            settings_schema: ArtifactSettings = ArtifactSettings()

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """
    )


def test_artifact_marker_bare_in_default_degrades_not_500():
    """A marker in default= (only valid for options) degrades to code_only, never raises."""
    result = parse_source(_select_default_source("nd.AvailableArtifacts"))
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_COMPONENT_KWARG.value in error_codes(result)


def test_artifact_marker_call_in_default_degrades_not_500():
    result = parse_source(_select_default_source('nd.AvailableArtifacts(scope="global")'))
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_COMPONENT_KWARG.value in error_codes(result)


# -- publishes lifting ---------------------------------------------------------


def _publishes_source(publishes_expr: str) -> str:
    return textwrap.dedent(
        f"""
        import polars as pl

        from flowfile import node_designer as nd


        class PublisherNode(nd.CustomNodeBase):
            node_name: str = "Publisher"
            publishes: list[nd.Artifact] = {publishes_expr}

            def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
                return inputs[0]
        """
    )


def test_publishes_artifact_calls_lift():
    result = parse_source(_publishes_source('[nd.Artifact("model"), nd.Artifact("scaler", type="sklearn.X")]'))
    assert result.mode == "designer"
    assert error_codes(result) == set()
    assert [(p.name, p.type) for p in result.designer_state.publishes] == [
        ("model", None),
        ("scaler", "sklearn.X"),
    ]


def test_publishes_plain_strings_canonicalize():
    result = parse_source(_publishes_source('["model", "scaler"]'))
    assert result.mode == "designer"
    assert [(p.name, p.type) for p in result.designer_state.publishes] == [("model", None), ("scaler", None)]


def test_publishes_mixed_entries_lift():
    result = parse_source(_publishes_source('["model", nd.Artifact("scaler", type="T")]'))
    assert result.mode == "designer"
    assert [(p.name, p.type) for p in result.designer_state.publishes] == [("model", None), ("scaler", "T")]


def test_publishes_empty_is_default():
    result = parse_source(_publishes_source("[]"))
    assert result.mode == "designer"
    assert result.designer_state.publishes == []


def test_publishes_non_literal_element_is_code_only():
    result = parse_source(_publishes_source("[SOME_NAME]"))
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_ATTR.value in error_codes(result)


def test_publishes_non_list_is_code_only():
    result = parse_source(_publishes_source('"model"'))
    assert result.mode == "code_only"
    assert ParseIssueCode.NON_LITERAL_ATTR.value in error_codes(result)


def test_extract_manifest_publishes_best_effort():
    manifest = extract_manifest(_publishes_source('[nd.Artifact("model"), "scaler", nd.Artifact("x", type="T")]'))
    assert [(p.name, p.type) for p in manifest.publishes] == [("model", None), ("scaler", None), ("x", "T")]

    # malformed entries are skipped, never fatal
    manifest = extract_manifest(_publishes_source("[SOME_NAME, nd.Artifact(123)]"))
    assert manifest.publishes == []
