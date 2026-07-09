"""Tests for the canonical DesignerState -> .py generator.

Covers: the fixed-point property against the corpus, a golden-source snapshot,
CodegenError cases, and ruff-format stability of the generated scaffolding.
"""

import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from flowfile_core.flowfile.node_designer.codegen import CodegenError, generate_source
from flowfile_core.flowfile.node_designer.parsing import parse_source
from flowfile_core.flowfile.node_designer.state import (
    ColumnSelectorState,
    DesignerState,
    EnvironmentState,
    ExampleInput,
    SectionState,
    SelectOption,
    SelectState,
    TextInputState,
)

CORPUS = Path(__file__).parent / "corpus"
IN_SUBSET = sorted(CORPUS.glob("insubset_*.py"))


def _designer_state(name: str) -> DesignerState:
    result = parse_source((CORPUS / name).read_text(encoding="utf-8"), file_name=name)
    assert result.mode == "designer", f"{name} did not parse as designer: {[i.code for i in result.issues]}"
    return result.designer_state


@pytest.mark.parametrize("name", [p.name for p in IN_SUBSET])
def test_generated_source_is_valid_python(name: str):
    import ast

    ast.parse(generate_source(_designer_state(name)))


@pytest.mark.parametrize("name", [p.name for p in IN_SUBSET])
def test_fixed_point_parse_of_generate(name: str):
    """parse(generate(s)).designer_state == s for every in-subset corpus state."""
    state = _designer_state(name)
    generated = generate_source(state)
    reparsed = parse_source(generated)
    assert reparsed.mode == "designer", [i.code for i in reparsed.issues]
    assert reparsed.designer_state == state


@pytest.mark.parametrize("name", [p.name for p in IN_SUBSET])
def test_generate_is_idempotent(name: str):
    """generate(parse(generate(s))) is byte-equal to generate(s)."""
    state = _designer_state(name)
    generated = generate_source(state)
    reparsed = parse_source(generated).designer_state
    assert generate_source(reparsed) == generated


def test_golden_docs_example():
    """Snapshot of the canonical form: import line, nd.-prefix, magic trailing commas."""
    state = _designer_state("insubset_docs_example.py")
    expected = (
        "import polars as pl\n"
        "from flowfile import node_designer as nd\n"
        "\n"
        "\n"
        "class GreetingSettings(nd.NodeSettings):\n"
        "    main_config: nd.Section = nd.Section(\n"
        '        title="Greeting Configuration",\n'
        '        description="Configure how to greet each row",\n'
        "        name_column=nd.ColumnSelector(\n"
        '            label="Name Column",\n'
        "            required=True,\n"
        "            data_types=[\n"
        '                "Categorical",\n'
        '                "String",\n'
        "            ],\n"
        "        ),\n"
        "        greeting=nd.SingleSelect(\n"
        '            label="Greeting",\n'
        "            options=[\n"
        '                ("formal", "Hello"),\n'
        '                ("casual", "Hey"),\n'
        "            ],\n"
        '            default="casual",\n'
        "        ),\n"
        "    )\n"
        "\n"
        "\n"
        "class GreetingNode(nd.CustomNodeBase):\n"
        '    node_name: str = "Greeting Generator"\n'
        '    node_category: str = "Text Processing"\n'
        '    title: str = "Add greetings"\n'
        '    intro: str = "Prefix a name column with a greeting."\n'
        "    settings_schema: GreetingSettings = GreetingSettings()\n"
        "\n"
        "    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:\n"
        "        df = inputs[0]\n"
        "        name_col = self.settings_schema.main_config.name_column.value\n"
        "        style = self.settings_schema.main_config.greeting.value\n"
        '        word = "Hello" if style == "formal" else "Hey"\n'
        "        return df.with_columns(\n"
        '            pl.concat_str([pl.lit(f"{word}, "), pl.col(name_col)]).alias("greeting")\n'
        "        )\n"
    )
    assert generate_source(state) == expected


# -- non-default-only emission -------------------------------------------------


def _minimal_state(**overrides) -> DesignerState:
    base = {
        "class_name": "MyNode",
        "settings_class_name": "MySettings",
        "node_name": "My Node",
        "process_code": "def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:\n    return inputs[0]",
    }
    base.update(overrides)
    return DesignerState(**base)


def test_defaults_are_omitted():
    src = generate_source(_minimal_state())
    assert "node_category" not in src
    assert "node_group" not in src
    assert "node_icon" not in src
    assert "environment" not in src
    assert "output_names" not in src
    assert "settings_schema" not in src  # no sections -> no settings class binding
    assert 'node_name: str = "My Node"' in src


def test_kernel_environment_and_dependencies_emitted():
    src = generate_source(
        _minimal_state(environment=EnvironmentState(kind="kernel", dependencies=["polars-ds==0.6.0"]))
    )
    assert 'environment: str = "kernel"' in src
    assert '"polars-ds==0.6.0"' in src


def test_example_inputs_and_settings_emitted():
    src = generate_source(
        _minimal_state(
            example_inputs=[ExampleInput(data={"a": [1, 2], "b": ["x", "y"]})],
            example_settings={"main": {"threshold": 5}},
        )
    )
    assert "example_inputs" in src
    assert "example_settings" in src
    # data must round-trip through the parser
    reparsed = parse_source(src).designer_state
    assert reparsed.example_inputs == [ExampleInput(data={"a": [1, 2], "b": ["x", "y"]})]
    assert reparsed.example_settings == {"main": {"threshold": 5}}


def test_incoming_columns_marker_emitted():
    section = SectionState(
        name="main",
        title="Main",
        components=[SelectState(name="col", component_type="SingleSelect", options_source="incoming_columns")],
    )
    src = generate_source(_minimal_state(sections=[section]))
    assert "options=nd.IncomingColumns" in src
    assert parse_source(src).designer_state.sections[0].components[0].options_source == "incoming_columns"


def test_section_layout_horizontal_emitted_and_round_trips():
    section = SectionState(
        name="main",
        title="Main",
        layout="horizontal",
        components=[TextInputState(name="note", label="Note")],
    )
    src = generate_source(_minimal_state(sections=[section]))
    assert 'layout="horizontal"' in src
    assert parse_source(src).designer_state.sections[0] == section


def test_section_default_layout_emits_no_kwarg():
    section = SectionState(name="main", title="Main", components=[TextInputState(name="note")])
    assert section.layout == "vertical"
    src = generate_source(_minimal_state(sections=[section]))
    assert "layout=" not in src


def test_empty_section_collapses_to_single_line():
    """A section with no kwargs and no components emits ``nd.Section()`` inline (ruff-stable)."""
    section = SectionState(name="bare")
    src = generate_source(_minimal_state(sections=[section]))
    assert "bare: nd.Section = nd.Section()" in src
    assert parse_source(src).designer_state.sections[0] == section
    stable = _ruff_stable(src)
    if stable is not None:
        assert stable


def test_empty_component_call_is_inline():
    """A component with no non-default kwargs emits ``nd.TextInput()`` inline."""
    section = SectionState(name="m", components=[TextInputState(name="t")])
    src = generate_source(_minimal_state(sections=[section]))
    assert "t=nd.TextInput()," in src
    assert parse_source(src).designer_state.sections[0] == section


def test_action_option_labels_emitted():
    from flowfile_core.flowfile.node_designer.state import ColumnActionInputState

    section = SectionState(
        name="main",
        components=[
            ColumnActionInputState(
                name="ops",
                actions=[SelectOption(value="sum", label="Sum"), SelectOption(value="min")],
            )
        ],
    )
    src = generate_source(_minimal_state(sections=[section]))
    assert 'nd.ActionOption("sum", "Sum")' in src
    assert '"min"' in src
    assert parse_source(src).designer_state.sections[0].components[0] == section.components[0]


# -- CodegenError cases --------------------------------------------------------


def test_codegen_error_on_keyword_class_name():
    # DesignerState validates identifiers on construction, so bypass via model_construct
    # to reach the generator's own guard.
    state = _minimal_state()
    bad = state.model_copy(update={"class_name": "class"})
    object.__setattr__(bad, "class_name", "class")
    with pytest.raises(CodegenError):
        generate_source(bad)


def test_codegen_error_on_bad_section_name():
    section = SectionState.model_construct(
        name="not an identifier",
        title=None,
        description=None,
        hidden=False,
        components=[],
    )
    with pytest.raises(CodegenError):
        generate_source(_minimal_state(sections=[section]))


def test_codegen_error_on_empty_process():
    with pytest.raises(CodegenError):
        generate_source(_minimal_state(process_code=""))


# -- ruff-format stability of the scaffolding ----------------------------------


def _ruff_stable(source: str) -> bool | None:
    ruff = shutil.which("ruff")
    if ruff is None:
        return None
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as f:
        f.write(source)
        path = f.name
    try:
        result = subprocess.run([ruff, "format", "--check", path], capture_output=True, text=True)
        return result.returncode == 0
    finally:
        Path(path).unlink(missing_ok=True)


def test_scaffolding_is_ruff_stable():
    """Generated scaffolding (with an already-ruff-clean process body) is untouched by ruff format."""
    state = _minimal_state(
        node_category="Text Processing",
        sections=[
            SectionState(
                name="main",
                title="Main",
                components=[
                    ColumnSelectorState(name="cols", label="Columns", required=True, data_types=["String"]),
                    TextInputState(name="prefix", label="Prefix", default="x"),
                ],
            )
        ],
        example_inputs=[ExampleInput(data={"a": [1, 2, 3], "b": ["p", "q", "r"]})],
    )
    src = generate_source(state)
    stable = _ruff_stable(src)
    if stable is None:
        pytest.skip("ruff not available")
    assert stable, f"generated scaffolding was not ruff-stable:\n{src}"
