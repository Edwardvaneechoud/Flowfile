"""Multi-entry formula node: sequential evaluation, attribution and compatibility."""

import os

import polars as pl
import pytest
import yaml
from polars.testing import assert_frame_equal

# Imported for its side effect: the ff code generator falls back to the legacy
# flowfile_formulas emission when flowfile_frame cannot be imported, so the pinned
# native emission below is only deterministic once it is on the path.
import flowfile_frame.expr  # noqa: F401
from flowfile_core.flowfile.flow_data_engine.formula_entries import FormulaEntry, FormulaEntryError
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.schemas import input_schema, schemas, transform_schema


BASE_ROWS = [
    {"First": "Ann", "Last": "Lee", "Email": "ann@acme.com", "Hired": "2015-06-01"},
    {"First": "Bob", "Last": "Ray", "Email": "bob@ext.com", "Hired": "2022-01-15"},
    {"First": "Cid", "Last": "Fox", "Email": "cid@acme.com", "Hired": "2023-09-30"},
]

WORKED_EXAMPLE: list[tuple[str, str, str]] = [
    ("FullName", '[First] + " " + [Last]', "String"),
    ("Domain", 'if contains([Email], "@acme.com") then "acme.com" else "external" endif', "String"),
    ("IsInternal", 'if [Domain] = "acme.com" then 1 else 0 endif', "Integer"),
    ("Tenure", "year(today()) - year(to_date([Hired]))", "Integer"),
    ("Segment", 'if [IsInternal] = 1 and [Tenure] > 2 then "veteran" else "other" endif', "String"),
]


def make_entry(name: str, expression: str, data_type: str = "Auto") -> transform_schema.FunctionInput:
    return transform_schema.FunctionInput(
        field=transform_schema.FieldInput(name=name, data_type=data_type),
        function=expression,
    )


def create_graph(flow_id: int = 1, execution_mode: str = "Development") -> FlowGraph:
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id,
            name="formula_multi_entry",
            path=".",
            execution_mode=execution_mode,
            execution_location="local",
        )
    )
    return handler.get_flow(flow_id)


def add_source(graph: FlowGraph, rows: list[dict] | None = None, node_id: int = 1) -> FlowGraph:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=graph.flow_id,
            node_id=node_id,
            raw_data_format=input_schema.RawData.from_pylist(rows if rows is not None else BASE_ROWS),
        )
    )
    return graph


def add_formula_node(
    graph: FlowGraph,
    entries: list[transform_schema.FunctionInput],
    node_id: int = 2,
    depending_on_id: int = 1,
) -> None:
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="formula"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depending_on_id, node_id))
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=graph.flow_id,
            node_id=node_id,
            depending_on_id=depending_on_id,
            functions=entries,
        )
    )


def build_multi_entry_graph(
    specs: list[tuple[str, str, str]], rows: list[dict] | None = None
) -> FlowGraph:
    """One formula node holding every spec as an entry."""
    graph = add_source(create_graph(), rows)
    add_formula_node(graph, [make_entry(*spec) for spec in specs])
    return graph


def build_chained_graph(specs: list[tuple[str, str, str]], rows: list[dict] | None = None) -> FlowGraph:
    """One single-entry formula node per spec, chained in the same order."""
    graph = add_source(create_graph(), rows)
    previous = 1
    for index, spec in enumerate(specs, start=2):
        add_formula_node(graph, [make_entry(*spec)], node_id=index, depending_on_id=previous)
        previous = index
    return graph


def run_and_collect(graph: FlowGraph, node_id: int) -> pl.DataFrame:
    graph.run_graph()
    node = graph.get_node(node_id)
    assert node.results.errors in (None, ""), node.results.errors
    return node.get_resulting_data().data_frame.collect()


def run_expecting_failure(graph: FlowGraph, node_id: int) -> str:
    graph.run_graph()
    errors = graph.get_node(node_id).results.errors
    assert errors, "expected the formula node to fail"
    return errors


# --- 1 / 2 / 9 / 10: sequential semantics ---------------------------------------------------


def test_worked_example_matches_chained_single_formula_nodes():
    multi = build_multi_entry_graph(WORKED_EXAMPLE)
    chained = build_chained_graph(WORKED_EXAMPLE)
    assert_frame_equal(
        run_and_collect(multi, 2),
        run_and_collect(chained, len(WORKED_EXAMPLE) + 1),
    )


def test_entry_reads_the_previous_entrys_output():
    graph = build_multi_entry_graph(
        [("Doubled", "[Value] * 2", "Auto"), ("Quadrupled", "[Doubled] * 2", "Auto")],
        rows=[{"Value": 1}, {"Value": 5}],
    )
    result = run_and_collect(graph, 2)
    assert result["Quadrupled"].to_list() == [4, 20]


def test_predicted_schema_matches_the_result_schema():
    graph = build_multi_entry_graph(WORKED_EXAMPLE)
    graph.get_node(1).get_resulting_data()
    predicted = graph.get_node(2).get_predicted_schema()
    assert predicted, "no predicted schema for the five-entry chain"
    predicted_pairs = [(c.name, c.data_type) for c in predicted]
    run_and_collect(graph, 2)
    actual_pairs = [(c.name, c.data_type) for c in graph.get_node(2).get_resulting_data().schema]
    assert predicted_pairs == actual_pairs


def test_per_entry_data_types_reach_the_output_schema():
    graph = build_multi_entry_graph(WORKED_EXAMPLE)
    result = run_and_collect(graph, 2)
    assert result.schema["FullName"] == pl.String
    assert result.schema["IsInternal"] == pl.Int64
    assert result.schema["Tenure"] == pl.Int64
    # "Auto" resolves to whatever the expression produces.
    auto_graph = build_multi_entry_graph([("Flag", 'contains([Email], "acme")', "Auto")])
    assert run_and_collect(auto_graph, 2).schema["Flag"] == pl.Boolean


# --- 3: forward references fail, order is preserved -----------------------------------------


def test_forward_reference_fails_and_names_the_referencing_entry():
    swapped = [WORKED_EXAMPLE[2], WORKED_EXAMPLE[1]]  # IsInternal before Domain
    graph = build_multi_entry_graph(swapped)
    errors = run_expecting_failure(graph, 2)
    assert errors.startswith('Formula 1 ("IsInternal"):')
    assert "column 'Domain' not found" in errors
    entries = graph.get_node(2).setting_input.entries
    assert [entry.field.name for entry in entries] == ["IsInternal", "Domain"]


# --- 4 / 5 / 7 / 8: shape invariants ---------------------------------------------------------


def test_row_count_and_column_order_invariants():
    graph = build_multi_entry_graph(
        [("FullName", '[First] + " " + [Last]', "String"), ("Email", "uppercase([Email])", "String")]
    )
    result = run_and_collect(graph, 2)
    assert result.height == len(BASE_ROWS)
    # Upstream order preserved (Email overwritten in place), new column appended last.
    assert result.columns == ["First", "Last", "Email", "Hired", "FullName"]


def test_duplicate_output_names_last_wins_and_column_appears_once():
    graph = build_multi_entry_graph(
        [("Tag", '"first"', "String"), ("Other", "1", "Integer"), ("Tag", '"second"', "String")]
    )
    result = run_and_collect(graph, 2)
    assert result.columns.count("Tag") == 1
    assert result["Tag"].to_list() == ["second"] * len(BASE_ROWS)
    # The overwritten column keeps the position its first entry gave it.
    assert result.columns == ["First", "Last", "Email", "Hired", "Tag", "Other"]


def test_overwriting_an_input_column_is_visible_to_later_entries():
    graph = build_multi_entry_graph(
        [("Value", "[Value] * 10", "Auto"), ("Derived", "[Value] + 1", "Auto")],
        rows=[{"Value": 1}, {"Value": 2}],
    )
    result = run_and_collect(graph, 2)
    assert result["Value"].to_list() == [10, 20]
    assert result["Derived"].to_list() == [11, 21]


# --- 6: zero entries --------------------------------------------------------------------------


def test_zero_entries_is_a_pass_through():
    graph = build_multi_entry_graph([])
    result = run_and_collect(graph, 2)
    source = graph.get_node(1).get_resulting_data().data_frame.collect()
    assert_frame_equal(result, source)


def test_blank_expression_entries_are_skipped():
    graph = build_multi_entry_graph([("Ignored", "   ", "String"), ("Kept", "1", "Integer")])
    result = run_and_collect(graph, 2)
    assert "Ignored" not in result.columns
    assert result["Kept"].to_list() == [1] * len(BASE_ROWS)


# --- 14 / 15: attribution ---------------------------------------------------------------------


def test_failure_in_entry_three_names_only_entry_three():
    graph = build_multi_entry_graph(
        [
            ("A", "1", "Integer"),
            ("B", "2", "Integer"),
            ("IsInternal", "[Nope] + 1", "Integer"),
            ("D", "4", "Integer"),
        ]
    )
    errors = run_expecting_failure(graph, 2)
    assert errors.splitlines()[0] == "Formula 3 (\"IsInternal\"): column 'Nope' not found"
    assert '"A"' not in errors
    assert '"B"' not in errors


def test_blank_rows_still_count_toward_the_reported_position():
    graph = build_multi_entry_graph([("A", "", "Integer"), ("B", "[Nope]", "Integer")])
    errors = run_expecting_failure(graph, 2)
    assert errors.startswith('Formula 2 ("B"):')


def test_missing_column_and_parse_failures_are_distinguishable():
    engine = build_multi_entry_graph([]).get_node(1).get_resulting_data()

    with pytest.raises(FormulaEntryError) as missing:
        engine.apply_sql_formulas([FormulaEntry(1, "X", "[Nope] + 1", None)])
    assert missing.value.kind == "missing_column"
    assert str(missing.value) == "Formula 1 (\"X\"): column 'Nope' not found"

    with pytest.raises(FormulaEntryError) as parse:
        engine.apply_sql_formulas([FormulaEntry(1, "X", "[First] +* ", None)])
    assert parse.value.kind == "parse"

    with pytest.raises(FormulaEntryError) as config:
        engine.apply_sql_formulas([FormulaEntry(3, "  ", "1", None)])
    assert config.value.kind == "config"
    assert str(config.value) == "Formula 3: output column name is empty"


def test_missing_column_entry_fails_instead_of_yielding_nulls():
    graph = build_multi_entry_graph([("Good", "1", "Integer"), ("Bad", "[Nope]", "Integer")])
    run_expecting_failure(graph, 2)
    assert graph.get_node(2).node_stats.has_completed_last_run is False
    engine = graph.get_node(1).get_resulting_data()
    with pytest.raises(FormulaEntryError):
        engine.apply_sql_formulas([FormulaEntry(1, "Bad", "[Nope]", None)]).data_frame.collect()


# --- 16: schema / serialisation compatibility -------------------------------------------------


def test_single_entry_exposes_the_same_object_on_both_fields():
    settings = input_schema.NodeFormula(flow_id=1, node_id=2, function=make_entry("X", "1"))
    assert settings.function is settings.functions[0]
    assert len(settings.entries) == 1


def test_missing_and_explicit_none_function_yield_zero_entries():
    assert input_schema.NodeFormula(flow_id=1, node_id=2).entries == []
    assert input_schema.NodeFormula(flow_id=1, node_id=2, function=None).entries == []


def test_functions_wins_when_both_fields_are_given():
    settings = input_schema.NodeFormula(
        flow_id=1,
        node_id=2,
        function=make_entry("Legacy", "1"),
        functions=[make_entry("A", "1"), make_entry("B", "2")],
    )
    assert [entry.field.name for entry in settings.entries] == ["A", "B"]
    assert settings.function is None


def test_dump_omits_function_unless_there_is_exactly_one_entry():
    one = input_schema.NodeFormula(flow_id=1, node_id=2, functions=[make_entry("A", "1")]).model_dump()
    assert one["function"]["field"]["name"] == "A"
    assert len(one["functions"]) == 1

    many = input_schema.NodeFormula(
        flow_id=1, node_id=2, functions=[make_entry("A", "1"), make_entry("B", "2")]
    ).model_dump()
    assert "function" not in many
    assert len(many["functions"]) == 2

    assert "function" not in input_schema.NodeFormula(flow_id=1, node_id=2).model_dump()


def test_node_hash_handles_the_list_field():
    graph = build_multi_entry_graph(WORKED_EXAMPLE)
    assert isinstance(graph.get_node(2).hash, str)


def test_legacy_flow_without_functions_opens_runs_and_resaves(tmp_path):
    api_graph = build_multi_entry_graph([("Total", "[Value] * 2", "Integer")], rows=[{"Value": 3}])
    expected = run_and_collect(api_graph, 2)

    legacy_path = tmp_path / "legacy.yaml"
    api_graph.save_flow(str(legacy_path))
    raw = yaml.safe_load(legacy_path.read_text())
    formula_node = next(node for node in raw["nodes"] if node["type"] == "formula")
    assert "functions" in formula_node["setting_input"]
    del formula_node["setting_input"]["functions"]  # what a pre-multi-entry Flowfile wrote
    legacy_path.write_text(yaml.safe_dump(raw))

    reopened = open_flow(legacy_path)
    settings = reopened.get_node(2).setting_input
    assert [entry.field.name for entry in settings.entries] == ["Total"]
    assert_frame_equal(run_and_collect(reopened, 2), expected)

    resaved_path = tmp_path / "resaved.yaml"
    reopened.save_flow(str(resaved_path))
    resaved = yaml.safe_load(resaved_path.read_text())
    resaved_settings = next(n for n in resaved["nodes"] if n["type"] == "formula")["setting_input"]
    assert len(resaved_settings["functions"]) == 1
    assert resaved_settings["function"]["field"]["name"] == "Total"


def test_multi_entry_flow_round_trips_through_save_and_open(tmp_path):
    graph = build_multi_entry_graph(WORKED_EXAMPLE)
    expected = run_and_collect(graph, 2)
    path = tmp_path / "multi.yaml"
    graph.save_flow(str(path))

    saved_settings = next(
        n for n in yaml.safe_load(path.read_text())["nodes"] if n["type"] == "formula"
    )["setting_input"]
    assert "function" not in saved_settings
    assert len(saved_settings["functions"]) == len(WORKED_EXAMPLE)

    reopened = open_flow(path)
    assert_frame_equal(run_and_collect(reopened, 2), expected)


def test_settings_validation_ignores_columns_an_earlier_entry_produces():
    from flowfile_core.flowfile.settings_validation import validate_flow_settings

    graph = build_multi_entry_graph(WORKED_EXAMPLE)
    graph.flow_settings.validate_settings = True
    result = validate_flow_settings(graph)
    assert [node for node in result.nodes if node.node_id == 2] == []


def test_settings_validation_flags_a_genuinely_missing_input_column():
    from flowfile_core.flowfile.settings_validation import validate_flow_settings

    graph = build_multi_entry_graph([("A", "1", "Integer"), ("B", "[Nope] + [A]", "Integer")])
    graph.flow_settings.validate_settings = True
    result = validate_flow_settings(graph)
    issues = [issue for node in result.nodes if node.node_id == 2 for issue in node.issues]
    assert any(issue.missing_columns == ["Nope"] for issue in issues)


# --- 18: code generation ----------------------------------------------------------------------


# Captured at the pre-multi-entry baseline (845818d0); a one-entry node must keep emitting
# exactly this. `string_similarity` has no native translation, so it pins the fallback branch.
ONE_ENTRY_PINS: list[tuple[str, str, list[tuple[str, str, str]], list[str]]] = [
    (
        "polars",
        "native",
        [("Total", "[Value] * 2", "Integer")],
        ['        .with_columns([(pl.col("Value") * pl.lit(2)).alias("Total").cast(pl.Int64)])'],
    ),
    (
        "polars",
        "fallback",
        [("Sim", "string_similarity([Name], 'x')", "Double")],
        [
            "        .with_columns([",
            "        simple_function_to_expr(\"string_similarity([Name], 'x')\").alias(\"Sim\")",
            "            .cast(pl.Float64)",
            "        ])",
        ],
    ),
    (
        "ff",
        "native",
        [("Total", "[Value] * 2", "Integer")],
        ['        .with_columns((ff.col("Value") * ff.lit(2)).alias("Total").cast(ff.Int64))'],
    ),
    (
        "ff",
        "fallback",
        [("Sim", "string_similarity([Name], 'x')", "Double")],
        [
            "        .with_columns(flowfile_formulas=[\"string_similarity([Name], 'x')\"], "
            "output_column_names=['Sim'], output_column_datatypes=['Double'])"
        ],
    ),
]


@pytest.mark.parametrize(
    ("dialect", "branch", "specs", "expected_lines"),
    ONE_ENTRY_PINS,
    ids=[f"{dialect}-{branch}" for dialect, branch, _, _ in ONE_ENTRY_PINS],
)
def test_one_entry_emission_is_unchanged(dialect, branch, specs, expected_lines):
    from flowfile_core.flowfile.code_generator.code_generator import (
        export_flow_to_flowframe,
        export_flow_to_polars,
    )

    graph = build_multi_entry_graph(specs, rows=[{"Value": 3, "Name": "a"}])
    exporter = export_flow_to_polars if dialect == "polars" else export_flow_to_flowframe
    lines = exporter(graph).splitlines()
    start = lines.index(expected_lines[0])
    assert lines[start : start + len(expected_lines)] == expected_lines


@pytest.mark.parametrize("dialect", ["polars", "ff"])
def test_multi_entry_emission_chains_and_matches_the_engine(dialect):
    from flowfile_core.flowfile.code_generator.code_generator import (
        export_flow_to_flowframe,
        export_flow_to_polars,
    )

    graph = build_multi_entry_graph(WORKED_EXAMPLE)
    expected = run_and_collect(graph, 2)
    exporter = export_flow_to_polars if dialect == "polars" else export_flow_to_flowframe
    code = exporter(graph)
    assert code.count(".with_columns(") == len(WORKED_EXAMPLE)

    exec_globals: dict = {}
    exec(code, exec_globals)
    result = exec_globals["run_etl_pipeline"]()
    if hasattr(result, "collect"):
        result = result.collect()
    assert_frame_equal(result, expected)


def test_zero_entry_emission_is_a_pass_through():
    from flowfile_core.flowfile.code_generator.code_generator import export_flow_to_polars

    graph = build_multi_entry_graph([])
    code = export_flow_to_polars(graph)
    exec_globals: dict = {}
    exec(code, exec_globals)
    assert_frame_equal(
        exec_globals["run_etl_pipeline"]().collect(),
        graph.get_node(1).get_resulting_data().data_frame.collect(),
    )


if __name__ == "__main__":
    pytest.main([os.path.abspath(__file__)])
