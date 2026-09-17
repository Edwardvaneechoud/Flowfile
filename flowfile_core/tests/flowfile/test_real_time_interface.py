"""Tests for the data-free expression check used by the static settings validation."""

import polars as pl
import pytest

from flowfile_core.flowfile._extensions.real_time_interface import (
    check_expression,
    check_expression_chain,
)
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.formula_entries import (
    FormulaEntryError,
    apply_formula_entries,
    entry_label,
    formula_entry,
)
from flowfile_core.schemas import transform_schema

SCHEMA = {"number": pl.Int32, "text2": pl.Utf8, "moment": pl.Date}


@pytest.mark.parametrize("expression", [
    "[number] + 1",
    "concat([text2], \"x\")",
    "if [number] > 1 then \"a\" else [text2] endif",
    "round([number], 2)",
    "to_string([number])",
])
def test_valid_expressions_return_none(expression):
    assert check_expression(SCHEMA, expression) is None


@pytest.mark.parametrize("expression", ["", "   ", None])
def test_blank_expression_returns_none(expression):
    assert check_expression(SCHEMA, expression) is None


def test_type_error_matches_the_runtime_message():
    issue = check_expression(SCHEMA, '[number] + "a"')
    assert issue.kind == "type"
    assert issue.message == (
        "arithmetic on dtypes i32 and str is not allowed "
        "(lhs: column 'number', rhs: expression `\"a\"`); try an explicit cast first"
    )


def test_type_error_message_is_a_single_short_line():
    issue = check_expression(SCHEMA, "[text2] / 2")
    assert "\n" not in issue.message
    assert "Resolved plan" not in issue.message
    assert len(issue.message) <= 200
    assert "division with 'String' datatypes is not allowed" in issue.message


def test_temporal_arithmetic_is_flagged():
    issue = check_expression(SCHEMA, "[moment] + 1")
    assert issue.kind == "type"


@pytest.mark.parametrize("expression", ["sum([number])", "((( [number]", "[number] + ${undefined}"])
def test_unparseable_expressions_are_parse_issues(expression):
    issue = check_expression(SCHEMA, expression)
    assert issue.kind == "parse"
    assert "\n" not in issue.message


def test_missing_column_is_classified_separately():
    issue = check_expression(SCHEMA, "[gone] + 1")
    assert issue.kind == "missing_column"


def test_predicate_mode_requires_a_boolean_result():
    assert check_expression(SCHEMA, "[number] > 1", as_predicate=True) is None

    issue = check_expression(SCHEMA, "[text2]", as_predicate=True)
    assert issue.kind == "type"
    assert "filter predicate must be of type" in issue.message

    # the same expression is perfectly valid as a column
    assert check_expression(SCHEMA, "[text2]") is None


def test_empty_schema_still_reports_missing_columns():
    issue = check_expression({}, "[number] + 1")
    assert issue.kind == "missing_column"


# --- formula entry chain -------------------------------------------------

CHAIN_SCHEMA = {"First": pl.Utf8, "Last": pl.Utf8, "Email": pl.Utf8, "Amount": pl.Int64}


def entry(name: str, expression: str, data_type: str | None = None) -> transform_schema.FunctionInput:
    return transform_schema.FunctionInput(
        field=transform_schema.FieldInput(name=name, data_type=data_type),
        function=expression,
    )


def test_entry_may_reference_the_entry_above_it():
    """criterion 11: entry N referencing entry N-1's output is correct, not an error."""
    result = check_expression_chain(CHAIN_SCHEMA, [
        entry("FullName", '[First] + " " + [Last]'),
        entry("Greeting", '"Hi " + [FullName]'),
    ])
    assert result.issues == [None, None]
    assert "FullName" in result.schemas[0]
    assert set(result.schemas[1]) == set(CHAIN_SCHEMA) | {"FullName", "Greeting"}


def test_forward_reference_is_flagged_on_the_referencing_entry_only():
    """criterion 12: entry N referencing entry N+1's output fails on N, not on N+1."""
    result = check_expression_chain(CHAIN_SCHEMA, [
        entry("Early", '"x" + [Late]'),
        entry("Late", '"y" + [First]'),
    ])
    assert result.issues[0].kind == "missing_column"
    assert "Late" in result.issues[0].message
    assert result.issues[1] is None


def test_a_broken_entry_does_not_cascade_onto_the_entries_below_it():
    """criterion 13: a failing entry still contributes its declared column."""
    result = check_expression_chain(CHAIN_SCHEMA, [
        entry("Broken", '[Amount] + "x"'),
        entry("Uses", '[Broken] + "!"'),
        entry("AlsoUses", "[Broken]", "String"),
    ])
    assert result.issues[0].kind == "type"
    assert result.issues[1] is None
    assert result.issues[2] is None
    assert result.schemas[0]["Broken"] == pl.String


def test_a_broken_entry_contributes_its_declared_type():
    result = check_expression_chain(CHAIN_SCHEMA, [
        entry("Broken", "[Missing] * 2", "Int64"),
        entry("Doubled", "[Broken] * 2"),
    ])
    assert result.issues[0].kind == "missing_column"
    assert result.schemas[0]["Broken"] == pl.Int64
    assert result.issues[1] is None


def test_blank_expression_is_skipped_everywhere():
    """criterion 26: a blank row yields no issue and contributes no column."""
    result = check_expression_chain(CHAIN_SCHEMA, [
        entry("Ignored", "   "),
        entry("Real", "[First]"),
    ])
    assert result.issues == [None, None]
    assert "Ignored" not in result.schemas[0]
    assert result.schemas[0] == CHAIN_SCHEMA
    assert "Real" in result.schemas[1]


def test_blank_output_name_with_an_expression_is_a_config_issue():
    result = check_expression_chain(CHAIN_SCHEMA, [entry("  ", "[First]")])
    assert result.issues[0].kind == "config"
    assert result.issues[0].message == "output column name is empty"
    assert result.schemas[0] == CHAIN_SCHEMA


def test_duplicate_output_names_warn_on_the_later_entry():
    result = check_expression_chain(CHAIN_SCHEMA, [
        entry("Tag", '"a"'),
        entry("Other", '"b"'),
        entry("Tag", '"c"'),
    ])
    assert result.issues[0] is None
    assert result.issues[1] is None
    assert result.issues[2].kind == "duplicate"
    assert "formula 1" in result.issues[2].message
    # the later entry wins in the schema
    assert result.schemas[2]["Tag"] == pl.String


def test_overwriting_an_input_column_is_not_a_duplicate():
    result = check_expression_chain(CHAIN_SCHEMA, [entry("First", '[First] + "!"')])
    assert result.issues == [None]


def test_declared_data_type_is_applied_like_the_engine():
    result = check_expression_chain(CHAIN_SCHEMA, [entry("AsText", "[Amount]", "String")])
    assert result.issues == [None]
    assert result.schemas[0]["AsText"] == pl.String


def test_auto_data_type_leaves_the_resolved_type_alone():
    result = check_expression_chain(CHAIN_SCHEMA, [entry("Doubled", "[Amount] * 2", "Auto")])
    assert result.issues == [None]
    assert result.schemas[0]["Doubled"] == pl.Int64


def test_base_schema_is_exposed_and_never_mutated():
    entries = [entry("Extra", "[First]")]
    result = check_expression_chain(CHAIN_SCHEMA, entries)
    assert result.base_schema == CHAIN_SCHEMA
    assert "Extra" not in result.base_schema
    assert len(result.schemas) == len(entries) == len(result.issues)


def test_entry_label_matches_the_runtime_error_prefix():
    """The editor prefix and the run-time FormulaEntryError label must read identically."""
    error = FormulaEntryError(3, "IsInternal", "column 'Domain' not found", "missing_column")
    assert str(error) == f"{entry_label(3, 'IsInternal')}: column 'Domain' not found"
    assert str(FormulaEntryError(2, "", "output column name is empty", "config")).startswith(entry_label(2, ""))


# --- agreement with the engine -------------------------------------------

AGREEMENT_CASES = [
    ("clean chain", [("A", "[Amount] * 2", None), ("B", "[A] + 1", None)], None),
    ("forward reference", [("A", "[B] + 1", None), ("B", "[Amount]", None)], (1, "missing_column")),
    ("unknown column", [("A", "[Nope] + 1", None)], (1, "missing_column")),
    ("type error", [("A", '[Amount] + "x"', None)], (1, "type")),
    ("parse error", [("A", "((( [Amount]", None)], (1, "parse")),
    ("blank name", [("", "[Amount]", None)], (1, "config")),
    ("failure below a good entry", [("A", "[Amount] * 2", None), ("B", '[A] + "x"', None)], (2, "type")),
    ("declared cast", [("A", "[Amount]", "String"), ("B", '[A] + "!"', None)], None),
    ("blank expression skipped", [("A", "  ", None), ("B", "[Amount] + 1", None)], None),
]


@pytest.mark.parametrize("label,rows,expected", AGREEMENT_CASES, ids=[c[0] for c in AGREEMENT_CASES])
def test_chain_check_agrees_with_apply_sql_formulas(label, rows, expected):
    """spec 6.3: the static check and the engine must reach the same verdict."""
    # A lazy frame, like every in-graph input: apply_sql_formulas attributes failures at
    # collect_schema, which an eager frame would raise past on with_columns.
    frame = FlowDataEngine(pl.LazyFrame({"Amount": [1, 2, 3]}))
    schema = {"Amount": pl.Int64}

    engine_entries = [
        formula_entry(position, entry(name, expression, dt))
        for position, (name, expression, dt) in enumerate(rows, start=1)
        if expression.strip()
    ]
    engine_failure = None
    try:
        engine_result = frame.apply_sql_formulas(engine_entries)
        engine_schema = dict(engine_result.data_frame.collect_schema())
    except FormulaEntryError as exc:
        engine_failure = (exc.position, exc.kind)
        engine_schema = None

    result = check_expression_chain(schema, [entry(n, e, dt) for n, e, dt in rows])
    first = next(
        ((position, issue.kind) for position, issue in enumerate(result.issues, start=1) if issue is not None),
        None,
    )
    assert first == engine_failure == expected, f"{label}: static={first} engine={engine_failure}"
    if engine_schema is not None:
        # criteria 24/28: the accumulated schema equals what the engine produces.
        assert result.schemas[-1] == engine_schema


def test_accumulated_schema_after_each_entry_matches_the_engine_step_by_step():
    """criterion 24: every intermediate schema, not just the last one."""
    rows = [("A", "[Amount] * 2", None), ("B", '[A] + 1', None), ("C", "[B]", "String")]
    base = pl.LazyFrame({"Amount": [1, 2, 3]})
    result = check_expression_chain({"Amount": pl.Int64}, [entry(n, e, dt) for n, e, dt in rows])
    for step in range(1, len(rows) + 1):
        entries = [
            formula_entry(position, entry(n, e, dt)) for position, (n, e, dt) in enumerate(rows[:step], start=1)
        ]
        engine = FlowDataEngine(base).apply_sql_formulas(entries)
        assert result.schemas[step - 1] == dict(engine.data_frame.collect_schema())


def test_apply_formula_entries_runs_entries_in_order():
    lf = pl.LazyFrame({"Amount": [5]})
    entries = [formula_entry(1, entry("A", "[Amount] * 2")), formula_entry(2, entry("B", "[A] + 1"))]
    assert apply_formula_entries(lf, entries).collect().item(0, "B") == 11


def test_apply_formula_entries_reports_the_failing_position():
    lf = pl.LazyFrame({"Amount": [5]})
    entries = [formula_entry(1, entry("A", "[Amount] * 2")), formula_entry(2, entry("B", "[Nope] + 1"))]
    with pytest.raises(FormulaEntryError) as failure:
        apply_formula_entries(lf, entries)
    assert failure.value.position == 2
    assert failure.value.kind == "missing_column"
