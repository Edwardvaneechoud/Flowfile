"""Tests for the data-free expression check used by the static settings validation."""

import polars as pl
import pytest

from flowfile_core.flowfile._extensions.real_time_interface import check_expression

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


DATE_SCHEMA = {"d": pl.String, "when": pl.Date, "stamp": pl.Datetime, "n": pl.Int64}


def test_date_function_on_text_is_a_type_issue():
    issue = check_expression({"d": pl.String}, 'format_date([d], "%Y")')
    assert issue.kind == "type"


def test_date_function_on_a_date_column_stays_silent():
    assert check_expression({"d": pl.Date}, 'format_date([d], "%Y")') is None


@pytest.mark.parametrize("expression", [
    'format_date([d], "%A, %d %B, %Y")',
    "year([d])",
    "month([d])",
    "day([d])",
])
def test_temporal_namespace_on_text_is_caught(expression):
    # collect_schema() answered these without type-checking .dt, so they validated clean
    assert check_expression(DATE_SCHEMA, expression).kind == "type"


def test_type_message_names_the_column_and_the_fix():
    issue = check_expression(DATE_SCHEMA, 'format_date([d], "%A, %d %B, %Y")')
    assert issue.message == (
        "format_date needs a Date or Datetime column; 'd' is text "
        '— wrap it in to_date([d], "%Y-%m-%d")'
    )


def test_time_of_day_function_suggests_to_datetime():
    issue = check_expression(DATE_SCHEMA, "hour([d])")
    assert issue.message == (
        "hour needs a Datetime column; 'd' is text "
        '— wrap it in to_datetime([d], "%Y-%m-%d %H:%M:%S")'
    )


def test_the_suggested_wrap_is_itself_valid():
    assert check_expression(DATE_SCHEMA, 'format_date(to_date([d], "%Y-%m-%d"), "%Y")') is None
    assert check_expression(DATE_SCHEMA, 'hour(to_datetime([d], "%Y-%m-%d %H:%M:%S"))') is None


@pytest.mark.parametrize("expression", [
    'format_date([d], "%Y") + [text2]',  # two text columns: which one is wrong would be a guess
    "year([n])",  # not the text shape at all
])
def test_unreadable_shapes_keep_the_polars_wording(expression):
    issue = check_expression(DATE_SCHEMA | SCHEMA, expression)
    assert issue.kind == "type"
    assert "wrap it in" not in issue.message


def test_a_function_name_inside_a_string_literal_is_not_a_call():
    issue = check_expression(DATE_SCHEMA, 'format_date([d], "year(")')
    assert issue.message.startswith("format_date needs a Date or Datetime column")


def test_predicate_over_text_date_is_caught():
    issue = check_expression(DATE_SCHEMA, 'format_date([d], "%Y") == "2005"', as_predicate=True)
    assert issue.kind == "type"

    assert check_expression(DATE_SCHEMA, 'format_date([when], "%Y") == "2005"', as_predicate=True) is None


@pytest.mark.parametrize("expression", [
    "[number] + 1",
    "round([number], 2)",
    "to_string([text2])",
    'concat([text2], "x")',
    "greatest([number], 2)",
    'if [number] > 1 then "a" else [text2] endif',
])
def test_zero_row_probe_does_not_reject_working_expressions(expression):
    # the probe frame is empty; nothing may fail merely because there are no rows
    assert check_expression(SCHEMA, expression) is None


@pytest.mark.parametrize("expression", ["[number] > 1", '[number] > 1 and [text2] == "a"'])
def test_zero_row_probe_keeps_predicates_silent(expression):
    assert check_expression(SCHEMA, expression, as_predicate=True) is None
