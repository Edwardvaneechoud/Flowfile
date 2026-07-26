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
    assert issue.message == "arithmetic on string and numeric not allowed, try an explicit cast first"


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
