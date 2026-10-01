"""
Tests for the filter_expressions module.

Run with:
    pytest flowfile_core/tests/flowfile/test_filter_expressions.py -v

Tests cover:
- Helper functions for value checking and quoting
- Individual operator expression builders
- The main build_filter_expression function
- Edge cases and type inference
"""

import datetime as dt
import time
from decimal import Decimal

import polars as pl
import pytest
from polars_expr_transformer import simple_function_to_expr

from flowfile_core.flowfile.filter_expressions import (
    _build_between_expression,
    _build_comparison_expression,
    _build_contains_expression,
    _build_ends_with_expression,
    _build_equals_expression,
    _build_greater_than_expression,
    _build_greater_than_or_equals_expression,
    _build_in_expression,
    _build_is_not_null_expression,
    _build_is_null_expression,
    _build_less_than_expression,
    _build_less_than_or_equals_expression,
    _build_not_contains_expression,
    _build_not_equals_expression,
    _build_not_in_expression,
    _build_starts_with_expression,
    _format_field,
    _is_numeric_string,
    _should_quote_value,
    build_filter_expression,
    resolve_filter_field_type,
    supports_native_membership,
)
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas.transform_schema import BasicFilter, FilterOperator


# Helper Function Tests


class TestIsNumericString:
    """Tests for _is_numeric_string helper."""

    def test_integer(self):
        """Test integer string detection."""
        assert _is_numeric_string("123") is True
        assert _is_numeric_string("0") is True
        assert _is_numeric_string("999999") is True

    def test_negative_integer(self):
        """Test negative integer string detection."""
        assert _is_numeric_string("-123") is True
        assert _is_numeric_string("-1") is True

    def test_float(self):
        """Test float string detection."""
        assert _is_numeric_string("123.45") is True
        assert _is_numeric_string("0.5") is True
        assert _is_numeric_string(".5") is True  # Valid float without leading digit
        assert _is_numeric_string("5.") is True  # Valid float without trailing digit

    def test_negative_float(self):
        """Test negative float string detection."""
        assert _is_numeric_string("-123.45") is True
        assert _is_numeric_string("-0.5") is True
        assert _is_numeric_string("-.5") is True  # Negative without leading digit

    def test_non_numeric(self):
        """Test non-numeric string detection."""
        assert _is_numeric_string("abc") is False
        assert _is_numeric_string("12abc") is False
        assert _is_numeric_string("abc12") is False
        assert _is_numeric_string("1.2.3") is False  # Multiple dots
        assert _is_numeric_string("1-2") is False  # Dash not at start

    def test_empty_string(self):
        """Test empty string returns False."""
        assert _is_numeric_string("") is False

    def test_whitespace(self):
        """Test whitespace handling."""
        # float() strips whitespace, which is fine since values are pre-stripped in usage
        assert _is_numeric_string(" 123") is True
        assert _is_numeric_string("123 ") is True
        assert _is_numeric_string("  -12.5  ") is True

    def test_special_characters(self):
        """Test special characters return False."""
        assert _is_numeric_string("1,000") is False  # Comma
        assert _is_numeric_string("$100") is False
        assert _is_numeric_string("100%") is False


class TestShouldQuoteValue:
    """Tests for _should_quote_value helper."""

    def test_explicit_string_type(self):
        """Test that string type always quotes."""
        assert _should_quote_value("123", "str") is True
        assert _should_quote_value("abc", "str") is True

    def test_explicit_numeric_type(self):
        """Test that numeric type never quotes."""
        assert _should_quote_value("123", "numeric") is False
        assert _should_quote_value("abc", "numeric") is False  # Even non-numeric values

    def test_inferred_from_value(self):
        """Test type inference when field_data_type is None."""
        assert _should_quote_value("123", None) is False  # Numeric value
        assert _should_quote_value("abc", None) is True  # Non-numeric value

    def test_date_type(self):
        """Test date type (not explicitly handled, falls back to value check)."""
        assert _should_quote_value("2024-01-01", "date") is True  # Not numeric
        assert _should_quote_value("123", "date") is False  # Numeric


class TestFormatField:
    """Tests for _format_field helper."""

    def test_simple_field(self):
        """Test simple field name formatting."""
        assert _format_field("name") == "[name]"
        assert _format_field("age") == "[age]"

    def test_field_with_spaces(self):
        """Test field name with spaces."""
        assert _format_field("first name") == "[first name]"

    def test_field_with_special_chars(self):
        """Test field name with special characters."""
        assert _format_field("column_1") == "[column_1]"
        assert _format_field("col-name") == "[col-name]"


# Comparison Expression Builder Tests


class TestComparisonExpressionBuilders:
    """Tests for comparison expression builders."""

    def test_build_comparison_quoted(self):
        """Test comparison with quoted value."""
        result = _build_comparison_expression("[name]", "=", "John", True)
        assert result == '[name]="John"'

    def test_build_comparison_unquoted(self):
        """Test comparison with unquoted value."""
        result = _build_comparison_expression("[age]", ">", "30", False)
        assert result == "[age]>30"

    def test_equals_quoted(self):
        """Test equals expression with quoted value."""
        result = _build_equals_expression("[city]", "New York", True)
        assert result == '[city]="New York"'

    def test_equals_unquoted(self):
        """Test equals expression with unquoted value."""
        result = _build_equals_expression("[id]", "123", False)
        assert result == "[id]=123"

    def test_not_equals_quoted(self):
        """Test not equals expression with quoted value."""
        result = _build_not_equals_expression("[status]", "active", True)
        assert result == '[status]!="active"'

    def test_not_equals_unquoted(self):
        """Test not equals expression with unquoted value."""
        result = _build_not_equals_expression("[count]", "0", False)
        assert result == "[count]!=0"

    def test_greater_than(self):
        """Test greater than expression."""
        assert _build_greater_than_expression("[age]", "30", False) == "[age]>30"
        assert _build_greater_than_expression("[age]", "30", True) == '[age]>"30"'

    def test_greater_than_or_equals(self):
        """Test greater than or equals expression."""
        assert _build_greater_than_or_equals_expression("[age]", "18", False) == "[age]>=18"
        assert _build_greater_than_or_equals_expression("[age]", "18", True) == '[age]>="18"'

    def test_less_than(self):
        """Test less than expression."""
        assert _build_less_than_expression("[price]", "100", False) == "[price]<100"
        assert _build_less_than_expression("[price]", "100", True) == '[price]<"100"'

    def test_less_than_or_equals(self):
        """Test less than or equals expression."""
        assert _build_less_than_or_equals_expression("[score]", "50", False) == "[score]<=50"
        assert _build_less_than_or_equals_expression("[score]", "50", True) == '[score]<="50"'


# String Function Expression Builder Tests


class TestStringFunctionExpressionBuilders:
    """Tests for string function expression builders."""

    def test_contains(self):
        """Test contains expression."""
        result = _build_contains_expression("[name]", "John")
        assert result == 'contains([name], "John")'

    def test_not_contains(self):
        """Test not contains expression."""
        result = _build_not_contains_expression("[name]", "John")
        assert result == 'contains([name], "John") = false'

    def test_starts_with(self):
        """Test starts with expression."""
        result = _build_starts_with_expression("[name]", "Jo")
        assert result == 'left([name], 2) = "Jo"'

    def test_starts_with_longer_value(self):
        """Test starts with with longer value."""
        result = _build_starts_with_expression("[email]", "admin@")
        assert result == 'left([email], 6) = "admin@"'

    def test_ends_with(self):
        """Test ends with expression."""
        result = _build_ends_with_expression("[name]", "son")
        assert result == 'right([name], 3) = "son"'

    def test_ends_with_longer_value(self):
        """Test ends with with longer value."""
        result = _build_ends_with_expression("[email]", "@example.com")
        assert result == 'right([email], 12) = "@example.com"'


# Null Check Expression Builder Tests


class TestNullCheckExpressionBuilders:
    """Tests for null check expression builders."""

    def test_is_null(self):
        """Test is null expression."""
        result = _build_is_null_expression("[notes]")
        assert result == "is_empty([notes])"

    def test_is_not_null(self):
        """Test is not null expression."""
        result = _build_is_not_null_expression("[name]")
        assert result == "is_not_empty([name])"


# IN/NOT_IN Expression Builder Tests


class TestInExpressionBuilders:
    """Tests for IN and NOT_IN expression builders."""

    def test_in_single_value_string(self):
        """Test IN with single string value."""
        result = _build_in_expression("[city]", "New York", "str")
        assert result == '[city]="New York"'

    def test_in_single_value_numeric(self):
        """Test IN with single numeric value."""
        result = _build_in_expression("[id]", "1", "numeric")
        assert result == "[id]=1"

    def test_in_multiple_values_string(self):
        """Test IN with multiple string values."""
        result = _build_in_expression("[city]", "New York, Boston, Chicago", "str")
        assert result == '([city]="New York") | ([city]="Boston") | ([city]="Chicago")'

    def test_in_multiple_values_numeric(self):
        """Test IN with multiple numeric values."""
        result = _build_in_expression("[id]", "1, 2, 3", "numeric")
        assert result == "([id]=1) | ([id]=2) | ([id]=3)"

    def test_in_mixed_values_no_type(self):
        """Test IN with numeric values and no field type - each value checked individually."""
        result = _build_in_expression("[id]", "1, 2, 3", None)
        assert result == "([id]=1) | ([id]=2) | ([id]=3)"

    def test_in_mixed_values_with_non_numeric(self):
        """Test IN where some values are not numeric."""
        result = _build_in_expression("[code]", "A, B, 1", None)
        assert result == '([code]="A") | ([code]="B") | ([code]=1)'

    def test_not_in_single_value_string(self):
        """Test NOT_IN with single string value."""
        result = _build_not_in_expression("[city]", "New York", "str")
        assert result == '[city]!="New York"'

    def test_not_in_single_value_numeric(self):
        """Test NOT_IN with single numeric value."""
        result = _build_not_in_expression("[id]", "1", "numeric")
        assert result == "[id]!=1"

    def test_not_in_multiple_values_string(self):
        """Test NOT_IN with multiple string values."""
        result = _build_not_in_expression("[city]", "New York, Boston", "str")
        assert result == '([city]!="New York") & ([city]!="Boston")'

    def test_not_in_multiple_values_numeric(self):
        """Test NOT_IN with multiple numeric values."""
        result = _build_not_in_expression("[id]", "1, 2", "numeric")
        assert result == "([id]!=1) & ([id]!=2)"

    def test_not_in_numeric_without_field_type(self):
        """Test NOT_IN with numeric values and no field type - the original bug case."""
        result = _build_not_in_expression("[id]", "1, 2", None)
        assert result == "([id]!=1) & ([id]!=2)"

    def test_in_whitespace_handling(self):
        """Test that values are trimmed properly."""
        result = _build_in_expression("[id]", "  1  ,  2  ,  3  ", "numeric")
        assert result == "([id]=1) | ([id]=2) | ([id]=3)"


# BETWEEN Expression Builder Tests


class TestBetweenExpressionBuilder:
    """Tests for BETWEEN expression builder."""

    def test_between_numeric(self):
        """Test BETWEEN with numeric values."""
        result = _build_between_expression("[age]", "18", "65", "numeric")
        assert result == "([age]>=18) & ([age]<=65)"

    def test_between_string(self):
        """Test BETWEEN with string values."""
        result = _build_between_expression("[name]", "A", "M", "str")
        assert result == '([name]>="A") & ([name]<="M")'

    def test_between_inferred_numeric(self):
        """Test BETWEEN with inferred numeric type."""
        result = _build_between_expression("[score]", "0", "100", None)
        assert result == "([score]>=0) & ([score]<=100)"

    def test_between_inferred_string(self):
        """Test BETWEEN with inferred string type."""
        result = _build_between_expression("[date]", "2024-01-01", "2024-12-31", None)
        assert result == '([date]>="2024-01-01") & ([date]<="2024-12-31")'

    def test_between_missing_value2(self):
        """Test BETWEEN raises error when value2 is None."""
        with pytest.raises(ValueError, match="BETWEEN operator requires value2"):
            _build_between_expression("[age]", "18", None, "numeric")


# Main build_filter_expression Function Tests


class TestBuildFilterExpression:
    """Tests for the main build_filter_expression function."""

    def test_equals_string(self):
        """Test EQUALS with string field."""
        bf = BasicFilter(field="name", operator=FilterOperator.EQUALS, value="John")
        result = build_filter_expression(bf, "str")
        assert result == '[name]="John"'

    def test_equals_numeric(self):
        """Test EQUALS with numeric field."""
        bf = BasicFilter(field="age", operator=FilterOperator.EQUALS, value="30")
        result = build_filter_expression(bf, "numeric")
        assert result == "[age]=30"

    def test_equals_inferred_numeric(self):
        """Test EQUALS with inferred numeric type."""
        bf = BasicFilter(field="count", operator=FilterOperator.EQUALS, value="42")
        result = build_filter_expression(bf, None)
        assert result == "[count]=42"

    def test_equals_inferred_string(self):
        """Test EQUALS with inferred string type."""
        bf = BasicFilter(field="code", operator=FilterOperator.EQUALS, value="ABC")
        result = build_filter_expression(bf, None)
        assert result == '[code]="ABC"'

    def test_not_equals(self):
        """Test NOT_EQUALS operator."""
        bf = BasicFilter(field="status", operator=FilterOperator.NOT_EQUALS, value="inactive")
        result = build_filter_expression(bf, "str")
        assert result == '[status]!="inactive"'

    def test_greater_than(self):
        """Test GREATER_THAN operator."""
        bf = BasicFilter(field="price", operator=FilterOperator.GREATER_THAN, value="100")
        result = build_filter_expression(bf, "numeric")
        assert result == "[price]>100"

    def test_greater_than_or_equals(self):
        """Test GREATER_THAN_OR_EQUALS operator."""
        bf = BasicFilter(field="age", operator=FilterOperator.GREATER_THAN_OR_EQUALS, value="18")
        result = build_filter_expression(bf, "numeric")
        assert result == "[age]>=18"

    def test_less_than(self):
        """Test LESS_THAN operator."""
        bf = BasicFilter(field="score", operator=FilterOperator.LESS_THAN, value="50")
        result = build_filter_expression(bf, "numeric")
        assert result == "[score]<50"

    def test_less_than_or_equals(self):
        """Test LESS_THAN_OR_EQUALS operator."""
        bf = BasicFilter(field="weight", operator=FilterOperator.LESS_THAN_OR_EQUALS, value="100")
        result = build_filter_expression(bf, "numeric")
        assert result == "[weight]<=100"

    def test_contains(self):
        """Test CONTAINS operator."""
        bf = BasicFilter(field="description", operator=FilterOperator.CONTAINS, value="sale")
        result = build_filter_expression(bf, "str")
        assert result == 'contains([description], "sale")'

    def test_not_contains(self):
        """Test NOT_CONTAINS operator."""
        bf = BasicFilter(field="tags", operator=FilterOperator.NOT_CONTAINS, value="deprecated")
        result = build_filter_expression(bf, "str")
        assert result == 'contains([tags], "deprecated") = false'

    def test_starts_with(self):
        """Test STARTS_WITH operator."""
        bf = BasicFilter(field="name", operator=FilterOperator.STARTS_WITH, value="Dr.")
        result = build_filter_expression(bf, "str")
        assert result == 'left([name], 3) = "Dr."'

    def test_ends_with(self):
        """Test ENDS_WITH operator."""
        bf = BasicFilter(field="email", operator=FilterOperator.ENDS_WITH, value=".com")
        result = build_filter_expression(bf, "str")
        assert result == 'right([email], 4) = ".com"'

    def test_is_null(self):
        """Test IS_NULL operator."""
        bf = BasicFilter(field="notes", operator=FilterOperator.IS_NULL, value="")
        result = build_filter_expression(bf, "str")
        assert result == "is_empty([notes])"

    def test_is_not_null(self):
        """Test IS_NOT_NULL operator."""
        bf = BasicFilter(field="name", operator=FilterOperator.IS_NOT_NULL, value="")
        result = build_filter_expression(bf, "str")
        assert result == "is_not_empty([name])"

    def test_in_string(self):
        """Test IN operator with string field."""
        bf = BasicFilter(field="city", operator=FilterOperator.IN, value="New York, Boston")
        result = build_filter_expression(bf, "str")
        assert result == '([city]="New York") | ([city]="Boston")'

    def test_in_numeric(self):
        """Test IN operator with numeric field."""
        bf = BasicFilter(field="id", operator=FilterOperator.IN, value="1, 2, 3")
        result = build_filter_expression(bf, "numeric")
        assert result == "([id]=1) | ([id]=2) | ([id]=3)"

    def test_not_in_string(self):
        """Test NOT_IN operator with string field."""
        bf = BasicFilter(field="status", operator=FilterOperator.NOT_IN, value="deleted, archived")
        result = build_filter_expression(bf, "str")
        assert result == '([status]!="deleted") & ([status]!="archived")'

    def test_not_in_numeric(self):
        """Test NOT_IN operator with numeric field."""
        bf = BasicFilter(field="id", operator=FilterOperator.NOT_IN, value="1, 2")
        result = build_filter_expression(bf, "numeric")
        assert result == "([id]!=1) & ([id]!=2)"

    def test_not_in_numeric_inferred(self):
        """Test NOT_IN with numeric values but no field type - the original bug case."""
        bf = BasicFilter(field="id", operator=FilterOperator.NOT_IN, value="1, 2")
        result = build_filter_expression(bf, None)
        assert result == "([id]!=1) & ([id]!=2)"

    def test_between(self):
        """Test BETWEEN operator."""
        bf = BasicFilter(field="age", operator=FilterOperator.BETWEEN, value="18", value2="65")
        result = build_filter_expression(bf, "numeric")
        assert result == "([age]>=18) & ([age]<=65)"

    def test_between_string(self):
        """Test BETWEEN operator with string values."""
        bf = BasicFilter(field="grade", operator=FilterOperator.BETWEEN, value="A", value2="C")
        result = build_filter_expression(bf, "str")
        assert result == '([grade]>="A") & ([grade]<="C")'

    def test_operator_from_string(self):
        """Test that string operators are converted correctly."""
        bf = BasicFilter(field="age", operator=">", value="30")
        result = build_filter_expression(bf, "numeric")
        assert result == "[age]>30"

    def test_operator_from_symbol(self):
        """Test that symbol operators are converted correctly."""
        bf = BasicFilter(field="name", operator="=", value="John")
        result = build_filter_expression(bf, "str")
        assert result == '[name]="John"'


# Edge Case and Regression Tests


class TestEdgeCases:
    """Tests for edge cases and regressions."""

    def test_empty_value(self):
        """Test with empty value."""
        bf = BasicFilter(field="name", operator=FilterOperator.EQUALS, value="")
        result = build_filter_expression(bf, "str")
        assert result == '[name]=""'

    def test_value_with_quotes(self):
        """Test value containing quotes (note: not escaped in current implementation)."""
        bf = BasicFilter(field="name", operator=FilterOperator.EQUALS, value='John "Jack"')
        result = build_filter_expression(bf, "str")
        assert result == '[name]="John "Jack""'

    def test_negative_number(self):
        """Test negative number value."""
        bf = BasicFilter(field="temperature", operator=FilterOperator.LESS_THAN, value="-10")
        result = build_filter_expression(bf, "numeric")
        assert result == "[temperature]<-10"

    def test_float_value(self):
        """Test float value."""
        bf = BasicFilter(field="price", operator=FilterOperator.EQUALS, value="19.99")
        result = build_filter_expression(bf, "numeric")
        assert result == "[price]=19.99"

    def test_field_with_spaces(self):
        """Test field name with spaces."""
        bf = BasicFilter(field="first name", operator=FilterOperator.EQUALS, value="John")
        result = build_filter_expression(bf, "str")
        assert result == '[first name]="John"'

    def test_field_with_underscores(self):
        """Test field name with underscores."""
        bf = BasicFilter(field="user_id", operator=FilterOperator.EQUALS, value="123")
        result = build_filter_expression(bf, "numeric")
        assert result == "[user_id]=123"

    def test_single_item_in_list(self):
        """Test IN/NOT_IN with single item."""
        bf = BasicFilter(field="id", operator=FilterOperator.IN, value="1")
        result = build_filter_expression(bf, "numeric")
        assert result == "[id]=1"

    def test_in_preserves_order(self):
        """Test that IN preserves the order of values."""
        bf = BasicFilter(field="id", operator=FilterOperator.IN, value="3, 1, 2")
        result = build_filter_expression(bf, "numeric")
        assert result == "([id]=3) | ([id]=1) | ([id]=2)"

    def test_large_number_of_in_values(self):
        """Test IN with many values."""
        values = ", ".join(str(i) for i in range(10))
        bf = BasicFilter(field="id", operator=FilterOperator.IN, value=values)
        result = build_filter_expression(bf, "numeric")
        expected_parts = [f"([id]={i})" for i in range(10)]
        assert result == " | ".join(expected_parts)

    def test_original_bug_case(self):
        """Test the original bug case: NOT_IN with numeric values and no field type.

        The original bug was that value='1, 2' would be checked as a whole string
        which contains comma and space, making it non-numeric, causing all values
        to be quoted incorrectly.
        """
        bf = BasicFilter(
            field="id", operator=FilterOperator.NOT_IN, value="1, 2", value2=None
        )
        result = build_filter_expression(bf, None)
        assert result == "([id]!=1) & ([id]!=2)"
        assert '"1"' not in result
        assert '"2"' not in result


class TestTemporalFields:
    """Date / datetime columns wrap the value in ``to_date`` / ``to_datetime``.

    Polars refuses to compare a temporal column with a string literal, so the
    expression must parse the value first; the frontend date picker emits
    ``YYYY-MM-DD`` for Date and ``YYYY-MM-DD HH:mm:ss`` for Datetime columns.
    """

    class _Column:
        def __init__(self, data_type: str, generic: str = "str"):
            self.data_type = data_type
            self._generic = generic

        def generic_datatype(self):
            return self._generic

    def test_resolve_date(self):
        assert resolve_filter_field_type(self._Column("Date", "date")) == "date"

    def test_resolve_datetime_parametrized(self):
        # generic_datatype() misses the parametrized form; the resolver must not.
        col = self._Column("Datetime(time_unit='us', time_zone=None)", "str")
        assert resolve_filter_field_type(col) == "datetime"

    def test_resolve_falls_back_to_generic(self):
        assert resolve_filter_field_type(self._Column("Int64", "numeric")) == "numeric"
        assert resolve_filter_field_type(self._Column("Time", "date")) == "date"

    def test_date_comparison(self):
        bf = BasicFilter(field="d", operator=FilterOperator.GREATER_THAN, value="2024-03-01")
        assert build_filter_expression(bf, "date") == '[d]>to_date("2024-03-01")'

    def test_datetime_comparison(self):
        bf = BasicFilter(field="ts", operator=FilterOperator.EQUALS, value="2024-03-01 12:30:00")
        assert build_filter_expression(bf, "datetime") == '[ts]=to_datetime("2024-03-01 12:30:00")'

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("2024-03-01", "2024-03-01 00:00:00"),
            ("2024-03-01T12:30:00", "2024-03-01 12:30:00"),
            ("2024-03-01 12:30", "2024-03-01 12:30:00"),
        ],
    )
    def test_datetime_value_is_normalized(self, value, expected):
        bf = BasicFilter(field="ts", operator=FilterOperator.LESS_THAN, value=value)
        assert build_filter_expression(bf, "datetime") == f'[ts]<to_datetime("{expected}")'

    def test_date_between(self):
        bf = BasicFilter(field="d", operator=FilterOperator.BETWEEN, value="2024-01-01", value2="2024-06-30")
        assert build_filter_expression(bf, "date") == '([d]>=to_date("2024-01-01")) & ([d]<=to_date("2024-06-30"))'

    def test_date_in(self):
        bf = BasicFilter(field="d", operator=FilterOperator.IN, value="2024-01-01, 2024-02-01")
        assert build_filter_expression(bf, "date") == '([d]=to_date("2024-01-01")) | ([d]=to_date("2024-02-01"))'

    def test_date_not_in_single(self):
        bf = BasicFilter(field="d", operator=FilterOperator.NOT_IN, value="2024-01-01")
        assert build_filter_expression(bf, "date") == '[d]!=to_date("2024-01-01")'

    def test_string_operators_unchanged_on_date(self):
        bf = BasicFilter(field="d", operator=FilterOperator.IS_NULL, value="")
        assert build_filter_expression(bf, "date") == "is_empty([d])"

    def test_none_type_still_quotes_a_date_string(self):
        bf = BasicFilter(field="d", operator=FilterOperator.EQUALS, value="2024-01-01")
        assert build_filter_expression(bf, None) == '[d]="2024-01-01"'


class TestNativeMembership:
    """``native=True`` renders a multi-value IN / NOT_IN as ``[field] in (...)`` / ``not in (...)``."""

    def test_in_numeric(self):
        assert _build_in_expression("[id]", "1, 2, 3", "numeric", native=True) == "[id] in (1, 2, 3)"

    def test_not_in_numeric(self):
        assert _build_not_in_expression("[id]", "1, 2", "numeric", native=True) == "[id] not in (1, 2)"

    def test_in_string(self):
        result = _build_in_expression("[city]", "New York, Boston", "str", native=True)
        assert result == '[city] in ("New York", "Boston")'

    def test_not_in_date(self):
        result = _build_not_in_expression("[d]", "2024-01-01, 2024-02-01", "date", native=True)
        assert result == '[d] not in (to_date("2024-01-01"), to_date("2024-02-01"))'

    def test_in_datetime(self):
        result = _build_in_expression("[ts]", "2024-01-01, 2024-02-01 12:30", "datetime", native=True)
        assert result == '[ts] in (to_datetime("2024-01-01 00:00:00"), to_datetime("2024-02-01 12:30:00"))'

    def test_single_value_keeps_comparison(self):
        assert _build_in_expression("[id]", "1", "numeric", native=True) == "[id]=1"
        assert _build_not_in_expression("[id]", "1", "numeric", native=True) == "[id]!=1"

    def test_whitespace_handling(self):
        assert _build_in_expression("[id]", "  1  ,  2  ", "numeric", native=True) == "[id] in (1, 2)"

    def test_empty_bare_member_keeps_chain(self):
        # polars-expr-transformer silently ends an in (...) list at an empty bare member.
        assert _build_not_in_expression("[id]", "1, , 3", "numeric", native=True) == "([id]!=1) & ([id]!=) & ([id]!=3)"
        assert _build_in_expression("[id]", "1, ", "numeric", native=True) == "([id]=1) | ([id]=)"

    def test_empty_string_member_stays_native(self):
        assert _build_in_expression("[s]", "a, ", "str", native=True) == '[s] in ("a", "")'

    def test_build_filter_expression_passes_flag(self):
        bf = BasicFilter(field="user_id", operator=FilterOperator.NOT_IN, value="19, 33, 35")
        assert build_filter_expression(bf, "numeric", native_membership=True) == "[user_id] not in (19, 33, 35)"
        assert build_filter_expression(bf, "numeric") == "([user_id]!=19) & ([user_id]!=33) & ([user_id]!=35)"


def _column(dtype: pl.DataType) -> FlowfileColumn:
    return FlowfileColumn.create_from_polars_dtype("x", dtype)


class TestSupportsNativeMembership:
    @pytest.mark.parametrize(
        "dtype",
        [
            pl.Int8,
            pl.Int64,
            pl.Int128,
            pl.Float64,
            pl.String,
            pl.Categorical,
            pl.Date,
            pl.Datetime("us"),
        ],
    )
    def test_supported(self, dtype):
        assert supports_native_membership(_column(dtype)) is True

    @pytest.mark.parametrize(
        "dtype",
        [
            pl.UInt8,
            pl.UInt64,
            pl.Float32,
            pl.Boolean,
            pl.Decimal(10, 2),
            pl.Enum(["a", "b"]),
            pl.Datetime("ns"),
            pl.Datetime("ms"),
            pl.Datetime("us", "UTC"),
            pl.Time,
            pl.Null,
        ],
    )
    def test_keeps_chain(self, dtype):
        assert supports_native_membership(_column(dtype)) is False


_D = dt.date
_DT = dt.datetime

# (dtype, column values incl. a null, basic-filter value, whether the native form is used)
_MEMBERSHIP_CASES = [
    (pl.Int64, [19, 33, 7, None], "19, 33, 35", True),
    (pl.Int32, [1, 2, 3, None], "1, 3", True),
    (pl.Int64, [-1, 2, -3, None], "-1, -3", True),
    (pl.Int64, [1, 2, 3, None], "1, 2.5", True),
    (pl.Float64, [0.1, 2.5, 3.0, None], "0.1, 2.5", True),
    (pl.Decimal(10, 2), [Decimal("0.10"), Decimal("1.50"), Decimal("2.00"), None], "0.1, 2", False),
    (pl.String, ["a", "b", "c", None], "a, c", True),
    (pl.Categorical, ["a", "b", "c", None], "a, z", True),
    (pl.Boolean, [True, False, None], "1, 0", False),
    (pl.Date, [_D(2024, 1, 1), _D(2024, 2, 1), None], "2024-01-01, 2024-03-01", True),
    (
        pl.Datetime("us"),
        [_DT(2024, 1, 1, 12), _DT(2024, 2, 1), None],
        "2024-01-01 12:00:00, 2024-03-01",
        True,
    ),
    (pl.Datetime("ns"), [_DT(2024, 1, 1, 12), _DT(2024, 2, 1), None], "2024-01-01 12:00:00, 2024-03-01", False),
    (pl.Datetime("ms"), [_DT(2024, 1, 1, 12), _DT(2024, 2, 1), None], "2024-01-01 12:00:00, 2024-03-01", False),
    (pl.Float32, [0.1, 2.0, None], "0.1, 3.5", False),
    (pl.Enum(["a", "b"]), ["a", "b", None], "a, z", False),
    (pl.UInt8, [1, 2, 3, None], "1, -3", False),
]


class TestMembershipMatchesChain:
    """The runtime's IN / NOT_IN expression keeps the rows the ``=`` / ``!=`` chain keeps, nulls included."""

    @pytest.mark.parametrize("operator", [FilterOperator.IN, FilterOperator.NOT_IN])
    @pytest.mark.parametrize("dtype,values,filter_value,native", _MEMBERSHIP_CASES)
    def test_same_rows_as_chain(self, operator, dtype, values, filter_value, native):
        df = pl.DataFrame({"x": pl.Series(values, dtype=dtype)})
        column = _column(dtype)
        field_type = resolve_filter_field_type(column)
        bf = BasicFilter(field="x", operator=operator, value=filter_value)

        chain = build_filter_expression(bf, field_type)
        runtime = build_filter_expression(bf, field_type, supports_native_membership(column))

        assert (" in (" in runtime) is native
        expected = df.filter(simple_function_to_expr(chain))
        result = df.filter(simple_function_to_expr(runtime))
        assert result.equals(expected)
        assert result["x"].null_count() == 0


def _best_compile_ms(expression: str, runs: int = 3) -> float:
    best = float("inf")
    for _ in range(runs):
        start = time.perf_counter()
        simple_function_to_expr(expression)
        best = min(best, (time.perf_counter() - start) * 1000)
    return best


class TestCompileTime:
    """Guards against polars-expr-transformer's exponential compile on long chains (fixed in 0.6.4)."""

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("operator", [FilterOperator.IN, FilterOperator.NOT_IN])
    @pytest.mark.parametrize(
        "field_type,values",
        [
            ("numeric", [str(i) for i in range(200)]),
            ("str", [f"value_{i}" for i in range(200)]),
        ],
    )
    def test_200_value_membership_filter(self, operator, field_type, values):
        bf = BasicFilter(field="x", operator=operator, value=", ".join(values))
        expression = build_filter_expression(bf, field_type, native_membership=True)
        assert _best_compile_ms(expression) < 50

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("joiner,comparison", [(" & ", "!="), (" | ", "=")])
    def test_50_term_hand_written_chain(self, joiner, comparison):
        expression = joiner.join(f"([x]{comparison}{i})" for i in range(50))
        assert _best_compile_ms(expression) < 100
