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

import pytest

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
)
from flowfile_core.schemas.transform_schema import BasicFilter, FilterOperator


# =============================================================================
# Helper Function Tests
# =============================================================================


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


# =============================================================================
# Comparison Expression Builder Tests
# =============================================================================


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


# =============================================================================
# String Function Expression Builder Tests
# =============================================================================


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


# =============================================================================
# Null Check Expression Builder Tests
# =============================================================================


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


# =============================================================================
# IN/NOT_IN Expression Builder Tests
# =============================================================================


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
        # A and B are not numeric so quoted, 1 is numeric so not quoted
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
        # Each value is checked individually, not the whole "1, 2" string
        assert result == "([id]!=1) & ([id]!=2)"

    def test_in_whitespace_handling(self):
        """Test that values are trimmed properly."""
        result = _build_in_expression("[id]", "  1  ,  2  ,  3  ", "numeric")
        assert result == "([id]=1) | ([id]=2) | ([id]=3)"


# =============================================================================
# BETWEEN Expression Builder Tests
# =============================================================================


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


# =============================================================================
# Main build_filter_expression Function Tests
# =============================================================================


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
        # Should NOT quote numeric values, even when type is not specified
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


# =============================================================================
# Edge Case and Regression Tests
# =============================================================================


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
        # Should produce unquoted numeric values
        assert result == "([id]!=1) & ([id]!=2)"
        # Should NOT produce quoted strings
        assert '"1"' not in result
        assert '"2"' not in result
