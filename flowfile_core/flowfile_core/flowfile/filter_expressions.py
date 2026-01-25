"""Filter expression builder for converting BasicFilter objects to expression strings.

This module provides utilities for building filter expressions from BasicFilter objects
that are compatible with the Flowfile expression language (polars_expr_transformer).

The main entry point is `build_filter_expression()` which converts a BasicFilter
to a filter expression string.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flowfile_core.schemas.transform_schema import BasicFilter

from flowfile_core.schemas.transform_schema import FilterOperator


def _is_numeric_string(value: str) -> bool:
    """Check if a string value represents a numeric value.

    Args:
        value: The string to check.

    Returns:
        True if the value is numeric (int or float), False otherwise.
    """
    if not value:
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False


def _should_quote_value(value: str, field_data_type: str | None) -> bool:
    """Determine if a value should be quoted in the expression.

    Args:
        value: The value to check.
        field_data_type: The data type of the field ("str", "numeric", "date", or None).

    Returns:
        True if the value should be quoted, False otherwise.
    """
    # If field is explicitly string type, always quote
    if field_data_type == "str":
        return True
    # If field is explicitly numeric type, never quote
    if field_data_type == "numeric":
        return False
    # Otherwise, quote if the value doesn't look numeric
    return not _is_numeric_string(value)


def _format_field(field_name: str) -> str:
    """Format a field name for use in an expression.

    Args:
        field_name: The name of the field.

    Returns:
        The field name wrapped in brackets.
    """
    return f"[{field_name}]"


def _build_comparison_expression(
    field: str, operator_symbol: str, value: str, should_quote: bool
) -> str:
    """Build a simple comparison expression.

    Args:
        field: The formatted field name (e.g., "[column]").
        operator_symbol: The comparison operator (e.g., "=", "!=", ">").
        value: The value to compare against.
        should_quote: Whether to quote the value.

    Returns:
        The comparison expression string.
    """
    if should_quote:
        return f'{field}{operator_symbol}"{value}"'
    return f"{field}{operator_symbol}{value}"


def _build_equals_expression(field: str, value: str, should_quote: bool) -> str:
    """Build an equals expression."""
    return _build_comparison_expression(field, "=", value, should_quote)


def _build_not_equals_expression(field: str, value: str, should_quote: bool) -> str:
    """Build a not equals expression."""
    return _build_comparison_expression(field, "!=", value, should_quote)


def _build_greater_than_expression(field: str, value: str, should_quote: bool) -> str:
    """Build a greater than expression."""
    return _build_comparison_expression(field, ">", value, should_quote)


def _build_greater_than_or_equals_expression(field: str, value: str, should_quote: bool) -> str:
    """Build a greater than or equals expression."""
    return _build_comparison_expression(field, ">=", value, should_quote)


def _build_less_than_expression(field: str, value: str, should_quote: bool) -> str:
    """Build a less than expression."""
    return _build_comparison_expression(field, "<", value, should_quote)


def _build_less_than_or_equals_expression(field: str, value: str, should_quote: bool) -> str:
    """Build a less than or equals expression."""
    return _build_comparison_expression(field, "<=", value, should_quote)


def _build_contains_expression(field: str, value: str) -> str:
    """Build a contains expression."""
    return f'contains({field}, "{value}")'


def _build_not_contains_expression(field: str, value: str) -> str:
    """Build a not contains expression."""
    return f'contains({field}, "{value}") = false'


def _build_starts_with_expression(field: str, value: str) -> str:
    """Build a starts with expression."""
    return f'left({field}, {len(value)}) = "{value}"'


def _build_ends_with_expression(field: str, value: str) -> str:
    """Build an ends with expression."""
    return f'right({field}, {len(value)}) = "{value}"'


def _build_is_null_expression(field: str) -> str:
    """Build an is null expression."""
    return f"is_empty({field})"


def _build_is_not_null_expression(field: str) -> str:
    """Build an is not null expression."""
    return f"is_not_empty({field})"


def _build_in_expression(field: str, value: str, field_data_type: str | None) -> str:
    """Build an IN expression for matching any of multiple values.

    Args:
        field: The formatted field name.
        value: Comma-separated list of values.
        field_data_type: The data type of the field.

    Returns:
        An OR-combined expression for each value.
    """
    values = [v.strip() for v in value.split(",")]
    if len(values) == 1:
        should_quote = _should_quote_value(values[0], field_data_type)
        return _build_equals_expression(field, values[0], should_quote)

    conditions = []
    for v in values:
        should_quote = _should_quote_value(v, field_data_type)
        if should_quote:
            conditions.append(f'({field}="{v}")')
        else:
            conditions.append(f"({field}={v})")
    return " | ".join(conditions)


def _build_not_in_expression(field: str, value: str, field_data_type: str | None) -> str:
    """Build a NOT IN expression for excluding multiple values.

    Args:
        field: The formatted field name.
        value: Comma-separated list of values.
        field_data_type: The data type of the field.

    Returns:
        An AND-combined expression for each value.
    """
    values = [v.strip() for v in value.split(",")]
    if len(values) == 1:
        should_quote = _should_quote_value(values[0], field_data_type)
        return _build_not_equals_expression(field, values[0], should_quote)

    conditions = []
    for v in values:
        should_quote = _should_quote_value(v, field_data_type)
        if should_quote:
            conditions.append(f'({field}!="{v}")')
        else:
            conditions.append(f"({field}!={v})")
    return " & ".join(conditions)


def _build_between_expression(
    field: str, value: str, value2: str, field_data_type: str | None
) -> str:
    """Build a BETWEEN expression for range filtering.

    Args:
        field: The formatted field name.
        value: The lower bound.
        value2: The upper bound.
        field_data_type: The data type of the field.

    Returns:
        An AND-combined range expression.

    Raises:
        ValueError: If value2 is None.
    """
    if value2 is None:
        raise ValueError("BETWEEN operator requires value2")

    should_quote_v1 = _should_quote_value(value, field_data_type)
    should_quote_v2 = _should_quote_value(value2, field_data_type)

    if should_quote_v1:
        lower = f'({field}>="{value}")'
    else:
        lower = f"({field}>={value})"

    if should_quote_v2:
        upper = f'({field}<="{value2}")'
    else:
        upper = f"({field}<={value2})"

    return f"{lower} & {upper}"


def build_filter_expression(
    basic_filter: BasicFilter, field_data_type: str | None = None
) -> str:
    """Build a filter expression string from a BasicFilter object.

    Uses the Flowfile expression language that is compatible with polars_expr_transformer.

    Args:
        basic_filter: The basic filter configuration.
        field_data_type: The data type of the field ("str", "numeric", "date", or None).
            If None, the type is inferred from the value.

    Returns:
        A filter expression string compatible with polars_expr_transformer.

    Examples:
        >>> from flowfile_core.schemas.transform_schema import BasicFilter, FilterOperator
        >>> bf = BasicFilter(field="age", operator=FilterOperator.GREATER_THAN, value="30")
        >>> build_filter_expression(bf, "numeric")
        '[age]>30'

        >>> bf = BasicFilter(field="name", operator=FilterOperator.EQUALS, value="John")
        >>> build_filter_expression(bf, "str")
        '[name]="John"'

        >>> bf = BasicFilter(field="id", operator=FilterOperator.NOT_IN, value="1, 2, 3")
        >>> build_filter_expression(bf, "numeric")
        '([id]!=1) & ([id]!=2) & ([id]!=3)'
    """
    field = _format_field(basic_filter.field)
    value = basic_filter.value
    value2 = basic_filter.value2

    try:
        operator = basic_filter.get_operator()
    except (ValueError, AttributeError):
        operator = FilterOperator.from_symbol(str(basic_filter.operator))

    # For simple comparison operators, determine quoting based on the single value
    should_quote = _should_quote_value(value, field_data_type)

    if operator == FilterOperator.EQUALS:
        return _build_equals_expression(field, value, should_quote)

    elif operator == FilterOperator.NOT_EQUALS:
        return _build_not_equals_expression(field, value, should_quote)

    elif operator == FilterOperator.GREATER_THAN:
        return _build_greater_than_expression(field, value, should_quote)

    elif operator == FilterOperator.GREATER_THAN_OR_EQUALS:
        return _build_greater_than_or_equals_expression(field, value, should_quote)

    elif operator == FilterOperator.LESS_THAN:
        return _build_less_than_expression(field, value, should_quote)

    elif operator == FilterOperator.LESS_THAN_OR_EQUALS:
        return _build_less_than_or_equals_expression(field, value, should_quote)

    elif operator == FilterOperator.CONTAINS:
        return _build_contains_expression(field, value)

    elif operator == FilterOperator.NOT_CONTAINS:
        return _build_not_contains_expression(field, value)

    elif operator == FilterOperator.STARTS_WITH:
        return _build_starts_with_expression(field, value)

    elif operator == FilterOperator.ENDS_WITH:
        return _build_ends_with_expression(field, value)

    elif operator == FilterOperator.IS_NULL:
        return _build_is_null_expression(field)

    elif operator == FilterOperator.IS_NOT_NULL:
        return _build_is_not_null_expression(field)

    elif operator == FilterOperator.IN:
        return _build_in_expression(field, value, field_data_type)

    elif operator == FilterOperator.NOT_IN:
        return _build_not_in_expression(field, value, field_data_type)

    elif operator == FilterOperator.BETWEEN:
        return _build_between_expression(field, value, value2, field_data_type)

    else:
        # Fallback for unknown operators - use legacy format
        if should_quote:
            return f'{field}{operator.to_symbol()}"{value}"'
        return f"{field}{operator.to_symbol()}{value}"
