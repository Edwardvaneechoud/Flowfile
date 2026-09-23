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

_COMPARISON_OPERATORS = frozenset(
    {
        FilterOperator.EQUALS,
        FilterOperator.NOT_EQUALS,
        FilterOperator.GREATER_THAN,
        FilterOperator.GREATER_THAN_OR_EQUALS,
        FilterOperator.LESS_THAN,
        FilterOperator.LESS_THAN_OR_EQUALS,
    }
)


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
    if field_data_type == "str":
        return True
    if field_data_type == "numeric":
        return False
    return not _is_numeric_string(value)


def resolve_filter_field_type(column) -> str | None:
    """The ``field_data_type`` a column needs: "str", "numeric", "date", "datetime" or None.

    ``generic_datatype`` lumps Date, Datetime and Time together (and misses a
    parametrized ``Datetime(time_unit=...)``), but the literal a comparison needs
    differs per dtype, so the base dtype token decides for temporal columns.
    """
    base = str(column.data_type).split("(", 1)[0]
    if base == "Date":
        return "date"
    if base == "Datetime":
        return "datetime"
    return column.generic_datatype()


def _normalize_datetime_value(value: str) -> str:
    """Bring a typed or ISO datetime into the ``%Y-%m-%d %H:%M:%S`` shape ``to_datetime`` parses."""
    value = value.strip().replace("T", " ", 1)
    if len(value) == 10:
        return f"{value} 00:00:00"
    if len(value) == 16:
        return f"{value}:00"
    return value


def _render_value(value: str, field_data_type: str | None) -> str:
    """The literal to embed for ``value``: a temporal parse call, a quoted string or a bare number."""
    if field_data_type == "date":
        return f'to_date("{value.strip()}")'
    if field_data_type == "datetime":
        return f'to_datetime("{_normalize_datetime_value(value)}")'
    if _should_quote_value(value, field_data_type):
        return f'"{value}"'
    return value


def _format_field(field_name: str) -> str:
    """Format a field name for use in an expression.

    Args:
        field_name: The name of the field.

    Returns:
        The field name wrapped in brackets.
    """
    return f"[{field_name}]"


def _build_comparison_expression(field: str, operator_symbol: str, value: str, should_quote: bool) -> str:
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
        return _build_equals_expression(field, _render_value(values[0], field_data_type), False)

    conditions = [f"({field}={_render_value(v, field_data_type)})" for v in values]
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
        return _build_not_equals_expression(field, _render_value(values[0], field_data_type), False)

    conditions = [f"({field}!={_render_value(v, field_data_type)})" for v in values]
    return " & ".join(conditions)


def _build_between_expression(field: str, value: str, value2: str, field_data_type: str | None) -> str:
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

    lower = f"({field}>={_render_value(value, field_data_type)})"
    upper = f"({field}<={_render_value(value2, field_data_type)})"
    return f"{lower} & {upper}"


def build_filter_expression(basic_filter: BasicFilter, field_data_type: str | None = None) -> str:
    """Build a filter expression string from a BasicFilter object.

    Uses the Flowfile expression language that is compatible with polars_expr_transformer.

    Args:
        basic_filter: The basic filter configuration.
        field_data_type: The data type of the field ("str", "numeric", "date", "datetime", or None).
            If None, the type is inferred from the value. Date and datetime columns wrap the
            value in ``to_date``/``to_datetime`` so the comparison is typed.

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

    if operator in _COMPARISON_OPERATORS:
        value = _render_value(value, field_data_type)

    if operator == FilterOperator.EQUALS:
        return _build_equals_expression(field, value, False)

    elif operator == FilterOperator.NOT_EQUALS:
        return _build_not_equals_expression(field, value, False)

    elif operator == FilterOperator.GREATER_THAN:
        return _build_greater_than_expression(field, value, False)

    elif operator == FilterOperator.GREATER_THAN_OR_EQUALS:
        return _build_greater_than_or_equals_expression(field, value, False)

    elif operator == FilterOperator.LESS_THAN:
        return _build_less_than_expression(field, value, False)

    elif operator == FilterOperator.LESS_THAN_OR_EQUALS:
        return _build_less_than_or_equals_expression(field, value, False)

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
        return f"{field}{operator.to_symbol()}{_render_value(value, field_data_type)}"
