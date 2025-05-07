# Assume these imports are correct from your original context
from flowfile_frame.expr import Column
from flowfile_core.schemas import transform_schema


def _normalize_columns_to_list(columns):
    """Convert a column specification to a list format.

    Args:
        columns: Column name or list of column names

    Returns:
        List of column names/expressions
    """
    if columns is None:
        return []
    elif isinstance(columns, str):
        return [columns]
    elif isinstance(columns, (list, tuple)):
        return list(columns)
    else:
        return [columns]  # Single non-string item


def _extract_column_name(col_expr):
    """Extract a simple column name from various column representations.

    Args:
        col_expr: Column expression (string, Column object, etc.)

    Returns:
        tuple: (column_name, needs_polars_code)
        - column_name is the string name if possible
        - needs_polars_code is True if this expression requires polars code generation
    """
    if isinstance(col_expr, str):
        return col_expr, False

    if isinstance(col_expr, Column):
        # If it's a simple unaltered column, use its name
        if not col_expr._select_input.is_altered:
            return col_expr.name, False
        # Otherwise, this requires polars code
        return col_expr, True

    # Any other expression type needs polars code
    return col_expr, True


def _create_join_mappings(left_columns, right_columns):
    """Create join mappings between left and right columns.

    Args:
        left_columns: List of left join columns
        right_columns: List of right join columns

    Returns:
        tuple: (join_mappings, needs_polars_code)
        - join_mappings is a list of JoinMap objects
        - needs_polars_code is True if any column requires polars code generation
    """
    join_mappings = []
    needs_polars_code = False

    for left_col, right_col in zip(left_columns, right_columns):
        left_name, left_needs_code = _extract_column_name(left_col)
        right_name, right_needs_code = _extract_column_name(right_col)

        needs_polars_code = needs_polars_code or left_needs_code or right_needs_code

        # Only create standard join mappings if both columns are simple strings
        if not left_needs_code and not right_needs_code:
            join_mappings.append(transform_schema.JoinMap(left_col=left_name, right_col=right_name))

    return join_mappings, needs_polars_code
