import polars as pl


def format_error(
    node_type: str, node_id: int, error: Exception, df: pl.DataFrame | None = None, column: str = None
) -> str:
    """Format error message with context and suggestions"""
    error_str = str(error)
    msg_parts = [f"{node_type.replace('_', ' ').title()} error on node #{node_id}:"]

    # Check for common column-related errors
    column_keywords = ["column", "ColumnNotFoundError", "not found", "SchemaError"]
    is_column_error = any(kw.lower() in error_str.lower() for kw in column_keywords)

    if is_column_error and df is not None:
        available_cols = df.columns
        msg_parts.append(f"'{column or 'unknown'}' - {error_str}")
        msg_parts.append(f"Available columns: {', '.join(available_cols)}")

        # Try to suggest similar column names
        if column:
            similar = [c for c in available_cols if column.lower() in c.lower() or c.lower() in column.lower()]
            if similar:
                msg_parts.append(f"Did you mean: {', '.join(similar)}?")
    else:
        msg_parts.append(error_str)

    # Add suggestions based on error type
    if "type" in error_str.lower() and "cannot" in error_str.lower():
        msg_parts.append("Suggestion: Check that the column data types match the operation.")
    elif "null" in error_str.lower() or "none" in error_str.lower():
        msg_parts.append("Suggestion: Consider filtering out null values first.")
    elif "parse" in error_str.lower() or "csv" in error_str.lower():
        msg_parts.append("Suggestion: Check your CSV delimiter and header settings.")

    return " ".join(msg_parts)


def format_error_lf(
    node_type: str, node_id: int, error: Exception, lf: pl.LazyFrame | None = None, column: str = None
) -> str:
    """Format error message with context for LazyFrame operations"""
    error_str = str(error)
    msg_parts = [f"{node_type.replace('_', ' ').title()} error on node #{node_id}:"]

    column_keywords = ["column", "ColumnNotFoundError", "not found", "SchemaError"]
    is_column_error = any(kw.lower() in error_str.lower() for kw in column_keywords)

    if is_column_error and lf is not None:
        try:
            schema = lf.collect_schema()
            available_cols = list(schema.keys())
            msg_parts.append(f"'{column or 'unknown'}' - {error_str}")
            msg_parts.append(f"Available columns: {', '.join(available_cols)}")

            if column:
                similar = [c for c in available_cols if column.lower() in c.lower() or c.lower() in column.lower()]
                if similar:
                    msg_parts.append(f"Did you mean: {', '.join(similar)}?")
        except Exception:
            msg_parts.append(error_str)
    else:
        msg_parts.append(error_str)

    if "type" in error_str.lower() and "cannot" in error_str.lower():
        msg_parts.append("Suggestion: Check that the column data types match the operation.")
    elif "null" in error_str.lower() or "none" in error_str.lower():
        msg_parts.append("Suggestion: Consider filtering out null values first.")
    elif "parse" in error_str.lower() or "csv" in error_str.lower():
        msg_parts.append("Suggestion: Check your CSV delimiter and header settings.")

    return " ".join(msg_parts)
