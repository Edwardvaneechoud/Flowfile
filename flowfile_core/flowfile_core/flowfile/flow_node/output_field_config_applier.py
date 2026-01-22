"""Utility module for applying output field configuration to FlowDataEngine results."""

from typing import List, Set
import polars as pl
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.type_registry import convert_pl_type_to_string
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.schemas.input_schema import OutputFieldConfig, OutputFieldInfo


def _parse_default_value(field: OutputFieldInfo) -> pl.Expr:
    """Parse default value from field configuration.

    Args:
        field: Output field info containing default_value

    Returns:
        Polars expression for the default value
    """
    if field.default_value is None:
        return pl.lit(None)

    # Try to parse as expression if it looks like one
    if field.default_value.startswith("pl."):
        try:
            return eval(field.default_value)
        except Exception as e:
            logger.warning(
                f"Failed to evaluate expression '{field.default_value}' "
                f"for column '{field.name}': {e}. Using null instead."
            )
            return pl.lit(None)

    # Treat as literal value
    return pl.lit(field.default_value)


def _select_columns_in_order(df: pl.DataFrame, fields: List[OutputFieldInfo]) -> pl.DataFrame:
    """Select columns in the specified field order.

    Args:
        df: Input dataframe
        fields: List of fields specifying column order

    Returns:
        DataFrame with columns selected in specified order
    """
    return df.select([field.name for field in fields])


def _apply_raise_on_missing(
    df: pl.DataFrame,
    fields: List[OutputFieldInfo],
    current_columns: Set[str],
    expected_columns: Set[str]
) -> pl.DataFrame:
    """Apply raise_on_missing validation mode.

    Raises error if any expected columns are missing, then selects columns in order.

    Args:
        df: Input dataframe
        fields: List of expected output fields
        current_columns: Set of current column names
        expected_columns: Set of expected column names

    Returns:
        DataFrame with selected columns in specified order

    Raises:
        ValueError: If any expected columns are missing
    """
    missing_columns = expected_columns - current_columns
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing_columns))}")

    return _select_columns_in_order(df, fields)


def _apply_add_missing(
    df: pl.DataFrame,
    fields: List[OutputFieldInfo],
    current_columns: Set[str]
) -> pl.DataFrame:
    """Apply add_missing validation mode.

    Adds missing columns with default values, then selects columns in order.

    Args:
        df: Input dataframe
        fields: List of expected output fields
        current_columns: Set of current column names

    Returns:
        DataFrame with missing columns added and all columns in specified order
    """
    # Add missing columns with default values
    for field in fields:
        if field.name not in current_columns:
            default_expr = _parse_default_value(field)
            df = df.with_columns(default_expr.alias(field.name))

    return _select_columns_in_order(df, fields)


def _apply_select_only(
    df: pl.DataFrame,
    fields: List[OutputFieldInfo],
    current_columns: Set[str]
) -> pl.DataFrame:
    """Apply select_only validation mode.

    Only selects columns that exist in the dataframe.

    Args:
        df: Input dataframe
        fields: List of expected output fields
        current_columns: Set of current column names

    Returns:
        DataFrame with only existing columns selected in specified order
    """
    columns_to_select = [field.name for field in fields if field.name in current_columns]
    if not columns_to_select:
        return df

    return df.select(columns_to_select)


def _validate_data_types(df: pl.DataFrame, fields: List[OutputFieldInfo]) -> None:
    """Validate that dataframe column types match expected types.

    Uses existing FlowfileColumn infrastructure for type conversion.

    Args:
        df: Input dataframe
        fields: List of expected output fields with data types

    Raises:
        ValueError: If any data type mismatches are found
    """
    mismatches = []
    for field in fields:
        if field.name not in df.columns:
            continue

        actual_dtype = df[field.name].dtype
        expected_type_name = field.data_type

        try:
            # Use existing infrastructure to convert string to Polars type
            expected_dtype = cast_str_to_polars_type(expected_type_name)

            # Compare the actual dtype with expected dtype
            # Handle cases where types might be equivalent (e.g., Utf8 vs String)
            if actual_dtype != expected_dtype:
                # Convert actual type to string format for comparison
                actual_type_str = convert_pl_type_to_string(actual_dtype)
                expected_type_str = convert_pl_type_to_string(expected_dtype)

                # Normalize type strings for comparison (remove "pl." prefix)
                actual_normalized = actual_type_str.replace("pl.", "")
                expected_normalized = expected_type_str.replace("pl.", "")

                # Check if types match after normalization (handles Utf8/String equivalence)
                if actual_normalized != expected_normalized:
                    mismatches.append(
                        f"Column '{field.name}': expected {expected_type_name}, got {actual_normalized}"
                    )
        except Exception as e:
            logger.warning(
                f"Could not validate type for column '{field.name}' with expected type '{expected_type_name}': {e}"
            )
            continue

    if mismatches:
        error_msg = "Data type validation failed:\n" + "\n".join(f"  - {m}" for m in mismatches)
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"Data type validation passed for {len(fields)} fields")


def apply_output_field_config(
    flow_data_engine: FlowDataEngine, output_field_config: OutputFieldConfig
) -> FlowDataEngine:
    """Apply output field configuration to enforce schema requirements.

    Args:
        flow_data_engine: The FlowDataEngine instance to apply configuration to
        output_field_config: The output field configuration specifying behavior

    Returns:
        Modified FlowDataEngine with enforced schema

    Raises:
        ValueError: If raise_on_missing behavior is set and required columns are missing,
                   or if data type validation fails
    """
    if not output_field_config or not output_field_config.enabled:
        return flow_data_engine

    if not output_field_config.fields:
        return flow_data_engine

    df = flow_data_engine.data_frame
    if df is None:
        return flow_data_engine

    try:
        # Get column sets for validation
        current_columns = set(df.columns)
        expected_columns = {field.name for field in output_field_config.fields}

        # Apply validation mode behavior
        mode = output_field_config.validation_mode_behavior
        if mode == "raise_on_missing":
            df = _apply_raise_on_missing(df, output_field_config.fields, current_columns, expected_columns)
        elif mode == "add_missing":
            df = _apply_add_missing(df, output_field_config.fields, current_columns)
        elif mode == "select_only":
            df = _apply_select_only(df, output_field_config.fields, current_columns)

        # Validate data types if enabled
        if output_field_config.validate_data_types:
            _validate_data_types(df, output_field_config.fields)

        # Update the flow data engine
        flow_data_engine.data_frame = df
        flow_data_engine._schema = None  # Force schema recalculation

        logger.info(
            f"Applied output field config: behavior={mode}, "
            f"fields={len(output_field_config.fields)}, "
            f"validate_data_types={output_field_config.validate_data_types}"
        )

    except Exception as e:
        logger.error(f"Error applying output field config: {e}")
        raise

    return flow_data_engine
