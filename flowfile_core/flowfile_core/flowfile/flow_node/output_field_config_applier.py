"""Utility module for applying output field configuration to FlowDataEngine results."""

from typing import List, Set
import polars as pl
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_data_engine.flow_file_column.utils import cast_str_to_polars_type
from flowfile_core.schemas.input_schema import OutputFieldConfig, OutputFieldInfo


def _parse_default_value(field: OutputFieldInfo) -> pl.Expr:
    """Parse default value from field configuration.

    Args:
        field: Output field info containing default_value

    Returns:
        Polars expression for the default value cast to the target data type
    """
    # Get target Polars dtype from the field's data_type
    target_dtype = cast_str_to_polars_type(field.data_type)
    # Treat as literal value and cast to target type
    return pl.lit(field.default_value).cast(target_dtype, strict=False, wrap_numerical=True)


def _select_columns_in_order(df: pl.DataFrame, fields: list[OutputFieldInfo]) -> pl.DataFrame:
    """Select columns in the specified field order.

    Args:
        df: Input dataframe
        fields: List of fields specifying column order

    Returns:
        DataFrame with columns selected in specified order
    """
    return df.select([field.name for field in fields])


def _apply_raise_on_missing(
    flowfile_engine: FlowDataEngine,
    fields: list[OutputFieldInfo],
) -> FlowDataEngine:
    """Apply raise_on_missing validation mode.

    Raises error if any expected columns are missing, then selects columns in order.

    Args:
        flowfile_engine: Input flow data engine
        fields: List of expected output fields

    Returns:
        Flow Data Engine with columns selected in specified order

    Raises:
        ValueError: If any expected columns are missing
    """
    cols = [f.name for f in fields]
    missing_columns = set(cols) - set(flowfile_engine.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing_columns))}")
    if flowfile_engine.columns != cols:
        return FlowDataEngine(_select_columns_in_order(flowfile_engine.data_frame, fields))
    else:
        return flowfile_engine


def _apply_add_missing(
    engine: FlowDataEngine,
    fields: list[OutputFieldInfo],
) -> FlowDataEngine:
    """Apply add_missing validation mode.

    Adds missing columns with default values, then selects columns in order.
    Extra columns not in the config are removed.

    Args:
        engine: Input flow data engine
        fields: List of expected output fields

    Returns:
        FlowDataEngine with missing columns added and only configured columns in specified order
    """
    # Add missing columns with default values
    current_columns = set(engine.columns)
    expressions = [_parse_default_value(field).alias(field.name)
                   for field in fields if field.name not in current_columns]
    if expressions:
        new_df = engine.data_frame.with_columns(expressions)
    else:
        new_df = engine.data_frame
    return FlowDataEngine(_select_columns_in_order(new_df, fields))


def _apply_add_missing_keep_extra(
    engine: FlowDataEngine,
    fields: list[OutputFieldInfo],
) -> FlowDataEngine:
    """Apply add_missing_keep_extra validation mode.

    Adds missing columns with default values, but keeps all incoming columns.
    Configured columns come first in specified order, followed by extra columns.

    Args:
        engine: Input flow data engine
        fields: List of expected output fields

    Returns:
        FlowDataEngine with missing columns added and all columns preserved
        (configured columns first, then extras)
    """
    current_columns = set(engine.columns)
    configured_names = {field.name for field in fields}

    # Add missing columns with default values
    expressions = [_parse_default_value(field).alias(field.name)
                   for field in fields if field.name not in current_columns]
    if expressions:
        new_df = engine.data_frame.with_columns(expressions)
    else:
        new_df = engine.data_frame

    # Build column order: configured columns first (in order), then extras
    configured_column_order = [field.name for field in fields]
    extra_columns = [col for col in engine.columns if col not in configured_names]

    final_column_order = configured_column_order + extra_columns
    return FlowDataEngine(new_df.select(final_column_order))


def _validate_data_types(df: FlowDataEngine, fields: list[OutputFieldInfo]) -> None:
    """Validate that dataframe column types match expected types.

    Args:
        df: Input dataframe or lazyframe
        fields: List of expected output fields with data types

    Raises:
        ValueError: If any data type mismatches are found
    """
    # Get schema (works for both DataFrame and LazyFrame)
    schema: dict[str, FlowfileColumn] = {column.column_name: column for column in df.schema}
    mismatches = []
    for field in fields:

        if field.name not in schema:
            continue
        # Use FlowfileColumn infrastructure to convert dtype to string
        column = schema.get(field.name)
        column.get_minimal_field_info()
        if column.data_type != field.data_type:
            mismatches.append(
                f"Column '{field.name}': expected {field.data_type}, got {column.data_type}"
            )

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
    # breakpoint()
    try:
        # Get column sets for validation (works for both DataFrame and LazyFrame)

        # Apply validation mode behavior
        mode = output_field_config.validation_mode_behavior
        if mode == "raise_on_missing":
            new_flow_engine = _apply_raise_on_missing(flow_data_engine, output_field_config.fields)
        elif mode == "add_missing":
            new_flow_engine = _apply_add_missing(engine=flow_data_engine, fields=output_field_config.fields)
        elif mode == "add_missing_keep_extra":
            new_flow_engine = _apply_add_missing_keep_extra(engine=flow_data_engine, fields=output_field_config.fields)
        elif mode == "select_only":
            new_flow_engine = flow_data_engine.select_columns(
                [field.name for field in output_field_config.fields]
            )
        else:
            raise ValueError(f"Unknown validation mode behavior: {mode}")
        # Validate data types if enabled
        if output_field_config.validate_data_types:
            _validate_data_types(new_flow_engine, output_field_config.fields)

        logger.info(
            f"Applied output field config: behavior={mode}, "
            f"fields={len(output_field_config.fields)}, "
            f"validate_data_types={output_field_config.validate_data_types}"
        )

    except Exception as e:
        logger.error(f"Error applying output field config: {e}")
        raise

    return new_flow_engine
