"""Utility module for applying output field configuration to FlowDataEngine results."""

import polars as pl
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas.input_schema import OutputFieldConfig


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
        ValueError: If raise_on_missing behavior is set and required columns are missing
    """
    if not output_field_config or not output_field_config.enabled:
        return flow_data_engine

    if not output_field_config.fields:
        return flow_data_engine

    df = flow_data_engine.data_frame
    if df is None:
        return flow_data_engine

    try:
        # Get the expected field names and data types
        expected_fields = {field.name: field for field in output_field_config.fields}
        current_columns = set(df.columns)
        expected_columns = set(expected_fields.keys())

        # Handle behavior based on vm_behavior setting
        if output_field_config.vm_behavior == "raise_on_missing":
            # Raise error if any expected columns are missing
            missing_columns = expected_columns - current_columns
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(sorted(missing_columns))}")

            # Select only the expected columns in the specified order
            df = df.select([field.name for field in output_field_config.fields])

        elif output_field_config.vm_behavior == "add_missing":
            # Add missing columns with default values
            for field in output_field_config.fields:
                if field.name not in current_columns:
                    # Determine the default value
                    if field.default_value is not None:
                        # Try to parse as expression if it looks like one
                        if field.default_value.startswith("pl."):
                            # This is a polars expression
                            try:
                                default_expr = eval(field.default_value)
                            except Exception as e:
                                logger.warning(
                                    f"Failed to evaluate expression '{field.default_value}' "
                                    f"for column '{field.name}': {e}. Using null instead."
                                )
                                default_expr = pl.lit(None)
                        else:
                            # Treat as literal value
                            default_expr = pl.lit(field.default_value)
                    else:
                        # No default value specified, use null
                        default_expr = pl.lit(None)

                    # Add the column with the default value
                    df = df.with_columns(default_expr.alias(field.name))

            # Select only the expected columns in the specified order
            df = df.select([field.name for field in output_field_config.fields])

        elif output_field_config.vm_behavior == "select_only":
            # Only select columns that exist
            columns_to_select = [field.name for field in output_field_config.fields if field.name in current_columns]
            if columns_to_select:
                df = df.select(columns_to_select)

        # Update the data frame in the flow data engine
        flow_data_engine.data_frame = df

        # Force schema recalculation by clearing the cached schema
        # The schema property will recalculate when accessed next time
        flow_data_engine._schema = None

        logger.info(
            f"Applied output field config: behavior={output_field_config.vm_behavior}, "
            f"fields={len(output_field_config.fields)}"
        )

    except Exception as e:
        logger.error(f"Error applying output field config: {e}")
        raise

    return flow_data_engine
