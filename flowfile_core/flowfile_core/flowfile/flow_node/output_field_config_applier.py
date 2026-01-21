"""Utility module for applying output field configuration to FlowDataEngine results."""

import polars as pl
from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas.input_schema import OutputFieldConfig


def polars_dtype_to_data_type_str(polars_dtype: str) -> str:
    """Map Polars dtype string to our DataTypeStr format.

    Args:
        polars_dtype: String representation of Polars dtype (e.g., "Int64", "Utf8")

    Returns:
        Corresponding DataTypeStr value
    """
    # Normalize the dtype string
    dtype_lower = polars_dtype.lower()

    # Map common Polars dtypes to our format
    dtype_map = {
        "int8": "Int8",
        "int16": "Int16",
        "int32": "Int32",
        "int64": "Int64",
        "uint8": "UInt8",
        "uint16": "UInt16",
        "uint32": "UInt32",
        "uint64": "UInt64",
        "float32": "Float32",
        "float64": "Float64",
        "utf8": "String",
        "str": "String",
        "string": "String",
        "bool": "Boolean",
        "boolean": "Boolean",
        "date": "Date",
        "datetime": "Datetime",
        "time": "Time",
        "duration": "Duration",
        "list": "List",
        "struct": "Struct",
        "categorical": "Categorical",
        "null": "Null",
    }

    # Try exact match first
    if dtype_lower in dtype_map:
        return dtype_map[dtype_lower]

    # Try partial matches for complex types
    for key, value in dtype_map.items():
        if key in dtype_lower:
            return value

    # Default to String if unknown
    logger.warning(f"Unknown Polars dtype '{polars_dtype}', treating as String")
    return "String"


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

        # Handle behavior based on validation_mode_behavior setting
        if output_field_config.validation_mode_behavior == "raise_on_missing":
            # Raise error if any expected columns are missing
            missing_columns = expected_columns - current_columns
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(sorted(missing_columns))}")

            # Select only the expected columns in the specified order
            df = df.select([field.name for field in output_field_config.fields])

        elif output_field_config.validation_mode_behavior == "add_missing":
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

        elif output_field_config.validation_mode_behavior == "select_only":
            # Only select columns that exist
            columns_to_select = [field.name for field in output_field_config.fields if field.name in current_columns]
            if columns_to_select:
                df = df.select(columns_to_select)

        # Validate data types if enabled
        if output_field_config.validate_data_types:
            mismatches = []
            for field in output_field_config.fields:
                if field.name in df.columns:
                    actual_dtype_polars = str(df[field.name].dtype)
                    actual_dtype = polars_dtype_to_data_type_str(actual_dtype_polars)
                    expected_dtype = field.data_type

                    # Check if types match (case-insensitive comparison)
                    if actual_dtype.lower() != expected_dtype.lower():
                        mismatches.append(
                            f"Column '{field.name}': expected {expected_dtype}, got {actual_dtype} (Polars: {actual_dtype_polars})"
                        )

            if mismatches:
                error_msg = "Data type validation failed:\n" + "\n".join(f"  - {m}" for m in mismatches)
                logger.error(error_msg)
                raise ValueError(error_msg)

            logger.info(f"Data type validation passed for {len(output_field_config.fields)} fields")

        # Update the data frame in the flow data engine
        flow_data_engine.data_frame = df

        # Force schema recalculation by clearing the cached schema
        # The schema property will recalculate when accessed next time
        flow_data_engine._schema = None

        logger.info(
            f"Applied output field config: behavior={output_field_config.validation_mode_behavior}, "
            f"fields={len(output_field_config.fields)}, validate_data_types={output_field_config.validate_data_types}"
        )

    except Exception as e:
        logger.error(f"Error applying output field config: {e}")
        raise

    return flow_data_engine
