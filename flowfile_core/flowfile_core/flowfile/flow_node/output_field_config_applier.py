"""Utility module for applying output field configuration to FlowDataEngine results."""


import polars as pl

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.schemas.input_schema import OutputFieldConfig, OutputFieldInfo

# Mapping from Polars DataType to our string representation
POLARS_TO_STRING: dict[type, str] = {
    pl.String: "String",
    pl.Utf8: "String",
    pl.Int64: "Int64",
    pl.Int32: "Int32",
    pl.Int16: "Int16",
    pl.Int8: "Int8",
    pl.UInt64: "UInt64",
    pl.UInt32: "UInt32",
    pl.UInt16: "UInt16",
    pl.UInt8: "UInt8",
    pl.Float64: "Float64",
    pl.Float32: "Float32",
    pl.Boolean: "Boolean",
    pl.Bool: "Boolean",
    pl.Date: "Date",
    pl.Datetime: "Datetime",
    pl.Time: "Time",
    pl.Duration: "Duration",
    pl.List: "List",
    pl.Struct: "Struct",
    pl.Categorical: "Categorical",
    pl.Null: "Null",
}

# Mapping from our string representation to acceptable Polars DataTypes
STRING_TO_POLARS: dict[str, list[type]] = {
    "String": [pl.String, pl.Utf8],
    "Int64": [pl.Int64],
    "Int32": [pl.Int32],
    "Int16": [pl.Int16],
    "Int8": [pl.Int8],
    "UInt64": [pl.UInt64],
    "UInt32": [pl.UInt32],
    "UInt16": [pl.UInt16],
    "UInt8": [pl.UInt8],
    "Float64": [pl.Float64],
    "Float32": [pl.Float32],
    "Boolean": [pl.Boolean, pl.Bool],
    "Date": [pl.Date],
    "Datetime": [pl.Datetime],
    "Time": [pl.Time],
    "Duration": [pl.Duration],
    "List": [pl.List],
    "Struct": [pl.Struct],
    "Categorical": [pl.Categorical],
    "Null": [pl.Null],
}


def polars_dtype_to_data_type_str(polars_dtype) -> str:
    """Map Polars dtype to our DataTypeStr format.

    Args:
        polars_dtype: Polars DataType object or type

    Returns:
        Corresponding DataTypeStr value
    """
    # Try direct lookup in mapping
    dtype_type = type(polars_dtype) if not isinstance(polars_dtype, type) else polars_dtype

    if dtype_type in POLARS_TO_STRING:
        return POLARS_TO_STRING[dtype_type]

    # For complex types like List[Int64], try to extract base type
    dtype_str = str(polars_dtype).lower()
    if "list" in dtype_str:
        return "List"
    if "struct" in dtype_str:
        return "Struct"
    if "datetime" in dtype_str:
        return "Datetime"

    # Default to String if unknown
    logger.warning(f"Unknown Polars dtype '{polars_dtype}', treating as String")
    return "String"


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
    df: pl.DataFrame,
    fields: list[OutputFieldInfo],
    current_columns: set[str],
    expected_columns: set[str]
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
    fields: list[OutputFieldInfo],
    current_columns: set[str]
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
    fields: list[OutputFieldInfo],
    current_columns: set[str]
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


def _validate_data_types(df: pl.DataFrame, fields: list[OutputFieldInfo]) -> None:
    """Validate that dataframe column types match expected types.

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

        # Get acceptable Polars dtypes for the expected type (case-insensitive)
        expected_dtypes = None
        for type_name, dtypes in STRING_TO_POLARS.items():
            if type_name.lower() == expected_type_name.lower():
                expected_dtypes = dtypes
                break

        if expected_dtypes is None:
            logger.warning(f"Unknown expected type '{expected_type_name}' for column '{field.name}', skipping validation")
            continue

        # Check if actual dtype matches any acceptable dtype
        actual_dtype_type = type(actual_dtype)
        if actual_dtype_type not in expected_dtypes:
            # Get string representation for error message
            actual_type_name = POLARS_TO_STRING.get(actual_dtype_type, str(actual_dtype))
            mismatches.append(
                f"Column '{field.name}': expected {expected_type_name}, got {actual_type_name}"
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
