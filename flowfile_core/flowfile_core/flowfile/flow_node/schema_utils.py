"""Schema utilities for output field configuration."""

from flowfile_core.configs import logger
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.schemas.input_schema import OutputFieldConfig


def create_schema_from_output_field_config(output_field_config: OutputFieldConfig) -> list[FlowfileColumn]:
    """Create a FlowfileColumn schema from OutputFieldConfig.

    This is used for schema prediction - instead of running the transformation,
    we can directly return the configured output schema.

    Args:
        output_field_config: The output field configuration

    Returns:
        List of FlowfileColumn objects representing the expected output schema
    """
    if not output_field_config or not output_field_config.enabled or not output_field_config.fields:
        logger.debug("create_schema_from_output_field_config: config not enabled or no fields, returning None")
        return None

    schema = [
        FlowfileColumn.from_input(column_name=field.name, data_type=field.data_type)
        for field in output_field_config.fields
    ]
    logger.info(
        f"create_schema_from_output_field_config: Created schema with {len(schema)} fields: "
        f"{[f.name for f in schema]}"
    )
    return schema


def create_schema_callback_with_output_config(
    base_schema_callback: callable,
    output_field_config: OutputFieldConfig | None
) -> callable:
    """Wraps a schema callback to use output_field_config when available.

    This allows nodes to use their configured output schema for prediction
    instead of running through transformation logic.

    Args:
        base_schema_callback: The original schema callback function
        output_field_config: The output field configuration, if any

    Returns:
        A wrapped schema callback that prioritizes output_field_config
    """
    logger.debug(
        f"create_schema_callback_with_output_config called: "
        f"base_callback={'present' if base_schema_callback else 'None'}, "
        f"config={'enabled' if (output_field_config and output_field_config.enabled) else 'disabled/None'}"
    )

    def wrapped_schema_callback():
        # If output_field_config is enabled, use it directly for schema prediction
        if output_field_config and output_field_config.enabled and output_field_config.fields:
            logger.info(
                f"wrapped_schema_callback: Using output_field_config for schema prediction "
                f"(vm_behavior={output_field_config.vm_behavior}, {len(output_field_config.fields)} fields)"
            )
            return create_schema_from_output_field_config(output_field_config)

        # Otherwise fall back to the original schema callback
        if base_schema_callback:
            logger.debug("wrapped_schema_callback: Falling back to base_schema_callback")
            return base_schema_callback()
        else:
            logger.warning("wrapped_schema_callback: No base_schema_callback and no output_field_config, returning None")
            return None

    return wrapped_schema_callback
