#!/usr/bin/env python3
"""
Generate JSON Schema for Flowfile YAML files.

This script generates a JSON Schema that can be used by VS Code's YAML extension
(redhat.vscode-yaml) to provide autocompletion and validation for Flowfile YAML files.

Usage:
    poetry run python -m tools.yaml_schema.generate_yaml_schema

The generated schema will be written to tools/yaml_schema/flowfile.schema.json
"""

import json
from pathlib import Path
from typing import Any, get_type_hints

from pydantic import BaseModel

from flowfile_core.schemas import input_schema
from flowfile_core.schemas.schemas import (
    NODE_TYPE_TO_SETTINGS_CLASS,
    FlowfileData,
    FlowfileNode,
    FlowfileSettings,
)


def fix_discriminated_unions(schema: dict[str, Any]) -> None:
    """
    Fix discriminated unions for YAML validation.

    Pydantic's discriminated unions expect the discriminator property to be inside
    the nested object, but in YAML serialization it's often in the parent.
    We convert 'oneOf' with discriminator to 'anyOf' without discriminator.
    """
    if "discriminator" in schema and "oneOf" in schema:
        # Remove discriminator and convert oneOf to anyOf for permissive validation
        del schema["discriminator"]
        schema["anyOf"] = schema.pop("oneOf")

    # Process nested properties
    if "properties" in schema:
        for prop_schema in schema["properties"].values():
            if isinstance(prop_schema, dict):
                fix_discriminated_unions(prop_schema)

    # Process $defs
    if "$defs" in schema:
        for def_schema in schema["$defs"].values():
            fix_discriminated_unions(def_schema)

    # Process array items
    if "items" in schema and isinstance(schema["items"], dict):
        fix_discriminated_unions(schema["items"])

    # Process allOf, anyOf, oneOf
    for keyword in ["allOf", "anyOf", "oneOf"]:
        if keyword in schema and isinstance(schema[keyword], list):
            for item in schema[keyword]:
                if isinstance(item, dict):
                    fix_discriminated_unions(item)


def make_fields_optional_recursive(schema: dict[str, Any], defs: dict[str, Any] | None = None) -> None:
    """
    Make fields optional in nested schemas by removing 'required' constraints.

    For YAML validation, we want to be permissive - Pydantic will handle
    actual validation with defaults/validators at runtime.
    """
    # Fields that have validators creating defaults or are truly optional in practice
    # These should not be marked as required in the schema
    optional_fields = {
        "table_settings",  # Has validator that creates defaults
        "fields",  # Often optional
        "abs_file_path",  # Set by validator
    }

    if "required" in schema:
        schema["required"] = [r for r in schema["required"] if r not in optional_fields]
        if not schema["required"]:
            del schema["required"]

    # Process nested $defs
    if "$defs" in schema:
        for def_name, def_schema in schema["$defs"].items():
            make_fields_optional_recursive(def_schema, schema["$defs"])

    # Process properties recursively
    if "properties" in schema:
        for prop_name, prop_schema in schema["properties"].items():
            if isinstance(prop_schema, dict):
                make_fields_optional_recursive(prop_schema, defs)

    # Process $ref targets if we have access to defs
    if defs and "$ref" in schema:
        ref = schema["$ref"]
        if ref.startswith("#/$defs/"):
            def_name = ref.split("/")[-1]
            if def_name in defs:
                make_fields_optional_recursive(defs[def_name], defs)


def get_node_setting_schema(node_class: type[BaseModel]) -> dict[str, Any]:
    """Generate JSON schema for a node settings class, excluding internal fields."""
    schema = node_class.model_json_schema()

    # Remove internal fields that shouldn't appear in YAML
    internal_fields = {
        "flow_id",
        "node_id",
        "pos_x",
        "pos_y",
        "is_setup",
        "description",
        "user_id",
        "is_flow_output",
        "is_user_defined",
        "depending_on_id",
        "depending_on_ids",
    }

    if "properties" in schema:
        for field in internal_fields:
            schema["properties"].pop(field, None)

        # Also remove from required list
        if "required" in schema:
            schema["required"] = [r for r in schema["required"] if r not in internal_fields]

    # Make fields with validators optional
    make_fields_optional_recursive(schema)

    return schema


def merge_definitions(base_defs: dict, new_defs: dict, prefix: str = "") -> dict:
    """Merge definitions from new schema into base, with optional prefix to avoid conflicts."""
    result = dict(base_defs)
    for key, value in new_defs.items():
        new_key = f"{prefix}{key}" if prefix else key
        if new_key not in result:
            result[new_key] = value
        elif result[new_key] != value:
            # If there's a conflict, add with prefix
            result[f"{prefix}_{key}"] = value
    return result


def generate_flowfile_schema() -> dict[str, Any]:
    """Generate the complete JSON Schema for Flowfile YAML files."""

    # Start with the base schema structure
    schema: dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://flowfile.dev/schemas/flowfile.schema.json",
        "title": "Flowfile YAML Schema",
        "description": "Schema for Flowfile data flow definition files (.yaml, .yml)",
        "type": "object",
        "required": ["flowfile_version", "flowfile_id", "flowfile_name", "flowfile_settings", "nodes"],
        "additionalProperties": False,
        "$defs": {},
    }

    # Generate FlowfileSettings schema
    settings_schema = FlowfileSettings.model_json_schema()
    if "$defs" in settings_schema:
        schema["$defs"].update(settings_schema["$defs"])
        del settings_schema["$defs"]

    # Build node type to settings mapping and collect all definitions
    node_settings_schemas: dict[str, dict] = {}
    all_node_types = list(NODE_TYPE_TO_SETTINGS_CLASS.keys())

    for node_type, settings_class in NODE_TYPE_TO_SETTINGS_CLASS.items():
        node_schema = get_node_setting_schema(settings_class)

        # Extract and merge definitions
        if "$defs" in node_schema:
            schema["$defs"] = merge_definitions(schema["$defs"], node_schema["$defs"], prefix="")
            del node_schema["$defs"]

        node_settings_schemas[node_type] = node_schema

    # Store node settings schemas in $defs
    for node_type, node_schema in node_settings_schemas.items():
        def_name = f"NodeSettings_{node_type}"
        # Clean up the schema - remove $schema if present
        node_schema.pop("$schema", None)
        schema["$defs"][def_name] = node_schema

    # Create the FlowfileNode schema with conditional setting_input based on type
    node_base_properties = {
        "id": {"type": "integer", "description": "Unique identifier for this node within the flow"},
        "type": {
            "type": "string",
            "enum": all_node_types,
            "description": "The type of node (determines what settings are available)",
        },
        "is_start_node": {
            "type": "boolean",
            "default": False,
            "description": "Whether this node is a starting point for the flow (no inputs)",
        },
        "description": {
            "type": ["string", "null"],
            "default": "",
            "description": "Optional description of what this node does",
        },
        "x_position": {
            "type": ["integer", "null"],
            "default": 0,
            "description": "X coordinate on the canvas",
        },
        "y_position": {
            "type": ["integer", "null"],
            "default": 0,
            "description": "Y coordinate on the canvas",
        },
        "left_input_id": {
            "type": ["integer", "null"],
            "description": "Node ID for left input (used by join nodes)",
        },
        "right_input_id": {
            "type": ["integer", "null"],
            "description": "Node ID for right input (used by join nodes)",
        },
        "input_ids": {
            "type": ["array", "null"],
            "items": {"type": "integer"},
            "description": "List of node IDs that provide input to this node",
        },
        "outputs": {
            "type": ["array", "null"],
            "items": {"type": "integer"},
            "description": "List of node IDs that this node outputs to",
        },
        "setting_input": {
            "description": "Node-specific settings (structure depends on node type)",
        },
    }

    # Create allOf with if/then conditions for each node type
    node_conditionals = []
    for node_type in all_node_types:
        node_conditionals.append(
            {
                "if": {"properties": {"type": {"const": node_type}}, "required": ["type"]},
                "then": {"properties": {"setting_input": {"$ref": f"#/$defs/NodeSettings_{node_type}"}}},
            }
        )

    # Build the FlowfileNode schema
    flowfile_node_schema = {
        "type": "object",
        "required": ["id", "type"],
        "additionalProperties": False,
        "properties": node_base_properties,
        "allOf": node_conditionals,
    }

    schema["$defs"]["FlowfileNode"] = flowfile_node_schema

    # Post-process all definitions to make validator-handled fields optional
    for def_name in schema["$defs"]:
        make_fields_optional_recursive(schema["$defs"][def_name], schema["$defs"])

    # Fix discriminated unions - convert to anyOf for permissive YAML validation
    for def_name in schema["$defs"]:
        fix_discriminated_unions(schema["$defs"][def_name])

    # Build the main schema properties
    schema["properties"] = {
        "flowfile_version": {
            "type": "string",
            "description": "Version of the Flowfile format",
            "examples": ["0.5.0", "0.5.1", "0.5.2"],
        },
        "flowfile_id": {"type": "integer", "description": "Unique identifier for this flow"},
        "flowfile_name": {"type": "string", "description": "Human-readable name for this flow"},
        "flowfile_settings": {
            "type": "object",
            "description": "Global settings for the flow",
            "additionalProperties": False,
            "properties": {
                "description": {
                    "type": ["string", "null"],
                    "description": "Description of the flow",
                },
                "execution_mode": {
                    "type": "string",
                    "enum": ["Development", "Performance"],
                    "default": "Performance",
                    "description": "Execution mode - Development for debugging, Performance for production",
                },
                "execution_location": {
                    "type": "string",
                    "enum": ["local", "remote"],
                    "default": "local",
                    "description": "Where to execute the flow - locally or on a remote worker",
                },
                "auto_save": {
                    "type": "boolean",
                    "default": False,
                    "description": "Whether to automatically save changes",
                },
                "show_detailed_progress": {
                    "type": "boolean",
                    "default": True,
                    "description": "Whether to show detailed progress during execution",
                },
            },
        },
        "nodes": {
            "type": "array",
            "description": "List of nodes in the flow",
            "items": {"$ref": "#/$defs/FlowfileNode"},
        },
    }

    return schema


def main():
    """Generate and save the JSON Schema."""
    schema = generate_flowfile_schema()

    # Determine output path
    script_dir = Path(__file__).parent
    output_path = script_dir / "flowfile.schema.json"

    # Write the schema
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    print(f"Generated schema at: {output_path}")
    print(f"Schema contains {len(schema.get('$defs', {}))} definitions")

    # Also print a summary of node types
    node_types = [k for k in schema.get("$defs", {}).keys() if k.startswith("NodeSettings_")]
    print(f"Included {len(node_types)} node types")


if __name__ == "__main__":
    main()
