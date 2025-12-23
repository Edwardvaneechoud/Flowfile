"""
Compatibility enhancements for opening old flowfile versions.
Migrates old schema structures to new ones during file load.
"""
import pickle
from typing import Any
from pathlib import Path

from flowfile_core.schemas import schemas, input_schema
from tools.migrate.legacy_schemas import LEGACY_CLASS_MAP


# =============================================================================
# LEGACY PICKLE LOADING
# =============================================================================

class LegacyUnpickler(pickle.Unpickler):
    """
    Custom unpickler that redirects class lookups to legacy dataclass definitions.

    When loading old .flowfile pickles, transform_schema classes were dataclasses.
    Now they're Pydantic BaseModels. This unpickler intercepts those classes and
    loads them as the legacy dataclass versions, which can then be migrated.
    """

    def find_class(self, module: str, name: str):
        """Override to redirect transform_schema dataclasses to legacy definitions."""
        if name in LEGACY_CLASS_MAP:
            return LEGACY_CLASS_MAP[name]
        return super().find_class(module, name)


def load_flowfile_pickle(path: str) -> Any:
    """
    Load a flowfile pickle using legacy-compatible unpickling.

    This handles old flowfiles where transform_schema classes were dataclasses
    by loading them as legacy dataclass instances, which can then be migrated
    to the new Pydantic BaseModel versions.

    Args:
        path: Path to the .flowfile pickle

    Returns:
        The deserialized FlowInformation object
    """
    resolved_path = Path(path).resolve()
    with open(resolved_path, 'rb') as f:
        return LegacyUnpickler(f).load()


# =============================================================================
# DATACLASS DETECTION AND MIGRATION
# =============================================================================

def _is_dataclass_instance(obj: Any) -> bool:
    """Check if an object is a dataclass instance (not a Pydantic model)."""
    return hasattr(obj, '__dataclass_fields__') and not hasattr(obj, 'model_dump')


def _migrate_dataclass_to_basemodel(obj: Any, model_class: type) -> Any:
    """Convert a dataclass instance to a Pydantic BaseModel instance."""
    if obj is None:
        return None

    if not _is_dataclass_instance(obj):
        return obj  # Already a BaseModel or dict

    from dataclasses import fields, asdict
    try:
        data = asdict(obj)
    except Exception:
        # Fallback: manually extract attributes
        data = {f.name: getattr(obj, f.name, None) for f in fields(obj)}

    return model_class.model_validate(data)


# =============================================================================
# NODE-SPECIFIC COMPATIBILITY FUNCTIONS
# =============================================================================

def ensure_compatibility_node_read(node_read: input_schema.NodeRead):
    """Migrate old NodeRead/ReceivedTable structure to new table_settings format."""
    if not hasattr(node_read, 'received_file') or node_read.received_file is None:
        return

    received_file = node_read.received_file

    # Ensure fields list exists
    if not hasattr(received_file, 'fields'):
        setattr(received_file, 'fields', [])

    # Check if already migrated (has table_settings as proper object, not dict)
    if hasattr(received_file, 'table_settings') and received_file.table_settings is not None:
        if not isinstance(received_file.table_settings, dict):
            return

    # Determine file_type - use existing or infer from attributes
    file_type = getattr(received_file, 'file_type', None)
    if file_type is None:
        path = getattr(received_file, 'path', '') or ''
        if path.endswith('.parquet'):
            file_type = 'parquet'
        elif path.endswith(('.xlsx', '.xls')):
            file_type = 'excel'
        elif path.endswith('.json'):
            file_type = 'json'
        else:
            file_type = 'csv'

    # Build table_settings based on file_type, extracting old flat attributes
    table_settings_dict = _build_input_table_settings(received_file, file_type)

    # Re-validate the entire ReceivedTable to get proper Pydantic model
    received_file_dict = received_file.model_dump()
    received_file_dict['file_type'] = file_type
    received_file_dict['table_settings'] = table_settings_dict

    # Create new validated ReceivedTable and replace
    new_received_file = input_schema.ReceivedTable.model_validate(received_file_dict)
    node_read.received_file = new_received_file


def _build_input_table_settings(received_file: Any, file_type: str) -> dict:
    """Build appropriate table_settings dict from old flat attributes."""

    if file_type == 'csv':
        return {
            'file_type': 'csv',
            'reference': getattr(received_file, 'reference', ''),
            'starting_from_line': getattr(received_file, 'starting_from_line', 0),
            'delimiter': getattr(received_file, 'delimiter', ','),
            'has_headers': getattr(received_file, 'has_headers', True),
            'encoding': getattr(received_file, 'encoding', 'utf-8'),
            'parquet_ref': getattr(received_file, 'parquet_ref', None),
            'row_delimiter': getattr(received_file, 'row_delimiter', '\n'),
            'quote_char': getattr(received_file, 'quote_char', '"'),
            'infer_schema_length': getattr(received_file, 'infer_schema_length', 10_000),
            'truncate_ragged_lines': getattr(received_file, 'truncate_ragged_lines', False),
            'ignore_errors': getattr(received_file, 'ignore_errors', False),
        }

    elif file_type == 'json':
        return {
            'file_type': 'json',
            'reference': getattr(received_file, 'reference', ''),
            'starting_from_line': getattr(received_file, 'starting_from_line', 0),
            'delimiter': getattr(received_file, 'delimiter', ','),
            'has_headers': getattr(received_file, 'has_headers', True),
            'encoding': getattr(received_file, 'encoding', 'utf-8'),
            'parquet_ref': getattr(received_file, 'parquet_ref', None),
            'row_delimiter': getattr(received_file, 'row_delimiter', '\n'),
            'quote_char': getattr(received_file, 'quote_char', '"'),
            'infer_schema_length': getattr(received_file, 'infer_schema_length', 10_000),
            'truncate_ragged_lines': getattr(received_file, 'truncate_ragged_lines', False),
            'ignore_errors': getattr(received_file, 'ignore_errors', False),
        }

    elif file_type == 'parquet':
        return {'file_type': 'parquet'}

    elif file_type == 'excel':
        return {
            'file_type': 'excel',
            'sheet_name': getattr(received_file, 'sheet_name', None),
            'start_row': getattr(received_file, 'start_row', 0),
            'start_column': getattr(received_file, 'start_column', 0),
            'end_row': getattr(received_file, 'end_row', 0),
            'end_column': getattr(received_file, 'end_column', 0),
            'has_headers': getattr(received_file, 'has_headers', True),
            'type_inference': getattr(received_file, 'type_inference', False),
        }

    # Default to csv settings
    return {'file_type': 'csv', 'delimiter': ',', 'encoding': 'utf-8', 'has_headers': True}


def ensure_compatibility_node_output(node_output: input_schema.NodeOutput):
    """Migrate old OutputSettings structure to new table_settings format."""
    if not hasattr(node_output, 'output_settings') or node_output.output_settings is None:
        return

    output_settings = node_output.output_settings

    # Check if already migrated (has table_settings as proper object, not dict)
    if hasattr(output_settings, 'table_settings') and output_settings.table_settings is not None:
        if not isinstance(output_settings.table_settings, dict):
            return

    # Migrate from old separate fields to new table_settings
    file_type = getattr(output_settings, 'file_type', 'csv')
    table_settings_dict = _build_output_table_settings(output_settings, file_type)

    # Re-validate the entire OutputSettings to get proper Pydantic model
    output_settings_dict = output_settings.model_dump()
    output_settings_dict['table_settings'] = table_settings_dict

    # Remove old fields if they exist
    for old_field in ['output_csv_table', 'output_parquet_table', 'output_excel_table']:
        output_settings_dict.pop(old_field, None)

    # Create new validated OutputSettings and replace
    new_output_settings = input_schema.OutputSettings.model_validate(output_settings_dict)
    node_output.output_settings = new_output_settings


def _build_output_table_settings(output_settings: Any, file_type: str) -> dict:
    """Build appropriate output table_settings from old separate table fields."""

    if file_type == 'csv':
        old_csv = getattr(output_settings, 'output_csv_table', None)
        if old_csv is not None:
            return {
                'file_type': 'csv',
                'delimiter': getattr(old_csv, 'delimiter', ','),
                'encoding': getattr(old_csv, 'encoding', 'utf-8'),
            }
        return {'file_type': 'csv', 'delimiter': ',', 'encoding': 'utf-8'}

    elif file_type == 'parquet':
        return {'file_type': 'parquet'}

    elif file_type == 'excel':
        old_excel = getattr(output_settings, 'output_excel_table', None)
        if old_excel is not None:
            return {
                'file_type': 'excel',
                'sheet_name': getattr(old_excel, 'sheet_name', 'Sheet1'),
            }
        return {'file_type': 'excel', 'sheet_name': 'Sheet1'}

    return {'file_type': 'csv', 'delimiter': ',', 'encoding': 'utf-8'}


def ensure_compatibility_node_select(node_select: input_schema.NodeSelect):
    """Ensure NodeSelect has position attributes, sorted_by field, and handle dataclass migrations."""
    if not hasattr(node_select, 'select_input'):
        return

    # Handle dataclass -> BaseModel migration for select_input items
    if node_select.select_input:
        from flowfile_core.schemas import transform_schema
        new_select_input = []
        needs_migration = any(_is_dataclass_instance(si) for si in node_select.select_input)

        if needs_migration:
            for si in node_select.select_input:
                if _is_dataclass_instance(si):
                    new_si = _migrate_dataclass_to_basemodel(si, transform_schema.SelectInput)
                    new_select_input.append(new_si)
                else:
                    new_select_input.append(si)
            node_select.select_input = new_select_input

    # Ensure position attributes exist
    if any(not hasattr(select_input, 'position') for select_input in node_select.select_input):
        for _index, select_input in enumerate(node_select.select_input):
            setattr(select_input, 'position', _index)

    if not hasattr(node_select, 'sorted_by'):
        setattr(node_select, 'sorted_by', 'none')


def ensure_compatibility_node_joins(node_settings: input_schema.NodeFuzzyMatch | input_schema.NodeJoin):
    """Ensure join nodes have position attributes on renames and handle dataclass migrations."""
    if not hasattr(node_settings, 'join_input') or node_settings.join_input is None:
        return

    join_input = node_settings.join_input

    # Check if right_select and left_select exist
    if not hasattr(join_input, 'right_select') or not hasattr(join_input, 'left_select'):
        return

    from flowfile_core.schemas import transform_schema

    # Handle dataclass -> BaseModel migration for join_mapping
    if hasattr(join_input, 'join_mapping') and join_input.join_mapping:
        new_mapping = []
        for jm in join_input.join_mapping:
            if _is_dataclass_instance(jm):
                new_jm = _migrate_dataclass_to_basemodel(jm, transform_schema.JoinMap)
                new_mapping.append(new_jm)
            else:
                new_mapping.append(jm)
        join_input.join_mapping = new_mapping

    # Handle dataclass -> BaseModel migration for renames in selects
    for select_attr in ['right_select', 'left_select']:
        select = getattr(join_input, select_attr, None)
        if select is None:
            continue

        renames = getattr(select, 'renames', []) or []
        if renames and any(_is_dataclass_instance(r) for r in renames):
            new_renames = []
            for r in renames:
                if _is_dataclass_instance(r):
                    new_r = _migrate_dataclass_to_basemodel(r, transform_schema.SelectInput)
                    new_renames.append(new_r)
                else:
                    new_renames.append(r)
            select.renames = new_renames

    right_renames = getattr(join_input.right_select, 'renames', []) or []
    left_renames = getattr(join_input.left_select, 'renames', []) or []

    # Ensure position attributes exist
    if any(not hasattr(r, 'position') for r in right_renames + left_renames):
        for _index, select_input in enumerate(right_renames + left_renames):
            setattr(select_input, 'position', _index)


def ensure_description(node: input_schema.NodeBase):
    """Ensure node has description field."""
    if not hasattr(node, 'description'):
        setattr(node, 'description', '')


def ensure_compatibility_node_polars(node_polars: input_schema.NodePolarsCode):
    """Migrate old NodePolarsCode structure:
    - depending_on_id (single) -> depending_on_ids (list)
    - PolarsCodeInput from dataclass to BaseModel
    """
    # Handle depending_on_id -> depending_on_ids migration
    if hasattr(node_polars, 'depending_on_id'):
        old_id = getattr(node_polars, 'depending_on_id', None)
        if not hasattr(node_polars, 'depending_on_ids') or node_polars.depending_on_ids is None:
            if old_id is not None:
                setattr(node_polars, 'depending_on_ids', [old_id])
            else:
                setattr(node_polars, 'depending_on_ids', [])

    # Handle PolarsCodeInput dataclass -> BaseModel migration
    if hasattr(node_polars, 'polars_code_input') and node_polars.polars_code_input is not None:
        polars_code_input = node_polars.polars_code_input

        if _is_dataclass_instance(polars_code_input):
            from flowfile_core.schemas import transform_schema
            new_polars_code_input = _migrate_dataclass_to_basemodel(
                polars_code_input, transform_schema.PolarsCodeInput
            )
            node_polars.polars_code_input = new_polars_code_input


# =============================================================================
# FLOW-LEVEL COMPATIBILITY
# =============================================================================

def ensure_flow_settings(flow_storage_obj: schemas.FlowInformation, flow_path: str):
    """Ensure flow_settings exists and has all required fields."""
    if not hasattr(flow_storage_obj, 'flow_settings') or flow_storage_obj.flow_settings is None:
        flow_settings = schemas.FlowSettings(
            flow_id=flow_storage_obj.flow_id,
            path=flow_path,
            name=flow_storage_obj.flow_name
        )
        setattr(flow_storage_obj, 'flow_settings', flow_settings)
        flow_storage_obj = schemas.FlowInformation.model_validate(flow_storage_obj)
        return flow_storage_obj

    fs = flow_storage_obj.flow_settings

    if not hasattr(fs, 'execution_location'):
        setattr(fs, 'execution_location', "remote")

    if not hasattr(fs, 'is_running'):
        setattr(fs, 'is_running', False)

    if not hasattr(fs, 'is_canceled'):
        setattr(fs, 'is_canceled', False)

    if not hasattr(fs, 'show_detailed_progress'):
        setattr(fs, 'show_detailed_progress', True)

    return flow_storage_obj


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def ensure_compatibility(flow_storage_obj: schemas.FlowInformation, flow_path: str):
    """
    Main compatibility function - migrates old flowfile schemas to current version.

    Handles migrations for:
    - FlowSettings structure
    - NodeRead (ReceivedTable with table_settings)
    - NodeOutput (OutputSettings with table_settings)
    - NodeSelect (position attributes, dataclass -> BaseModel)
    - NodeJoin/NodeFuzzyMatch (join input positions, dataclass -> BaseModel)
    - NodePolarsCode (depending_on_ids, dataclass -> BaseModel)
    - Node descriptions
    """
    flow_storage_obj = ensure_flow_settings(flow_storage_obj, flow_path)

    for _id, node_information in flow_storage_obj.data.items():
        if not hasattr(node_information, 'setting_input') or node_information.setting_input is None:
            continue

        setting_input = node_information.setting_input
        class_name = setting_input.__class__.__name__

        if class_name == 'NodeRead':
            ensure_compatibility_node_read(setting_input)
        elif class_name == 'NodeSelect':
            ensure_compatibility_node_select(setting_input)
        elif class_name == 'NodeOutput':
            ensure_compatibility_node_output(setting_input)
        elif class_name in ('NodeJoin', 'NodeFuzzyMatch'):
            ensure_compatibility_node_joins(setting_input)
        elif class_name == 'NodePolarsCode':
            ensure_compatibility_node_polars(setting_input)

        ensure_description(setting_input)

    return flow_storage_obj


def load_and_migrate_flowfile(flow_path: str) -> schemas.FlowInformation:
    """
    Convenience function: Load a flowfile and apply all compatibility migrations.

    Args:
        flow_path: Path to the .flowfile pickle

    Returns:
        Fully migrated FlowInformation object
    """
    flow_storage_obj = load_flowfile_pickle(flow_path)
    return ensure_compatibility(flow_storage_obj, flow_path)
