"""
Migration logic for converting old flowfile pickles to new YAML format.
"""

import pickle
from dataclasses import fields, is_dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional
import sys

try:
    import yaml
except ImportError:
    yaml = None

from tools.migrate.legacy_schemas import LEGACY_CLASS_MAP


class LegacyUnpickler(pickle.Unpickler):
    """
    Custom unpickler that redirects class lookups to legacy dataclass definitions.

    ONLY intercepts classes from transform_schema.py that changed from @dataclass to BaseModel.
    All other classes (schemas.py, input_schema.py) were already Pydantic and load normally.
    """

    # ONLY these classes changed from @dataclass to BaseModel
    # These are all from flowfile_core/schemas/transform_schema.py
    DATACLASS_TO_PYDANTIC = {
        'SelectInput',
        'FieldInput',
        'FunctionInput',
        'BasicFilter',
        'FilterInput',
        'SelectInputs',
        'JoinInputs',
        'JoinMap',
        'CrossJoinInput',
        'JoinInput',
        'FuzzyMatchInput',
        'AggColl',
        'GroupByInput',
        'PivotInput',
        'SortByInput',
        'RecordIdInput',
        'TextToRowsInput',
        'UnpivotInput',
        'UnionInput',
        'UniqueInput',
        'GraphSolverInput',
        'PolarsCodeInput',
    }

    def find_class(self, module: str, name: str):
        """Override to redirect ONLY transform_schema dataclasses to legacy definitions."""
        # Only intercept classes that changed from dataclass to Pydantic
        if name in self.DATACLASS_TO_PYDANTIC and name in LEGACY_CLASS_MAP:
            return LEGACY_CLASS_MAP[name]

        # Everything else (schemas.py, input_schema.py) loads with actual Pydantic classes
        return super().find_class(module, name)


def load_legacy_flowfile(path: Path) -> Any:
    """
    Load an old flowfile using legacy class definitions.

    Args:
        path: Path to the .flowfile pickle

    Returns:
        The deserialized FlowInformation object (as legacy dataclass)
    """
    with open(path, 'rb') as f:
        return LegacyUnpickler(f).load()


def convert_to_dict(obj: Any, _seen: set = None) -> Any:
    """
    Recursively convert dataclasses, Pydantic models, and complex objects to plain dicts.

    Handles:
    - Pydantic BaseModel instances (via model_dump)
    - Dataclasses (via asdict or manual conversion)
    - Lists, dicts, tuples
    - Primitive types

    Args:
        obj: Object to convert
        _seen: Set of seen object IDs (for cycle detection)

    Returns:
        Plain dict/list/primitive representation
    """
    if _seen is None:
        _seen = set()

    # Handle None
    if obj is None:
        return None

    # Handle primitives
    if isinstance(obj, (str, int, float, bool)):
        return obj

    # Cycle detection
    obj_id = id(obj)
    if obj_id in _seen:
        return f"<circular reference to {type(obj).__name__}>"
    _seen.add(obj_id)

    try:
        # Handle Pydantic models FIRST (check for model_dump method)
        if hasattr(obj, 'model_dump') and callable(obj.model_dump):
            try:
                data = obj.model_dump()
                # Recursively convert any nested structures
                return convert_to_dict(data, _seen)
            except Exception:
                # Fall through to other methods if model_dump fails
                pass

        # Handle dataclasses
        if is_dataclass(obj) and not isinstance(obj, type):
            try:
                # Try asdict first (handles nested dataclasses)
                return asdict(obj)
            except Exception:
                # Fall back to manual conversion
                result = {}
                for f in fields(obj):
                    value = getattr(obj, f.name, None)
                    result[f.name] = convert_to_dict(value, _seen)
                return result

        # Handle dicts
        if isinstance(obj, dict):
            return {k: convert_to_dict(v, _seen) for k, v in obj.items()}

        # Handle lists and tuples - convert both to lists for clean YAML
        if isinstance(obj, (list, tuple)):
            return [convert_to_dict(item, _seen) for item in obj]

        # Handle sets
        if isinstance(obj, set):
            return [convert_to_dict(item, _seen) for item in obj]

        # Handle Path objects
        if isinstance(obj, Path):
            return str(obj)

        # Handle objects with __dict__ (generic fallback)
        if hasattr(obj, '__dict__'):
            return {k: convert_to_dict(v, _seen) for k, v in obj.__dict__.items()
                    if not k.startswith('_')}

        # Fallback: try to convert to string
        return str(obj)

    finally:
        _seen.discard(obj_id)


def transform_to_new_schema(data: Dict) -> Dict:
    """
    Transform the legacy schema structure to the new FlowfileData format.

    This handles:
    - ReceivedTable: flat fields -> nested table_settings
    - OutputSettings: separate table fields -> unified table_settings
    - Field name changes (flow_id -> flowfile_id, etc.)

    Args:
        data: Dict representation of legacy FlowInformation

    Returns:
        Transformed dict ready for YAML serialization (FlowfileData format)
    """
    node_starts = set(data.get('node_starts', []))

    result = {
        'flowfile_version': '2.0',
        'flowfile_id': data.get('flow_id', 1),
        'flowfile_name': data.get('flow_name', ''),
        'flowfile_settings': _transform_flow_settings(data.get('flow_settings', {})),
        'nodes': _transform_nodes(data.get('data', {}), node_starts),
    }

    return result


def _transform_flow_settings(settings: Dict) -> Dict:
    """Transform flow settings to FlowfileSettings format."""
    if not settings:
        return {
            'execution_mode': 'Development',
            'execution_location': 'local',
            'auto_save': False,
            'show_detailed_progress': True,
        }

    return {
        'description': settings.get('description'),
        'execution_mode': settings.get('execution_mode', 'Development'),
        'execution_location': settings.get('execution_location', 'local'),
        'auto_save': settings.get('auto_save', False),
        'show_detailed_progress': settings.get('show_detailed_progress', True),
    }


def _transform_nodes(nodes_data: Dict, node_starts: set) -> List[Dict]:
    """Transform nodes dict to FlowfileNode list format."""
    nodes = []

    for node_id, node_info in nodes_data.items():
        if not isinstance(node_info, dict):
            node_info = convert_to_dict(node_info)

        actual_node_id = node_info.get('id', node_id)

        node = {
            'id': actual_node_id,
            'type': node_info.get('type', ''),
            'is_start_node': actual_node_id in node_starts,
            'description': node_info.get('description', ''),
            'x_position': int(node_info.get('x_position', 0) or 0),
            'y_position': int(node_info.get('y_position', 0) or 0),
            'left_input_id': node_info.get('left_input_id'),
            'right_input_id': node_info.get('right_input_id'),
            'input_ids': node_info.get('input_ids', []),
            'outputs': node_info.get('outputs', []),
        }

        # Transform settings based on node type
        setting_input = node_info.get('setting_input', {})
        if setting_input:
            if not isinstance(setting_input, dict):
                setting_input = convert_to_dict(setting_input)
            node['setting_input'] = _transform_node_settings(node['type'], setting_input)

        nodes.append(node)

    return nodes


def _transform_node_settings(node_type: str, settings: Dict) -> Dict:
    """Transform node-specific settings to new format.

    Handles structural changes for various node types:
    - read: ReceivedTable flat → nested table_settings
    - output: OutputSettings separate tables → unified table_settings
    - polars_code: PolarsCodeInput extraction
    - select: Ensure sorted_by field exists
    - join/fuzzy_match: Handle JoinInput/FuzzyMatchInput changes
    """
    # Remove common fields that are stored elsewhere
    settings = {k: v for k, v in settings.items()
                if k not in ('flow_id', 'node_id', 'pos_x', 'pos_y', 'is_setup',
                            'description', 'cache_results', 'user_id', 'is_flow_output',
                            'is_user_defined')}

    # Handle specific node types
    if node_type == 'read':
        return _transform_read_settings(settings)
    elif node_type == 'output':
        return _transform_output_settings(settings)
    elif node_type == 'polars_code':
        return _transform_polars_code_settings(settings)
    elif node_type == 'select':
        return _transform_select_settings(settings)
    elif node_type in ('join', 'fuzzy_match', 'cross_join'):
        return _transform_join_settings(settings)

    return settings


def _transform_select_settings(settings: Dict) -> Dict:
    """Transform NodeSelect settings - ensure all fields exist."""
    # Ensure sorted_by field exists (added in new version)
    if 'sorted_by' not in settings:
        settings['sorted_by'] = 'none'

    # Ensure select_input items have position field
    select_input = settings.get('select_input', [])
    if isinstance(select_input, list):
        for i, item in enumerate(select_input):
            if isinstance(item, dict) and item.get('position') is None:
                item['position'] = i

    return settings


def _transform_join_settings(settings: Dict) -> Dict:
    """Transform join-related node settings.

    Handles migration of old JoinInput where left_select/right_select could be None.
    New schema requires these to be JoinInputs with renames list.
    """
    # Handle join_input transformation
    join_input = settings.get('join_input') or settings.get('cross_join_input')
    if join_input and isinstance(join_input, dict):
        # ADD DEFAULT EMPTY JoinInputs IF MISSING (required in new schema)
        for side in ['left_select', 'right_select']:
            if join_input.get(side) is None:
                join_input[side] = {'renames': []}

            select = join_input.get(side)
            if select and isinstance(select, dict):
                # Ensure renames key exists
                if 'renames' not in select:
                    select['renames'] = []

                renames = select.get('renames', [])
                if isinstance(renames, list):
                    for i, item in enumerate(renames):
                        if isinstance(item, dict) and item.get('position') is None:
                            item['position'] = i

    return settings


def _transform_read_settings(settings: Dict) -> Dict:
    """Transform NodeRead settings - extract table_settings from old flat structure.

    OLD structure (flat):
        received_file:
            file_type: csv
            delimiter: ","
            encoding: "utf-8"
            sheet_name: null  # Excel fields mixed in
            ...

    NEW structure (nested):
        received_file:
            file_type: csv
            table_settings:
                file_type: csv
                delimiter: ","
                encoding: "utf-8"
    """
    received_file = settings.get('received_file', {})
    if not received_file:
        return settings

    # Check if already transformed (has table_settings)
    if 'table_settings' in received_file and isinstance(received_file['table_settings'], dict):
        return settings

    file_type = received_file.get('file_type', 'csv')

    # Build table_settings based on file_type, extracting from flat structure
    if file_type == 'csv':
        table_settings = {
            'file_type': 'csv',
            'reference': received_file.get('reference', ''),
            'starting_from_line': received_file.get('starting_from_line', 0),
            'delimiter': received_file.get('delimiter', ','),
            'has_headers': received_file.get('has_headers', True),
            'encoding': received_file.get('encoding', 'utf-8') or 'utf-8',
            'parquet_ref': received_file.get('parquet_ref'),
            'row_delimiter': received_file.get('row_delimiter', '\n'),
            'quote_char': received_file.get('quote_char', '"'),
            'infer_schema_length': received_file.get('infer_schema_length', 10000),
            'truncate_ragged_lines': received_file.get('truncate_ragged_lines', False),
            'ignore_errors': received_file.get('ignore_errors', False),
        }
    elif file_type == 'json':
        table_settings = {
            'file_type': 'json',
            'reference': received_file.get('reference', ''),
            'starting_from_line': received_file.get('starting_from_line', 0),
            'delimiter': received_file.get('delimiter', ','),
            'has_headers': received_file.get('has_headers', True),
            'encoding': received_file.get('encoding', 'utf-8') or 'utf-8',
            'parquet_ref': received_file.get('parquet_ref'),
            'row_delimiter': received_file.get('row_delimiter', '\n'),
            'quote_char': received_file.get('quote_char', '"'),
            'infer_schema_length': received_file.get('infer_schema_length', 10000),
            'truncate_ragged_lines': received_file.get('truncate_ragged_lines', False),
            'ignore_errors': received_file.get('ignore_errors', False),
        }
    elif file_type == 'excel':
        table_settings = {
            'file_type': 'excel',
            'sheet_name': received_file.get('sheet_name'),
            'start_row': received_file.get('start_row', 0),
            'start_column': received_file.get('start_column', 0),
            'end_row': received_file.get('end_row', 0),
            'end_column': received_file.get('end_column', 0),
            'has_headers': received_file.get('has_headers', True),
            'type_inference': received_file.get('type_inference', False),
        }
    elif file_type == 'parquet':
        table_settings = {'file_type': 'parquet'}
    else:
        # Unknown file type - try to preserve what we can
        table_settings = {'file_type': file_type or 'csv'}

    # Build new structure with metadata + nested table_settings
    return {
        'received_file': {
            # Metadata fields (preserved from old structure)
            'id': received_file.get('id'),
            'name': received_file.get('name'),
            'path': received_file.get('path', ''),
            'directory': received_file.get('directory'),
            'analysis_file_available': received_file.get('analysis_file_available', False),
            'status': received_file.get('status'),
            'fields': received_file.get('fields', []),
            'abs_file_path': received_file.get('abs_file_path'),
            # New discriminator field
            'file_type': file_type,
            # Nested table settings
            'table_settings': table_settings,
        }
    }


def _transform_output_settings(settings: Dict) -> Dict:
    """Transform NodeOutput settings - consolidate separate table settings into single field.

    OLD structure:
        output_settings:
            file_type: csv
            output_csv_table: {delimiter: ",", encoding: "utf-8"}
            output_parquet_table: {}
            output_excel_table: {sheet_name: "Sheet1"}

    NEW structure:
        output_settings:
            file_type: csv
            table_settings:
                file_type: csv
                delimiter: ","
                encoding: "utf-8"
    """
    output_settings = settings.get('output_settings', {})
    if not output_settings:
        return settings

    # Check if already transformed
    if 'table_settings' in output_settings and isinstance(output_settings['table_settings'], dict):
        return settings

    file_type = output_settings.get('file_type', 'csv')

    # Build table_settings from old separate fields
    if file_type == 'csv':
        old_csv = output_settings.get('output_csv_table', {}) or {}
        table_settings = {
            'file_type': 'csv',
            'delimiter': old_csv.get('delimiter', ','),
            'encoding': old_csv.get('encoding', 'utf-8'),
        }
    elif file_type == 'excel':
        old_excel = output_settings.get('output_excel_table', {}) or {}
        table_settings = {
            'file_type': 'excel',
            'sheet_name': old_excel.get('sheet_name', 'Sheet1'),
        }
    elif file_type == 'parquet':
        table_settings = {'file_type': 'parquet'}
    else:
        table_settings = {'file_type': file_type or 'csv'}

    return {
        'output_settings': {
            'name': output_settings.get('name', ''),
            'directory': output_settings.get('directory', ''),
            'file_type': file_type,
            'fields': output_settings.get('fields', []),
            'write_mode': output_settings.get('write_mode', 'overwrite'),
            'abs_file_path': output_settings.get('abs_file_path'),
            'table_settings': table_settings,
        }
    }


def _transform_polars_code_settings(settings: Dict) -> Dict:
    """Transform NodePolarsCode settings.

    Extracts polars_code from PolarsCodeInput and handles depending_on_id → depending_on_ids.
    """
    polars_code_input = settings.get('polars_code_input', {})

    # Extract the actual code
    polars_code = ''
    if isinstance(polars_code_input, dict):
        polars_code = polars_code_input.get('polars_code', '')
    elif hasattr(polars_code_input, 'polars_code'):
        polars_code = polars_code_input.polars_code

    # Handle depending_on_id → depending_on_ids migration
    depending_on_ids = settings.get('depending_on_ids', [])
    if not depending_on_ids or depending_on_ids == [-1]:
        old_id = settings.get('depending_on_id')
        if old_id is not None and old_id != -1:
            depending_on_ids = [old_id]
        else:
            depending_on_ids = []

    return {
        'polars_code_input': {
            'polars_code': polars_code,
        },
        'depending_on_ids': depending_on_ids,
    }


def migrate_flowfile(input_path: Path, output_path: Path = None, format: str = 'yaml') -> Path:
    """
    Migrate a single flowfile from pickle to YAML format.

    Args:
        input_path: Path to the .flowfile pickle
        output_path: Output path (default: same name with .yaml extension)
        format: Output format ('yaml' or 'json')

    Returns:
        Path to the created output file
    """
    if format == 'yaml' and yaml is None:
        raise ImportError("PyYAML is required for YAML output. Install with: pip install pyyaml")

    # Determine output path
    if output_path is None:
        suffix = '.yaml' if format == 'yaml' else '.json'
        output_path = input_path.with_suffix(suffix)

    print(f"Loading: {input_path}")

    # Load legacy flowfile
    legacy_data = load_legacy_flowfile(input_path)

    # Convert to dict
    data_dict = convert_to_dict(legacy_data)

    # Transform to new schema
    transformed = transform_to_new_schema(data_dict)

    # Write output
    print(f"Writing: {output_path}")

    with open(output_path, 'w', encoding='utf-8') as f:
        if format == 'yaml':
            yaml.dump(transformed, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        else:
            import json
            json.dump(transformed, f, indent=2, ensure_ascii=False)

    print(f"✓ Migrated: {input_path.name} → {output_path.name}")
    return output_path


def migrate_directory(dir_path: Path, output_dir: Path = None, format: str = 'yaml') -> List[Path]:
    """
    Migrate all flowfiles in a directory.

    Args:
        dir_path: Directory containing .flowfile pickles
        output_dir: Output directory (default: same as input)
        format: Output format ('yaml' or 'json')

    Returns:
        List of created output file paths
    """
    output_dir = output_dir or dir_path
    output_dir.mkdir(parents=True, exist_ok=True)

    flowfiles = list(dir_path.glob('**/*.flowfile'))

    if not flowfiles:
        print(f"No .flowfile files found in {dir_path}")
        return []

    print(f"Found {len(flowfiles)} flowfile(s) to migrate\n")

    migrated = []
    failed = []

    for flowfile in flowfiles:
        # Preserve directory structure
        relative = flowfile.relative_to(dir_path)
        suffix = '.yaml' if format == 'yaml' else '.json'
        output_path = output_dir / relative.with_suffix(suffix)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            migrate_flowfile(flowfile, output_path, format)
            migrated.append(output_path)
        except Exception as e:
            print(f"✗ Failed: {flowfile.name} - {e}")
            failed.append((flowfile, e))

    print(f"\n{'='*50}")
    print(f"Migration complete: {len(migrated)} succeeded, {len(failed)} failed")

    return migrated
