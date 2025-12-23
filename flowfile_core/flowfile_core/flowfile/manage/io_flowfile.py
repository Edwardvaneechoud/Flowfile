from flowfile_core.schemas import schemas, input_schema
from typing import List, Tuple
from flowfile_core.flowfile.manage.compatibility_enhancements import ensure_compatibility, load_flowfile_pickle
from flowfile_core.flowfile.flow_graph import FlowGraph
from pathlib import Path
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE
from flowfile_core.configs.settings import IS_RUNNING_IN_DOCKER
import json
from shared.storage_config import storage


try:
    import yaml
except ImportError:
    yaml = None


def _validate_flow_path(flow_path: Path) -> Path:
    """Validate flow path is within allowed directories or is an explicit absolute path."""
    resolved = flow_path.resolve()

    # Check extension
    allowed_extensions = {'.yaml', '.yml', '.json', '.flowfile'}
    if resolved.suffix.lower() not in allowed_extensions:
        raise ValueError(f"Unsupported file extension: {resolved.suffix}")

    # Check file exists
    if not resolved.is_file():
        raise FileNotFoundError(f"Flow file not found: {resolved}")

    # Allow paths within known safe directories

    if IS_RUNNING_IN_DOCKER:
        safe_directories = [
            storage.flows_directory,
            storage.uploads_directory,
            storage.temp_directory_for_flows,
        ]
        is_safe = any(
            resolved.is_relative_to(safe_dir)
            for safe_dir in safe_directories
        )
    else:
        is_safe = True

    if not is_safe and not flow_path.is_absolute():
        raise ValueError(
            f"Relative paths must be within flows or uploads directory. "
            f"Use absolute path or place file in: {storage.flows_directory}"
        )

    return resolved


def _derive_connections_from_nodes(nodes: List[schemas.FlowfileNode]) -> List[Tuple[int, int]]:
    """Derive node connections from the outputs stored in each node."""
    connections = []
    for node in nodes:
        if node.outputs:
            for output_id in node.outputs:
                connections.append((node.id, output_id))
    return connections


def determine_insertion_order(node_storage: schemas.FlowInformation):
    ingest_order: List[int] = []
    ingest_order_set: set[int] = set()
    all_nodes = set(node_storage.data.keys())

    def assure_output_id(input_node: schemas.NodeInformation, output_node: schemas.NodeInformation):
        # assure the output id is in the list with outputs of the input node this is a quick fix
        if output_node.id not in input_node.outputs:
            input_node.outputs.append(output_node.id)

    def determine_order(node_id: int):
        current_node = node_storage.data.get(node_id)
        if current_node is None:
            return
        output_ids = current_node.outputs
        main_input_ids = current_node.input_ids if current_node.input_ids else []
        input_ids = [n for n in [current_node.left_input_id,
                                 current_node.right_input_id] + main_input_ids if (n is not None
                                                                                   and n not in
                                                                                   ingest_order_set)]
        if len(input_ids) > 0:
            for input_id in input_ids:
                new_node = node_storage.data.get(input_id)
                if new_node is None:
                    ingest_order.append(current_node.id)
                    ingest_order_set.add(current_node.id)
                    continue
                assure_output_id(new_node, current_node)
                if new_node.id not in ingest_order_set:
                    determine_order(input_id)
        elif current_node.id not in ingest_order_set:
            ingest_order.append(current_node.id)
            ingest_order_set.add(current_node.id)

        for output_id in output_ids:
            if output_id not in ingest_order_set:
                determine_order(output_id)

    if len(node_storage.node_starts) > 0:
        determine_order(node_storage.node_starts[0])
    # add the random not connected nodes
    else:
        for node_id in all_nodes:
            determine_order(node_id)
    ingest_order += list(all_nodes - ingest_order_set)
    return ingest_order


def _load_flowfile_yaml(flow_path: Path) -> schemas.FlowInformation:
    """
    Load a flowfile from YAML format and convert to FlowInformation.

    Args:
        flow_path: Path to the YAML file

    Returns:
        FlowInformation object
    """
    if yaml is None:
        raise ImportError("PyYAML is required for YAML files. Install with: pip install pyyaml")
    flow_path = _validate_flow_path(flow_path)
    with open(flow_path, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    # Load as FlowfileData first (handles setting_input validation via node type)
    flowfile_data = schemas.FlowfileData.model_validate(data)
    # Convert to FlowInformation
    return _flowfile_data_to_flow_information(flowfile_data)


def _load_flowfile_json(flow_path: Path) -> schemas.FlowInformation:
    """
    Load a flowfile from JSON format and convert to FlowInformation.

    Args:
        flow_path: Path to the JSON file

    Returns:
        FlowInformation object
    """
    flow_path = _validate_flow_path(flow_path)
    with open(flow_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Load as FlowfileData first (handles setting_input validation via node type)
    flowfile_data = schemas.FlowfileData.model_validate(data)

    # Convert to FlowInformation
    return _flowfile_data_to_flow_information(flowfile_data)


def _flowfile_data_to_flow_information(flowfile_data: schemas.FlowfileData) -> schemas.FlowInformation:
    from flowfile_core.schemas.schemas import get_settings_class_for_node_type

    nodes_dict = {}
    node_starts = []
    for node in flowfile_data.nodes:
        setting_input = None
        if node.setting_input is not None:
            model_class = get_settings_class_for_node_type(node.type)

            if model_class is None:
                raise ValueError(f"Unknown node type: {node.type}")

            is_user_defined = model_class == input_schema.UserDefinedNode

            # Inject fields that were excluded during serialization
            setting_data = node.setting_input if isinstance(node.setting_input, dict) else node.setting_input.model_dump()
            setting_data['flow_id'] = flowfile_data.flowfile_id
            setting_data['node_id'] = node.id
            setting_data['pos_x'] = float(node.x_position or 0)
            setting_data['pos_y'] = float(node.y_position or 0)
            setting_data['description'] = node.description or ''
            setting_data['is_setup'] = True

            if is_user_defined:
                setting_data['is_user_defined'] = True
                depending_ids = list(node.input_ids or [])
                if node.left_input_id:
                    depending_ids.append(node.left_input_id)
                if node.right_input_id:
                    depending_ids.append(node.right_input_id)
                setting_data['depending_on_ids'] = depending_ids
            else:
                if 'depending_on_id' in model_class.model_fields:
                    setting_data['depending_on_id'] = node.input_ids[0] if node.input_ids else -1
                if 'depending_on_ids' in model_class.model_fields:
                    depending_ids = list(node.input_ids or [])
                    if node.left_input_id:
                        depending_ids.append(node.left_input_id)
                    if node.right_input_id:
                        depending_ids.append(node.right_input_id)
                    setting_data['depending_on_ids'] = depending_ids

                if node.type == 'output' and 'output_settings' in setting_data:
                    output_settings = setting_data['output_settings']
                    file_type = output_settings.get('file_type', None)
                    if file_type is None:
                        raise ValueError("Output node's output_settings must include 'file_type'")
                    if 'table_settings' not in output_settings:
                        output_settings['table_settings'] = {"file_type": file_type}

            setting_input = model_class.model_validate(setting_data)

        node_info = schemas.NodeInformation(
            id=node.id,
            type=node.type,
            is_setup=setting_input is not None,
            description=node.description,
            x_position=node.x_position,
            y_position=node.y_position,
            left_input_id=node.left_input_id,
            right_input_id=node.right_input_id,
            input_ids=node.input_ids,
            outputs=node.outputs,
            setting_input=setting_input,
        )
        nodes_dict[node.id] = node_info
        if node.is_start_node:
            node_starts.append(node.id)

    connections = _derive_connections_from_nodes(flowfile_data.nodes)

    flow_settings = schemas.FlowSettings(
        flow_id=flowfile_data.flowfile_id,
        name=flowfile_data.flowfile_name,
        description=flowfile_data.flowfile_settings.description,
        execution_mode=flowfile_data.flowfile_settings.execution_mode,
        execution_location=flowfile_data.flowfile_settings.execution_location,
        auto_save=flowfile_data.flowfile_settings.auto_save,
        show_detailed_progress=flowfile_data.flowfile_settings.show_detailed_progress,
    )

    return schemas.FlowInformation(
        flow_id=flowfile_data.flowfile_id,
        flow_name=flowfile_data.flowfile_name,
        flow_settings=flow_settings,
        data=nodes_dict,
        node_starts=node_starts,
        node_connections=connections,
    )

def _load_flow_storage(flow_path: Path) -> schemas.FlowInformation:
    """
    Load flow storage from any supported format.

    Supports:
    - .flowfile (pickle) - legacy format
    - .yaml / .yml - new YAML format
    - .json - JSON format

    Args:
        flow_path: Path to the flowfile

    Returns:
        FlowInformation object
    """
    flow_path = _validate_flow_path(flow_path)
    suffix = flow_path.suffix.lower()
    if suffix == '.flowfile':
        try:
            flow_storage_obj = load_flowfile_pickle(str(flow_path))
            ensure_compatibility(flow_storage_obj, str(flow_path))
            return flow_storage_obj
        except Exception as e:
            raise ValueError(
                f"Failed to open legacy .flowfile: {e}\n\n"
                f"Try migrating: migrate_flowfile('{flow_path}')"
            ) from e

    elif suffix in ('.yaml', '.yml'):
        return _load_flowfile_yaml(flow_path)

    elif suffix == '.json':
        return _load_flowfile_json(flow_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def open_flow(flow_path: Path) -> FlowGraph:
    """
    Open a flowfile from a given path.

    Supports multiple formats:
    - .flowfile (pickle) - legacy format, auto-migrated
    - .yaml / .yml - new YAML format
    - .json - JSON format

    Args:
        flow_path (Path): The absolute or relative path to the flowfile

    Returns:
        FlowGraph: The flowfile object
    """
    # Load flow storage (handles format detection)
    flow_path = _validate_flow_path(flow_path)
    flow_storage_obj = _load_flow_storage(flow_path)
    flow_storage_obj.flow_settings.path = str(flow_path)
    flow_storage_obj.flow_settings.name = str(flow_path.stem)
    flow_storage_obj.flow_name = str(flow_path.stem)

    # Determine node insertion order
    ingestion_order = determine_insertion_order(flow_storage_obj)

    # Create new FlowGraph
    new_flow = FlowGraph(name=flow_storage_obj.flow_name, flow_settings=flow_storage_obj.flow_settings)

    # First pass: add node promises
    for node_id in ingestion_order:
        node_info: schemas.NodeInformation = flow_storage_obj.data[node_id]
        node_promise = input_schema.NodePromise(
            flow_id=new_flow.flow_id,
            node_id=node_info.id,
            pos_x=node_info.x_position,
            pos_y=node_info.y_position,
            node_type=node_info.type
        )
        if hasattr(node_info.setting_input, 'cache_results'):
            node_promise.cache_results = node_info.setting_input.cache_results
        new_flow.add_node_promise(node_promise)

    for node_id in ingestion_order:
        node_info: schemas.NodeInformation = flow_storage_obj.data[node_id]
        if node_info.is_setup:
            if hasattr(node_info.setting_input, "is_user_defined") and node_info.setting_input.is_user_defined:
                if node_info.type not in CUSTOM_NODE_STORE:
                    continue
                user_defined_node_class = CUSTOM_NODE_STORE[node_info.type]
                new_flow.add_user_defined_node(
                    custom_node=user_defined_node_class.from_settings(node_info.setting_input.settings),
                    user_defined_node_settings=node_info.setting_input
                )
            else:
                getattr(new_flow, 'add_' + node_info.type)(node_info.setting_input)

        # Setup connections
        from_node = new_flow.get_node(node_id)
        for output_node_id in (node_info.outputs or []):

            to_node = new_flow.get_node(output_node_id)
            if to_node is not None:
                output_node_obj = flow_storage_obj.data[output_node_id]
                is_left_input = (output_node_obj.left_input_id == node_id) and (
                    to_node.left_input.node_id != node_id if to_node.left_input is not None else True
                )
                is_right_input = (output_node_obj.right_input_id == node_id) and (
                    to_node.right_input.node_id != node_id if to_node.right_input is not None else True
                )
                is_main_input = node_id in (output_node_obj.input_ids or [])

                if is_left_input:
                    insert_type = 'left'
                elif is_right_input:
                    insert_type = 'right'
                elif is_main_input:
                    insert_type = 'main'
                else:
                    continue
                to_node.add_node_connection(from_node, insert_type)
            else:
                from_node.delete_lead_to_node(output_node_id)
                if not (from_node.node_id, output_node_id) in flow_storage_obj.node_connections:
                    continue
                flow_storage_obj.node_connections.pop(
                    flow_storage_obj.node_connections.index((from_node.node_id, output_node_id))
                )

    # Handle any missing connections
    for missing_connection in set(flow_storage_obj.node_connections) - set(new_flow.node_connections):
        to_node = new_flow.get_node(missing_connection[1])
        if not to_node.has_input:
            test_if_circular_connection(missing_connection, new_flow)
            from_node = new_flow.get_node(missing_connection[0])
            if from_node:
                to_node.add_node_connection(from_node)

    return new_flow


def test_if_circular_connection(connection: Tuple[int, int], flow: FlowGraph):
    to_node = flow.get_node(connection[1])
    leads_to_nodes_queue = [n for n in to_node.leads_to_nodes]
    circular_connection: bool = False
    while len(leads_to_nodes_queue) > 0:
        leads_to_node = leads_to_nodes_queue.pop(0)
        if leads_to_node.node_id == connection[0]:
            circular_connection = True
            break
        for leads_to_node_leads_to in leads_to_node.leads_to_nodes:
            leads_to_nodes_queue.append(leads_to_node_leads_to)
    return circular_connection
