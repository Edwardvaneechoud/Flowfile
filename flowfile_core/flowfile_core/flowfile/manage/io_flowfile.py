from flowfile_core.schemas import schemas, input_schema
from typing import List, Tuple
from flowfile_core.flowfile.manage.compatibility_enhancements import ensure_compatibility, load_flowfile_pickle
from flowfile_core.flowfile.flow_graph import FlowGraph
from pathlib import Path
from flowfile_core.configs.node_store import CUSTOM_NODE_STORE

try:
    import yaml
except ImportError:
    yaml = None


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
    import json

    with open(flow_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Load as FlowfileData first (handles setting_input validation via node type)
    flowfile_data = schemas.FlowfileData.model_validate(data)

    # Convert to FlowInformation
    return _flowfile_data_to_flow_information(flowfile_data)


def _flowfile_data_to_flow_information(flowfile_data: schemas.FlowfileData) -> schemas.FlowInformation:
    """
    Convert FlowfileData (new YAML format) to FlowInformation (internal format).

    Args:
        flowfile_data: The FlowfileData from YAML

    Returns:
        FlowInformation object for use with FlowGraph
    """
    # Convert nodes list to dict keyed by ID
    nodes_dict = {}
    for node in flowfile_data.nodes:
        node_id = node.id
        nodes_dict[node_id] = node

    # Convert NodeConnection objects to tuples
    connections = [
        (conn.from_node_id, conn.to_node_id)
        for conn in flowfile_data.node_connections
    ]

    return schemas.FlowInformation(
        flow_id=flowfile_data.flowfile_id,
        flow_name=flowfile_data.flowfile_name,
        flow_settings=flowfile_data.flowfile_settings,
        data=nodes_dict,
        node_starts=flowfile_data.starting_node_ids or [],
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
    suffix = flow_path.suffix.lower()
    if suffix == '.flowfile':
        # Legacy pickle format
        flow_storage_obj = load_flowfile_pickle(str(flow_path))
        ensure_compatibility(flow_storage_obj, str(flow_path))
        return flow_storage_obj

    elif suffix in ('.yaml', '.yml'):
        # New YAML format
        return _load_flowfile_yaml(flow_path)

    elif suffix == '.json':
        # JSON format
        return _load_flowfile_json(flow_path)

    else:
        # Try to detect format by content
        try:
            # Try YAML first
            return _load_flowfile_yaml(flow_path)
        except Exception:
            # Fall back to pickle
            flow_storage_obj = load_flowfile_pickle(str(flow_path))
            ensure_compatibility(flow_storage_obj, str(flow_path))
            return flow_storage_obj


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
    flow_storage_obj = _load_flow_storage(flow_path)
    # Update path info
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

    # Second pass: add actual nodes and connections
    for node_id in ingestion_order:
        node_info: schemas.NodeInformation = flow_storage_obj.data[node_id]

        # Handle user-defined nodes
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