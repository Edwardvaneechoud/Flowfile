import json
from collections.abc import Callable
from pathlib import Path

from flowfile_core.configs.node_store import CUSTOM_NODE_STORE, register_missing_node_template
from flowfile_core.configs.settings import is_docker_mode
from flowfile_core.flowfile.flow_graph import FlowGraph, restore_dynamic_input_connections
from flowfile_core.flowfile.flow_node.multi_output import DEFAULT_OUTPUT_HANDLE
from flowfile_core.flowfile.manage.compatibility_enhancements import ensure_compatibility, load_flowfile_pickle
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.schemas import get_settings_class_for_node_type
from shared.storage_config import storage

try:
    import yaml
except ImportError:
    yaml = None


def _validate_flow_path(flow_path: Path) -> Path:
    """Validate flow path is within allowed directories or is an explicit absolute path."""
    resolved = flow_path.resolve()

    allowed_extensions = {".yaml", ".yml", ".json", ".flowfile"}
    if resolved.suffix.lower() not in allowed_extensions:
        raise ValueError(f"Unsupported file extension: {resolved.suffix}")

    if not resolved.is_file():
        raise FileNotFoundError(f"Flow file not found: {resolved}")

    if is_docker_mode():
        safe_directories = [
            storage.flows_directory,
            storage.uploads_directory,
            storage.temp_directory_for_flows,
        ]
        is_safe = any(resolved.is_relative_to(safe_dir) for safe_dir in safe_directories)
    else:
        is_safe = True

    if not is_safe and not flow_path.is_absolute():
        raise ValueError(
            f"Relative paths must be within flows or uploads directory. "
            f"Use absolute path or place file in: {storage.flows_directory}"
        )

    return resolved


def _derive_connections_from_nodes(nodes: list[schemas.FlowfileNode]) -> list[tuple[int, int]]:
    """Derive node connections from the outputs stored in each node."""
    connections = []
    for node in nodes:
        if node.outputs:
            for output_id in node.outputs:
                connections.append((node.id, output_id))
    return connections


def determine_insertion_order(node_storage: schemas.FlowInformation):
    ingest_order: list[int] = []
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
        input_ids = [
            n
            for n in [current_node.left_input_id, current_node.right_input_id] + main_input_ids
            if (n is not None and n not in ingest_order_set)
        ]
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
    with open(flow_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    # Load as FlowfileData first (handles setting_input validation via node type)
    flowfile_data = schemas.FlowfileData.model_validate(data)
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
    with open(flow_path, encoding="utf-8") as f:
        data = json.load(f)

    # Load as FlowfileData first (handles setting_input validation via node type)
    flowfile_data = schemas.FlowfileData.model_validate(data)

    return _flowfile_data_to_flow_information(flowfile_data)


def _keep_auto_description_auto(setting_input) -> None:
    """A stored description equal to the node's default stays auto-generated (empty) instead of freezing as text."""
    try:
        if setting_input.description and setting_input.description == setting_input.get_default_description():
            setting_input.description = ""
    except Exception:
        pass


def _flowfile_data_to_flow_information(flowfile_data: schemas.FlowfileData) -> schemas.FlowInformation:
    nodes_dict = {}
    node_starts = []
    for node in flowfile_data.nodes:
        setting_input = None
        if node.setting_input is not None:
            raw_setting = node.setting_input if isinstance(node.setting_input, dict) else None
            model_class = get_settings_class_for_node_type(node.type, raw_setting)

            if model_class is None:
                raise ValueError(f"Unknown node type: {node.type}")

            is_user_defined = model_class == input_schema.UserDefinedNode

            # Inject fields that were excluded during serialization
            setting_data = (
                node.setting_input if isinstance(node.setting_input, dict) else node.setting_input.model_dump()
            )
            setting_data["flow_id"] = flowfile_data.flowfile_id
            setting_data["node_id"] = node.id
            setting_data["pos_x"] = float(node.x_position or 0)
            setting_data["pos_y"] = float(node.y_position or 0)
            setting_data["group_id"] = node.group_id
            setting_data["description"] = node.description or ""
            setting_data["node_reference"] = node.node_reference
            setting_data["is_setup"] = True

            if is_user_defined:
                setting_data["is_user_defined"] = True
                depending_ids = list(node.input_ids or [])
                if node.left_input_id:
                    depending_ids.append(node.left_input_id)
                if node.right_input_id:
                    depending_ids.append(node.right_input_id)
                setting_data["depending_on_ids"] = depending_ids
            else:
                if "depending_on_id" in model_class.model_fields:
                    setting_data["depending_on_id"] = node.input_ids[0] if node.input_ids else -1
                if "depending_on_ids" in model_class.model_fields:
                    depending_ids = list(node.input_ids or [])
                    if node.left_input_id:
                        depending_ids.append(node.left_input_id)
                    if node.right_input_id:
                        depending_ids.append(node.right_input_id)
                    setting_data["depending_on_ids"] = depending_ids

                if node.type == "output" and "output_settings" in setting_data:
                    output_settings = setting_data["output_settings"]
                    file_type = output_settings.get("file_type", None)
                    if file_type is None:
                        raise ValueError("Output node's output_settings must include 'file_type'")
                    if "table_settings" not in output_settings:
                        output_settings["table_settings"] = {"file_type": file_type}

            setting_input = model_class.model_validate(setting_data)
            _keep_auto_description_auto(setting_input)

        node_info = schemas.NodeInformation(
            id=node.id,
            type=node.type,
            is_setup=setting_input is not None,
            description=node.description,
            node_reference=node.node_reference,
            x_position=node.x_position,
            y_position=node.y_position,
            group_id=node.group_id,
            left_input_id=node.left_input_id,
            right_input_id=node.right_input_id,
            input_ids=node.input_ids,
            outputs=node.outputs,
            output_handles=node.output_handles,
            input_connections=node.input_connections,
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
        validate_settings=flowfile_data.flowfile_settings.validate_settings,
        max_parallel_workers=flowfile_data.flowfile_settings.max_parallel_workers,
        source_registration_id=flowfile_data.flowfile_settings.source_registration_id,
        parameters=flowfile_data.flowfile_settings.parameters,
    )

    return schemas.FlowInformation(
        flow_id=flowfile_data.flowfile_id,
        flow_name=flowfile_data.flowfile_name,
        flow_settings=flow_settings,
        data=nodes_dict,
        node_starts=node_starts,
        node_connections=connections,
        groups=[schemas.GroupInformation(**group.model_dump()) for group in flowfile_data.groups],
        comments=[schemas.CommentInformation(**comment.model_dump()) for comment in flowfile_data.comments],
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
    # legacy method
    if suffix == ".flowfile":
        try:
            flow_storage_obj = load_flowfile_pickle(str(flow_path))
            ensure_compatibility(flow_storage_obj, str(flow_path))
            return flow_storage_obj
        except Exception as e:
            raise ValueError(
                f"Failed to open legacy .flowfile: {e}\n\n" f"Try migrating: migrate_flowfile('{flow_path}')"
            ) from e

    elif suffix in (".yaml", ".yml"):
        return _load_flowfile_yaml(flow_path)

    elif suffix == ".json":
        return _load_flowfile_json(flow_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def _resolve_flow_name(flow_path: Path, stored_name: str | None) -> str:
    """The display name for a flow being opened from disk.

    App-named scratch files get machine-generated stems
    ("Unnamed_flow_20260730_181546_493427479"), and an unregistered draft has no catalog
    row to supply a name, so the name stored in the file is the only readable one it has.
    For a path the user chose the stem stays authoritative, so renaming or copying a flow
    file still renames the flow.
    """
    name = (stored_name or "").strip()
    if name and storage.is_scratch_flow_path(str(flow_path)):
        return name
    return flow_path.stem


def _source_handle(flow_info: schemas.FlowInformation, source_id: int, target_id: int) -> str:
    """The output handle the saved edge source -> target leaves through (``outputs`` is parallel to it)."""
    source = flow_info.data.get(source_id)
    if source is None:
        return DEFAULT_OUTPUT_HANDLE
    # Legacy pickled NodeInformation may lack the field entirely.
    handles = getattr(source, "output_handles", None) or []
    for index, output_id in enumerate(source.outputs or []):
        if output_id == target_id:
            return handles[index] if index < len(handles) else DEFAULT_OUTPUT_HANDLE
    return DEFAULT_OUTPUT_HANDLE


def _add_node_promise(graph: FlowGraph, node_info: schemas.NodeInformation) -> None:
    if getattr(node_info.setting_input, "is_user_defined", False) and node_info.type not in CUSTOM_NODE_STORE:
        register_missing_node_template(node_info.type)
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_info.id,
        pos_x=node_info.x_position or 0,
        pos_y=node_info.y_position or 0,
        node_type=node_info.type,
        group_id=getattr(node_info, "group_id", None),
    )
    if node_info.setting_input is None:
        # Unconfigured nodes hold their metadata on the promise itself, assigned the way the editor routes store it.
        node_promise.description = node_info.description or ""
        node_promise.node_reference = node_info.node_reference
    if hasattr(node_info.setting_input, "cache_results"):
        node_promise.cache_results = node_info.setting_input.cache_results
    graph.add_node_promise(node_promise)


def _wire_static_inputs(graph: FlowGraph, node_info: schemas.NodeInformation, flow_info: schemas.FlowInformation):
    """Connect a node's static inputs in their saved order: main inputs, then left, then right."""
    to_node = graph.get_node(node_info.id)
    if to_node is None or to_node.accepts_dynamic_inputs:
        return  # keyed edges are restored from input_connections
    wiring = [("main", source_id) for source_id in node_info.input_ids or []]
    wiring.append(("left", getattr(node_info, "left_input_id", None)))
    wiring.append(("right", getattr(node_info, "right_input_id", None)))
    for insert_type, source_id in wiring:
        from_node = graph.get_node(source_id) if source_id is not None else None
        if from_node is not None:
            to_node.add_node_connection(
                from_node, insert_type, output_handle=_source_handle(flow_info, source_id, node_info.id)
            )


def _apply_node_settings(
    graph: FlowGraph, node_info: schemas.NodeInformation, owner_of: Callable[[int], int | None] | None
) -> None:
    setting_input = node_info.setting_input
    if hasattr(setting_input, "flow_id"):
        setting_input.flow_id = graph.flow_id
    if owner_of is not None and hasattr(setting_input, "user_id"):
        setting_input.user_id = owner_of(node_info.id)
    if getattr(setting_input, "is_user_defined", False):
        # Execs the node module lazily; a missing or broken node lands in the error path with its settings kept.
        graph._place_user_defined_node(node_info.type, setting_input)
    else:
        getattr(graph, "add_" + node_info.type)(setting_input)


def _repair_source_only_connections(graph: FlowGraph, flow_info: schemas.FlowInformation) -> None:
    """Legacy files can list an edge only on its source; wire it as main into an input-less target."""
    for from_id, to_id in set(flow_info.node_connections) - set(graph.node_connections):
        from_node, to_node = graph.get_node(from_id), graph.get_node(to_id)
        if from_node is None or to_node is None or to_node.accepts_dynamic_inputs:
            continue
        if not to_node.has_input:
            to_node.add_node_connection(from_node)


def populate_graph_from_flow_information(
    graph: FlowGraph,
    flow_info: schemas.FlowInformation,
    owner_of: Callable[[int], int | None] | None = None,
) -> None:
    """Build every node, edge, group and comment of ``flow_info`` into an empty ``graph``.

    The single graph builder: ``open_flow`` and ``FlowGraph.restore_from_snapshot`` both
    go through it, so a snapshot and a saved file rebuild identically
    (``snapshot(build(s)) == s``). Nodes are created in their saved order, then configured
    in dependency order with each node's static inputs wired (in saved input order, with
    their saved output handles) before its settings are applied; keyed edges of
    dynamic-input nodes follow once every node is configured. Start nodes are not read
    from the file: each source's ``add_*`` marks itself, so a stale ``is_start_node``
    flag can never resurface. The caller holds ``graph.rebuilding()``, so nothing is
    recorded.

    Args:
        graph: An empty graph whose settings and identity are kept as-is.
        flow_info: The flow to build.
        owner_of: Maps a node id to the ``user_id`` to stamp on its settings; None leaves
            ``user_id`` untouched.
    """
    for node_info in flow_info.data.values():
        _add_node_promise(graph, node_info)

    for node_id in determine_insertion_order(flow_info):
        node_info = flow_info.data[node_id]
        _wire_static_inputs(graph, node_info, flow_info)
        if node_info.is_setup and node_info.setting_input is not None:
            _apply_node_settings(graph, node_info, owner_of)

    restore_dynamic_input_connections(graph, flow_info)
    _repair_source_only_connections(graph, flow_info)

    # Legacy pickles may lack groups/comments entirely.
    graph.restore_groups(getattr(flow_info, "groups", None) or [])
    graph.restore_comments(getattr(flow_info, "comments", None) or [])


def open_flow(flow_path: Path, user_id: int | None = None) -> FlowGraph:
    """
    Open a flowfile from a given path.

    Supports multiple formats:
    - .flowfile (pickle) - legacy format, auto-migrated
    - .yaml / .yml - new YAML format
    - .json - JSON format

    The flow opens with an empty undo history and as the clean saved baseline.

    Args:
        flow_path (Path): The absolute or relative path to the flowfile
        user_id (int | None): The ID of the user importing the flow, used to resolve cloud connections.
    Returns:
        FlowGraph: The flowfile object
    """
    flow_path = _validate_flow_path(flow_path)
    flow_storage_obj = _load_flow_storage(flow_path)
    flow_storage_obj.flow_settings.path = str(flow_path)
    resolved_name = _resolve_flow_name(flow_path, flow_storage_obj.flow_name)
    flow_storage_obj.flow_settings.name = resolved_name
    flow_storage_obj.flow_name = resolved_name

    new_flow = FlowGraph(name=flow_storage_obj.flow_name, flow_settings=flow_storage_obj.flow_settings)
    if user_id is not None:
        new_flow._owner_user_id = user_id
    owner_of = (lambda _node_id: user_id) if user_id is not None else None
    with new_flow.rebuilding():
        populate_graph_from_flow_information(new_flow, flow_storage_obj, owner_of=owner_of)
    new_flow.mark_as_saved()
    return new_flow
