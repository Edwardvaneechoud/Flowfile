from flowfile_core.schemas import schemas, input_schema


def ensure_compatibility_node_read(node_read: input_schema.NodeRead):
    if hasattr(node_read, 'received_file'):
        if not hasattr(node_read.received_file, 'fields'):
            print('setting fields')
            setattr(node_read.received_file, 'fields', [])


def ensure_compatibility_node_select(node_select: input_schema.NodeSelect):
    if hasattr(node_select, 'select_input'):
        if any(not hasattr(select_input, 'position') for select_input in node_select.select_input):
            for _index, select_input in enumerate(node_select.select_input):
                setattr(select_input, 'position', _index)


def ensure_compatibility_node_joins(node_settings: input_schema.NodeFuzzyMatch | input_schema.NodeJoin):
    if any(not hasattr(r,'position') for r in node_settings.join_input.right_select.renames):
        for _index, select_input in enumerate(node_settings.join_input.right_select.renames +
                                              node_settings.join_input.left_select.renames):
            setattr(select_input, 'position', _index)


def ensure_description(node: input_schema.NodeBase):
    if not hasattr(node, 'description'):
        setattr(node, 'description', '')


def ensure_compatibility(flow_storage_obj: schemas.FlowInformation, flow_path: str):
    if not hasattr(flow_storage_obj, 'flow_settings'):
        flow_settings = schemas.FlowSettings(flow_id=flow_storage_obj.flow_id, path=flow_path,
                                             name=flow_storage_obj.flow_name)
        setattr(flow_storage_obj, 'flow_settings', flow_settings)
        flow_storage_obj = schemas.FlowInformation.parse_obj(flow_storage_obj)
    for _id, node_information in flow_storage_obj.data.items():
        if not hasattr(node_information, 'setting_input'):
            continue
        if node_information.setting_input.__class__.__name__ == 'NodeRead':
            ensure_compatibility_node_read(node_information.setting_input)
        elif node_information.setting_input.__class__.__name__ == 'NodeSelect':
            ensure_compatibility_node_select(node_information.setting_input)
        elif node_information.setting_input.__class__.__name__ in ('NodeJoin', 'NodeFuzzyMatch'):
            ensure_compatibility_node_joins(node_information.setting_input)
        ensure_description(node_information.setting_input)


