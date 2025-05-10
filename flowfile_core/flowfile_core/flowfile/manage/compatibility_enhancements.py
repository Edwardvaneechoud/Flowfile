from flowfile_core.schemas import schemas, input_schema


def ensure_compatibility_node_read(node_read: input_schema.NodeRead):
    if hasattr(node_read, 'received_file'):
        if not hasattr(node_read.received_file, 'fields'):
            print('setting fields')
            setattr(node_read.received_file, 'fields', [])


def ensure_compatibility_node_output(node_output: input_schema.NodeOutput):
    if hasattr(node_output, 'output_settings'):
        if not hasattr(node_output.output_settings, 'abs_file_path'):
            new_output_settings = input_schema.OutputSettings.model_validate(node_output.output_settings.model_dump())
            setattr(node_output, 'output_settings', new_output_settings)


def ensure_compatibility_node_select(node_select: input_schema.NodeSelect):
    if hasattr(node_select, 'select_input'):
        if any(not hasattr(select_input, 'position') for select_input in node_select.select_input):
            for _index, select_input in enumerate(node_select.select_input):
                setattr(select_input, 'position', _index)
        if not hasattr(node_select, 'sorted_by'):
            setattr(node_select, 'sorted_by', 'none')


def ensure_compatibility_node_joins(node_settings: input_schema.NodeFuzzyMatch | input_schema.NodeJoin):
    if any(not hasattr(r, 'position') for r in node_settings.join_input.right_select.renames):
        for _index, select_input in enumerate(node_settings.join_input.right_select.renames +
                                              node_settings.join_input.left_select.renames):
            setattr(select_input, 'position', _index)


def ensure_description(node: input_schema.NodeBase):
    if not hasattr(node, 'description'):
        setattr(node, 'description', '')


def ensure_compatibility_node_polars(node_polars: input_schema.NodePolarsCode):
    if hasattr(node_polars, 'depending_on_id'):
        setattr(node_polars, 'depending_on_ids', [getattr(node_polars, 'depending_on_id')])


def ensure_compatibility(flow_storage_obj: schemas.FlowInformation, flow_path: str):
    if not hasattr(flow_storage_obj, 'flow_settings'):
        flow_settings = schemas.FlowSettings(flow_id=flow_storage_obj.flow_id, path=flow_path,
                                             name=flow_storage_obj.flow_name)
        setattr(flow_storage_obj, 'flow_settings', flow_settings)
        flow_storage_obj = schemas.FlowInformation.model_validate(flow_storage_obj)
    elif not hasattr(getattr(flow_storage_obj, 'flow_settings'), 'execution_location'):
        setattr(getattr(flow_storage_obj, 'flow_settings'), 'execution_location', 'auto')
    elif not hasattr(flow_storage_obj.flow_settings, 'is_running'):
        setattr(flow_storage_obj.flow_settings, 'is_running', False)
        setattr(flow_storage_obj.flow_settings, 'is_canceled', False)
    if not hasattr(flow_storage_obj.flow_settings, 'show_detailed_progress'):
        setattr(flow_storage_obj.flow_settings, 'show_detailed_progress', True)
    for _id, node_information in flow_storage_obj.data.items():
        if not hasattr(node_information, 'setting_input'):
            continue
        if node_information.setting_input.__class__.__name__ == 'NodeRead':
            ensure_compatibility_node_read(node_information.setting_input)
        elif node_information.setting_input.__class__.__name__ == 'NodeSelect':
            ensure_compatibility_node_select(node_information.setting_input)
        elif node_information.setting_input.__class__.__name__ == 'NodeOutput':
            ensure_compatibility_node_output(node_information.setting_input)
        elif node_information.setting_input.__class__.__name__ in ('NodeJoin', 'NodeFuzzyMatch'):
            ensure_compatibility_node_joins(node_information.setting_input)
        elif node_information.setting_input.__class__.__name__ == 'NodePolarsCode':
            ensure_compatibility_node_polars(node_information.setting_input)
        ensure_description(node_information.setting_input)
