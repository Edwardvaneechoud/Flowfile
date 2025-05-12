from typing import Dict, Set, List

from flowfile_core.flowfile.analytics.graphic_walker import (get_initial_gf_data_from_ff,
                                                             convert_ff_columns_to_gw_fields)
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.schemas.input_schema import NodeExploreData
from flowfile_core.schemas.analysis_schemas.graphic_walker_schemas import GraphicWalkerInput, DataModel, MutField, ViewField
from flowfile_core.configs import logger


class AnalyticsProcessor:

    @staticmethod
    def process_graphic_walker_input(node_step: FlowNode) -> NodeExploreData:
        node_explore_data: NodeExploreData = node_step.setting_input
        if hasattr(node_explore_data, 'graphic_walker_input'):
            graphic_walker_input = node_explore_data.graphic_walker_input
        else:
            logger.error("NodeExploreData is not an instance of GraphicWalkerInput.")
            raise ValueError("NodeExploreData is not an instance of GraphicWalkerInput.")
        graphic_walker_input = AnalyticsProcessor.create_graphic_walker_input(node_step, graphic_walker_input)
        node_explore_data.is_setup = True
        node_explore_data.graphic_walker_input = graphic_walker_input
        return node_explore_data

    @staticmethod
    def create_graphic_walker_input(node_step: FlowNode,
                                    graphic_walker_input: GraphicWalkerInput = None) -> GraphicWalkerInput:
        if not node_step.results.analysis_data_generator:
            fields = convert_ff_columns_to_gw_fields(node_step.get_predicted_schema())
            data_model = DataModel(data=[], fields=fields)
        else:
            data_model = get_initial_gf_data_from_ff(node_step.get_resulting_data())
            data_model.data = node_step.results.analysis_data_generator().to_pylist()
        if graphic_walker_input:
            if graphic_walker_input.specList:
                validate_spec_lists_with_data_model(graphic_walker_input.specList, data_model)
            graphic_walker_input.dataModel = data_model
        else:
            graphic_walker_input = GraphicWalkerInput(dataModel=data_model)
        return graphic_walker_input


def check_if_field_in_spec_list_encodings(spec_list: Dict, field_name: str) -> bool:
    for encoding in spec_list['encodings']['dimensions'].values():
        if field_name in encoding['fid']:
            return True
    for encoding in spec_list['encodings']['measures'].values():
        if field_name in encoding['fid']:
            return True
    return False


def get_existing_encoding_fields(spec_list: Dict) -> Set[str]:
    """
    Get the existing encoding fields from the spec_list.

    Args:
        spec_list (Dict): The spec list to check.

    Returns:
        Set[str]: A set of existing encoding fields.
    """
    dimensions = {encoding['fid'] for encoding in spec_list['encodings']['dimensions']}
    measures = {encoding['fid'] for encoding in spec_list['encodings']['measures']}
    return dimensions.union(measures)


def transform_mut_field_to_view_field(mut_field: MutField) -> ViewField:
    view_field = ViewField(**mut_field.model_dump())
    return view_field


def add_field_to_spec_list(spec_list: Dict, mut_field: MutField) -> None:
    """
    Add a field to the spec_list.

    Args:
        spec_list (Dict): The spec list to modify.
        mut_field (MutField): The field to add.

    Returns:
        None
    """
    view_field = transform_mut_field_to_view_field(mut_field)
    if mut_field.analyticType == 'measure':
        spec_list['encodings']['measures'].append(view_field.model_dump_dict())
    else:
        spec_list['encodings']['dimensions'].append(view_field.model_dump_dict())


def validate_spec_list_with_data_model_types(spec_list: Dict, data_model: DataModel) -> None:
    """
    Validate the spec_list with the data model types.

    Args:
        spec_list (Dict): The spec list to validate.
        data_model (DataModel): The data model to validate against.

    Returns:
        bool: True if the spec list is valid, False otherwise.
    """

    # validate dimensions:
    existing_encoding_fields = get_existing_encoding_fields(spec_list)
    for field in data_model.fields:
        if field.fid not in existing_encoding_fields:
            add_field_to_spec_list(spec_list, field)


def validate_spec_lists_with_data_model(spec_lists: List[Dict], data_model: DataModel) -> None:
    """
    Validate the spec lists with the data model.

    Args:
        spec_lists (List[Dict]): The list of spec lists to validate.
        data_model (DataModel): The data model to validate against.

    Returns:
        List[Dict]: The validated spec lists.
    """
    for spec_list in spec_lists:
        validate_spec_list_with_data_model_types(spec_list, data_model)
