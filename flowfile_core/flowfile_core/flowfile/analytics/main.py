from flowfile_core.flowfile.analytics.graphic_walker import (get_initial_gf_data_from_ff,
                                               convert_ff_columns_to_gw_fields)
from flowfile_core.flowfile.node_step.node_step import NodeStep
from flowfile_core.schemas.input_schema import NodeExploreData
from flowfile_core.schemas.analysis_schemas.graphic_walker_schemas import GraphicWalkerInput, DataModel
from flowfile_core.configs import logger


class AnalyticsProcessor:

    @staticmethod
    def depr_create_graphic_walker_input(node_step: NodeStep) -> NodeExploreData:
        node_graphic_walker: NodeExploreData = node_step.setting_input
        if not isinstance(node_graphic_walker, GraphicWalkerInput):
            logger.warning(f"NodeExploreData is not an instance of GraphicWalkerInput. ")
        _needs_run = node_step.needs_run(False)
        if _needs_run:
            raw_fields = convert_ff_columns_to_gw_fields(node_step.get_predicted_schema())
            data_model = DataModel(data=[], fields=raw_fields)
            node_graphic_walker.is_setup = False
        else:
            data_model = get_initial_gf_data_from_ff(node_step.get_resulting_data())
            node_graphic_walker.is_setup = True
        if node_graphic_walker.graphic_walker_input.is_initial:
            node_graphic_walker.graphic_walker_input = GraphicWalkerInput(is_initial=True, data_model=data_model)
        else:
            node_graphic_walker.graphic_walker_input.data_model = data_model
        node_graphic_walker.is_setup = True
        return node_graphic_walker

    @staticmethod
    def process_graphic_walker_input(node_step: NodeStep) -> NodeExploreData:
        node_explore_data: NodeExploreData = node_step.setting_input
        if hasattr(node_explore_data, 'graphic_walker_input'):
            graphic_walker_input = node_explore_data.graphic_walker_input
        else:
            logger.error(f"NodeExploreData is not an instance of GraphicWalkerInput. ")
            raise ValueError(f"NodeExploreData is not an instance of GraphicWalkerInput. ")
        graphic_walker_input = AnalyticsProcessor.create_graphic_walker_input(node_step, graphic_walker_input)
        node_explore_data.is_setup = True
        node_explore_data.graphic_walker_input = graphic_walker_input
        return node_explore_data

    @staticmethod
    def create_graphic_walker_input(node_step: NodeStep,
                                    graphic_walker_input: GraphicWalkerInput = None) -> GraphicWalkerInput:
        if node_step.needs_run(False):
            fields = convert_ff_columns_to_gw_fields(node_step.get_predicted_schema())
            data_model = DataModel(data=[], fields=fields)
        else:
            data_model = get_initial_gf_data_from_ff(node_step.get_resulting_data())
        if graphic_walker_input:
            graphic_walker_input.dataModel = data_model
        else:
            graphic_walker_input = GraphicWalkerInput(dataModel=data_model)
        return graphic_walker_input
