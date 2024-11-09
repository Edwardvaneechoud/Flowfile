from flowfile_core.schemas.input_schema import NodeExploreData, NodePromise
from flowfile_core.schemas.analysis_schemas.graphic_walker_schemas import GraphicWalkerInput


def create_graphic_walker_node_from_node_promise(node_promise: NodePromise) -> NodeExploreData:
    node_graphic_walker = NodeExploreData.model_validate(node_promise.__dict__)
    node_graphic_walker.graphic_walker_input = GraphicWalkerInput()
    node_graphic_walker.is_setup = False
    return node_graphic_walker
