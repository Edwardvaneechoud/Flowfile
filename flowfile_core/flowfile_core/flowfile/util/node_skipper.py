from typing import List
from flowfile_core.flowfile.flow_node.flow_node import FlowNode

def determine_nodes_to_skip(nodes : List[FlowNode]) -> List[FlowNode]:
    """ Finds nodes to skip on the execution step. """
    skip_nodes = [node for node in nodes if not node.is_correct]
    skip_nodes.extend([lead_to_node for node in skip_nodes for lead_to_node in node.leads_to_nodes])
    return skip_nodes