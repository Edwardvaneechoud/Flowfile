from collections import deque

from flowfile_core.flowfile.flow_node.flow_node import FlowNode


def determine_nodes_to_skip(nodes: list[FlowNode]) -> list[FlowNode]:
    """Finds nodes to skip on the execution step.

    Skips every node whose input connections are invalid, plus the full
    downstream closure of those nodes — a node cannot run if any ancestor
    on its data path is unrunnable.
    """
    skip_nodes = [node for node in nodes if not node.is_correct]
    skipped_ids = {node.node_id for node in skip_nodes}
    queue = deque(skip_nodes)
    while queue:
        node = queue.popleft()
        for downstream in node.leads_to_nodes:
            if downstream.node_id not in skipped_ids:
                skipped_ids.add(downstream.node_id)
                skip_nodes.append(downstream)
                queue.append(downstream)
    return skip_nodes
