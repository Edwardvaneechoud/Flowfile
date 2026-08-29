from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from flowfile_core.flowfile.util.skip_rules import NodeRunStatus, classify_graph


def determine_nodes_to_skip(nodes: list[FlowNode]) -> list[FlowNode]:
    """Finds nodes that are statically unrunnable on the execution step.

    A node is skipped when its own input connections are invalid, or when the
    trigger rules propagate an error-ish status to it from an unrunnable
    ancestor (see ``skip_rules.classify_from_inputs``). For graphs without
    gates this is exactly the old behavior: every misconfigured node plus its
    full downstream closure. Deliberate (gate-driven) skips are not part of
    this set — they are decided by the same rule engine but reported
    separately, so a gated branch is never treated as unrunnable.
    """
    status = classify_graph(nodes)
    return [node for node in nodes if status.get(node.node_id) == NodeRunStatus.SKIPPED_ERROR]
