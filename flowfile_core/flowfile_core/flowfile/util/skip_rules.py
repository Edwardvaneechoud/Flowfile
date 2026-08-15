"""Trigger-rule engine deciding which nodes run, from their immediate inputs.

Replaces the two blind transitive closures (static BFS in ``node_skipper`` and
the runtime ``get_all_dependent_nodes()`` fan-out) with one local rule applied
in topological order. Because stages are topological, every node can be decided
from its immediate inputs alone:

- ALL (default, every existing node): any error-ish input skips the node as an
  error; otherwise any deliberately-skipped input skips it deliberately.
- ANY (union only): the node runs when at least one input survived and no input
  is error-ish. Failure never inherits ANY — a failed branch must not silently
  become "half the data written successfully".

Error-ish covers failures, misconfiguration, and cancellation. Deliberate skips
originate only from closed gates, so graphs without gates behave exactly as
before. ANY is restricted to ``union`` because the other multi-input node types
address their inputs positionally (``input_df_1..N``) — eliding one would
silently renumber the rest.
"""

from collections import deque
from enum import Enum

from flowfile_core.flowfile.flow_node.flow_node import FlowNode

ANY_INPUT_NODE_TYPES: frozenset[str] = frozenset({"union"})
GATE_NODE_TYPE = "gate"


class NodeRunStatus(str, Enum):
    RUN = "run"
    FAILED = "failed"
    SKIPPED_ERROR = "skipped_error"
    SKIPPED_DELIBERATE = "skipped_deliberate"


def is_error_ish(status: NodeRunStatus) -> bool:
    return status in (NodeRunStatus.FAILED, NodeRunStatus.SKIPPED_ERROR)


def is_skipped(status: NodeRunStatus) -> bool:
    return status in (NodeRunStatus.SKIPPED_ERROR, NodeRunStatus.SKIPPED_DELIBERATE)


def uses_any_rule(node: FlowNode) -> bool:
    return node.node_type in ANY_INPUT_NODE_TYPES


def effective_input_status(
    statuses: dict[str | int, "NodeRunStatus"],
    input_node_id: str | int,
    closed_gate_ids: frozenset | set | None = None,
) -> "NodeRunStatus":
    """The status a consumer should see for one of its inputs.

    A closed gate runs successfully (it "ran and decided") but declines its
    downstream: consumers see it as a deliberate skip. The mapping applies
    only when the gate itself is healthy — a gate that error-skipped or
    failed propagates the error regardless of its condition.
    """
    actual = statuses.get(input_node_id, NodeRunStatus.RUN)
    if closed_gate_ids and input_node_id in closed_gate_ids and actual == NodeRunStatus.RUN:
        return NodeRunStatus.SKIPPED_DELIBERATE
    return actual


def classify_from_inputs(node: FlowNode, input_statuses: list[NodeRunStatus]) -> NodeRunStatus:
    """Decide a node's run status locally from its immediate inputs' statuses."""
    if any(is_error_ish(status) for status in input_statuses):
        return NodeRunStatus.SKIPPED_ERROR
    if uses_any_rule(node):
        if any(status == NodeRunStatus.RUN for status in input_statuses):
            return NodeRunStatus.RUN
        if input_statuses:
            return NodeRunStatus.SKIPPED_DELIBERATE
        return NodeRunStatus.RUN
    if any(status == NodeRunStatus.SKIPPED_DELIBERATE for status in input_statuses):
        return NodeRunStatus.SKIPPED_DELIBERATE
    return NodeRunStatus.RUN


def classify_graph(
    nodes: list[FlowNode],
    deliberate_seed_ids: set[str | int] | None = None,
    closed_gate_ids: set[str | int] | None = None,
) -> dict[str | int, NodeRunStatus]:
    """Classify every node of the (unfiltered) graph in topological order.

    Seeds: nodes failing ``is_correct`` are error-ish; ids in
    ``deliberate_seed_ids`` are deliberate. Ids in ``closed_gate_ids`` are
    healthy gates whose condition evaluated false: the gate itself classifies
    normally (it runs), but its consumers read it as a deliberate skip via
    ``effective_input_status``. Runs Kahn over all zero-in-degree nodes —
    never over ``flow_starts`` — so unreachable subgraphs are classified too.

    Cycles do not raise here: cycle members downstream of an error-ish node are
    error-skipped (matching the old BFS closure); other cycle members stay
    unclassified so the stage builder keeps raising on genuine cycles.
    """
    deliberate_seed_ids = deliberate_seed_ids or set()
    closed_gate_ids = closed_gate_ids or set()
    node_by_id: dict[str | int, FlowNode] = {node.node_id: node for node in nodes}
    status: dict[str | int, NodeRunStatus] = {}

    # Predecessors derive from leads_to_nodes — the same edge source the stage
    # topology uses — so classification and ordering can never disagree.
    in_degree: dict[str | int, int] = {node_id: 0 for node_id in node_by_id}
    predecessors: dict[str | int, list[str | int]] = {node_id: [] for node_id in node_by_id}
    for node in nodes:
        for downstream in node.leads_to_nodes:
            if downstream.node_id in in_degree:
                in_degree[downstream.node_id] += 1
                predecessors[downstream.node_id].append(node.node_id)

    queue = deque(node_id for node_id, degree in in_degree.items() if degree == 0)
    processed: set[str | int] = set()
    while queue:
        node_id = queue.popleft()
        if node_id in processed:
            continue
        processed.add(node_id)
        node = node_by_id[node_id]
        if not node.is_correct:
            status[node_id] = NodeRunStatus.SKIPPED_ERROR
        elif node_id in deliberate_seed_ids:
            status[node_id] = NodeRunStatus.SKIPPED_DELIBERATE
        else:
            input_statuses = [
                effective_input_status(status, predecessor_id, closed_gate_ids)
                for predecessor_id in predecessors[node_id]
            ]
            status[node_id] = classify_from_inputs(node, input_statuses)
        for downstream in node.leads_to_nodes:
            if downstream.node_id in in_degree:
                in_degree[downstream.node_id] -= 1
                if in_degree[downstream.node_id] == 0 and downstream.node_id not in processed:
                    queue.append(downstream.node_id)

    if len(processed) != len(node_by_id):
        _propagate_errors_into_cycles(node_by_id, status, processed)
    return status


def _propagate_errors_into_cycles(
    node_by_id: dict[str | int, FlowNode],
    status: dict[str | int, NodeRunStatus],
    processed: set[str | int],
) -> None:
    """Error-skip cycle members reachable from an error-ish node.

    Mirrors the old forward BFS, which traversed into cycles: misconfigured
    cycle members and everything downstream of any error-ish node get error
    status; untouched cycle members remain unclassified for the cycle check.
    """
    seeds: deque[str | int] = deque()
    for node_id, node_status in status.items():
        if node_status == NodeRunStatus.SKIPPED_ERROR:
            seeds.append(node_id)
    for node_id, node in node_by_id.items():
        if node_id not in processed and not node.is_correct:
            status[node_id] = NodeRunStatus.SKIPPED_ERROR
            seeds.append(node_id)
    while seeds:
        node_id = seeds.popleft()
        for downstream in node_by_id[node_id].leads_to_nodes:
            downstream_id = downstream.node_id
            if (
                downstream_id in node_by_id
                and downstream_id not in processed
                and status.get(downstream_id) != NodeRunStatus.SKIPPED_ERROR
            ):
                status[downstream_id] = NodeRunStatus.SKIPPED_ERROR
                seeds.append(downstream_id)
