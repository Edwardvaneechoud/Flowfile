from pydantic import BaseModel

from flowfile_core.flowfile.flow_node.flow_node import FlowNode

from flowfile_core.flowfile.graph_tree.models import BranchInfo, InputInfo


def calculate_depth(node_id: int, node_info: dict[int, BranchInfo], visited: set = None) -> int:
    """Calculates the depth of each node."""

    if visited is None:
        visited = set()
    if node_id in visited:
        return node_info[node_id].depth
    visited.add(node_id)
    
    max_input_depth = -1
    inputs = node_info[node_id].inputs
    
    for main_id in inputs.main:
        max_input_depth = max(max_input_depth, calculate_depth(main_id, node_info, visited))
    if inputs.left:
        max_input_depth = max(max_input_depth, calculate_depth(inputs.left, node_info, visited))
    if inputs.right:
        max_input_depth = max(max_input_depth, calculate_depth(inputs.right, node_info, visited))
    
    node_info[node_id].depth = max_input_depth + 1
    return node_info[node_id].depth


# Trace paths from each root
def trace_path(node_id: int, node_info: dict[int, BranchInfo], merge_points: dict[int, list[int]],
               current_path: list[int] | None = None):
    """Define the trace of each node path"""
    if current_path is None:
        current_path = []
    
    current_path = current_path + [node_id]
    outputs = node_info[node_id].outputs
    
    if not outputs:
        # End of path
        return [current_path]
    
    # If this node has multiple outputs or connects to a merge point, branch
    all_paths = []
    for output_id in outputs:
        if output_id in merge_points and len(merge_points[output_id]) > 1:
            # This is a merge point, end this path here
            all_paths.append(current_path + [output_id])
        else:
            # Continue the path
            all_paths.extend(trace_path(output_id, node_info, merge_points, current_path))
    return all_paths


def build_node_info(nodes: list[FlowNode]) -> dict[int, BranchInfo]:
    """Builds node information used to construct the graph tree."""

    node_info = {}
    for node in nodes:
        node_id = node.node_id

        # Get node label
        operation = node.node_type.replace("_", " ").title() if node.node_type else "Unknown"
        label = f"{operation} (id={node_id})"
        if hasattr(node, 'setting_input') and hasattr(node.setting_input, 'description'):
            if node.setting_input.description:
                desc = node.setting_input.description
                if len(desc) > 20:  # Truncate long descriptions
                    desc = desc[:17] + "..."
                label = f"{operation} ({node_id}): {desc}"

        # Get inputs and outputs
        inputs = InputInfo(
            main=[n.node_id for n in (node.node_inputs.main_inputs or [])],
            left=node.node_inputs.left_input.node_id if node.node_inputs.left_input else None,
            right=node.node_inputs.right_input.node_id if node.node_inputs.right_input else None
        )
        outputs = [n.node_id for n in node.leads_to_nodes]

        node_info[node_id] = BranchInfo(
            label=label,
            short_label=f"{operation} ({node_id})",
            inputs=inputs,
            outputs=outputs,
            depth=0
        )
    
    return node_info


def group_nodes_by_depth(node_info: dict[int, BranchInfo]) -> tuple[dict[int, list[int]], int]:
    """Groups each node by depth"""
    depth_groups = {}
    max_depth = 0
    for node_id, info in node_info.items():
        depth = info.depth
        max_depth = max(max_depth, depth)
        if depth not in depth_groups:
            depth_groups[depth] = []
        depth_groups[depth].append(node_id)

    return depth_groups, max_depth


def define_node_connections(node_info: dict[int, BranchInfo]) -> dict[int, list[int]]:
    """Defines node connections to merge"""
    merge_points = {}  # target_id -> list of source_ids
    for node_id, info in node_info.items():
        for output_id in info.outputs:
            if output_id not in merge_points:
                merge_points[output_id] = []
            merge_points[output_id].append(node_id)
    
    return merge_points


def build_flow_paths(node_info: dict[int, BranchInfo], flow_starts: list[FlowNode],
                     merge_points: dict[int, list[int]]):
    """Build the flow paths to be drawn"""


    # Find all root nodes (no inputs)
    root_nodes = [nid for nid, info in node_info.items()
                  if not info.inputs.main and not info.inputs.left and not info.inputs.right]
    
    if not root_nodes and flow_starts:
        root_nodes = [n.node_id for n in flow_starts]
    paths = []  # List of paths through the graph

    # Get all paths
    for root_id in root_nodes:
        paths.extend(trace_path(root_id, node_info, merge_points))

    return paths


def group_paths(paths:list, merge_points:dict):
    """Groups each node path."""
    paths_by_merge = {}
    standalone_paths = []

    for path in paths:
        if len(path) > 1 and path[-1] in merge_points and len(merge_points[path[-1]]) > 1:
            merge_id = path[-1]
            if merge_id not in paths_by_merge:
                paths_by_merge[merge_id] = []
            paths_by_merge[merge_id].append(path)
        else:
            standalone_paths.append(path)
    return paths_by_merge, standalone_paths


def draw_merged_paths(node_info: dict[int, BranchInfo],
                      merge_points: dict[int, list[int]],
                      paths_by_merge: dict[int, list[list[int]]],
                      merge_drawn: set,
                      drawn_nodes: set,
                      lines: list[str]):
    """Draws paths for each node that merges."""
    for merge_id, merge_paths in paths_by_merge.items():
        if merge_id in merge_drawn:
            continue
        merge_info = node_info[merge_id]
        sources = merge_points[merge_id]

        # Draw each source path leading to the merge
        for i, source_id in enumerate(sources):
            # Find the path containing this source
            source_path = None
            for path in merge_paths:
                if source_id in path:

                    source_path = path[:path.index(source_id) + 1]
                    break

            if source_path:
                # Build the line for this path
                line_parts = []
                for j, nid in enumerate(source_path):
                    if j == 0:
                        line_parts.append(node_info[nid].label)
                    else:
                        line_parts.append(f" ──> {node_info[nid].short_label}")

                # Add the merge arrow
                if i == 0:
                    # First source
                    line = "".join(line_parts) + " ─────┐"
                    lines.append(line)
                elif i == len(sources) - 1:
                    # Last source
                    line = "".join(line_parts) + " ─────┴──> " + merge_info.label
                    lines.append(line)

                    # Continue with the rest of the path after merge
                    remaining = node_info[merge_id].outputs
                    while remaining:
                        next_id = remaining[0]
                        lines[-1] += f" ──> {node_info[next_id].label}"
                        remaining = node_info[next_id].outputs
                        drawn_nodes.add(next_id)
                else:
                    # Middle sources
                    line = "".join(line_parts) + " ─────┤"
                    lines.append(line)

                for nid in source_path:
                    drawn_nodes.add(nid)

        drawn_nodes.add(merge_id)
        merge_drawn.add(merge_id)
        lines.append("")  # Add spacing between merge groups
    return paths_by_merge


def draw_standalone_paths(drawn_nodes: set[int], standalone_paths: list[list[int]], lines: list[str],
                          node_info: dict[int, BranchInfo]):
    """ Draws paths that do not merge."""
    # Draw standalone paths
    for path in standalone_paths:
        if all(nid in drawn_nodes for nid in path):
            continue
        
        line_parts = []
        for i, node_id in enumerate(path):
            if node_id not in drawn_nodes:
                if i == 0:
                    line_parts.append(node_info[node_id].label)
                else:
                    line_parts.append(f" ──> {node_info[node_id].short_label}")
                drawn_nodes.add(node_id)
        
        if line_parts:
            lines.append("".join(line_parts))


def add_un_drawn_nodes(drawn_nodes: set[int], node_info: dict[int, BranchInfo],  lines: list[str]):
    """Adds isolated nodes if exists."""
    # Add any remaining undrawn nodes

    for node_id in node_info:
        if node_id not in drawn_nodes:
            lines.append(node_info[node_id].label + " (isolated)")

    lines.append("")
    lines.append("=" * 80)
    lines.append("Execution Order")
    lines.append("=" * 80)
