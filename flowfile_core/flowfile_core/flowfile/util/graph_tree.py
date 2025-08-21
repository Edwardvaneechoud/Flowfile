
# Calculate depth for each node
def calculate_depth(node_id, node_info, visited=None):
    if visited is None:
        visited = set()
    if node_id in visited:
        return node_info[node_id]['depth']
    visited.add(node_id)
    
    max_input_depth = -1
    inputs = node_info[node_id]['inputs']
    
    for main_id in inputs['main']:
        max_input_depth = max(max_input_depth, calculate_depth(main_id, node_info, visited))
    if inputs['left']:
        max_input_depth = max(max_input_depth, calculate_depth(inputs['left'], node_info, visited))
    if inputs['right']:
        max_input_depth = max(max_input_depth, calculate_depth(inputs['right'], node_info, visited))
    
    node_info[node_id]['depth'] = max_input_depth + 1
    return node_info[node_id]['depth']


# Trace paths from each root
def trace_path(node_id, node_info, merge_points, current_path=None):
    if current_path is None:
        current_path = []
    
    current_path = current_path + [node_id]
    outputs = node_info[node_id]['outputs']
    
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