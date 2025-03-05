// nodeLogic.ts

import { ref, Ref } from 'vue'
import { NodeData, nodeData as nodeDataRef, TableExample } from '../nodeInterfaces'
import { SelectInput, NodeSelect } from '../nodeInput'
import axios from 'axios'

export const createSelectInput = (column_name: string, data_type: string | undefined = undefined, 
  position: number | undefined = undefined,
    original_position: number | undefined = undefined): SelectInput => {
    const selectInput: SelectInput = {
      old_name: column_name,
      new_name: column_name,
      data_type: data_type,
      keep: true,
      join_key: false,
      is_altered: false,
      data_type_change: false,
      is_available: true,
      position: position ?? 0,
      original_position: original_position ?? 0,
    }
    return selectInput
  }

  export const updateNodeSelect = (nodeTable: TableExample, nodeSelectRef: Ref<NodeSelect | null>): void => {
    if (!nodeTable?.table_schema || !nodeSelectRef.value) return;
    
    // Create a map for fast lookups instead of using find() in a loop
    const existingInputMap = new Map<string, SelectInput>();
    
    // Pre-populate the map with existing select inputs
    nodeSelectRef.value.select_input.forEach(input => {
      existingInputMap.set(input.old_name, input);
    });
    
    // Process all schema items
    nodeTable.table_schema.forEach((schema, index) => {
      const existingInput = existingInputMap.get(schema.name);
      
      if (existingInput) {
        // Update existing input if not altered
        if (!existingInput.is_altered) {
          existingInput.data_type = schema.data_type;
        }
        if (!existingInput.original_position) {
          existingInput.original_position = index;
        }
      } else {
        // Add new input and update the map
        const newInput = createSelectInput(schema.name, schema.data_type, index, index);
        nodeSelectRef.value?.select_input.push(newInput);
        existingInputMap.set(schema.name, newInput);
      }
    });
  }
export const createNodeSelect = (
  flowId = -1,
  nodeId = -1,
  pos_x = 0,
  pos_y = 0,
  cache_input = false,
  keep_missing = false,
): Ref<NodeSelect> => {
  const selectInputData: SelectInput[] = []
  const nodeSelectRef: Ref<NodeSelect> = ref({
    flow_id: flowId,
    node_id: nodeId,
    pos_x: pos_x,
    pos_y: pos_y,
    cache_input: cache_input,
    keep_missing: keep_missing,
    select_input: selectInputData,
    cache_results: false,
    sorted_by: 'none',
  })
  return nodeSelectRef
}

export const createNewSelect = (org_node_select: NodeSelect, node_id: number): NodeSelect => {
  const newNodeSelect = createNodeSelect()
  newNodeSelect.value.select_input = org_node_select.select_input
  newNodeSelect.value.flow_id = org_node_select.flow_id
  newNodeSelect.value.depending_on_id = org_node_select.node_id
  newNodeSelect.value.node_id = node_id
  return newNodeSelect.value
}

export const insertSelect = async (select_input: NodeSelect): Promise<NodeSelect> => {
  const response = await axios.post('/transform/select', select_input, {
    headers: {
      'Content-Type': 'application/json',
    },
  })
  return select_input
}
