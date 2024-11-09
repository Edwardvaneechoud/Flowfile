// nodeLogic.ts

import { ref, Ref } from 'vue'
import { NodeData, nodeData as nodeDataRef, TableExample } from '../nodeInterfaces'
import { SelectInput, NodeSelect } from '../nodeInput'
import axios from 'axios'

export const createSelectInput = (column_name: string, data_type: string | undefined = undefined): SelectInput => {
  const selectInput: SelectInput = {
    old_name: column_name,
    new_name: column_name,
    data_type: data_type,
    keep: true,
    join_key: false,
    is_altered: false,
    data_type_change: false,
    is_available: true,
    position: 0,
  }
  return selectInput
}

export const updateNodeSelect = (nodeTable: TableExample, nodeSelectRef: Ref<NodeSelect | null>): void => {
  if (nodeTable?.table_schema) {
    for (const schema of nodeTable.table_schema) {
      // Check if schema.name is not already in select_input
      const existingInput = nodeSelectRef.value?.select_input.find(
        (selectInput) => selectInput.old_name === schema.name,
      )

      if (existingInput) {
        // If function is not altered, change the data type
        if (!existingInput.is_altered) {
          existingInput.data_type = schema.data_type
        }
      } else {
        // If schema.name is not in select_input, add it
        nodeSelectRef.value?.select_input.push(createSelectInput(schema.name, schema.data_type))
      }
    }
    console.log(nodeDataRef.value)
  }
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
  const response = await axios.post('http://localhost:5667/transform/select', select_input, {
    headers: {
      'Content-Type': 'application/json',
    },
  })
  return select_input
}
