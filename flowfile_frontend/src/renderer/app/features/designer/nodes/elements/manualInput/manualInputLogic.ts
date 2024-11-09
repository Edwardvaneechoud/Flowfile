import { ref, Ref } from 'vue'
import { NodeManualInput } from '../../../baseNode/nodeInput'

export const createManualInput = (flowId = -1, nodeId = -1, pos_x = 0, pos_y = 0): Ref<NodeManualInput> => {
  const nodeManualInput: Ref<NodeManualInput> = ref({
    flow_id: flowId,
    node_id: nodeId,
    pos_x: pos_x,
    pos_y: pos_y,
    cache_input: false,
    raw_data: [],
    cache_results: false, // Add the missing property 'cache_results'
  })
  return nodeManualInput
}
