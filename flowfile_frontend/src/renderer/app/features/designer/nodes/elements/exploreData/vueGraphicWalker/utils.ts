import axios from 'axios'
import { NodeGraphicWalker } from './interfaces'

export const fetchGraphicWalkerData = async (flowId: number, nodeId: number): Promise<NodeGraphicWalker> => {
  const response = await axios.get<NodeGraphicWalker>('/analysis_data/graphic_walker_input', {
    params: { flow_id: flowId, node_id: nodeId },
    headers: { Accept: 'application/json' },
  })
  return response.data
}
