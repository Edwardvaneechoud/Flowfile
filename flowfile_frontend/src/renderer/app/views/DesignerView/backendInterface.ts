// DEPRECATED: Import from '@/api' or '../../api' instead
// This file is kept for backward compatibility during migration

import { FlowApi } from '../../api/flow.api'
import type { NodeConnection, NodePromise } from '../../types'

// Re-export types
export type { AxiosResponse, NodeInputConnection, NodeOutputConnection, NodeConnection } from '../../types/canvas.types'
export type { FlowSettings } from '../../types/flow.types'

// Legacy function wrappers that delegate to the new API
export const connectNode = async (flowId: number, nodeConnection: NodeConnection) => {
  console.log('Connecting node where it should happen', nodeConnection)
  await FlowApi.connectNode(flowId, nodeConnection)
}

export const deleteConnection = async (flowId: number, nodeConnection: NodeConnection): Promise<any> => {
  return FlowApi.deleteConnection(flowId, nodeConnection)
}

export const closeFlow = async (flow_id: number): Promise<any> => {
  return FlowApi.closeFlow(flow_id)
}

export const deleteNode = async (flow_id: number, node_id: number): Promise<any> => {
  return FlowApi.deleteNode(flow_id, node_id)
}

export const insertNode = async (
  flow_id: number,
  node_id: number,
  node_type: string,
  pos_x: number = 0,
  pos_y: number = 0
): Promise<any> => {
  console.log('inserting a note')
  return FlowApi.insertNode(flow_id, node_id, node_type, pos_x, pos_y)
}

export const copyNode = async (
  nodeIdToCopyFrom: number,
  flowIdToCopyFrom: number,
  nodePromise: NodePromise
): Promise<any> => {
  console.log('copying a note')
  return FlowApi.copyNode(nodeIdToCopyFrom, flowIdToCopyFrom, nodePromise)
}

export const getAllFlows = async () => {
  return FlowApi.getAllFlows()
}

export const getFlowData = async (flowId: number) => {
  return FlowApi.getFlowData(flowId)
}

export const importSavedFlow = async (flowPath: string) => {
  console.log('Importing flow from path:', flowPath)
  try {
    return await FlowApi.importFlow(flowPath)
  } catch (error) {
    console.error('There was an error fetching the flow:', error)
  }
}
