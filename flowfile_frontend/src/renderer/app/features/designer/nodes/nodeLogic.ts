// DEPRECATED: Import from '@/api' or '../../../api' instead
// This file is kept for backward compatibility during migration

import { ref, Ref } from 'vue'
import axios from 'axios'
import { FlowApi } from '../../../api/flow.api'
import type { NodeData, RunInformation } from '../../../types/node.types'
import type { FlowSettings, LocalFileInfo } from '../../../types/flow.types'
import { AxiosResponse } from 'axios'

// Re-export types for backward compatibility
export type { ExecutionMode, ExecutionLocation, FlowSettings, LocalFileInfo } from '../../../types/flow.types'

// Legacy function wrappers that delegate to the new API

export const insertNode = async (flow_id: number, node_id: number, node_type: string): Promise<AxiosResponse> => {
  const response = await axios.post(
    'editor/add_node/',
    {},
    {
      params: {
        flow_id: flow_id,
        node_id: node_id,
        node_type: node_type,
      },
      headers: {
        accept: 'application/json',
      },
    },
  )
  return response
}

export async function createFlow(flowPath: string | null = null, name: string | null = null): Promise<number> {
  return FlowApi.createFlow(flowPath, name)
}

export async function getFlowSettings(flow_id: number): Promise<FlowSettings | null> {
  return FlowApi.getFlowSettings(flow_id)
}

export async function updateFlowSettings(flowSettings: FlowSettings): Promise<null> {
  console.log(flowSettings)
  return FlowApi.updateFlowSettings(flowSettings)
}

export async function getSavedFlows(): Promise<LocalFileInfo[]> {
  return FlowApi.getSavedFlows()
}

export async function deleteConnection(flow_id: number, nodeConnection: object): Promise<any> {
  try {
    const response: AxiosResponse = await axios.post(
      '/editor/delete_connection/',
      nodeConnection,
      {
        params: {
          flow_id,
        },
        headers: {
          accept: 'application/json',
        },
      },
    )

    return response.data
  } catch (error) {
    console.error('There was an error:', error)
    throw error
  }
}

export const getNodeData = async (flow_id: number, node_id: number): Promise<Ref<NodeData>> => {
  const response = await axios.get('/node', {
    params: { flow_id: flow_id, node_id: node_id },
    headers: { accept: 'application/json' },
  })
  const nodeData: Ref<NodeData> = ref(response.data)
  return nodeData
}

export const addNodeSettings = async (node_type: string, nodeSettings: any) => {
  const response = await axios.post('update_settings', nodeSettings, { params: { node_type: node_type } })
  console.log(response)
}

export async function deleteNode(flow_id: number, node_id: number): Promise<any> {
  return FlowApi.deleteNode(flow_id, node_id)
}

const isResponseSuccessful = (status: number): boolean =>
  status >= 200 && status < 300

export const getRunStatus = async (flowId: number): Promise<AxiosResponse<RunInformation>> => {
  const response = await axios.get('/flow/run_status/', {
    params: { flow_id: flowId },
    headers: { accept: 'application/json' },
  })
  return response
}

export const updateRunStatus = async (
  flowId: number,
  nodeStore: { insertRunResult: (result: RunInformation) => void }
): Promise<AxiosResponse<RunInformation>> => {
  const response = await getRunStatus(flowId)
  if (isResponseSuccessful(response.status)) {
    nodeStore.insertRunResult(response.data)
  }

  return response
}
