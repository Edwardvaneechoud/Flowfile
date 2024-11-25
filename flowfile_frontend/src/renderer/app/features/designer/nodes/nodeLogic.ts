import { ref, Ref } from 'vue'
import axios from 'axios'
import { NodeData, nodeData as nodeDataRef, TableExample } from '../baseNode/nodeInterfaces'

export interface AxiosResponse {
  data: any // You can replace 'any' with the specific structure if known.
  status: number
  statusText: string
  headers: any // Use a more detailed type if you want.
  config: any // Use a more detailed type if you want.
}

export type ExecutionMode = 'Development' | 'Performance';

export interface FlowSettings {
  flow_id: number
  name: string
  description?: string
  save_location?: string
  auto_save: boolean
  modified_on?: number
  path?: string
  execution_mode: ExecutionMode
}


export interface LocalFileInfo {
  path: string
  file_name: string
  file_type: string
  last_modified_date_timestamp?: number
  exists: boolean
}

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
export async function createFlow(flowPath: string): Promise<Number> {
  console.log('Creating flow', flowPath)
  const response = await axios.post(
    '/editor/create_flow',
    {},
    {
      headers: { accept: 'application/json' },
      params: { flow_path: flowPath },
    },
  )
  if (response.status === 200) {
    return response.data
  }
  throw Error('Error creating flow')
}

export async function getFlowSettings(flow_id: number): Promise<FlowSettings> {
  try {
    const response = await axios.get('/editor/flow', {
      headers: { accept: 'application/json' },
      params: { flow_id: flow_id },
    })
    if (response.status === 200) {
      return response.data
    } else if (response.status === 404) {
      return {} as FlowSettings  // Return an empty FlowSettings object or some default value
    }
    throw Error('Error fetching flow data')
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 404) {
      return {} as FlowSettings  // Handle the 404 case if it reaches here
    }
    throw Error('Error fetching flow data')
  }
}

export async function updateFlowSettings(flowSettings: FlowSettings): Promise<null> {
  console.log(flowSettings)
  const response = await axios.post('/flow_settings/', flowSettings, {
    headers: { accept: 'application/json' },
  })
  if (response.status === 200) {
    return null
  }
  throw Error('Error updating flow settings')
}

export async function getSavedFlows(): Promise<LocalFileInfo[]> {
  const response = await axios.get('/files/available_flow_files', {
    headers: { accept: 'application/json' },
  })
  if (response.status === 200) {
    return response.data
  }
  throw Error('Error fetching flow data')
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
  try {
    const response: AxiosResponse = await axios.post(
      '/editor/delete_node/',
      {},
      {
        params: {
          flow_id,
          node_id,
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
