import axios, { AxiosError } from 'axios'
import {VueFlowInput, EdgeInput, NodeInput} from '../../types'
import { FlowSettings } from '../../nodes/nodeLogic'

export interface AxiosResponse {
    data: any // You can replace 'any' with the specific structure if known.
    status: number
    statusText: string
    headers: any // Use a more detailed type if you want.
    config: any // Use a more detailed type if you want.
  }  

export interface NodeInputConnection {
    node_id: number;
    connection_class: 'input-0'|'input-1'|'input-2'|'input-3'|'input-4'|'input-5'|'input-6'|'input-7'|'input-8'|'input-9';
}

export interface NodeOutputConnection {
  node_id: number;
  connection_class: 'output-0'|'output-1'|'output-2'|'output-3'|'output-4'|'output-5'|'output-6'|'output-7'|'output-8'|'output-9';
}

export interface NodeConnection {
    input_connection: NodeInputConnection;
    output_connection: NodeOutputConnection;
}

export const connectNode = async(flowId: number, nodeConnection: NodeConnection) => {
  console.log('Connecting node where it should happen', nodeConnection)
  try {
    const response = await axios.post('/editor/connect_node/', nodeConnection, {
      headers: {
        'Content-Type': 'application/json',
        accept: 'application/json',
      },
      params: {
        flow_id: flowId
      },
    })
  }catch (error) {
    console.error('There was an error:', error)
    throw error
  }
}



  export async function deleteConnection(flowId: number, nodeConnection: NodeConnection): Promise<any> {
    try {
      const response: AxiosResponse = await axios.post(
        '/editor/delete_connection/',
        nodeConnection,
        {
          params: {
            flow_id: flowId,
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


  export async function closeFlow(flow_id: number): Promise<any> {
    try {
      const response: AxiosResponse = await axios.post(
        '/editor/close_flow/',
        {},
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
      console.error('Error closing flow:', error)
      throw error
    }
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


export const insertNode = async (flow_id: number, node_id: number, node_type: string, pos_x: number = 0, pos_y: number = 0): Promise<AxiosResponse> => {
    const response = await axios.post(
      'editor/add_node/',
      {},
      {
        params: {
          flow_id: flow_id,
          node_id: node_id,
          node_type: node_type,
          pos_x: pos_x,
          pos_y: pos_y,
        },
        headers: {
          accept: 'application/json',
        },
      },
    )
    return response
  }

  export const getAllFlows = async (): Promise<FlowSettings[]> => {
    try {
      const response = await axios.get<FlowSettings[]>("/active_flowfile_sessions/", {
        headers: {
          accept: "application/json",
        },
      });
      
      return response.data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError<{ message?: string }>; // Add type for response data
        throw new Error(
          `Failed to fetch flows: ${axiosError.response?.data?.message || axiosError.message}`
        );
      }
      
      // Handle non-Axios errors
      throw new Error('Failed to fetch flows: Unknown error occurred');
    }
  };
  

export const getFlowData = async (flowId: number): Promise<VueFlowInput> => {
    const response = await axios.get("/flow_data/v2", {
      params: { flow_id: flowId },
      headers: { accept: "application/json" },
    });
    return response.data;
  };
  


export const importSavedFlow = async (flowPath: string) => {
  console.log('Importing flow from path:', flowPath)
    try {
      const response = await axios.get("/import_flow/", {
        params: { flow_path: flowPath },
        headers: { accept: "application/json" },
      });
      return response.data;
    } catch (error) {
      console.error("There was an error fetching the flow:", error);
    }
  }