// Flow-as-API service - manage published flow endpoints and their API keys.
import axios from "../services/axios.config";

export type ApiParamType = "string" | "integer" | "float" | "boolean" | "enum";

export interface ApiParamSpec {
  name: string;
  type: ApiParamType;
  required: boolean;
  default: string | null;
  enum_values: string[] | null;
}

export interface ApiEndpoint {
  id: number;
  registration_id: number;
  owner_id: number;
  slug: string;
  enabled: boolean;
  response_node_id: number | null;
  parameters: ApiParamSpec[];
  path: string;
  flow_name: string | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface FlowParamInfo {
  name: string;
  default: string;
}

export interface ApiTestResult {
  data: Record<string, unknown>[] | Record<string, unknown[]>;
  row_count: number;
  orientation: "records" | "columns";
}

export interface ApiEndpointCreate {
  registration_id: number;
  slug: string;
  enabled?: boolean;
  parameters?: ApiParamSpec[];
}

export interface ApiEndpointUpdate {
  slug?: string;
  enabled?: boolean;
  parameters?: ApiParamSpec[];
}

export interface ApiKey {
  id: number;
  endpoint_id: number;
  name: string;
  key_prefix: string;
  enabled: boolean;
  last_used_at: string | null;
  expires_at: string | null;
  created_at: string | null;
}

export interface ApiKeyCreated extends ApiKey {
  api_key: string;
}

export class FlowApiApi {
  static async getEndpointForFlow(registrationId: number): Promise<ApiEndpoint | null> {
    const res = await axios.get<ApiEndpoint[]>("/flow-api/endpoints", {
      params: { registration_id: registrationId },
    });
    return res.data.length > 0 ? res.data[0] : null;
  }

  static async listAllEndpoints(): Promise<ApiEndpoint[]> {
    const res = await axios.get<ApiEndpoint[]>("/flow-api/endpoints");
    return res.data;
  }

  static async getFlowParameters(registrationId: number): Promise<FlowParamInfo[]> {
    const res = await axios.get<FlowParamInfo[]>(`/flow-api/flows/${registrationId}/parameters`);
    return res.data;
  }

  static async testEndpoint(
    endpointId: number,
    params: Record<string, string>,
  ): Promise<ApiTestResult> {
    const res = await axios.post<ApiTestResult>(`/flow-api/endpoints/${endpointId}/test`, {
      params,
    });
    return res.data;
  }

  static async publishEndpoint(body: ApiEndpointCreate): Promise<ApiEndpoint> {
    const res = await axios.post<ApiEndpoint>("/flow-api/endpoints", body);
    return res.data;
  }

  static async updateEndpoint(id: number, body: ApiEndpointUpdate): Promise<ApiEndpoint> {
    const res = await axios.put<ApiEndpoint>(`/flow-api/endpoints/${id}`, body);
    return res.data;
  }

  static async deleteEndpoint(id: number): Promise<void> {
    await axios.delete(`/flow-api/endpoints/${id}`);
  }

  static async listKeys(endpointId: number): Promise<ApiKey[]> {
    const res = await axios.get<ApiKey[]>(`/flow-api/endpoints/${endpointId}/keys`);
    return res.data;
  }

  static async createKey(endpointId: number, name: string): Promise<ApiKeyCreated> {
    const res = await axios.post<ApiKeyCreated>(`/flow-api/endpoints/${endpointId}/keys`, { name });
    return res.data;
  }

  static async deleteKey(endpointId: number, keyId: number): Promise<void> {
    await axios.delete(`/flow-api/endpoints/${endpointId}/keys/${keyId}`);
  }
}
