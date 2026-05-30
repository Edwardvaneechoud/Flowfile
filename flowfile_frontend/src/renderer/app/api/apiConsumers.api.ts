// API consumers (reusable API clients / service accounts) - manage consumers,
// their flow grants, and their rotatable API keys.
import axios from "../services/axios.config";
import type { ApiEndpoint, ApiKey, ApiKeyCreated } from "./flowApi.api";

export interface ApiConsumer {
  id: number;
  name: string;
  description: string | null;
  owner_id: number;
  enabled: boolean;
  is_implicit: boolean;
  endpoint_count: number;
  key_count: number;
  created_at: string | null;
  updated_at: string | null;
}

export interface ApiConsumerCreate {
  name: string;
  description?: string | null;
  enabled?: boolean;
}

export interface ApiConsumerUpdate {
  name?: string;
  description?: string | null;
  enabled?: boolean;
}

export class ApiConsumersApi {
  static async listConsumers(): Promise<ApiConsumer[]> {
    const res = await axios.get<ApiConsumer[]>("/api-consumers");
    return res.data;
  }

  static async getConsumer(id: number): Promise<ApiConsumer> {
    const res = await axios.get<ApiConsumer>(`/api-consumers/${id}`);
    return res.data;
  }

  static async createConsumer(body: ApiConsumerCreate): Promise<ApiConsumer> {
    const res = await axios.post<ApiConsumer>("/api-consumers", body);
    return res.data;
  }

  static async updateConsumer(id: number, body: ApiConsumerUpdate): Promise<ApiConsumer> {
    const res = await axios.put<ApiConsumer>(`/api-consumers/${id}`, body);
    return res.data;
  }

  static async deleteConsumer(id: number): Promise<void> {
    await axios.delete(`/api-consumers/${id}`);
  }

  // Grants (which published flows the consumer may call)
  static async listGrantedEndpoints(id: number): Promise<ApiEndpoint[]> {
    const res = await axios.get<ApiEndpoint[]>(`/api-consumers/${id}/endpoints`);
    return res.data;
  }

  static async listAvailableEndpoints(id: number): Promise<ApiEndpoint[]> {
    const res = await axios.get<ApiEndpoint[]>(`/api-consumers/${id}/available-endpoints`);
    return res.data;
  }

  static async grantEndpoint(id: number, endpointId: number): Promise<ApiEndpoint> {
    const res = await axios.post<ApiEndpoint>(`/api-consumers/${id}/endpoints`, {
      endpoint_id: endpointId,
    });
    return res.data;
  }

  static async revokeEndpoint(id: number, endpointId: number): Promise<void> {
    await axios.delete(`/api-consumers/${id}/endpoints/${endpointId}`);
  }

  // Keys
  static async listKeys(id: number): Promise<ApiKey[]> {
    const res = await axios.get<ApiKey[]>(`/api-consumers/${id}/keys`);
    return res.data;
  }

  static async createKey(id: number, name: string): Promise<ApiKeyCreated> {
    const res = await axios.post<ApiKeyCreated>(`/api-consumers/${id}/keys`, { name });
    return res.data;
  }

  static async updateKey(
    id: number,
    keyId: number,
    body: { name?: string; enabled?: boolean },
  ): Promise<ApiKey> {
    const res = await axios.patch<ApiKey>(`/api-consumers/${id}/keys/${keyId}`, body);
    return res.data;
  }

  static async deleteKey(id: number, keyId: number): Promise<void> {
    await axios.delete(`/api-consumers/${id}/keys/${keyId}`);
  }
}
