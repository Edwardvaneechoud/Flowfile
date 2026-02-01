// Catalog API Service - Handles all catalog-related HTTP requests
import axios from "../services/axios.config";
import type {
  CatalogNamespace,
  CatalogStats,
  FlowRegistration,
  FlowRegistrationCreate,
  FlowRegistrationUpdate,
  FlowRun,
  FlowRunDetail,
  NamespaceCreate,
  NamespaceTree,
  NamespaceUpdate,
} from "../types";

export class CatalogApi {
  // ====== Namespaces ======

  static async getNamespaces(parentId?: number | null): Promise<CatalogNamespace[]> {
    const params: Record<string, any> = {};
    if (parentId !== undefined && parentId !== null) params.parent_id = parentId;
    const response = await axios.get<CatalogNamespace[]>("/catalog/namespaces", { params });
    return response.data;
  }

  static async getNamespaceTree(): Promise<NamespaceTree[]> {
    const response = await axios.get<NamespaceTree[]>("/catalog/namespaces/tree");
    return response.data;
  }

  static async createNamespace(body: NamespaceCreate): Promise<CatalogNamespace> {
    const response = await axios.post<CatalogNamespace>("/catalog/namespaces", body);
    return response.data;
  }

  static async updateNamespace(id: number, body: NamespaceUpdate): Promise<CatalogNamespace> {
    const response = await axios.put<CatalogNamespace>(`/catalog/namespaces/${id}`, body);
    return response.data;
  }

  static async deleteNamespace(id: number): Promise<void> {
    await axios.delete(`/catalog/namespaces/${id}`);
  }

  // ====== Flow Registrations ======

  static async getFlows(namespaceId?: number | null): Promise<FlowRegistration[]> {
    const params: Record<string, any> = {};
    if (namespaceId !== undefined && namespaceId !== null) params.namespace_id = namespaceId;
    const response = await axios.get<FlowRegistration[]>("/catalog/flows", { params });
    return response.data;
  }

  static async getFlow(id: number): Promise<FlowRegistration> {
    const response = await axios.get<FlowRegistration>(`/catalog/flows/${id}`);
    return response.data;
  }

  static async registerFlow(body: FlowRegistrationCreate): Promise<FlowRegistration> {
    const response = await axios.post<FlowRegistration>("/catalog/flows", body);
    return response.data;
  }

  static async updateFlow(id: number, body: FlowRegistrationUpdate): Promise<FlowRegistration> {
    const response = await axios.put<FlowRegistration>(`/catalog/flows/${id}`, body);
    return response.data;
  }

  static async deleteFlow(id: number): Promise<void> {
    await axios.delete(`/catalog/flows/${id}`);
  }

  // ====== Favorites ======

  static async getFavorites(): Promise<FlowRegistration[]> {
    const response = await axios.get<FlowRegistration[]>("/catalog/favorites");
    return response.data;
  }

  static async addFavorite(flowId: number): Promise<void> {
    await axios.post(`/catalog/flows/${flowId}/favorite`);
  }

  static async removeFavorite(flowId: number): Promise<void> {
    await axios.delete(`/catalog/flows/${flowId}/favorite`);
  }

  // ====== Follows ======

  static async getFollowing(): Promise<FlowRegistration[]> {
    const response = await axios.get<FlowRegistration[]>("/catalog/following");
    return response.data;
  }

  static async addFollow(flowId: number): Promise<void> {
    await axios.post(`/catalog/flows/${flowId}/follow`);
  }

  static async removeFollow(flowId: number): Promise<void> {
    await axios.delete(`/catalog/flows/${flowId}/follow`);
  }

  // ====== Runs ======

  static async getRuns(
    registrationId?: number | null,
    limit = 50,
    offset = 0,
  ): Promise<FlowRun[]> {
    const params: Record<string, any> = { limit, offset };
    if (registrationId !== undefined && registrationId !== null)
      params.registration_id = registrationId;
    const response = await axios.get<FlowRun[]>("/catalog/runs", { params });
    return response.data;
  }

  static async getRunDetail(runId: number): Promise<FlowRunDetail> {
    const response = await axios.get<FlowRunDetail>(`/catalog/runs/${runId}`);
    return response.data;
  }

  // ====== Stats ======

  static async getStats(): Promise<CatalogStats> {
    const response = await axios.get<CatalogStats>("/catalog/stats");
    return response.data;
  }
}
