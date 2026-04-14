// Catalog API Service - Handles all catalog-related HTTP requests
import axios from "../services/axios.config";
import type {
  ActiveFlowRun,
  CatalogNamespace,
  CatalogStats,
  CatalogTable,
  CatalogTableCreate,
  CatalogTablePreview,
  CatalogTableUpdate,
  DeltaTableHistory,
  FlowRegistration,
  FlowRegistrationCreate,
  FlowRegistrationUpdate,
  FlowRun,
  FlowRunDetail,
  FlowSchedule,
  FlowScheduleCreate,
  FlowScheduleUpdate,
  GlobalArtifact,
  NamespaceCreate,
  NamespaceTree,
  NamespaceUpdate,
  PaginatedFlowRuns,
  QueryVirtualTableCreate,
  SchedulerStatus,
  SqlQueryResult,
  VirtualFlowTableCreate,
  VirtualFlowTableUpdate,
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

  static async runFlow(flowId: number): Promise<FlowRun> {
    const response = await axios.post<FlowRun>(`/catalog/flows/${flowId}/run`);
    return response.data;
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
    scheduleId?: number | null,
    runType?: string | null,
  ): Promise<PaginatedFlowRuns> {
    const params: Record<string, any> = { limit, offset };
    if (registrationId !== undefined && registrationId !== null)
      params.registration_id = registrationId;
    if (scheduleId !== undefined && scheduleId !== null) params.schedule_id = scheduleId;
    if (runType) params.run_type = runType;
    const response = await axios.get<PaginatedFlowRuns>("/catalog/runs", { params });
    return response.data;
  }

  static async getRunDetail(runId: number): Promise<FlowRunDetail> {
    const response = await axios.get<FlowRunDetail>(`/catalog/runs/${runId}`);
    return response.data;
  }

  // ====== Default Namespace ======

  static async getDefaultNamespaceId(): Promise<number | null> {
    const response = await axios.get<number | null>("/catalog/default-namespace-id");
    return response.data;
  }

  // ====== Open Snapshot ======

  static async getRunLog(runId: number): Promise<string> {
    const response = await axios.get<{ log: string }>(`/catalog/runs/${runId}/log`);
    return response.data.log;
  }

  static async openRunSnapshot(runId: number): Promise<number> {
    const response = await axios.post<{ flow_id: number }>(`/catalog/runs/${runId}/open`);
    return response.data.flow_id;
  }

  // ====== Artifacts ======

  /** List active artifacts produced by a specific registered flow. */
  static async getFlowArtifacts(registrationId: number): Promise<GlobalArtifact[]> {
    const response = await axios.get<GlobalArtifact[]>(
      `/catalog/flows/${registrationId}/artifacts`,
    );
    return response.data;
  }

  // ====== Table Favorites ======

  static async getTableFavorites(): Promise<CatalogTable[]> {
    const response = await axios.get<CatalogTable[]>("/catalog/table-favorites");
    return response.data;
  }

  static async addTableFavorite(tableId: number): Promise<void> {
    await axios.post(`/catalog/tables/${tableId}/favorite`);
  }

  static async removeTableFavorite(tableId: number): Promise<void> {
    await axios.delete(`/catalog/tables/${tableId}/favorite`);
  }

  // ====== Catalog Tables ======

  static async getTables(namespaceId?: number | null): Promise<CatalogTable[]> {
    const params: Record<string, any> = {};
    if (namespaceId !== undefined && namespaceId !== null) params.namespace_id = namespaceId;
    const response = await axios.get<CatalogTable[]>("/catalog/tables", { params });
    return response.data;
  }

  static async getTable(tableId: number): Promise<CatalogTable> {
    const response = await axios.get<CatalogTable>(`/catalog/tables/${tableId}`);
    return response.data;
  }

  static async registerTable(body: CatalogTableCreate): Promise<CatalogTable> {
    const response = await axios.post<CatalogTable>("/catalog/tables", body);
    return response.data;
  }

  static async updateTable(id: number, body: CatalogTableUpdate): Promise<CatalogTable> {
    const response = await axios.put<CatalogTable>(`/catalog/tables/${id}`, body);
    return response.data;
  }

  static async deleteTable(id: number): Promise<void> {
    await axios.delete(`/catalog/tables/${id}`);
  }

  static async getTablePreview(
    tableId: number,
    limit = 100,
    version?: number | null,
  ): Promise<CatalogTablePreview> {
    const url = `/catalog/tables/${tableId}/preview`;
    const params: Record<string, any> = { limit };
    if (version !== undefined && version !== null) params.version = version;
    const response = await axios.get<CatalogTablePreview>(url, { params });
    return response.data;
  }

  static async getTableHistory(tableId: number): Promise<DeltaTableHistory> {
    const response = await axios.get<DeltaTableHistory>(`/catalog/tables/${tableId}/history`);
    return response.data;
  }

  // ====== Virtual Flow Tables ======

  static async createVirtualTable(body: VirtualFlowTableCreate): Promise<CatalogTable> {
    const response = await axios.post<CatalogTable>("/catalog/virtual-tables", body);
    return response.data;
  }

  static async updateVirtualTable(
    id: number,
    body: VirtualFlowTableUpdate,
  ): Promise<CatalogTable> {
    const response = await axios.put<CatalogTable>(`/catalog/virtual-tables/${id}`, body);
    return response.data;
  }

  static async resolveVirtualTable(
    tableId: number,
    limit = 100,
  ): Promise<CatalogTablePreview> {
    const response = await axios.post<CatalogTablePreview>(
      `/catalog/virtual-tables/${tableId}/resolve`,
      null,
      { params: { limit } },
    );
    return response.data;
  }

  // ====== Query-based Virtual Tables ======

  static async createQueryVirtualTable(body: QueryVirtualTableCreate): Promise<CatalogTable> {
    const response = await axios.post<CatalogTable>("/catalog/query-virtual-tables", body);
    return response.data;
  }

  // ====== Schedules ======

  static async getSchedule(scheduleId: number): Promise<FlowSchedule> {
    const response = await axios.get<FlowSchedule>(`/catalog/schedules/${scheduleId}`);
    return response.data;
  }

  static async getSchedules(registrationId?: number | null): Promise<FlowSchedule[]> {
    const params: Record<string, any> = {};
    if (registrationId !== undefined && registrationId !== null)
      params.registration_id = registrationId;
    const response = await axios.get<FlowSchedule[]>("/catalog/schedules", { params });
    return response.data;
  }

  static async createSchedule(body: FlowScheduleCreate): Promise<FlowSchedule> {
    const response = await axios.post<FlowSchedule>("/catalog/schedules", body);
    return response.data;
  }

  static async updateSchedule(id: number, body: FlowScheduleUpdate): Promise<FlowSchedule> {
    const response = await axios.put<FlowSchedule>(`/catalog/schedules/${id}`, body);
    return response.data;
  }

  static async deleteSchedule(id: number): Promise<void> {
    await axios.delete(`/catalog/schedules/${id}`);
  }

  static async triggerScheduleNow(scheduleId: number): Promise<FlowRun> {
    const response = await axios.post<FlowRun>(`/catalog/schedules/${scheduleId}/run-now`);
    return response.data;
  }

  // ====== Active Runs ======

  static async getActiveRuns(): Promise<ActiveFlowRun[]> {
    const response = await axios.get<ActiveFlowRun[]>("/catalog/active-runs");
    return response.data;
  }

  static async cancelRun(runId: number): Promise<void> {
    await axios.post(`/catalog/runs/${runId}/cancel`);
  }

  // ====== Scheduler ======

  static async getSchedulerStatus(): Promise<SchedulerStatus> {
    const response = await axios.get<SchedulerStatus>("/catalog/scheduler/status");
    return response.data;
  }

  static async startScheduler(): Promise<void> {
    await axios.post("/catalog/scheduler/start");
  }

  static async stopScheduler(): Promise<void> {
    await axios.post("/catalog/scheduler/stop");
  }

  // ====== Stats ======

  static async getStats(): Promise<CatalogStats> {
    const response = await axios.get<CatalogStats>("/catalog/stats");
    return response.data;
  }

  // ====== SQL Query ======

  static async executeSqlQuery(query: string, maxRows = 10_000): Promise<SqlQueryResult> {
    const response = await axios.post<SqlQueryResult>("/catalog/sql/execute", {
      query,
      max_rows: maxRows,
    });
    return response.data;
  }

  static async saveQueryAsFlow(
    query: string,
    name: string,
    namespaceId?: number,
    description?: string,
    usedTables?: string[],
  ): Promise<{ flow_id: number }> {
    const response = await axios.post<{ flow_id: number }>("/catalog/sql/save-as-flow", {
      query,
      name,
      namespace_id: namespaceId,
      description,
      used_tables: usedTables ?? [],
    });
    return response.data;
  }
}
