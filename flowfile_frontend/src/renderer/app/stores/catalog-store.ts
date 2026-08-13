// Catalog Store - Manages catalog tree, flow registrations, favorites, follows, and run history
import { defineStore } from "pinia";
import type { AxiosRequestConfig } from "axios";
import { CatalogApi } from "../api/catalog.api";
import { ARTIFACT_VERSIONS_PAGE_SIZE } from "../views/CatalogView/artifactVersions";
import type {
  ActiveFlowRun,
  ArtifactPruneResult,
  ArtifactVersionInfo,
  CatalogStats,
  CatalogTab,
  CatalogTable,
  CatalogTablePreview,
  CatalogVisualization,
  DeltaTableHistory,
  FlowRegistration,
  FlowRun,
  FlowRunDetail,
  FlowSchedule,
  GlobalArtifact,
  NamespaceTree,
  SchedulerStatus,
  VisualizationCreatePayload,
  VisualizationUpdatePayload,
  VizSourceDescriptor,
} from "../types";

// Monotonic token guarding the artifact-versions fetch against out-of-order responses.
let artifactVersionsRequestToken = 0;

interface CatalogState {
  tree: NamespaceTree[];
  allFlows: FlowRegistration[];
  favorites: FlowRegistration[];
  following: FlowRegistration[];
  runs: FlowRun[];
  runsTotal: number;
  runsTotalSuccess: number;
  runsTotalFailed: number;
  runsTotalRunning: number;
  runsPage: number;
  runsPageSize: number;
  runsTriggerFilter: string | null;
  runsSearch: string | null;
  stats: CatalogStats | null;
  selectedFlowId: number | null;
  selectedRunId: number | null;
  selectedRunDetail: FlowRunDetail | null;
  selectedArtifactId: number | null;
  selectedArtifact: GlobalArtifact | null;
  flowArtifacts: GlobalArtifact[];
  loadingArtifacts: boolean;
  artifactVersions: ArtifactVersionInfo[];
  artifactVersionsTotal: number;
  artifactVersionsPage: number;
  artifactVersionsRef: string | null;
  artifactVersionsNamespaceId: number | null;
  loadingArtifactVersions: boolean;
  selectedNamespaceId: number | null;
  selectedNamespace: NamespaceTree | null;
  selectedTableId: number | null;
  selectedTable: CatalogTable | null;
  tablePreview: CatalogTablePreview | null;
  loadingTablePreview: boolean;
  previewError: string | null;
  tableHistory: DeltaTableHistory | null;
  loadingTableHistory: boolean;
  tableHistoryStale: boolean;
  selectedVersion: number | null;
  allTables: CatalogTable[];
  schedules: FlowSchedule[];
  flowSchedules: FlowSchedule[];
  selectedScheduleId: number | null;
  selectedSchedule: FlowSchedule | null;
  scheduleRuns: FlowRun[];
  scheduleRunsTotal: number;
  scheduleRunsTotalSuccess: number;
  scheduleRunsTotalFailed: number;
  scheduleRunsTotalRunning: number;
  scheduleRunsPage: number;
  scheduleRunsTriggerFilter: string | null;
  activeRuns: ActiveFlowRun[];
  schedulerStatus: SchedulerStatus | null;
  activeTab: CatalogTab;
  loading: boolean;
  error: string | null;
  visualizationsByTable: Record<number, CatalogVisualization[]>;
  visualizationFieldsBySource: Record<string, Record<string, any>[]>;
  loadingVisualizations: boolean;
  visualizationLibrary: CatalogVisualization[];
  loadingVisualizationLibrary: boolean;
}

// Version history is cached in sessionStorage so a recently-loaded table shows its versions
// automatically on reopen (even after a reload) without re-reading object storage. Entries older than
// HISTORY_STALE_MS are still shown, but flagged as possibly stale (the refresh icon re-fetches).
const HISTORY_CACHE_KEY = "flowfile.catalog.tableHistory";
const HISTORY_STALE_MS = 5 * 60 * 1000;

type HistoryCacheEntry = { ts: number; history: DeltaTableHistory };

function readHistoryCache(): Record<string, HistoryCacheEntry> {
  try {
    const raw = sessionStorage.getItem(HISTORY_CACHE_KEY);
    return raw ? (JSON.parse(raw) as Record<string, HistoryCacheEntry>) : {};
  } catch {
    return {};
  }
}

function readCachedHistory(tableId: number): { history: DeltaTableHistory; stale: boolean } | null {
  const entry = readHistoryCache()[String(tableId)];
  if (!entry) return null;
  return { history: entry.history, stale: Date.now() - entry.ts > HISTORY_STALE_MS };
}

function writeCachedHistory(tableId: number, history: DeltaTableHistory): void {
  try {
    const cache = readHistoryCache();
    cache[String(tableId)] = { ts: Date.now(), history };
    sessionStorage.setItem(HISTORY_CACHE_KEY, JSON.stringify(cache));
  } catch {
    // ignore quota / serialization / unavailable sessionStorage
  }
}

export const useCatalogStore = defineStore("catalog", {
  state: (): CatalogState => ({
    tree: [],
    allFlows: [],
    favorites: [],
    following: [],
    runs: [],
    runsTotal: 0,
    runsTotalSuccess: 0,
    runsTotalFailed: 0,
    runsTotalRunning: 0,
    runsPage: 1,
    runsPageSize: 25,
    runsTriggerFilter: null,
    runsSearch: null,
    stats: null,
    selectedFlowId: null,
    selectedRunId: null,
    selectedRunDetail: null,
    selectedArtifactId: null,
    selectedArtifact: null,
    flowArtifacts: [],
    loadingArtifacts: false,
    artifactVersions: [],
    artifactVersionsTotal: 0,
    artifactVersionsPage: 1,
    artifactVersionsRef: null,
    artifactVersionsNamespaceId: null,
    loadingArtifactVersions: false,
    selectedNamespaceId: null,
    selectedNamespace: null,
    selectedTableId: null,
    selectedTable: null,
    tablePreview: null,
    loadingTablePreview: false,
    previewError: null,
    tableHistory: null,
    loadingTableHistory: false,
    tableHistoryStale: false,
    selectedVersion: null,
    allTables: [],
    schedules: [],
    flowSchedules: [],
    selectedScheduleId: null,
    selectedSchedule: null,
    scheduleRuns: [],
    scheduleRunsTotal: 0,
    scheduleRunsTotalSuccess: 0,
    scheduleRunsTotalFailed: 0,
    scheduleRunsTotalRunning: 0,
    scheduleRunsPage: 1,
    scheduleRunsTriggerFilter: null,
    activeRuns: [],
    schedulerStatus: null,
    activeTab: "runs",
    loading: false,
    error: null,
    visualizationsByTable: {},
    visualizationFieldsBySource: {},
    loadingVisualizations: false,
    visualizationLibrary: [],
    loadingVisualizationLibrary: false,
  }),

  getters: {
    selectedFlow: (state): FlowRegistration | null =>
      state.allFlows.find((f) => f.id === state.selectedFlowId) ?? null,

    flowRuns: (state): FlowRun[] => {
      if (state.selectedFlowId === null) return state.runs;
      return state.runs.filter((r) => r.registration_id === state.selectedFlowId);
    },

    runsTotalPages: (state): number => Math.max(1, Math.ceil(state.runsTotal / state.runsPageSize)),

    getScheduleById:
      (state) =>
      (scheduleId: number): FlowSchedule | undefined =>
        state.schedules.find((s) => s.id === scheduleId),

    scheduleRunsTotalPages: (state): number =>
      Math.max(1, Math.ceil(state.scheduleRunsTotal / state.runsPageSize)),

    enrichedSchedules(state) {
      const activeScheduleIds = new Set(
        state.activeRuns.map((r) => r.schedule_id).filter((id) => id !== null),
      );
      // Registration-scoped: any active run of the flow (including manual/designer runs with no
      // schedule_id) blocks a new Run Now, even though the badge stays schedule-scoped.
      const activeRegistrationIds = new Set(
        state.activeRuns.map((r) => r.registration_id).filter((id) => id !== null),
      );
      return state.schedules.map((s) => ({
        ...s,
        flowName:
          state.allFlows.find((f) => f.id === s.registration_id)?.name ??
          `Flow #${s.registration_id}`,
        isRunning: activeScheduleIds.has(s.id),
        isFlowRunning: activeRegistrationIds.has(s.registration_id),
      }));
    },
  },

  actions: {
    async loadTree() {
      this.loading = true;
      this.error = null;
      try {
        this.tree = await CatalogApi.getNamespaceTree();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load catalog tree";
      } finally {
        this.loading = false;
      }
    },

    async loadAllFlows() {
      try {
        this.allFlows = await CatalogApi.getFlows();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load flows";
      }
    },

    async loadFavorites() {
      try {
        this.favorites = await CatalogApi.getFavorites();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load favorites";
      }
    },

    async loadFollowing() {
      try {
        this.following = await CatalogApi.getFollowing();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load following";
      }
    },

    async loadRuns(registrationId?: number | null) {
      try {
        const offset = (this.runsPage - 1) * this.runsPageSize;
        let scheduleId: number | undefined;
        let runType: string | undefined;
        if (this.runsTriggerFilter) {
          if (this.runsTriggerFilter.startsWith("schedule:")) {
            scheduleId = Number(this.runsTriggerFilter.split(":")[1]);
          } else {
            runType = this.runsTriggerFilter;
          }
        }
        const result = await CatalogApi.getRuns(
          registrationId,
          this.runsPageSize,
          offset,
          scheduleId,
          runType,
          this.runsSearch,
        );
        this.runs = result.items;
        this.runsTotal = result.total;
        this.runsTotalSuccess = result.total_success;
        this.runsTotalFailed = result.total_failed;
        this.runsTotalRunning = result.total_running;
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load runs";
      }
    },

    setRunsPage(page: number, registrationId?: number | null) {
      this.runsPage = page;
      this.loadRuns(registrationId);
    },

    setTriggerFilter(filter: string | null) {
      this.runsTriggerFilter = filter;
      this.runsPage = 1;
      this.loadRuns(this.selectedFlowId);
    },

    setRunsSearch(search: string | null) {
      this.runsSearch = search && search.trim() ? search.trim() : null;
      this.runsPage = 1;
      this.loadRuns(this.selectedFlowId);
    },

    async loadRunDetail(runId: number) {
      try {
        this.selectedRunId = runId;
        this.selectedRunDetail = await CatalogApi.getRunDetail(runId);
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load run detail";
      }
    },

    async loadStats() {
      try {
        this.stats = await CatalogApi.getStats();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load stats";
      }
    },

    async toggleFavorite(flowId: number) {
      const flow = this.allFlows.find((f) => f.id === flowId);
      if (!flow) return;
      try {
        if (flow.is_favorite) {
          await CatalogApi.removeFavorite(flowId);
        } else {
          await CatalogApi.addFavorite(flowId);
        }
        flow.is_favorite = !flow.is_favorite;
        // Update the flag in-place on tree nodes so we don't reset expanded state
        this.updateFavoriteInTree(flowId, flow.is_favorite);
        await Promise.all([this.loadFavorites(), this.loadStats()]);
      } catch (e: any) {
        this.error = e?.message ?? "Failed to toggle favorite";
      }
    },

    /** Update is_favorite on a flow within the tree without replacing the tree. */
    updateFavoriteInTree(flowId: number, isFavorite: boolean) {
      const walk = (nodes: NamespaceTree[]) => {
        for (const node of nodes) {
          for (const f of node.flows) {
            if (f.id === flowId) f.is_favorite = isFavorite;
          }
          walk(node.children);
        }
      };
      walk(this.tree);
    },

    async toggleTableFavorite(tableId: number) {
      const table = this.findTableInTree(tableId);
      if (!table) return;
      try {
        if (table.is_favorite) {
          await CatalogApi.removeTableFavorite(tableId);
        } else {
          await CatalogApi.addTableFavorite(tableId);
        }
        table.is_favorite = !table.is_favorite;
        if (this.selectedTable && this.selectedTable.id === tableId) {
          this.selectedTable.is_favorite = table.is_favorite;
        }
        const allTable = this.allTables.find((t) => t.id === tableId);
        if (allTable) allTable.is_favorite = table.is_favorite;
        await this.loadStats();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to toggle table favorite";
      }
    },

    async toggleFollow(flowId: number) {
      const flow = this.allFlows.find((f) => f.id === flowId);
      if (!flow) return;
      try {
        if (flow.is_following) {
          await CatalogApi.removeFollow(flowId);
        } else {
          await CatalogApi.addFollow(flowId);
        }
        flow.is_following = !flow.is_following;
        await Promise.all([this.loadFollowing(), this.loadTree()]);
      } catch (e: any) {
        this.error = e?.message ?? "Failed to toggle follow";
      }
    },

    async loadFlowArtifacts(registrationId: number) {
      this.loadingArtifacts = true;
      try {
        this.flowArtifacts = await CatalogApi.getFlowArtifacts(registrationId);
      } catch {
        this.flowArtifacts = [];
      } finally {
        this.loadingArtifacts = false;
      }
    },

    async selectArtifact(artifactId: number) {
      this.selectedArtifactId = artifactId;
      const local =
        this.flowArtifacts.find((a) => a.id === artifactId) ??
        this.findArtifactInTree(artifactId) ??
        null;
      this.selectedArtifact = local;
      if (local) return;
      // The tree carries one row per family, so a link minted against an older
      // version's id misses. Resolve that row and land on its family instead.
      try {
        const row = await CatalogApi.getArtifactById(artifactId);
        if (this.selectedArtifactId !== artifactId) return;
        const match = this.findArtifactByName(row.name, row.namespace_id ?? null);
        if (match) {
          this.selectedArtifact = match;
          this.selectedArtifactId = match.id;
        }
      } catch {
        // Id no longer resolves (deleted, or not visible to this user).
      }
    },

    clearArtifactSelection() {
      this.selectedArtifactId = null;
      this.selectedArtifact = null;
      this.clearArtifactVersions();
    },

    // -- Artifact version actions --

    clearArtifactVersions() {
      artifactVersionsRequestToken++;
      this.artifactVersions = [];
      this.artifactVersionsTotal = 0;
      this.artifactVersionsPage = 1;
      this.artifactVersionsRef = null;
      this.artifactVersionsNamespaceId = null;
    },

    /**
     * Load one page of an artifact's version history. `ref` may be namespace-qualified.
     * A superseded request's response is dropped: rows landing under another
     * artifact's panel would point the per-row delete/restore actions at it.
     */
    async loadArtifactVersions(ref: string, namespaceId: number | null, page = 1) {
      const token = ++artifactVersionsRequestToken;
      this.artifactVersionsRef = ref;
      this.artifactVersionsNamespaceId = namespaceId;
      this.artifactVersionsPage = page;
      this.loadingArtifactVersions = true;
      try {
        const result = await CatalogApi.getArtifactVersions(
          ref,
          namespaceId,
          ARTIFACT_VERSIONS_PAGE_SIZE,
          (page - 1) * ARTIFACT_VERSIONS_PAGE_SIZE,
        );
        if (token !== artifactVersionsRequestToken) return;
        this.artifactVersions = result.all_versions;
        this.artifactVersionsTotal = result.total_versions;
      } catch (e: any) {
        if (token !== artifactVersionsRequestToken) return;
        this.artifactVersions = [];
        this.artifactVersionsTotal = 0;
        this.error = e?.response?.data?.detail ?? e?.message ?? "Failed to load model versions";
      } finally {
        if (token === artifactVersionsRequestToken) this.loadingArtifactVersions = false;
      }
    },

    /** Re-fetch the current (or a given) page of the loaded artifact's versions. */
    async reloadArtifactVersions(page?: number) {
      if (this.artifactVersionsRef === null) return;
      await this.loadArtifactVersions(
        this.artifactVersionsRef,
        this.artifactVersionsNamespaceId,
        page ?? this.artifactVersionsPage,
      );
    },

    setArtifactVersionsPage(page: number) {
      this.reloadArtifactVersions(page);
    },

    async deleteArtifactVersion(artifactId: number, force = false) {
      await CatalogApi.deleteArtifactVersion(artifactId, force);
    },

    async promoteArtifactVersion(artifactId: number) {
      return CatalogApi.promoteArtifactVersion(artifactId);
    },

    async pruneArtifactVersions(keep: number, dryRun: boolean): Promise<ArtifactPruneResult> {
      if (this.artifactVersionsRef === null) throw new Error("No artifact selected");
      return CatalogApi.pruneArtifactVersions(
        this.artifactVersionsRef,
        this.artifactVersionsNamespaceId,
        keep,
        dryRun,
      );
    },

    /**
     * Re-resolve the selected artifact after a version mutation: promote mints a
     * new latest row and a forced latest-version delete retires one, so the id
     * the panel was opened with can stop being the family's latest row.
     */
    async refreshSelectedArtifact() {
      const current = this.selectedArtifact;
      if (!current) return;
      await this.loadTree();
      const match = this.findArtifactByName(current.name, current.namespace_id);
      this.selectedArtifact = match;
      this.selectedArtifactId = match?.id ?? null;
    },

    /** Walk the namespace tree for the latest row of an artifact family. */
    findArtifactByName(name: string, namespaceId: number | null): GlobalArtifact | null {
      const walk = (nodes: NamespaceTree[]): GlobalArtifact | null => {
        for (const node of nodes) {
          for (const a of node.artifacts ?? []) {
            if (a.name === name && a.namespace_id === namespaceId) return a;
          }
          const nested = walk(node.children ?? []);
          if (nested) return nested;
        }
        return null;
      };
      return walk(this.tree);
    },

    // -- Catalog Table actions --

    async loadAllTables() {
      try {
        this.allTables = await CatalogApi.getTables();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load tables";
      }
    },

    selectTable(tableId: number | null) {
      this.selectedTableId = tableId;
      this.selectedFlowId = null;
      this.selectedRunId = null;
      this.selectedRunDetail = null;
      this.selectedArtifactId = null;
      this.selectedArtifact = null;
      this.clearNamespaceSelection();
      this.tablePreview = null;
      this.previewError = null;
      // Show recently-loaded version history automatically from the sessionStorage cache (flagged
      // stale after 5 min). Only the data preview stays gated for object-storage tables.
      const cachedHistory = tableId !== null ? readCachedHistory(tableId) : null;
      this.tableHistory = cachedHistory?.history ?? null;
      this.tableHistoryStale = cachedHistory?.stale ?? false;
      this.selectedVersion = null;

      if (tableId !== null) {
        this.selectedTable = this.findTableInTree(tableId) ?? null;
        const isPhysical = this.selectedTable?.table_type !== "virtual";
        const isRemote = !!this.selectedTable?.is_remote_storage;
        // Data preview: local auto-loads; object storage loads on demand (a button).
        if (isPhysical && !isRemote) {
          this.loadTablePreview(tableId);
        }
        // Version history auto-loads only when nothing is cached for this table.
        if (isPhysical && !this.tableHistory) {
          this.loadTableHistory(tableId);
        }
      } else {
        this.selectedTable = null;
      }
    },

    clearTableSelection() {
      this.selectedTableId = null;
      this.selectedTable = null;
      this.tablePreview = null;
      this.previewError = null;
      this.tableHistory = null;
      this.tableHistoryStale = false;
      this.selectedVersion = null;
    },

    async loadTablePreview(tableId: number, limit = 100) {
      this.loadingTablePreview = true;
      this.previewError = null;
      try {
        this.tablePreview = await CatalogApi.getTablePreview(tableId, limit, this.selectedVersion);
      } catch (e: any) {
        this.tablePreview = null;
        this.previewError = e?.response?.data?.detail ?? "Failed to load preview.";
      } finally {
        this.loadingTablePreview = false;
      }
    },

    async loadTableHistory(tableId: number) {
      this.loadingTableHistory = true;
      try {
        this.tableHistory = await CatalogApi.getTableHistory(tableId);
        this.tableHistoryStale = false;
        if (this.tableHistory) {
          writeCachedHistory(tableId, this.tableHistory);
        }
      } catch {
        this.tableHistory = null;
      } finally {
        this.loadingTableHistory = false;
      }
    },

    /** On-demand preview load (preview only; version history auto-loads + caches separately). */
    async loadSelectedPreview() {
      if (this.selectedTableId !== null) {
        await this.loadTablePreview(this.selectedTableId);
      }
    },

    /** Re-fetch version history for the selected table (refresh icon), updating the cache. */
    async refreshTableHistory() {
      if (this.selectedTableId !== null) {
        await this.loadTableHistory(this.selectedTableId);
      }
    },

    /** Refresh state after the edit surface wrote to a table (data, schema and version moved). */
    async handleTableEdited(table: CatalogTable) {
      if (this.selectedTableId === table.id) {
        this.selectedTable = table;
        this.selectedVersion = null;
        if (!table.is_remote_storage) {
          this.loadTablePreview(table.id);
        }
        await this.loadTableHistory(table.id);
      }
      await this.loadAllTables();
    },

    async optimizeTable(tableId: number, zOrderColumns?: string[] | null) {
      const result = await CatalogApi.optimizeTable(tableId, zOrderColumns);
      if (this.selectedTable && this.selectedTable.id === tableId) {
        this.selectedTable.size_bytes = result.size_bytes;
      }
      await this.loadTableHistory(tableId);
      await this.loadAllTables();
      return result;
    },

    async vacuumTable(tableId: number, retentionHours: number, dryRun: boolean) {
      const result = await CatalogApi.vacuumTable(tableId, retentionHours, dryRun);
      if (!dryRun) {
        if (this.selectedTable && this.selectedTable.id === tableId) {
          this.selectedTable.size_bytes = result.size_bytes;
        }
        await this.loadTableHistory(tableId);
        await this.loadAllTables();
      }
      return result;
    },

    selectVersion(version: number | null) {
      // Viewing the current version is the same as viewing "latest". The plain latest
      // read stays valid after old versions are vacuumed, whereas a version-pinned read
      // can reference files that vacuum removed — so route current → latest (null).
      const current = this.tableHistory?.current_version ?? null;
      this.selectedVersion = version !== null && version === current ? null : version;
      if (this.selectedTableId === null) return;
      // Mirror selectTable: object-storage previews load on demand (the "Load preview"
      // button), never eagerly on a version click. Local tables still auto-load.
      if (this.selectedTable?.is_remote_storage) {
        this.tablePreview = null;
        this.previewError = null;
      } else {
        this.loadTablePreview(this.selectedTableId);
      }
    },

    /** Walk the namespace tree to find a table by ID. */
    findTableInTree(tableId: number): CatalogTable | null {
      for (const cat of this.tree) {
        for (const t of cat.tables ?? []) {
          if (t.id === tableId) return t;
        }
        for (const schema of cat.children) {
          for (const t of schema.tables ?? []) {
            if (t.id === tableId) return t;
          }
        }
      }
      return null;
    },

    /** Walk the namespace tree to find an artifact by ID. */
    findArtifactInTree(artifactId: number): GlobalArtifact | null {
      for (const cat of this.tree) {
        for (const a of cat.artifacts) {
          if (a.id === artifactId) return a;
        }
        for (const schema of cat.children) {
          for (const a of schema.artifacts) {
            if (a.id === artifactId) return a;
          }
        }
      }
      return null;
    },

    getNamespaceName(namespaceId: number): string | null {
      for (const cat of this.tree) {
        if (cat.id === namespaceId) return cat.name;
        for (const schema of cat.children) {
          if (schema.id === namespaceId) return schema.name;
        }
      }
      return null;
    },

    // -- Namespace (catalog) detail actions --

    findNamespaceInTree(namespaceId: number): NamespaceTree | null {
      return this.tree.find((c) => c.id === namespaceId) ?? null;
    },

    selectNamespace(namespaceId: number) {
      this.selectedNamespaceId = namespaceId;
      this.selectedNamespace = this.findNamespaceInTree(namespaceId);
      this.selectedFlowId = null;
      this.selectedRunId = null;
      this.selectedRunDetail = null;
      this.clearTableSelection();
      this.clearArtifactSelection();
      this.clearScheduleSelection();
    },

    clearNamespaceSelection() {
      this.selectedNamespaceId = null;
      this.selectedNamespace = null;
    },

    // -- Schedule actions --

    async loadSchedules() {
      try {
        this.schedules = await CatalogApi.getSchedules();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load schedules";
      }
    },

    async loadFlowSchedules(registrationId: number) {
      try {
        this.flowSchedules = await CatalogApi.getSchedules(registrationId);
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load flow schedules";
      }
    },

    // -- Schedule detail actions --

    async selectSchedule(scheduleId: number) {
      this.selectedScheduleId = scheduleId;
      this.selectedFlowId = null;
      this.selectedRunId = null;
      this.selectedRunDetail = null;
      this.clearTableSelection();
      this.clearArtifactSelection();
      this.clearNamespaceSelection();
      this.scheduleRunsPage = 1;
      await Promise.all([this.loadScheduleDetail(scheduleId), this.loadScheduleRuns(scheduleId)]);
    },

    async loadScheduleDetail(scheduleId: number) {
      try {
        this.selectedSchedule = await CatalogApi.getSchedule(scheduleId);
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load schedule detail";
        this.selectedSchedule = null;
      }
    },

    async loadScheduleRuns(scheduleId: number) {
      try {
        const offset = (this.scheduleRunsPage - 1) * this.runsPageSize;
        const runType = this.scheduleRunsTriggerFilter ?? undefined;
        const result = await CatalogApi.getRuns(
          null,
          this.runsPageSize,
          offset,
          scheduleId,
          runType,
        );
        this.scheduleRuns = result.items;
        this.scheduleRunsTotal = result.total;
        this.scheduleRunsTotalSuccess = result.total_success;
        this.scheduleRunsTotalFailed = result.total_failed;
        this.scheduleRunsTotalRunning = result.total_running;
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load schedule runs";
      }
    },

    setScheduleRunsPage(page: number, scheduleId: number) {
      this.scheduleRunsPage = page;
      this.loadScheduleRuns(scheduleId);
    },

    setScheduleTriggerFilter(filter: string | null, scheduleId: number) {
      this.scheduleRunsTriggerFilter = filter;
      this.scheduleRunsPage = 1;
      this.loadScheduleRuns(scheduleId);
    },

    clearScheduleSelection() {
      this.selectedScheduleId = null;
      this.selectedSchedule = null;
      this.scheduleRuns = [];
      this.scheduleRunsTotal = 0;
      this.scheduleRunsTotalSuccess = 0;
      this.scheduleRunsTotalFailed = 0;
      this.scheduleRunsTotalRunning = 0;
      this.scheduleRunsPage = 1;
      this.scheduleRunsTriggerFilter = null;
    },

    // -- Scheduler actions --

    async loadSchedulerStatus() {
      try {
        this.schedulerStatus = await CatalogApi.getSchedulerStatus();
      } catch {
        // Non-critical — leave current state
      }
    },

    async startScheduler() {
      try {
        await CatalogApi.startScheduler();
        await this.loadSchedulerStatus();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to start scheduler";
      }
    },

    async stopScheduler() {
      try {
        await CatalogApi.stopScheduler();
        await this.loadSchedulerStatus();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to stop scheduler";
      }
    },

    // -- Active runs actions --

    async loadActiveRuns() {
      try {
        this.activeRuns = await CatalogApi.getActiveRuns();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load active runs";
      }
    },

    async cancelRun(runId: number) {
      try {
        await CatalogApi.cancelRun(runId);
        await this.loadActiveRuns();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to cancel run";
      }
    },

    selectFlow(flowId: number | null) {
      this.selectedFlowId = flowId;
      this.selectedRunId = null;
      this.selectedRunDetail = null;
      this.clearTableSelection();
      this.clearScheduleSelection();
      this.clearNamespaceSelection();
      if (flowId !== null) {
        this.runsPage = 1;
        this.loadRuns(flowId);
        this.loadFlowArtifacts(flowId);
        this.loadFlowSchedules(flowId);
      }
    },

    setActiveTab(tab: CatalogTab) {
      this.activeTab = tab;
      this.selectedFlowId = null;
      this.selectedRunId = null;
      this.selectedRunDetail = null;
      this.selectedArtifactId = null;
      this.selectedArtifact = null;
      this.clearTableSelection();
      this.clearScheduleSelection();
      this.clearNamespaceSelection();
      if (tab === "favorites") this.loadFavorites();
      else if (tab === "following") this.loadFollowing();
      else if (tab === "runs") this.loadRuns();
      else if (tab === "schedules") this.loadSchedules();
      else if (tab === "catalog") this.loadTree();
    },

    async initialize() {
      await Promise.all([
        this.loadTree(),
        this.loadAllFlows(),
        this.loadAllTables(),
        this.loadStats(),
        this.loadFavorites(),
        this.loadRuns(),
        this.loadSchedules(),
        this.loadActiveRuns(),
        this.loadSchedulerStatus(),
      ]);
    },

    // ============== Visualizations ==============

    async loadVisualizations(tableId: number) {
      this.loadingVisualizations = true;
      try {
        const items = await CatalogApi.listVisualizationsForTable(tableId);
        this.visualizationsByTable = { ...this.visualizationsByTable, [tableId]: items };
      } finally {
        this.loadingVisualizations = false;
      }
    },

    async createVisualization(payload: VisualizationCreatePayload) {
      const created = await CatalogApi.createVisualization(payload);
      if (created.catalog_table_id !== null) {
        const current = this.visualizationsByTable[created.catalog_table_id] ?? [];
        this.visualizationsByTable = {
          ...this.visualizationsByTable,
          [created.catalog_table_id]: [created, ...current],
        };
      }
      // Refresh library so the catalog tab reflects the new entry.
      this.loadVisualizationLibrary().catch(() => undefined);
      return created;
    },

    async updateVisualization(
      vizId: number,
      payload: VisualizationUpdatePayload,
      opts?: { refreshLibrary?: boolean; config?: AxiosRequestConfig },
    ) {
      const updated = await CatalogApi.updateVisualization(vizId, payload, opts?.config);
      // Preserve the access stamp when the response lacks one (e.g. electron mode).
      const merge = (prev: CatalogVisualization): CatalogVisualization => ({
        ...updated,
        access: updated.access ?? prev.access,
      });
      if (updated.catalog_table_id !== null) {
        const current = this.visualizationsByTable[updated.catalog_table_id] ?? [];
        this.visualizationsByTable = {
          ...this.visualizationsByTable,
          [updated.catalog_table_id]: current.map((v) => (v.id === vizId ? merge(v) : v)),
        };
      }
      if (opts?.refreshLibrary === false) {
        // Autosave ticks patch the library in place instead of re-fetching it.
        this.visualizationLibrary = this.visualizationLibrary.map((v) =>
          v.id === vizId ? merge(v) : v,
        );
      } else {
        this.loadVisualizationLibrary().catch(() => undefined);
      }
      return updated;
    },

    async deleteVisualization(vizId: number) {
      await CatalogApi.deleteVisualization(vizId);
      // Drop from any per-table cache that might be holding it.
      const next = { ...this.visualizationsByTable };
      for (const tid of Object.keys(next)) {
        next[Number(tid)] = next[Number(tid)].filter((v) => v.id !== vizId);
      }
      this.visualizationsByTable = next;
      this.visualizationLibrary = this.visualizationLibrary.filter((v) => v.id !== vizId);
    },

    async loadVisualizationFields(source: VizSourceDescriptor) {
      const key = JSON.stringify(source);
      if (this.visualizationFieldsBySource[key]) return this.visualizationFieldsBySource[key];
      const result = await CatalogApi.getVisualizationFields(source);
      if (!result.error) {
        this.visualizationFieldsBySource = {
          ...this.visualizationFieldsBySource,
          [key]: result.fields,
        };
      }
      return result.fields;
    },

    /** Clear-all: SQL-source descriptor keys make per-table invalidation impossible. */
    invalidateVisualizationFields() {
      this.visualizationFieldsBySource = {};
    },

    async loadVisualizationLibrary() {
      this.loadingVisualizationLibrary = true;
      try {
        this.visualizationLibrary = await CatalogApi.listVisualizationLibrary();
      } finally {
        this.loadingVisualizationLibrary = false;
      }
    },
  },
});
