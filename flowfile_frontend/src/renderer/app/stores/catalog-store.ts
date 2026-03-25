// Catalog Store - Manages catalog tree, flow registrations, favorites, follows, and run history
import { defineStore } from "pinia";
import { CatalogApi } from "../api/catalog.api";
import type {
  ActiveFlowRun,
  CatalogStats,
  CatalogTab,
  CatalogTable,
  CatalogTablePreview,
  FlowRegistration,
  FlowRun,
  FlowRunDetail,
  FlowSchedule,
  GlobalArtifact,
  NamespaceTree,
  SchedulerStatus,
} from "../types";

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
  stats: CatalogStats | null;
  selectedFlowId: number | null;
  selectedRunId: number | null;
  selectedRunDetail: FlowRunDetail | null;
  selectedArtifactId: number | null;
  selectedArtifact: GlobalArtifact | null;
  flowArtifacts: GlobalArtifact[];
  loadingArtifacts: boolean;
  selectedTableId: number | null;
  selectedTable: CatalogTable | null;
  tablePreview: CatalogTablePreview | null;
  loadingTablePreview: boolean;
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
    stats: null,
    selectedFlowId: null,
    selectedRunId: null,
    selectedRunDetail: null,
    selectedArtifactId: null,
    selectedArtifact: null,
    flowArtifacts: [],
    loadingArtifacts: false,
    selectedTableId: null,
    selectedTable: null,
    tablePreview: null,
    loadingTablePreview: false,
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
      const activeIds = new Set(
        state.activeRuns.map((r) => r.registration_id).filter((id) => id !== null),
      );
      return state.schedules.map((s) => ({
        ...s,
        flowName:
          state.allFlows.find((f) => f.id === s.registration_id)?.name ??
          `Flow #${s.registration_id}`,
        isRunning: activeIds.has(s.registration_id),
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
        // Update selected table if it matches
        if (this.selectedTable && this.selectedTable.id === tableId) {
          this.selectedTable.is_favorite = table.is_favorite;
        }
        // Update in allTables
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

    selectArtifact(artifactId: number) {
      this.selectedArtifactId = artifactId;
      this.selectedArtifact =
        this.flowArtifacts.find((a) => a.id === artifactId) ??
        this.findArtifactInTree(artifactId) ??
        null;
    },

    clearArtifactSelection() {
      this.selectedArtifactId = null;
      this.selectedArtifact = null;
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
      this.tablePreview = null;

      if (tableId !== null) {
        this.selectedTable = this.findTableInTree(tableId) ?? null;
        this.loadTablePreview(tableId);
      } else {
        this.selectedTable = null;
      }
    },

    clearTableSelection() {
      this.selectedTableId = null;
      this.selectedTable = null;
      this.tablePreview = null;
    },

    async loadTablePreview(tableId: number, limit = 100) {
      this.loadingTablePreview = true;
      try {
        this.tablePreview = await CatalogApi.getTablePreview(tableId, limit);
      } catch {
        this.tablePreview = null;
      } finally {
        this.loadingTablePreview = false;
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
        const result = await CatalogApi.getRuns(null, this.runsPageSize, offset, scheduleId, runType);
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
  },
});
