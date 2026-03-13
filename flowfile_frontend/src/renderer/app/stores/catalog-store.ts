// Catalog Store - Manages catalog tree, flow registrations, favorites, follows, and run history
import { defineStore } from "pinia";
import { CatalogApi } from "../api/catalog.api";
import type {
  CatalogStats,
  CatalogTab,
  CatalogTable,
  CatalogTablePreview,
  FlowRegistration,
  FlowRun,
  FlowRunDetail,
  GlobalArtifact,
  NamespaceTree,
} from "../types";

interface CatalogState {
  tree: NamespaceTree[];
  allFlows: FlowRegistration[];
  favorites: FlowRegistration[];
  following: FlowRegistration[];
  runs: FlowRun[];
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
    activeTab: "catalog",
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
        this.runs = await CatalogApi.getRuns(registrationId);
      } catch (e: any) {
        this.error = e?.message ?? "Failed to load runs";
      }
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
        await Promise.all([this.loadFavorites(), this.loadStats(), this.loadTree()]);
      } catch (e: any) {
        this.error = e?.message ?? "Failed to toggle favorite";
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

    selectFlow(flowId: number | null) {
      this.selectedFlowId = flowId;
      this.selectedRunId = null;
      this.selectedRunDetail = null;
      this.clearTableSelection();
      if (flowId !== null) {
        this.loadRuns(flowId);
        this.loadFlowArtifacts(flowId);
      }
    },

    setActiveTab(tab: CatalogTab) {
      this.activeTab = tab;
      if (tab === "favorites") this.loadFavorites();
      else if (tab === "following") this.loadFollowing();
      else if (tab === "runs") this.loadRuns();
      else if (tab === "catalog") this.loadTree();
    },

    async initialize() {
      await Promise.all([
        this.loadTree(),
        this.loadAllFlows(),
        this.loadAllTables(),
        this.loadStats(),
        this.loadFavorites(),
      ]);
    },
  },
});
