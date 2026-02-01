// Catalog Store - Manages catalog tree, flow registrations, favorites, follows, and run history
import { defineStore } from "pinia";
import { CatalogApi } from "../api/catalog.api";
import type {
  CatalogStats,
  CatalogTab,
  FlowRegistration,
  FlowRun,
  FlowRunDetail,
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
        await this.loadFavorites();
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
        await this.loadFollowing();
      } catch (e: any) {
        this.error = e?.message ?? "Failed to toggle follow";
      }
    },

    selectFlow(flowId: number | null) {
      this.selectedFlowId = flowId;
      this.selectedRunId = null;
      this.selectedRunDetail = null;
      if (flowId !== null) {
        this.loadRuns(flowId);
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
        this.loadStats(),
        this.loadFavorites(),
      ]);
    },
  },
});
