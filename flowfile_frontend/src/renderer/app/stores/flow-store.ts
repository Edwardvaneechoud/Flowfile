// Flow Store - Manages flow ID and flow-level state
import { defineStore } from "pinia";
import type { VueFlowStore } from "@vue-flow/core";
import type { HistoryState, FlowArtifactData, NodeArtifactSummary } from "../types";
import { FlowApi } from "../api";

const FLOW_ID_STORAGE_KEY = "last_flow_id";

// Default history state
const defaultHistoryState: HistoryState = {
  can_undo: false,
  can_redo: false,
  undo_description: null,
  redo_description: null,
  undo_count: 0,
  redo_count: 0,
};

const defaultArtifactData: FlowArtifactData = {
  nodes: {},
  edges: [],
};

export const useFlowStore = defineStore("flow", {
  state: () => {
    const savedFlowId = sessionStorage.getItem(FLOW_ID_STORAGE_KEY);
    const initialFlowId = savedFlowId ? parseInt(savedFlowId) : -1;

    return {
      flowId: initialFlowId as number,
      vueFlowInstance: null as any | VueFlowStore,
      // History state for undo/redo
      historyState: { ...defaultHistoryState } as HistoryState,
      // Artifact visualization data
      artifactData: { ...defaultArtifactData } as FlowArtifactData,
      showArtifactEdges: true,
    };
  },

  getters: {
    currentFlowId: (state) => state.flowId,
    canUndo: (state) => state.historyState.can_undo,
    canRedo: (state) => state.historyState.can_redo,
    undoDescription: (state) => state.historyState.undo_description,
    redoDescription: (state) => state.historyState.redo_description,
    hasArtifacts: (state) =>
      Object.keys(state.artifactData.nodes).length > 0 || state.artifactData.edges.length > 0,
  },

  actions: {
    setFlowId(flowId: number) {
      this.flowId = flowId;
      try {
        sessionStorage.setItem(FLOW_ID_STORAGE_KEY, flowId.toString());
      } catch (error) {
        console.warn("Failed to store flow ID in session storage:", error);
      }
      // Reset history state when flow changes
      this.historyState = { ...defaultHistoryState };
      this.artifactData = { ...defaultArtifactData };
    },

    setVueFlowInstance(vueFlowInstance: VueFlowStore) {
      this.vueFlowInstance = vueFlowInstance;
    },

    getVueFlowInstance() {
      return this.vueFlowInstance;
    },

    // Update history state from API response
    updateHistoryState(historyState: HistoryState) {
      this.historyState = historyState;
    },

    // Reset history state
    resetHistoryState() {
      this.historyState = { ...defaultHistoryState };
    },

    // Artifact actions
    async fetchArtifacts() {
      if (this.flowId < 0) return;
      try {
        this.artifactData = await FlowApi.getArtifacts(this.flowId);
      } catch {
        // Artifacts are optional; don't break the UI
        this.artifactData = { ...defaultArtifactData };
      }
    },

    getNodeArtifactSummary(nodeId: number): NodeArtifactSummary | null {
      return this.artifactData.nodes[String(nodeId)] ?? null;
    },

    toggleArtifactEdges() {
      this.showArtifactEdges = !this.showArtifactEdges;
    },
  },
});
