// Flow Store - Manages flow ID and flow-level state
import { defineStore } from "pinia";
import type { VueFlowStore } from "@vue-flow/core";
import type { HistoryState, FlowArtifactData, NodeArtifactSummary } from "../types";
import { FlowApi } from "../api";
import { useEditorStore } from "./editor-store";

export const FLOW_ID_STORAGE_KEY = "last_flow_id";

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
      // Monotonic counter bumped by `requestReload()`. Components rendering
      // the canvas (e.g. `Canvas.vue`'s `loadFlow`) watch this and re-fetch
      // the graph on bump. Used when the backend mutates the flow without
      // going through the in-canvas mutation paths — for instance when
      // `useAiDiffStore.accept()` applies a server-staged diff (W46/W41).
      // `graphVersion` is a different signal: it bumps on EVERY mutation
      // (incl. canvas-local ones) and drives dirty-state UI; watching it
      // for re-fetch would loop. `pendingReloadCounter` is "external
      // mutation happened; please re-fetch" only.
      pendingReloadCounter: 0,
      // W71 v2.3 — Monotonic counter bumped by `requestLayoutReset()`.
      // `Canvas.vue` watches this and runs the same routine the
      // "Reset layout graph" button triggers (applyStandardLayout +
      // viewport reset + fitView). Used by the post-agent_live
      // layout-reorganize prompt so the banner's [Reorganize] button
      // can call into the canvas without prop-drilling.
      pendingLayoutResetCounter: 0,
    };
  },

  getters: {
    currentFlowId: (state) => state.flowId,
    canUndo: (state) => state.historyState.can_undo,
    canRedo: (state) => state.historyState.can_redo,
    undoDescription: (state) => state.historyState.undo_description,
    redoDescription: (state) => state.historyState.redo_description,
  },

  actions: {
    setFlowId(flowId: number) {
      if (this.flowId === flowId) return;
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

    // Update history state from API response.
    // Every canvas-level mutation (add/delete/connect/disconnect/undo/redo)
    // routes through here, so this is also the hook point for bumping the
    // dirty-state counter.
    updateHistoryState(historyState: HistoryState) {
      this.historyState = historyState;
      useEditorStore().bumpGraphVersion();
    },

    // Reset history state
    resetHistoryState() {
      this.historyState = { ...defaultHistoryState };
    },

    // Artifact actions. Caller may pass an explicit flowId to pin the request
    // to a specific flow — useful from loadFlow where the active flowId can
    // change mid-fetch. The result is dropped if the store has moved on.
    async fetchArtifacts(flowId?: number) {
      const targetId = flowId ?? this.flowId;
      if (targetId < 0) return;
      try {
        const data = await FlowApi.getArtifacts(targetId);
        if (this.flowId !== targetId) return;
        this.artifactData = data;
      } catch {
        // Artifacts are optional; don't break the UI
        if (this.flowId === targetId) this.artifactData = { ...defaultArtifactData };
      }
    },

    getNodeArtifactSummary(nodeId: number): NodeArtifactSummary | null {
      return this.artifactData.nodes[String(nodeId)] ?? null;
    },

    // Signal "the backend mutated the flow; please re-fetch and re-render".
    // Canvas.vue watches `pendingReloadCounter` and calls its local
    // `loadFlow()` on bump. Safe to call multiple times — the watcher's
    // debouncing (via `loadToken`) cancels stale runs.
    requestReload() {
      this.pendingReloadCounter += 1;
    },

    // W71 v2.3 — Signal "please re-run the standard auto-layout".
    // Canvas.vue watches `pendingLayoutResetCounter` and runs the same
    // routine the "Reset layout graph" button triggers
    // (`handleResetLayoutGraph`: applyStandardLayout + viewport reset
    // + fitView). Used by the post-agent_live layout-reorganize banner
    // so the [Reorganize] button can call into the canvas without
    // prop-drilling. Safe to call multiple times.
    requestLayoutReset() {
      this.pendingLayoutResetCounter += 1;
    },
  },
});
