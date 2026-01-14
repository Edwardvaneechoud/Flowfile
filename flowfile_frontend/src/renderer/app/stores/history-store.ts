// History Store - Manages undo/redo state and operations
import { defineStore } from "pinia";
import { FlowApi } from "../api/flow.api";
import type { HistoryState, UndoRedoResult } from "../types";

export const useHistoryStore = defineStore("history", {
  state: () => ({
    canUndo: false,
    canRedo: false,
    undoDescription: null as string | null,
    redoDescription: null as string | null,
    undoCount: 0,
    redoCount: 0,
    isLoading: false,
  }),

  getters: {
    hasUndoHistory: (state) => state.canUndo,
    hasRedoHistory: (state) => state.canRedo,
  },

  actions: {
    /**
     * Refresh the history status from the backend
     */
    async refreshStatus(flowId: number): Promise<void> {
      console.log(`[HistoryStore] refreshStatus called for flowId=${flowId}`);
      if (flowId < 0) {
        console.log("[HistoryStore] refreshStatus skipped - invalid flowId");
        return;
      }

      try {
        const status = await FlowApi.getHistoryStatus(flowId);
        console.log("[HistoryStore] refreshStatus received:", status);
        this.canUndo = status.canUndo;
        this.canRedo = status.canRedo;
        this.undoDescription = status.undoDescription;
        this.redoDescription = status.redoDescription;
        this.undoCount = status.undoCount;
        this.redoCount = status.redoCount;
        console.log(
          `[HistoryStore] State updated: canUndo=${this.canUndo}, canRedo=${this.canRedo}, undoCount=${this.undoCount}`,
        );
      } catch (error) {
        console.error("[HistoryStore] Failed to refresh history status:", error);
      }
    },

    /**
     * Undo the last action
     * @returns The result of the undo operation
     */
    async undo(flowId: number): Promise<UndoRedoResult | null> {
      console.log(
        `[HistoryStore] undo called - flowId=${flowId}, canUndo=${this.canUndo}, isLoading=${this.isLoading}`,
      );
      if (flowId < 0 || !this.canUndo || this.isLoading) {
        console.log("[HistoryStore] undo skipped - preconditions not met");
        return null;
      }

      this.isLoading = true;
      try {
        console.log("[HistoryStore] Calling FlowApi.undo...");
        const result = await FlowApi.undo(flowId);
        console.log("[HistoryStore] FlowApi.undo result:", result);
        // Refresh status after undo
        await this.refreshStatus(flowId);
        return result;
      } catch (error) {
        console.error("[HistoryStore] Undo failed:", error);
        return {
          success: false,
          actionDescription: null,
          errorMessage: error instanceof Error ? error.message : "Unknown error",
        };
      } finally {
        this.isLoading = false;
      }
    },

    /**
     * Redo the last undone action
     * @returns The result of the redo operation
     */
    async redo(flowId: number): Promise<UndoRedoResult | null> {
      console.log(
        `[HistoryStore] redo called - flowId=${flowId}, canRedo=${this.canRedo}, isLoading=${this.isLoading}`,
      );
      if (flowId < 0 || !this.canRedo || this.isLoading) {
        console.log("[HistoryStore] redo skipped - preconditions not met");
        return null;
      }

      this.isLoading = true;
      try {
        console.log("[HistoryStore] Calling FlowApi.redo...");
        const result = await FlowApi.redo(flowId);
        console.log("[HistoryStore] FlowApi.redo result:", result);
        // Refresh status after redo
        await this.refreshStatus(flowId);
        return result;
      } catch (error) {
        console.error("[HistoryStore] Redo failed:", error);
        return {
          success: false,
          actionDescription: null,
          errorMessage: error instanceof Error ? error.message : "Unknown error",
        };
      } finally {
        this.isLoading = false;
      }
    },

    /**
     * Clear all history
     */
    async clearHistory(flowId: number): Promise<void> {
      if (flowId < 0) return;

      try {
        await FlowApi.clearHistory(flowId);
        this.canUndo = false;
        this.canRedo = false;
        this.undoDescription = null;
        this.redoDescription = null;
        this.undoCount = 0;
        this.redoCount = 0;
      } catch (error) {
        console.error("Failed to clear history:", error);
      }
    },

    /**
     * Reset the store state (e.g., when switching flows)
     */
    reset(): void {
      this.canUndo = false;
      this.canRedo = false;
      this.undoDescription = null;
      this.redoDescription = null;
      this.undoCount = 0;
      this.redoCount = 0;
      this.isLoading = false;
    },
  },
});
