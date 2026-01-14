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
      if (flowId < 0) return;

      try {
        const status = await FlowApi.getHistoryStatus(flowId);
        this.canUndo = status.canUndo;
        this.canRedo = status.canRedo;
        this.undoDescription = status.undoDescription;
        this.redoDescription = status.redoDescription;
        this.undoCount = status.undoCount;
        this.redoCount = status.redoCount;
      } catch (error) {
        console.error("Failed to refresh history status:", error);
      }
    },

    /**
     * Undo the last action
     * @returns The result of the undo operation
     */
    async undo(flowId: number): Promise<UndoRedoResult | null> {
      if (flowId < 0 || !this.canUndo || this.isLoading) {
        return null;
      }

      this.isLoading = true;
      try {
        const result = await FlowApi.undo(flowId);
        // Refresh status after undo
        await this.refreshStatus(flowId);
        return result;
      } catch (error) {
        console.error("Undo failed:", error);
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
      if (flowId < 0 || !this.canRedo || this.isLoading) {
        return null;
      }

      this.isLoading = true;
      try {
        const result = await FlowApi.redo(flowId);
        // Refresh status after redo
        await this.refreshStatus(flowId);
        return result;
      } catch (error) {
        console.error("Redo failed:", error);
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
