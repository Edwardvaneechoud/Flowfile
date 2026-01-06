// Results Store - Manages run results, node results, and caching
import { defineStore } from "pinia";
import type {
  RunInformation,
  RunInformationDictionary,
  NodeResult,
  NodeValidation,
  NodeValidationInput,
} from "../types";

export const useResultsStore = defineStore("results", {
  state: () => ({
    runResults: {} as RunInformationDictionary,
    runNodeResults: {} as Record<number, Record<number, NodeResult>>,
    runNodeValidations: {} as Record<number, Record<number, NodeValidation>>,
    currentRunResult: null as RunInformation | null,
    resultVersion: 0,
  }),

  getters: {
    getCurrentRunResult: (state) => state.currentRunResult,
  },

  actions: {
    // ========== Result Cache Management ==========
    initializeResultCache(flowId: number): void {
      if (!this.runNodeResults[flowId]) {
        this.runNodeResults[flowId] = {};
      }
    },

    setNodeResult(flowId: number, nodeId: number, result: NodeResult): void {
      this.initializeResultCache(flowId);
      this.runNodeResults[flowId][nodeId] = result;
    },

    getNodeResult(flowId: number, nodeId: number): NodeResult | undefined {
      return this.runNodeResults[flowId]?.[nodeId];
    },

    resetNodeResult(): void {
      console.log("Clearing node results");
      this.runNodeResults = {};
    },

    clearFlowResults(flowId: number): void {
      if (this.runNodeResults[flowId]) {
        delete this.runNodeResults[flowId];
      }
    },

    // ========== Validation Cache Management ==========
    initializeValidationCache(flowId: number): void {
      if (!this.runNodeValidations[flowId]) {
        this.runNodeValidations[flowId] = {};
      }
    },

    setNodeValidation(
      flowId: number,
      nodeId: number | string,
      nodeValidationInput: NodeValidationInput,
    ): void {
      if (typeof nodeId === "string") {
        nodeId = parseInt(nodeId);
      }
      this.initializeValidationCache(flowId);
      const nodeValidation: NodeValidation = {
        ...nodeValidationInput,
        validationTime: Date.now() / 1000,
      };
      this.runNodeValidations[flowId][nodeId] = nodeValidation;
    },

    resetNodeValidation(): void {
      this.runNodeValidations = {};
    },

    getNodeValidation(flowId: number, nodeId: number): NodeValidation {
      return (
        this.runNodeValidations[flowId]?.[nodeId] || {
          isValid: true,
          error: "",
          validationTime: 0,
        }
      );
    },

    // ========== Run Results Management ==========
    insertRunResult(runResult: RunInformation): void {
      this.currentRunResult = runResult;
      this.runResults[runResult.flow_id] = runResult;
      this.initializeResultCache(runResult.flow_id);

      runResult.node_step_result.forEach((nodeResult) => {
        this.runNodeResults[runResult.flow_id][nodeResult.node_id] = nodeResult;
      });

      this.resultVersion++;
    },

    resetRunResults(): void {
      this.runNodeResults = {};
      this.runResults = {};
      this.currentRunResult = null;
    },

    getRunResult(flowId: number): RunInformation | null {
      return this.runResults[flowId] || null;
    },
  },
});
