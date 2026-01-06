// Flow Store - Manages flow ID and flow-level state
import { defineStore } from "pinia";
import type { VueFlowStore } from "@vue-flow/core";

const FLOW_ID_STORAGE_KEY = "last_flow_id";

export const useFlowStore = defineStore("flow", {
  state: () => {
    const savedFlowId = sessionStorage.getItem(FLOW_ID_STORAGE_KEY);
    const initialFlowId = savedFlowId ? parseInt(savedFlowId) : -1;

    return {
      flowId: initialFlowId as number,
      vueFlowInstance: null as any | VueFlowStore,
    };
  },

  getters: {
    currentFlowId: (state) => state.flowId,
  },

  actions: {
    setFlowId(flowId: number) {
      this.flowId = flowId;
      try {
        sessionStorage.setItem(FLOW_ID_STORAGE_KEY, flowId.toString());
      } catch (error) {
        console.warn("Failed to store flow ID in session storage:", error);
      }
    },

    setVueFlowInstance(vueFlowInstance: VueFlowStore) {
      this.vueFlowInstance = vueFlowInstance;
    },

    getVueFlowInstance() {
      return this.vueFlowInstance;
    },
  },
});
