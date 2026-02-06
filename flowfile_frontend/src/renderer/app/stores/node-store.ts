// Node Store - Manages node data, validation, and node-level state
// IMPORTANT: This store includes backward compatibility proxies to other stores
import { defineStore } from "pinia";
import type { Node } from "@vue-flow/core";
import type {
  NodeData,
  TableExample,
  NodeDescriptionDictionary,
  NodeDescriptionEntry,
  NodeReferenceDictionary,
  ExpressionsOverview,
} from "../types";
import { NodeApi, ExpressionsApi } from "../api";
import { useFlowStore } from "./flow-store";
import { useResultsStore } from "./results-store";
import { useEditorStore } from "./editor-store";

export const useNodeStore = defineStore("node", {
  state: () => ({
    nodeId: -1 as number,
    previousNodeId: -1 as number,
    nodeValidateFuncs: new Map<number, () => void>(),
    nodeData: null as NodeData | null,
    nodeExists: false,
    isLoaded: false,
    sizeDataPreview: 300 as number,
    dataTypes: ["String", "Datetime", "Int64", "Int32", "Int16", "Float64", "Float32", "Boolean"],
    nodeDescriptions: {} as NodeDescriptionDictionary,
    nodeReferences: {} as NodeReferenceDictionary,
    allExpressions: null as null | ExpressionsOverview[],
  }),

  getters: {
    currentNodeId: (state) => state.nodeId,
    currentNodeData: (state) => state.nodeData,

    // Backward compatibility: read-only getters (use snake_case for legacy)
    /** @deprecated Use `nodeId` (camelCase) instead. This getter is read-only. */
    node_id: (state) => state.nodeId,
    /** @deprecated Use `isLoaded` (camelCase) instead. This getter is read-only. */
    is_loaded: (state) => state.isLoaded,

    // Proxy getters to other stores
    /** @deprecated Use `useFlowStore().flowId` directly. This getter is read-only. */
    flow_id(): number {
      return useFlowStore().flowId;
    },
    /** @deprecated Use `useFlowStore().vueFlowInstance` directly. This getter is read-only. */
    vueFlowInstance() {
      return useFlowStore().vueFlowInstance;
    },
    /** @deprecated Use `useEditorStore().isRunning` directly. This getter is read-only. */
    isRunning(): boolean {
      return useEditorStore()?.isRunning || false;
    },
    /** @deprecated Use `useEditorStore().isDrawerOpen` directly. This getter is read-only. */
    isDrawerOpen(): boolean {
      return useEditorStore()?.isDrawerOpen || false;
    },
    /** @deprecated Use `useEditorStore().activeDrawerComponent` directly. This getter is read-only. */
    activeDrawerComponent() {
      return useEditorStore()?.activeDrawerComponent;
    },
    /** @deprecated Use `useEditorStore().drawerProps` directly. This getter is read-only. */
    drawerProps() {
      return useEditorStore()?.drawerProps || {};
    },
    /** @deprecated Use `useEditorStore().showCodeGenerator` directly. This getter is read-only. */
    showCodeGenerator(): boolean {
      return useEditorStore()?.showCodeGenerator || false;
    },
    /** @deprecated Use `useEditorStore().showFlowResult` directly. This getter is read-only. */
    showFlowResult(): boolean {
      return useEditorStore()?.showFlowResult || false;
    },
    /** @deprecated Use `useEditorStore().isShowingLogViewer` directly. This getter is read-only. */
    isShowingLogViewer(): boolean {
      return useEditorStore()?.isShowingLogViewer || false;
    },
    /** @deprecated Use `useEditorStore().hideLogViewerForThisRun` directly. This getter is read-only. */
    hideLogViewerForThisRun(): boolean {
      return useEditorStore()?.hideLogViewerForThisRun || false;
    },
    /** @deprecated Use `useEditorStore().displayLogViewer` directly. This getter is read-only. */
    displayLogViewer(): boolean {
      const editorStore = useEditorStore();
      return editorStore?.displayLogViewer !== undefined ? editorStore.displayLogViewer : true;
    },
    /** @deprecated Use `useEditorStore().inputCode` directly. This getter is read-only. */
    inputCode(): string {
      return useEditorStore()?.inputCode || "";
    },
    /** @deprecated Use `useResultsStore().currentRunResult` directly. This getter is read-only. */
    currentRunResult() {
      return useResultsStore()?.currentRunResult;
    },
    /** @deprecated Use `useResultsStore().runResults` directly. This getter is read-only. */
    runResults() {
      return useResultsStore()?.runResults || {};
    },
  },

  actions: {
    // ========== Node Data Management ==========
    async getNodeData(nodeId: number, useCache = true): Promise<NodeData | null> {
      const flowStore = useFlowStore();

      if (this.nodeId === nodeId && useCache) {
        if (this.nodeData) {
          this.isLoaded = true;
          return this.nodeData;
        }
      }

      try {
        console.log("Getting node data");
        const data = await NodeApi.getNodeData(flowStore.flowId, nodeId);
        this.nodeData = data;
        this.isLoaded = true;
        this.nodeExists = true;
        return this.nodeData;
      } catch (error) {
        console.error("Error fetching node data:", error);
        this.nodeData = null;
        this.isLoaded = false;
        this.nodeExists = false;
        return null;
      }
    },

    async reloadCurrentNodeData(): Promise<NodeData | null> {
      return this.getNodeData(this.nodeId, false);
    },

    getCurrentNodeData(): NodeData | null {
      return this.nodeData;
    },

    async getTableExample(flowId: number, nodeId: number): Promise<TableExample | null> {
      try {
        return await NodeApi.getTableExample(flowId, nodeId);
      } catch (error) {
        console.error("Error fetching table example:", error);
        return null;
      }
    },

    // ========== Node ID Management ==========
    setFlowIdAndNodeId(flowId: number, nodeId: number) {
      const flowStore = useFlowStore();

      if (this.nodeId === nodeId && flowStore.flowId === flowId) {
        return;
      }

      console.log("Automatically pushing the node data");
      if (flowStore.flowId !== flowId) {
        flowStore.setFlowId(flowId);
      }
      this.nodeId = nodeId;
    },

    doReset() {
      this.isLoaded = false;
    },

    // ========== Node Validation ==========
    setNodeValidateFunc(nodeId: number | string, func: () => void) {
      if (typeof nodeId === "string") {
        nodeId = parseInt(nodeId);
      }
      this.nodeValidateFuncs.set(nodeId, func);
    },

    async validateNode(nodeId: number | string) {
      if (typeof nodeId === "string") {
        nodeId = parseInt(nodeId);
      }
      const func = this.nodeValidateFuncs.get(nodeId);
      if (func) {
        func();
      }
    },

    // ========== Node Descriptions ==========
    initializeDescriptionCache(flowId: number): void {
      if (!this.nodeDescriptions[flowId]) {
        this.nodeDescriptions[flowId] = {};
      }
    },

    cacheNodeDescriptionDict(
      flowId: number,
      nodeId: number,
      description: string,
      isAutoGenerated: boolean = false,
    ): void {
      this.initializeDescriptionCache(flowId);
      this.nodeDescriptions[flowId][nodeId] = { description, is_auto_generated: isAutoGenerated };
      if (this.nodeData && this.nodeData.node_id === nodeId && this.nodeData.setting_input) {
        this.nodeData.setting_input.description = isAutoGenerated
          ? ""
          : description;
      }
    },

    clearNodeDescriptionCache(flowId: number, nodeId: number): void {
      if (this.nodeDescriptions[flowId] && this.nodeDescriptions[flowId][nodeId]) {
        delete this.nodeDescriptions[flowId][nodeId];
      }
    },

    clearFlowDescriptionCache(flowId: number): void {
      if (this.nodeDescriptions[flowId]) {
        delete this.nodeDescriptions[flowId];
      }
    },

    clearAllDescriptionCaches(): void {
      this.nodeDescriptions = {};
    },

    async getNodeDescription(nodeId: number, forceRefresh = false): Promise<string> {
      const flowStore = useFlowStore();
      this.initializeDescriptionCache(flowStore.flowId);

      const cached = this.nodeDescriptions[flowStore.flowId]?.[nodeId];
      if (!forceRefresh && cached) {
        return cached.description;
      }

      try {
        const response = await NodeApi.getNodeDescription(flowStore.flowId, nodeId);
        this.cacheNodeDescriptionDict(
          flowStore.flowId,
          nodeId,
          response.description,
          response.is_auto_generated,
        );
        return response.description;
      } catch (error) {
        console.info("Error fetching node description:", error);
        if (cached) {
          console.warn("Using cached description due to API error");
          return cached.description;
        }
        return "";
      }
    },

    async setNodeDescription(nodeId: number, description: string): Promise<void> {
      const flowStore = useFlowStore();

      try {
        this.cacheNodeDescriptionDict(flowStore.flowId, nodeId, description, false);
        const result = await NodeApi.setNodeDescription(flowStore.flowId, nodeId, description);

        if (result === true) {
          console.log("Description updated successfully");
        } else {
          console.warn("Unexpected response:", result);
        }
      } catch (error: any) {
        if (error.response) {
          console.error("API error:", error.response.data.message);
        } else if (error.request) {
          console.error("The request was made but no response was received");
        } else {
          console.error("Error", error.message);
        }
        throw error;
      }
    },

    updateNodeDescription(nodeId: number, description: string) {
      const flowStore = useFlowStore();
      this.cacheNodeDescriptionDict(flowStore.flowId, nodeId, description, false);
    },

    // ========== Node References ==========
    initializeReferenceCache(flowId: number): void {
      if (!this.nodeReferences[flowId]) {
        this.nodeReferences[flowId] = {};
      }
    },

    cacheNodeReferenceDict(flowId: number, nodeId: number, reference: string): void {
      this.initializeReferenceCache(flowId);
      this.nodeReferences[flowId][nodeId] = reference;
      if (this.nodeData && this.nodeData.node_id === nodeId && this.nodeData.setting_input) {
        this.nodeData.setting_input.node_reference = reference;
      }
    },

    clearNodeReferenceCache(flowId: number, nodeId: number): void {
      if (this.nodeReferences[flowId] && this.nodeReferences[flowId][nodeId]) {
        delete this.nodeReferences[flowId][nodeId];
      }
    },

    clearFlowReferenceCache(flowId: number): void {
      if (this.nodeReferences[flowId]) {
        delete this.nodeReferences[flowId];
      }
    },

    clearAllReferenceCaches(): void {
      this.nodeReferences = {};
    },

    async getNodeReference(nodeId: number, forceRefresh = false): Promise<string> {
      const flowStore = useFlowStore();
      this.initializeReferenceCache(flowStore.flowId);

      if (!forceRefresh && this.nodeReferences[flowStore.flowId]?.[nodeId]) {
        return this.nodeReferences[flowStore.flowId][nodeId];
      }

      try {
        const reference = await NodeApi.getNodeReference(flowStore.flowId, nodeId);
        this.cacheNodeReferenceDict(flowStore.flowId, nodeId, reference);
        return reference;
      } catch (error) {
        console.info("Error fetching node reference:", error);
        if (this.nodeReferences[flowStore.flowId]?.[nodeId]) {
          console.warn("Using cached reference due to API error");
          return this.nodeReferences[flowStore.flowId][nodeId];
        }
        return "";
      }
    },

    async setNodeReference(nodeId: number, reference: string): Promise<void> {
      const flowStore = useFlowStore();

      try {
        this.cacheNodeReferenceDict(flowStore.flowId, nodeId, reference);
        const result = await NodeApi.setNodeReference(flowStore.flowId, nodeId, reference);

        if (result === true) {
          console.log("Reference updated successfully");
        } else {
          console.warn("Unexpected response:", result);
        }
      } catch (error: any) {
        if (error.response) {
          console.error("API error:", error.response.data.message || error.response.data.detail);
          throw new Error(error.response.data.detail || "Failed to update reference");
        } else if (error.request) {
          console.error("The request was made but no response was received");
        } else {
          console.error("Error", error.message);
        }
        throw error;
      }
    },

    async validateNodeReference(
      nodeId: number,
      reference: string,
    ): Promise<{ valid: boolean; error: string | null }> {
      const flowStore = useFlowStore();
      try {
        return await NodeApi.validateNodeReference(flowStore.flowId, nodeId, reference);
      } catch (error) {
        console.error("Error validating node reference:", error);
        return { valid: false, error: "Failed to validate reference" };
      }
    },

    updateNodeReference(nodeId: number, reference: string) {
      const flowStore = useFlowStore();
      this.cacheNodeReferenceDict(flowStore.flowId, nodeId, reference);
    },

    // ========== Node Settings Updates ==========
    async refreshAutoGeneratedDescription(nodeId: number): Promise<void> {
      const flowStore = useFlowStore();
      const cached = this.nodeDescriptions[flowStore.flowId]?.[nodeId];
      if (!cached || cached.is_auto_generated) {
        await this.getNodeDescription(nodeId, true);
      }
    },

    async updateSettingsDirectly(inputData: any): Promise<any> {
      const flowStore = useFlowStore();

      try {
        const node = flowStore.vueFlowInstance?.findNode(String(inputData.node_id)) as Node;
        inputData.pos_x = node.position.x;
        inputData.pos_y = node.position.y;

        const response = await NodeApi.updateSettingsDirectly(
          node.data.nodeTemplate.item,
          inputData,
        );

        const downstreamNodeIds = await NodeApi.getDownstreamNodeIds(
          flowStore.flowId,
          inputData.node_id,
        );
        downstreamNodeIds.map((nodeId) => {
          this.validateNode(nodeId);
        });

        this.refreshAutoGeneratedDescription(inputData.node_id);

        return response;
      } catch (error: any) {
        console.error("Error updating settings directly:", error.response?.data);
        throw error;
      }
    },

    async updateUserDefinedSettings(inputData: any): Promise<any> {
      const flowStore = useFlowStore();

      try {
        const node = flowStore.vueFlowInstance?.findNode(String(inputData.value.node_id)) as Node;
        const nodeType = node.data.nodeTemplate.item;
        inputData.value.pos_x = node.position.x;
        inputData.value.pos_y = node.position.y;

        const response = await NodeApi.updateUserDefinedSettings(nodeType, inputData.value);

        const downstreamNodeIds = await NodeApi.getDownstreamNodeIds(
          flowStore.flowId,
          inputData.value.node_id,
        );
        downstreamNodeIds.map((nodeId) => {
          this.validateNode(nodeId);
        });

        this.refreshAutoGeneratedDescription(inputData.value.node_id);

        return response;
      } catch (error: any) {
        console.error("Error updating settings:", error.response?.data);
        throw error;
      }
    },

    async updateSettings(inputData: any, inputNodeType?: string): Promise<any> {
      const flowStore = useFlowStore();

      try {
        const node = flowStore.vueFlowInstance?.findNode(String(inputData.value.node_id)) as Node;
        const nodeType = inputNodeType ?? node.data.nodeTemplate.item;

        inputData.value.pos_x = node.position.x;
        inputData.value.pos_y = node.position.y;

        const response = await NodeApi.updateSettingsDirectly(nodeType, inputData.value);

        const downstreamNodeIds = await NodeApi.getDownstreamNodeIds(
          flowStore.flowId,
          inputData.value.node_id,
        );
        downstreamNodeIds.map((nodeId) => {
          this.validateNode(nodeId);
        });

        this.refreshAutoGeneratedDescription(inputData.value.node_id);

        return response;
      } catch (error: any) {
        console.error("Error updating settings:", error.response?.data);
        throw error;
      }
    },

    // ========== Expressions ==========
    async fetchExpressionsOverview(): Promise<ExpressionsOverview[]> {
      try {
        const expressions = await ExpressionsApi.getExpressionsOverview();
        this.allExpressions = expressions;
        return this.allExpressions;
      } catch (error) {
        console.error("Error fetching expressions overview:", error);
        return [];
      }
    },

    async getExpressionsOverview(): Promise<ExpressionsOverview[]> {
      if (this.allExpressions) {
        return this.allExpressions;
      } else {
        return await this.fetchExpressionsOverview();
      }
    },

    // ========== Utilities ==========
    getDataTypes() {
      return this.dataTypes;
    },

    getSizeDataPreview() {
      return this.sizeDataPreview;
    },

    setSizeDataPreview(newHeight: number) {
      this.sizeDataPreview = newHeight;
    },

    getEditorNodeData() {
      const flowStore = useFlowStore();
      if (this.nodeId) {
        return flowStore.vueFlowInstance?.findNode(String(this.nodeId));
      }
      return null;
    },

    // ========== Backward Compatibility Actions (Proxy to other stores) ==========
    /** @deprecated Use `useFlowStore().setFlowId()` directly instead. */
    setFlowId(flowId: number) {
      const flowStore = useFlowStore();
      flowStore.setFlowId(flowId);
    },

    /** @deprecated Use `useFlowStore().setVueFlowInstance()` directly instead. */
    setVueFlowInstance(vueFlowInstance: any) {
      const flowStore = useFlowStore();
      flowStore.setVueFlowInstance(vueFlowInstance);
    },

    /** @deprecated Use `useFlowStore().getVueFlowInstance()` directly instead. */
    getVueFlowInstance() {
      const flowStore = useFlowStore();
      return flowStore.getVueFlowInstance();
    },

    /** @deprecated Use `useResultsStore().setNodeResult()` directly instead. */
    setNodeResult(nodeId: number, result: any) {
      const flowStore = useFlowStore();
      const resultsStore = useResultsStore();
      resultsStore.setNodeResult(flowStore.flowId, nodeId, result);
    },

    /** @deprecated Use `useResultsStore().getNodeResult()` directly instead. */
    getNodeResult(nodeId: number) {
      const flowStore = useFlowStore();
      const resultsStore = useResultsStore();
      return resultsStore.getNodeResult(flowStore.flowId, nodeId);
    },

    /** @deprecated Use `useResultsStore().resetNodeResult()` directly instead. */
    resetNodeResult() {
      const resultsStore = useResultsStore();
      resultsStore.resetNodeResult();
    },

    /** @deprecated Use `useResultsStore().clearFlowResults()` directly instead. */
    clearFlowResults(flowId: number) {
      const resultsStore = useResultsStore();
      resultsStore.clearFlowResults(flowId);
    },

    /** @deprecated Use `useResultsStore().setNodeValidation()` directly instead. */
    setNodeValidation(nodeId: number | string, nodeValidationInput: any) {
      const flowStore = useFlowStore();
      const resultsStore = useResultsStore();
      resultsStore.setNodeValidation(flowStore.flowId, nodeId, nodeValidationInput);
    },

    /** @deprecated Use `useResultsStore().resetNodeValidation()` directly instead. */
    resetNodeValidation() {
      const resultsStore = useResultsStore();
      resultsStore.resetNodeValidation();
    },

    /** @deprecated Use `useResultsStore().getNodeValidation()` directly instead. */
    getNodeValidation(nodeId: number) {
      const flowStore = useFlowStore();
      const resultsStore = useResultsStore();
      return resultsStore.getNodeValidation(flowStore.flowId, nodeId);
    },

    /** @deprecated Use `useResultsStore().insertRunResult()` directly instead. */
    insertRunResult(runResult: any) {
      const resultsStore = useResultsStore();
      resultsStore.insertRunResult(runResult);
    },

    /** @deprecated Use `useResultsStore().resetRunResults()` directly instead. */
    resetRunResults() {
      const resultsStore = useResultsStore();
      resultsStore.resetRunResults();
    },

    /** @deprecated Use `useResultsStore().getRunResult()` directly instead. */
    getRunResult(flowId: number) {
      const resultsStore = useResultsStore();
      return resultsStore.getRunResult(flowId);
    },

    /** @deprecated Use `useEditorStore().openDrawer()` directly instead. */
    openDrawer(component: any, nodeTitleInfo: any, props: Record<string, any> = {}) {
      const editorStore = useEditorStore();
      editorStore.openDrawer(component, nodeTitleInfo, props);
    },

    /** @deprecated Use `useEditorStore().closeDrawer()` directly instead. */
    closeDrawer() {
      const editorStore = useEditorStore();
      editorStore.closeDrawer();
      this.nodeId = -1;
    },

    /** @deprecated Use `useEditorStore().toggleDrawer()` directly instead. */
    toggleDrawer() {
      const editorStore = useEditorStore();
      editorStore.toggleDrawer();
    },

    /** @deprecated Use `useEditorStore().pushNodeData()` directly instead. */
    pushNodeData() {
      const editorStore = useEditorStore();
      editorStore.pushNodeData();
    },

    /** @deprecated Use `useEditorStore().setCloseFunction()` directly instead. */
    setCloseFunction(f: () => void) {
      const editorStore = useEditorStore();
      editorStore.setCloseFunction(f);
    },

    /** @deprecated Use `useEditorStore().executeDrawCloseFunction()` directly instead. */
    executeDrawCloseFunction() {
      const editorStore = useEditorStore();
      return editorStore.executeDrawCloseFunction();
    },

    /** @deprecated Use `useEditorStore().clearCloseFunction()` directly instead. */
    clearCloseFunction() {
      const editorStore = useEditorStore();
      editorStore.clearCloseFunction();
    },

    /** @deprecated Use `useEditorStore().toggleCodeGenerator()` directly instead. */
    toggleCodeGenerator() {
      const editorStore = useEditorStore();
      editorStore.toggleCodeGenerator();
    },

    /** @deprecated Use `useEditorStore().setCodeGeneratorVisibility()` directly instead. */
    setCodeGeneratorVisibility(visible: boolean) {
      const editorStore = useEditorStore();
      editorStore.setCodeGeneratorVisibility(visible);
    },

    /** @deprecated Use `useEditorStore().showLogViewer()` directly instead. */
    showLogViewer() {
      const editorStore = useEditorStore();
      editorStore.showLogViewer();
    },

    /** @deprecated Use `useEditorStore().hideLogViewer()` directly instead. */
    hideLogViewer() {
      const editorStore = useEditorStore();
      editorStore.hideLogViewer();
    },

    /** @deprecated Use `useEditorStore().toggleLogViewer()` directly instead. */
    toggleLogViewer() {
      const editorStore = useEditorStore();
      editorStore.toggleLogViewer();
    },

    /** @deprecated Use `useEditorStore().setInputCode()` directly instead. */
    setInputCode(newCode: string) {
      const editorStore = useEditorStore();
      editorStore.setInputCode(newCode);
    },

    /** @deprecated Use `useEditorStore().setInitialEditorData()` directly instead. */
    setInitialEditorData(editorDataString: string) {
      const editorStore = useEditorStore();
      editorStore.setInitialEditorData(editorDataString);
    },

    /** @deprecated Use `useEditorStore().getInitialEditorData()` directly instead. */
    getInitialEditorData() {
      const editorStore = useEditorStore();
      return editorStore.getInitialEditorData();
    },
  },
});
