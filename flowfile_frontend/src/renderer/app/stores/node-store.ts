// Node Store - Manages node data, validation, and node-level state
// IMPORTANT: This store includes backward compatibility proxies to other stores
import { defineStore } from 'pinia';
import type { Node } from '@vue-flow/core';
import type {
  NodeData,
  TableExample,
  NodeDescriptionDictionary,
  ExpressionsOverview,
} from '../features/designer/baseNode/nodeInterfaces';
import { NodeApi, ExpressionsApi } from '../services/api';
import { useFlowStore } from './flow-store';
import { useResultsStore } from './results-store';
import { useEditorStore } from './editor-store';

export const useNodeStore = defineStore('node', {
  state: () => ({
    nodeId: -1 as number,
    previousNodeId: -1 as number,
    nodeValidateFuncs: new Map<number, () => void>(),
    nodeData: null as NodeData | null,
    nodeExists: false,
    isLoaded: false,
    sizeDataPreview: 300 as number,
    dataTypes: ['String', 'Datetime', 'Int64', 'Int32', 'Int16', 'Float64', 'Float32', 'Boolean'],
    nodeDescriptions: {} as NodeDescriptionDictionary,
    allExpressions: null as null | ExpressionsOverview[],
  }),

  getters: {
    currentNodeId: (state) => state.nodeId,
    currentNodeData: (state) => state.nodeData,

    // Backward compatibility: read-only getters (use snake_case for legacy)
    node_id: (state) => state.nodeId,
    is_loaded: (state) => state.isLoaded,

    // Proxy getters to other stores
    flow_id(): number {
      return useFlowStore().flowId;
    },
    vueFlowInstance() {
      return useFlowStore().vueFlowInstance;
    },
    isRunning(): boolean {
      return useEditorStore()?.isRunning || false;
    },
    isDrawerOpen(): boolean {
      return useEditorStore()?.isDrawerOpen || false;
    },
    activeDrawerComponent() {
      return useEditorStore()?.activeDrawerComponent;
    },
    drawerProps() {
      return useEditorStore()?.drawerProps || {};
    },
    showCodeGenerator(): boolean {
      return useEditorStore()?.showCodeGenerator || false;
    },
    showFlowResult(): boolean {
      return useEditorStore()?.showFlowResult || false;
    },
    isShowingLogViewer(): boolean {
      return useEditorStore()?.isShowingLogViewer || false;
    },
    hideLogViewerForThisRun(): boolean {
      return useEditorStore()?.hideLogViewerForThisRun || false;
    },
    displayLogViewer(): boolean {
      const editorStore = useEditorStore();
      return editorStore?.displayLogViewer !== undefined ? editorStore.displayLogViewer : true;
    },
    inputCode(): string {
      return useEditorStore()?.inputCode || '';
    },
    currentRunResult() {
      return useResultsStore()?.currentRunResult;
    },
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
        console.log('Getting node data');
        const data = await NodeApi.getNodeData(flowStore.flowId, nodeId);
        this.nodeData = data;
        this.isLoaded = true;
        this.nodeExists = true;
        return this.nodeData;
      } catch (error) {
        console.error('Error fetching node data:', error);
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
        console.error('Error fetching table example:', error);
        return null;
      }
    },

    // ========== Node ID Management ==========
    setFlowIdAndNodeId(flowId: number, nodeId: number) {
      const flowStore = useFlowStore();

      if (this.nodeId === nodeId && flowStore.flowId === flowId) {
        return;
      }

      console.log('Automatically pushing the node data');
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
      if (typeof nodeId === 'string') {
        nodeId = parseInt(nodeId);
      }
      this.nodeValidateFuncs.set(nodeId, func);
    },

    async validateNode(nodeId: number | string) {
      if (typeof nodeId === 'string') {
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

    cacheNodeDescriptionDict(flowId: number, nodeId: number, description: string): void {
      this.initializeDescriptionCache(flowId);
      this.nodeDescriptions[flowId][nodeId] = description;
      if (this.nodeData && this.nodeData.node_id === nodeId && this.nodeData.setting_input) {
        this.nodeData.setting_input.description = description;
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

      if (!forceRefresh && this.nodeDescriptions[flowStore.flowId]?.[nodeId]) {
        return this.nodeDescriptions[flowStore.flowId][nodeId];
      }

      try {
        const description = await NodeApi.getNodeDescription(flowStore.flowId, nodeId);
        this.cacheNodeDescriptionDict(flowStore.flowId, nodeId, description);
        return description;
      } catch (error) {
        console.info('Error fetching node description:', error);
        if (this.nodeDescriptions[flowStore.flowId]?.[nodeId]) {
          console.warn('Using cached description due to API error');
          return this.nodeDescriptions[flowStore.flowId][nodeId];
        }
        return '';
      }
    },

    async setNodeDescription(nodeId: number, description: string): Promise<void> {
      const flowStore = useFlowStore();

      try {
        this.cacheNodeDescriptionDict(flowStore.flowId, nodeId, description);
        const result = await NodeApi.setNodeDescription(flowStore.flowId, nodeId, description);

        if (result === true) {
          console.log('Description updated successfully');
        } else {
          console.warn('Unexpected response:', result);
        }
      } catch (error: any) {
        if (error.response) {
          console.error('API error:', error.response.data.message);
        } else if (error.request) {
          console.error('The request was made but no response was received');
        } else {
          console.error('Error', error.message);
        }
        throw error;
      }
    },

    updateNodeDescription(nodeId: number, description: string) {
      const flowStore = useFlowStore();
      this.cacheNodeDescriptionDict(flowStore.flowId, nodeId, description);
    },

    // ========== Node Settings Updates ==========
    async updateSettingsDirectly(inputData: any): Promise<any> {
      const flowStore = useFlowStore();

      try {
        const node = flowStore.vueFlowInstance?.findNode(String(inputData.node_id)) as Node;
        inputData.pos_x = node.position.x;
        inputData.pos_y = node.position.y;

        const response = await NodeApi.updateSettingsDirectly(
          node.data.nodeTemplate.item,
          inputData
        );

        const downstreamNodeIds = await NodeApi.getDownstreamNodeIds(
          flowStore.flowId,
          inputData.node_id
        );
        downstreamNodeIds.map((nodeId) => {
          this.validateNode(nodeId);
        });

        return response;
      } catch (error: any) {
        console.error('Error updating settings directly:', error.response?.data);
        throw error;
      }
    },

    async updateUserDefinedSettings(inputData: any): Promise<any> {
      const flowStore = useFlowStore();

      try {
        const node = flowStore.vueFlowInstance?.findNode(
          String(inputData.value.node_id)
        ) as Node;
        const nodeType = node.data.nodeTemplate.item;
        inputData.value.pos_x = node.position.x;
        inputData.value.pos_y = node.position.y;

        const response = await NodeApi.updateUserDefinedSettings(nodeType, inputData.value);

        const downstreamNodeIds = await NodeApi.getDownstreamNodeIds(
          flowStore.flowId,
          inputData.value.node_id
        );
        downstreamNodeIds.map((nodeId) => {
          this.validateNode(nodeId);
        });

        return response;
      } catch (error: any) {
        console.error('Error updating settings:', error.response?.data);
        throw error;
      }
    },

    async updateSettings(inputData: any, inputNodeType?: string): Promise<any> {
      const flowStore = useFlowStore();

      try {
        const node = flowStore.vueFlowInstance?.findNode(
          String(inputData.value.node_id)
        ) as Node;
        const nodeType = inputNodeType ?? node.data.nodeTemplate.item;

        inputData.value.pos_x = node.position.x;
        inputData.value.pos_y = node.position.y;

        const response = await NodeApi.updateSettingsDirectly(nodeType, inputData.value);

        const downstreamNodeIds = await NodeApi.getDownstreamNodeIds(
          flowStore.flowId,
          inputData.value.node_id
        );
        downstreamNodeIds.map((nodeId) => {
          this.validateNode(nodeId);
        });

        return response;
      } catch (error: any) {
        console.error('Error updating settings:', error.response?.data);
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
        console.error('Error fetching expressions overview:', error);
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
    setFlowId(flowId: number) {
      const flowStore = useFlowStore();
      flowStore.setFlowId(flowId);
    },

    setVueFlowInstance(vueFlowInstance: any) {
      const flowStore = useFlowStore();
      flowStore.setVueFlowInstance(vueFlowInstance);
    },

    getVueFlowInstance() {
      const flowStore = useFlowStore();
      return flowStore.getVueFlowInstance();
    },

    setNodeResult(nodeId: number, result: any) {
      const flowStore = useFlowStore();
      const resultsStore = useResultsStore();
      resultsStore.setNodeResult(flowStore.flowId, nodeId, result);
    },

    getNodeResult(nodeId: number) {
      const flowStore = useFlowStore();
      const resultsStore = useResultsStore();
      return resultsStore.getNodeResult(flowStore.flowId, nodeId);
    },

    resetNodeResult() {
      const resultsStore = useResultsStore();
      resultsStore.resetNodeResult();
    },

    clearFlowResults(flowId: number) {
      const resultsStore = useResultsStore();
      resultsStore.clearFlowResults(flowId);
    },

    setNodeValidation(nodeId: number | string, nodeValidationInput: any) {
      const flowStore = useFlowStore();
      const resultsStore = useResultsStore();
      resultsStore.setNodeValidation(flowStore.flowId, nodeId, nodeValidationInput);
    },

    resetNodeValidation() {
      const resultsStore = useResultsStore();
      resultsStore.resetNodeValidation();
    },

    getNodeValidation(nodeId: number) {
      const flowStore = useFlowStore();
      const resultsStore = useResultsStore();
      return resultsStore.getNodeValidation(flowStore.flowId, nodeId);
    },

    insertRunResult(runResult: any, showResult: boolean = true) {
      const resultsStore = useResultsStore();
      resultsStore.insertRunResult(runResult, showResult);
    },

    resetRunResults() {
      const resultsStore = useResultsStore();
      resultsStore.resetRunResults();
    },

    getRunResult(flowId: number) {
      const resultsStore = useResultsStore();
      return resultsStore.getRunResult(flowId);
    },

    openDrawer(component: any, nodeTitleInfo: any, props: Record<string, any> = {}) {
      const editorStore = useEditorStore();
      editorStore.openDrawer(component, nodeTitleInfo, props);
    },

    closeDrawer() {
      const editorStore = useEditorStore();
      editorStore.closeDrawer();
      this.nodeId = -1;
    },

    toggleDrawer() {
      const editorStore = useEditorStore();
      editorStore.toggleDrawer();
    },

    pushNodeData() {
      const editorStore = useEditorStore();
      editorStore.pushNodeData();
    },

    setCloseFunction(f: () => void) {
      const editorStore = useEditorStore();
      editorStore.setCloseFunction(f);
    },

    executeDrawCloseFunction() {
      const editorStore = useEditorStore();
      return editorStore.executeDrawCloseFunction();
    },

    toggleCodeGenerator() {
      const editorStore = useEditorStore();
      editorStore.toggleCodeGenerator();
    },

    setCodeGeneratorVisibility(visible: boolean) {
      const editorStore = useEditorStore();
      editorStore.setCodeGeneratorVisibility(visible);
    },

    showLogViewer() {
      const editorStore = useEditorStore();
      editorStore.showLogViewer();
    },

    hideLogViewer() {
      const editorStore = useEditorStore();
      editorStore.hideLogViewer();
    },

    toggleLogViewer() {
      const editorStore = useEditorStore();
      editorStore.toggleLogViewer();
    },

    setInputCode(newCode: string) {
      const editorStore = useEditorStore();
      editorStore.setInputCode(newCode);
    },

    setInitialEditorData(editorDataString: string) {
      const editorStore = useEditorStore();
      editorStore.setInitialEditorData(editorDataString);
    },

    getInitialEditorData() {
      const editorStore = useEditorStore();
      return editorStore.getInitialEditorData();
    },
  },
});
