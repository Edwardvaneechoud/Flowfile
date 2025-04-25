import { defineStore } from 'pinia'
import { ref } from 'vue'
import axios from 'axios'
import {
  NodeData,
  TableExample,
  RunInformation,
  RunInformationDictionary,
  NodeValidationInput,
  NodeResult,
  NodeValidation,
  NodeDescriptionDictionary,
  ExpressionsOverview,
} from '../features/designer/baseNode/nodeInterfaces'
import { VueFlowStore, Node } from '@vue-flow/core';
import { get } from 'lodash';

// Session storage key for the flow ID
const FLOW_ID_STORAGE_KEY = 'last_flow_id';

export const nodeData = ref<NodeData | null>(null)

export const useColumnStore = defineStore('column', {
  state: () => {
    return {
      localColumns: [] as string[],
    }
  },

  actions: {
    setColumns(columns: string[]) {
      this.localColumns = columns
    },

    addColumn(column: string) {
      this.localColumns.push(column)
    },

    // any other actions that modify `localColumns`
  },
})

export const getDownstreamNodeIds = async (flow_id: number, node_id: number): Promise<number[]> => {
  const response = await axios.get('/node/downstream_node_ids', {
    params: { flow_id: flow_id, node_id: node_id },
    headers: { accept: 'application/json' },
  })
  return response.data
}


const loadDownstreamNodeIds = async (flowId: number, nodeId: number) => {
  const downstreamNodeIds = await getDownstreamNodeIds(flowId, nodeId)
  return downstreamNodeIds
}


export const useNodeStore = defineStore('node', {
  state: () => {
    // Try to get the last flow ID from session storage
    const savedFlowId = sessionStorage.getItem(FLOW_ID_STORAGE_KEY);
    const initialFlowId = savedFlowId ? parseInt(savedFlowId) : -1;
    
    return {
      inputCode: '',
      flow_id: initialFlowId as number,
      node_id: -1 as number,
      previous_node_id: -1 as number,
      nodeValidateFuncs: new Map<number, () => void>(),
      nodeData: null as NodeData | null,
      node_exists: false,
      is_loaded: false,
      size_data_preview: 300 as number,
      dataTypes: ['String', 'Datetime', 'Int64', 'Int32', 'Int16', 'Float64', 'Float32', 'Boolean'], // Adding the data types here
      isDrawerOpen: false,
      isAnalysisOpen: false,
      drawCloseFunction: null as any,
      initialEditorData: '' as string,
      runResults: {} as RunInformationDictionary,
      nodeDescriptions: {} as NodeDescriptionDictionary,
      runNodeResults: {} as Record<number, Record<number, NodeResult>>,
      runNodeValidations: {} as Record<number, Record<number, NodeValidation>>,
      currentRunResult: null as RunInformation | null,
      isRunning: false,
      showFlowResult: false,
      tableVisible: false,
      resultVersion: 0,
      vueFlowInstance: null as any | VueFlowStore,
      allExpressions: null as null| ExpressionsOverview[],
      isShowingLogViewer : false,
      isStreamingLogs: false,
      displayLogViewer: true,
    }
  },

  actions: {

    initializeResultCache(flowId: number): void {
      if (!this.runNodeResults[flowId]) {
        this.runNodeResults[flowId] = {};
      }
    },

    // Initialize validation structure for a flow if it doesn't exist
    initializeValidationCache(flowId: number): void {
      if (!this.runNodeValidations[flowId]) {
        this.runNodeValidations[flowId] = {};
      }
    },

    // Set a node result
    setNodeResult(nodeId: number, result: NodeResult): void {
      this.initializeResultCache(this.flow_id);
      this.runNodeResults[this.flow_id][nodeId] = result;
    },

    // Get a node result
    getNodeResult(nodeId: number): NodeResult | undefined {
      return this.runNodeResults[this.flow_id]?.[nodeId];
    },

    // Reset all node results
    resetNodeResult(): void {
      console.log('Clearing node results');
      this.runNodeResults = {};
    },

    // Clear results for a specific flow
    clearFlowResults(flowId: number): void {
      if (this.runNodeResults[flowId]) {
        delete this.runNodeResults[flowId];
      }
    },

    // Set a node validation
    setNodeValidation(nodeId: number | string, nodeValidationInput: NodeValidationInput): void {
      if (typeof nodeId === 'string') {
        nodeId = parseInt(nodeId);
      }
      
      this.initializeValidationCache(this.flow_id);
      
      const nodeValidation: NodeValidation = {
        ...nodeValidationInput,
        validationTime: Date.now() / 1000
      };
      
      this.runNodeValidations[this.flow_id][nodeId] = nodeValidation;
    },

    // Reset all node validations
    resetNodeValidation(): void {
      this.runNodeValidations = {};
    },

    // Get a node validation
    getNodeValidation(nodeId: number): NodeValidation {
      return this.runNodeValidations[this.flow_id]?.[nodeId] || 
             {isValid: true, error: '', validationTime: 0};
    },

    // Insert run result - modified to use the new structure
    insertRunResult(runResult: RunInformation, showResult: boolean = true): void {
      this.currentRunResult = runResult;
      this.runResults[runResult.flow_id] = runResult;
      this.showFlowResult = showResult;
      this.isShowingLogViewer = this.displayLogViewer && showResult;
      
      // Store node results in the new format
      this.initializeResultCache(runResult.flow_id);
      runResult.node_step_result.forEach((nodeResult) => {
        this.runNodeResults[runResult.flow_id][nodeResult.node_id] = nodeResult;
      });
      
      this.resultVersion++;
    },

    // Reset run results - modified to use the new structure
    resetRunResults(): void {
      this.runNodeResults = {};
      this.runResults = {};
      this.currentRunResult = null;
    },


  
    initializeDescriptionCache(flowId: number): void {
      if (!this.nodeDescriptions[flowId]) {
        this.nodeDescriptions[flowId] = {};
      }
    },

    setNodeValidateFunc(nodeId: number|string, func: () => void) {
      if (typeof nodeId === 'string') {
        nodeId = parseInt(nodeId)
      }
      this.nodeValidateFuncs.set(nodeId, func)
    },

    async validateNode(nodeId: number|string) {
      if (typeof nodeId === 'string') {
        nodeId = parseInt(nodeId)
      }
      const func = this.nodeValidateFuncs.get(nodeId)
      if (func) {
        func()
      }
      else {
        console.warn('No validation function found for node', nodeId)
      }
    },

    setFlowId(flowId: number) {
      this.flow_id = flowId;
      
      // Store the flow ID in session storage
      try {
        sessionStorage.setItem(FLOW_ID_STORAGE_KEY, flowId.toString());
      } catch (error) {
        console.warn('Failed to store flow ID in session storage:', error);
      }
    },


    setVueFlowInstance(vueFlowInstance: VueFlowStore) {
      this.vueFlowInstance = vueFlowInstance
    },

    setInitialEditorData(editorDataString: string) {
      this.initialEditorData = editorDataString
    },
    getInitialEditorData() {
      return this.initialEditorData
    },

    cacheNodeDescriptionDict(flowId: number, nodeId: number, description: string): void {
      this.initializeDescriptionCache(flowId);
      this.nodeDescriptions[flowId][nodeId] = description;
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
      this.initializeDescriptionCache(this.flow_id);
      
      // Return cached description if available and not forced to refresh
      if (!forceRefresh && this.nodeDescriptions[this.flow_id]?.[nodeId]) {
        return this.nodeDescriptions[this.flow_id][nodeId];
      }
      
      try {
        // Fetch a fresh description from the API
        const response = await axios.get('/node/description', {
          params: {
            node_id: nodeId,
            flow_id: this.flow_id,
          },
        });
        
        // Cache the new description
        this.cacheNodeDescriptionDict(this.flow_id, nodeId, response.data);
        return response.data;
      } catch (error) {
        console.info('Error fetching node description:', error);
        
        // Return cached version if available
        if (this.nodeDescriptions[this.flow_id]?.[nodeId]) {
          console.warn('Using cached description due to API error');
          return this.nodeDescriptions[this.flow_id][nodeId];
        }
        
        // Return empty string if no cache available
        return '';
      }
    },

    async setNodeDescription(nodeId: number, description: string): Promise<void> {
      try {
        // Update the cache immediately for responsive UI
        this.cacheNodeDescriptionDict(this.flow_id, nodeId, description);
        
        const response = await axios.post('/node/description/', JSON.stringify(description), {
          params: {
            flow_id: this.flow_id,
            node_id: nodeId,
          },
          headers: {
            'Content-Type': 'application/json',
          },
        });
        
        if (response.data.status === 'success') {
          console.log(response.data.message);
        } else {
          console.warn('Unexpected success response structure:', response.data);
        }
      } catch (error: any) {
        // Handle the error, but keep the updated cache value
        if (error.response) {
          console.error('API error:', error.response.data.message);
        } else if (error.request) {
          console.error('The request was made but no response was received');
        } else {
          console.error('Error', error.message);
        }
        
        throw error; // Re-throw to allow callers to handle the error
      }
    },

    setCloseFunction(f: () => void): void {
      this.drawCloseFunction = f
    },
    getSizeDataPreview() {
      return this.size_data_preview
    },
    setSizeDataPreview(newHeight: number) {
      this.size_data_preview = newHeight
    },

    toggleDrawer() {
      console.log('toggleDrawer in column-store.ts')
      if (this.isDrawerOpen && this.drawCloseFunction) {
        this.pushNodeData()
      }
      this.isDrawerOpen = !this.isDrawerOpen
    },
    pushNodeData() {
      // console.log('pushNodeData called in column-store.ts')
      if (this.drawCloseFunction && !this.isRunning) {
        this.drawCloseFunction()
        this.drawCloseFunction = null
      }
    },

    openDrawer(close_function?: () => void) {
      console.log('openDrawer in column-store.ts')
      if (this.isDrawerOpen) {
        console.log('pushing data')
        this.pushNodeData()
      }
      if (close_function) {
        this.drawCloseFunction = close_function
      }
      this.isDrawerOpen = true
    },

    closeDrawer() {
      this.isDrawerOpen = false
      if (this.drawCloseFunction) {
        this.pushNodeData()
      }
      this.node_id = -1
    },
    openAnalysisDrawer(close_function?: () => void) {
      console.log('openAnalysisDrawer in column-store.ts')
      if (this.isAnalysisOpen) {
        this.pushNodeData()
      }
      if (close_function) {
        this.drawCloseFunction = close_function
      }
      this.isAnalysisOpen = true
    },

    closeAnalysisDrawer() {
      this.isAnalysisOpen = false
      if (this.drawCloseFunction) {
        console.log('closeDrawer in column-store.ts')
        this.pushNodeData()
      }
    },
    getDataTypes() {
      return this.dataTypes
    },

    setInputCode(newCode: string) {
      this.inputCode = newCode
    },

    showLogViewer() {
      console.log('triggered show log viewer')
      this.isShowingLogViewer = this.displayLogViewer;
    },

    hideLogViewer() {
      this.isShowingLogViewer = false;
    },

    toggleLogViewer() {
      console.log('triggered toggle log viewer')
      this.isShowingLogViewer = !this.isShowingLogViewer;
    },


    getRunResult(flow_id: number): RunInformation | null {
      return this.runResults[flow_id] || null
    },

    async getTableExample(flow_id: number, node_id: number): Promise<TableExample | null> {
      try {
        const response = await axios.get<TableExample>('/node/data', {
          params: { flow_id: flow_id, node_id: node_id },
          headers: { accept: 'application/json' },
        })
        return response.data // Return the TableExample or null
      } catch (error) {
        console.error('Error fetching table example:', error)
        return null // Return null in case of an error
      }
    },

    async getNodeData(node_id: number, useCache = true): Promise<NodeData | null> {
      if (this.node_id === node_id && useCache) {
        if (this.nodeData) {
          this.is_loaded = true
          return this.nodeData
        }
      }

      try {
        this.setFlowIdAndNodeId(this.flow_id, node_id)
        const response = await axios.get<NodeData>('/node', {
          params: { flow_id: this.flow_id, node_id: this.node_id },
          headers: { accept: 'application/json' },
        })
        this.nodeData = response.data
        this.is_loaded = true
        this.node_exists = true
        return this.nodeData
      } catch (error) {
        console.error('Error fetching node data:', error)
        this.nodeData = null
        this.is_loaded = false
        this.node_exists = false
        return null // Return null in case of an error
      }
    },
    async reloadCurrentNodeData(): Promise<NodeData | null> {
      return this.getNodeData(this.node_id, false)
    },
    setFlowIdAndNodeId(flow_id: number, node_id: number) {
      if (this.node_id === node_id && this.flow_id === flow_id) {
        return
      }
      console.log('Automatically pushing the node data ')
      this.pushNodeData()
      this.previous_node_id = this.node_id
      
      // Update flow_id and store in session storage
      if (this.flow_id !== flow_id) {
        this.setFlowId(flow_id);
      }
      
      this.node_id = node_id
    },
    getCurrentNodeData(): NodeData | null {
      return this.nodeData
    },
    doReset() {
      this.is_loaded = false
    },
    getVueFlowInstance() {
      return this.vueFlowInstance
    },
    getEditorNodeData() {
      if (this.node_id) {
        return this.vueFlowInstance?.findNode(String(this.node_id))
      }
      return null
    },

    async fetchExpressionsOverview(): Promise<ExpressionsOverview[]> {
      try {
        const response = await axios.get<ExpressionsOverview[]>('/editor/expression_doc')
        this.allExpressions = response.data
        return this.allExpressions
      } catch (error) {
        console.error('Error fetching expressions overview:', error)
        return []
      }
    },

    async getExpressionsOverview(): Promise<ExpressionsOverview[]> {
      if (this.allExpressions) {
        return this.allExpressions
      } else {
        return await this.fetchExpressionsOverview()
      }
    },

    async updateSettingsDirectly(inputData: any): Promise<any> {
      try {
        const node = this.vueFlowInstance?.findNode(String(this.node_id)) as Node

        inputData.pos_x = node.position.x
        inputData.pos_y = node.position.y
        console.log("updating settings")
        console.log('node', node)
        const response = await axios.post('/update_settings/', inputData, {
          params: {
            node_type: node.data.component.__name,
          },
        }
        )
        const downstreamNodeIds = await loadDownstreamNodeIds(this.flow_id, inputData.node_id)
        downstreamNodeIds.map((nodeId) => {
          this.validateNode(nodeId)
        }
        )

        return response.data;
      } catch (error: any) {
        console.error('Error updating settings:', error.response?.data)
        throw error
      }
    },

    async updateSettings(inputData: any): Promise<any> {
      try {
        const node = this.vueFlowInstance?.findNode(String(this.node_id)) as Node

        inputData.value.pos_x = node.position.x
        inputData.value.pos_y = node.position.y
        console.log("updating settings")
        console.log('node', node)
        const response = await axios.post('/update_settings/', inputData.value, {
          params: {
            node_type: node.data.component.__name,
          },
        }
        )
        const downstreamNodeIds = await loadDownstreamNodeIds(this.flow_id, inputData.value.node_id)
        downstreamNodeIds.map((nodeId) => {
          this.validateNode(nodeId)
        }
        )

        return response.data;
      } catch (error: any) {
        console.error('Error updating settings:', error.response?.data)
        throw error
      }
    },
    updateNodeDescription(nodeId: number, description: string) {
      // Update cache first for immediate feedback
      this.cacheNodeDescriptionDict(this.flow_id, nodeId, description);
    }
  },
})