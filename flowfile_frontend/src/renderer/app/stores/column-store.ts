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


const loadDownstreamNodeIds = async (nodeId: number) => {
  const downstreamNodeIds = await getDownstreamNodeIds(1, nodeId)
  return downstreamNodeIds
}


export const useNodeStore = defineStore('node', {
  state: () => ({
    inputCode: '',
    flow_id: -1 as number,
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
    runNodeResultMap: new Map<number, NodeResult>(),
    runNodeValidationMap: new Map<number, NodeValidation>(),
    currentRunResult: null as RunInformation | null,
    isRunning: false,
    showFlowResult: false,
    tableVisible: false,
    resultVersion: 0,
    vueFlowInstance: null as any | null, // Updated type
    allExpressions: null as null| ExpressionsOverview[],
    isShowingLogViewer : false,
    isStreamingLogs: false,
    displayLogViewer: true,
  }),

  actions: {

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
      this.flow_id = flowId
    },

    setNodeResult(nodeId: number, result: NodeResult) {
      const nodeResult = this.runNodeResultMap.get(nodeId)
      this.runNodeResultMap.set(nodeId, result)
    },

    getNodeResult(nodeId: number): NodeResult|undefined {
      return this.runNodeResultMap.get(nodeId)
    },

    resetNodeResult() {
      console.log('Clearing node results')
      this.runNodeResultMap = new Map<number, NodeResult>()
    },

    setNodeValidation(nodeId: number|string, nodeValidationInput: NodeValidationInput) {
      if (typeof nodeId === 'string') {
        nodeId = parseInt(nodeId)
      }
      const nodeValidation: NodeValidation = {
        ...nodeValidationInput,
        validationTime: Date.now()/1000
      };
      this.runNodeValidationMap.set(nodeId, nodeValidation)
    },

    resetNodeValidation() {
      this.runNodeValidationMap.clear()
    },

    getNodeValidation(nodeId: number): NodeValidation | null {
      // Validate the node without running it
      return this.runNodeValidationMap.get(nodeId) || {isValid: true, error: '', validationTime: 0}
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

    cacheNodeDescriptionDict(nodeId: number, description: string) {
      this.nodeDescriptions[nodeId] = description
    },

    async getNodeDescription(nodeId: number): Promise<string> {
      if (!this.nodeDescriptions[nodeId]) {
        const response = await axios.get('/node/description', {
          params: {
            node_id: nodeId,
            flow_id: this.flow_id,
          },
        })
        this.cacheNodeDescriptionDict(nodeId, response.data)
      }
      return this.nodeDescriptions[nodeId]
    },

    async setNodeDescription(nodeId: number, description: string): Promise<void> {
      try {
        const response = await axios.post('/node/description/', JSON.stringify(description), {
          params: {
            flow_id: this.flow_id,
            node_id: nodeId,
          },
          headers: {
            'Content-Type': 'application/json',
          },
        })
        if (response.data.status === 'success') {
          console.log(response.data.message)
        } else {
          console.warn('Unexpected success response structure:', response.data)
        }
      } catch (error: any) {
        if (error.response) {
          console.error(error.response.data.message)
        } else if (error.request) {
          console.error('The request was made but no response was received')
        } else {
          console.error('Error', error.message)
        }
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
      if (this.drawCloseFunction) {
        this.drawCloseFunction()
        this.drawCloseFunction = null
      }
    },

    openDrawer(close_function?: () => void) {
      console.log('openDrawer in column-store.ts')
      if (this.isDrawerOpen) {
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

    resetRunResults() {
      this.runNodeResultMap = new Map<number, NodeResult>()
      this.runResults = {}
      this.currentRunResult = null
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
    
    insertRunResult(runResult: RunInformation, showResult: boolean = true) {
      this.currentRunResult = runResult
      this.runResults[runResult.flow_id] = runResult
      this.showFlowResult = showResult
      this.isShowingLogViewer = this.displayLogViewer && showResult
      runResult.node_step_result.forEach((nodeResult) => {
        this.runNodeResultMap.set(nodeResult.node_id, nodeResult)
      })
      this.resultVersion++
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

    async getNodeData(flow_id: number, node_id: number, useCache = true): Promise<NodeData | null> {
      if (this.flow_id === flow_id && this.node_id === node_id && useCache) {
        if (this.nodeData) {
          this.is_loaded = true
          return this.nodeData
        }
      }

      try {
        this.setFlowIdAndNodeId(flow_id, node_id)
        const response = await axios.get<NodeData>('/node', {
          params: { flow_id: this.flow_id, node_id: this.node_id },
          headers: { accept: 'application/json' },
        })
        this.nodeData = response.data
        this.is_loaded = true
        this.node_exists = true
        console.log('Node data:', this.nodeData)
        return this.nodeData // Return the NodeData or null
      } catch (error) {
        console.error('Error fetching node data:', error)
        this.nodeData = null
        this.is_loaded = false
        this.node_exists = false
        return null // Return null in case of an error
      }
    },
    async reloadCurrentNodeData(): Promise<NodeData | null> {
      return this.getNodeData(this.flow_id, this.node_id, false)
    },
    setFlowIdAndNodeId(flow_id: number, node_id: number) {
      console.log('setFlowIdAndNodeId called')
      if (this.node_id === node_id && this.flow_id === flow_id) {
        return
      }
      console.log('Automatically pushing the node data ')
      this.pushNodeData()
      this.previous_node_id = this.node_id
      this.flow_id = flow_id
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

    async updateSettings(inputData: any): Promise<any> {
      try {
        const node = this.vueFlowInstance?.findNode(String(this.node_id)) as Node
        inputData.value.pos_x = node.position.x
        inputData.value.pos_y = node.position.y
        const response = await axios.post('/update_settings/', inputData.value, {
          params: {
            node_type: node.data.component.__name,
          },
        }
        )
        const downstreamNodeIds = await loadDownstreamNodeIds(inputData.value.node_id)
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
      this.cacheNodeDescriptionDict(nodeId, description);
    }
  },
})
