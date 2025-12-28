// composables/useFlowExecution.ts
// Core flow execution composable for managing flow runs and polling
import { ref, onUnmounted, Ref } from "vue"
import axios from "axios"
import { ElNotification } from "element-plus"
import { useNodeStore } from "../stores/column-store"
import { useEditorStore } from "../stores/editor-store"
import { useResultsStore } from "../stores/results-store"
import { VueFlowStore } from "@vue-flow/core"
import type { RunInformation, FlowSettings } from "../types"
import { FlowApi } from "../api"

interface PollingConfig {
  interval?: number
  enabled?: boolean
  maxAttempts?: number
}

interface NotificationConfig {
  title: string
  message: string
  type: "success" | "error"
}

// Singleton state that persists across component instances
class FlowExecutionState {
  private static instance: FlowExecutionState
  private pollingIntervals: Map<string, number> = new Map()
  private activeExecutions: Map<string, boolean> = new Map()

  static getInstance(): FlowExecutionState {
    if (!FlowExecutionState.instance) {
      FlowExecutionState.instance = new FlowExecutionState()
    }
    return FlowExecutionState.instance
  }

  setPollingInterval(key: string, interval: number) {
    this.pollingIntervals.set(key, interval)
  }

  getPollingInterval(key: string): number | null {
    return this.pollingIntervals.get(key) || null
  }

  clearPollingInterval(key: string) {
    const interval = this.pollingIntervals.get(key)
    if (interval) {
      clearInterval(interval)
      this.pollingIntervals.delete(key)
    }
  }

  setExecutionState(key: string, state: boolean) {
    this.activeExecutions.set(key, state)
  }

  getExecutionState(key: string): boolean {
    return this.activeExecutions.get(key) || false
  }

  clearAll() {
    // Clear all intervals
    this.pollingIntervals.forEach((interval) => clearInterval(interval))
    this.pollingIntervals.clear()
    this.activeExecutions.clear()
  }
}

const isResponseSuccessful = (status: number): boolean =>
  status >= 200 && status < 300

const getRunStatus = async (flowId: number) => {
  const response = await axios.get("/flow/run_status/", {
    params: { flow_id: flowId },
    headers: { accept: "application/json" },
  })
  return response
}

const updateRunStatus = async (
  flowId: number,
  nodeStore: { insertRunResult: (result: RunInformation) => void }
) => {
  const response = await getRunStatus(flowId)
  if (isResponseSuccessful(response.status)) {
    nodeStore.insertRunResult(response.data)
  }

  return response
}

export function useFlowExecution(
  flowId: Ref<number> | number,
  pollingConfig: PollingConfig = {
    interval: 2000,
    enabled: true,
    maxAttempts: Infinity,
  },
  options: {
    persistPolling?: boolean // If true, polling continues even when component unmounts
    pollingKey?: string // Custom key for tracking this specific polling instance
  } = {}
) {
  const nodeStore = useNodeStore()
  const editorStore = useEditorStore()
  const resultsStore = useResultsStore()
  const state = FlowExecutionState.getInstance()
  const localPollingInterval = ref<number | null>(null)
  const isExecuting = ref(false)

  // Get the actual flow ID value
  const getFlowId = () => {
    return typeof flowId === 'number' ? flowId : flowId.value
  }

  // Generate a unique key for this flow's polling
  const getPollingKey = (suffix: string = '') => {
    const customKey = options.pollingKey || `flow_${getFlowId()}`
    return suffix ? `${customKey}_${suffix}` : customKey
  }

  // Flow control methods
  const freezeFlow = () => {
    const vueFlowElement: VueFlowStore = nodeStore.vueFlowInstance
    if (vueFlowElement) {
      vueFlowElement.nodesDraggable.value = false
      vueFlowElement.nodesConnectable.value = false
      vueFlowElement.elementsSelectable.value = false
    }
  }

  const unFreezeFlow = () => {
    const vueFlowElement: VueFlowStore = nodeStore.vueFlowInstance
    if (vueFlowElement) {
      vueFlowElement.nodesDraggable.value = true
      vueFlowElement.nodesConnectable.value = true
      vueFlowElement.elementsSelectable.value = true
    }
  }

  // Notification helper
  const showNotification = (
    title: string,
    message: string,
    type?: "success" | "error",
    dangerouslyUseHTMLString?: boolean
  ) => {
    ElNotification({
      title,
      message,
      type,
      position: "top-left",
      dangerouslyUseHTMLString,
    })
  }

  // Polling management with persistence option
  const startPolling = (checkFn: () => Promise<void>, pollingKeySuffix: string = '') => {
    const key = getPollingKey(pollingKeySuffix)

    if (options.persistPolling) {
      // Use global state for persistent polling
      const existingInterval = state.getPollingInterval(key)
      if (existingInterval === null && pollingConfig.enabled) {
        const interval = setInterval(checkFn, pollingConfig.interval || 2000) as unknown as number
        state.setPollingInterval(key, interval)
      }
    } else {
      // Use local polling that will be cleaned up on unmount
      if (localPollingInterval.value === null && pollingConfig.enabled) {
        localPollingInterval.value = setInterval(
          checkFn,
          pollingConfig.interval || 2000
        ) as unknown as number
      }
    }
  }

  const stopPolling = (pollingKeySuffix: string = '') => {
    if (options.persistPolling) {
      const key = getPollingKey(pollingKeySuffix)
      state.clearPollingInterval(key)
    } else {
      if (localPollingInterval.value !== null) {
        clearInterval(localPollingInterval.value)
        localPollingInterval.value = null
      }
    }
  }

  // Check if any polling is active for this flow
  const isPollingActive = (pollingKeySuffix: string = ''): boolean => {
    if (options.persistPolling) {
      const key = getPollingKey(pollingKeySuffix)
      return state.getPollingInterval(key) !== null
    }
    return localPollingInterval.value !== null
  }

  // Create notification config based on run information
  const createNotificationConfig = (runInfo: RunInformation): NotificationConfig => ({
    title: runInfo.success ? "Success" : "Error",
    message: runInfo.success
      ? "The operation has completed successfully"
      : "There were issues with the operation, check the logging for more information",
    type: runInfo.success ? "success" : "error",
  })

  // Check run status
  const checkRunStatus = async (customSuccessMessage?: string, pollingKeySuffix: string = '') => {
    try {
      const response = await updateRunStatus(getFlowId(), nodeStore)

      if (response.status === 200) {
        stopPolling(pollingKeySuffix)
        unFreezeFlow()
        editorStore.isRunning = false
        isExecuting.value = false
        state.setExecutionState(getPollingKey(pollingKeySuffix), false)

        // Update log viewer visibility after successful run
        editorStore.setShowFlowResult(true)
        editorStore.updateLogViewerVisibility(true)

        console.log("response data", response.data)
        const notificationConfig = createNotificationConfig(response.data)
        if (customSuccessMessage && response.data.success) {
          notificationConfig.message = customSuccessMessage
        }

        showNotification(
          notificationConfig.title,
          notificationConfig.message,
          notificationConfig.type
        )
      } else if (response.status === 404) {
        stopPolling(pollingKeySuffix)
        unFreezeFlow()
        editorStore.isRunning = false
        isExecuting.value = false
        state.setExecutionState(getPollingKey(pollingKeySuffix), false)
        resultsStore.resetRunResults()
      }
    } catch (error) {
      console.error("Error checking run status:", error)
      stopPolling(pollingKeySuffix)
      unFreezeFlow()
      editorStore.isRunning = false
      isExecuting.value = false
      state.setExecutionState(getPollingKey(pollingKeySuffix), false)
    }
  }

  // HTML escape helper
  const escapeHtml = (text: string): string => {
    const div = document.createElement("div")
    div.textContent = text
    return div.innerHTML
  }

  // Run entire flow
  const runFlow = async () => {
    const flowSettings: FlowSettings | null = await FlowApi.getFlowSettings(getFlowId())
    if (!flowSettings) {
      throw new Error("Failed to retrieve flow settings")
    }

    freezeFlow()
    nodeStore.resetNodeResult()
    isExecuting.value = true
    editorStore.isRunning = true
    editorStore.hideLogViewerForThisRun = false
    state.setExecutionState(getPollingKey(), true)

    const executionLocationText = flowSettings.execution_location === "local" ? "Local" : "Remote"
    const escapedFlowName = escapeHtml(flowSettings.name)

    const notificationMessage = `
      <div style="line-height: 1.4;">
        <div><strong>Flow:</strong> "${escapedFlowName}"</div>
        <div><strong>Mode:</strong> ${flowSettings.execution_mode}</div>
        <div><strong>Location:</strong> ${executionLocationText}</div>
      </div>
    `

    showNotification("🚀 Flow Started", notificationMessage, undefined, true)

    try {
      await axios.post("/flow/run/", null, {
        params: { flow_id: getFlowId() },
        headers: { accept: "application/json" },
      })
      nodeStore.showLogViewer()
      startPolling(() => checkRunStatus())
    } catch (error) {
      console.error("Error starting run:", error)
      unFreezeFlow()
      editorStore.isRunning = false
      isExecuting.value = false
      state.setExecutionState(getPollingKey(), false)
      showNotification("Error", "Failed to start the flow", "error")
    }
  }

  // Trigger fetch for a specific node
  const triggerNodeFetch = async (nodeId: number) => {
    const pollingKeySuffix = `node_${nodeId}`

    // Check if already fetching this node
    if (isPollingActive(pollingKeySuffix)) {
      console.log(`Node ${nodeId} fetch already in progress`)
      return
    }

    freezeFlow()
    nodeStore.resetNodeResult()
    isExecuting.value = true
    editorStore.isRunning = true
    state.setExecutionState(getPollingKey(pollingKeySuffix), true)

    showNotification(
      "📊 Fetching Node Data",
      `Starting data fetch for node ${nodeId}...`,
      undefined,
      false
    )

    try {
      await axios.post("/node/trigger_fetch_data", null, {
        params: {
          flow_id: getFlowId(),
          node_id: nodeId,
        },
        headers: { accept: "application/json" },
      })

      nodeStore.showLogViewer()
      startPolling(
        () => checkRunStatus("Node data has been fetched successfully", pollingKeySuffix),
        pollingKeySuffix
      )
    } catch (error: any) {
      console.error("Error triggering node fetch:", error)
      unFreezeFlow()
      editorStore.isRunning = false
      isExecuting.value = false
      state.setExecutionState(getPollingKey(pollingKeySuffix), false)

      const errorMessage = error.response?.data?.detail || "Failed to fetch node data"
      showNotification("Error", errorMessage, "error")
      throw error
    }
  }

  // Cancel flow execution
  const cancelFlow = async () => {
    try {
      await axios.post("/flow/cancel/", null, {
        params: { flow_id: getFlowId() },
        headers: { accept: "application/json" },
      })
      showNotification("Cancelling", "The operation is being cancelled")
      unFreezeFlow()
      editorStore.isRunning = false
      isExecuting.value = false

      // Stop all polling for this flow
      stopPolling()
      // Also stop any node-specific polling if using persistent polling
      if (options.persistPolling) {
        // Clear any node-specific polling
        for (let i = 0; i < 100; i++) { // Assuming max 100 nodes
          state.clearPollingInterval(getPollingKey(`node_${i}`))
        }
      }
    } catch (error) {
      console.error("Error cancelling run:", error)
      showNotification("Error", "Failed to cancel the operation", "error")
    }
  }

  // Cleanup on unmount - only clean up local polling
  onUnmounted(() => {
    if (!options.persistPolling && localPollingInterval.value !== null) {
      clearInterval(localPollingInterval.value)
      localPollingInterval.value = null
    }
  })

  return {
    // State
    isExecuting,

    // Methods
    runFlow,
    triggerNodeFetch,
    cancelFlow,
    showNotification,
    startPolling,
    stopPolling,
    checkRunStatus,
    isPollingActive,

    // Expose flow control if needed
    freezeFlow,
    unFreezeFlow,
  }
}
