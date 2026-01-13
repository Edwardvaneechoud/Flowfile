/**
 * Demo Flow Composable
 *
 * Handles loading and managing the demo flow for first-time visitors.
 * The demo showcases a data transformation pipeline using sample sales data.
 */

import { computed, ref } from 'vue'
import { useFlowStore } from '../stores/flow-store'
import { inferSchemaFromCsv } from '../stores/schema-inference'
import yaml from 'js-yaml'
import type { FlowfileData } from '../types'

const DEMO_SHOWN_KEY = 'flowfile_demo_shown'
const DEMO_DISMISSED_KEY = 'flowfile_demo_dismissed'
const ORDERS_NODE_ID = 1 // Must match the read_csv node ID for orders in sample-flow.yaml
const REGIONS_NODE_ID = 2 // Must match the read_csv node ID for regions in sample-flow.yaml

// Track loading state
const isLoading = ref(false)
const loadError = ref<string | null>(null)

// Reactive state for dismissed (needs to be reactive for UI updates)
const isDismissed = ref(localStorage.getItem(DEMO_DISMISSED_KEY) === 'true')

export function useDemo() {
  const flowStore = useFlowStore()

  /**
   * Check if the user has seen the demo before
   */
  const hasSeenDemo = computed(() => {
    return localStorage.getItem(DEMO_SHOWN_KEY) === 'true'
  })

  /**
   * Check if the demo button has been dismissed
   */
  const hasDismissedDemo = computed(() => {
    return isDismissed.value
  })

  /**
   * Mark the demo as seen in localStorage
   */
  function markDemoAsSeen() {
    localStorage.setItem(DEMO_SHOWN_KEY, 'true')
  }

  /**
   * Reset the demo state (useful for testing)
   */
  function resetDemoState() {
    localStorage.removeItem(DEMO_SHOWN_KEY)
    localStorage.removeItem(DEMO_DISMISSED_KEY)
    isDismissed.value = false
  }

  /**
   * Dismiss the demo button (hide it permanently)
   */
  function dismissDemo() {
    localStorage.setItem(DEMO_DISMISSED_KEY, 'true')
    isDismissed.value = true
  }

  /**
   * Load the demo flow with sample data
   *
   * @param confirmReplace - If true, will prompt before replacing existing flow
   * @returns Promise<boolean> - True if demo was loaded successfully
   */
  async function loadDemo(confirmReplace: boolean = true): Promise<boolean> {
    // Check if there's an existing flow with nodes
    if (confirmReplace && flowStore.nodes.size > 0) {
      const confirmed = window.confirm(
        'This will replace your current flow. Do you want to continue?'
      )
      if (!confirmed) {
        return false
      }
    }

    isLoading.value = true
    loadError.value = null

    try {
      // Fetch all demo files in parallel
      const [flowResponse, ordersResponse, regionsResponse] = await Promise.all([
        fetch('/demo/sample-flow.yaml'),
        fetch('/demo/sales-data.csv'),
        fetch('/demo/regions.csv')
      ])

      if (!flowResponse.ok) {
        throw new Error(`Failed to fetch flow definition: ${flowResponse.status}`)
      }
      if (!ordersResponse.ok) {
        throw new Error(`Failed to fetch orders data: ${ordersResponse.status}`)
      }
      if (!regionsResponse.ok) {
        throw new Error(`Failed to fetch regions data: ${regionsResponse.status}`)
      }

      const [flowYaml, ordersContent, regionsContent] = await Promise.all([
        flowResponse.text(),
        ordersResponse.text(),
        regionsResponse.text()
      ])

      // Parse the YAML flow definition
      const flowData = yaml.load(flowYaml) as FlowfileData
      if (!flowData || !flowData.nodes) {
        throw new Error('Invalid flow definition')
      }

      // Import the flow definition (this clears existing flow)
      const imported = flowStore.importFromFlowfile(flowData)
      if (!imported) {
        throw new Error('Failed to import flow')
      }

      // Load the orders CSV content into the first Read CSV node
      flowStore.setFileContent(ORDERS_NODE_ID, ordersContent)
      const ordersSchema = inferSchemaFromCsv(ordersContent, true, ',')
      if (ordersSchema) {
        flowStore.setSourceNodeSchema(ORDERS_NODE_ID, ordersSchema)
      }

      // Load the regions CSV content into the second Read CSV node
      flowStore.setFileContent(REGIONS_NODE_ID, regionsContent)
      const regionsSchema = inferSchemaFromCsv(regionsContent, true, ',')
      if (regionsSchema) {
        flowStore.setSourceNodeSchema(REGIONS_NODE_ID, regionsSchema)
      }

      // Propagate schemas downstream
      await flowStore.propagateSchemas()

      // Mark demo as seen
      markDemoAsSeen()

      return true
    } catch (error) {
      console.error('Failed to load demo:', error)
      loadError.value = error instanceof Error ? error.message : 'Unknown error'
      return false
    } finally {
      isLoading.value = false
    }
  }

  return {
    hasSeenDemo,
    hasDismissedDemo,
    isLoading,
    loadError,
    loadDemo,
    markDemoAsSeen,
    resetDemoState,
    dismissDemo
  }
}
