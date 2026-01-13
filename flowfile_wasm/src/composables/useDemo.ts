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
const CSV_NODE_ID = 1 // Must match the read_csv node ID in sample-flow.yaml

// Track loading state
const isLoading = ref(false)
const loadError = ref<string | null>(null)

export function useDemo() {
  const flowStore = useFlowStore()

  /**
   * Check if the user has seen the demo before
   */
  const hasSeenDemo = computed(() => {
    return localStorage.getItem(DEMO_SHOWN_KEY) === 'true'
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
      // Fetch the YAML flow definition
      const flowResponse = await fetch('/demo/sample-flow.yaml')
      if (!flowResponse.ok) {
        throw new Error(`Failed to fetch flow definition: ${flowResponse.status}`)
      }
      const flowYaml = await flowResponse.text()

      // Fetch the sample CSV data
      const dataResponse = await fetch('/demo/sample-data.csv')
      if (!dataResponse.ok) {
        throw new Error(`Failed to fetch sample data: ${dataResponse.status}`)
      }
      const csvContent = await dataResponse.text()

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

      // Load the CSV content into the Read CSV node
      flowStore.setFileContent(CSV_NODE_ID, csvContent)

      // Infer schema from the CSV content
      const schema = inferSchemaFromCsv(csvContent, true, ',')
      if (schema) {
        flowStore.setSourceNodeSchema(CSV_NODE_ID, schema)
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
    isLoading,
    loadError,
    loadDemo,
    markDemoAsSeen,
    resetDemoState
  }
}
