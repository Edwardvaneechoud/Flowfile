/**
 * Demo Flow Composable
 *
 * Handles loading and managing the demo flow for first-time visitors.
 * The demo showcases a data transformation pipeline using sample sales data.
 */

import { computed, ref } from 'vue'
import { useFlowStore } from '../stores/flow-store'
import yaml from 'js-yaml'
import type { FlowfileData } from '../types'

const DEMO_SHOWN_KEY = 'flowfile_demo_shown'
const DEMO_DISMISSED_KEY = 'flowfile_demo_dismissed'

const isLoading = ref(false)
const loadError = ref<string | null>(null)

// Reactive state for dismissed (needs to be reactive for UI updates)
const isDismissed = ref(localStorage.getItem(DEMO_DISMISSED_KEY) === 'true')

/**
 * Check if the demo should be auto-loaded based on URL parameters or subdomain
 * Supports:
 * - demo.flowfile.org (subdomain)
 * - ?demo=true (query parameter)
 */
function shouldAutoLoadDemo(): boolean {
  const hostname = window.location.hostname
  const isOnDemoSubdomain = hostname.startsWith('demo.')

  const urlParams = new URLSearchParams(window.location.search)
  const hasDemoParam = urlParams.get('demo') === 'true'

  return isOnDemoSubdomain || hasDemoParam
}

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
      const flowResponse = await fetch('/demo/sample-flow.yaml')
      if (!flowResponse.ok) {
        throw new Error(`Failed to fetch flow definition: ${flowResponse.status}`)
      }

      const flowData = yaml.load(await flowResponse.text()) as FlowfileData
      if (!flowData || !flowData.nodes) {
        throw new Error('Invalid flow definition')
      }

      // Import the flow definition (this clears existing flow)
      const imported = flowStore.importFromFlowfile(flowData)
      if (!imported) {
        throw new Error('Failed to import flow')
      }

      // The demo's read nodes carry their data sources as URLs (GitHub raw),
      // same as any user flow — fetch them through the standard remote path.
      const failures = await flowStore.refetchRemoteFiles()
      if (failures.length) {
        throw new Error(`Failed to fetch demo data: ${failures.map((f) => f.fileName).join(', ')}`)
      }

      await flowStore.propagateSchemas()

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

  /**
   * Auto-load the demo if URL conditions are met
   * This is called on app startup and loads without confirmation
   *
   * @returns Promise<boolean> - True if demo was auto-loaded
   */
  async function autoLoadDemoIfNeeded(): Promise<boolean> {
    if (shouldAutoLoadDemo()) {
      // Load without confirmation since user explicitly navigated to demo URL
      return await loadDemo(false)
    }
    return false
  }

  return {
    hasSeenDemo,
    hasDismissedDemo,
    isLoading,
    loadError,
    loadDemo,
    markDemoAsSeen,
    resetDemoState,
    dismissDemo,
    shouldAutoLoadDemo,
    autoLoadDemoIfNeeded
  }
}
