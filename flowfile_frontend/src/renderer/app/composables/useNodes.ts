// composables/useNodes.ts
// Node templates composable for managing available nodes
import { ref, onMounted, DefineComponent, markRaw } from "vue"
import axios from "axios"
import type { NodeTemplate } from "../types"
import { ENV } from "../../config/environment"

// Utility function to convert snake_case to TitleCase
function toTitleCase(str: string): string {
  return str
    .split('_')
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join('')
}

// Cache for node templates - persists for the entire session
let nodeTemplatesCache: NodeTemplate[] | null = null
let cachePromise: Promise<NodeTemplate[]> | null = null

// Component cache to avoid re-importing
const componentCache: Map<string, Promise<DefineComponent>> = new Map()

/**
 * Fetches node templates with caching to minimize API calls
 * Returns cached data if available, otherwise fetches from API
 */
async function fetchNodeTemplates(): Promise<NodeTemplate[]> {
  // If we already have cached data, return it immediately
  if (nodeTemplatesCache !== null) {
    return nodeTemplatesCache
  }

  // If a fetch is already in progress, wait for it to complete
  // This prevents multiple simultaneous API calls
  if (cachePromise !== null) {
    return cachePromise
  }

  // Start a new fetch and cache the promise
  cachePromise = axios.get("/node_list")
    .then(response => {
      const allNodes = response.data as NodeTemplate[]
      // Apply production filter if needed
      nodeTemplatesCache = ENV.isProduction
        ? allNodes.filter(node => node.prod_ready)
        : allNodes
      return nodeTemplatesCache
    })
    .catch(error => {
      console.error("Failed to fetch node templates:", error)
      // Reset the promise on error so it can be retried
      cachePromise = null
      throw error
    })

  return cachePromise
}

/**
 * Clears the node templates cache
 * Useful for forcing a refresh if needed
 */
export function clearNodeTemplatesCache(): void {
  nodeTemplatesCache = null
  cachePromise = null
}

/**
 * Gets a specific node template by item name
 * Uses cached data when available
 */
export async function getNodeTemplateByItem(item: string): Promise<NodeTemplate | undefined> {
  try {
    const allNodes = await fetchNodeTemplates()
    return allNodes.find(node => node.item === item)
  } catch (error) {
    console.error(`Failed to get node template for item ${item}:`, error)
    return undefined
  }
}

/**
 * Gets all node templates matching the given items
 * Useful for bulk operations
 */
export async function getNodeTemplatesByItems(items: string[]): Promise<Map<string, NodeTemplate>> {
  try {
    const allNodes = await fetchNodeTemplates()
    const templateMap = new Map<string, NodeTemplate>()

    for (const node of allNodes) {
      if (items.includes(node.item)) {
        templateMap.set(node.item, node)
      }
    }

    return templateMap
  } catch (error) {
    console.error("Failed to get node templates by items:", error)
    return new Map()
  }
}

/**
 * Gets a Vue component for a node, with caching
 */
export async function getComponent(node: NodeTemplate | string): Promise<DefineComponent> {
  const nodeItem = typeof node === 'string' ? node : node.item

  // Check component cache first
  if (componentCache.has(nodeItem)) {
    return componentCache.get(nodeItem)!
  }

  // If we have a string, we need to fetch the NodeTemplate first
  const nodeTemplate = typeof node === 'string'
    ? await getNodeTemplateByItem(node)
    : node

  if (!nodeTemplate) {
    throw new Error(`Node template not found for item: ${nodeItem}`)
  }

  const formattedItemName = toTitleCase(nodeTemplate.item)
  console.log(`Loading component: ${formattedItemName}`)

  // Create and cache the component promise
  const componentPromise = import(`../features/designer/nodes/${formattedItemName}.vue`)
    .then(module => {
      const component = markRaw(module.default)
      return component
    })
    .catch(error => {
      console.error(`Failed to load component for ${formattedItemName}:`, error)
      // Remove from cache on error so it can be retried
      componentCache.delete(nodeItem)
      throw error
    })

  componentCache.set(nodeItem, componentPromise)
  return componentPromise
}

/**
 * Hook for using node templates in Vue components
 */
export const useNodes = () => {
  const nodes = ref<NodeTemplate[]>([])
  const loading = ref(false)
  const error = ref<Error | null>(null)

  const fetchNodes = async () => {
    loading.value = true
    error.value = null

    try {
      nodes.value = await fetchNodeTemplates()
    } catch (err) {
      error.value = err as Error
      nodes.value = []
    } finally {
      loading.value = false
    }
  }

  // Fetch on mount
  onMounted(fetchNodes)

  return {
    nodes,
    loading,
    error,
    refetch: fetchNodes,
    clearCache: () => {
      clearNodeTemplatesCache()
      return fetchNodes()
    }
  }
}

/**
 * Preloads all node templates and optionally their components
 * Useful for improving initial load performance
 */
export async function preloadNodeTemplates(loadComponents = false): Promise<void> {
  try {
    const templates = await fetchNodeTemplates()

    if (loadComponents) {
      // Preload all components in parallel
      const componentPromises = templates.map(template =>
        getComponent(template).catch(err =>
          console.warn(`Failed to preload component for ${template.item}:`, err)
        )
      )
      await Promise.all(componentPromises)
    }
  } catch (error) {
    console.error("Failed to preload node templates:", error)
  }
}
