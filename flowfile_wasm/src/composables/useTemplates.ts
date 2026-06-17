/**
 * Template loading composable.
 *
 * Loads a built-in template into the live flow: fetches the flow YAML, imports
 * it, then loads each read node's CSV — from the node's remote URL when it has
 * one (received_file.path, like the demo), otherwise from the template's local
 * dataDir. Designed to be wrapped by the tabs store's `openWith` so a template
 * opens in its own tab without disturbing other open flows.
 */

import { ref } from 'vue'
import yaml from 'js-yaml'
import { useFlowStore } from '../stores/flow-store'
import { inferSchemaFromCsv } from '../stores/schema-inference'
import type { FlowfileData, NodeReadSettings } from '../types'
import type { FlowTemplate } from '../config/templates'

const isLoading = ref(false)
const loadError = ref<string | null>(null)

/** Resolve an asset path against the app base, avoiding double slashes. */
function assetUrl(relativePath: string): string {
  const base = import.meta.env.BASE_URL || '/'
  return `${base.replace(/\/$/, '')}/${relativePath.replace(/^\//, '')}`
}

export function useTemplates() {
  const flowStore = useFlowStore()

  /**
   * Load a template into the current (live) flow. Replaces whatever is loaded,
   * so call this inside a fresh tab (e.g. via flowTabsStore.openWith).
   * @returns true on success.
   */
  async function loadTemplate(template: FlowTemplate): Promise<boolean> {
    isLoading.value = true
    loadError.value = null
    try {
      const flowResponse = await fetch(assetUrl(template.flowPath))
      if (!flowResponse.ok) {
        throw new Error(`Failed to fetch template flow: ${flowResponse.status}`)
      }
      const flowData = yaml.load(await flowResponse.text()) as FlowfileData
      if (!flowData || !flowData.nodes) {
        throw new Error('Invalid template flow definition')
      }

      const imported = flowStore.importFromFlowfile(flowData)
      if (!imported) {
        throw new Error('Failed to import template flow')
      }
      // Name the flow after the template so its tab/label reads nicely.
      flowStore.currentFlowName = template.name

      // Load every read node's CSV: URL-sourced nodes go through the standard
      // remote path, the rest come from the template's data directory.
      const remoteNodeIds: number[] = []
      for (const node of flowData.nodes) {
        if (node.type !== 'read') continue
        const settings = node.setting_input as NodeReadSettings | undefined
        const path = settings?.received_file?.path ?? ''
        if (path.startsWith('http://') || path.startsWith('https://')) {
          remoteNodeIds.push(node.id)
          continue
        }
        const fileName = settings?.file_name || settings?.received_file?.name
        if (!fileName) continue

        const csvResponse = await fetch(assetUrl(template.dataDir + fileName))
        if (!csvResponse.ok) {
          throw new Error(`Failed to fetch "${fileName}" for template: ${csvResponse.status}`)
        }
        const csv = await csvResponse.text()
        flowStore.setFileContent(node.id, csv)
        const schema = inferSchemaFromCsv(csv, true, ',')
        if (schema) {
          flowStore.setSourceNodeSchema(node.id, schema)
        }
      }

      if (remoteNodeIds.length) {
        const failures = await flowStore.refetchRemoteFiles(remoteNodeIds)
        if (failures.length) {
          throw new Error(
            `Failed to fetch template data: ${failures.map((f) => f.fileName).join(', ')}`
          )
        }
      }

      await flowStore.propagateSchemas()
      return true
    } catch (error) {
      console.error('[useTemplates] failed to load template:', error)
      loadError.value = error instanceof Error ? error.message : 'Unknown error'
      return false
    } finally {
      isLoading.value = false
    }
  }

  return { isLoading, loadError, loadTemplate }
}
