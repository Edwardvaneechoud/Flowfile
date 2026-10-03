/**
 * The canvas notebook: the open flow rendered as `import flowfile as ff` cells.
 *
 * Rendering happens in the Pyodide engine (`engine/notebook_render.py`), from the
 * flow in flowfile_core's dialect, so the cells are the ones the full app shows
 * for the same flow. Nothing here executes a node: a render reads settings only.
 */

import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { useFlowStore } from './flow-store'
import { usePyodideStore } from './pyodide-store'
import { EXPR_TRANSFORMER_PACKAGE } from '../composables/useFormulaTranslation'
import { toCoreCompatibleFlow } from '../utils/coreExport'
import { codeFingerprint } from '../utils/notebookFingerprint'
import { placeholderReason } from '../utils/placeholder'
import type { ColumnSchema, FlowNode } from '../types'

export interface NotebookCell {
  cell_id: string
  node_ids: number[]
  kind: 'imports' | 'node'
  code: string
  defines: string[]
  uses: string[]
  status: 'code' | 'placeholder'
  reason: string | null
}

export interface NotebookRendering {
  cells: NotebookCell[]
  warnings: string[]
  var_by_node: Record<string, string>
}

/** Node types that only exist in this editor: the full app's code has no call that rebuilds them. */
const BROWSER_ONLY_TYPES: Record<string, string> = {
  external_data: 'reads a dataset its host page provides',
  external_output: 'hands its rows to the host page',
  read_from_catalog: "reads this browser's own catalog",
  write_to_catalog: "writes to this browser's own catalog"
}

/** Node types whose text is a formula, which the render translates with the formula package. */
const usesFormulas = (node: FlowNode): boolean =>
  node.type === 'formula' ||
  (node.type === 'filter' && (node.settings as any)?.filter_input?.mode === 'advanced')

const pythonJson = (value: unknown): string => JSON.stringify(JSON.stringify(value))

export const useNotebookStore = defineStore('notebook', () => {
  const flowStore = useFlowStore()
  const pyodideStore = usePyodideStore()

  const cells = ref<NotebookCell[]>([])
  const warnings = ref<string[]>([])
  const varByNode = ref<Record<string, string>>({})
  /** Fingerprint of the flow the cells were rendered from; null before the first render. */
  const fingerprint = ref<string | null>(null)
  const loading = ref(false)
  const error = ref<string | null>(null)
  let renderEpoch = 0

  /** Why the editor keeps a node out of code, or null. Such a node's settings never cross the bridge. */
  function lockReason(node: FlowNode): string | null {
    if (flowStore.isPlaceholderNode(node.id)) return placeholderReason(node.settings)
    if (flowStore.untrustedCodeNodes.some(locked => locked.nodeId === node.id)) {
      return 'locked until the shared flow is trusted'
    }
    return BROWSER_ONLY_TYPES[node.type] ?? null
  }

  /** Everything a render reads: the flow's code-relevant state, the trust lock and the known schemas. */
  const liveFingerprint = computed(() => {
    const schemas: string[] = []
    flowStore.nodeResults.forEach((result, id) => {
      if (result.schema?.length) schemas.push(`${id}:${result.schema.map(c => `${c.name}=${c.data_type}`).join(',')}`)
    })
    const locked = flowStore.untrustedCodeNodes.map(node => node.nodeId).join(',')
    return `${codeFingerprint(flowStore.nodes, flowStore.edges)}|${locked}|${schemas.sort().join(';')}`
  })

  const stale = computed(() => fingerprint.value !== liveFingerprint.value)

  /** Render the open flow as cells. A no-op before Pyodide is ready; never initializes it. */
  async function render(): Promise<void> {
    if (!pyodideStore.isReady) return
    const epoch = ++renderEpoch
    const rendered = liveFingerprint.value
    loading.value = true
    error.value = null
    try {
      const flow = JSON.parse(JSON.stringify(toCoreCompatibleFlow(flowStore.exportToFlowfile(flowStore.currentFlowName))))
      const locked: Record<number, string> = {}
      const schemas: Record<number, ColumnSchema[]> = {}
      let formulas = false
      for (const node of flowStore.nodes.values()) {
        const reason = lockReason(node)
        if (reason) locked[node.id] = reason
        else if (usesFormulas(node)) formulas = true
        const schema = flowStore.nodeResults.get(node.id)?.schema
        if (schema?.length) schemas[node.id] = schema
      }
      for (const node of flow.nodes) {
        if (locked[node.id]) node.setting_input = {}
      }
      if (formulas) {
        // Without the package every formula keeps its text form, which is still valid code.
        await pyodideStore.ensurePyPackages([EXPR_TRANSFORMER_PACKAGE]).catch(() => undefined)
      }
      const rendering = (await pyodideStore.runPythonWithResult(`
import json
from engine.notebook_render import render_notebook
render_notebook(json.loads(${pythonJson(flow)}), json.loads(${pythonJson(schemas)}), json.loads(${pythonJson(locked)}))
`)) as NotebookRendering
      // A newer render started while this one ran: its result is the one to show.
      if (epoch !== renderEpoch) return
      cells.value = rendering.cells
      warnings.value = rendering.warnings ?? []
      varByNode.value = rendering.var_by_node ?? {}
      fingerprint.value = rendered
    } catch (err) {
      if (epoch !== renderEpoch) return
      error.value = err instanceof Error ? err.message : String(err)
    } finally {
      if (epoch === renderEpoch) loading.value = false
    }
  }

  return { cells, warnings, varByNode, fingerprint, loading, error, stale, liveFingerprint, render, lockReason }
})
