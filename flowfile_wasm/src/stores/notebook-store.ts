/**
 * The canvas notebook: the open flow rendered as `import flowfile as ff` cells.
 *
 * Rendering happens in the Pyodide engine (`engine/notebook_render.py`), from the
 * flow in flowfile_core's dialect, so the cells are the ones the full app shows
 * for the same flow. A render reads settings only; a node executes only when
 * `runCell` or `runAll` is called, which the Run buttons do and nothing else.
 */

import { defineStore } from 'pinia'
import { computed, reactive, ref } from 'vue'
import { useFlowStore } from './flow-store'
import { usePyodideStore } from './pyodide-store'
import { EXPR_TRANSFORMER_PACKAGE } from '../composables/useFormulaTranslation'
import { toCoreCompatibleFlow } from '../utils/coreExport'
import { codeFingerprint } from '../utils/notebookFingerprint'
import { placeholderReason } from '../utils/placeholder'
import type { ColumnSchema, DataPreview, FlowNode, NodeResult } from '../types'

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

/** What a cell shows under its code after a Run. */
export type CellOutput =
  | { state: 'running' }
  | { state: 'rows'; columns: string[]; rows: Record<string, unknown>[]; total: number }
  | { state: 'error'; message: string }
  | { state: 'blocked'; message: string }

/** What the notebook keeps for one open flow. In memory only: never saved, shared or persisted. */
interface FlowNotebookState {
  outputs: Record<string, CellOutput>
}

/** Rows a cell shows; the canvas preview reads the same cache, so an explore node asks for what it asks for. */
export const CELL_ROW_LIMIT = 100
const previewRows = (type: string): number => (type === 'explore_data' ? 1000 : CELL_ROW_LIMIT)

function toRows(preview: DataPreview): Record<string, unknown>[] {
  return preview.data
    .slice(0, CELL_ROW_LIMIT)
    .map(row => Object.fromEntries(preview.columns.map((column, index) => [column, row[index]])))
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
  const running = ref(false)
  const flowStates = new WeakMap<object, FlowNotebookState>()

  /** This flow's notebook state. It follows the flow through tab switches and goes when the flow does. */
  const flowState = computed(() => {
    const key = flowStore.flowSessionKey
    let state = flowStates.get(key)
    if (!state) {
      state = reactive({ outputs: {} })
      flowStates.set(key, state)
    }
    return state
  })
  const outputs = computed(() => flowState.value.outputs)
  const canRun = computed(() => pyodideStore.isReady && !running.value && !flowStore.isExecuting)

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
      cells.value = rendering.cells ?? []
      warnings.value = rendering.warnings ?? []
      varByNode.value = rendering.var_by_node ?? {}
      fingerprint.value = rendered
      const shown = new Set(cells.value.map(cell => cell.cell_id))
      for (const cellId of Object.keys(outputs.value)) {
        if (!shown.has(cellId)) delete outputs.value[cellId]
      }
    } catch (err) {
      if (epoch !== renderEpoch) return
      error.value = err instanceof Error ? err.message : String(err)
    } finally {
      if (epoch === renderEpoch) loading.value = false
    }
  }

  /** The node a cell's Run shows: its last one. */
  function runTarget(cell: NotebookCell): number | null {
    if (cell.kind !== 'node' || cell.node_ids.length === 0) return null
    return cell.node_ids[cell.node_ids.length - 1]
  }

  /** What to show for a node that was just run: why it did not, its error, or its first rows. */
  async function outputFor(nodeId: number, result: NodeResult | undefined): Promise<CellOutput> {
    const node = flowStore.nodes.get(nodeId)
    if (!node) return { state: 'error', message: 'This node is no longer on the canvas.' }
    const blocked = flowStore.blockedNodes.get(nodeId) ?? result?.blocked
    if (blocked) return { state: 'blocked', message: blocked.message }
    if (!result?.success) return { state: 'error', message: result?.error ?? 'The node did not run.' }
    const preview = await flowStore.fetchNodePreview(nodeId, { maxRows: previewRows(node.type) })
    const data = preview.data as DataPreview | undefined
    if (!preview.success || !data) return { state: 'error', message: preview.error ?? 'Its rows could not be read.' }
    return { state: 'rows', columns: data.columns, rows: toRows(data), total: data.total_rows }
  }

  /** Runs under the busy flag and writes only to the flow that started the run. */
  async function withRun(body: (state: FlowNotebookState) => Promise<void>): Promise<void> {
    if (!canRun.value) return
    const state = flowState.value
    running.value = true
    try {
      await body(state)
    } finally {
      for (const [cellId, output] of Object.entries(state.outputs)) {
        if (output.state === 'running') delete state.outputs[cellId]
      }
      running.value = false
    }
  }

  /** Run one cell: its last node with whatever upstream still has to run, then show its rows. */
  async function runCell(cellId: string): Promise<void> {
    const cell = cells.value.find(each => each.cell_id === cellId)
    const nodeId = cell ? runTarget(cell) : null
    if (nodeId === null) return
    await withRun(async state => {
      state.outputs[cellId] = { state: 'running' }
      const blocked = flowStore.blockedNodes.has(nodeId)
      const result = blocked ? undefined : await flowStore.executeNodeWithUpstream(nodeId)
      const output = await outputFor(nodeId, result)
      if (flowState.value === state) state.outputs[cellId] = output
    })
  }

  /** Run the whole flow, as the canvas Run does, then show every cell's rows. */
  async function runAll(): Promise<void> {
    const targets = cells.value.flatMap(cell => {
      const nodeId = runTarget(cell)
      return nodeId === null ? [] : [{ cellId: cell.cell_id, nodeId }]
    })
    if (targets.length === 0) return
    await withRun(async state => {
      for (const { cellId } of targets) state.outputs[cellId] = { state: 'running' }
      await flowStore.executeFlow()
      for (const { cellId, nodeId } of targets) {
        if (flowState.value !== state) return
        state.outputs[cellId] = await outputFor(nodeId, flowStore.nodeResults.get(nodeId))
      }
    })
  }

  return {
    cells,
    warnings,
    varByNode,
    fingerprint,
    loading,
    error,
    stale,
    liveFingerprint,
    outputs,
    running,
    canRun,
    render,
    lockReason,
    runCell,
    runAll
  }
})
