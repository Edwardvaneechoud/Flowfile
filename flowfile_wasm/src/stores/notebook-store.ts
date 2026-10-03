/**
 * The canvas notebook: the open flow rendered as `import flowfile as ff` cells.
 *
 * Rendering happens in the Pyodide engine (`engine/notebook_render.py`), from the
 * flow in flowfile_core's dialect, so the cells are the ones the full app shows
 * for the same flow. A render reads settings only; a node executes only when
 * `runCell` or `runAll` is called, which the Run buttons do and nothing else.
 *
 * A cell the user changed is a draft, kept in memory beside the rendered text.
 * A sync (`push`, and Run when there are drafts) has the engine read the drafts
 * (`engine/notebook_cells.py`), which never executes them, and lands what they
 * change as one undo step.
 */

import { defineStore } from 'pinia'
import { computed, reactive, ref } from 'vue'
import { useFlowStore } from './flow-store'
import { usePyodideStore } from './pyodide-store'
import { EXPR_TRANSFORMER_PACKAGE } from '../composables/useFormulaTranslation'
import { toCoreCompatibleFlow } from '../utils/coreExport'
import { codeFingerprint } from '../utils/notebookFingerprint'
import { isEmptyPatch, syncPatch, type NotebookSyncFailure, type NotebookSyncResult } from '../utils/notebookSync'
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

/** Why the last sync was refused, on the cell and line it happened. */
export interface CellSyncError {
  cellId: string
  line: number | null
  kind: NotebookSyncFailure['kind']
  message: string
  /** The text that was refused: the error stands while the cell still says it. */
  code: string
}

export type CellSyncState = 'edited' | 'synced' | 'failed'

/** What the notebook keeps for one open flow. In memory only: never saved, shared or persisted. */
interface FlowNotebookState {
  outputs: Record<string, CellOutput>
  /** Cells the user changed: cell id -> their text, while it differs from the rendered one. */
  drafts: Record<string, string>
  /** Cells the last sync pushed and that were not changed since. */
  synced: string[]
  syncError: CellSyncError | null
  notice: string | null
}

/** The whole Python source of a sync: what it reads arrives as data, never as part of this text. */
export const SYNC_SOURCE = `
import json
from engine.notebook_cells import sync_notebook
sync_notebook(**json.loads(_notebook_sync_request))
`
const SYNC_REQUEST = '_notebook_sync_request'

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
      state = reactive({ outputs: {}, drafts: {}, synced: [], syncError: null, notice: null })
      flowStates.set(key, state)
    }
    return state
  })
  const outputs = computed(() => flowState.value.outputs)
  const drafts = computed(() => flowState.value.drafts)
  const syncError = computed(() => flowState.value.syncError)
  const notice = computed(() => flowState.value.notice)
  const syncing = ref(false)
  const needsSync = computed(() => Object.keys(flowState.value.drafts).length > 0)
  const idle = computed(() => pyodideStore.isReady && !running.value && !syncing.value && !flowStore.isExecuting)
  const canRun = idle
  const canPush = computed(() => idle.value && needsSync.value)

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

  /** The flow a sync is read against: its nodes, wiring and locks. Schemas arriving do not make it another flow. */
  const structure = computed(
    () => `${codeFingerprint(flowStore.nodes, flowStore.edges)}|${flowStore.untrustedCodeNodes.map(node => node.nodeId).join(',')}`
  )

  /** What the engine reads of the flow: core's dialect, the known schemas, and locked nodes with no settings. */
  function engineArguments() {
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
    return { arguments: { flow, schemas, locked }, formulas }
  }

  /** Without the formula package every formula keeps its text form, which is still valid code. */
  const loadFormulaPackage = () => pyodideStore.ensurePyPackages([EXPR_TRANSFORMER_PACKAGE]).catch(() => undefined)

  /** Render the open flow as cells. A no-op before Pyodide is ready; never initializes it. */
  async function render(): Promise<void> {
    if (!pyodideStore.isReady) return
    const epoch = ++renderEpoch
    const rendered = liveFingerprint.value
    loading.value = true
    error.value = null
    try {
      const engine = engineArguments()
      const { flow, schemas, locked } = engine.arguments
      if (engine.formulas) await loadFormulaPackage()
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
      const state = flowState.value
      const shown = new Map(cells.value.map(cell => [cell.cell_id, cell.code]))
      for (const cellId of Object.keys(state.outputs)) {
        if (!shown.has(cellId)) delete state.outputs[cellId]
      }
      // A draft stands while its cell does and still differs from what the canvas says.
      for (const [cellId, draft] of Object.entries(state.drafts)) {
        if (shown.get(cellId) === undefined || shown.get(cellId) === draft) delete state.drafts[cellId]
      }
      if (state.syncError && state.drafts[state.syncError.cellId] !== state.syncError.code) state.syncError = null
      state.synced = state.synced.filter(cellId => shown.has(cellId))
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

  /** A cell the user may change: one that is code. Imports are derived and placeholders stay on the canvas. */
  function isEditable(cell: NotebookCell): boolean {
    return cell.kind === 'node' && cell.status === 'code'
  }

  /** The text a cell shows: the user's while it is changed, else the rendered one. */
  function cellCode(cell: NotebookCell): string {
    return flowState.value.drafts[cell.cell_id] ?? cell.code
  }

  function setCellCode(cellId: string, code: string): void {
    const cell = cells.value.find(each => each.cell_id === cellId)
    if (!cell || !isEditable(cell)) return
    const state = flowState.value
    if (code === cell.code) delete state.drafts[cellId]
    else state.drafts[cellId] = code
    state.synced = state.synced.filter(each => each !== cellId)
    if (state.syncError?.cellId === cellId && state.syncError.code !== code) state.syncError = null
  }

  function cellSyncState(cellId: string): CellSyncState | null {
    const state = flowState.value
    if (state.syncError?.cellId === cellId) return 'failed'
    if (cellId in state.drafts) return 'edited'
    return state.synced.includes(cellId) ? 'synced' : null
  }

  function dismissNotice(): void {
    flowState.value.notice = null
  }

  /**
   * Land the changed cells on the canvas as one undo step. The engine reads them and never
   * executes them; the request crosses the bridge as data. True when the canvas now says
   * what the cells say (also when nothing was changed).
   */
  async function sync(): Promise<boolean> {
    if (!pyodideStore.isReady || syncing.value) return false
    syncing.value = true
    const state = flowState.value
    try {
      // Render first: the drafts are read against what the canvas says now.
      await render()
      if (error.value) return false
      const sent = { ...state.drafts }
      if (Object.keys(sent).length === 0) return true
      const read = structure.value
      const engine = engineArguments()
      if (engine.formulas) await loadFormulaPackage()
      const request = { ...engine.arguments, drafts: sent }
      let result: NotebookSyncResult | NotebookSyncFailure
      pyodideStore.setGlobal(SYNC_REQUEST, JSON.stringify(request))
      try {
        result = (await pyodideStore.runPythonWithResult(SYNC_SOURCE)) as NotebookSyncResult | NotebookSyncFailure
      } finally {
        pyodideStore.deleteGlobal(SYNC_REQUEST)
      }
      if (flowState.value !== state || structure.value !== read) {
        state.notice = 'The canvas changed while the notebook was being read. Nothing was pushed; try again.'
        return false
      }
      if (!result.ok) {
        state.syncError = {
          cellId: result.cell_id,
          line: result.line ?? null,
          kind: result.kind,
          message: result.message,
          code: sent[result.cell_id] ?? ''
        }
        return false
      }
      const patch = syncPatch({ nodes: flowStore.nodes, edges: flowStore.edges }, result)
      if (!isEmptyPatch(patch)) flowStore.applyFlowPatch(patch)
      // A cell changed again while this ran keeps its newer text.
      for (const [cellId, code] of Object.entries(sent)) {
        if (state.drafts[cellId] === code) delete state.drafts[cellId]
      }
      state.synced = Object.keys(sent).filter(cellId => !(cellId in state.drafts))
      state.syncError = null
      state.notice = result.warnings?.length ? result.warnings.join('\n') : null
      await render()
      return true
    } catch (err) {
      state.notice = `The notebook could not be pushed: ${err instanceof Error ? err.message : String(err)}`
      return false
    } finally {
      syncing.value = false
    }
  }

  /** Push the changed cells to the canvas without running anything. */
  async function push(): Promise<boolean> {
    if (!canPush.value) return false
    return sync()
  }

  /** Run one cell: its last node with whatever upstream still has to run, then show its rows. */
  async function runCell(cellId: string): Promise<void> {
    if (!canRun.value) return
    if (needsSync.value && !(await sync())) return
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
    if (!canRun.value) return
    if (needsSync.value && !(await sync())) return
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
    drafts,
    syncError,
    notice,
    syncing,
    needsSync,
    canPush,
    render,
    lockReason,
    isEditable,
    cellCode,
    setCellCode,
    cellSyncState,
    dismissNotice,
    push,
    runCell,
    runAll
  }
})
