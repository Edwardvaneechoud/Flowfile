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
import { editorNodeType, toCoreCompatibleFlow } from '../utils/coreExport'
import { codeFingerprint } from '../utils/notebookFingerprint'
import {
  isEmptyPatch,
  syncPatch,
  type NotebookSyncFailure,
  type NotebookSyncRemoval,
  type NotebookSyncResult
} from '../utils/notebookSync'
import { stableOrder } from '../utils/notebookLayout'
import { placeholderReason } from '../utils/placeholder'
import type { NotebookSurface } from '../components/notebook/notebookCompletions'
import type { SchemaColumn } from '../components/notebook/dataframeSchemaTypes'
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
  /** A cell the user added that the flow has no node for yet. */
  fresh?: boolean
  /** A changed cell whose steps the canvas no longer shows as one cell: kept as text, never pushed. */
  detached?: boolean
}

export interface NotebookRendering {
  cells: NotebookCell[]
  warnings: string[]
  var_by_node: Record<string, string>
}

/** What a cell shows under its code after a Run. */
export type CellOutput =
  | { state: 'running' }
  | {
      state: 'rows'
      nodeId: number
      columns: string[]
      dtypes: Record<string, string>
      rows: Record<string, unknown>[]
      total: number
    }
  | { state: 'error'; nodeId?: number; message: string }
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

/** `plain`: a cell a push read and found no step in, unchanged since. `detached`: a change the canvas outgrew. */
export type CellSyncState = 'new' | 'edited' | 'synced' | 'failed' | 'plain' | 'detached'

/** A cell the user wrote: the nodes each of its lines holds, and the node whose cell it stands after. */
interface WrittenCell {
  lines: number[][]
  after: number | null
}

/** A cell the user added, until a push gives it nodes. */
interface NewCell {
  id: string
  code: string
  /** The cell it was added after; with that cell gone it goes to the end. */
  after: string | null
  /** The text a push read and found no step in: nothing in it names a frame, so it has nothing to push. */
  read?: string
}

/** A changed cell a re-render left without a cell to stand in: its text, and the cell it stood after. */
interface DetachedCell {
  id: string
  code: string
  after: string | null
  /** The cell it was a change of: when that cell is back (an undo), the text goes back to it. */
  origin: string
}

/** What the notebook keeps for one open flow. In memory only: never saved, shared or persisted. */
interface FlowNotebookState {
  /** The last render of this flow: its cells, warnings and names, and the fingerprint it was made from. */
  cells: NotebookCell[]
  warnings: string[]
  varByNode: Record<string, string>
  fingerprint: string | null
  error: string | null
  /** The cell ids in the order the notebook showed them before the last render; a render keeps that order where it can. */
  order: string[]
  /** Which nodes each cell held after the last sync read it: a cell keeps its place under a new id. */
  lastRead: Record<string, number[][]>
  detached: DetachedCell[]
  outputs: Record<string, CellOutput>
  /** Cells the user changed: cell id -> their text, while it differs from the rendered one. */
  drafts: Record<string, string>
  /** Cells the last sync pushed and that were not changed since. */
  synced: string[]
  syncError: CellSyncError | null
  notice: string | null
  /** The cells the user wrote: their nodes stay together, in the place they were written. */
  layout: WrittenCell[]
  newCells: NewCell[]
  newCellCount: number
  /** The nodes the last push put on the canvas, and a count that moves with every such push. */
  added: { nodes: number[]; pushes: number }
}

/** The whole Python source of a sync: what it reads arrives as data, never as part of this text. */
export const SYNC_SOURCE = `
import json
from engine.notebook_cells import sync_notebook
sync_notebook(**json.loads(_notebook_sync_request))
`
const SYNC_REQUEST = '_notebook_sync_request'
/** The whole Python source of a render: the flow it renders arrives as data. */
export const RENDER_SOURCE = `
import json
from engine.notebook_render import render_notebook
render_notebook(**json.loads(_notebook_render_request))
`
const RENDER_REQUEST = '_notebook_render_request'
/** A check reads the changed cells as a push does and lands nothing; it has a global of its own. */
export const CHECK_SOURCE = `
import json
from engine.notebook_cells import sync_notebook
sync_notebook(**json.loads(_notebook_check_request))
`
const CHECK_REQUEST = '_notebook_check_request'
/** How long typing pauses before the changed cells are checked. */
export const CHECK_DELAY_MS = 400
const SURFACE_SOURCE = `
from engine.notebook_cells import notebook_surface
notebook_surface()
`

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


export const useNotebookStore = defineStore('notebook', () => {
  const flowStore = useFlowStore()
  const pyodideStore = usePyodideStore()

  const loading = ref(false)
  let renderEpoch = 0
  const running = ref(false)
  /** What a cell may write, read once from the engine for completions; null until then. */
  const surface = ref<NotebookSurface | null>(null)
  const flowStates = new WeakMap<object, FlowNotebookState>()
  let engineQueue: Promise<unknown> = Promise.resolve()

  /**
   * One engine call: `request` set as the global `name` for `source` to read, then taken away. Calls run one
   * after another, because Python starts a call on a later task and two calls must not share or drop a global.
   */
  function callEngine<T>(source: string, name: string, request: unknown): Promise<T> {
    const call = async () => {
      pyodideStore.setGlobal(name, JSON.stringify(request))
      try {
        return (await pyodideStore.runPythonWithResult(source)) as T
      } finally {
        pyodideStore.deleteGlobal(name)
      }
    }
    const result = engineQueue.then(call, call)
    engineQueue = result.catch(() => undefined)
    return result
  }

  /** This flow's notebook state. It follows the flow through tab switches and goes when the flow does. */
  const flowState = computed(() => {
    const key = flowStore.flowSessionKey
    let state = flowStates.get(key)
    if (!state) {
      state = reactive({
        cells: [],
        warnings: [],
        varByNode: {},
        fingerprint: null,
        error: null,
        order: [],
        lastRead: {},
        detached: [],
        outputs: {},
        drafts: {},
        synced: [],
        syncError: null,
        notice: null,
        layout: [],
        newCells: [],
        newCellCount: 0,
        added: { nodes: [], pushes: 0 }
      })
      flowStates.set(key, state)
    }
    return state
  })
  const cells = computed(() => flowState.value.cells)
  const warnings = computed(() => flowState.value.warnings)
  const varByNode = computed(() => flowState.value.varByNode)
  /** Fingerprint of the flow the cells were rendered from; null before the first render. */
  const fingerprint = computed(() => flowState.value.fingerprint)
  const error = computed(() => flowState.value.error)
  const outputs = computed(() => flowState.value.outputs)
  const drafts = computed(() => flowState.value.drafts)
  const syncError = computed(() => flowState.value.syncError)
  const notice = computed(() => flowState.value.notice)
  const added = computed(() => flowState.value.added)
  const syncing = ref(false)
  /** The first node of a cell the last sync read, or undefined. */
  const firstRead = (cellId: string): number | undefined => flowState.value.lastRead[cellId]?.[0]?.[0]

  /**
   * The cells as the notebook shows them: the rendered ones, a written cell standing after the
   * cell it was written after (while every name it uses is still defined above it), and the
   * cells the user added that have no node yet.
   */
  const shownCells = computed<NotebookCell[]>(() => {
    const state = flowState.value
    const ordered = [...cells.value]
    for (const written of state.layout) {
      const after = written.after
      const from = ordered.findIndex(cell => cell.node_ids.includes(written.lines[0][0]))
      if (from < 0 || (after !== null && ordered[from].node_ids.includes(after))) continue
      const [cell] = ordered.splice(from, 1)
      // Written above every step, a cell stands right under the imports.
      const anchor = ordered.findIndex(each => (after === null ? each.kind === 'imports' : each.node_ids.includes(after)))
      const above = new Set(ordered.slice(0, anchor + 1).flatMap(each => each.defines))
      const fits = (anchor >= 0 || after === null) && cell.uses.every(name => above.has(name))
      ordered.splice(fits ? anchor + 1 : from, 0, cell)
    }
    const unrendered = [
      ...state.newCells.map(fresh => ({ id: fresh.id, after: fresh.after, extra: { fresh: true } })),
      ...state.detached.map(gone => ({ id: gone.id, after: gone.after, extra: { detached: true } }))
    ]
    for (const { id, after, extra } of unrendered) {
      const anchor = ordered.findIndex(cell => cell.cell_id === after)
      const cell: NotebookCell = {
        cell_id: id,
        node_ids: [],
        kind: 'node',
        code: '',
        defines: [],
        uses: [],
        status: 'code',
        reason: null,
        ...extra
      }
      if (anchor < 0) ordered.push(cell)
      else ordered.splice(anchor + 1, 0, cell)
    }
    return stableOrder(state.order, ordered)
  })

  const writtenNewCells = computed(() => flowState.value.newCells.filter(cell => cell.code.trim() !== ''))
  /** A new cell is plain once a push read it and found no step in it, for as long as it says the same. */
  const isPlain = (cell: NewCell): boolean => cell.code.trim() !== '' && cell.code === cell.read
  const changedCount = computed(
    () => Object.keys(flowState.value.drafts).length + writtenNewCells.value.filter(cell => !isPlain(cell)).length
  )
  const needsSync = computed(() => changedCount.value > 0)
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

  /** A written cell without some nodes, and without the lines that leaves empty. */
  const without = (written: WrittenCell, gone: (id: number) => boolean): WrittenCell => ({
    ...written,
    lines: written.lines.map(line => line.filter(id => !gone(id))).filter(line => line.length > 0)
  })

  /** The written cells as the engine takes them: node ids per line per cell, without nodes that are gone. */
  function writtenLayout(): number[][][] {
    const state = flowState.value
    state.layout = state.layout
      .map(written => without(written, id => !flowStore.nodes.has(id)))
      .filter(written => written.lines.length > 0)
    return state.layout.map(written => written.lines)
  }

  /** What a cell says now, when the user changed or added it. */
  function currentText(cellId: string): string | undefined {
    const state = flowState.value
    return (
      state.newCells.find(cell => cell.id === cellId)?.code ??
      state.detached.find(cell => cell.id === cellId)?.code ??
      state.drafts[cellId]
    )
  }

  /** Render the open flow as cells. A no-op before Pyodide is ready; never initializes it. */
  async function render(): Promise<void> {
    if (!pyodideStore.isReady) return
    const epoch = ++renderEpoch
    const rendered = liveFingerprint.value
    const state = flowState.value
    loading.value = true
    state.error = null
    try {
      const engine = engineArguments()
      const { flow, schemas, locked } = engine.arguments
      if (engine.formulas) await loadFormulaPackage()
      const rendering = await callEngine<NotebookRendering>(RENDER_SOURCE, RENDER_REQUEST, {
        flow,
        schemas,
        locked,
        layout: writtenLayout()
      })
      // A newer render started while this one ran: its result is the one to show.
      if (epoch !== renderEpoch) return
      const before = state.cells
      if (flowState.value === state) state.order = shownCells.value.map(cell => cell.cell_id)
      state.cells = rendering.cells ?? []
      state.warnings = rendering.warnings ?? []
      state.varByNode = rendering.var_by_node ?? {}
      state.fingerprint = rendered
      const shown = new Map(state.cells.map(cell => [cell.cell_id, cell.code]))
      for (const cellId of Object.keys(state.outputs)) {
        if (!shown.has(cellId)) delete state.outputs[cellId]
      }
      keepDrafts(state, before)
      if (state.syncError && currentText(state.syncError.cellId) !== state.syncError.code) state.syncError = null
      state.synced = state.synced.filter(cellId => shown.has(cellId))
      if (Object.keys(state.drafts).length || writtenNewCells.value.length) scheduleCheck()
    } catch (err) {
      if (epoch !== renderEpoch) return
      state.error = err instanceof Error ? err.message : String(err)
    } finally {
      if (epoch === renderEpoch) loading.value = false
    }
    if (!surface.value && !state.error) await loadSurface()
  }

  /**
   * A draft stands while its cell does and still differs from what the canvas says. When its cell is
   * gone, it moves to the one cell that now holds all of its nodes; failing that it is kept as a detached
   * cell, shown where it stood and never pushed, so a canvas change never loses what the user typed.
   */
  function keepDrafts(state: FlowNotebookState, before: NotebookCell[]): void {
    const now = new Map(state.cells.map(cell => [cell.cell_id, cell]))
    for (const gone of [...state.detached]) {
      const cell = now.get(gone.origin)
      if (!cell || gone.origin in state.drafts) continue
      state.detached = state.detached.filter(each => each !== gone)
      if (cell.code !== gone.code) state.drafts[gone.origin] = gone.code
    }
    for (const [cellId, draft] of Object.entries(state.drafts)) {
      const cell = now.get(cellId)
      if (cell) {
        if (cell.code === draft) delete state.drafts[cellId]
        continue
      }
      delete state.drafts[cellId]
      const nodes = before.find(each => each.cell_id === cellId)?.node_ids ?? []
      const home = nodes.length
        ? state.cells.find(each => isEditable(each) && nodes.every(id => each.node_ids.includes(id)))
        : undefined
      if (home && !(home.cell_id in state.drafts)) {
        if (home.code !== draft) state.drafts[home.cell_id] = draft
        continue
      }
      const at = state.order.indexOf(cellId)
      const after = [...state.order.slice(0, Math.max(at, 0))].reverse().find(id => now.has(id)) ?? null
      state.detached.push({ id: `detached-${++state.newCellCount}`, code: draft, after, origin: cellId })
      state.notice =
        'The canvas changed under a cell you were editing, so its text no longer matches a step. ' +
        'It is kept as a detached cell: copy what you need, or use it as a new cell.'
    }
  }

  /** The engine's dialect, once a render has the engine loaded. Without it a cell simply offers no completions. */
  async function loadSurface(): Promise<void> {
    try {
      const answer = (await pyodideStore.runPythonWithResult(SURFACE_SOURCE)) as NotebookSurface | null
      surface.value =
        Array.isArray(answer?.ff) && answer.methods && Array.isArray(answer.pushable) && Array.isArray(answer.node_types)
          ? answer
          : null
    } catch {
      surface.value = null
    }
  }

  const columnsOf = (nodeId: number): SchemaColumn[] =>
    (flowStore.nodeResults.get(nodeId)?.schema ?? []).map(column => ({ name: column.name, dtype: column.data_type }))

  /** The node each name the render gave holds. */
  const nodeByName = computed(() => new Map(Object.entries(varByNode.value).map(([id, name]) => [name, Number(id)])))

  /** The columns the canvas knows for the frame a name holds, or null when the name holds none. */
  function frameColumns(name: string): SchemaColumn[] | null {
    const nodeId = nodeByName.value.get(name)
    if (nodeId === undefined) return null
    const found = columnsOf(nodeId)
    return found.length ? found : null
  }

  /** The columns of a cell's steps and of what they read, each name once: what a string in it most likely names. */
  function cellColumns(cellId: string): SchemaColumn[] {
    const cell = shownCells.value.find(each => each.cell_id === cellId)
    const seen = new Map<string, SchemaColumn>()
    for (const nodeId of cell?.node_ids ?? []) {
      const node = flowStore.nodes.get(nodeId)
      const inputs = node ? [...node.inputIds, ...(node.rightInputId === undefined ? [] : [node.rightInputId])] : []
      for (const column of [...inputs.flatMap(columnsOf), ...columnsOf(nodeId)]) {
        if (!seen.has(column.name)) seen.set(column.name, column)
      }
    }
    return [...seen.values()]
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
    if (!result?.success) return { state: 'error', nodeId, message: result?.error ?? 'The node did not run.' }
    const preview = await flowStore.fetchNodePreview(nodeId, { maxRows: previewRows(node.type) })
    const data = preview.data as DataPreview | undefined
    if (!preview.success || !data) return { state: 'error', nodeId, message: preview.error ?? 'Its rows could not be read.' }
    const schema = flowStore.nodeResults.get(nodeId)?.schema ?? []
    const dtypes = Object.fromEntries(schema.map(column => [column.name, column.data_type]))
    return { state: 'rows', nodeId, columns: data.columns, dtypes, rows: toRows(data), total: data.total_rows }
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
    return cell.kind === 'node' && cell.status === 'code' && !cell.detached && !canvasOnly(cell)
  }

  /** The node types (this editor's names) a push can change, once the engine said which. */
  const changeableTypes = computed(() =>
    surface.value ? new Set(surface.value.node_types.map(type => editorNodeType(type))) : null
  )

  /** A cell of steps no push can change yet (a read, a join, Polars code): it stays as the render wrote it. */
  function canvasOnly(cell: NotebookCell): boolean {
    const types = changeableTypes.value
    if (!types || cell.kind !== 'node' || cell.status !== 'code' || cell.fresh || cell.detached) return false
    if (cell.node_ids.length === 0 || cell.cell_id in flowState.value.drafts) return false
    return cell.node_ids.every(id => {
      const type = flowStore.nodes.get(id)?.type
      return type !== undefined && !types.has(type)
    })
  }

  /** The text a cell shows: the user's while it is changed, else the rendered one. */
  function cellCode(cell: NotebookCell): string {
    return currentText(cell.cell_id) ?? cell.code
  }

  function setCellCode(cellId: string, code: string): void {
    const state = flowState.value
    const fresh = state.newCells.find(each => each.id === cellId)
    const cell = cells.value.find(each => each.cell_id === cellId)
    if (fresh) fresh.code = code
    else if (!cell || !isEditable(cell)) return
    else if (code === cell.code) delete state.drafts[cellId]
    else state.drafts[cellId] = code
    state.synced = state.synced.filter(each => each !== cellId)
    if (state.syncError?.cellId === cellId && state.syncError.code !== code) state.syncError = null
    scheduleCheck()
  }

  let checkTimer: ReturnType<typeof setTimeout> | null = null
  let checkSeq = 0

  function scheduleCheck(): void {
    if (checkTimer) clearTimeout(checkTimer)
    checkTimer = setTimeout(() => {
      checkTimer = null
      void check()
    }, CHECK_DELAY_MS)
  }

  /**
   * Read the changed cells the way a push does, without landing anything, so a cell a push would refuse
   * shows why on its line while it is being written. The request crosses as data, as a sync's does.
   */
  async function check(): Promise<void> {
    if (!pyodideStore.isReady || syncing.value || running.value || flowStore.isExecuting || stale.value) return
    const state = flowState.value
    const sent = { ...state.drafts }
    const fresh = Object.fromEntries(writtenNewCells.value.map(cell => [cell.id, cell.code]))
    if (Object.keys(sent).length + Object.keys(fresh).length === 0) return
    const seq = ++checkSeq
    const read = structure.value
    const engine = engineArguments()
    if (engine.formulas) await loadFormulaPackage()
    const request = {
      ...engine.arguments,
      drafts: sent,
      next_id: flowStore.nextNodeId,
      layout: writtenLayout(),
      order: shownCells.value.map(cell => cell.cell_id),
      new_cells: fresh
    }
    let result: NotebookSyncResult | NotebookSyncFailure
    try {
      result = await callEngine<NotebookSyncResult | NotebookSyncFailure>(CHECK_SOURCE, CHECK_REQUEST, request)
    } catch {
      return
    }
    // Typing, a push or a canvas change since the check began: its answer is about cells that are gone.
    if (seq !== checkSeq || syncing.value || flowState.value !== state || structure.value !== read) return
    if (result.ok) return
    const code = sent[result.cell_id] ?? fresh[result.cell_id]
    if (code === undefined || currentText(result.cell_id) !== code) return
    state.syncError = { cellId: result.cell_id, line: result.line ?? null, kind: result.kind, message: result.message, code }
  }

  function cellSyncState(cellId: string): CellSyncState | null {
    const state = flowState.value
    if (state.syncError?.cellId === cellId) return 'failed'
    if (state.detached.some(cell => cell.id === cellId)) return 'detached'
    const fresh = state.newCells.find(cell => cell.id === cellId)
    if (fresh) return isPlain(fresh) ? 'plain' : 'new'
    if (cellId in state.drafts) return 'edited'
    return state.synced.includes(cellId) ? 'synced' : null
  }

  /** Add an empty cell after `afterCellId`, or at the end. A push turns what is written in it into nodes. */
  function addCell(afterCellId: string | null = null): string {
    const state = flowState.value
    const shown = shownCells.value
    const id = `new-${++state.newCellCount}`
    state.newCells.push({ id, code: '', after: afterCellId ?? shown[shown.length - 1]?.cell_id ?? null })
    return id
  }

  /** Drop a cell the user added and did not push. */
  function discardCell(cellId: string): void {
    const state = flowState.value
    if (state.detached.some(cell => cell.id === cellId)) {
      state.detached = state.detached.filter(cell => cell.id !== cellId)
      return
    }
    const gone = state.newCells.find(cell => cell.id === cellId)
    if (!gone) return
    state.newCells = state.newCells.filter(cell => cell !== gone)
    for (const cell of state.newCells) if (cell.after === cellId) cell.after = gone.after
    if (state.syncError?.cellId === cellId) state.syncError = null
  }

  /** Make a detached cell a new cell: a push then reads every call in it as a new step. */
  function adoptDetached(cellId: string): void {
    const state = flowState.value
    const cell = state.detached.find(each => each.id === cellId)
    if (!cell) return
    state.detached = state.detached.filter(each => each !== cell)
    state.newCells.push({ id: `new-${++state.newCellCount}`, code: cell.code, after: cell.after })
  }

  /** An output whose step, or a step before it, changed since it ran: what it shows may no longer hold. */
  function outputStale(cellId: string): boolean {
    const output = flowState.value.outputs[cellId]
    return !!output && 'nodeId' in output && output.nodeId !== undefined && flowStore.isNodeDirty(output.nodeId)
  }

  function clearOutputs(): void {
    flowState.value.outputs = {}
  }

  function dismissNotice(): void {
    flowState.value.notice = null
  }

  /**
   * Land the changed cells on the canvas as one undo step. The engine reads them and never
   * executes them; the request crosses the bridge as data. A step a changed cell no longer
   * writes is removed once the user agrees. True when the canvas now says what the cells say
   * (also when nothing was changed).
   */
  async function sync(upTo?: string): Promise<boolean> {
    if (!pyodideStore.isReady || syncing.value) return false
    syncing.value = true
    checkSeq++
    const state = flowState.value
    try {
      // The drafts are read against what the canvas says now: render first unless the cells already show it.
      if (stale.value || state.fingerprint === null) await render()
      if (state.error) return false
      // Cells are read in order, so a cell below `upTo` cannot change it: Run leaves those for later.
      const shownIds = shownCells.value.map(cell => cell.cell_id)
      const limit = upTo === undefined ? shownIds.length : shownIds.indexOf(upTo)
      const inReach = (cellId: string) => limit < 0 || shownIds.indexOf(cellId) <= limit
      const sent = Object.fromEntries(Object.entries(state.drafts).filter(([cellId]) => inReach(cellId)))
      const fresh = Object.fromEntries(
        writtenNewCells.value.filter(cell => inReach(cell.id)).map(cell => [cell.id, cell.code])
      )
      state.lastRead = {}
      if (Object.keys(sent).length + Object.keys(fresh).length === 0) return true
      const read = structure.value
      const shown = shownCells.value
      const engine = engineArguments()
      if (engine.formulas) await loadFormulaPackage()
      const request = {
        ...engine.arguments,
        drafts: sent,
        next_id: flowStore.nextNodeId,
        layout: writtenLayout(),
        order: shown.map(cell => cell.cell_id),
        new_cells: fresh
      }
      const result = await callEngine<NotebookSyncResult | NotebookSyncFailure>(SYNC_SOURCE, SYNC_REQUEST, request)
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
          code: sent[result.cell_id] ?? fresh[result.cell_id] ?? ''
        }
        return false
      }
      if (result.removed?.length && !confirmRemoval(result.removed)) return false
      const patch = syncPatch({ nodes: flowStore.nodes, edges: flowStore.edges }, result, flowStore.defaultSettings)
      if (!isEmptyPatch(patch)) flowStore.applyFlowPatch(patch)
      // A cell changed again while this ran keeps its newer text.
      for (const [cellId, code] of Object.entries(sent)) {
        if (state.drafts[cellId] === code) delete state.drafts[cellId]
      }
      state.lastRead = result.node_ids_by_cell ?? {}
      keepWritten(state, shown, state.lastRead, fresh, result.unnamed_by_cell ?? {})
      state.syncError = null
      state.notice = result.warnings?.length ? result.warnings.join('\n') : null
      if (result.added?.length) state.added = { nodes: result.added.map(node => node.id), pushes: state.added.pushes + 1 }
      await render()
      state.synced = [...Object.keys(sent), ...Object.keys(fresh)]
        .map(cellId => cells.value.find(cell => cell.node_ids.includes(firstRead(cellId)!))?.cell_id ?? cellId)
        .filter(cellId => !(cellId in state.drafts))
      return true
    } catch (err) {
      state.notice = `The notebook could not be pushed: ${err instanceof Error ? err.message : String(err)}`
      return false
    } finally {
      syncing.value = false
    }
  }

  /** Ask before a push takes steps off the canvas, as the full app's push does. */
  function confirmRemoval(removed: NotebookSyncRemoval[]): boolean {
    const one = removed.length === 1
    const message =
      `${removed.map(step => step.label).join(', ')} ${one ? 'is' : 'are'} no longer in the notebook. ` +
      `Remove ${one ? 'it' : 'them'} from the canvas? Undo brings ${one ? 'it' : 'them'} back.`
    return typeof window !== 'undefined' && typeof window.confirm === 'function' && window.confirm(message)
  }

  /**
   * After a push, each cell that was read is a written cell: its nodes stay together and it stays
   * after the cell it stood after. A new cell that got nodes is one of them from here on; one that
   * got none is a plain cell, and lines that named no frame stay as a plain cell under their cell.
   */
  function keepWritten(
    state: FlowNotebookState,
    shown: NotebookCell[],
    read: Record<string, number[][]>,
    fresh: Record<string, string>,
    unnamed: Record<string, string[]>
  ): void {
    const nodesOf = (cell: NotebookCell): number[] => read[cell.cell_id]?.flat() ?? cell.node_ids
    for (const [cellId, lines] of Object.entries(read)) {
      const nodes = lines.flat()
      if (nodes.length === 0) continue
      const index = shown.findIndex(cell => cell.cell_id === cellId)
      const before = shown.slice(0, Math.max(index, 0)).reverse().find(cell => nodesOf(cell).length > 0)
      const after = before ? nodesOf(before)[nodesOf(before).length - 1] : null
      state.layout = state.layout
        .map(written => without(written, id => nodes.includes(id)))
        .filter(written => written.lines.length > 0)
      state.layout.push({ lines: lines.map(line => [...line]), after })
    }
    // A new cell that got nodes is rendered from now on; one added after it follows the cell it became.
    const first = (cellId: string | null) => (cellId ? read[cellId]?.flat()[0] : undefined)
    const became = (cellId: string | null) => (first(cellId) === undefined ? cellId : `cell-${first(cellId)}`)
    const kept: NewCell[] = []
    for (const cell of state.newCells) {
      const sent = fresh[cell.id]
      if (sent === undefined) kept.push(cell)
      else if (first(cell.id) === undefined) kept.push({ ...cell, read: sent })
      // Typed while the push ran: a change to the cell it became, never a second cell that adds its nodes again.
      else if (cell.code !== sent) state.drafts[became(cell.id)!] = cell.code
    }
    state.newCells = kept.map(cell => ({ ...cell, after: became(cell.after) }))
    for (const [cellId, lines] of Object.entries(unnamed)) {
      if (first(cellId) === undefined || lines.length === 0) continue
      const code = lines.join('\n')
      state.newCells.push({ id: `new-${++state.newCellCount}`, code, after: became(cellId), read: code })
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
    if (needsSync.value) {
      const state = flowState.value
      if (!(await sync(cellId))) {
        if (state.syncError && state.syncError.cellId !== cellId) {
          state.outputs[cellId] = {
            state: 'blocked',
            message: 'a changed cell above could not be pushed. Fix or revert it (it is marked), then run again.'
          }
        }
        return
      }
      // The cell may show under another id now: it is the one that holds the nodes it was read into.
      const first = firstRead(cellId)
      if (first !== undefined) cellId = cells.value.find(each => each.node_ids.includes(first))?.cell_id ?? cellId
    }
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
    surface,
    frameColumns,
    cellColumns,
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
    shownCells,
    changedCount,
    added,
    render,
    lockReason,
    isEditable,
    canvasOnly,
    check,
    cellCode,
    setCellCode,
    cellSyncState,
    addCell,
    discardCell,
    adoptDetached,
    outputStale,
    clearOutputs,
    dismissNotice,
    push,
    runCell,
    runAll
  }
})
