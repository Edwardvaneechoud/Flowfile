/**
 * What a notebook sync changes, as the patch the flow store lands in one undo step.
 *
 * The engine reads the changed cells (`engine/notebook_cells.py`) and answers in
 * flowfile_core's dialect: per node the settings to lay over its own, and per
 * node the inputs it reads now. This turns that answer into a `FlowPatch` in
 * this editor's dialect. Pure: no store, no Vue.
 */
import type { FlowEdge, FlowNode, NodeSettings } from '../types'
import type { FlowPatch, GraphState } from './flowPatch'

export interface NotebookSyncChange {
  settings?: Record<string, unknown>
  description?: string
  node_reference?: string | null
}

export interface NotebookSyncPorts {
  main: number[]
  right?: number | null
  left?: number | null
}

export interface NotebookSyncResult {
  ok: true
  nodes: Record<string, NotebookSyncChange>
  inputs: Record<string, NotebookSyncPorts>
  warnings: string[]
}

export interface NotebookSyncFailure {
  ok: false
  cell_id: string
  line?: number | null
  kind: 'error' | 'needs_kernel' | 'refused'
  message: string
}

const isRecord = (value: unknown): value is Record<string, any> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

/** Lay `patch` over `base`: objects merge key by key, anything else (lists too) is replaced. */
export function mergeSettings<T>(base: T, patch: unknown): T {
  if (!isRecord(base) || !isRecord(patch)) return JSON.parse(JSON.stringify(patch ?? null))
  const merged: Record<string, any> = { ...base }
  for (const [key, value] of Object.entries(patch)) merged[key] = mergeSettings(merged[key], value)
  return merged as T
}

/**
 * Keep this editor's own spelling of a setting in step with the core one a sync wrote:
 * `toCoreSettings` (coreExport.ts) derives the core keys from these, so a stale one would win.
 */
function alignEditorKeys(type: string, settings: Record<string, any>, patch: Record<string, unknown>): void {
  if (type === 'unique' && patch.unique_input && settings.unique_input) {
    settings.unique_input.subset = settings.unique_input.columns ?? []
    settings.unique_input.keep = settings.unique_input.strategy
  }
  if (type === 'record_id' && patch.record_id_input && settings.record_id_input) {
    settings.record_id_input.name = settings.record_id_input.output_column_name
  }
  if (type === 'head' && 'sample_size' in patch && settings.head_input) {
    settings.head_input.n = settings.sample_size
  }
  if (type === 'formula' && Array.isArray(settings.functions) && 'functions' in patch) {
    if (settings.functions.length === 1) settings.function = settings.functions[0]
    else delete settings.function
  }
}

const incoming = (target: number, source: number, targetHandle: string): Omit<FlowEdge, 'id'> => ({
  source: String(source),
  target: String(target),
  sourceHandle: 'output-0',
  targetHandle
})

/** The patch that turns `graph` into what the sync describes; empty when the sync changed nothing. */
export function syncPatch(graph: Pick<GraphState, 'nodes' | 'edges'>, result: NotebookSyncResult): FlowPatch {
  const updateNodes: NonNullable<FlowPatch['updateNodes']> = []
  for (const [key, change] of Object.entries(result.nodes ?? {})) {
    const node = graph.nodes.get(Number(key))
    if (!node) throw new Error(`The notebook changed node ${key}, which is no longer on the canvas`)
    let settings = node.settings as Record<string, any>
    if (change.settings) {
      settings = mergeSettings(settings, change.settings)
      alignEditorKeys(node.type, settings, change.settings)
    }
    const update: Partial<FlowNode> & Pick<FlowNode, 'id'> = { id: node.id }
    if (change.description !== undefined) {
      update.description = change.description
      settings = { ...settings, description: change.description }
    }
    if ('node_reference' in change) {
      // An empty reference, not a missing one: a patch cannot unset a field, and '' reads as none everywhere.
      update.node_reference = change.node_reference || ''
      settings = { ...settings, node_reference: change.node_reference || undefined }
    }
    if (settings !== node.settings) update.settings = settings as unknown as NodeSettings
    updateNodes.push(update)
  }

  const removeEdges: NonNullable<FlowPatch['removeEdges']> = []
  const addEdges: NonNullable<FlowPatch['addEdges']> = []
  for (const [key, ports] of Object.entries(result.inputs ?? {})) {
    const target = Number(key)
    if (!graph.nodes.has(target)) throw new Error(`The notebook rewired node ${key}, which is no longer on the canvas`)
    if (ports.left != null) throw new Error(`Node ${key} has an input this editor cannot connect`)
    // Every input is laid again, in order: a union reads its inputs in the order they were connected.
    for (const edge of graph.edges) {
      if (edge.target === String(target)) {
        removeEdges.push({ source: edge.source, target: edge.target, sourceHandle: edge.sourceHandle, targetHandle: edge.targetHandle })
      }
    }
    for (const source of ports.main ?? []) addEdges.push(incoming(target, source, 'input-0'))
    if (ports.right != null) addEdges.push(incoming(target, ports.right, 'input-1'))
  }

  const patch: FlowPatch = {}
  if (updateNodes.length) patch.updateNodes = updateNodes
  if (removeEdges.length) patch.removeEdges = removeEdges
  if (addEdges.length) patch.addEdges = addEdges
  return patch
}

export const isEmptyPatch = (patch: FlowPatch): boolean => Object.keys(patch).length === 0
