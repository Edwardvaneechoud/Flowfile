/**
 * Placeholder nodes: parts of a flow this in-browser build cannot run.
 *
 * They arrive two ways:
 *  - Share links minted by flowfile_core demote unsupported/incompatible nodes
 *    to a sentinel type `<core_type>__unsupported` with a settings stub
 *    `{ is_placeholder: true, original_type, reason, label, inputs, outputs }`.
 *    The sentinel is load-bearing: an older deployed app fails loudly on the
 *    unknown type instead of running a stripped-settings node as a silent no-op.
 *  - A flowfile_core-saved file opened directly can carry node types this build
 *    never implemented; those are detected locally by type.
 *
 * Everything here is pure — no store or Vue imports — so the flow store, the
 * canvas components and the tests share one definition. The security invariant
 * (a placeholder's settings never reach the Pyodide bridge, so nothing a sender
 * wrote can be exec'd) is enforced in flow-store.ts and in the Python engine's
 * schema_propagation guard; this module only answers "is this a placeholder".
 */
import type { FlowEdge } from '../types'
import { isSupportedNodeType } from '../config/nodeCatalog'

export const PLACEHOLDER_TYPE_SUFFIX = '__unsupported'
export const DEFAULT_PLACEHOLDER_REASON = 'Not supported in the browser version'

export interface PlaceholderStub {
  is_placeholder: true
  original_type?: string
  reason?: string
  label?: string
  inputs?: number
  outputs?: number
  description?: string
}

export function isPlaceholderSettings(settings: unknown): settings is PlaceholderStub {
  return !!settings && typeof settings === 'object' && (settings as any).is_placeholder === true
}

export function isPlaceholderType(type: string): boolean {
  return type.endsWith(PLACEHOLDER_TYPE_SUFFIX)
}

/** A node this build must treat as a locked, non-runnable placeholder. */
export function isPlaceholderNode(type: string, settings: unknown): boolean {
  return isPlaceholderType(type) || isPlaceholderSettings(settings) || !isSupportedNodeType(type)
}

/** The node type the sender authored (for icon/label/docs lookup). */
export function originalNodeType(type: string, settings: unknown): string {
  if (isPlaceholderSettings(settings) && typeof settings.original_type === 'string' && settings.original_type) {
    return settings.original_type
  }
  return isPlaceholderType(type) ? type.slice(0, -PLACEHOLDER_TYPE_SUFFIX.length) : type
}

export function placeholderReason(settings: unknown): string {
  if (isPlaceholderSettings(settings) && typeof settings.reason === 'string' && settings.reason) {
    return settings.reason
  }
  return DEFAULT_PLACEHOLDER_REASON
}

/** Display name: the stub's label, else a title-cased original type. */
export function placeholderLabel(type: string, settings: unknown): string {
  if (isPlaceholderSettings(settings) && typeof settings.label === 'string' && settings.label) {
    return settings.label
  }
  const original = originalNodeType(type, settings)
  return original
    .split('_')
    .map(w => (w ? w[0].toUpperCase() + w.slice(1) : w))
    .join(' ')
}

function handleIndex(handle: string | undefined, prefix: string): number {
  if (!handle || !handle.startsWith(prefix)) return 0
  const n = parseInt(handle.slice(prefix.length), 10)
  return Number.isFinite(n) ? n : 0
}

/**
 * Handle counts for a placeholder node. Priority: the palette definition (when
 * the original type is a locked teaser), else the stub the sender shipped,
 * else 1 — always floored by the actual edges, so an under-reporting stub can
 * never render a node whose second edge has no handle to land on.
 */
export function placeholderHandleCounts(
  nodeId: number,
  settings: unknown,
  edges: readonly FlowEdge[],
  paletteCounts?: { inputs: number; outputs: number }
): { inputs: number; outputs: number } {
  const id = String(nodeId)
  let inFloor = 0
  let outFloor = 0
  for (const edge of edges) {
    if (edge.target === id) inFloor = Math.max(inFloor, handleIndex(edge.targetHandle, 'input-') + 1)
    if (edge.source === id) outFloor = Math.max(outFloor, handleIndex(edge.sourceHandle, 'output-') + 1)
  }
  const stub = isPlaceholderSettings(settings) ? settings : undefined
  return {
    inputs: Math.max(paletteCounts?.inputs ?? stub?.inputs ?? 1, inFloor),
    outputs: Math.max(paletteCounts?.outputs ?? stub?.outputs ?? 1, outFloor)
  }
}
