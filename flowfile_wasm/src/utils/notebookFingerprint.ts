/**
 * A fingerprint of everything the notebook's code is rendered from: each node's
 * type, settings, description, reference and incoming edges. Moving a node on
 * the canvas keeps it; any change that would change the rendered code moves it.
 *
 * The same contract as flowfile_core's `notebook.render.code_fingerprint`. The
 * value itself is this editor's own and is never compared with core's.
 */
import type { FlowEdge, FlowNode } from '../types'

const LAYOUT_FIELDS = new Set(['pos_x', 'pos_y', 'is_setup', 'flow_id', 'user_id'])

function canonical(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonical)
  if (value && typeof value === 'object') {
    const out: Record<string, unknown> = {}
    for (const key of Object.keys(value as object).sort()) out[key] = canonical((value as Record<string, unknown>)[key])
    return out
  }
  return value
}

function withoutLayout(settings: unknown): unknown {
  if (!settings || typeof settings !== 'object') return settings ?? null
  const out: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(settings as Record<string, unknown>)) {
    if (!LAYOUT_FIELDS.has(key)) out[key] = value
  }
  return out
}

/** cyrb53: a 53-bit string hash, plenty to tell two flows apart. */
function hash(text: string): string {
  let h1 = 0xdeadbeef
  let h2 = 0x41c6ce57
  for (let i = 0; i < text.length; i++) {
    const ch = text.charCodeAt(i)
    h1 = Math.imul(h1 ^ ch, 2654435761)
    h2 = Math.imul(h2 ^ ch, 1597334677)
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909)
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909)
  return (4294967296 * (2097151 & h2) + (h1 >>> 0)).toString(36)
}

export function codeFingerprint(nodes: Map<number, FlowNode>, edges: readonly FlowEdge[]): string {
  const described = [...nodes.values()]
    .sort((a, b) => a.id - b.id)
    .map(node => ({
      id: node.id,
      type: node.type,
      description: node.description ?? '',
      reference: node.node_reference ?? null,
      settings: withoutLayout(node.settings),
      edges: edges
        .filter(edge => edge.target === String(node.id))
        .map(edge => [edge.source, edge.sourceHandle, edge.targetHandle])
    }))
  return hash(JSON.stringify(canonical(described)))
}
