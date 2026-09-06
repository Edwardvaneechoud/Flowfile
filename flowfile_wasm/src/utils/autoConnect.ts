// Alteryx-style auto-connect: a node dropped from the palette next to an
// existing node is wired up without a cable drag. Pure geometry over flow
// coordinates; mirrors flowfile_frontend's utils/autoConnect.ts.

export const AUTO_CONNECT_RADIUS = 180

export interface AutoConnectNode {
  id: string
  x: number
  y: number
  width: number
  height: number
  // Input handles that can still accept an edge, most preferred first.
  freeInputs: string[]
  // Output handles, most preferred first (outputs may fan out, so never empty
  // for a node that has outputs).
  outputs: string[]
}

export interface AutoConnectDrop {
  // Top-left of the new node in flow coordinates.
  x: number
  y: number
  // Size when known (an existing node being dragged); a palette drop has no
  // size yet and uses the median size of the nodes on the canvas.
  width?: number
  height?: number
  // The new node's first data input / first output, null when it has none.
  inputHandle: string | null
  outputHandle: string | null
}

export interface AutoConnectMatch {
  nodeId: string
  // upstream: existing -> new; downstream: new -> existing.
  direction: 'upstream' | 'downstream'
  existingHandle: string
  newHandle: string
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
}

/**
 * Pick the existing node the dropped node should connect to, or null.
 *
 * A match requires the two facing handle anchors to lie within `radius` of
 * each other and the new node to sit on the correct side of the existing
 * node's centre, so a drop on top of or behind a node never connects.
 * Upstream wins ties.
 */
export function findAutoConnectMatch(
  drop: AutoConnectDrop,
  nodes: AutoConnectNode[],
  radius = AUTO_CONNECT_RADIUS
): AutoConnectMatch | null {
  if (!nodes.length) return null
  const estWidth = drop.width ?? median(nodes.map(n => n.width))
  const estHeight = drop.height ?? median(nodes.map(n => n.height))
  const newInput = { x: drop.x, y: drop.y + estHeight / 2 }
  const newOutput = { x: drop.x + estWidth, y: drop.y + estHeight / 2 }

  const candidates: Array<{ match: AutoConnectMatch; distance: number }> = []
  for (const node of nodes) {
    const centerX = node.x + node.width / 2
    const midY = node.y + node.height / 2
    if (drop.inputHandle && node.outputs.length && newInput.x >= centerX) {
      candidates.push({
        match: {
          nodeId: node.id,
          direction: 'upstream',
          existingHandle: node.outputs[0],
          newHandle: drop.inputHandle
        },
        distance: Math.hypot(newInput.x - (node.x + node.width), newInput.y - midY)
      })
    }
    if (drop.outputHandle && node.freeInputs.length && newOutput.x <= centerX) {
      candidates.push({
        match: {
          nodeId: node.id,
          direction: 'downstream',
          existingHandle: node.freeInputs[0],
          newHandle: drop.outputHandle
        },
        distance: Math.hypot(newOutput.x - node.x, newOutput.y - midY)
      })
    }
  }
  let best: { match: AutoConnectMatch; distance: number } | undefined
  for (const candidate of candidates) {
    if (candidate.distance <= radius && (!best || candidate.distance < best.distance)) {
      best = candidate
    }
  }
  return best?.match ?? null
}

// Outputs may fan out, but a drop next to a split node should land on the
// output nobody uses yet, so unused ones sort first.
export function preferUnusedOutputs(outputs: string[], used: Set<string>): string[] {
  return [...outputs.filter(h => !used.has(h)), ...outputs.filter(h => used.has(h))]
}
