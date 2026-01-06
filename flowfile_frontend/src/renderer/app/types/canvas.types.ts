// Canvas-related TypeScript interfaces and types
// Consolidated from features/designer/components/Canvas/types.ts

import type { NodeBase } from './node.types'
import type { NodeTemplate } from './flow.types'

// ============================================================================
// Node Copy Types
// ============================================================================

export interface NodeCopyValue {
  nodeIdToCopyFrom: number
  type: string // CamelCase
  label: string // readable
  description: string
  numberOfInputs: number
  numberOfOutputs: number
  multi?: boolean
  typeSnakeCase: string
  flowIdToCopyFrom: number
  nodeTemplate?: NodeTemplate
  // Relative position from top-left of selection bounding box (for multi-node copy)
  relativeX?: number
  relativeY?: number
}

export interface NodeCopyInput extends NodeCopyValue {
  posX: number
  posY: number
  flowId: number
}

// ============================================================================
// Multi-Node Copy Types (for copying multiple nodes with connections)
// ============================================================================

export interface EdgeCopyValue {
  sourceNodeId: number
  targetNodeId: number
  sourceHandle: string
  targetHandle: string
}

export interface MultiNodeCopyValue {
  nodes: NodeCopyValue[]
  edges: EdgeCopyValue[]
  flowIdToCopyFrom: number
}

// ============================================================================
// Cursor and Context Menu Types
// ============================================================================

export interface CursorPosition {
  x: number
  y: number
}

export interface ContextMenuAction {
  actionId: string
  targetType: 'node' | 'edge' | 'pane'
  targetId: string
  position: CursorPosition
}

// ============================================================================
// Node Promise Types
// ============================================================================

export interface NodePromise extends NodeBase {
  is_setup?: boolean
  node_type: string
}

// ============================================================================
// Backend Interface Types
// ============================================================================

export interface AxiosResponse {
  data: any
  status: number
  statusText: string
  headers: any
  config: any
}

export interface NodeInputConnection {
  node_id: number
  connection_class: 'input-0' | 'input-1' | 'input-2' | 'input-3' | 'input-4' | 'input-5' | 'input-6' | 'input-7' | 'input-8' | 'input-9'
}

export interface NodeOutputConnection {
  node_id: number
  connection_class: 'output-0' | 'output-1' | 'output-2' | 'output-3' | 'output-4' | 'output-5' | 'output-6' | 'output-7' | 'output-8' | 'output-9'
}

export interface NodeConnection {
  input_connection: NodeInputConnection
  output_connection: NodeOutputConnection
}
