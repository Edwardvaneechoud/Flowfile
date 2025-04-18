import { NodeBase } from './../../baseNode/nodeInput'


export interface NodeCopyValue {
    nodeIdToCopyFrom: number
    type: string // CamelCase
    label: string // readable
    description: string
    numberOfInputs: number
    numberOfOutputs: number
    multi?: boolean;
    typeSnakeCase: string;
    flowIdToCopyFrom: number
}


export interface NodeCopyInput extends NodeCopyValue {
    posX: number
    posY: number
    flowId: number
}

export interface CursorPosition {
  x: number;
  y: number;
}

export interface ContextMenuAction {
    actionId: string;
    targetType: 'node' | 'edge' | 'pane';
    targetId: string;
    position: CursorPosition
  }


export interface NodePromise extends NodeBase {
    is_setup?: boolean
    node_type: string
}