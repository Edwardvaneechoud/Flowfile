import { SelectInputs, NodeMultiInput } from '../../../baseNode/nodeInput'

export interface NodeJoin extends NodeMultiInput {
  auto_generate_selection: boolean
  verify_integrity: boolean
  join_input: JoinInput
}

export interface JoinMap {
  left_col: string
  right_col: string
}

export interface JoinInput {
  join_mapping: JoinMap[]
  left_select: SelectInputs
  right_select: SelectInputs
  how: 'inner' | 'left' | 'right' | 'full' | 'semi' | 'anti' | 'cross'
}
