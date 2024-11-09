import { SelectInput, NodeSelect, SelectInputs } from '../../../baseNode/nodeInput'

export interface NodeMultiInput {
  dependingOnIds: number[] | null
}

export interface NodeCrossJoin extends NodeMultiInput {
  auto_generate_selection: boolean
  verify_integrity: boolean
  cross_join_input: CrossJoinInput
}

export interface JoinMap {
  left_col: string
  right_col: string
}

export interface CrossJoinInput {
  left_select: SelectInputs
  right_select: SelectInputs
  how: string
}
