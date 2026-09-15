import { SelectInputs, NodeMultiInput } from "../../../baseNode/nodeInput";

// The join strategies the drawer offers. Mirrors the backend's JoinKeyStrategy
// (flowfile_core/flowfile_core/schemas/transform_schema.py), which deliberately
// excludes "cross" — Cartesian joins are the dedicated cross_join node's job.
// "outer" is accepted by the backend as a legacy alias for "full" but is not
// offered here; flows built elsewhere may still carry it.
// Kept in sync by flowfile_core/tests/flowfile/test_join_type_parity.py.
export const JOIN_TYPES = ["inner", "left", "right", "full", "semi", "anti"] as const;

export type JoinType = (typeof JOIN_TYPES)[number];

export interface NodeJoin extends NodeMultiInput {
  auto_generate_selection: boolean;
  verify_integrity: boolean;
  join_input: JoinInput;
}

export interface JoinMap {
  left_col: string;
  right_col: string;
}

export interface JoinInput {
  join_mapping: JoinMap[];
  left_select: SelectInputs;
  right_select: SelectInputs;
  how: JoinType | "outer";
}
