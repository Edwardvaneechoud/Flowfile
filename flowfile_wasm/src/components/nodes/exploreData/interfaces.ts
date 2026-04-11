// Re-export Graphic Walker types for convenience in Vue components.
// These are type-only imports so they do not add to the bundle.
// We import from the package root (which re-exports from ./interfaces and
// ./store/visualSpecStore) instead of deep-linking into dist/, because the
// package's exports map does not expose those submodules explicitly.
export type {
  IRow,
  IMutField,
  IChart,
  IGWProps,
  VizSpecStore,
} from '@kanaries/graphic-walker'

import type { IRow, IMutField, IChart } from '@kanaries/graphic-walker'

export interface DataModel {
  data: IRow[]
  fields: IMutField[]
}

export interface GraphicWalkerInput {
  is_initial: boolean
  dataModel: DataModel
  specList: IChart[]
}
