import type { ColumnSchema, DataPreview, DownloadInfo } from '../../types'

export type CatalogKind = 'file' | 'external' | 'output'
export type CatalogStatus = 'success' | 'failure' | 'pending'

/**
 * A unified, client-side catalog entry. Built from the three WASM data sources:
 * loaded CSV files, host-injected external datasets, and executed node outputs.
 */
export interface CatalogItem {
  id: string
  kind: CatalogKind
  name: string
  subtitle?: string
  nodeId?: number
  datasetName?: string
  schema?: ColumnSchema[]
  rows?: number | null
  columns?: number | null
  sizeBytes?: number
  status?: CatalogStatus
  unavailable?: boolean
  preview?: DataPreview | null
  download?: DownloadInfo
}
