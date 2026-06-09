import type { ColumnSchema, DataPreview, DownloadInfo } from '../../types'

export type CatalogKind = 'file' | 'external' | 'output' | 'catalog' | 'flow'
export type CatalogStatus = 'success' | 'failure' | 'pending'

/**
 * A unified, client-side catalog entry. Built from the WASM data sources
 * (loaded CSV files, host-injected external datasets, uploaded catalog tables)
 * plus saved flows — flows and tables live in one catalog, as in the full app.
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
  // Flow entries (kind === 'flow') — the stable saved-flow id + metadata.
  flowId?: string
  nodeCount?: number
  createdAt?: number
  updatedAt?: number
  description?: string
}
