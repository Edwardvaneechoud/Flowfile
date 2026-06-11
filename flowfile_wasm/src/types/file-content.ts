import { markRaw } from 'vue'

export type BinaryFormat = 'excel' | 'parquet' | 'arrow-ipc'

export interface TextFileContent {
  kind: 'text'
  data: string
}

export interface BinaryFileContent {
  kind: 'binary'
  data: Uint8Array
  format: BinaryFormat
}

/**
 * Tagged content for node files. Text rides the legacy string paths
 * (sessionStorage JSON, CSV inference); binary lives only in memory and
 * IndexedDB (structured clone), never JSON.
 */
export type FileContent = TextFileContent | BinaryFileContent

export function textContent(data: string): TextFileContent {
  return { kind: 'text', data }
}

/** Binary wrappers are markRaw'd: a reactive proxy around the wrapper would
 * fail structured cloning into IndexedDB (DataCloneError). */
export function binaryContent(data: Uint8Array, format: BinaryFormat): BinaryFileContent {
  return markRaw({ kind: 'binary', data, format })
}

export function asFileContent(value: string | FileContent | null | undefined): FileContent {
  // Null-guard: legacy persisted states can carry null/undefined entries
  if (value == null) return textContent('')
  if (typeof value === 'string') return textContent(value)
  if (value.kind === 'binary') return markRaw(value)
  return value
}

export function isBinary(content: FileContent | undefined | null): content is BinaryFileContent {
  return content?.kind === 'binary'
}

export function contentByteSize(content: FileContent): number {
  return content.kind === 'binary' ? content.data.byteLength : new Blob([content.data]).size
}

/** File format from the file name; anything not recognized as binary is text/csv. */
export function detectFormat(fileName: string): 'csv' | BinaryFormat {
  const ext = fileName.split('.').pop()?.toLowerCase() ?? ''
  if (ext === 'xlsx' || ext === 'xlsm') return 'excel'
  if (ext === 'parquet' || ext === 'pq') return 'parquet'
  if (ext === 'arrow' || ext === 'ipc') return 'arrow-ipc'
  return 'csv'
}

export function mimeFor(format: 'csv' | BinaryFormat): string {
  switch (format) {
    case 'excel':
      return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    case 'parquet':
      return 'application/vnd.apache.parquet'
    case 'arrow-ipc':
      return 'application/vnd.apache.arrow.stream'
    default:
      return 'text/csv'
  }
}
