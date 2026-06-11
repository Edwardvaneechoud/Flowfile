const MB = 1024 * 1024

export interface FileSizeLimit {
  warnBytes: number
  limitBytes: number
  /** Why the cap exists, appended to warnings/errors. */
  reason: string
}

/** Browser comfort zones per format (Pyodide heap is ~2GB). */
export const FILE_SIZE_LIMITS: Record<'csv' | 'excel' | 'parquet' | 'arrow-ipc', FileSizeLimit> = {
  csv: { warnBytes: 100 * MB, limitBytes: 200 * MB, reason: 'large CSVs are slow to parse in the browser' },
  excel: {
    warnBytes: 25 * MB,
    limitBytes: 50 * MB,
    reason: 'xlsx parsing runs in pure Python on the main thread'
  },
  parquet: {
    warnBytes: 50 * MB,
    limitBytes: 100 * MB,
    reason: 'Parquet decompresses 2-10x in memory'
  },
  'arrow-ipc': { warnBytes: 100 * MB, limitBytes: 200 * MB, reason: 'IPC bytes are held uncompressed in memory' }
}

export type FileSizeCheck = { ok: true; warning?: string } | { ok: false; error: string }

export function checkFileSize(format: keyof typeof FILE_SIZE_LIMITS, bytes: number, fileName = 'file'): FileSizeCheck {
  const limit = FILE_SIZE_LIMITS[format]
  const mb = (bytes / MB).toFixed(0)
  if (bytes > limit.limitBytes) {
    return {
      ok: false,
      error:
        `${fileName} is ${mb}MB — over the ${limit.limitBytes / MB}MB browser comfort zone for ` +
        `${format} (${limit.reason}). Convert it to a smaller file or CSV and try again.`
    }
  }
  if (bytes > limit.warnBytes) {
    return {
      ok: true,
      warning: `${fileName} is ${mb}MB — ${format} over ${limit.warnBytes / MB}MB may be slow (${limit.reason}).`
    }
  }
  return { ok: true }
}
