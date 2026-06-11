import { binaryContent, detectFormat, type BinaryFileContent } from '../types/file-content'
import { checkFileSize } from './file-size-limits'
import { detectBinaryFormat } from './binary-format'

export interface RemoteFile {
  fileName: string
  format: 'csv' | 'excel' | 'parquet'
  content: string | BinaryFileContent
  warning?: string
}

export const CORS_HINT =
  'The server must allow cross-origin requests (Access-Control-Allow-Origin). ' +
  'Download the file and drag it in instead.'

/**
 * Fetch a data file over HTTPS for the Read File node. Plain CORS applies
 * (COEP does not block fetch); failures map to actionable messages.
 */
export async function fetchRemoteFile(url: string): Promise<RemoteFile> {
  let parsed: URL
  try {
    parsed = new URL(url.trim())
  } catch {
    throw new Error('Enter a valid http(s) URL')
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    throw new Error('Only http(s) URLs are supported')
  }

  let fileName = decodeURIComponent(parsed.pathname.split('/').pop() || '') || 'remote-file'
  if (detectFormat(fileName) === 'arrow-ipc') {
    throw new Error('Arrow files are not supported by the Read File node — use CSV, Excel or Parquet')
  }

  let response: Response
  try {
    response = await fetch(parsed.toString())
  } catch {
    throw new Error(`Could not fetch ${fileName}: the request was blocked. ${CORS_HINT}`)
  }
  if (!response.ok) {
    throw new Error(`Could not fetch ${fileName}: the server answered ${response.status} ${response.statusText}`.trim())
  }

  // Pre-check the declared size so an oversized download aborts before the
  // body lands: the typed extension's cap when recognized, else the most
  // permissive (csv) — the precise per-format check runs after download
  const declared = Number(response.headers.get('content-length'))
  if (Number.isFinite(declared) && declared > 0) {
    const preFormat = detectFormat(fileName)
    const check = checkFileSize(preFormat === 'arrow-ipc' ? 'csv' : preFormat, declared, fileName)
    if (!check.ok) throw new Error(check.error)
  }

  // Redirects can land somewhere more truthful than what the user typed
  try {
    const finalName = decodeURIComponent(new URL(response.url).pathname.split('/').pop() || '')
    if (finalName && detectFormat(finalName) !== 'csv') fileName = finalName
  } catch {
    /* keep the typed-URL name */
  }

  const bytes = new Uint8Array(await response.arrayBuffer())

  // Format: recognized extension wins; otherwise sniff magic bytes so a
  // shortlink/API URL doesn't get a parquet file UTF-8-mangled into "CSV"
  let format = detectFormat(fileName) as 'csv' | 'excel' | 'parquet'
  if (format === 'csv') {
    const sniffed = detectBinaryFormat(bytes)
    if (sniffed === 'parquet') format = 'parquet'
    else if (bytes.length >= 2 && bytes[0] === 0x50 && bytes[1] === 0x4b && /\.(xlsx|xlsm)([?#]|$)/i.test(response.url)) {
      format = 'excel'
    }
  }

  const check = checkFileSize(format, bytes.byteLength, fileName)
  if (!check.ok) throw new Error(check.error)

  if (format === 'csv') {
    return { fileName, format, content: new TextDecoder().decode(bytes), warning: check.warning }
  }
  return {
    fileName,
    format,
    content: binaryContent(bytes, format),
    warning: check.warning
  }
}
