import type { FlowfileData } from '../types'

/** URL hash payload for serverless flow sharing: the flow definition plus any
 * small text file contents, deflate-compressed and base64url-encoded. Lives in
 * the fragment so flow data never reaches server logs. */
export const SHARE_HASH_PREFIX = '#flow='

export interface ShareEnvelopeV1 {
  v: 1
  flow: FlowfileData
  files?: Record<number, string>
}

export function hasShareHash(hash: string): boolean {
  return hash.startsWith(SHARE_HASH_PREFIX) && hash.length > SHARE_HASH_PREFIX.length
}

/** Pump bytes through a Compression/DecompressionStream without Response/Blob
 * (happy-dom lacks them as stream sinks). The write must run concurrently with
 * the read loop: awaiting close() first deadlocks once the internal queue
 * fills on large inputs. */
async function pumpThrough(
  ts: CompressionStream | DecompressionStream,
  input: Uint8Array,
): Promise<Uint8Array> {
  const writer = ts.writable.getWriter()
  let writeError: unknown = null
  // The catch must attach immediately: when the stream errors, the reader loop
  // throws first and an uncaught writer rejection would escape as unhandled.
  const writeDone = writer
    .write(input)
    .then(() => writer.close())
    .catch((e) => {
      writeError = e ?? new Error('stream write failed')
    })
  const reader = ts.readable.getReader()
  const chunks: Uint8Array[] = []
  let total = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    chunks.push(value)
    total += value.length
  }
  await writeDone
  if (writeError) throw writeError
  const out = new Uint8Array(total)
  let offset = 0
  for (const chunk of chunks) {
    out.set(chunk, offset)
    offset += chunk.length
  }
  return out
}

function bytesToBase64Url(bytes: Uint8Array): string {
  let bin = ''
  // Chunked fromCharCode: a single spread of a multi-MB array overflows the call stack.
  for (let i = 0; i < bytes.length; i += 0x8000) {
    bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000))
  }
  return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

function base64UrlToBytes(encoded: string): Uint8Array {
  const base64 = encoded.replace(/-/g, '+').replace(/_/g, '/')
  const padded = base64 + '='.repeat((4 - (base64.length % 4)) % 4)
  const bin = atob(padded)
  const bytes = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i)
  return bytes
}

export async function encodeShareHash(
  flow: FlowfileData,
  files?: Record<number, string>,
): Promise<string> {
  const envelope: ShareEnvelopeV1 = { v: 1, flow }
  if (files && Object.keys(files).length > 0) envelope.files = files
  const json = new TextEncoder().encode(JSON.stringify(envelope))
  const compressed = await pumpThrough(new CompressionStream('deflate-raw'), json)
  return SHARE_HASH_PREFIX + bytesToBase64Url(compressed)
}

/** Returns null on any malformed input (bad base64, bad deflate, bad JSON,
 * unknown version, missing flow) — never throws. */
export async function decodeShareHash(hash: string): Promise<ShareEnvelopeV1 | null> {
  if (!hasShareHash(hash)) return null
  try {
    const bytes = base64UrlToBytes(hash.slice(SHARE_HASH_PREFIX.length))
    const json = await pumpThrough(new DecompressionStream('deflate-raw'), bytes)
    const envelope = JSON.parse(new TextDecoder().decode(json))
    if (!envelope || typeof envelope !== 'object' || envelope.v !== 1) return null
    const flow = envelope.flow
    if (!flow || typeof flow !== 'object' || !Array.isArray(flow.nodes)) return null
    if (envelope.files !== undefined) {
      if (typeof envelope.files !== 'object' || envelope.files === null) return null
      for (const value of Object.values(envelope.files)) {
        if (typeof value !== 'string') return null
      }
    }
    return envelope as ShareEnvelopeV1
  } catch {
    return null
  }
}
