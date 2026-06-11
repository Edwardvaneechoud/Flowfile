import type { BinaryFormat } from '../types/file-content'

/** Best-effort format sniff for host-provided bytes when no explicit format
 * is given. Parquet files start with the PAR1 magic; Arrow IPC streams have
 * no magic prefix, so they're the fallback. */
export function detectBinaryFormat(bytes: Uint8Array): BinaryFormat {
  if (
    bytes.length >= 4 &&
    bytes[0] === 0x50 && // P
    bytes[1] === 0x41 && // A
    bytes[2] === 0x52 && // R
    bytes[3] === 0x31 // 1
  ) {
    return 'parquet'
  }
  return 'arrow-ipc'
}
