import { describe, it, expect } from 'vitest'
import { detectBinaryFormat } from '../../src/utils/binary-format'

describe('detectBinaryFormat', () => {
  it('detects parquet via the PAR1 magic', () => {
    expect(detectBinaryFormat(new Uint8Array([0x50, 0x41, 0x52, 0x31, 0, 0]))).toBe('parquet')
  })

  it('falls back to arrow-ipc otherwise', () => {
    expect(detectBinaryFormat(new Uint8Array([0xff, 0xff, 0xff, 0xff]))).toBe('arrow-ipc')
    expect(detectBinaryFormat(new Uint8Array([]))).toBe('arrow-ipc')
    expect(detectBinaryFormat(new Uint8Array([0x50, 0x41]))).toBe('arrow-ipc')
  })
})
