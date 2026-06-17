import { describe, it, expect } from 'vitest'
import {
  SHARE_HASH_PREFIX,
  hasShareHash,
  encodeShareHash,
  decodeShareHash,
} from '../../src/utils/share-link'
import type { FlowfileData } from '../../src/types'

function makeFlow(name = 'Test Flow'): FlowfileData {
  return {
    flowfile_version: '1.0.0',
    flowfile_id: 1,
    flowfile_name: name,
    flowfile_settings: {
      description: '',
      execution_mode: 'Development',
      execution_location: 'local',
      auto_save: true,
      show_detailed_progress: true,
    },
    nodes: [
      {
        id: 1,
        type: 'manual_input',
        is_start_node: true,
        description: '',
        x_position: 100,
        y_position: 200,
        input_ids: [],
        outputs: [2],
        setting_input: { raw_data_format: { columns: [], data: [['a', 1]] } },
      },
    ],
    connections: [{ from_node: 1, to_node: 2, from_handle: 'main', to_handle: 'main' }],
  }
}

describe('share-link environment', () => {
  it('has native compression streams (requires Node >= 18 for tests)', () => {
    expect(typeof CompressionStream).toBe('function')
    expect(typeof DecompressionStream).toBe('function')
  })
})

describe('hasShareHash', () => {
  it('accepts only a non-empty #flow= payload', () => {
    expect(hasShareHash('#flow=abc')).toBe(true)
    expect(hasShareHash('')).toBe(false)
    expect(hasShareHash('#')).toBe(false)
    expect(hasShareHash('#flow=')).toBe(false)
    expect(hasShareHash('#other=x')).toBe(false)
  })
})

describe('encodeShareHash / decodeShareHash', () => {
  it('round-trips a flow without files', async () => {
    const flow = makeFlow()
    const hash = await encodeShareHash(flow)
    const decoded = await decodeShareHash(hash)
    expect(decoded).toEqual({ v: 1, flow })
  })

  it('round-trips a flow with file contents', async () => {
    const flow = makeFlow()
    const files = { 1: 'a,b\n1,2\n', 7: 'x\ny\n' }
    const decoded = await decodeShareHash(await encodeShareHash(flow, files))
    expect(decoded?.flow).toEqual(flow)
    expect(decoded?.files).toEqual({ 1: 'a,b\n1,2\n', 7: 'x\ny\n' })
  })

  it('omits the files key when empty', async () => {
    const decoded = await decodeShareHash(await encodeShareHash(makeFlow(), {}))
    expect(decoded?.files).toBeUndefined()
  })

  it('survives unicode in names and file contents', async () => {
    const flow = makeFlow('売上 🚀 flów')
    const files = { 1: 'naïve,数値\n"héllo",42\n' }
    const decoded = await decodeShareHash(await encodeShareHash(flow, files))
    expect(decoded?.flow.flowfile_name).toBe('売上 🚀 flów')
    expect(decoded?.files?.[1]).toBe('naïve,数値\n"héllo",42\n')
  })

  it('emits only URL-safe characters', async () => {
    const hash = await encodeShareHash(makeFlow(), { 1: 'a,b\n'.repeat(500) })
    expect(hash).toMatch(/^#flow=[A-Za-z0-9_-]+$/)
  })

  it('compresses a large repetitive payload below raw JSON size', async () => {
    const flow = makeFlow()
    const files = { 1: 'city,sales\nAmsterdam,123\n'.repeat(40000) }
    const hash = await encodeShareHash(flow, files)
    const rawLength = JSON.stringify({ v: 1, flow, files }).length
    expect(hash.length).toBeLessThan(rawLength)
    const decoded = await decodeShareHash(hash)
    expect(decoded?.files?.[1]).toBe(files[1])
  })

  it('returns null for malformed inputs without throwing', async () => {
    const valid = await encodeShareHash(makeFlow())
    const cases = [
      '',
      '#',
      '#other=x',
      '#flow=',
      '#flow=%%%',
      '#flow=not!valid!base64',
      SHARE_HASH_PREFIX + 'AAAA', // valid base64url, not deflate data
      valid.slice(0, Math.floor(valid.length / 2)), // truncated payload
    ]
    for (const input of cases) {
      await expect(decodeShareHash(input)).resolves.toBeNull()
    }
  })

  it('returns null for valid deflate of wrong shapes', async () => {
    const reEncode = async (envelope: unknown) => {
      // Build a hash whose deflate payload is arbitrary JSON, bypassing encodeShareHash's envelope.
      const json = new TextEncoder().encode(JSON.stringify(envelope))
      const stream = new CompressionStream('deflate-raw')
      const writer = stream.writable.getWriter()
      const done = writer.write(json).then(() => writer.close())
      const reader = stream.readable.getReader()
      const chunks: Uint8Array[] = []
      for (;;) {
        const { done: d, value } = await reader.read()
        if (d) break
        chunks.push(value)
      }
      await done
      const total = chunks.reduce((n, c) => n + c.length, 0)
      const bytes = new Uint8Array(total)
      let off = 0
      for (const c of chunks) {
        bytes.set(c, off)
        off += c.length
      }
      let bin = ''
      for (let i = 0; i < bytes.length; i += 0x8000) {
        bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000))
      }
      return (
        SHARE_HASH_PREFIX + btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
      )
    }
    await expect(decodeShareHash(await reEncode('plain string'))).resolves.toBeNull()
    await expect(decodeShareHash(await reEncode({ v: 99, flow: makeFlow() }))).resolves.toBeNull()
    await expect(decodeShareHash(await reEncode({ v: 1 }))).resolves.toBeNull()
    await expect(decodeShareHash(await reEncode({ v: 1, flow: { nodes: 'x' } }))).resolves.toBeNull()
    await expect(
      decodeShareHash(await reEncode({ v: 1, flow: makeFlow(), files: { 1: 42 } })),
    ).resolves.toBeNull()
  })
})
