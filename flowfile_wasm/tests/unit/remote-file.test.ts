import { describe, it, expect, vi, afterEach } from 'vitest'
import { fetchRemoteFile, CORS_HINT } from '../../src/utils/remote-file'
import { FILE_SIZE_LIMITS } from '../../src/utils/file-size-limits'

function mockFetchResponse(
  body: string | Uint8Array,
  init: { status?: number; contentLength?: number; url?: string } = {}
) {
  const blob = new Blob([body])
  return vi.fn().mockResolvedValue({
    ok: (init.status ?? 200) < 400,
    status: init.status ?? 200,
    statusText: init.status === 404 ? 'Not Found' : 'OK',
    url: init.url ?? 'https://example.com/response',
    headers: new Headers(
      init.contentLength !== undefined ? { 'content-length': String(init.contentLength) } : {}
    ),
    arrayBuffer: () => blob.arrayBuffer()
  })
}

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('fetchRemoteFile', () => {
  it('fetches a CSV as text content', async () => {
    vi.stubGlobal('fetch', mockFetchResponse('a,b\n1,2\n'))

    const remote = await fetchRemoteFile('https://example.com/data/sales.csv')

    expect(remote.fileName).toBe('sales.csv')
    expect(remote.format).toBe('csv')
    expect(remote.content).toBe('a,b\n1,2\n')
  })

  it('fetches a parquet file as binary content', async () => {
    vi.stubGlobal('fetch', mockFetchResponse(new Uint8Array([0x50, 0x41, 0x52, 0x31])))

    const remote = await fetchRemoteFile('https://example.com/events.parquet')

    expect(remote.format).toBe('parquet')
    expect(remote.content).toMatchObject({ kind: 'binary', format: 'parquet' })
  })

  it('maps blocked requests to the CORS hint', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new TypeError('Failed to fetch')))

    await expect(fetchRemoteFile('https://example.com/x.csv')).rejects.toThrow(CORS_HINT.slice(0, 30))
  })

  it('reports HTTP errors with the status', async () => {
    vi.stubGlobal('fetch', mockFetchResponse('', { status: 404 }))

    await expect(fetchRemoteFile('https://example.com/x.csv')).rejects.toThrow('404')
  })

  it('aborts on an oversized Content-Length before downloading', async () => {
    const fetchMock = mockFetchResponse('tiny', { contentLength: FILE_SIZE_LIMITS.parquet.limitBytes + 1 })
    vi.stubGlobal('fetch', fetchMock)

    await expect(fetchRemoteFile('https://example.com/huge.parquet')).rejects.toThrow('comfort zone')
  })

  it('rejects non-http and invalid URLs', async () => {
    await expect(fetchRemoteFile('ftp://example.com/x.csv')).rejects.toThrow('http')
    await expect(fetchRemoteFile('not a url')).rejects.toThrow('valid')
  })

  it('rejects unsupported arrow files', async () => {
    await expect(fetchRemoteFile('https://example.com/frame.arrow')).rejects.toThrow('not supported')
  })

  it('sniffs parquet magic on extension-less URLs instead of mangling bytes as text', async () => {
    vi.stubGlobal(
      'fetch',
      mockFetchResponse(new Uint8Array([0x50, 0x41, 0x52, 0x31, 0xff, 0x00]), {
        url: 'https://example.com/api/download'
      })
    )

    const remote = await fetchRemoteFile('https://example.com/api/download?file=events')

    expect(remote.format).toBe('parquet')
    expect(remote.content).toMatchObject({ kind: 'binary', format: 'parquet' })
  })

  it('prefers the post-redirect file name when it is more truthful', async () => {
    vi.stubGlobal(
      'fetch',
      mockFetchResponse(new Uint8Array([0x50, 0x41, 0x52, 0x31]), {
        url: 'https://cdn.example.com/blobs/events.parquet'
      })
    )

    const remote = await fetchRemoteFile('https://example.com/dl/12345')

    expect(remote.fileName).toBe('events.parquet')
    expect(remote.format).toBe('parquet')
  })
})
