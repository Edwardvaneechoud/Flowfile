/**
 * Remote auto-refetch tests.
 * Read nodes whose content came from a URL keep it in received_file.path;
 * refetchRemoteFiles re-downloads missing content so flows opened from a
 * share link, file or library hydrate themselves.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => ({
    isReady: false,
    runPython: vi.fn(),
    runPythonWithResult: vi.fn(),
    setGlobal: vi.fn(),
    deleteGlobal: vi.fn()
  })
}))

vi.mock('../../src/stores/file-storage', () => ({
  SIZE_THRESHOLD: 5 * 1024 * 1024,
  fileStorage: {
    setFileContent: vi.fn().mockResolvedValue(undefined),
    getFileContent: vi.fn().mockResolvedValue(null),
    deleteFileContent: vi.fn().mockResolvedValue(undefined),
    getDownloadContent: vi.fn().mockResolvedValue(null),
    setDownloadContent: vi.fn().mockResolvedValue(undefined),
    clearAll: vi.fn().mockResolvedValue(undefined),
    shouldUseIndexedDB: vi.fn().mockReturnValue(false),
    getAllCatalogDatasets: vi.fn().mockResolvedValue([]),
    putCatalogDataset: vi.fn().mockResolvedValue(undefined),
    deleteCatalogDataset: vi.fn().mockResolvedValue(undefined),
    getAllRecentFlows: vi.fn().mockResolvedValue([]),
    putRecentFlow: vi.fn().mockResolvedValue(undefined),
    pruneRecentFlows: vi.fn().mockResolvedValue(undefined),
    getRecentFlow: vi.fn().mockResolvedValue(null),
    getAllSavedFlows: vi.fn().mockResolvedValue([]),
    getSavedFlow: vi.fn().mockResolvedValue(null),
    putSavedFlow: vi.fn().mockResolvedValue(undefined),
    deleteSavedFlow: vi.fn().mockResolvedValue(undefined),
    duplicateSavedFlow: vi.fn().mockResolvedValue(null),
    putRun: vi.fn().mockResolvedValue(undefined),
    pruneRuns: vi.fn().mockResolvedValue(undefined)
  }
}))

import { useFlowStore } from '../../src/stores/flow-store'
import type { NodeReadSettings } from '../../src/types'

const CSV_URL = 'https://raw.githubusercontent.com/user/repo/main/data.csv'
const CSV_BODY = 'name,age\nalice,30\nbob,25\n'

function csvResponse(body = CSV_BODY, url = CSV_URL) {
  const bytes = new TextEncoder().encode(body)
  return {
    ok: true,
    status: 200,
    statusText: 'OK',
    url,
    headers: { get: (h: string) => (h === 'content-length' ? String(bytes.length) : null) },
    arrayBuffer: () => Promise.resolve(bytes.buffer)
  }
}

function addUrlReadNode(flow: ReturnType<typeof useFlowStore>, url = CSV_URL): number {
  const id = flow.addNode('read', 0, 0)
  const settings = flow.getNode(id)!.settings as NodeReadSettings
  settings.file_name = 'data.csv'
  settings.received_file = {
    name: 'data.csv',
    path: url,
    file_type: 'csv',
    table_settings: { file_type: 'csv', delimiter: ',', has_headers: true, encoding: 'utf-8' }
  } as NodeReadSettings['received_file']
  return id
}

describe('refetchRemoteFiles', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
  })

  it('downloads content for URL-sourced read nodes with none loaded', async () => {
    const flow = useFlowStore()
    const id = addUrlReadNode(flow)
    const fetchMock = vi.fn().mockResolvedValue(csvResponse())
    vi.stubGlobal('fetch', fetchMock)

    const failures = await flow.refetchRemoteFiles()

    expect(failures).toEqual([])
    expect(fetchMock).toHaveBeenCalledWith(CSV_URL)
    expect(flow.hasFileContent(id)).toBe(true)
    // setFileContent infers a source schema from the CSV text.
    expect(flow.nodeResults.get(id)?.schema?.map((c) => c.name)).toEqual(['name', 'age'])
  })

  it('skips nodes that already have content and nodes without a URL', async () => {
    const flow = useFlowStore()
    const withContent = addUrlReadNode(flow)
    flow.setFileContent(withContent, 'a,b\n1,2\n')
    const localId = flow.addNode('read', 0, 0)
    ;(flow.getNode(localId)!.settings as NodeReadSettings).received_file!.path = 'local.csv'
    const fetchMock = vi.fn().mockResolvedValue(csvResponse())
    vi.stubGlobal('fetch', fetchMock)

    const failures = await flow.refetchRemoteFiles()

    expect(failures).toEqual([])
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('dedupes concurrent fetches for the same node', async () => {
    const flow = useFlowStore()
    addUrlReadNode(flow)
    let resolveFetch!: (r: unknown) => void
    const fetchMock = vi.fn().mockReturnValue(new Promise((r) => (resolveFetch = r)))
    vi.stubGlobal('fetch', fetchMock)

    const first = flow.refetchRemoteFiles()
    const second = flow.refetchRemoteFiles()
    resolveFetch(csvResponse())
    await Promise.all([first, second])

    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('reports failures with the node file name and a message', async () => {
    const flow = useFlowStore()
    const id = addUrlReadNode(flow)
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({ ...csvResponse(), ok: false, status: 404, statusText: 'Not Found' })
    )

    const failures = await flow.refetchRemoteFiles()

    expect(failures).toHaveLength(1)
    expect(failures[0].nodeId).toBe(id)
    expect(failures[0].fileName).toBe('data.csv')
    expect(failures[0].error).toContain('404')
    expect(flow.hasFileContent(id)).toBe(false)
  })
})

describe('getMissingFileNodes with URL sources', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
  })

  it('does not flag URL-sourced nodes as missing (they self-heal)', () => {
    const flow = useFlowStore()
    addUrlReadNode(flow)
    const localId = flow.addNode('read', 0, 0)
    const settings = flow.getNode(localId)!.settings as NodeReadSettings
    settings.file_name = 'picked.csv'
    settings.received_file!.path = 'picked.csv'

    const missing = flow.getMissingFileNodes()

    expect(missing).toEqual([{ nodeId: localId, fileName: 'picked.csv' }])
  })
})
