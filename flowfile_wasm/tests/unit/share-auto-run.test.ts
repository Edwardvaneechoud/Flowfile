/**
 * Pins the NARROWNESS of the one exception to the explicit-only execution rule
 * (see no-auto-run.test.ts and CLAUDE.md): only autoRunSharedFlow — invoked by
 * AppLayout after a share-link import once Pyodide is up — may execute without
 * a user Run action. The import itself stays inert.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const pyodideMock = vi.hoisted(() => ({
  isReady: true,
  runPython: vi.fn().mockResolvedValue(undefined),
  runPythonWithResult: vi.fn(),
  runPythonGetBytes: vi.fn(),
  ensurePyPackages: vi.fn(),
  setGlobal: vi.fn(),
  deleteGlobal: vi.fn(),
  packageStatus: {} as Record<string, string>
}))

vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => pyodideMock
}))

vi.mock('../../src/utils/parquet-bridge', () => ({
  parquetToIpcStream: vi.fn(),
  ipcStreamToParquet: vi.fn()
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
    getSavedFlow: vi.fn().mockResolvedValue(null),
    putSavedFlow: vi.fn().mockResolvedValue(undefined),
    putRun: vi.fn().mockResolvedValue(undefined),
    pruneRuns: vi.fn().mockResolvedValue(undefined),
    getAllCatalogDatasets: vi.fn().mockResolvedValue([]),
    putCatalogDataset: vi.fn().mockResolvedValue(undefined),
    deleteCatalogDataset: vi.fn().mockResolvedValue(undefined)
  }
}))

import { useShareLink } from '../../src/composables/useShareLink'
import { useFlowStore } from '../../src/stores/flow-store'
import { encodeShareHash } from '../../src/utils/share-link'
import type { FlowfileData } from '../../src/types'

const flushPromises = () => new Promise((resolve) => setTimeout(resolve, 0))
const executeCalls = () =>
  pyodideMock.runPythonWithResult.mock.calls
    .map((c) => String(c[0]))
    .filter((s) => /execute_/.test(s))

function tinyFlow(): FlowfileData {
  return {
    flowfile_version: '1.0.0',
    flowfile_id: 1,
    flowfile_name: 'Tiny',
    flowfile_settings: {
      description: '',
      execution_mode: 'Development',
      execution_location: 'local',
      auto_save: true,
      show_detailed_progress: false
    },
    nodes: [
      {
        id: 1,
        type: 'manual_input',
        is_start_node: true,
        description: '',
        x_position: 0,
        y_position: 0,
        input_ids: [],
        outputs: [],
        setting_input: {
          node_id: 1,
          is_setup: true,
          raw_data_format: { columns: [{ name: 'a', data_type: 'Int64', values: ['1'] }], data: [{ a: 1 }] }
        }
      }
    ]
  }
}

describe('share auto-run is the only non-user execution trigger, and only after import', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = true
    pyodideMock.runPython.mockResolvedValue(undefined)
    pyodideMock.runPythonWithResult.mockResolvedValue({
      success: true,
      data: { columns: [], data: [], total_rows: 0 }
    })
  })

  it('importing a share hash never executes by itself', async () => {
    const { importShareHash } = useShareLink()
    const hash = await encodeShareHash(tinyFlow())

    const result = await importShareHash(hash)
    await flushPromises()

    expect(result.status).toBe('imported')
    expect(executeCalls()).toEqual([])
  })

  it('autoRunSharedFlow executes the imported flow once Pyodide is ready', async () => {
    const { importShareHash, autoRunSharedFlow } = useShareLink()
    await importShareHash(await encodeShareHash(tinyFlow()))
    await flushPromises()
    pyodideMock.runPythonWithResult.mockClear()

    await autoRunSharedFlow()

    expect(executeCalls().length).toBeGreaterThan(0)
  })

  it('autoRunSharedFlow is a no-op before Pyodide is ready or with an empty canvas', async () => {
    const { importShareHash, autoRunSharedFlow } = useShareLink()

    await autoRunSharedFlow()  // empty canvas
    expect(executeCalls()).toEqual([])

    await importShareHash(await encodeShareHash(tinyFlow()))
    await flushPromises()
    pyodideMock.runPythonWithResult.mockClear()
    pyodideMock.isReady = false

    await autoRunSharedFlow()  // runtime not up yet
    expect(executeCalls()).toEqual([])
  })

  it('a plain file import (not a share link) has no auto-run path of its own', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(tinyFlow())
    await flushPromises()

    expect(executeCalls()).toEqual([])
  })
})
