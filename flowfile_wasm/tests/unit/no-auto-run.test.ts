/**
 * Regression guard for the "Execution is explicit-only" rule (see
 * flowfile_wasm/CLAUDE.md). Data must run ONLY when the user clicks a Run action
 * (Run flow / Run Now / Apply / Fetch data). Selecting a node and opening the
 * Settings/Table panels must NEVER execute the pipeline — at most they may
 * materialize a *preview* of an already-computed node.
 *
 * Execution goes through bare-name `execute_<type>(...)` Python bridges;
 * previews go through `fetch_preview(...)`. We assert on the strings passed to
 * the mocked `runPythonWithResult` to tell the two apart.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const pyodideMock = vi.hoisted(() => ({
  isReady: true,
  runPython: vi.fn(),
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

const parquetBridgeMock = vi.hoisted(() => ({
  parquetToIpcStream: vi.fn(),
  ipcStreamToParquet: vi.fn()
}))

vi.mock('../../src/utils/parquet-bridge', () => parquetBridgeMock)

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

import { useFlowStore } from '../../src/stores/flow-store'

const flushPromises = () => new Promise((resolve) => setTimeout(resolve, 0))

// Bridge strings sent to Python, split by what they do.
const bridgeStrings = () =>
  pyodideMock.runPythonWithResult.mock.calls.map((call) => String(call[0]))
const executeCalls = () => bridgeStrings().filter((src) => /execute_/.test(src))
const previewCalls = () => bridgeStrings().filter((src) => /fetch_preview\(/.test(src))

describe('Execution is explicit-only: opening panels never runs the pipeline', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = true
    pyodideMock.runPythonWithResult.mockResolvedValue({
      success: true,
      data: { columns: [], data: [], total_rows: 0 }
    })
  })

  it('selecting an un-run node with the Table open neither executes nor previews', async () => {
    const store = useFlowStore()
    const id = store.addNode('manual_input', 0, 0)

    // Isolate the assertion to the act of opening/selecting (ignore addNode setup).
    pyodideMock.runPythonWithResult.mockClear()

    store.showTablePreview = true
    store.selectNode(id)
    await flushPromises()

    expect(executeCalls()).toEqual([])
    expect(previewCalls()).toEqual([])
  })

  it('selecting an already-run node with the Table open previews but never executes', async () => {
    const store = useFlowStore()
    const id = store.addNode('manual_input', 0, 0)
    // Simulate a node that has already been run (e.g. by a prior Run flow).
    store.nodeResults.set(id, { success: true })

    pyodideMock.runPythonWithResult.mockClear()

    store.showTablePreview = true
    store.selectNode(id)
    await flushPromises()

    expect(executeCalls()).toEqual([])
    expect(previewCalls().length).toBeGreaterThan(0)
  })

  it('selecting an already-run node with the Table CLOSED does not even preview', async () => {
    const store = useFlowStore()
    const id = store.addNode('manual_input', 0, 0)
    store.nodeResults.set(id, { success: true })
    // Let addNode's deferred schema propagation settle so it doesn't bleed in.
    await flushPromises()

    pyodideMock.runPythonWithResult.mockClear()

    // Table panel closed → a plain settings-select must not fetch a preview
    // (and certainly not execute). Schema propagation, if any, is allowed.
    store.showTablePreview = false
    store.selectNode(id)
    await flushPromises()

    expect(executeCalls()).toEqual([])
    expect(previewCalls()).toEqual([])
  })
})
