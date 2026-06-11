/**
 * Write to Catalog node — execution wiring.
 * The node materializes its input to CSV via execute_output (like external_output)
 * and persists it to the Catalog via addCatalogDataset. We mock the Pyodide call
 * to return a CSV "download" and assert the table lands in the catalog.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const mocks = vi.hoisted(() => ({
  runPythonWithResult: vi.fn(),
  putCatalogDataset: vi.fn().mockResolvedValue(undefined)
}))

vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => ({
    isReady: true,
    runPython: vi.fn().mockResolvedValue(undefined),
    runPythonWithResult: mocks.runPythonWithResult,
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
    putCatalogDataset: mocks.putCatalogDataset,
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
import type { NodeWriteToCatalogSettings } from '../../src/types'

const CSV = 'a,b\n1,2\n3,4'

describe('Write to Catalog node', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
  })

  function buildFlow(datasetName: string) {
    const store = useFlowStore()
    const srcId = store.addNode('manual_input', 0, 0)
    const writeId = store.addNode('write_to_catalog', 100, 0)
    store.addEdge({
      id: `e${srcId}-${writeId}`,
      source: String(srcId),
      target: String(writeId),
      sourceHandle: 'output-0',
      targetHandle: 'input-0'
    })
    store.updateNodeSettings(writeId, {
      ...(store.getNode(writeId)!.settings as NodeWriteToCatalogSettings),
      dataset_name: datasetName
    })
    return { store, writeId }
  }

  it('has a default dataset_name of empty string', () => {
    const store = useFlowStore()
    const id = store.addNode('write_to_catalog', 0, 0)
    expect((store.getNode(id)!.settings as NodeWriteToCatalogSettings).dataset_name).toBe('')
  })

  it('writes the materialized CSV into the catalog on execute', async () => {
    mocks.runPythonWithResult.mockResolvedValue({
      success: true,
      schema: [{ name: 'a', data_type: 'Int64' }, { name: 'b', data_type: 'Int64' }],
      download: { content: CSV, file_name: 'sales.csv', file_type: 'csv', mime_type: 'text/csv', row_count: 2 }
    })

    const { store, writeId } = buildFlow('sales')
    const result = await store.executeNode(writeId)

    expect(result.success).toBe(true)
    // It went through the output materialization path. (Search all calls: an
    // always-on schema-propagation call may interleave after the run.)
    const allCalls = mocks.runPythonWithResult.mock.calls.map((c) => c[0] as string)
    expect(allCalls.some((c) => c.includes('execute_output'))).toBe(true)
    // The table is now in the persistent catalog.
    expect(store.getCatalogDatasetNames()).toContain('sales')
    expect(store.getCatalogDatasetContent('sales')).toBe(CSV)
    expect(mocks.putCatalogDataset).toHaveBeenCalledWith({ name: 'sales', content: CSV })
  })

  it('fails clearly when no catalog table name is set', async () => {
    const { store, writeId } = buildFlow('')
    const result = await store.executeNode(writeId)
    expect(result.success).toBe(false)
    expect(result.error).toMatch(/catalog table name/i)
    expect(store.getCatalogDatasetNames()).not.toContain('')
  })

  it('fails clearly when no input is connected', async () => {
    const store = useFlowStore()
    const writeId = store.addNode('write_to_catalog', 0, 0)
    store.updateNodeSettings(writeId, {
      ...(store.getNode(writeId)!.settings as NodeWriteToCatalogSettings),
      dataset_name: 'x'
    })
    const result = await store.executeNode(writeId)
    expect(result.success).toBe(false)
    expect(result.error).toMatch(/no input connected/i)
  })
})
