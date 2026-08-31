/**
 * The import direction the share feature depends on: a genuinely
 * flowfile_core-authored flow file through importFromFlowfile. The export
 * suites (flowfile-core-export, core-execution-parity) prove wasm→core; this
 * proves core→wasm — types resolve, nothing becomes a placeholder for an
 * all-supported flow, and a core-only node type degrades to exactly one
 * placeholder with its descendants blocked (not failed).
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import yaml from 'js-yaml'

const pyodideMock = vi.hoisted(() => ({
  isReady: false,
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

import { useFlowStore } from '../../src/stores/flow-store'
import { isSupportedNodeType } from '../../src/config/nodeCatalog'
import type { FlowfileData } from '../../src/types'

const SALES_PIPELINE = resolve(__dirname, '../../../docs/assets/flows/sales_pipeline.yaml')

function loadSalesPipeline(): FlowfileData {
  return yaml.load(readFileSync(SALES_PIPELINE, 'utf-8')) as FlowfileData
}

describe('a core-authored flow file imports into the browser editor', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = false
  })

  it('sales_pipeline.yaml: every node resolves to a supported editor type, zero placeholders', () => {
    const store = useFlowStore()
    const data = loadSalesPipeline()
    expect(store.importFromFlowfile(data)).toBe(true)

    expect(store.nodes.size).toBe(data.nodes.length)
    for (const [id, node] of store.nodes) {
      expect(isSupportedNodeType(node.type), `node ${id} (${node.type}) should be supported`).toBe(true)
      expect(store.isPlaceholderNode(id)).toBe(false)
    }
    expect(store.blockedNodes.size).toBe(0)
    // Edges derived from core's implicit input_ids topology: a linear pipeline.
    expect(store.edges.length).toBe(data.nodes.length - 1)
  })

  it('the same flow with one core-only node degrades to exactly one placeholder + blocked descendants', () => {
    const store = useFlowStore()
    const data = loadSalesPipeline()
    // Swap the reader for a database reader — a full-app-only source.
    const reader = data.nodes.find(n => n.type === 'read')!
    reader.type = 'database_reader'
    reader.setting_input = { node_id: reader.id, is_setup: true }

    expect(store.importFromFlowfile(data)).toBe(true)

    const placeholders = [...store.nodes.keys()].filter(id => store.isPlaceholderNode(id))
    expect(placeholders).toEqual([reader.id])

    // The source is a placeholder, so the whole pipeline downstream is blocked.
    expect(store.getBlockedInfo(reader.id)?.reason).toBe('placeholder')
    for (const [id] of store.nodes) {
      if (id === reader.id) continue
      expect(store.getBlockedInfo(id)?.reason, `node ${id} should be blocked`).toBe('upstream_placeholder')
      expect(store.nodeResults.get(id)?.success).toBeUndefined()
    }
  })
})
