/**
 * Placeholder-node import: a share link (or a core-saved file) can carry nodes
 * this build cannot run — either as the sentinel type `<type>__unsupported`
 * with a settings stub, or as a bare core-only type. Both must hydrate as
 * locked placeholders, keep their edges (including secondary handles), and
 * block their downstream instead of failing it.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

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
import {
  isPlaceholderNode,
  originalNodeType,
  placeholderHandleCounts,
  placeholderLabel,
  placeholderReason
} from '../../src/utils/placeholder'
import type { FlowfileData } from '../../src/types'

function shareFlow(): FlowfileData {
  return {
    flowfile_version: '1.0.0',
    flowfile_id: 1,
    flowfile_name: 'Shared Flow',
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
        outputs: [3],
        setting_input: {
          node_id: 1,
          is_setup: true,
          raw_data_format: {
            columns: [{ name: 'a', data_type: 'Int64', values: ['1', '2'] }]
          }
        }
      },
      // Bare core-only type: detected locally, no stub.
      {
        id: 2,
        type: 'database_reader',
        is_start_node: true,
        description: '',
        x_position: 0,
        y_position: 100,
        input_ids: [],
        outputs: [3],
        setting_input: { node_id: 2, is_setup: true }
      },
      // Sentinel placeholder minted by core, 2-input stub.
      {
        id: 3,
        type: 'fuzzy_match__unsupported',
        is_start_node: false,
        description: '',
        x_position: 200,
        y_position: 50,
        input_ids: [1],
        right_input_id: 2,
        outputs: [4],
        setting_input: {
          is_placeholder: true,
          original_type: 'fuzzy_match',
          reason: 'Runs only in the full Flowfile app',
          label: 'Fuzzy Match',
          inputs: 2,
          outputs: 1
        }
      },
      {
        id: 4,
        type: 'filter',
        is_start_node: false,
        description: '',
        x_position: 400,
        y_position: 50,
        input_ids: [3],
        outputs: [],
        setting_input: {
          node_id: 4,
          is_setup: true,
          filter_input: { mode: 'basic', basic_filter: { field: 'a', operator: 'equals', value: '1' } }
        }
      }
    ],
    connections: [
      { from_node: 1, to_node: 3, from_handle: 'output-0', to_handle: 'input-0' },
      { from_node: 2, to_node: 3, from_handle: 'output-0', to_handle: 'input-1' },
      { from_node: 3, to_node: 4, from_handle: 'output-0', to_handle: 'input-0' }
    ]
  }
}

describe('placeholder nodes import as locked, connected, blocked-downstream', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = false
  })

  it('detects both sentinel-typed and bare core-only nodes as placeholders', () => {
    const store = useFlowStore()
    expect(store.importFromFlowfile(shareFlow())).toBe(true)

    expect(store.isPlaceholderNode(1)).toBe(false)
    expect(store.isPlaceholderNode(2)).toBe(true)
    expect(store.isPlaceholderNode(3)).toBe(true)
    expect(store.isPlaceholderNode(4)).toBe(false)

    const n3 = store.getNode(3)!
    expect(originalNodeType(n3.type, n3.settings)).toBe('fuzzy_match')
    expect(placeholderLabel(n3.type, n3.settings)).toBe('Fuzzy Match')
    expect(placeholderReason(n3.settings)).toBe('Runs only in the full Flowfile app')
  })

  it('keeps explicit connections, including the second input handle', () => {
    const store = useFlowStore()
    store.importFromFlowfile(shareFlow())

    const rightEdge = store.edges.find(e => e.target === '3' && e.targetHandle === 'input-1')
    expect(rightEdge).toBeTruthy()
    expect(rightEdge!.source).toBe('2')
    expect(store.edges).toHaveLength(3)
  })

  it('derives handle counts from the stub and floors them by the edges', () => {
    const store = useFlowStore()
    store.importFromFlowfile(shareFlow())

    const n3 = store.getNode(3)!
    const counts = placeholderHandleCounts(3, n3.settings, store.edges)
    expect(counts).toEqual({ inputs: 2, outputs: 1 })

    // An under-reporting stub cannot hide the second input: the edge floor wins.
    const counts2 = placeholderHandleCounts(3, { is_placeholder: true, inputs: 1 }, store.edges)
    expect(counts2.inputs).toBe(2)
  })

  it('blocks placeholders and everything downstream of them, with traceable reasons', () => {
    const store = useFlowStore()
    store.importFromFlowfile(shareFlow())

    expect(store.getBlockedInfo(1)).toBeUndefined()
    expect(store.getBlockedInfo(2)?.reason).toBe('placeholder')
    expect(store.getBlockedInfo(3)?.reason).toBe('placeholder')
    const downstream = store.getBlockedInfo(4)
    expect(downstream?.reason).toBe('upstream_placeholder')
    expect(downstream?.sourceNodeId).toBe(3)
    expect(downstream?.message).toContain('Fuzzy Match')
  })

  it('detecting a placeholder never mutates its settings (round-trips verbatim)', () => {
    const store = useFlowStore()
    const flow = shareFlow()
    store.importFromFlowfile(flow)

    const n3 = store.getNode(3)!
    expect(isPlaceholderNode(n3.type, n3.settings)).toBe(true)
    expect((n3.settings as any).is_placeholder).toBe(true)
    expect((n3.settings as any).original_type).toBe('fuzzy_match')
  })
})
