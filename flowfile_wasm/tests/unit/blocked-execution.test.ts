/**
 * THE security invariant of share-link placeholders: a placeholder node's
 * settings must never cross the JS↔Python bridge — not during execution and
 * not during schema propagation (which execs polars_code / evals advanced
 * filters automatically). A malicious link that smuggles code into a stub must
 * find no path to exec.
 *
 * Asserted the way no-auto-run.test.ts does: on the literal strings handed to
 * the mocked Pyodide bridge.
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

import { useFlowStore } from '../../src/stores/flow-store'
import type { FlowfileData } from '../../src/types'

const CANARY = 'CANARY_EXEC_import_js_beacon_9f3'

const flushPromises = () => new Promise((resolve) => setTimeout(resolve, 0))
const bridgeStrings = () => [
  ...pyodideMock.runPythonWithResult.mock.calls.map((c) => String(c[0])),
  ...pyodideMock.runPython.mock.calls.map((c) => String(c[0]))
]
const globalPayloads = () => pyodideMock.setGlobal.mock.calls.map((c) => JSON.stringify(c[1] ?? ''))

/** A hostile share payload: a sentinel placeholder that ALSO smuggles a
 * polars_code body inside the stub, plus a runnable downstream node. */
function hostileFlow(): FlowfileData {
  return {
    flowfile_version: '1.0.0',
    flowfile_id: 1,
    flowfile_name: 'Hostile',
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
        outputs: [2],
        setting_input: {
          node_id: 1,
          is_setup: true,
          raw_data_format: { columns: [{ name: 'a', data_type: 'Int64', values: ['1'] }], data: [{ a: 1 }] }
        }
      },
      {
        id: 2,
        type: 'polars_code__unsupported',
        is_start_node: false,
        description: '',
        x_position: 200,
        y_position: 0,
        input_ids: [1],
        outputs: [3],
        setting_input: {
          is_placeholder: true,
          original_type: 'polars_code',
          reason: 'Custom Python code does not travel in share links',
          polars_code_input: { polars_code: CANARY }
        }
      },
      {
        id: 3,
        type: 'select',
        is_start_node: false,
        description: '',
        x_position: 400,
        y_position: 0,
        input_ids: [2],
        outputs: [],
        setting_input: { node_id: 3, is_setup: true, select_input: [] }
      }
    ],
    connections: [
      { from_node: 1, to_node: 2, from_handle: 'output-0', to_handle: 'input-0' },
      { from_node: 2, to_node: 3, from_handle: 'output-0', to_handle: 'input-0' }
    ]
  }
}

describe('placeholder settings never reach the Pyodide bridge', () => {
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

  it('schema propagation omits the placeholder and its smuggled code', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(hostileFlow())
    await flushPromises()

    await store.propagateSchemas()

    for (const src of bridgeStrings()) expect(src).not.toContain(CANARY)
    for (const payload of globalPayloads()) expect(payload).not.toContain(CANARY)
  })

  it('executeNode on the placeholder returns blocked without touching Python', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(hostileFlow())
    await flushPromises()
    vi.clearAllMocks()

    const result = await store.executeNode(2)
    expect(result.blocked?.reason).toBe('placeholder')
    expect(result.success).toBeUndefined()
    expect(bridgeStrings().filter((s) => /execute_/.test(s))).toEqual([])
  })

  it('executeFlow runs the runnable subgraph, blocks the rest, reports no failure', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(hostileFlow())
    await flushPromises()

    await store.executeFlow()

    // The smuggled code never crossed the bridge in any form.
    for (const src of bridgeStrings()) expect(src).not.toContain(CANARY)
    for (const payload of globalPayloads()) expect(payload).not.toContain(CANARY)

    // Node 1 ran; 2 and 3 are blocked (not failed); the run is not an error.
    expect(store.nodeResults.get(1)?.success).toBe(true)
    expect(store.nodeResults.get(2)?.blocked?.reason).toBe('placeholder')
    expect(store.nodeResults.get(2)?.success).toBeUndefined()
    expect(store.nodeResults.get(3)?.blocked?.reason).toBe('upstream_placeholder')
    expect(store.nodeResults.get(3)?.success).toBeUndefined()
    expect(store.executionError).toBeNull()
  })

  it('executeNodeWithUpstream surfaces the blocked ancestor instead of running past it', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(hostileFlow())
    await flushPromises()
    vi.clearAllMocks()

    pyodideMock.runPythonWithResult.mockResolvedValue({
      success: true,
      data: { columns: [], data: [], total_rows: 0 }
    })
    const result = await store.executeNodeWithUpstream(3)
    expect(result.blocked).toBeTruthy()
    for (const src of bridgeStrings()) expect(src).not.toContain(CANARY)
  })
})
