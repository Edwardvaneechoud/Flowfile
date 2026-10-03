/**
 * THE security invariant of share-link placeholders: a placeholder node's
 * settings must never cross the JS↔Python bridge — not during execution and
 * not during schema propagation (which execs polars_code automatically). A
 * malicious link that smuggles code into a stub must find no path to exec.
 *
 * The same holds for a real Polars Code node that arrived by share link, until
 * the recipient trusts the flow.
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

/** A share payload whose Polars Code node is real, not a stub: manual_input → polars_code → select. */
function codeFlow(): FlowfileData {
  const flow = hostileFlow()
  flow.nodes[1] = {
    ...flow.nodes[1],
    type: 'polars_code',
    setting_input: { node_id: 2, is_setup: true, polars_code_input: { polars_code: CANARY } }
  }
  return flow
}

describe('untrusted shared code never reaches the Pyodide bridge', () => {
  const expectNoCanary = () => {
    for (const src of bridgeStrings()) expect(src).not.toContain(CANARY)
    for (const payload of globalPayloads()) expect(payload).not.toContain(CANARY)
  }

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

  it('schema propagation omits the code node', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(codeFlow(), { untrustedCode: true })
    await flushPromises()

    await store.propagateSchemas()

    expect(bridgeStrings().some((src) => src.includes('propagate_schemas('))).toBe(true)
    expectNoCanary()
  })

  it('the TypeScript fallback never lazily executes it', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(codeFlow(), { untrustedCode: true })
    await flushPromises()
    await store.executeNode(1)
    // A failing engine pass sends propagation down the TS path, which runs code nodes whose input has data.
    pyodideMock.runPythonWithResult.mockImplementation(async (src: string) => {
      if (String(src).includes('propagate_schemas(')) throw new Error('engine pass unavailable')
      return { success: true, data: { columns: [], data: [], total_rows: 0 } }
    })

    await store.propagateSchemas()

    expect(bridgeStrings().some((src) => src.includes('execute_polars_code'))).toBe(false)
    expectNoCanary()
  })

  it('Run flow, Run now and an upstream run all stop at the lock', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(codeFlow(), { untrustedCode: true })
    await flushPromises()

    await store.executeFlow()
    const runNow = await store.executeNode(2)
    const upstream = await store.executeNodeWithUpstream(3)

    expectNoCanary()
    expect(runNow.blocked?.reason).toBe('untrusted_code')
    expect(runNow.success).toBeUndefined()
    expect(upstream.blocked).toBeTruthy()
    expect(store.nodeResults.get(1)?.success).toBe(true)
    expect(store.nodeResults.get(3)?.blocked?.reason).toBe('upstream_untrusted_code')
    expect(store.executionError).toBeNull()
  })

  it('stays out of the payload when a placeholder already blocks it', async () => {
    const flow = codeFlow()
    flow.nodes.splice(1, 0, {
      ...hostileFlow().nodes[1],
      id: 4,
      input_ids: [1],
      outputs: [2],
      setting_input: { is_placeholder: true, original_type: 'sql_query', reason: 'Runs only in the full Flowfile app' }
    })
    flow.nodes[0].outputs = [4]
    flow.nodes[2].input_ids = [4]
    flow.connections = [
      { from_node: 1, to_node: 4, from_handle: 'output-0', to_handle: 'input-0' },
      { from_node: 4, to_node: 2, from_handle: 'output-0', to_handle: 'input-0' },
      { from_node: 2, to_node: 3, from_handle: 'output-0', to_handle: 'input-0' }
    ]
    const store = useFlowStore()
    store.importFromFlowfile(flow, { untrustedCode: true })
    await flushPromises()

    await store.propagateSchemas()
    await store.executeFlow()

    // The permanent block wins the label; the code is withheld either way.
    expect(store.blockedNodes.get(2)?.reason).toBe('upstream_placeholder')
    expectNoCanary()
  })

  it('trusting the flow lifts the lock, and only then does the code run', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(codeFlow(), { untrustedCode: true })
    await flushPromises()
    await store.executeFlow()
    expectNoCanary()

    store.trustSharedCode()
    expect(store.blockedNodes.size).toBe(0)
    expect(store.nodeResults.get(2)?.blocked).toBeUndefined()
    await store.executeFlow()

    expect(bridgeStrings().some((src) => src.includes('execute_polars_code') && src.includes(CANARY))).toBe(true)
  })

  it('a flow that did not come from a share link is never locked', async () => {
    const store = useFlowStore()
    store.importFromFlowfile(codeFlow())
    await flushPromises()

    expect(store.untrustedCodeNodes).toEqual([])
    expect(store.blockedNodes.size).toBe(0)
  })
})
