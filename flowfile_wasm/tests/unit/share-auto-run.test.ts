/**
 * Pins the NARROWNESS of the one exception to the explicit-only execution rule
 * (see no-auto-run.test.ts and CLAUDE.md): only autoRunSharedFlow — invoked by
 * AppLayout after a share-link import once Pyodide is up — may execute without
 * a user Run action. The import itself stays inert, and the exception never
 * covers the sender's code: a Polars Code node stays locked until the recipient
 * chooses "Trust and run".
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

const CANARY = 'CANARY_shared_code_7c1'

const flushPromises = () => new Promise((resolve) => setTimeout(resolve, 0))
const executeCalls = () =>
  pyodideMock.runPythonWithResult.mock.calls
    .map((c) => String(c[0]))
    .filter((s) => /execute_/.test(s))
/** Everything handed to Python, in any form. */
const bridgeTraffic = () => [
  ...pyodideMock.runPythonWithResult.mock.calls.map((c) => String(c[0])),
  ...pyodideMock.runPython.mock.calls.map((c) => String(c[0])),
  ...pyodideMock.setGlobal.mock.calls.map((c) => JSON.stringify(c[1] ?? ''))
]

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

/** manual_input → polars_code (the sender's Python) → select, plus a select beside the code. */
function codeFlow(): FlowfileData {
  const flow = tinyFlow()
  flow.flowfile_name = 'Carries code'
  flow.nodes[0].outputs = [2, 4]
  flow.nodes.push(
    {
      id: 2,
      type: 'polars_code',
      is_start_node: false,
      description: '',
      x_position: 200,
      y_position: 0,
      input_ids: [1],
      outputs: [3],
      setting_input: { node_id: 2, is_setup: true, polars_code_input: { polars_code: CANARY } }
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
    },
    {
      id: 4,
      type: 'select',
      is_start_node: false,
      description: '',
      x_position: 200,
      y_position: 200,
      input_ids: [1],
      outputs: [],
      setting_input: { node_id: 4, is_setup: true, select_input: [] }
    }
  )
  return flow
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

describe('a shared flow never runs its sender\'s code until the recipient trusts it', () => {
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

  it('import, schema propagation and the auto-run all leave the code node locked', async () => {
    const { importShareHash, autoRunSharedFlow } = useShareLink()
    const store = useFlowStore()

    await importShareHash(await encodeShareHash(codeFlow()))
    await flushPromises()
    await autoRunSharedFlow()

    for (const sent of bridgeTraffic()) expect(sent).not.toContain(CANARY)
    expect(store.untrustedCodeNodes).toEqual([{ nodeId: 2, label: 'Polars Code' }])
    expect(store.nodeResults.get(2)?.blocked?.reason).toBe('untrusted_code')
    expect(store.nodeResults.get(3)?.blocked?.reason).toBe('upstream_untrusted_code')
    // The part that carries no code still ran, and a lock is not a failure.
    expect(store.nodeResults.get(1)?.success).toBe(true)
    expect(store.nodeResults.get(4)?.success).toBe(true)
    expect(store.executionError).toBeNull()
  })

  it('"Trust and run" unlocks the code and runs the whole flow', async () => {
    const { importShareHash, trustAndRunSharedFlow } = useShareLink()
    const store = useFlowStore()
    await importShareHash(await encodeShareHash(codeFlow()))
    await flushPromises()

    await trustAndRunSharedFlow()

    expect(executeCalls().some((call) => call.includes('execute_polars_code') && call.includes(CANARY))).toBe(true)
    expect(store.untrustedCodeNodes).toEqual([])
    expect(store.blockedNodes.size).toBe(0)
    expect(store.nodeResults.get(2)?.blocked).toBeUndefined()
    expect(store.nodeResults.get(3)?.success).toBe(true)
  })

  it('the lock follows the flow through a tab switch', async () => {
    const { importShareHash } = useShareLink()
    const store = useFlowStore()
    await importShareHash(await encodeShareHash(codeFlow()))
    await flushPromises()

    const stashed = store.captureSnapshot()
    store.clearFlow()
    expect(store.untrustedCodeNodes).toEqual([])
    store.loadFromSnapshot(stashed)
    await flushPromises()

    expect(store.untrustedCodeNodes.map((node) => node.nodeId)).toEqual([2])
    for (const sent of bridgeTraffic()) expect(sent).not.toContain(CANARY)
  })

  it('the lock survives a page reload', async () => {
    const { importShareHash } = useShareLink()
    await importShareHash(await encodeShareHash(codeFlow()))
    // The session save is debounced.
    await new Promise((resolve) => setTimeout(resolve, 200))

    setActivePinia(createPinia())
    const reloaded = useFlowStore()
    await flushPromises()
    await reloaded.propagateSchemas()

    expect(reloaded.nodes.size).toBe(4)
    expect(reloaded.untrustedCodeNodes.map((node) => node.nodeId)).toEqual([2])
    for (const sent of bridgeTraffic()) expect(sent).not.toContain(CANARY)
  })

  it('a shared flow without code nodes needs no trust', async () => {
    const { importShareHash } = useShareLink()
    const store = useFlowStore()
    await importShareHash(await encodeShareHash(tinyFlow()))
    await flushPromises()

    expect(store.untrustedCodeNodes).toEqual([])
    expect(store.blockedNodes.size).toBe(0)
  })
})
