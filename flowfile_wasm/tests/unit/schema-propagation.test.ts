/**
 * Lazy schema propagation (Python-authority path) tests.
 *
 * These exercise the JS adapter that feeds the always-on Pyodide schema pass
 * and applies its results to nodeResults — proving that downstream node panels
 * get their input columns WITHOUT running the flow. The Pyodide runtime itself
 * is mocked (the suite never boots real Polars); runPythonWithResult returns a
 * canned propagate_schemas() result so we validate the wiring deterministically.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const runPythonWithResult = vi.fn()

vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => ({
    isReady: true,
    runPython: vi.fn().mockResolvedValue(undefined),
    runPythonWithResult,
    setGlobal: vi.fn(),
    deleteGlobal: vi.fn()
  })
}))

vi.mock('../../src/stores/file-storage', () => ({
  fileStorage: {
    setFileContent: vi.fn().mockResolvedValue(undefined),
    getFileContent: vi.fn().mockResolvedValue(null),
    deleteFileContent: vi.fn().mockResolvedValue(undefined),
    getDownloadContent: vi.fn().mockResolvedValue(null),
    setDownloadContent: vi.fn().mockResolvedValue(undefined),
    clearAll: vi.fn().mockResolvedValue(undefined),
    shouldUseIndexedDB: vi.fn().mockReturnValue(false)
  },
  SIZE_THRESHOLD: 5 * 1024 * 1024
}))

import { useFlowStore } from '../../src/stores/flow-store'

function connect(store: ReturnType<typeof useFlowStore>, source: number, target: number, handle = 'input-0') {
  store.addEdge({
    id: `e${source}-${target}`,
    source: String(source),
    target: String(target),
    sourceHandle: 'output-0',
    targetHandle: handle
  })
}

describe('Lazy schema propagation', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
  })

  it('propagates schema through a multi-hop chain without executing', async () => {
    const store = useFlowStore()
    const readId = store.addNode('read', 0, 0)
    const selectId = store.addNode('select', 200, 0)
    const groupId = store.addNode('group_by', 400, 0)
    connect(store, readId, selectId)
    connect(store, selectId, groupId)

    // What the always-on Polars pass would return: select renames a->id, keeps amount;
    // group_by counts by id.
    runPythonWithResult.mockResolvedValue({
      [readId]: { schema: [{ name: 'a', data_type: 'Int64' }, { name: 'amount', data_type: 'Float64' }], schema_resolved: true },
      [selectId]: { schema: [{ name: 'id', data_type: 'Int64' }, { name: 'amount', data_type: 'Float64' }], schema_resolved: true },
      [groupId]: { schema: [{ name: 'id', data_type: 'Int64' }, { name: 'cnt', data_type: 'Int64' }], schema_resolved: true }
    })

    await store.propagateSchemas()

    // Group By can see Select's (renamed) output columns without any run.
    expect(store.getNodeInputSchema(groupId).map((c) => c.name)).toEqual(['id', 'amount'])
    // And its own resolved output schema is recorded — but as inferred, not executed.
    const groupResult = store.getNodeResult(groupId)
    expect(groupResult?.schema?.map((c) => c.name)).toEqual(['id', 'cnt'])
    expect(groupResult?.success).toBeUndefined()
  })

  it('flags a node whose upstream output is data-dependent (pivot)', async () => {
    const store = useFlowStore()
    const readId = store.addNode('read', 0, 0)
    const pivotId = store.addNode('pivot', 200, 0)
    const selectId = store.addNode('select', 400, 0)
    connect(store, readId, pivotId)
    connect(store, pivotId, selectId)

    runPythonWithResult.mockResolvedValue({
      [readId]: { schema: [{ name: 'a', data_type: 'Int64' }], schema_resolved: true },
      [pivotId]: { schema: [], schema_resolved: false, error: 'Pivot output columns depend on the data; run the flow.' },
      [selectId]: { schema: [], schema_resolved: false, error: 'Upstream schema unavailable' }
    })

    await store.propagateSchemas()

    // The select sitting after the pivot is told its input schema isn't known yet.
    expect(store.isInputSchemaResolved(selectId)).toBe(false)
  })

  it('feeds last-known schemas to the Python pass so opaque upstreams do not freeze downstream', async () => {
    const store = useFlowStore()
    const codeId = store.addNode('polars_code', 0, 0)
    const groupId = store.addNode('group_by', 200, 0)
    connect(store, codeId, groupId)

    // Simulate a post-run state: Python resolves both nodes, caching their
    // schemas into nodeResults.
    runPythonWithResult.mockResolvedValue({
      [codeId]: { schema: [{ name: 'column_0', data_type: 'Int64' }], schema_resolved: true },
      [groupId]: { schema: [{ name: 'column_0_mean', data_type: 'Float64' }], schema_resolved: true }
    })
    await store.propagateSchemas()
    // Second pass now carries the cached schemas as the third propagate_schemas arg.
    await store.propagateSchemas()

    const lastCode = runPythonWithResult.mock.calls.at(-1)![0] as string
    // graph + source_schemas + known_schemas = three json.loads payloads.
    expect((lastCode.match(/json\.loads\(/g) || []).length).toBe(3)
    // The opaque polars_code start node and the group_by output are passed as known.
    expect(lastCode).toContain('column_0_mean')
    expect(lastCode).toContain('column_0')
  })

  // Mock the engine: the _lazyframes.keys() probe returns `builtIds`; every other
  // call (execute_*) reports success.
  function mockEngine(builtIds: number[]) {
    runPythonWithResult.mockImplementation((code: string) => {
      if (code.includes('_lazyframes.keys()')) return Promise.resolve(builtIds)
      return Promise.resolve({ success: true, schema: [{ name: 'a', data_type: 'Int64' }] })
    })
  }

  function buildChain(store: ReturnType<typeof useFlowStore>) {
    const readId = store.addNode('read', 0, 0)
    const filterId = store.addNode('filter', 200, 0)
    const uniqueId = store.addNode('unique', 400, 0)
    connect(store, readId, filterId)
    connect(store, filterId, uniqueId)
    store.setFileContent(readId, 'a,b\n1,2\n3,4')
    return { readId, filterId, uniqueId }
  }

  it('builds the full chain once when nothing is built yet', async () => {
    const store = useFlowStore()
    const { uniqueId } = buildChain(store)
    runPythonWithResult.mockClear()
    mockEngine([]) // fresh runtime: no pointers wired

    const result = await store.executeNodeWithUpstream(uniqueId)
    expect(result.success).toBe(true)

    // Read, then filter, then unique are wired in topological order.
    const codes = runPythonWithResult.mock.calls.map((c) => c[0] as string)
    const readIdx = codes.findIndex((c) => c.includes('execute_read_csv'))
    const filterIdx = codes.findIndex((c) => c.includes('execute_filter'))
    const uniqueIdx = codes.findIndex((c) => c.includes('execute_unique'))
    expect(readIdx).toBeGreaterThanOrEqual(0)
    expect(filterIdx).toBeGreaterThan(readIdx)
    expect(uniqueIdx).toBeGreaterThan(filterIdx)
  })

  it('reuses built & unchanged upstream — runs only the fetched node', async () => {
    const store = useFlowStore()
    const { readId, filterId, uniqueId } = buildChain(store)
    store.dirtyNodes.clear() // simulate an already-run, clean upstream
    runPythonWithResult.mockClear()
    mockEngine([readId, filterId]) // read + filter pointers already wired

    const result = await store.executeNodeWithUpstream(uniqueId)
    expect(result.success).toBe(true)

    const codes = runPythonWithResult.mock.calls.map((c) => c[0] as string)
    expect(codes.some((c) => c.includes('execute_read_csv'))).toBe(false)
    expect(codes.some((c) => c.includes('execute_filter'))).toBe(false)
    expect(codes.some((c) => c.includes('execute_unique'))).toBe(true)
  })

  it('rebuilds a dirty upstream node but still reuses clean ancestors', async () => {
    const store = useFlowStore()
    const { readId, filterId, uniqueId } = buildChain(store)
    store.dirtyNodes.clear()
    store.dirtyNodes.add(filterId) // filter's settings changed since it was built
    runPythonWithResult.mockClear()
    mockEngine([readId, filterId])

    const result = await store.executeNodeWithUpstream(uniqueId)
    expect(result.success).toBe(true)

    const codes = runPythonWithResult.mock.calls.map((c) => c[0] as string)
    expect(codes.some((c) => c.includes('execute_read_csv'))).toBe(false) // clean → reused
    expect(codes.some((c) => c.includes('execute_filter'))).toBe(true) // dirty → rebuilt
    expect(codes.some((c) => c.includes('execute_unique'))).toBe(true) // target → rebuilt
  })

  it('preserves an executed schema even when the lazy pass cannot resolve it', async () => {
    const store = useFlowStore()
    const readId = store.addNode('read', 0, 0)
    const pivotId = store.addNode('pivot', 200, 0)
    connect(store, readId, pivotId)

    // Simulate a prior successful run that produced real pivot columns.
    store.nodeResults.set(pivotId, { success: true, schema: [{ name: 'k', data_type: 'String' }, { name: 'v', data_type: 'Int64' }] })

    runPythonWithResult.mockResolvedValue({
      [readId]: { schema: [{ name: 'a', data_type: 'Int64' }], schema_resolved: true },
      [pivotId]: { schema: [], schema_resolved: false, error: 'Pivot output columns depend on the data; run the flow.' }
    })

    await store.propagateSchemas()

    const pivotResult = store.getNodeResult(pivotId)
    expect(pivotResult?.schema?.map((c) => c.name)).toEqual(['k', 'v'])
    // A successfully-executed node is not falsely flagged unresolved.
    expect(pivotResult?.schemaResolved).toBe(true)
  })

  // Regression: applying propagation results used to rebuild settings sub-objects
  // unconditionally, retriggering the settings watcher → propagate → … forever.
  it('converges — applying results does not retrigger the settings watcher', async () => {
    vi.useFakeTimers()
    try {
      const store = useFlowStore()
      const readId = store.addNode('read', 0, 0)
      const selectId = store.addNode('select', 200, 0)
      connect(store, readId, selectId)
      // Seed the source schema (applyPropagatedSchemas skips source nodes).
      store.nodeResults.set(readId, { schema: [{ name: 'a', data_type: 'Int64' }] })

      runPythonWithResult.mockResolvedValue({
        [readId]: { schema: [{ name: 'a', data_type: 'Int64' }], schema_resolved: true },
        [selectId]: { schema: [{ name: 'a', data_type: 'Int64' }], schema_resolved: true }
      })

      const propagateCalls = () =>
        runPythonWithResult.mock.calls.filter((c) => (c[0] as string).includes('propagate_schemas')).length

      await store.propagateSchemas()
      for (let i = 0; i < 10; i++) await vi.runAllTimersAsync()
      const settled = propagateCalls()
      for (let i = 0; i < 10; i++) await vi.runAllTimersAsync()
      expect(propagateCalls()).toBe(settled)
    } finally {
      vi.useRealTimers()
    }
  })

  it('re-propagation is idempotent — node and result identities stay stable', async () => {
    const store = useFlowStore()
    const readId = store.addNode('read', 0, 0)
    const selectId = store.addNode('select', 200, 0)
    connect(store, readId, selectId)
    store.nodeResults.set(readId, { schema: [{ name: 'a', data_type: 'Int64' }] })

    runPythonWithResult.mockResolvedValue({
      [readId]: { schema: [{ name: 'a', data_type: 'Int64' }], schema_resolved: true },
      [selectId]: { schema: [{ name: 'a', data_type: 'Int64' }], schema_resolved: true }
    })

    await store.propagateSchemas()
    const nodeAfterFirst = store.getNode(selectId)
    const selectInputAfterFirst = (nodeAfterFirst?.settings as any).select_input
    const resultAfterFirst = store.getNodeResult(selectId)
    expect(selectInputAfterFirst?.length).toBe(1)

    await store.propagateSchemas()
    expect(store.getNode(selectId)).toBe(nodeAfterFirst)
    expect((store.getNode(selectId)?.settings as any).select_input).toBe(selectInputAfterFirst)
    expect(store.getNodeResult(selectId)).toBe(resultAfterFirst)
  })
})
