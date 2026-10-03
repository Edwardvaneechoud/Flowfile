/**
 * Undo/redo and batch edits on the flow store.
 *
 * One step per user gesture, restored in place: a node the step did not touch
 * keeps its results, and neither undo, redo nor applyFlowPatch ever executes.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const pyodideMock = vi.hoisted(() => ({
  isReady: false,
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
import { useFlowTabsStore } from '../../src/stores/flow-tabs-store'
import { CONTINUOUS_EDIT_MS } from '../../src/stores/flow-history'
import type { FlowEdge } from '../../src/types'

type Store = ReturnType<typeof useFlowStore>

/** Let enough time pass that the next edit is its own gesture. */
const pause = () => vi.advanceTimersByTime(CONTINUOUS_EDIT_MS + 1)

const edge = (source: number, target: number, targetHandle = 'input-0'): FlowEdge => ({
  id: `e${source}-${target}-output-0-${targetHandle}`,
  source: String(source),
  target: String(target),
  sourceHandle: 'output-0',
  targetHandle
})

const settingsOf = (store: Store, id: number) => store.getNode(id)!.settings as any

/** manual_input(1) → filter(2) → sort(3), each added as its own step. */
function buildChain(store: Store) {
  const source = store.addNode('manual_input', 0, 0)
  pause()
  const filter = store.addNode('filter', 200, 0)
  pause()
  const sort = store.addNode('sort', 400, 0)
  pause()
  store.addEdge(edge(source, filter))
  pause()
  store.addEdge(edge(filter, sort))
  pause()
  return { source, filter, sort }
}

/** Pretend the flow ran: every node has a green result and nothing is dirty. */
function markAllRun(store: Store) {
  for (const id of store.nodes.keys()) {
    store.nodeResults.set(id, { success: true, schema: [{ name: 'a', data_type: 'Int64' }] })
  }
  store.dirtyNodes.clear()
}

describe('undo and redo', () => {
  let store: Store

  beforeEach(() => {
    vi.useFakeTimers()
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = false
    store = useFlowStore()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('starts with nothing to undo', () => {
    expect(store.canUndo).toBe(false)
    expect(store.canRedo).toBe(false)
    expect(store.undo()).toBe(false)
  })

  it('undoes and redoes adding a node', () => {
    const id = store.addNode('filter', 10, 20)
    expect(store.canUndo).toBe(true)

    expect(store.undo()).toBe(true)
    expect(store.nodes.size).toBe(0)
    expect(store.canRedo).toBe(true)

    expect(store.redo()).toBe(true)
    expect(store.getNode(id)?.type).toBe('filter')
    expect(store.getNode(id)?.x).toBe(10)
  })

  it('reuses the id a removed node had when the node is added again after an undo', () => {
    const first = store.addNode('filter', 0, 0)
    store.undo()
    pause()
    expect(store.addNode('sort', 0, 0)).toBe(first)
  })

  it('brings back a removed node with its edges, its inputs and its loaded data', () => {
    const { source, filter, sort } = buildChain(store)
    store.setFileContent(source, 'a\n1\n2')
    pause()

    store.removeNode(filter)
    expect(store.edges).toHaveLength(0)
    expect(store.getNode(sort)!.inputIds).toEqual([])

    store.undo()
    expect(store.getNode(filter)?.type).toBe('filter')
    expect(store.edges.map(e => e.id)).toEqual([edge(source, filter).id, edge(filter, sort).id])
    expect(store.getNode(sort)!.inputIds).toEqual([filter])
    expect(store.getNode(sort)!.leftInputId).toBe(filter)

    store.undo()  // the second edge
    store.undo()  // the first edge
    store.undo()  // the sort node
    store.undo()  // the filter node
    expect([...store.nodes.keys()]).toEqual([source])
    expect(store.getTextContent(source)).toBe('a\n1\n2')
  })

  it('restores the data of a removed input node', () => {
    const source = store.addNode('manual_input', 0, 0)
    store.setFileContent(source, 'a\n1')
    pause()

    store.removeNode(source)
    expect(store.hasFileContent(source)).toBe(false)

    store.undo()
    expect(store.getTextContent(source)).toBe('a\n1')
  })

  it('does not take away data that arrived after the step being undone', () => {
    const source = store.addNode('read', 0, 0)
    pause()
    store.addNode('filter', 200, 0)
    // A refetch, a re-pick or an import delivers the flow's data: not an edit of the user's.
    store.setFileContent(source, 'a\n1')

    store.undo()

    expect([...store.nodes.keys()]).toEqual([source])
    expect(store.getTextContent(source)).toBe('a\n1')
  })

  it('undoes connecting and disconnecting', () => {
    const { source, filter } = buildChain(store)

    store.removeEdge(edge(source, filter).id)
    expect(store.getNode(filter)!.inputIds).toEqual([])

    store.undo()
    expect(store.getNode(filter)!.inputIds).toEqual([source])
    expect(store.edges).toHaveLength(2)
  })

  it('merges the keystrokes of one settings edit into a single step', () => {
    const id = store.addNode('filter', 0, 0)
    pause()
    const base = settingsOf(store, id)

    for (const value of ['1', '12', '123']) {
      store.updateNodeSettings(id, { ...base, filter_input: { mode: 'advanced', advanced_filter: value } } as any)
      vi.advanceTimersByTime(200)
    }
    expect(settingsOf(store, id).filter_input.advanced_filter).toBe('123')

    store.undo()
    expect(settingsOf(store, id).filter_input).toEqual(base.filter_input)
    store.redo()
    expect(settingsOf(store, id).filter_input.advanced_filter).toBe('123')
  })

  it('keeps two edits apart when the user paused between them', () => {
    const id = store.addNode('filter', 0, 0)
    pause()
    const base = settingsOf(store, id)
    const withFilter = (value: string) => ({ ...base, filter_input: { mode: 'advanced', advanced_filter: value } }) as any

    store.updateNodeSettings(id, withFilter('first'))
    pause()
    store.updateNodeSettings(id, withFilter('second'))

    store.undo()
    expect(settingsOf(store, id).filter_input.advanced_filter).toBe('first')
  })

  it('is not changed by a panel that keeps editing the object it sent', () => {
    const id = store.addNode('filter', 0, 0)
    pause()
    const sent = { ...settingsOf(store, id), filter_input: { mode: 'advanced', advanced_filter: 'sent' } } as any
    store.updateNodeSettings(id, sent)

    sent.filter_input.advanced_filter = 'edited in the panel, not sent yet'

    expect(settingsOf(store, id).filter_input.advanced_filter).toBe('sent')
  })

  it('merges a whole drag into one step and puts the node back where it was', () => {
    const id = store.addNode('filter', 0, 0)
    pause()
    for (let x = 1; x <= 30; x++) {
      store.updateNode(id, { x, y: x })
      vi.advanceTimersByTime(16)
    }

    store.undo()
    expect(store.getNode(id)).toMatchObject({ x: 0, y: 0 })
    store.undo()
    expect(store.nodes.size).toBe(0)
  })

  it('does not count re-setting an unchanged position as an edit', () => {
    const id = store.addNode('filter', 5, 5)
    pause()
    store.updateNode(id, { x: 5, y: 5 })

    store.undo()
    expect(store.nodes.size).toBe(0)
  })

  it('undoes a description and a reference edit', () => {
    const id = store.addNode('filter', 0, 0)
    pause()
    store.updateNodeDescription(id, 'keep the big ones')
    pause()
    store.updateNodeReference(id, 'big_ones')

    store.undo()
    expect(store.getNode(id)!.node_reference).toBeUndefined()
    expect(store.getNode(id)!.description).toBe('keep the big ones')
    store.undo()
    expect(store.getNode(id)!.description).toBe('')
  })

  it('undoes a change of input data made in a panel, together with its settings', () => {
    const id = store.addNode('manual_input', 0, 0)
    pause()
    const base = settingsOf(store, id)

    // A panel writes the data and the settings in one go.
    store.setFileContent(id, 'a\n1', { undoable: true })
    store.updateNodeSettings(id, { ...base, is_setup: true } as any)
    expect(store.getTextContent(id)).toBe('a\n1')

    store.undo()
    expect(store.hasFileContent(id)).toBe(false)
    expect(settingsOf(store, id).is_setup).toBe(base.is_setup)
    store.redo()
    expect(store.getTextContent(id)).toBe('a\n1')
  })

  describe('results', () => {
    it('keeps the results of nodes a step did not touch, and marks the changed ones dirty', () => {
      const { source, filter, sort } = buildChain(store)
      const base = settingsOf(store, filter)
      markAllRun(store)

      store.updateNodeSettings(filter, { ...base, filter_input: { mode: 'advanced', advanced_filter: '[a] > 1' } } as any)
      markAllRun(store)
      store.undo()

      expect(store.nodeResults.get(source)?.success).toBe(true)
      expect(store.isNodeDirty(source)).toBe(false)
      // The filter went back to its old settings, so it and what follows must run again.
      expect(store.isNodeDirty(filter)).toBe(true)
      expect(store.isNodeDirty(sort)).toBe(true)
    })

    it('leaves results alone when only a position is undone', () => {
      const { source, filter, sort } = buildChain(store)
      markAllRun(store)
      store.updateNode(sort, { x: 900, y: 900 })

      store.undo()

      expect(store.getNode(sort)).toMatchObject({ x: 400, y: 0 })
      for (const id of [source, filter, sort]) {
        expect(store.nodeResults.get(id)?.success).toBe(true)
        expect(store.isNodeDirty(id)).toBe(false)
      }
    })

    it('never executes', () => {
      pyodideMock.isReady = true
      pyodideMock.runPythonWithResult.mockResolvedValue({})
      const { filter } = buildChain(store)
      store.removeNode(filter)
      pause()

      store.undo()
      store.redo()
      store.undo()
      vi.advanceTimersByTime(5000)

      const executed = pyodideMock.runPythonWithResult.mock.calls.filter(call => /execute_/.test(String(call[0])))
      expect(executed).toEqual([])
    })

    it('does nothing while the flow is running', () => {
      store.addNode('filter', 0, 0)
      store.isExecuting = true

      expect(store.undo()).toBe(false)
      expect(store.nodes.size).toBe(1)
    })
  })

  describe('the open settings panel', () => {
    it('is told to reload a node whose settings a step replaced', () => {
      const id = store.addNode('filter', 0, 0)
      pause()
      const epoch = store.settingsEpoch(id)
      store.updateNodeSettings(id, { ...settingsOf(store, id), description: 'changed' } as any)
      expect(store.settingsEpoch(id)).toBe(epoch)

      store.undo()
      expect(store.settingsEpoch(id)).toBe(epoch + 1)
    })

    it('closes when the selected node is undone away', () => {
      const id = store.addNode('filter', 0, 0)
      store.selectNode(id)
      store.showSettings = true

      store.undo()

      expect(store.selectedNodeId).toBeNull()
      expect(store.showSettings).toBe(false)
    })
  })

  describe('which flow a history belongs to', () => {
    it('starts over when another flow is loaded or the flow is cleared', () => {
      store.addNode('filter', 0, 0)
      const exported = store.exportToFlowfile('x')

      store.importFromFlowfile(exported)
      expect(store.canUndo).toBe(false)

      store.addNode('sort', 0, 0)
      store.clearFlow()
      expect(store.canUndo).toBe(false)
    })

    it('stays with its tab across a tab switch', () => {
      const tabs = useFlowTabsStore()
      tabs.init()
      const firstTab = tabs.activeTabId
      const kept = store.addNode('filter', 0, 0)
      pause()
      store.addNode('sort', 200, 0)

      tabs.newTab()
      expect(store.canUndo).toBe(false)
      store.addNode('head', 0, 0)

      tabs.switchTab(firstTab)
      expect(store.nodes.size).toBe(2)
      expect(store.undo()).toBe(true)
      expect([...store.nodes.keys()]).toEqual([kept])
    })
  })
})

describe('applyFlowPatch', () => {
  let store: Store

  beforeEach(() => {
    vi.useFakeTimers()
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = false
    store = useFlowStore()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('applies a batch of changes as one undo step', () => {
    const { source, filter, sort } = buildChain(store)
    const before = store.exportToFlowfile('x').nodes

    store.applyFlowPatch({
      removeNodeIds: [filter],
      addNodes: [{ id: 10, type: 'head', x: 200, y: 100, settings: { head_input: { n: 5 } } as any, description: 'first five' }],
      updateNodes: [{ id: sort, settings: { sort_input: [{ column: 'a', how: 'desc' }] } as any }],
      addEdges: [
        { source: String(source), target: '10', sourceHandle: 'output-0', targetHandle: 'input-0' },
        { source: '10', target: String(sort), sourceHandle: 'output-0', targetHandle: 'input-0' }
      ]
    })

    expect([...store.nodes.keys()].sort((a, b) => a - b)).toEqual([source, sort, 10])
    expect(store.getNode(10)!.inputIds).toEqual([source])
    expect(store.getNode(sort)!.inputIds).toEqual([10])
    expect(settingsOf(store, sort).sort_input).toEqual([{ column: 'a', how: 'desc' }])
    // A node added later does not collide with the patch's id.
    expect(store.generateNodeId()).toBe(11)

    store.undo()
    expect(store.exportToFlowfile('x').nodes).toEqual(before)
    expect(store.edges.map(e => e.id)).toEqual([edge(source, filter).id, edge(filter, sort).id])
  })

  it('keeps the id, position, description and results of nodes it does not touch', () => {
    const { source, filter, sort } = buildChain(store)
    store.updateNodeDescription(source, 'the input')
    markAllRun(store)
    const untouched = JSON.stringify(store.getNode(source))

    store.applyFlowPatch({ updateNodes: [{ id: sort, settings: { sort_input: [{ column: 'a', how: 'asc' }] } as any }] })

    expect(JSON.stringify(store.getNode(source))).toBe(untouched)
    for (const id of [source, filter]) {
      expect(store.nodeResults.get(id)?.success).toBe(true)
      expect(store.isNodeDirty(id)).toBe(false)
    }
    expect(store.isNodeDirty(sort)).toBe(true)
  })

  it('changes nothing and records nothing when the patch cannot be applied', () => {
    const { sort } = buildChain(store)
    const before = JSON.stringify([...store.nodes.values()])
    store.undo()
    store.redo()

    expect(() =>
      store.applyFlowPatch({
        updateNodes: [{ id: sort, description: 'would be applied first' }],
        addEdges: [{ source: '1', target: '99', sourceHandle: 'output-0', targetHandle: 'input-0' }]
      })
    ).toThrow('both nodes must exist')

    expect(JSON.stringify([...store.nodes.values()])).toBe(before)
    store.undo()
    expect(store.edges).toHaveLength(1)
  })

  it('never executes', () => {
    pyodideMock.isReady = true
    pyodideMock.runPythonWithResult.mockResolvedValue({})
    const { sort } = buildChain(store)

    store.applyFlowPatch({ updateNodes: [{ id: sort, settings: { sort_input: [] } as any }] })
    vi.advanceTimersByTime(5000)

    const executed = pyodideMock.runPythonWithResult.mock.calls.filter(call => /execute_/.test(String(call[0])))
    expect(executed).toEqual([])
  })
})
