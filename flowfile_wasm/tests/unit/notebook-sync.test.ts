/**
 * A notebook sync: the changed cells read by the engine, landed as one undo step.
 *
 * The cells cross the bridge as data beside a constant Python source, a locked
 * node's settings never cross at all, and a refused or overtaken sync changes
 * nothing. What the engine answers in flowfile_core's dialect is laid over the
 * node's own settings in this editor's dialect.
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
import { CHECK_DELAY_MS, CHECK_SOURCE, SYNC_SOURCE, useNotebookStore, type NotebookCell } from '../../src/stores/notebook-store'
import { isEmptyPatch, mergeSettings, syncPatch, type NotebookSyncResult } from '../../src/utils/notebookSync'
import type { FlowEdge, FlowNode, FlowfileData } from '../../src/types'

const CANARY = 'CANARY_SYNC_91c3'
const SORT_CODE = 'ordered_2 = source_1.sort(["a"], descending=[False])'
const SORT_DRAFT = 'ordered_2 = source_1.sort(["a"], descending=[True])'

const cell = (id: number, code: string, extra: Partial<NotebookCell> = {}): NotebookCell => ({
  cell_id: `cell-${id}`,
  node_ids: [id],
  kind: 'node',
  code,
  defines: [],
  uses: [],
  status: 'code',
  reason: null,
  ...extra
})

const IMPORTS = cell(0, 'import flowfile as ff', { cell_id: 'imports', node_ids: [], kind: 'imports' })
const CELLS = [IMPORTS, cell(1, 'source_1 = ff.from_raw_data({})'), cell(2, SORT_CODE)]
const SORTED: NotebookSyncResult = {
  ok: true,
  nodes: { '2': { settings: { sort_input: [{ column: 'a', how: 'desc' }] } } },
  inputs: {},
  node_ids_by_cell: { 'cell-2': [[2]] },
  warnings: []
}

const edge = (source: number, target: number, targetHandle = 'input-0'): FlowEdge => ({
  id: `e${source}-${target}-${targetHandle}`,
  source: String(source),
  target: String(target),
  sourceHandle: 'output-0',
  targetHandle
})

const bridgeSources = () => pyodideMock.runPythonWithResult.mock.calls.map(call => String(call[0]))
const syncCalls = () => bridgeSources().filter(source => source === SYNC_SOURCE)
const requests = () =>
  pyodideMock.setGlobal.mock.calls.filter(call => call[0] === '_notebook_sync_request').map(call => JSON.parse(call[1]))

/** Answer a render with `cells` and a sync with `answer` (a value, or what a function returns). */
function bridge(answer: unknown = SORTED, cells: NotebookCell[] = CELLS) {
  pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
    if (source === SYNC_SOURCE) return typeof answer === 'function' ? answer() : answer
    if (source.includes('render_notebook(')) return { cells, warnings: [], var_by_node: {} }
    if (source.includes('_lazyframes.keys()')) return []
    if (source.includes('fetch_preview(')) return { success: true, data: { columns: ['a'], data: [[1]], total_rows: 1 } }
    return { success: true }
  })
}

/** A source feeding a sort on `a` ascending, rendered as two cells. */
async function sourceAndSort() {
  const flow = useFlowStore()
  const source = flow.addNode('manual_input', 0, 0)
  flow.updateNodeSettings(source, {
    ...flow.getNode(source)!.settings,
    raw_data_format: { columns: [{ name: 'a', data_type: 'Int64' }], data: [[1, 2]] }
  } as any)
  const sort = flow.addNode('sort', 200, 0)
  flow.updateNodeSettings(sort, { ...flow.getNode(sort)!.settings, sort_input: [{ column: 'a', how: 'asc' }] } as any)
  flow.addEdge(edge(source, sort))
  const notebook = useNotebookStore()
  await notebook.render()
  pyodideMock.runPythonWithResult.mockClear()
  pyodideMock.setGlobal.mockClear()
  pyodideMock.deleteGlobal.mockClear()
  return { flow, notebook, source, sort }
}

const sortInput = (flow: ReturnType<typeof useFlowStore>, id: number) => (flow.getNode(id)!.settings as any).sort_input

describe('mergeSettings and syncPatch', () => {
  const node = (id: number, type: string, settings: Record<string, unknown>, inputIds: number[] = []): FlowNode =>
    ({ id, type, x: 0, y: 0, settings: { node_id: id, ...settings }, inputIds, description: '' }) as unknown as FlowNode

  const graphOf = (nodes: FlowNode[], edges: FlowEdge[] = []) => ({ nodes: new Map(nodes.map(each => [each.id, each])), edges })
  const answer = (parts: Partial<NotebookSyncResult>): NotebookSyncResult => ({ ok: true, nodes: {}, inputs: {}, warnings: [], ...parts })

  it('merges objects key by key and replaces everything else', () => {
    const base = { keep: 1, nested: { a: 1, b: [1, 2] }, list: [1, 2, 3] }
    const merged = mergeSettings(base, { nested: { b: [9] }, list: [4], added: null })
    expect(merged).toEqual({ keep: 1, nested: { a: 1, b: [9] }, list: [4], added: null })
    expect(base.nested.b).toEqual([1, 2])
  })

  it('is empty when the sync changed nothing', () => {
    expect(isEmptyPatch(syncPatch(graphOf([node(1, 'sort', {})]), answer({})))).toBe(true)
  })

  it('lays the settings over the node and keeps what the code does not say', () => {
    const graph = graphOf([node(2, 'sort', { cache_results: true, sort_input: [{ column: 'a', how: 'asc' }] })])
    const patch = syncPatch(graph, SORTED)
    expect(patch).toEqual({
      updateNodes: [{ id: 2, settings: { node_id: 2, cache_results: true, sort_input: [{ column: 'a', how: 'desc' }] } }]
    })
  })

  it("keeps this editor's own spelling in step with the core one", () => {
    const graph = graphOf([
      node(1, 'unique', { unique_input: { subset: ['a'], keep: 'first', columns: ['a'], strategy: 'first' } }),
      node(2, 'record_id', { record_id_input: { name: 'record_id', offset: 1 } }),
      node(3, 'head', { sample_size: 10, head_input: { n: 10 } }),
      node(4, 'formula', { function: { field: { name: 'x' }, function: '1' }, functions: [{ field: { name: 'x' }, function: '1' }] }),
      node(5, 'unique', { unique_input: { subset: ['a'], keep: 'first' } })
    ])
    const two = [{ field: { name: 'x' }, function: '2' }, { field: { name: 'y' }, function: '3' }]
    const patch = syncPatch(
      graph,
      answer({
        nodes: {
          '1': { settings: { unique_input: { columns: ['a', 'b'], strategy: 'last' } } },
          '2': { settings: { record_id_input: { output_column_name: 'row', offset: 5 } } },
          '3': { settings: { sample_size: 3 } },
          '4': { settings: { functions: two } },
          '5': { settings: { unique_input: { columns: null, strategy: 'any' } } }
        }
      })
    )
    const settings = Object.fromEntries(patch.updateNodes!.map(update => [update.id, update.settings as any]))
    expect(settings[1].unique_input).toEqual({ subset: ['a', 'b'], keep: 'last', columns: ['a', 'b'], strategy: 'last' })
    expect(settings[2].record_id_input).toEqual({ name: 'row', output_column_name: 'row', offset: 5 })
    expect(settings[3]).toMatchObject({ sample_size: 3, head_input: { n: 3 } })
    expect(settings[4].functions).toEqual(two)
    expect('function' in settings[4]).toBe(false)
    expect(settings[5].unique_input).toEqual({ subset: [], keep: 'any', columns: null, strategy: 'any' })
  })

  it('replaces a group by’s aggregations and a select’s columns as a whole, as the cell lists them', () => {
    const key = { old_name: 'product', new_name: 'product', agg: 'groupby', is_available: true }
    const graph = graphOf([
      node(1, 'group_by', { groupby_input: { agg_cols: [key, { old_name: 'revenue', new_name: 'total', agg: 'sum' }] } }),
      node(2, 'select', { keep_missing: false, select_input: [{ old_name: 'a', new_name: 'a', keep: true, position: 0 }] })
    ])
    const aggCols = [{ old_name: 'revenue', new_name: 'spread', agg: 'std' }]
    const selectInput = [
      { old_name: 'a', new_name: 'b', keep: true, position: 0 },
      { old_name: 'c', new_name: 'c', keep: false, position: 1 }
    ]
    const patch = syncPatch(
      graph,
      answer({
        nodes: {
          '1': { settings: { groupby_input: { agg_cols: aggCols } } },
          '2': { settings: { select_input: selectInput, keep_missing: false } }
        }
      })
    )
    const settings = Object.fromEntries(patch.updateNodes!.map(update => [update.id, update.settings as any]))
    expect(settings[1].groupby_input.agg_cols).toEqual(aggCols)
    expect(settings[2].select_input).toEqual(selectInput)
  })

  it('sets a description and a reference on the node and in its settings, and clears a reference', () => {
    const graph = graphOf([node(2, 'sort', {}), { ...node(3, 'sort', { node_reference: 'old' }), node_reference: 'old' }])
    const patch = syncPatch(
      graph,
      answer({ nodes: { '2': { description: 'Cheapest first', node_reference: 'cheap' }, '3': { node_reference: null } } })
    )
    expect(patch.updateNodes).toEqual([
      { id: 2, description: 'Cheapest first', node_reference: 'cheap', settings: { node_id: 2, description: 'Cheapest first', node_reference: 'cheap' } },
      { id: 3, node_reference: '', settings: { node_id: 3, node_reference: undefined } }
    ])
  })

  it("lays a rewired node's inputs again, in the order written", () => {
    const graph = graphOf(
      [node(1, 'manual_input', {}), node(2, 'manual_input', {}), node(3, 'manual_input', {}), node(4, 'union', {}, [1, 2]), node(5, 'join', {}, [1])],
      [edge(1, 4), edge(2, 4), edge(1, 5), edge(2, 5, 'input-1')]
    )
    const patch = syncPatch(graph, answer({ inputs: { '4': { main: [3, 1] }, '5': { main: [2], right: 3 } } }))
    const bare = ({ id: _id, ...rest }: FlowEdge) => rest
    expect(patch.removeEdges).toEqual([edge(1, 4), edge(2, 4), edge(1, 5), edge(2, 5, 'input-1')].map(bare))
    expect(patch.addEdges).toEqual([edge(3, 4), edge(1, 4), edge(2, 5), edge(3, 5, 'input-1')].map(bare))
  })

  describe('new nodes', () => {
    const defaults = (type: string, id: number, x: number, y: number) =>
      ({ node_id: id, pos_x: x, pos_y: y, is_setup: false, description: '', type_default: type, head_input: { n: 10 } }) as any
    const at = (id: number, x: number, y: number) => ({ ...node(id, 'manual_input', {}), x, y })

    it('adds a node in this editor’s type, on its defaults, right of its input and connected to it', () => {
      const graph = graphOf([at(1, 100, 40)])
      const patch = syncPatch(
        graph,
        answer({
          added: [{ id: 2, type: 'sample', settings: { sample_size: 5 }, description: 'Top five', node_reference: 'top' }],
          inputs: { '2': { main: [1] } }
        }),
        defaults
      )
      expect(patch.addNodes).toEqual([
        {
          id: 2,
          type: 'head',
          x: 350,
          y: 40,
          description: 'Top five',
          node_reference: 'top',
          settings: {
            node_id: 2,
            pos_x: 350,
            pos_y: 40,
            is_setup: false,
            description: 'Top five',
            node_reference: 'top',
            type_default: 'head',
            sample_size: 5,
            head_input: { n: 5 }
          }
        }
      ])
      expect(patch.addEdges).toEqual([{ source: '1', target: '2', sourceHandle: 'output-0', targetHandle: 'input-0' }])
      expect(patch.removeEdges).toBeUndefined()
    })

    it('places a run of new nodes one after the other, below anything already there, and a source below it all', () => {
      const graph = graphOf([at(1, 100, 40), at(2, 350, 40)], [edge(1, 2)])
      const patch = syncPatch(
        graph,
        answer({
          added: [
            { id: 3, type: 'sort', settings: {} },
            { id: 4, type: 'filter', settings: {} },
            { id: 5, type: 'manual_input', settings: {} }
          ],
          inputs: { '3': { main: [1] }, '4': { main: [3] } }
        }),
        defaults
      )
      expect(patch.addNodes!.map(added => [added.id, added.x, added.y])).toEqual([
        [3, 350, 170],
        [4, 600, 170],
        [5, 50, 340]
      ])
    })

    it('removes the steps a cell no longer writes, and a step written in their place takes the spot', () => {
      const graph = graphOf([at(1, 100, 40), at(2, 350, 40)], [edge(1, 2)])
      const patch = syncPatch(
        graph,
        answer({
          removed: [{ id: 2, label: '#2 Select data' }],
          added: [{ id: 3, type: 'group_by', settings: {} }],
          inputs: { '3': { main: [1] } }
        }),
        defaults
      )
      expect(patch.removeNodeIds).toEqual([2])
      expect(patch.addNodes!.map(added => [added.id, added.x, added.y])).toEqual([[3, 350, 40]])
      expect(isEmptyPatch(syncPatch(graph, answer({ removed: [] })))).toBe(true)
    })

    it('refuses new nodes it was given no defaults for, and an id that is taken', () => {
      const graph = graphOf([at(1, 0, 0)])
      const added = [{ id: 2, type: 'sort', settings: {} }]
      expect(() => syncPatch(graph, answer({ added }))).toThrow(/no defaults/)
      expect(() => syncPatch(graph, answer({ added: [{ id: 1, type: 'sort', settings: {} }] }), defaults)).toThrow(/already in use/)
    })
  })

  it('refuses an answer about a node that is no longer there', () => {
    const graph = graphOf([node(1, 'sort', {})])
    expect(() => syncPatch(graph, answer({ nodes: { '9': { description: 'x' } } }))).toThrow(/no longer on the canvas/)
    expect(() => syncPatch(graph, answer({ inputs: { '9': { main: [1] } } }))).toThrow(/no longer on the canvas/)
  })
})

describe('syncing the notebook', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = true
    pyodideMock.ensurePyPackages.mockResolvedValue(undefined)
    pyodideMock.runPython.mockResolvedValue(undefined)
    bridge()
  })

  it('a cell is a draft only while its text differs, and only a code cell can be changed', async () => {
    bridge(SORTED, [...CELLS, cell(3, 'x = ff.canvas_node(3)', { status: 'placeholder', reason: 'locked' })])
    const { notebook } = await sourceAndSort()
    await notebook.render()

    notebook.setCellCode('cell-2', SORT_DRAFT)
    notebook.setCellCode('imports', 'import os')
    notebook.setCellCode('cell-3', 'x = 1')
    expect(notebook.drafts).toEqual({ 'cell-2': SORT_DRAFT })
    expect(notebook.cellSyncState('cell-2')).toBe('edited')
    expect(notebook.cellCode(CELLS[2])).toBe(SORT_DRAFT)
    expect(notebook.needsSync).toBe(true)
    expect(notebook.canPush).toBe(true)

    notebook.setCellCode('cell-2', SORT_CODE)
    expect(notebook.drafts).toEqual({})
    expect(notebook.cellSyncState('cell-2')).toBeNull()
    expect(notebook.canPush).toBe(false)
  })

  it('does not ask the engine to read anything when no cell is changed', async () => {
    const { flow, notebook } = await sourceAndSort()

    expect(await notebook.push()).toBe(false)
    await notebook.runCell('cell-2')

    expect(syncCalls()).toEqual([])
    expect(pyodideMock.setGlobal.mock.calls.filter(call => call[0] === '_notebook_sync_request')).toEqual([])
    expect(flow.canUndo).toBe(true)
  })

  it('sends the changed cells as data beside a constant source, and takes the data away again', async () => {
    const { notebook, sort } = await sourceAndSort()
    notebook.setCellCode('cell-2', `${SORT_DRAFT}  # ${CANARY}`)

    await notebook.push()

    expect(syncCalls()).toHaveLength(1)
    for (const source of bridgeSources()) expect(source).not.toContain(CANARY)
    expect(requests()).toHaveLength(1)
    const request = requests()[0]
    expect(request.drafts).toEqual({ 'cell-2': `${SORT_DRAFT}  # ${CANARY}` })
    expect(request.next_id).toBe(sort + 1)
    expect(request.flow.nodes.map((node: any) => node.id)).toEqual([1, sort])
    expect(request.locked).toEqual({})
    expect(pyodideMock.deleteGlobal).toHaveBeenCalledWith('_notebook_sync_request')
  })

  it('lands the change as one undo step, marks the cell synced and renders again', async () => {
    const { flow, notebook, sort } = await sourceAndSort()
    notebook.setCellCode('cell-2', SORT_DRAFT)

    expect(await notebook.push()).toBe(true)

    expect(sortInput(flow, sort)).toEqual([{ column: 'a', how: 'desc' }])
    expect(notebook.drafts).toEqual({})
    expect(notebook.cellSyncState('cell-2')).toBe('synced')
    expect(notebook.syncError).toBeNull()
    // The cells already showed the canvas, so only the render after the push was needed.
    expect(bridgeSources().filter(source => source.includes('render_notebook('))).toHaveLength(1)
    expect(bridgeSources().filter(source => /execute_|fetch_preview\(/.test(source))).toEqual([])

    expect(flow.undo()).toBe(true)
    expect(sortInput(flow, sort)).toEqual([{ column: 'a', how: 'asc' }])
    expect(flow.redo()).toBe(true)
    expect(sortInput(flow, sort)).toEqual([{ column: 'a', how: 'desc' }])

    notebook.setCellCode('cell-2', SORT_DRAFT)
    expect(notebook.cellSyncState('cell-2')).toBe('edited')
  })

  it('adds the nodes the cells call for, connected and placed, as part of the same undo step', async () => {
    const { flow, notebook, sort } = await sourceAndSort()
    const added = sort + 1
    bridge({
      ok: true,
      nodes: {},
      added: [{ id: added, type: 'sample', settings: { sample_size: 3 }, description: '', node_reference: 'top' }],
      inputs: { [added]: { main: [sort] } },
      warnings: []
    })
    notebook.setCellCode('cell-2', `${SORT_CODE}\ntop = ordered_2.head(3)`)

    expect(await notebook.push()).toBe(true)

    const node = flow.getNode(added)!
    expect([node.type, node.inputIds, node.node_reference]).toEqual(['head', [sort], 'top'])
    expect((node.settings as any).sample_size).toBe(3)
    expect([node.x, node.y]).toEqual([flow.getNode(sort)!.x + 250, flow.getNode(sort)!.y])
    expect(flow.nextNodeId).toBe(added + 1)

    expect(flow.undo()).toBe(true)
    expect(flow.getNode(added)).toBeUndefined()
    expect(flow.edges.some(each => each.target === String(added))).toBe(false)
  })

  it('asks before a push removes a step, and removes it as one undo step only when the user agrees', async () => {
    const { flow, notebook, sort } = await sourceAndSort()
    bridge({ ok: true, nodes: {}, inputs: {}, removed: [{ id: sort, label: `#${sort} Sort data` }], warnings: [] })
    notebook.setCellCode('cell-2', '')
    const confirm = vi.fn(() => false)
    vi.stubGlobal('confirm', confirm)

    try {
      expect(await notebook.push()).toBe(false)
      expect(confirm).toHaveBeenCalledWith(expect.stringContaining(`#${sort} Sort data is no longer in the notebook`))
      expect(flow.getNode(sort)).toBeDefined()
      expect(notebook.drafts).toEqual({ 'cell-2': '' })
      expect(notebook.syncError).toBeNull()

      confirm.mockReturnValue(true)
      expect(await notebook.push()).toBe(true)
      expect(flow.getNode(sort)).toBeUndefined()
      expect(flow.edges).toEqual([])

      expect(flow.undo()).toBe(true)
      expect(flow.getNode(sort)).toBeDefined()
      expect(flow.edges.map(each => [each.source, each.target])).toEqual([['1', String(sort)]])
    } finally {
      vi.unstubAllGlobals()
    }
  })

  describe('cells the user adds and writes', () => {
    const HEAD = 'top = ordered_2.head(3)'
    /** The engine's answer to a new cell holding one new head node, and the render that follows it. */
    function pushesNewHead(sort: number) {
      const added = sort + 1
      const answer = {
        ok: true,
        nodes: {},
        added: [{ id: added, type: 'sample', settings: { sample_size: 3 }, description: '', node_reference: 'top' }],
        inputs: { [added]: { main: [sort] } },
        node_ids_by_cell: { 'new-1': [[added]] },
        warnings: []
      }
      const after = [...CELLS, cell(added, HEAD, { defines: ['top'], uses: ['ordered_2'] })]
      let pushed = false
      pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
        if (source === SYNC_SOURCE) {
          pushed = true
          return answer
        }
        if (source.includes('render_notebook(')) return { cells: pushed ? after : CELLS, warnings: [], var_by_node: {} }
        if (source.includes('_lazyframes.keys()')) return []
        if (source.includes('fetch_preview(')) return { success: true, data: { columns: ['a'], data: [[1]], total_rows: 1 } }
        return { success: true }
      })
      return added
    }

    it('adds an empty cell after the one given, or at the end, and drops it again', async () => {
      const { notebook } = await sourceAndSort()

      const last = notebook.addCell()
      const middle = notebook.addCell('cell-1')

      expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', middle, 'cell-2', last])
      expect(notebook.shownCells.filter(each => each.fresh).map(each => each.cell_id)).toEqual([middle, last])
      expect(notebook.cellSyncState(last)).toBe('new')
      expect(notebook.isEditable(notebook.shownCells[2])).toBe(true)
      expect(notebook.needsSync).toBe(false)

      notebook.setCellCode(last, HEAD)
      expect(notebook.cellCode(notebook.shownCells[4])).toBe(HEAD)
      expect([notebook.needsSync, notebook.changedCount]).toEqual([true, 1])

      notebook.discardCell(last)
      notebook.discardCell(middle)
      expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2'])
      expect(notebook.needsSync).toBe(false)
    })

    it('sends a written new cell with the order the notebook shows, and leaves an empty one out', async () => {
      const { notebook, sort } = await sourceAndSort()
      pushesNewHead(sort)
      const empty = notebook.addCell('cell-1')
      const written = notebook.addCell()
      notebook.setCellCode(written, HEAD)

      await notebook.push()

      const request = requests()[0]
      expect(request.new_cells).toEqual({ [written]: HEAD })
      expect(request.drafts).toEqual({})
      expect(request.order).toEqual(['imports', 'cell-1', empty, 'cell-2', written])
      expect(request.layout).toEqual([])
    })

    it('keeps the cell where it was written: its nodes stay together and it stays after the cell before it', async () => {
      const { flow, notebook, sort } = await sourceAndSort()
      const added = pushesNewHead(sort)
      const written = notebook.addCell()
      notebook.setCellCode(written, HEAD)

      expect(await notebook.push()).toBe(true)

      expect(flow.getNode(added)?.type).toBe('head')
      expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2', `cell-${added}`])
      expect(notebook.shownCells.some(each => each.fresh)).toBe(false)
      expect(notebook.cellSyncState(`cell-${added}`)).toBe('synced')
      expect(notebook.added).toEqual({ nodes: [added], pushes: 1 })
      // The next render is asked to keep that cell's nodes together.
      const render = pyodideMock.setGlobal.mock.calls.filter(call => call[0] === '_notebook_render_request').at(-1)!
      expect(JSON.parse(render[1]).layout).toEqual([[[added]]])

      // A second push sends the written cell as the layout, and the cell is not read again.
      notebook.setCellCode('cell-2', SORT_DRAFT)
      await notebook.push()
      expect(requests().at(-1).layout).toEqual([[[added]]])
    })

    it('moves a written cell back under the cell it was written after', async () => {
      const { notebook, sort } = await sourceAndSort()
      const added = pushesNewHead(sort)
      const written = notebook.addCell('cell-2')
      notebook.setCellCode(written, HEAD)
      await notebook.push()

      // The flow gains an unrelated step the render lists before the written cell's node.
      const other = cell(99, 'other_99 = source_1.head(1)', { defines: ['other_99'], uses: ['source_1'] })
      const head = cell(added, HEAD, { defines: ['top'], uses: ['ordered_2'] })
      bridge(SORTED, [IMPORTS, CELLS[1], { ...CELLS[2], defines: ['ordered_2'] }, other, head])
      await notebook.render()
      expect(notebook.cells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2', 'cell-99', `cell-${added}`])
      expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2', `cell-${added}`, 'cell-99'])

      // A cell cannot stand above the name it reads.
      bridge(SORTED, [IMPORTS, CELLS[1], { ...CELLS[2], defines: ['renamed'] }, other, head])
      await notebook.render()
      expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2', 'cell-99', `cell-${added}`])
    })

    it('Run on a new cell pushes it and shows the rows under the cell it became', async () => {
      const { notebook, sort } = await sourceAndSort()
      const added = pushesNewHead(sort)
      const written = notebook.addCell()
      notebook.setCellCode(written, HEAD)

      await notebook.runCell(written)

      expect(Object.keys(notebook.outputs)).toEqual([`cell-${added}`])
      expect(notebook.outputs[`cell-${added}`].state).toBe('rows')
    })

    it('a cell that names no frame adds nothing, stays as written and has nothing left to push', async () => {
      const { flow, notebook } = await sourceAndSort()
      const LOOK = 'ordered_2.head(3)'
      const nothing = { ok: true, nodes: {}, added: [], inputs: {}, warnings: [] }
      bridge({ ...nothing, node_ids_by_cell: { 'new-1': [] }, unnamed_by_cell: { 'new-1': [LOOK] } })
      const written = notebook.addCell()
      notebook.setCellCode(written, LOOK)
      const nodes = flow.nodes.size
      const undoable = flow.canUndo

      await notebook.runCell(written)

      expect([flow.nodes.size, flow.canUndo]).toEqual([nodes, undoable])
      expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2', written])
      expect(notebook.cellCode(notebook.shownCells.at(-1)!)).toBe(LOOK)
      expect(notebook.cellSyncState(written)).toBe('plain')
      expect([notebook.needsSync, notebook.changedCount, notebook.canPush]).toEqual([false, 0, false])
      expect(notebook.outputs).toEqual({})

      // Run again, any number of times: nothing is read again and nothing is added.
      await notebook.runCell(written)
      await notebook.runCell(written)
      expect(syncCalls()).toHaveLength(1)
      expect(flow.nodes.size).toBe(nodes)

      // It still rides along when another cell is pushed, so a name it binds is there for the cells below.
      notebook.setCellCode('cell-2', SORT_DRAFT)
      expect(notebook.changedCount).toBe(1)
      bridge({ ...SORTED, node_ids_by_cell: { 'cell-2': [[2]], 'new-1': [] }, unnamed_by_cell: { 'new-1': [LOOK] } })
      await notebook.push()
      expect(requests().at(-1).new_cells).toEqual({ [written]: LOOK })
      expect(notebook.cellSyncState(written)).toBe('plain')

      // Changed, it is a new cell again.
      notebook.setCellCode(written, `top = ${LOOK}`)
      expect([notebook.cellSyncState(written), notebook.needsSync]).toEqual(['new', true])
    })

    it('lines that name no frame stay as a cell of their own under the cell their steps became', async () => {
      const { flow, notebook, sort } = await sourceAndSort()
      const added = pushesNewHead(sort)
      const answer = await pyodideMock.runPythonWithResult(SYNC_SOURCE)
      const after = [...CELLS, cell(added, HEAD, { defines: ['top'], uses: ['ordered_2'] })]
      let pushed = false
      pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
        if (source === SYNC_SOURCE) {
          pushed = true
          return { ...answer, unnamed_by_cell: { 'new-1': ['top.head(1)', 'top.head(2)'] } }
        }
        if (source.includes('render_notebook(')) return { cells: pushed ? after : CELLS, warnings: [], var_by_node: {} }
        return { success: true }
      })
      const written = notebook.addCell()
      notebook.setCellCode(written, `${HEAD}\ntop.head(1)\ntop.head(2)`)

      expect(await notebook.push()).toBe(true)

      expect(flow.getNode(added)?.type).toBe('head')
      const shown = notebook.shownCells
      expect(shown.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2', `cell-${added}`, 'new-2'])
      expect(notebook.cellCode(shown.at(-1)!)).toBe('top.head(1)\ntop.head(2)')
      expect(notebook.cellSyncState('new-2')).toBe('plain')
      expect(notebook.needsSync).toBe(false)
    })

    it('keys pressed while a push runs change the cell it became, not a second cell that adds the node again', async () => {
      const { flow, notebook, sort } = await sourceAndSort()
      const added = pushesNewHead(sort)
      const answer = await pyodideMock.runPythonWithResult(SYNC_SOURCE)
      const after = [...CELLS, cell(added, HEAD, { defines: ['top'], uses: ['ordered_2'] })]
      const written = notebook.addCell()
      let pushes = 0
      pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
        if (source === SYNC_SOURCE) {
          if (++pushes > 1) return { ok: true, nodes: {}, added: [], inputs: {}, node_ids_by_cell: {}, warnings: [] }
          notebook.setCellCode(written, 'top = ordered_2.head(4)')
          return answer
        }
        if (source.includes('render_notebook(')) return { cells: pushes ? after : CELLS, warnings: [], var_by_node: {} }
        return { success: true }
      })
      notebook.setCellCode(written, HEAD)
      pyodideMock.setGlobal.mockClear()

      expect(await notebook.push()).toBe(true)

      expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2', `cell-${added}`])
      expect(notebook.shownCells.some(each => each.fresh)).toBe(false)
      expect(notebook.drafts).toEqual({ [`cell-${added}`]: 'top = ordered_2.head(4)' })
      expect(notebook.cellSyncState(`cell-${added}`)).toBe('edited')

      const nodes = flow.nodes.size
      await notebook.push()
      expect(requests().at(-1)).toMatchObject({ new_cells: {}, drafts: { [`cell-${added}`]: 'top = ordered_2.head(4)' } })
      expect(flow.nodes.size).toBe(nodes)
    })

    it('a cell written above every step stands right under the imports', async () => {
      const { notebook, sort } = await sourceAndSort()
      const added = sort + 1
      const SOURCE = 'extra = ff.from_raw_data({})'
      const answer = {
        ok: true,
        nodes: {},
        added: [{ id: added, type: 'manual_input', settings: {}, description: '', node_reference: 'extra' }],
        inputs: {},
        node_ids_by_cell: { 'new-1': [[added]] },
        warnings: []
      }
      const made = cell(added, SOURCE, { defines: ['extra'], uses: ['ff'] })
      let pushed = false
      pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
        if (source === SYNC_SOURCE) {
          pushed = true
          return answer
        }
        const imports = { ...IMPORTS, defines: ['ff'] }
        if (source.includes('render_notebook(')) {
          return { cells: pushed ? [imports, CELLS[1], CELLS[2], made] : [imports, CELLS[1], CELLS[2]], warnings: [], var_by_node: {} }
        }
        return { success: true }
      })
      await notebook.render()
      const written = notebook.addCell('imports')
      notebook.setCellCode(written, SOURCE)

      expect(await notebook.push()).toBe(true)

      expect(notebook.cells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2', `cell-${added}`])
      expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', `cell-${added}`, 'cell-1', 'cell-2'])
    })

    it('a refused new cell keeps its text and shows the refusal on it', async () => {
      const { notebook } = await sourceAndSort()
      bridge({ ok: false, cell_id: 'new-1', line: 1, kind: 'error', message: "NameError: name 'x' is not defined" })
      const written = notebook.addCell()
      notebook.setCellCode(written, 'y = x.head(1)')

      expect(await notebook.push()).toBe(false)

      expect(notebook.syncError).toMatchObject({ cellId: written, line: 1, code: 'y = x.head(1)' })
      expect(notebook.cellSyncState(written)).toBe('failed')
      expect(notebook.cellCode(notebook.shownCells.at(-1)!)).toBe('y = x.head(1)')

      notebook.setCellCode(written, 'y = ordered_2.head(1)')
      expect(notebook.syncError).toBeNull()
      expect(notebook.cellSyncState(written)).toBe('new')
    })
  })

  it('a refused sync changes nothing and stands on its cell until the text changes', async () => {
    bridge({ ok: false, cell_id: 'cell-2', line: 1, kind: 'refused', message: 'This adds a step' })
    const { flow, notebook, sort } = await sourceAndSort()
    const undoable = flow.canUndo
    notebook.setCellCode('cell-2', SORT_DRAFT)
    const steps = JSON.stringify(sortInput(flow, sort))

    expect(await notebook.push()).toBe(false)

    expect(JSON.stringify(sortInput(flow, sort))).toBe(steps)
    expect(flow.canUndo).toBe(undoable)
    expect(notebook.syncError).toEqual({ cellId: 'cell-2', line: 1, kind: 'refused', message: 'This adds a step', code: SORT_DRAFT })
    expect(notebook.cellSyncState('cell-2')).toBe('failed')
    expect(notebook.drafts).toEqual({ 'cell-2': SORT_DRAFT })
    expect(pyodideMock.deleteGlobal).toHaveBeenCalledWith('_notebook_sync_request')

    await notebook.render()
    expect(notebook.syncError).not.toBeNull()

    notebook.setCellCode('cell-2', `${SORT_DRAFT} `)
    expect(notebook.syncError).toBeNull()
    expect(notebook.cellSyncState('cell-2')).toBe('edited')
  })

  it('pushes nothing when the canvas changed while the cells were being read', async () => {
    const { flow, notebook, sort } = await sourceAndSort()
    bridge(() => {
      flow.updateNodeDescription(sort, 'Changed on the canvas meanwhile')
      return SORTED
    })
    notebook.setCellCode('cell-2', SORT_DRAFT)

    expect(await notebook.push()).toBe(false)

    expect(sortInput(flow, sort)).toEqual([{ column: 'a', how: 'asc' }])
    expect(notebook.notice).toContain('The canvas changed')
    expect(notebook.drafts).toEqual({ 'cell-2': SORT_DRAFT })

    notebook.dismissNotice()
    expect(notebook.notice).toBeNull()
  })

  it('a schema arriving while the cells are read does not stop the push', async () => {
    const { flow, notebook, sort } = await sourceAndSort()
    bridge(() => {
      flow.nodeResults.set(sort, { success: true, schema: [{ name: 'a', data_type: 'Int64' }] } as any)
      return SORTED
    })
    notebook.setCellCode('cell-2', SORT_DRAFT)

    expect(await notebook.push()).toBe(true)
    expect(sortInput(flow, sort)).toEqual([{ column: 'a', how: 'desc' }])
  })

  it('keeps a cell changed again during the push as a draft', async () => {
    const { notebook } = await sourceAndSort()
    bridge(() => {
      notebook.setCellCode('cell-2', `${SORT_DRAFT}  # more`)
      return SORTED
    })
    notebook.setCellCode('cell-2', SORT_DRAFT)

    expect(await notebook.push()).toBe(true)

    expect(notebook.drafts).toEqual({ 'cell-2': `${SORT_DRAFT}  # more` })
    expect(notebook.cellSyncState('cell-2')).toBe('edited')
  })

  it('shows what the engine warned about, and why a push could not be read at all', async () => {
    const { notebook } = await sourceAndSort()
    bridge({ ...SORTED, warnings: ['`Top` cannot be kept as a name', 'Another'] })
    notebook.setCellCode('cell-2', SORT_DRAFT)
    await notebook.push()
    expect(notebook.notice).toBe('`Top` cannot be kept as a name\nAnother')

    bridge(() => {
      throw new Error('engine fell over')
    })
    notebook.setCellCode('cell-2', `${SORT_DRAFT} `)
    expect(await notebook.push()).toBe(false)
    expect(notebook.notice).toBe('The notebook could not be pushed: engine fell over')
    expect(notebook.syncing).toBe(false)
  })

  it('Run pushes the changed cells first, and runs nothing when the push is refused', async () => {
    const { flow, notebook, sort } = await sourceAndSort()
    notebook.setCellCode('cell-2', SORT_DRAFT)

    await notebook.runCell('cell-2')

    expect(syncCalls()).toHaveLength(1)
    expect(sortInput(flow, sort)).toEqual([{ column: 'a', how: 'desc' }])
    expect(notebook.outputs['cell-2'].state).toBe('rows')
    const sources = bridgeSources()
    expect(sources.indexOf(SYNC_SOURCE)).toBeLessThan(sources.findIndex(source => /execute_/.test(source)))

    pyodideMock.runPythonWithResult.mockClear()
    bridge({ ok: false, cell_id: 'cell-2', line: null, kind: 'error', message: 'no' })
    notebook.setCellCode('cell-2', `${SORT_DRAFT} `)
    await notebook.runCell('cell-2')
    await notebook.runAll()

    expect(bridgeSources().filter(source => /execute_|fetch_preview\(/.test(source))).toEqual([])
    expect(notebook.syncError?.line).toBeNull()
  })

  it('drops a draft its cell now says, keeps one whose cell is gone aside, and keeps drafts with their flow', async () => {
    const { flow, notebook } = await sourceAndSort()
    const sourceDraft = 'source_1 = ff.from_raw_data({"columns": [], "data": []})'
    notebook.setCellCode('cell-1', sourceDraft)
    notebook.setCellCode('cell-2', SORT_DRAFT)

    bridge(SORTED, [IMPORTS, cell(2, SORT_DRAFT)])
    await notebook.render()
    expect(notebook.drafts).toEqual({})
    expect(notebook.shownCells.filter(each => each.detached).map(each => notebook.cellCode(each))).toEqual([sourceDraft])

    bridge()
    await notebook.render()
    expect(notebook.drafts).toEqual({ 'cell-1': sourceDraft })
    notebook.setCellCode('cell-1', CELLS[1].code)
    notebook.setCellCode('cell-2', SORT_DRAFT)
    const first = flow.captureSnapshot()
    flow.importFromFlowfile(flow.exportToFlowfile('another'))
    expect(notebook.drafts).toEqual({})
    flow.loadFromSnapshot(first)
    expect(notebook.drafts).toEqual({ 'cell-2': SORT_DRAFT })
  })

  it("never sends a locked node's settings, and never a locked cell's text", async () => {
    const flow = useFlowStore()
    const shared = {
      flowfile_version: '1.0.0',
      flowfile_id: 1,
      flowfile_name: 'Shared',
      flowfile_settings: { description: '', execution_mode: 'Development', execution_location: 'local', auto_save: true, show_detailed_progress: false },
      nodes: [
        { id: 1, type: 'manual_input', is_start_node: true, description: '', x_position: 0, y_position: 0, input_ids: [], outputs: [2, 3], setting_input: { node_id: 1, is_setup: true, raw_data_format: { columns: [{ name: 'a', data_type: 'Int64' }], data: [[1, 2]] } } },
        { id: 2, type: 'polars_code', is_start_node: false, description: '', x_position: 200, y_position: 0, input_ids: [1], outputs: [], setting_input: { node_id: 2, is_setup: true, polars_code_input: { polars_code: `output_df = ${CANARY}` } } },
        { id: 3, type: 'sort', is_start_node: false, description: '', x_position: 200, y_position: 200, input_ids: [1], outputs: [], setting_input: { node_id: 3, is_setup: true, sort_input: [{ column: 'a', how: 'asc' }] } }
      ]
    } as unknown as FlowfileData
    expect(flow.importFromFlowfile(shared, { untrustedCode: true })).toBe(true)
    const cells = [IMPORTS, cell(1, 'source_1 = ff.from_raw_data({})'), cell(2, 'transformed_2 = ff.canvas_node(2, source_1)', { status: 'placeholder', reason: 'locked' }), cell(3, 'ordered_3 = source_1.sort(["a"], descending=[False])')]
    bridge({ ok: true, nodes: {}, inputs: {}, warnings: [] }, cells)
    const notebook = useNotebookStore()
    await notebook.render()
    notebook.setCellCode('cell-2', `transformed_2 = ${CANARY}`)
    notebook.setCellCode('cell-3', 'ordered_3 = source_1.sort(["a"], descending=[True])')

    await notebook.push()

    const request = requests()[0]
    expect(Object.keys(request.drafts)).toEqual(['cell-3'])
    expect(request.locked).toEqual({ '2': 'locked until the shared flow is trusted' })
    expect(request.flow.nodes.find((node: any) => node.id === 2).setting_input).toEqual({})
    expect(JSON.stringify(request)).not.toContain(CANARY)
    for (const source of bridgeSources()) expect(source).not.toContain(CANARY)
  })
})

describe('running, and cells the canvas changes under the user', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = true
    pyodideMock.runPython.mockResolvedValue(undefined)
    pyodideMock.ensurePyPackages.mockResolvedValue(undefined)
    bridge()
  })

  const SOURCE_DRAFT = 'source_1 = ff.from_raw_data({"columns": []})'

  it('Run pushes only the changed cells above the one it runs, and leaves the rest changed', async () => {
    const { notebook } = await sourceAndSort()
    bridge({ ok: true, nodes: {}, inputs: {}, node_ids_by_cell: { 'cell-1': [[1]] }, warnings: [] })
    notebook.setCellCode('cell-1', SOURCE_DRAFT)
    notebook.setCellCode('cell-2', SORT_DRAFT)

    await notebook.runCell('cell-1')

    expect(Object.keys(requests()[0].drafts)).toEqual(['cell-1'])
    expect(notebook.drafts).toEqual({ 'cell-2': SORT_DRAFT })
  })

  it('says on the cell that was run why it did not run when a cell above is refused', async () => {
    const { flow, notebook } = await sourceAndSort()
    bridge({ ok: false, cell_id: 'cell-1', line: 1, kind: 'refused', message: 'no handler' })
    notebook.setCellCode('cell-1', SOURCE_DRAFT)
    const runs = vi.spyOn(flow, 'executeNodeWithUpstream')

    await notebook.runCell('cell-2')

    expect(runs).not.toHaveBeenCalled()
    expect(notebook.syncError?.cellId).toBe('cell-1')
    expect(notebook.outputs['cell-2']).toMatchObject({ state: 'blocked', message: expect.stringContaining('could not be pushed') })
  })

  it('moves a changed cell to the cell that holds its nodes now', async () => {
    const { notebook } = await sourceAndSort()
    notebook.setCellCode('cell-2', SORT_DRAFT)
    bridge(SORTED, [IMPORTS, CELLS[1], { ...cell(2, SORT_CODE), cell_id: 'cell-2b' }])

    await notebook.render()

    expect(notebook.drafts).toEqual({ 'cell-2b': SORT_DRAFT })
    expect(notebook.shownCells.some(each => each.detached)).toBe(false)
  })

  it('keeps a changed cell whose steps are gone as a detached cell that is never pushed', async () => {
    const { notebook } = await sourceAndSort()
    notebook.setCellCode('cell-2', SORT_DRAFT)
    bridge(SORTED, [IMPORTS, CELLS[1]])

    await notebook.render()

    const detached = notebook.shownCells.find(each => each.detached)!
    expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', detached.cell_id])
    expect(notebook.cellCode(detached)).toBe(SORT_DRAFT)
    expect(notebook.isEditable(detached)).toBe(false)
    expect(notebook.cellSyncState(detached.cell_id)).toBe('detached')
    expect(notebook.notice).toContain('detached cell')
    expect(notebook.changedCount).toBe(0)

    // The step is back (an undo): the text goes back to its cell.
    bridge(SORTED, CELLS)
    await notebook.render()
    expect(notebook.shownCells.some(each => each.detached)).toBe(false)
    expect(notebook.drafts).toEqual({ 'cell-2': SORT_DRAFT })

    bridge(SORTED, [IMPORTS, CELLS[1]])
    await notebook.render()
    const again = notebook.shownCells.find(each => each.detached)!
    notebook.adoptDetached(again.cell_id)
    const adopted = notebook.shownCells.find(each => each.fresh)!
    expect(notebook.cellCode(adopted)).toBe(SORT_DRAFT)
    expect(notebook.changedCount).toBe(1)
    expect(notebook.shownCells.some(each => each.detached)).toBe(false)
  })

  it('keeps the cells in the order they were shown when a re-render would move them', async () => {
    const { notebook } = await sourceAndSort()
    const third = cell(3, 'other_3 = ff.from_raw_data({})')
    bridge(SORTED, [IMPORTS, CELLS[1], CELLS[2], third])
    await notebook.render()
    bridge(SORTED, [IMPORTS, CELLS[1], third, CELLS[2]])

    await notebook.render()

    expect(notebook.shownCells.map(each => each.cell_id)).toEqual(['imports', 'cell-1', 'cell-2', 'cell-3'])
  })

  it('a cell run holds the flow busy, so the canvas Run and undo wait for it', async () => {
    const { flow, notebook, sort } = await sourceAndSort()
    let busy: boolean | null = null
    pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
      if (source.includes('_lazyframes.keys()')) {
        busy = flow.isExecuting
        return []
      }
      if (source.includes('fetch_preview(')) return { success: true, data: { columns: ['a'], data: [[1]], total_rows: 1 } }
      return { success: true }
    })

    await notebook.runCell(`cell-${sort}`)

    expect(busy).toBe(true)
    expect(flow.isExecuting).toBe(false)
  })
})

describe('checking changed cells while they are written', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = true
    pyodideMock.runPython.mockResolvedValue(undefined)
    pyodideMock.ensurePyPackages.mockResolvedValue(undefined)
    bridge()
  })

  const REFUSED = { ok: false, cell_id: 'cell-2', line: 1, kind: 'refused', message: '`pivot` cannot be written' }

  /** Answer a check with `answer`; renders and syncs as `bridge` does. */
  function checks(answer: unknown) {
    pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
      if (source === CHECK_SOURCE) return typeof answer === 'function' ? answer() : answer
      if (source === SYNC_SOURCE) return SORTED
      if (source.includes('render_notebook(')) return { cells: CELLS, warnings: [], var_by_node: {} }
      return { success: true }
    })
  }

  it('shows on its line why a push would refuse a cell, a moment after typing stops, and lands nothing', async () => {
    vi.useFakeTimers()
    try {
      const { flow, notebook, sort } = await sourceAndSort()
      checks(REFUSED)
      const draft = `ordered_2 = source_1.pivot(on="a")  # ${CANARY}`
      notebook.setCellCode('cell-2', draft)
      expect(notebook.syncError).toBeNull()

      await vi.advanceTimersByTimeAsync(CHECK_DELAY_MS)

      expect(notebook.syncError).toMatchObject({ cellId: 'cell-2', line: 1, kind: 'refused', code: draft })
      expect(notebook.cellSyncState('cell-2')).toBe('failed')
      expect(notebook.drafts).toEqual({ 'cell-2': draft })
      expect(sortInput(flow, sort)).toEqual([{ column: 'a', how: 'asc' }])
      expect(syncCalls()).toHaveLength(0)
      const sent = pyodideMock.setGlobal.mock.calls.filter(call => call[0] === '_notebook_check_request')
      expect(JSON.parse(sent[0][1]).drafts).toEqual({ 'cell-2': draft })
      for (const source of bridgeSources()) expect(source).not.toContain(CANARY)
    } finally {
      vi.useRealTimers()
    }
  })

  it('drops an answer about text that changed while it was being checked', async () => {
    const { notebook } = await sourceAndSort()
    let typeMore: () => void = () => undefined
    checks(() => {
      typeMore()
      return REFUSED
    })
    notebook.setCellCode('cell-2', SORT_DRAFT)
    typeMore = () => notebook.setCellCode('cell-2', `${SORT_DRAFT}  # more`)

    await notebook.check()

    expect(notebook.syncError).toBeNull()
  })

  it('keeps a cell of steps no push can change as the render wrote it', async () => {
    const { notebook } = await sourceAndSort()
    pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
      if (source.includes('notebook_surface()')) return { ff: [], methods: {}, pushable: [], node_types: ['sort'] }
      if (source.includes('render_notebook(')) return { cells: CELLS, warnings: [], var_by_node: {} }
      return { success: true }
    })
    await notebook.render()

    const [, source, sort] = notebook.shownCells
    expect(notebook.canvasOnly(source)).toBe(true)
    expect(notebook.isEditable(source)).toBe(false)
    notebook.setCellCode(source.cell_id, 'source_1 = ff.from_raw_data({"columns": []})')
    expect(notebook.drafts).toEqual({})
    expect(notebook.canvasOnly(sort)).toBe(false)
    expect(notebook.isEditable(sort)).toBe(true)
  })
})
