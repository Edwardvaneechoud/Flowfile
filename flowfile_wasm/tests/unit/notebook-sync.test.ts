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
import { SYNC_SOURCE, useNotebookStore, type NotebookCell } from '../../src/stores/notebook-store'
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
    expect(bridgeSources().filter(source => source.includes('render_notebook('))).toHaveLength(2)
    expect(bridgeSources().filter(source => /execute_|fetch_preview\(/.test(source))).toEqual([])

    expect(flow.undo()).toBe(true)
    expect(sortInput(flow, sort)).toEqual([{ column: 'a', how: 'asc' }])
    expect(flow.redo()).toBe(true)
    expect(sortInput(flow, sort)).toEqual([{ column: 'a', how: 'desc' }])

    notebook.setCellCode('cell-2', SORT_DRAFT)
    expect(notebook.cellSyncState('cell-2')).toBe('edited')
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

  it('drops a draft whose cell is gone or now says the same, and keeps drafts with their flow', async () => {
    const { flow, notebook } = await sourceAndSort()
    notebook.setCellCode('cell-1', 'source_1 = ff.from_raw_data({"columns": [], "data": []})')
    notebook.setCellCode('cell-2', SORT_DRAFT)

    bridge(SORTED, [IMPORTS, cell(2, SORT_DRAFT)])
    await notebook.render()
    expect(notebook.drafts).toEqual({})

    bridge()
    await notebook.render()
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
