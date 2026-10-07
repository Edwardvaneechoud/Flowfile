/**
 * The notebook store renders the open flow as cells and runs a cell only when asked.
 *
 * A render reads settings only: it never executes a node, never previews one and
 * never starts Pyodide. A node the editor keeps out of code (a placeholder, shared
 * code not yet trusted, a browser-only type) crosses the bridge with empty settings,
 * so nothing its sender wrote reaches Python. `runCell` and `runAll` are the only
 * calls that execute, and a locked node is never among what they execute.
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
import { useNotebookStore, type NotebookCell } from '../../src/stores/notebook-store'
import { EXPR_TRANSFORMER_PACKAGE } from '../../src/composables/useFormulaTranslation'
import type { FlowEdge, FlowfileData } from '../../src/types'

const CANARY = 'CANARY_NOTEBOOK_4b7e'

const cell = (id: number, code: string): NotebookCell => ({
  cell_id: `cell-${id}`,
  node_ids: [id],
  kind: 'node',
  code,
  defines: [],
  uses: [],
  status: 'code',
  reason: null
})

const RENDERING = { cells: [cell(1, 'source_1 = ff.from_raw_data({})')], warnings: [], var_by_node: { '1': 'source_1' } }

const bridgeStrings = () => pyodideMock.runPythonWithResult.mock.calls.map(call => String(call[0]))
const renderCalls = () => bridgeStrings().filter(src => src.includes('render_notebook('))

/** What the last render was asked: the flow, the schemas, the locked nodes and the written cells, sent as data. */
function renderArguments(): [any, Record<string, unknown>, Record<string, string>, number[][]] {
  const sent = pyodideMock.setGlobal.mock.calls.filter(call => call[0] === '_notebook_render_request').at(-1)!
  const { flow, schemas, locked, layout } = JSON.parse(sent[1])
  return [flow, schemas, locked, layout]
}

function sharedFlow(): FlowfileData {
  const node = (id: number, type: string, input_ids: number[], outputs: number[], setting_input: Record<string, unknown>) => ({
    id,
    type,
    is_start_node: input_ids.length === 0,
    description: '',
    x_position: id * 200,
    y_position: 0,
    input_ids,
    outputs,
    setting_input: { node_id: id, is_setup: true, ...setting_input }
  })
  return {
    flowfile_version: '1.0.0',
    flowfile_id: 1,
    flowfile_name: 'Shared',
    flowfile_settings: {
      description: '',
      execution_mode: 'Development',
      execution_location: 'local',
      auto_save: true,
      show_detailed_progress: false
    },
    nodes: [
      node(1, 'manual_input', [], [2, 3], {
        raw_data_format: { columns: [{ name: 'a', data_type: 'Int64' }], data: [[1, 2]] }
      }),
      node(2, 'polars_code', [1], [], { polars_code_input: { polars_code: `output_df = ${CANARY}_code` } }),
      node(3, 'database_reader__unsupported', [1], [], {
        is_placeholder: true,
        original_type: 'database_reader',
        reason: 'Needs a database connection',
        query: `${CANARY}_placeholder`
      })
    ]
  } as unknown as FlowfileData
}

describe('notebook store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = true
    pyodideMock.runPythonWithResult.mockResolvedValue(RENDERING)
    pyodideMock.ensurePyPackages.mockResolvedValue(undefined)
    pyodideMock.runPython.mockResolvedValue(undefined)
  })

  it('does nothing before Pyodide is ready', async () => {
    pyodideMock.isReady = false
    useFlowStore().addNode('manual_input', 0, 0)
    const notebook = useNotebookStore()
    pyodideMock.runPythonWithResult.mockClear()

    await notebook.render()

    expect(pyodideMock.runPythonWithResult).not.toHaveBeenCalled()
    expect(pyodideMock.ensurePyPackages).not.toHaveBeenCalled()
    expect(notebook.cells).toEqual([])
    expect(notebook.fingerprint).toBeNull()
  })

  it('renders through one bridge call that executes and previews nothing', async () => {
    const flow = useFlowStore()
    const id = flow.addNode('manual_input', 0, 0)
    const notebook = useNotebookStore()
    pyodideMock.runPythonWithResult.mockClear()

    await notebook.render()

    expect(renderCalls()).toHaveLength(1)
    expect(bridgeStrings().filter(src => /execute_|fetch_preview\(/.test(src))).toEqual([])
    expect(pyodideMock.runPython).not.toHaveBeenCalled()
    const [sent, , locked] = renderArguments()
    expect(sent.nodes.map((node: any) => node.id)).toEqual([id])
    expect(locked).toEqual({})
    expect(notebook.cells).toEqual(RENDERING.cells)
    expect(notebook.varByNode).toEqual({ '1': 'source_1' })
    expect(notebook.error).toBeNull()
    expect(notebook.loading).toBe(false)
  })

  it('is stale until rendered and again once the flow changes, but not when a node only moves', async () => {
    const flow = useFlowStore()
    const id = flow.addNode('manual_input', 0, 0)
    const notebook = useNotebookStore()
    expect(notebook.stale).toBe(true)

    await notebook.render()
    expect(notebook.stale).toBe(false)

    const node = flow.getNode(id)!
    node.x = 400
    flow.updateNodeSettings(id, { ...node.settings, pos_x: 400 } as any)
    expect(notebook.stale).toBe(false)

    flow.updateNodeDescription(id, 'The source')
    expect(notebook.stale).toBe(true)
  })

  it('sends a locked node with empty settings: shared code, a placeholder and a browser-only node', async () => {
    const flow = useFlowStore()
    expect(flow.importFromFlowfile(sharedFlow(), { untrustedCode: true })).toBe(true)
    const external = flow.addNode('external_data', 0, 0)
    flow.updateNodeSettings(external, { ...flow.getNode(external)!.settings, dataset_name: `${CANARY}_external` } as any)
    const notebook = useNotebookStore()
    pyodideMock.runPythonWithResult.mockClear()

    await notebook.render()

    // What a sender wrote reaches no bridge at all; the render carries no locked node's settings.
    for (const source of bridgeStrings()) {
      expect(source).not.toContain(`${CANARY}_code`)
      expect(source).not.toContain(`${CANARY}_placeholder`)
    }
    for (const source of renderCalls()) expect(source).not.toContain(CANARY)
    const [sent, , locked] = renderArguments()
    expect(locked).toEqual({
      2: 'locked until the shared flow is trusted',
      3: 'Needs a database connection',
      [external]: 'reads a dataset its host page provides'
    })
    const settings = Object.fromEntries(sent.nodes.map((node: any) => [node.id, node.setting_input]))
    expect(settings[2]).toEqual({})
    expect(settings[3]).toEqual({})
    expect(settings[external]).toEqual({})
    expect(settings[1].raw_data_format).toBeDefined()
  })

  it('sends shared code once it is trusted, and trusting makes the cells stale', async () => {
    const flow = useFlowStore()
    flow.importFromFlowfile(sharedFlow(), { untrustedCode: true })
    const notebook = useNotebookStore()
    await notebook.render()
    expect(notebook.stale).toBe(false)

    flow.trustSharedCode()
    expect(notebook.stale).toBe(true)
    await notebook.render()

    const [sent, , locked] = renderArguments()
    expect(Object.keys(locked)).toEqual(['3'])
    const code = sent.nodes.find((node: any) => node.id === 2).setting_input.polars_code_input.polars_code
    expect(code).toBe(`output_df = ${CANARY}_code`)
  })

  it('loads the formula package only when a formula is in the flow, and renders without it', async () => {
    const flow = useFlowStore()
    flow.addNode('manual_input', 0, 0)
    const notebook = useNotebookStore()
    await notebook.render()
    expect(pyodideMock.ensurePyPackages).not.toHaveBeenCalled()

    flow.addNode('formula', 200, 0)
    pyodideMock.ensurePyPackages.mockRejectedValue(new Error('offline'))
    await notebook.render()

    expect(pyodideMock.ensurePyPackages).toHaveBeenCalledWith([EXPR_TRANSFORMER_PACKAGE])
    expect(notebook.error).toBeNull()
    expect(renderCalls()).toHaveLength(2)
  })

  it('sends the schemas the editor already knows', async () => {
    const flow = useFlowStore()
    const id = flow.addNode('manual_input', 0, 0)
    flow.nodeResults.set(id, { success: true, schema: [{ name: 'a', data_type: 'Int64' }] } as any)
    const notebook = useNotebookStore()
    pyodideMock.runPythonWithResult.mockClear()

    await notebook.render()

    expect(renderArguments()[1]).toEqual({ [id]: [{ name: 'a', data_type: 'Int64' }] })
  })

  it('shows the newer of two renders, and runs engine calls one after another', async () => {
    useFlowStore().addNode('manual_input', 0, 0)
    const notebook = useNotebookStore()
    let finishFirst: (value: unknown) => void = () => undefined
    const answers = [
      () => new Promise(resolve => (finishFirst = resolve)),
      () => Promise.resolve({ ...RENDERING, cells: [cell(1, 'newer = 1')] })
    ]
    pyodideMock.runPythonWithResult.mockImplementation(async (source: string) =>
      source.includes('render_notebook(') ? answers.shift()!() : { success: true }
    )
    pyodideMock.runPythonWithResult.mockClear()

    const first = notebook.render()
    const second = notebook.render()
    await vi.waitFor(() => expect(renderCalls()).toHaveLength(1))
    // The second render's request is not set while the first one's is still being read.
    expect(pyodideMock.setGlobal.mock.calls.filter(call => call[0] === '_notebook_render_request')).toHaveLength(1)
    finishFirst({ ...RENDERING, cells: [cell(1, 'older = 1')] })
    await Promise.all([first, second])

    expect(notebook.cells[0].code).toBe('newer = 1')
    expect(notebook.loading).toBe(false)
  })

  it('reports a failed render and keeps the cells it had', async () => {
    useFlowStore().addNode('manual_input', 0, 0)
    const notebook = useNotebookStore()
    await notebook.render()

    pyodideMock.runPythonWithResult.mockRejectedValueOnce(new Error('boom'))
    await notebook.render()

    expect(notebook.error).toBe('boom')
    expect(notebook.cells).toEqual(RENDERING.cells)
    expect(notebook.loading).toBe(false)
  })
})

describe('running notebook cells', () => {
  const PREVIEW = { columns: ['a', 'b'], data: [[1, 'x'], [2, null]], total_rows: 2 }
  const CELLS = [
    { ...cell(0, 'import flowfile as ff'), cell_id: 'imports', node_ids: [], kind: 'imports' as const },
    cell(1, 'source_1 = ff.from_raw_data({})'),
    cell(2, 'kept_2 = source_1.filter(ff.col("a") > 1)')
  ]

  const executeCalls = () => bridgeStrings().filter(src => /execute_/.test(src))
  const previewCalls = () => bridgeStrings().filter(src => /fetch_preview\(/.test(src))

  /** Answer each bridge call by what it asks for; `execute` and `preview` override the happy path. */
  function bridge(answers: { execute?: (source: string) => unknown; preview?: (source: string) => unknown } = {}) {
    pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
      if (source.includes('render_notebook(')) return { cells: CELLS, warnings: [], var_by_node: {} }
      if (source.includes('_lazyframes.keys()')) return []
      if (source.includes('fetch_preview(')) return answers.preview?.(source) ?? { success: true, data: PREVIEW }
      if (/execute_/.test(source)) {
        return answers.execute?.(source) ?? { success: true, schema: [{ name: 'a', data_type: 'Int64' }] }
      }
      return { success: true }
    })
  }

  const edge = (source: number, target: number): FlowEdge => ({
    id: `e${source}-${target}`,
    source: String(source),
    target: String(target),
    sourceHandle: 'output-0',
    targetHandle: 'input-0'
  })

  /** A source with rows feeding a filter, rendered as two cells. */
  async function sourceAndFilter() {
    const flow = useFlowStore()
    const source = flow.addNode('manual_input', 0, 0)
    flow.updateNodeSettings(source, {
      ...flow.getNode(source)!.settings,
      raw_data_format: { columns: [{ name: 'a', data_type: 'Int64' }], data: [[1, 2]] }
    } as any)
    const filter = flow.addNode('filter', 200, 0)
    flow.addEdge(edge(source, filter))
    const notebook = useNotebookStore()
    await notebook.render()
    pyodideMock.runPythonWithResult.mockClear()
    return { flow, notebook, source, filter }
  }

  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    pyodideMock.isReady = true
    pyodideMock.ensurePyPackages.mockResolvedValue(undefined)
    pyodideMock.runPython.mockResolvedValue(undefined)
    bridge()
  })

  it('runs a cell: its node with the upstream that has to run, then its first rows', async () => {
    const { notebook, source, filter } = await sourceAndFilter()

    await notebook.runCell('cell-2')

    expect(executeCalls()).toHaveLength(2)
    expect(executeCalls()[0]).toContain(`execute_manual_input(${source},`)
    expect(executeCalls()[1]).toContain(`(${filter}, ${source},`)
    expect(previewCalls()).toHaveLength(1)
    expect(previewCalls()[0]).toContain(`fetch_preview(${filter}, max_rows=100`)
    expect(notebook.outputs).toEqual({
      'cell-2': {
        state: 'rows',
        nodeId: filter,
        columns: ['a', 'b'],
        dtypes: { a: 'Int64' },
        rows: [{ a: 1, b: 'x' }, { a: 2, b: null }],
        total: 2
      }
    })
    expect(notebook.running).toBe(false)
    expect(notebook.canRun).toBe(true)
  })

  it('keeps at most 100 rows and the true row count', async () => {
    const { notebook } = await sourceAndFilter()
    const data = Array.from({ length: 130 }, (_, index) => [index, 'x'])
    bridge({ preview: () => ({ success: true, data: { columns: ['a', 'b'], data, total_rows: 5000 } }) })

    await notebook.runCell('cell-2')

    const output = notebook.outputs['cell-2']
    expect(output.state).toBe('rows')
    if (output.state !== 'rows') return
    expect(output.rows).toHaveLength(100)
    expect(output.total).toBe(5000)
  })

  it("shows the node's error and reads no rows", async () => {
    const { notebook, filter } = await sourceAndFilter()
    bridge({
      execute: source =>
        source.includes(`(${filter}, `) ? { success: false, error: 'column "a" not found' } : { success: true }
    })

    await notebook.runCell('cell-2')

    expect(notebook.outputs['cell-2']).toMatchObject({ state: 'error', message: 'column "a" not found' })
    expect(previewCalls()).toEqual([])
  })

  it('shows why the rows could not be read', async () => {
    const { notebook } = await sourceAndFilter()
    bridge({ preview: () => ({ success: false, error: 'the plan failed to collect' }) })

    await notebook.runCell('cell-2')

    expect(notebook.outputs['cell-2']).toMatchObject({ state: 'error', message: 'the plan failed to collect' })
  })

  it('runs neither the imports cell nor a cell it does not know', async () => {
    const { notebook } = await sourceAndFilter()

    await notebook.runCell('imports')
    await notebook.runCell('cell-99')

    expect(pyodideMock.runPythonWithResult).not.toHaveBeenCalled()
    expect(notebook.outputs).toEqual({})
  })

  it('never executes a locked node: the cell says why', async () => {
    const flow = useFlowStore()
    expect(flow.importFromFlowfile(sharedFlow(), { untrustedCode: true })).toBe(true)
    const notebook = useNotebookStore()
    pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
      if (source.includes('render_notebook(')) {
        return { cells: [cell(1, 'a'), cell(2, 'b'), cell(3, 'c')], warnings: [], var_by_node: {} }
      }
      return { success: true }
    })
    await notebook.render()
    pyodideMock.runPythonWithResult.mockClear()

    await notebook.runCell('cell-2')
    await notebook.runCell('cell-3')

    expect(pyodideMock.runPythonWithResult).not.toHaveBeenCalled()
    expect(notebook.outputs['cell-2'].state).toBe('blocked')
    expect(notebook.outputs['cell-3']).toEqual({ state: 'blocked', message: 'Needs a database connection' })
  })

  it('Run all runs the flow as the canvas does and shows every node cell', async () => {
    const { notebook, source, filter } = await sourceAndFilter()

    await notebook.runAll()

    expect(pyodideMock.runPython).toHaveBeenCalledWith('clear_all()')
    expect(executeCalls()).toHaveLength(2)
    expect(previewCalls().map(src => /fetch_preview\((\d+),/.exec(src)![1])).toEqual([String(source), String(filter)])
    expect(Object.keys(notebook.outputs).sort()).toEqual(['cell-1', 'cell-2'])
    expect(notebook.outputs['cell-1'].state).toBe('rows')
    expect(notebook.outputs['cell-2'].state).toBe('rows')
    expect(notebook.running).toBe(false)
  })

  it('Run all leaves locked nodes alone: nothing a sender wrote reaches Python', async () => {
    const flow = useFlowStore()
    flow.importFromFlowfile(sharedFlow(), { untrustedCode: true })
    const notebook = useNotebookStore()
    pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
      if (source.includes('render_notebook(')) {
        return { cells: [cell(1, 'a'), cell(2, 'b'), cell(3, 'c')], warnings: [], var_by_node: {} }
      }
      if (source.includes('fetch_preview(')) return { success: true, data: PREVIEW }
      return { success: true }
    })
    await notebook.render()
    pyodideMock.runPythonWithResult.mockClear()

    await notebook.runAll()

    expect(executeCalls()).toHaveLength(1)
    expect(executeCalls()[0]).toContain('execute_manual_input(1,')
    expect(previewCalls()).toHaveLength(1)
    for (const source of bridgeStrings()) expect(source).not.toContain(CANARY)
    expect(notebook.outputs['cell-1'].state).toBe('rows')
    expect(notebook.outputs['cell-2'].state).toBe('blocked')
    expect(notebook.outputs['cell-3'].state).toBe('blocked')
  })

  it('runs one thing at a time', async () => {
    const { notebook } = await sourceAndFilter()
    let finish: (value: unknown) => void = () => undefined
    bridge({ execute: () => new Promise(resolve => (finish = resolve)) })

    const first = notebook.runCell('cell-1')
    await vi.waitFor(() => expect(executeCalls()).toHaveLength(1))
    expect(notebook.outputs['cell-1']).toEqual({ state: 'running' })
    expect(notebook.canRun).toBe(false)

    await notebook.runCell('cell-2')
    await notebook.runAll()
    expect(executeCalls()).toHaveLength(1)
    expect(notebook.outputs['cell-2']).toBeUndefined()

    finish({ success: true })
    await first
    expect(notebook.outputs['cell-1'].state).toBe('rows')
    expect(notebook.canRun).toBe(true)
  })

  it('does not run while the canvas is running the flow, or before Pyodide is ready', async () => {
    const { flow, notebook } = await sourceAndFilter()

    flow.isExecuting = true
    await notebook.runCell('cell-2')
    await notebook.runAll()
    flow.isExecuting = false
    pyodideMock.isReady = false
    await notebook.runCell('cell-2')

    expect(pyodideMock.runPythonWithResult).not.toHaveBeenCalled()
    expect(notebook.outputs).toEqual({})
  })

  it('marks an output out of date once its step changes, and clears outputs on request', async () => {
    const { flow, notebook, source } = await sourceAndFilter()
    await notebook.runCell('cell-2')
    expect(notebook.outputStale('cell-2')).toBe(false)

    flow.updateNodeSettings(source, { ...flow.getNode(source)!.settings, description: 'changed upstream' } as any)
    expect(notebook.outputStale('cell-2')).toBe(true)

    notebook.clearOutputs()
    expect(notebook.outputs).toEqual({})
    expect(notebook.outputStale('cell-2')).toBe(false)
  })

  it('drops an output when its cell goes, and keeps outputs with their flow', async () => {
    const { flow, notebook } = await sourceAndFilter()
    await notebook.runCell('cell-1')
    await notebook.runCell('cell-2')
    expect(Object.keys(notebook.outputs).sort()).toEqual(['cell-1', 'cell-2'])

    pyodideMock.runPythonWithResult.mockResolvedValueOnce({ cells: [CELLS[0], CELLS[1]], warnings: [], var_by_node: {} })
    await notebook.render()
    expect(Object.keys(notebook.outputs)).toEqual(['cell-1'])

    const first = flow.captureSnapshot()
    flow.importFromFlowfile(sharedFlow())
    expect(notebook.outputs).toEqual({})

    flow.loadFromSnapshot(first)
    expect(Object.keys(notebook.outputs)).toEqual(['cell-1'])
  })

  it("keeps each flow's cells with it, so another tab never shows them", async () => {
    const { flow, notebook } = await sourceAndFilter()
    const shown = notebook.cells.map(cell => cell.cell_id)
    expect(shown.length).toBeGreaterThan(1)

    const first = flow.captureSnapshot()
    flow.importFromFlowfile(sharedFlow())
    expect(notebook.cells).toEqual([])
    expect(notebook.fingerprint).toBeNull()

    flow.loadFromSnapshot(first)
    expect(notebook.cells.map(cell => cell.cell_id)).toEqual(shown)
  })
})

