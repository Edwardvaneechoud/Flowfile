/**
 * The notebook store renders the open flow as cells and does nothing else.
 *
 * A render reads settings only: it never executes a node, never previews one and
 * never starts Pyodide. A node the editor keeps out of code (a placeholder, shared
 * code not yet trusted, a browser-only type) crosses the bridge with empty settings,
 * so nothing its sender wrote reaches Python.
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
import type { FlowfileData } from '../../src/types'

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

/** The three JSON arguments of the one render call: the flow, the schemas and the locked nodes. */
function renderArguments(): [any, Record<string, unknown>, Record<string, string>] {
  const source = renderCalls().at(-1)!
  const args = [...source.matchAll(/json\.loads\(("(?:[^"\\]|\\.)*")\)/g)].map(match => JSON.parse(JSON.parse(match[1])))
  expect(args).toHaveLength(3)
  return args as [any, Record<string, unknown>, Record<string, string>]
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

  it('keeps the newer render when an older one finishes last', async () => {
    useFlowStore().addNode('manual_input', 0, 0)
    const notebook = useNotebookStore()
    let finishFirst: (value: unknown) => void = () => undefined
    pyodideMock.runPythonWithResult
      .mockImplementationOnce(() => new Promise(resolve => (finishFirst = resolve)))
      .mockResolvedValueOnce({ ...RENDERING, cells: [cell(1, 'newer = 1')] })

    const first = notebook.render()
    await notebook.render()
    finishFirst({ ...RENDERING, cells: [cell(1, 'older = 1')] })
    await first

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
