/**
 * NotebookPane Component Tests
 *
 * The pane shows the open flow as read-only cells. It renders only while it is
 * the tab showing and Pyodide is ready, follows the flow as it changes, and a
 * picked cell selects its node on the canvas.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mount } from '@vue/test-utils'
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

vi.mock('vue-codemirror', () => ({
  Codemirror: {
    props: ['modelValue', 'disabled'],
    template: '<pre class="cm-stub" :data-disabled="String(disabled)">{{ modelValue }}</pre>'
  }
}))

import NotebookPane from '../../src/components/notebook/NotebookPane.vue'
import { useFlowStore } from '../../src/stores/flow-store'
import type { NotebookCell } from '../../src/stores/notebook-store'

const RENDER_DELAY = 200

const cell = (cell_id: string, node_ids: number[], code: string, extra: Partial<NotebookCell> = {}): NotebookCell => ({
  cell_id,
  node_ids,
  kind: 'node',
  code,
  defines: [],
  uses: [],
  status: 'code',
  reason: null,
  ...extra
})

const renderCalls = () =>
  pyodideMock.runPythonWithResult.mock.calls.filter(call => String(call[0]).includes('render_notebook('))

/** A source feeding a filter; returns their ids and the rendering the bridge answers with. */
function twoNodeFlow() {
  const flow = useFlowStore()
  const source = flow.addNode('manual_input', 0, 0)
  const filter = flow.addNode('filter', 200, 0)
  flow.updateNodeDescription(filter, 'Keep the big ones')
  const cells = [
    cell('imports', [], 'import flowfile as ff', { kind: 'imports' }),
    cell('cell-1', [source], 'source_1 = ff.from_raw_data({})'),
    cell('cell-2', [filter], 'kept_2 = ff.canvas_node(2, source_1)', { status: 'placeholder', reason: 'renders no code' })
  ]
  pyodideMock.runPythonWithResult.mockResolvedValue({ cells, warnings: ['One warning'], var_by_node: {} })
  return { flow, source, filter, cells }
}

async function mountPane(active = true) {
  const wrapper = mount(NotebookPane, { props: { active } })
  await vi.advanceTimersByTimeAsync(RENDER_DELAY)
  return wrapper
}

describe('NotebookPane', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
    vi.useFakeTimers()
    pyodideMock.isReady = true
    pyodideMock.runPython.mockResolvedValue(undefined)
    pyodideMock.ensurePyPackages.mockResolvedValue(undefined)
    pyodideMock.runPythonWithResult.mockResolvedValue({ cells: [], warnings: [], var_by_node: {} })
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('waits for Python and asks the bridge for nothing meanwhile', async () => {
    pyodideMock.isReady = false
    twoNodeFlow()
    pyodideMock.runPythonWithResult.mockClear()

    const wrapper = await mountPane()

    expect(wrapper.text()).toContain('Python is still starting up')
    expect(pyodideMock.runPythonWithResult).not.toHaveBeenCalled()
  })

  it('does not render while another tab is showing, and renders once it is picked', async () => {
    twoNodeFlow()
    pyodideMock.runPythonWithResult.mockClear()

    const wrapper = await mountPane(false)
    expect(renderCalls()).toHaveLength(0)

    await wrapper.setProps({ active: true })
    await vi.advanceTimersByTimeAsync(RENDER_DELAY)
    expect(renderCalls()).toHaveLength(1)
  })

  it('shows each cell read-only under the nodes it stands for', async () => {
    const { source, filter } = twoNodeFlow()

    const wrapper = await mountPane()

    const cells = wrapper.findAll('.cell')
    expect(cells.map(each => each.find('.cell-label').text())).toEqual([
      'Imports',
      `#${source} Manual Input`,
      `#${filter} Keep the big ones`
    ])
    expect(cells.map(each => each.find('.cm-stub').text())).toEqual([
      'import flowfile as ff',
      'source_1 = ff.from_raw_data({})',
      'kept_2 = ff.canvas_node(2, source_1)'
    ])
    for (const each of cells) expect(each.find('.cm-stub').attributes('data-disabled')).toBe('true')
    expect(wrapper.find('.notebook-note--warning').text()).toBe('One warning')
  })

  it('marks a placeholder cell and says why on hover', async () => {
    twoNodeFlow()

    const wrapper = await mountPane()

    const badges = wrapper.findAll('.cell-badge')
    expect(badges).toHaveLength(1)
    expect(badges[0].text()).toBe('stays on the canvas')
    expect(badges[0].attributes('title')).toBe('renders no code')
    expect(wrapper.findAll('.cell--placeholder')).toHaveLength(1)
  })

  it('selects the node of a picked cell and asks the canvas to show it', async () => {
    const { flow, filter } = twoNodeFlow()
    const wrapper = await mountPane()

    await wrapper.find('[data-cell-id="imports"]').trigger('click')
    expect(wrapper.emitted('focus-node')).toBeUndefined()

    await wrapper.find('[data-cell-id="cell-2"]').trigger('click')
    expect(flow.selectedNodeId).toBe(filter)
    expect(wrapper.emitted('focus-node')).toEqual([[filter]])
    expect(wrapper.find('[data-cell-id="cell-2"]').classes()).toContain('cell--selected')
  })

  it('copies a cell without selecting it', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    const { flow } = twoNodeFlow()
    const wrapper = await mountPane()

    const copy = wrapper.find('[data-cell-id="cell-1"] .cell-action')
    await copy.trigger('click')
    await vi.advanceTimersByTimeAsync(0)

    expect(writeText).toHaveBeenCalledWith('source_1 = ff.from_raw_data({})')
    expect(copy.text()).toBe('Copied')
    expect(flow.selectedNodeId).toBeNull()
    expect(wrapper.emitted('focus-node')).toBeUndefined()
  })

  it('follows the flow: a change renders again, a move does not', async () => {
    const { flow, filter } = twoNodeFlow()
    await mountPane()
    expect(renderCalls()).toHaveLength(1)

    flow.updateNode(filter, { x: 480, y: 120 })
    await vi.advanceTimersByTimeAsync(RENDER_DELAY)
    expect(renderCalls()).toHaveLength(1)

    flow.updateNodeDescription(filter, 'Keep the small ones')
    await vi.advanceTimersByTimeAsync(RENDER_DELAY)
    expect(renderCalls()).toHaveLength(2)
  })

  it('offers another try when the render fails', async () => {
    twoNodeFlow()
    pyodideMock.runPythonWithResult.mockRejectedValue(new Error('boom'))

    const wrapper = await mountPane()
    const alert = wrapper.find('[role="alert"]')
    expect(alert.text()).toContain('The notebook could not be rendered: boom')

    pyodideMock.runPythonWithResult.mockResolvedValue({ cells: [cell('cell-1', [1], 'ok = 1')], warnings: [], var_by_node: {} })
    await alert.find('.note-action').trigger('click')
    await vi.advanceTimersByTimeAsync(0)

    expect(wrapper.find('[role="alert"]').exists()).toBe(false)
    expect(wrapper.find('.cm-stub').text()).toBe('ok = 1')
  })

  it('invites a first node when the flow is empty', async () => {
    pyodideMock.runPythonWithResult.mockResolvedValue({
      cells: [cell('imports', [], 'import flowfile as ff', { kind: 'imports' })],
      warnings: [],
      var_by_node: {}
    })

    const wrapper = await mountPane()

    expect(wrapper.text()).toContain('Add a node to the canvas and it appears here as code.')
  })
})
