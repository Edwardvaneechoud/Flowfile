/**
 * NotebookPane Component Tests
 *
 * The pane shows the open flow as read-only cells. It renders only while it is
 * the tab showing and Pyodide is ready, follows the flow as it changes, and a
 * picked cell selects its node on the canvas. A cell's Run shows its rows under
 * it; nothing runs until Run or Run all is pressed.
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
    emits: ['update:modelValue', 'ready'],
    template: '<pre class="cm-stub" :data-disabled="String(disabled)">{{ modelValue }}</pre>'
  }
}))

import { Codemirror } from 'vue-codemirror'
import NotebookPane from '../../src/components/notebook/NotebookPane.vue'
import { useFlowStore } from '../../src/stores/flow-store'
import { SYNC_SOURCE, type NotebookCell } from '../../src/stores/notebook-store'

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

  it('shows each cell under the nodes it stands for; only a code cell can be changed', async () => {
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
    expect(cells.map(each => each.find('.cm-stub').attributes('data-disabled'))).toEqual(['true', 'false', 'true'])
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

    await wrapper.find('[data-cell-id="imports"] .cell-head').trigger('click')
    expect(wrapper.emitted('focus-node')).toBeUndefined()

    // Clicking into the code places the caret; only the header picks the step.
    await wrapper.find('[data-cell-id="cell-2"] .cm-stub').trigger('click')
    expect(flow.selectedNodeId).toBeNull()

    await wrapper.find('[data-cell-id="cell-2"] .cell-head').trigger('click')
    expect(flow.selectedNodeId).toBe(filter)
    expect(wrapper.emitted('focus-node')).toEqual([[filter]])
    expect(wrapper.find('[data-cell-id="cell-2"]').classes()).toContain('cell--selected')
  })

  it('copies a cell without selecting it', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    const { flow } = twoNodeFlow()
    const wrapper = await mountPane()

    const copy = wrapper.find('[data-cell-id="cell-1"] [data-action="copy"]')
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

  describe('running', () => {
    const executeCalls = () =>
      pyodideMock.runPythonWithResult.mock.calls.map(call => String(call[0])).filter(src => /execute_|fetch_preview\(/.test(src))

    /** The two-node flow, connected and with rows in its source, and a bridge that answers runs and previews. */
    function runnableFlow() {
      const built = twoNodeFlow()
      built.flow.addEdge({
        id: 'e1-2',
        source: String(built.source),
        target: String(built.filter),
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })
      built.flow.updateNodeSettings(built.source, {
        ...built.flow.getNode(built.source)!.settings,
        raw_data_format: { columns: [{ name: 'a', data_type: 'Int64' }], data: [[1, 2]] }
      } as any)
      pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
        if (source.includes('render_notebook(')) return { cells: built.cells, warnings: [], var_by_node: {} }
        if (source.includes('_lazyframes.keys()')) return []
        if (source.includes('fetch_preview(')) {
          return { success: true, data: { columns: ['a', 'b'], data: [[1, 'x'], [2, null]], total_rows: 2 } }
        }
        return { success: true }
      })
      return built
    }

    it('offers Run on node cells and Run all above them, and runs nothing by showing them', async () => {
      const { flow, filter } = runnableFlow()

      const wrapper = await mountPane()
      flow.updateNodeDescription(filter, 'Keep the small ones')
      await vi.advanceTimersByTimeAsync(RENDER_DELAY)

      expect(wrapper.find('[data-cell-id="imports"] [data-action="run"]').exists()).toBe(false)
      expect(wrapper.findAll('[data-action="run"]')).toHaveLength(2)
      expect(wrapper.find('[data-action="run-all"]').exists()).toBe(true)
      expect(wrapper.find('.cell-output').exists()).toBe(false)
      expect(executeCalls()).toEqual([])
    })

    it('has no Run all when the flow has no nodes', async () => {
      pyodideMock.runPythonWithResult.mockResolvedValue({
        cells: [cell('imports', [], 'import flowfile as ff', { kind: 'imports' })],
        warnings: [],
        var_by_node: {}
      })

      const wrapper = await mountPane()

      expect(wrapper.find('[data-action="run-all"]').exists()).toBe(false)
    })

    it("shows a cell's rows under it without selecting the cell", async () => {
      const { flow } = runnableFlow()
      const wrapper = await mountPane()

      await wrapper.find('[data-cell-id="cell-1"] [data-action="run"]').trigger('click')
      await vi.waitFor(() => expect(wrapper.find('[data-cell-id="cell-1"] table').exists()).toBe(true))

      const output = wrapper.find('[data-cell-id="cell-1"] .cell-output')
      expect(output.attributes('data-output-state')).toBe('rows')
      expect(output.findAll('th').map(th => th.text())).toEqual(['a', 'b'])
      expect(output.findAll('tbody tr')).toHaveLength(2)
      expect(output.find('.output-note').text()).toBe('2 rows')
      expect(wrapper.find('[data-cell-id="cell-2"] .cell-output').exists()).toBe(false)
      expect(flow.selectedNodeId).toBeNull()

      await output.trigger('click')
      expect(flow.selectedNodeId).toBeNull()
    })

    it("shows a failed node's error under its cell", async () => {
      twoNodeFlow()
      const wrapper = await mountPane()

      await wrapper.find('[data-cell-id="cell-1"] [data-action="run"]').trigger('click')
      await vi.waitFor(() => expect(wrapper.find('.output-error').exists()).toBe(true))

      expect(wrapper.find('[data-cell-id="cell-1"] .output-error').text()).toBe('No data entered')
    })

    it('Run all fills every node cell and leaves the imports cell alone', async () => {
      runnableFlow()
      const wrapper = await mountPane()

      await wrapper.find('[data-action="run-all"]').trigger('click')
      await vi.waitFor(() => expect(wrapper.findAll('.cell-output--rows')).toHaveLength(2))

      expect(wrapper.find('[data-cell-id="imports"] .cell-output').exists()).toBe(false)
      expect(wrapper.find('[data-action="run-all"]').attributes('disabled')).toBeUndefined()
    })

    it('says how many rows there are beyond the ones shown, and names the columns of an empty result', async () => {
      const { cells } = runnableFlow()
      const wide = Array.from({ length: 100 }, (_, index) => [index, 'x'])
      let preview: unknown = { columns: ['a', 'b'], data: wide, total_rows: 12345 }
      pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
        if (source.includes('render_notebook(')) return { cells, warnings: [], var_by_node: {} }
        if (source.includes('_lazyframes.keys()')) return []
        if (source.includes('fetch_preview(')) return { success: true, data: preview }
        return { success: true }
      })
      const wrapper = await mountPane()

      await wrapper.find('[data-cell-id="cell-1"] [data-action="run"]').trigger('click')
      await vi.waitFor(() =>
        expect(wrapper.find('[data-cell-id="cell-1"] .output-note').text()).toBe('Showing 100 of 12,345 rows')
      )

      preview = { columns: ['a', 'b'], data: [], total_rows: 0 }
      await wrapper.find('[data-cell-id="cell-2"] [data-action="run"]').trigger('click')
      await vi.waitFor(() =>
        expect(wrapper.find('[data-cell-id="cell-2"] .output-note').text()).toBe('No rows. Columns: a, b')
      )
      expect(wrapper.find('[data-cell-id="cell-2"] table').exists()).toBe(false)
    })
  })

  describe('editing', () => {
    const EDITED = 'source_1 = ff.from_raw_data({"columns": [], "data": []})'

    /** Type `code` into the editor of the cell at `index`. */
    async function type(wrapper: ReturnType<typeof mount>, index: number, code: string) {
      wrapper.findAllComponents(Codemirror)[index].vm.$emit('update:modelValue', code)
      await vi.advanceTimersByTimeAsync(0)
    }

    /** Answer a sync with `answer`; everything else as the two-node flow does. */
    function syncAnswers(answer: unknown) {
      const { cells } = twoNodeFlow()
      pyodideMock.runPythonWithResult.mockImplementation(async (source: string) => {
        if (source === SYNC_SOURCE) return answer
        if (source.includes('render_notebook(')) return { cells, warnings: [], var_by_node: {} }
        return { success: true }
      })
    }

    it('marks a changed cell, counts it on Push and drops the change on Revert', async () => {
      twoNodeFlow()
      const wrapper = await mountPane()
      const push = wrapper.find('[data-action="push"]')
      expect(push.text()).toBe('Push')
      expect(push.attributes('disabled')).toBeDefined()
      expect(wrapper.find('[data-sync-state]').exists()).toBe(false)

      await type(wrapper, 1, EDITED)

      const changed = wrapper.find('[data-cell-id="cell-1"]')
      expect(changed.find('[data-sync-state]').text()).toBe('Edited')
      expect(changed.find('.cm-stub').text()).toBe(EDITED)
      expect(push.text()).toBe('Push (1)')
      expect(push.attributes('disabled')).toBeUndefined()

      await changed.find('[data-action="revert"]').trigger('click')

      expect(changed.find('[data-sync-state]').exists()).toBe(false)
      expect(changed.find('[data-action="revert"]').exists()).toBe(false)
      expect(changed.find('.cm-stub').text()).toBe('source_1 = ff.from_raw_data({})')
      expect(push.attributes('disabled')).toBeDefined()
    })

    it('copies what the cell says now', async () => {
      const writeText = vi.fn().mockResolvedValue(undefined)
      Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
      twoNodeFlow()
      const wrapper = await mountPane()
      await type(wrapper, 1, EDITED)

      await wrapper.find('[data-cell-id="cell-1"] [data-action="copy"]').trigger('click')

      expect(writeText).toHaveBeenCalledWith(EDITED)
    })

    it('Push lands the change and marks the cell synced', async () => {
      const raw = { columns: [{ name: 'a', data_type: 'Int64' }], data: [[1]] }
      syncAnswers({ ok: true, nodes: { '1': { settings: { raw_data_format: raw } } }, inputs: {}, warnings: [] })
      const flow = useFlowStore()
      const wrapper = await mountPane()
      await type(wrapper, 1, EDITED)

      await wrapper.find('[data-action="push"]').trigger('click')
      await vi.waitFor(() => expect(wrapper.find('[data-cell-id="cell-1"] [data-sync-state]').text()).toBe('Synced'))

      expect((flow.getNode(1)!.settings as any).raw_data_format).toEqual(raw)
      expect(wrapper.find('[data-action="push"]').text()).toBe('Push')
      expect(wrapper.find('.cell-error').exists()).toBe(false)
    })

    it('shows a refused push on its cell, with the line', async () => {
      syncAnswers({ ok: false, cell_id: 'cell-1', line: 3, kind: 'refused', message: 'This adds a step' })
      const wrapper = await mountPane()
      await type(wrapper, 1, EDITED)

      await wrapper.find('[data-action="push"]').trigger('click')
      await vi.waitFor(() => expect(wrapper.find('.cell-error').exists()).toBe(true))

      const failed = wrapper.find('[data-cell-id="cell-1"]')
      expect(failed.find('.cell-error').text()).toBe('Line 3: This adds a step')
      expect(failed.find('[data-sync-state]').text()).toBe('Sync failed')
      expect(failed.find('.cm-stub').text()).toBe(EDITED)

      await type(wrapper, 1, `${EDITED} `)
      expect(failed.find('.cell-error').exists()).toBe(false)
      expect(failed.find('[data-sync-state]').text()).toBe('Edited')
    })

    it('shows a notice until it is dismissed', async () => {
      syncAnswers({ ok: true, nodes: {}, inputs: {}, warnings: ['`Top` cannot be kept as a name'] })
      const wrapper = await mountPane()
      await type(wrapper, 1, EDITED)

      await wrapper.find('[data-action="push"]').trigger('click')
      await vi.waitFor(() => expect(wrapper.find('.notebook-note--notice').exists()).toBe(true))
      expect(wrapper.find('.notebook-note--notice span').text()).toBe('`Top` cannot be kept as a name')

      await wrapper.find('[data-action="dismiss-notice"]').trigger('click')
      expect(wrapper.find('.notebook-note--notice').exists()).toBe(false)
    })
  })
})
