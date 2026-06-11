/**
 * Flow Store Unit Tests
 * Tests for flow state management, serialization, and execution ordering
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

const pyodideMock = vi.hoisted(() => ({
  isReady: false,
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

const parquetBridgeMock = vi.hoisted(() => ({
  parquetToIpcStream: vi.fn(),
  ipcStreamToParquet: vi.fn()
}))

vi.mock('../../src/utils/parquet-bridge', () => parquetBridgeMock)

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
    pruneRuns: vi.fn().mockResolvedValue(undefined)
  }
}))

import { useFlowStore } from '../../src/stores/flow-store'
import { fileStorage } from '../../src/stores/file-storage'
import type { FlowfileData } from '../../src/types'

describe('Flow Store', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    sessionStorage.clear()
    vi.clearAllMocks()
  })

  describe('Node Management', () => {
    it('should add a node with generated ID', () => {
      const store = useFlowStore()

      const id = store.addNode('read_csv', 100, 200)

      expect(id).toBe(1)
      expect(store.nodes.size).toBe(1)

      const node = store.getNode(id)
      expect(node).toBeDefined()
      expect(node?.type).toBe('read_csv')
      expect(node?.x).toBe(100)
      expect(node?.y).toBe(200)
    })

    it('should generate unique IDs for multiple nodes', () => {
      const store = useFlowStore()

      const id1 = store.addNode('read_csv', 0, 0)
      const id2 = store.addNode('filter', 0, 0)
      const id3 = store.addNode('select', 0, 0)

      expect(id1).toBe(1)
      expect(id2).toBe(2)
      expect(id3).toBe(3)
    })

    it('should create node with default settings based on type', () => {
      const store = useFlowStore()

      store.addNode('filter', 0, 0)
      const node = store.getNode(1)

      expect(node?.settings).toBeDefined()
      expect((node?.settings as any).filter_input).toBeDefined()
      expect((node?.settings as any).filter_input.mode).toBe('basic')
    })

    it('should update node properties', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.updateNode(1, { x: 500, y: 300 })

      const node = store.getNode(1)
      expect(node?.x).toBe(500)
      expect(node?.y).toBe(300)
    })

    it('should update node settings', () => {
      const store = useFlowStore()

      store.addNode('filter', 0, 0)
      store.updateNodeSettings(1, {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: 'My filter',
        filter_input: {
          mode: 'basic',
          basic_filter: {
            field: 'name',
            operator: 'equals',
            value: 'test',
            value2: ''
          },
          advanced_filter: ''
        }
      })

      const node = store.getNode(1)
      expect((node?.settings as any).description).toBe('My filter')
      expect((node?.settings as any).filter_input.basic_filter.field).toBe('name')
    })

    it('should remove node and clean up edges', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)
      store.addEdge({
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })

      expect(store.edges.length).toBe(1)

      store.removeNode(1)

      expect(store.nodes.size).toBe(1)
      expect(store.edges.length).toBe(0)
    })

    it('should remove node from other nodes input lists', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)
      store.addEdge({
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })

      store.removeNode(1)

      const filterNode = store.getNode(2)
      expect(filterNode?.inputIds).not.toContain(1)
      expect(filterNode?.leftInputId).toBeUndefined()
    })
  })

  describe('Edge Management', () => {
    it('should add an edge', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)
      store.addEdge({
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })

      expect(store.edges.length).toBe(1)
      expect(store.edges[0].source).toBe('1')
      expect(store.edges[0].target).toBe('2')
    })

    it('should update target node inputIds when edge added', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)
      store.addEdge({
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })

      const filterNode = store.getNode(2)
      expect(filterNode?.inputIds).toContain(1)
      expect(filterNode?.leftInputId).toBe(1)
    })

    it('should not add duplicate edges', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)

      const edge = {
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      }

      store.addEdge(edge)
      store.addEdge(edge)

      expect(store.edges.length).toBe(1)
    })

    it('should set rightInputId for input-1 handle', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('read_csv', 0, 100)
      store.addNode('join', 100, 50)

      store.addEdge({
        id: 'e1-3',
        source: '1',
        target: '3',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })
      store.addEdge({
        id: 'e2-3',
        source: '2',
        target: '3',
        sourceHandle: 'output-0',
        targetHandle: 'input-1'
      })

      const joinNode = store.getNode(3)
      expect(joinNode?.leftInputId).toBe(1)
      expect(joinNode?.rightInputId).toBe(2)
    })

    it('should remove edge', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)
      store.addEdge({
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })

      store.removeEdge('e1-2')

      expect(store.edges.length).toBe(0)
    })

    it('should update target node when edge removed', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)
      store.addEdge({
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })

      store.removeEdge('e1-2')

      const filterNode = store.getNode(2)
      expect(filterNode?.inputIds).not.toContain(1)
      expect(filterNode?.leftInputId).toBeUndefined()
    })
  })

  describe('Node Selection', () => {
    it('should select a node', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.selectNode(1)

      expect(store.selectedNodeId).toBe(1)
    })

    it('should deselect when selecting null', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.selectNode(1)
      store.selectNode(null)

      expect(store.selectedNodeId).toBeNull()
    })
  })

  describe('Flow Export/Import', () => {
    it('should export flow to FlowfileData format', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 100, 200)
      store.addNode('filter', 300, 200)
      store.addEdge({
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })

      const exported = store.exportToFlowfile('Test Flow')

      expect(exported.flowfile_name).toBe('Test Flow')
      expect(exported.flowfile_version).toBe('1.0.0')
      expect(exported.nodes.length).toBe(2)

      const node1 = exported.nodes.find(n => n.id === 1)
      expect(node1?.type).toBe('read_csv')
      expect(node1?.x_position).toBe(100)
      expect(node1?.y_position).toBe(200)
      expect(node1?.is_start_node).toBe(true)

      const node2 = exported.nodes.find(n => n.id === 2)
      expect(node2?.type).toBe('filter')
      expect(node2?.is_start_node).toBe(false)
      expect(node2?.input_ids).toContain(1)
    })

    it('should import FlowfileData format', () => {
      const store = useFlowStore()

      const flowfileData: FlowfileData = {
        flowfile_version: '1.0.0',
        flowfile_id: 123,
        flowfile_name: 'Imported Flow',
        flowfile_settings: {
          description: '',
          execution_mode: 'Development',
          execution_location: 'local',
          auto_save: true,
          show_detailed_progress: false
        },
        nodes: [
          {
            id: 10,
            type: 'read_csv',
            is_start_node: true,
            description: '',
            x_position: 50,
            y_position: 100,
            input_ids: [],
            outputs: [20],
            setting_input: {}
          },
          {
            id: 20,
            type: 'filter',
            is_start_node: false,
            description: '',
            x_position: 200,
            y_position: 100,
            input_ids: [10],
            outputs: [],
            setting_input: {}
          }
        ]
      }

      const success = store.importFromFlowfile(flowfileData)

      expect(success).toBe(true)
      expect(store.nodes.size).toBe(2)

      const node10 = store.getNode(10)
      // Note: 'read_csv' is migrated to 'read' during import for backward compatibility
      expect(node10?.type).toBe('read')
      expect(node10?.x).toBe(50)

      const node20 = store.getNode(20)
      expect(node20?.type).toBe('filter')
      expect(node20?.inputIds).toContain(10)
    })

    it('should derive edges from node relationships on import', () => {
      const store = useFlowStore()

      const flowfileData: FlowfileData = {
        flowfile_version: '1.0.0',
        flowfile_id: 123,
        flowfile_name: 'Test',
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
            type: 'read_csv',
            is_start_node: true,
            description: '',
            x_position: 0,
            y_position: 0,
            input_ids: [],
            outputs: [2],
            setting_input: {}
          },
          {
            id: 2,
            type: 'filter',
            is_start_node: false,
            description: '',
            x_position: 100,
            y_position: 0,
            left_input_id: 1,
            input_ids: [1],
            outputs: [],
            setting_input: {}
          }
        ]
      }

      store.importFromFlowfile(flowfileData)

      expect(store.edges.length).toBe(1)
      expect(store.edges[0].source).toBe('1')
      expect(store.edges[0].target).toBe('2')
    })

    it('should import join nodes with left and right inputs', () => {
      const store = useFlowStore()

      const flowfileData: FlowfileData = {
        flowfile_version: '1.0.0',
        flowfile_id: 123,
        flowfile_name: 'Join Test',
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
            type: 'read_csv',
            is_start_node: true,
            description: '',
            x_position: 0,
            y_position: 0,
            input_ids: [],
            outputs: [3],
            setting_input: {}
          },
          {
            id: 2,
            type: 'read_csv',
            is_start_node: true,
            description: '',
            x_position: 0,
            y_position: 100,
            input_ids: [],
            outputs: [3],
            setting_input: {}
          },
          {
            id: 3,
            type: 'join',
            is_start_node: false,
            description: '',
            x_position: 200,
            y_position: 50,
            left_input_id: 1,
            right_input_id: 2,
            input_ids: [],
            outputs: [],
            setting_input: {}
          }
        ]
      }

      store.importFromFlowfile(flowfileData)

      const joinNode = store.getNode(3)
      expect(joinNode?.leftInputId).toBe(1)
      expect(joinNode?.rightInputId).toBe(2)

      expect(store.edges.length).toBe(2)
    })
  })

  describe('Clear Flow', () => {
    it('should clear all flow state', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)
      store.addEdge({
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })
      store.selectNode(1)

      store.clearFlow()

      expect(store.nodes.size).toBe(0)
      expect(store.edges.length).toBe(0)
      expect(store.selectedNodeId).toBeNull()
      expect(store.nodeResults.size).toBe(0)
      expect(store.fileContents.size).toBe(0)
    })
  })

  describe('File Content', () => {
    it('should set file content for a node', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.setFileContent(1, 'id,name\n1,Alice\n2,Bob')

      // Strings normalize into tagged FileContent wrappers
      expect(store.fileContents.get(1)).toEqual({ kind: 'text', data: 'id,name\n1,Alice\n2,Bob' })
      expect(store.getTextContent(1)).toBe('id,name\n1,Alice\n2,Bob')
    })
  })

  describe('Node clipboard (copy/paste)', () => {
    beforeEach(() => {
      localStorage.clear()
    })

    it('copies settings AND file content to the pasted node', () => {
      const store = useFlowStore()
      const id = store.addNode('read', 10, 20)
      const settings = store.getNode(id)!.settings as any
      settings.received_file.name = 'sales-data.csv'
      settings.received_file.table_settings.delimiter = ';'
      store.setFileContent(id, 'a;b\n1;2\n')

      expect(store.copyNode(id)).toBe(true)
      const newId = store.pasteNode(300, 400)

      expect(newId).not.toBeNull()
      expect(newId).not.toBe(id)
      const pasted = store.getNode(newId!)!
      const pastedSettings = pasted.settings as any
      expect(pastedSettings.received_file.name).toBe('sales-data.csv')
      expect(pastedSettings.received_file.table_settings.delimiter).toBe(';')
      expect(pastedSettings.node_id).toBe(newId)
      expect(pastedSettings.pos_x).toBe(300)
      // The loaded data is re-keyed to the new node id so the paste can run.
      expect(store.getTextContent(newId!)).toBe('a;b\n1;2\n')
    })

    it('pastes from localStorage when there is no in-memory clipboard (reload)', () => {
      localStorage.setItem(
        'flowfile-wasm-node-clipboard',
        JSON.stringify({
          type: 'read',
          settings: { file_name: 'x.csv' },
          description: '',
          fileContent: 'a,b\n1,2\n',
          copiedAt: 123
        })
      )
      const store = useFlowStore()

      const newId = store.pasteNode(0, 0)

      expect(newId).not.toBeNull()
      expect(store.getTextContent(newId!)).toBe('a,b\n1,2\n')
    })

    it('keeps large file content on the in-memory clipboard when too big for localStorage', () => {
      const store = useFlowStore()
      const id = store.addNode('read', 0, 0)
      store.setFileContent(id, 'big-content')
      vi.mocked(fileStorage.shouldUseIndexedDB).mockReturnValue(true)
      try {
        store.copyNode(id)

        const persisted = JSON.parse(localStorage.getItem('flowfile-wasm-node-clipboard')!)
        expect(persisted.fileContent).toBeUndefined()

        const newId = store.pasteNode(5, 5)
        expect(store.getTextContent(newId!)).toBe('big-content')
      } finally {
        vi.mocked(fileStorage.shouldUseIndexedDB).mockReturnValue(false)
      }
    })

    it('still pastes a settings-only copy (no file content)', () => {
      const store = useFlowStore()
      const id = store.addNode('filter', 0, 0)

      store.copyNode(id)
      const newId = store.pasteNode(50, 60)

      expect(newId).not.toBeNull()
      expect(store.getNode(newId!)!.type).toBe('filter')
      expect(store.getFileContent(newId!)).toBeUndefined()
    })

    it('never persists binary content to localStorage but pastes it in-memory', () => {
      const store = useFlowStore()
      const id = store.addNode('read', 0, 0)
      const bytes = new Uint8Array([1, 2, 3])
      store.setFileContent(id, { kind: 'binary', data: bytes, format: 'parquet' })

      store.copyNode(id)

      const persisted = JSON.parse(localStorage.getItem('flowfile-wasm-node-clipboard')!)
      expect(persisted.fileContent).toBeUndefined()

      const newId = store.pasteNode(5, 5)
      const pasted = store.getFileContent(newId!)
      expect(pasted?.kind).toBe('binary')
      if (pasted?.kind === 'binary') {
        expect(Array.from(pasted.data)).toEqual([1, 2, 3])
      }
    })
  })

  describe('Binary file content routing', () => {
    it('routes binary to IndexedDB regardless of size and skips CSV inference', () => {
      const store = useFlowStore()
      const id = store.addNode('read', 0, 0)
      vi.mocked(fileStorage.setFileContent).mockClear()

      store.setFileContent(id, { kind: 'binary', data: new Uint8Array([9]), format: 'excel' })

      expect(fileStorage.setFileContent).toHaveBeenCalledWith(
        id,
        expect.objectContaining({ kind: 'binary' })
      )
      expect(store.getTextContent(id)).toBeUndefined()
      // No CSV schema gets inferred from binary bytes
      expect(store.nodeResults.get(id)?.schema).toBeUndefined()
    })
  })

  describe('Excel read branch', () => {
    function excelNode(store: ReturnType<typeof useFlowStore>) {
      const id = store.addNode('read', 0, 0)
      const settings = store.getNode(id)!.settings as any
      settings.received_file.file_type = 'excel'
      settings.received_file.table_settings = { file_type: 'excel', sheet_name: null, has_headers: true }
      return id
    }

    it('installs openpyxl and executes via the _temp_bytes bridge', async () => {
      const store = useFlowStore()
      const id = excelNode(store)
      store.setFileContent(id, { kind: 'binary', data: new Uint8Array([0x50, 0x4b]), format: 'excel' })
      pyodideMock.ensurePyPackages.mockResolvedValue(undefined)
      pyodideMock.runPythonWithResult.mockResolvedValue({ success: true, schema: [] })

      const result = await store.executeNode(id)

      expect(result.success).toBe(true)
      expect(pyodideMock.ensurePyPackages).toHaveBeenCalledWith(['openpyxl==3.1.5'])
      expect(pyodideMock.setGlobal).toHaveBeenCalledWith('_temp_bytes', expect.any(Uint8Array))
      expect(pyodideMock.runPythonWithResult.mock.calls[0][0]).toContain('execute_read_excel')
      expect(pyodideMock.runPythonWithResult.mock.calls[0][0]).toContain('_temp_bytes.to_py()')
      expect(pyodideMock.deleteGlobal).toHaveBeenCalledWith('_temp_bytes')
    })

    it('fails with the install error when openpyxl cannot be downloaded', async () => {
      const store = useFlowStore()
      const id = excelNode(store)
      store.setFileContent(id, { kind: 'binary', data: new Uint8Array([1]), format: 'excel' })
      pyodideMock.ensurePyPackages.mockRejectedValueOnce(new Error('Could not download openpyxl from PyPI'))

      const result = await store.executeNode(id)

      expect(result.success).toBe(false)
      expect(result.error).toContain('PyPI')
    })

    it('fails an excel-configured node holding text content', async () => {
      const store = useFlowStore()
      const id = excelNode(store)
      store.setFileContent(id, 'a,b\n1,2\n')

      const result = await store.executeNode(id)

      expect(result.success).toBe(false)
      expect(result.error).toContain('re-pick')
    })

    it('fails a csv-configured node holding binary content', async () => {
      const store = useFlowStore()
      const id = store.addNode('read', 0, 0)
      store.setFileContent(id, { kind: 'binary', data: new Uint8Array([1]), format: 'excel' })

      const result = await store.executeNode(id)

      expect(result.success).toBe(false)
      expect(result.error).toContain('binary')
    })
  })

  describe('Parquet read branch', () => {
    function parquetNode(store: ReturnType<typeof useFlowStore>) {
      const id = store.addNode('read', 0, 0)
      const settings = store.getNode(id)!.settings as any
      settings.received_file.file_type = 'parquet'
      settings.received_file.table_settings = { file_type: 'parquet' }
      return id
    }

    it('decodes parquet to IPC in JS and executes via execute_read_ipc', async () => {
      const store = useFlowStore()
      const id = parquetNode(store)
      const parquetBytes = new Uint8Array([0x50, 0x41, 0x52, 0x31])
      store.setFileContent(id, { kind: 'binary', data: parquetBytes, format: 'parquet' })
      const ipcBytes = new Uint8Array([7, 7, 7])
      parquetBridgeMock.parquetToIpcStream.mockResolvedValue(ipcBytes)
      pyodideMock.runPythonWithResult.mockResolvedValue({ success: true, schema: [] })

      const result = await store.executeNode(id)

      expect(result.success).toBe(true)
      expect(parquetBridgeMock.parquetToIpcStream).toHaveBeenCalledWith(parquetBytes)
      expect(pyodideMock.setGlobal).toHaveBeenCalledWith('_temp_bytes', ipcBytes)
      expect(pyodideMock.runPythonWithResult.mock.calls[0][0]).toContain('execute_read_ipc')
      expect(pyodideMock.deleteGlobal).toHaveBeenCalledWith('_temp_bytes')
    })

    it('surfaces the CDN error when parquet-wasm cannot load', async () => {
      const store = useFlowStore()
      const id = parquetNode(store)
      store.setFileContent(id, { kind: 'binary', data: new Uint8Array([1]), format: 'parquet' })
      parquetBridgeMock.parquetToIpcStream.mockRejectedValueOnce(
        new Error('Could not load the Parquet engine from cdn.jsdelivr.net')
      )

      const result = await store.executeNode(id)

      expect(result.success).toBe(false)
      expect(result.error).toContain('cdn.jsdelivr.net')
    })
  })

  describe('External data binary branch (Arrow host contract)', () => {
    function externalNode(store: ReturnType<typeof useFlowStore>, datasetName: string) {
      const id = store.addNode('external_data', 0, 0)
      const settings = store.getNode(id)!.settings as any
      settings.dataset_name = datasetName
      return id
    }

    it('executes host-provided Arrow IPC bytes via execute_read_ipc', async () => {
      const store = useFlowStore()
      const ipcBytes = new Uint8Array([1, 2, 3, 4])
      const id = externalNode(store, 'events')
      store.setExternalDatasets({ events: { kind: 'binary', data: ipcBytes, format: 'arrow-ipc' } })
      pyodideMock.runPythonWithResult.mockResolvedValue({ success: true, schema: [] })

      const result = await store.executeNode(id)

      expect(result.success).toBe(true)
      expect(pyodideMock.setGlobal).toHaveBeenCalledWith('_temp_bytes', ipcBytes)
      expect(pyodideMock.runPythonWithResult.mock.calls[0][0]).toContain('execute_read_ipc')
    })

    it('decodes host-provided parquet bytes through the bridge first', async () => {
      const store = useFlowStore()
      const parquetBytes = new Uint8Array([0x50, 0x41, 0x52, 0x31])
      const ipcBytes = new Uint8Array([9])
      const id = externalNode(store, 'sales')
      store.setExternalDatasets({ sales: { kind: 'binary', data: parquetBytes, format: 'parquet' } })
      parquetBridgeMock.parquetToIpcStream.mockResolvedValue(ipcBytes)
      pyodideMock.runPythonWithResult.mockResolvedValue({ success: true, schema: [] })

      const result = await store.executeNode(id)

      expect(result.success).toBe(true)
      expect(parquetBridgeMock.parquetToIpcStream).toHaveBeenCalledWith(parquetBytes)
      expect(pyodideMock.setGlobal).toHaveBeenCalledWith('_temp_bytes', ipcBytes)
    })

    it('still runs text datasets through execute_manual_input', async () => {
      const store = useFlowStore()
      const id = externalNode(store, 'plain')
      store.setExternalDatasets({ plain: 'a,b\n1,2\n' })
      pyodideMock.runPythonWithResult.mockResolvedValue({ success: true, schema: [] })

      const result = await store.executeNode(id)

      expect(result.success).toBe(true)
      expect(pyodideMock.setGlobal).toHaveBeenCalledWith('_temp_content', 'a,b\n1,2\n')
      expect(pyodideMock.runPythonWithResult.mock.calls[0][0]).toContain('execute_manual_input')
    })

    it('exposes node results as Arrow bytes via getNodeResultArrow', async () => {
      const store = useFlowStore()
      pyodideMock.isReady = true
      try {
        const arrow = new Uint8Array([5, 5])
        pyodideMock.runPythonGetBytes.mockResolvedValue(arrow)

        const result = await store.getNodeResultArrow(3)

        expect(result).toBe(arrow)
        expect(pyodideMock.runPythonGetBytes).toHaveBeenCalledWith('get_node_arrow(3)')
      } finally {
        pyodideMock.isReady = false
      }
    })
  })

  describe('Parquet output branch', () => {
    it('converts staged IPC to parquet bytes before storing the download', async () => {
      const store = useFlowStore()
      const readId = store.addNode('read', 0, 0)
      const outId = store.addNode('output', 100, 0)
      store.addEdge({
        id: `e${readId}-${outId}`,
        source: String(readId),
        target: String(outId),
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })
      const settings = store.getNode(outId)!.settings as any
      settings.output_settings.file_type = 'parquet'
      settings.output_settings.name = 'out.parquet'
      settings.output_settings.table_settings = { file_type: 'parquet' }

      pyodideMock.runPythonWithResult.mockResolvedValue({
        success: true,
        schema: [],
        download: {
          content: '',
          content_kind: 'binary',
          transport: 'arrow-ipc',
          file_name: 'out.parquet',
          file_type: 'parquet',
          mime_type: 'application/vnd.apache.parquet',
          row_count: 3
        }
      })
      const ipcBytes = new Uint8Array([1, 2, 3])
      const parquetBytes = new Uint8Array([0x50, 0x41, 0x52, 0x31, 9])
      pyodideMock.runPythonGetBytes.mockResolvedValue(ipcBytes)
      parquetBridgeMock.ipcStreamToParquet.mockResolvedValue(parquetBytes)

      const result = await store.executeNode(outId)

      expect(result.success).toBe(true)
      expect(pyodideMock.runPythonGetBytes).toHaveBeenCalledWith(`take_output_binary(${outId})`)
      expect(parquetBridgeMock.ipcStreamToParquet).toHaveBeenCalledWith(ipcBytes)
      expect(fileStorage.setDownloadContent).toHaveBeenCalledWith(
        outId,
        parquetBytes,
        'out.parquet',
        'parquet',
        'application/vnd.apache.parquet',
        3
      )
    })
  })

  describe('Excel output branch', () => {
    it('pulls staged bytes via take_output_binary and stores a binary download', async () => {
      const store = useFlowStore()
      const readId = store.addNode('read', 0, 0)
      const outId = store.addNode('output', 100, 0)
      store.addEdge({
        id: `e${readId}-${outId}`,
        source: String(readId),
        target: String(outId),
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })
      const settings = store.getNode(outId)!.settings as any
      settings.output_settings.file_type = 'excel'
      settings.output_settings.name = 'out.xlsx'
      settings.output_settings.table_settings = { file_type: 'excel', sheet_name: 'Data' }

      pyodideMock.ensurePyPackages.mockResolvedValue(undefined)
      pyodideMock.runPythonWithResult.mockResolvedValue({
        success: true,
        schema: [],
        download: {
          content: '',
          content_kind: 'binary',
          file_name: 'out.xlsx',
          file_type: 'excel',
          mime_type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          row_count: 2
        }
      })
      const xlsxBytes = new Uint8Array([0x50, 0x4b, 9, 9])
      pyodideMock.runPythonGetBytes.mockResolvedValue(xlsxBytes)

      const result = await store.executeNode(outId)

      expect(result.success).toBe(true)
      expect(pyodideMock.ensurePyPackages).toHaveBeenCalledWith(['XlsxWriter==3.2.0'])
      expect(pyodideMock.runPythonGetBytes).toHaveBeenCalledWith(`take_output_binary(${outId})`)
      expect(fileStorage.setDownloadContent).toHaveBeenCalledWith(
        outId,
        xlsxBytes,
        'out.xlsx',
        'excel',
        expect.stringContaining('spreadsheetml'),
        2
      )
    })

    it('does not fire host output callbacks for binary downloads', async () => {
      const store = useFlowStore()
      const readId = store.addNode('read', 0, 0)
      const outId = store.addNode('output', 100, 0)
      store.addEdge({
        id: `e${readId}-${outId}`,
        source: String(readId),
        target: String(outId),
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })
      const settings = store.getNode(outId)!.settings as any
      settings.output_settings.file_type = 'excel'

      const onOutput = vi.fn()
      store.onOutput(onOutput)
      pyodideMock.ensurePyPackages.mockResolvedValue(undefined)
      pyodideMock.runPythonWithResult.mockResolvedValue({
        success: true,
        schema: [],
        download: {
          content: '',
          content_kind: 'binary',
          file_name: 'out.xlsx',
          file_type: 'excel',
          mime_type: 'application/x',
          row_count: 1
        }
      })
      pyodideMock.runPythonGetBytes.mockResolvedValue(new Uint8Array([1]))

      await store.executeNode(outId)

      expect(onOutput).not.toHaveBeenCalled()
    })
  })

  describe('Execution failure surfacing', () => {
    it('records a failed result when a read node runs without file content', async () => {
      const store = useFlowStore()
      const id = store.addNode('read', 0, 0)

      const result = await store.executeNode(id)

      expect(result.success).toBe(false)
      expect(result.error).toBe('No file loaded')
      // The failure must land in nodeResults so the node shows the error
      // instead of looking never-executed.
      expect(store.nodeResults.get(id)?.success).toBe(false)
      expect(store.nodeResults.get(id)?.error).toBe('No file loaded')
    })

    it('records a failed result when a transform node has no input', async () => {
      const store = useFlowStore()
      // Let the store's init-time schema propagation settle first — its TS
      // fallback intentionally clears no-input errors while editing.
      await new Promise(resolve => setTimeout(resolve, 0))
      const id = store.addNode('filter', 0, 0)

      const result = await store.executeNode(id)

      expect(result.success).toBe(false)
      expect(result.error).toBe('No input connected')
      expect(store.nodeResults.get(id)?.error).toBe('No input connected')
    })
  })

  describe('Schema Access', () => {
    it('should get input schema from upstream node', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)
      store.addEdge({
        id: 'e1-2',
        source: '1',
        target: '2',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })

      store.nodeResults.set(1, {
        success: true,
        schema: [
          { name: 'id', data_type: 'Int64' },
          { name: 'name', data_type: 'String' }
        ]
      })

      const inputSchema = store.getNodeInputSchema(2)

      expect(inputSchema).toHaveLength(2)
      expect(inputSchema[0].name).toBe('id')
      expect(inputSchema[1].name).toBe('name')
    })

    it('should return empty array when no input', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      const inputSchema = store.getNodeInputSchema(1)

      expect(inputSchema).toEqual([])
    })

    it('should get left input schema for join node', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('read_csv', 0, 100)
      store.addNode('join', 100, 50)

      store.addEdge({
        id: 'e1-3',
        source: '1',
        target: '3',
        sourceHandle: 'output-0',
        targetHandle: 'input-0'
      })
      store.addEdge({
        id: 'e2-3',
        source: '2',
        target: '3',
        sourceHandle: 'output-0',
        targetHandle: 'input-1'
      })

      store.nodeResults.set(1, {
        success: true,
        schema: [{ name: 'left_col', data_type: 'Int64' }]
      })
      store.nodeResults.set(2, {
        success: true,
        schema: [{ name: 'right_col', data_type: 'String' }]
      })

      const leftSchema = store.getLeftInputSchema(3)
      const rightSchema = store.getRightInputSchema(3)

      expect(leftSchema).toHaveLength(1)
      expect(leftSchema[0].name).toBe('left_col')

      expect(rightSchema).toHaveLength(1)
      expect(rightSchema[0].name).toBe('right_col')
    })
  })

  describe('nodeList computed', () => {
    it('should return array of all nodes', () => {
      const store = useFlowStore()

      store.addNode('read_csv', 0, 0)
      store.addNode('filter', 100, 0)
      store.addNode('select', 200, 0)

      expect(store.nodeList).toHaveLength(3)
      expect(store.nodeList.map(n => n.type)).toContain('read_csv')
      expect(store.nodeList.map(n => n.type)).toContain('filter')
      expect(store.nodeList.map(n => n.type)).toContain('select')
    })
  })

  describe('Save to catalog vs Export', () => {
    it('saveToLibrary mints a stable id and does NOT download a file', async () => {
      const store = useFlowStore()
      store.addNode('filter', 0, 0)
      const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})

      expect(store.currentFlowId).toBe(null)
      const res = await store.saveToLibrary('My Flow')

      expect(res.id).toBeTruthy()
      expect(store.currentFlowId).toBe(res.id)
      expect(store.currentFlowName).toBe('My Flow')
      expect(clickSpy).not.toHaveBeenCalled()

      // Re-saving keeps the same id (non-lossy).
      const again = await store.saveToLibrary('My Flow Renamed')
      expect(again.id).toBe(res.id)

      clickSpy.mockRestore()
    })

    it('exportFlowfile downloads a file', async () => {
      const store = useFlowStore()
      store.addNode('filter', 0, 0)
      const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
      ;(URL as any).createObjectURL = vi.fn(() => 'blob:mock')
      ;(URL as any).revokeObjectURL = vi.fn()

      await store.exportFlowfile('My Flow')
      expect(clickSpy).toHaveBeenCalled()

      clickSpy.mockRestore()
    })
  })
})
