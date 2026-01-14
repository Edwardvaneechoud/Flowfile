/**
 * Flow Store Unit Tests
 * Tests for flow state management, serialization, and execution ordering
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

// Mock the pyodide store before importing flow store
vi.mock('../../src/stores/pyodide-store', () => ({
  usePyodideStore: () => ({
    isReady: false,
    runPython: vi.fn(),
    runPythonWithResult: vi.fn(),
    setGlobal: vi.fn(),
    deleteGlobal: vi.fn()
  })
}))

// Mock file storage
vi.mock('../../src/stores/file-storage', () => ({
  fileStorage: {
    setFileContent: vi.fn().mockResolvedValue(undefined),
    getFileContent: vi.fn().mockResolvedValue(null),
    deleteFileContent: vi.fn().mockResolvedValue(undefined),
    getDownloadContent: vi.fn().mockResolvedValue(null),
    setDownloadContent: vi.fn().mockResolvedValue(undefined),
    clearAll: vi.fn().mockResolvedValue(undefined),
    shouldUseIndexedDB: vi.fn().mockReturnValue(false)
  }
}))

import { useFlowStore } from '../../src/stores/flow-store'
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

      store.addNode('read_csv', 0, 0) // Left input
      store.addNode('read_csv', 0, 100) // Right input
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
      expect(node10?.type).toBe('read_csv')
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

      // Should have 2 edges (one for each input)
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

      expect(store.fileContents.get(1)).toBe('id,name\n1,Alice\n2,Bob')
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

      // Set schema on source node
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
})
