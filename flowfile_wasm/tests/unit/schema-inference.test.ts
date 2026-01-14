/**
 * Schema Inference Unit Tests
 * Tests for pure TypeScript schema inference functions
 */

import { describe, it, expect } from 'vitest'
import {
  inferOutputSchema,
  isSourceNode,
  getDownstreamNodeIds,
  inferSchemaFromCsv,
  inferSchemaFromRawData
} from '../../src/stores/schema-inference'
import type {
  ColumnSchema,
  NodeSelectSettings,
  NodeGroupBySettings,
  NodeJoinSettings,
  NodeUnpivotSettings,
  NodePivotSettings
} from '../../src/types'

describe('Schema Inference', () => {
  // Test fixtures
  const basicSchema: ColumnSchema[] = [
    { name: 'id', data_type: 'Int64' },
    { name: 'name', data_type: 'String' },
    { name: 'value', data_type: 'Float64' },
    { name: 'active', data_type: 'Boolean' }
  ]

  const numericSchema: ColumnSchema[] = [
    { name: 'id', data_type: 'Int64' },
    { name: 'amount', data_type: 'Float64' },
    { name: 'count', data_type: 'Int64' }
  ]

  describe('isSourceNode', () => {
    it('should identify read_csv as source node', () => {
      expect(isSourceNode('read_csv')).toBe(true)
    })

    it('should identify manual_input as source node', () => {
      expect(isSourceNode('manual_input')).toBe(true)
    })

    it('should not identify transform nodes as source', () => {
      expect(isSourceNode('filter')).toBe(false)
      expect(isSourceNode('select')).toBe(false)
      expect(isSourceNode('join')).toBe(false)
      expect(isSourceNode('group_by')).toBe(false)
    })
  })

  describe('getDownstreamNodeIds', () => {
    it('should return downstream node IDs from edges', () => {
      const edges = [
        { source: '1', target: '2' },
        { source: '1', target: '3' },
        { source: '2', target: '4' }
      ]

      expect(getDownstreamNodeIds(1, edges)).toEqual([2, 3])
      expect(getDownstreamNodeIds(2, edges)).toEqual([4])
      expect(getDownstreamNodeIds(3, edges)).toEqual([])
    })

    it('should return empty array for nodes with no downstream', () => {
      const edges = [{ source: '1', target: '2' }]
      expect(getDownstreamNodeIds(2, edges)).toEqual([])
    })
  })

  describe('inferOutputSchema - Pass-through Nodes', () => {
    it('should return input schema for filter node', () => {
      const result = inferOutputSchema('filter', basicSchema, {} as any)
      expect(result).toEqual(basicSchema)
    })

    it('should return input schema for sort node', () => {
      const result = inferOutputSchema('sort', basicSchema, {} as any)
      expect(result).toEqual(basicSchema)
    })

    it('should return input schema for unique node', () => {
      const result = inferOutputSchema('unique', basicSchema, {} as any)
      expect(result).toEqual(basicSchema)
    })

    it('should return input schema for head node', () => {
      const result = inferOutputSchema('head', basicSchema, {} as any)
      expect(result).toEqual(basicSchema)
    })

    it('should return input schema for sample node', () => {
      const result = inferOutputSchema('sample', basicSchema, {} as any)
      expect(result).toEqual(basicSchema)
    })

    it('should return input schema for preview node', () => {
      const result = inferOutputSchema('preview', basicSchema, {} as any)
      expect(result).toEqual(basicSchema)
    })
  })

  describe('inferOutputSchema - Source Nodes', () => {
    it('should return null for read_csv (schema comes from data)', () => {
      const result = inferOutputSchema('read_csv', null, {} as any)
      expect(result).toBeNull()
    })

    it('should return null for manual_input (schema comes from data)', () => {
      const result = inferOutputSchema('manual_input', null, {} as any)
      expect(result).toBeNull()
    })
  })

  describe('inferOutputSchema - Nodes that cannot infer', () => {
    it('should return null for polars_code node', () => {
      const result = inferOutputSchema('polars_code', basicSchema, {} as any)
      expect(result).toBeNull()
    })

    it('should return null for formula node', () => {
      const result = inferOutputSchema('formula', basicSchema, {} as any)
      expect(result).toBeNull()
    })

    it('should return null for pivot node', () => {
      const settings: NodePivotSettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        pivot_input: {
          index_columns: ['id'],
          pivot_column: 'category',
          value_col: 'amount',
          aggregations: ['sum']
        }
      }
      const result = inferOutputSchema('pivot', basicSchema, settings)
      expect(result).toBeNull()
    })
  })

  describe('inferOutputSchema - Select Node', () => {
    it('should filter and rename columns based on select_input', () => {
      const settings: NodeSelectSettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        select_input: [
          { old_name: 'id', new_name: 'user_id', data_type: 'Int64', keep: true, position: 0 },
          { old_name: 'name', new_name: 'full_name', data_type: 'String', keep: true, position: 1 },
          { old_name: 'value', new_name: 'value', data_type: 'Float64', keep: false, position: 2 }
        ]
      }

      const result = inferOutputSchema('select', basicSchema, settings)

      expect(result).toEqual([
        { name: 'user_id', data_type: 'Int64' },
        { name: 'full_name', data_type: 'String' }
      ])
    })

    it('should return input schema when select_input is empty', () => {
      const settings: NodeSelectSettings = {
        node_id: 1,
        is_setup: false,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        select_input: []
      }

      const result = inferOutputSchema('select', basicSchema, settings)
      expect(result).toEqual(basicSchema)
    })

    it('should respect column position ordering', () => {
      const settings: NodeSelectSettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        select_input: [
          { old_name: 'name', new_name: 'name', data_type: 'String', keep: true, position: 1 },
          { old_name: 'id', new_name: 'id', data_type: 'Int64', keep: true, position: 0 }
        ]
      }

      const result = inferOutputSchema('select', basicSchema, settings)

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'name', data_type: 'String' }
      ])
    })
  })

  describe('inferOutputSchema - Group By Node', () => {
    it('should return grouped and aggregated columns', () => {
      const settings: NodeGroupBySettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        groupby_input: {
          agg_cols: [
            { old_name: 'id', new_name: 'id', agg: 'groupby' },
            { old_name: 'amount', new_name: 'total_amount', agg: 'sum' },
            { old_name: 'count', new_name: 'avg_count', agg: 'mean' }
          ]
        }
      }

      const result = inferOutputSchema('group_by', numericSchema, settings)

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'total_amount', data_type: 'Float64' },
        { name: 'avg_count', data_type: 'Float64' }
      ])
    })

    it('should infer correct output types for aggregations', () => {
      const settings: NodeGroupBySettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        groupby_input: {
          agg_cols: [
            { old_name: 'id', new_name: 'id', agg: 'groupby' },
            { old_name: 'amount', new_name: 'count_records', agg: 'count' },
            { old_name: 'count', new_name: 'unique_values', agg: 'n_unique' },
            { old_name: 'name', new_name: 'first_name', agg: 'first' }
          ]
        }
      }

      const result = inferOutputSchema('group_by', basicSchema, settings)

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'count_records', data_type: 'Int64' },
        { name: 'unique_values', data_type: 'Int64' },
        { name: 'first_name', data_type: 'String' }
      ])
    })

    it('should return null when no agg_cols defined', () => {
      const settings: NodeGroupBySettings = {
        node_id: 1,
        is_setup: false,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        groupby_input: {
          agg_cols: []
        }
      }

      const result = inferOutputSchema('group_by', numericSchema, settings)
      expect(result).toBeNull()
    })
  })

  describe('inferOutputSchema - Join Node', () => {
    const leftSchema: ColumnSchema[] = [
      { name: 'id', data_type: 'Int64' },
      { name: 'name', data_type: 'String' }
    ]

    const rightSchema: ColumnSchema[] = [
      { name: 'id', data_type: 'Int64' },
      { name: 'value', data_type: 'Float64' }
    ]

    it('should merge schemas for inner join', () => {
      const settings: NodeJoinSettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        depending_on_ids: [1, 2],
        join_input: {
          how: 'inner',
          join_mapping: [{ left_col: 'id', right_col: 'id' }]
        }
      }

      const result = inferOutputSchema('join', leftSchema, settings, rightSchema)

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'name', data_type: 'String' },
        { name: 'value', data_type: 'Float64' }
      ])
    })

    it('should add suffixes for overlapping non-key columns', () => {
      const leftSchemaWithOverlap: ColumnSchema[] = [
        { name: 'id', data_type: 'Int64' },
        { name: 'value', data_type: 'String' }
      ]

      const settings: NodeJoinSettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        depending_on_ids: [1, 2],
        join_input: {
          how: 'inner',
          join_mapping: [{ left_col: 'id', right_col: 'id' }],
          left_suffix: '_left',
          right_suffix: '_right'
        }
      }

      const result = inferOutputSchema('join', leftSchemaWithOverlap, settings, rightSchema)

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'value_left', data_type: 'String' },
        { name: 'value_right', data_type: 'Float64' }
      ])
    })

    it('should return left schema only for semi join', () => {
      const settings: NodeJoinSettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        depending_on_ids: [1, 2],
        join_input: {
          how: 'semi',
          join_mapping: [{ left_col: 'id', right_col: 'id' }]
        }
      }

      const result = inferOutputSchema('join', leftSchema, settings, rightSchema)
      expect(result).toEqual(leftSchema)
    })

    it('should return left schema only for anti join', () => {
      const settings: NodeJoinSettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        depending_on_ids: [1, 2],
        join_input: {
          how: 'anti',
          join_mapping: [{ left_col: 'id', right_col: 'id' }]
        }
      }

      const result = inferOutputSchema('join', leftSchema, settings, rightSchema)
      expect(result).toEqual(leftSchema)
    })

    it('should return null when right schema is missing', () => {
      const settings: NodeJoinSettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        depending_on_ids: [1, 2],
        join_input: {
          how: 'inner',
          join_mapping: [{ left_col: 'id', right_col: 'id' }]
        }
      }

      const result = inferOutputSchema('join', leftSchema, settings, null)
      expect(result).toBeNull()
    })
  })

  describe('inferOutputSchema - Unpivot Node', () => {
    it('should return index columns plus variable and value', () => {
      const settings: NodeUnpivotSettings = {
        node_id: 1,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        unpivot_input: {
          index_columns: ['id', 'name'],
          value_columns: ['value'],
          data_type_selector_mode: 'column'
        }
      }

      const result = inferOutputSchema('unpivot', basicSchema, settings)

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'name', data_type: 'String' },
        { name: 'variable', data_type: 'String' },
        { name: 'value', data_type: 'String' }
      ])
    })

    it('should return null when unpivot_input is missing', () => {
      const settings = {
        node_id: 1,
        is_setup: false,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: ''
      } as NodeUnpivotSettings

      const result = inferOutputSchema('unpivot', basicSchema, settings)
      expect(result).toBeNull()
    })
  })

  describe('inferOutputSchema - Edge Cases', () => {
    it('should return null when input schema is null', () => {
      const result = inferOutputSchema('filter', null, {} as any)
      expect(result).toBeNull()
    })

    it('should return null when input schema is empty', () => {
      const result = inferOutputSchema('filter', [], {} as any)
      expect(result).toBeNull()
    })

    it('should return input schema for unknown node types', () => {
      const result = inferOutputSchema('unknown_type', basicSchema, {} as any)
      expect(result).toEqual(basicSchema)
    })
  })

  describe('inferSchemaFromCsv', () => {
    it('should infer schema from CSV with headers', () => {
      const csv = `id,name,value
1,Alice,100.5
2,Bob,200.3
3,Charlie,300.7`

      const result = inferSchemaFromCsv(csv, true, ',')

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'name', data_type: 'String' },
        { name: 'value', data_type: 'Float64' }
      ])
    })

    it('should infer schema from CSV without headers', () => {
      const csv = `1,Alice,100.5
2,Bob,200.3`

      const result = inferSchemaFromCsv(csv, false, ',')

      expect(result).toEqual([
        { name: 'column_1', data_type: 'Int64' },
        { name: 'column_2', data_type: 'String' },
        { name: 'column_3', data_type: 'Float64' }
      ])
    })

    it('should handle tab-delimited CSV', () => {
      const csv = `id\tname\tactive
1\tAlice\ttrue
2\tBob\tfalse`

      const result = inferSchemaFromCsv(csv, true, '\\t')

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'name', data_type: 'String' },
        { name: 'active', data_type: 'Boolean' }
      ])
    })

    it('should handle semicolon-delimited CSV', () => {
      const csv = `id;name;value
1;Alice;100
2;Bob;200`

      const result = inferSchemaFromCsv(csv, true, ';')

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'name', data_type: 'String' },
        { name: 'value', data_type: 'Int64' }
      ])
    })

    it('should return null for empty CSV', () => {
      expect(inferSchemaFromCsv('', true, ',')).toBeNull()
      expect(inferSchemaFromCsv('   ', true, ',')).toBeNull()
    })

    it('should detect boolean columns', () => {
      const csv = `flag1,flag2
true,false
false,true
true,true`

      const result = inferSchemaFromCsv(csv, true, ',')

      expect(result).toEqual([
        { name: 'flag1', data_type: 'Boolean' },
        { name: 'flag2', data_type: 'Boolean' }
      ])
    })

    it('should detect integer vs float columns', () => {
      const csv = `int_col,float_col
1,1.5
2,2.5
3,3.0`

      const result = inferSchemaFromCsv(csv, true, ',')

      expect(result).toEqual([
        { name: 'int_col', data_type: 'Int64' },
        { name: 'float_col', data_type: 'Float64' }
      ])
    })
  })

  describe('inferSchemaFromRawData', () => {
    it('should convert field definitions to schema', () => {
      const fields = [
        { name: 'id', data_type: 'Int64' },
        { name: 'name', data_type: 'String' },
        { name: 'value', data_type: 'Float64' }
      ]

      const result = inferSchemaFromRawData(fields)

      expect(result).toEqual([
        { name: 'id', data_type: 'Int64' },
        { name: 'name', data_type: 'String' },
        { name: 'value', data_type: 'Float64' }
      ])
    })

    it('should default to String when data_type is missing', () => {
      const fields = [
        { name: 'col1', data_type: '' },
        { name: 'col2', data_type: 'Int64' }
      ]

      const result = inferSchemaFromRawData(fields)

      expect(result).toEqual([
        { name: 'col1', data_type: 'String' },
        { name: 'col2', data_type: 'Int64' }
      ])
    })

    it('should return null for empty fields array', () => {
      expect(inferSchemaFromRawData([])).toBeNull()
    })

    it('should return null when fields is null/undefined', () => {
      expect(inferSchemaFromRawData(null as any)).toBeNull()
      expect(inferSchemaFromRawData(undefined as any)).toBeNull()
    })
  })
})
