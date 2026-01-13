/**
 * Code Generation Unit Tests
 * Tests for converting flow graphs to Python/Polars code
 */

import { describe, it, expect } from 'vitest'
import { useCodeGeneration } from '../../src/composables/useCodeGeneration'
import type { FlowNode, FlowEdge } from '../../src/types'

describe('Code Generation', () => {
  const { generateCode } = useCodeGeneration()

  // Helper to create a basic node
  function createNode(id: number, type: string, settings: any, inputIds: number[] = []): FlowNode {
    return {
      id,
      type,
      x: 0,
      y: 0,
      settings: {
        node_id: id,
        is_setup: true,
        cache_results: true,
        pos_x: 0,
        pos_y: 0,
        description: '',
        ...settings
      },
      inputIds,
      leftInputId: undefined,
      rightInputId: undefined
    }
  }

  // Helper to create edges
  function createEdges(connections: [number, number][]): FlowEdge[] {
    return connections.map(([from, to]) => ({
      id: `e${from}-${to}`,
      source: String(from),
      target: String(to),
      sourceHandle: 'output-0',
      targetHandle: 'input-0'
    }))
  }

  describe('Single Node Generation', () => {
    it('should generate code for read_csv node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', {
        received_table: {
          name: 'data.csv',
          table_settings: {
            delimiter: ',',
            has_headers: true,
            starting_from_line: 0
          }
        }
      }))

      const code = generateCode({ nodes, edges: [] })

      expect(code).toContain('import polars as pl')
      expect(code).toContain('pl.scan_csv')
      expect(code).toContain('data.csv')
      expect(code).toContain('separator=","')
      expect(code).toContain('has_header=true')
    })

    it('should generate code for manual_input node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'manual_input', {
        raw_data_format: {
          columns: [
            { name: 'id', data_type: 'Int64' },
            { name: 'name', data_type: 'String' }
          ],
          data: [[1, 2, 3], ['Alice', 'Bob', 'Charlie']]
        }
      }))

      const code = generateCode({ nodes, edges: [] })

      expect(code).toContain('pl.LazyFrame')
      expect(code).toContain('"id"')
      expect(code).toContain('"name"')
    })

    it('should generate code for filter node with basic filter', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'filter', {
        filter_input: {
          mode: 'basic',
          basic_filter: {
            field: 'age',
            operator: 'greater_than',
            value: '18'
          }
        }
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.filter(')
      expect(code).toContain('pl.col("age")')
      expect(code).toContain('> 18')
    })

    it('should generate code for filter node with various operators', () => {
      const testCases = [
        { operator: 'equals', value: 'test', expected: '== "test"' },
        { operator: 'not_equals', value: 'test', expected: '!= "test"' },
        { operator: 'contains', value: 'sub', expected: '.str.contains("sub")' },
        { operator: 'starts_with', value: 'pre', expected: '.str.starts_with("pre")' },
        { operator: 'ends_with', value: 'suf', expected: '.str.ends_with("suf")' },
        { operator: 'is_null', value: '', expected: '.is_null()' },
        { operator: 'is_not_null', value: '', expected: '.is_not_null()' }
      ]

      for (const { operator, value, expected } of testCases) {
        const nodes = new Map<number, FlowNode>()
        nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
        nodes.set(2, createNode(2, 'filter', {
          filter_input: {
            mode: 'basic',
            basic_filter: { field: 'col', operator, value }
          }
        }, [1]))

        const code = generateCode({
          nodes,
          edges: createEdges([[1, 2]])
        })

        expect(code).toContain(expected)
      }
    })

    it('should generate code for select node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'select', {
        select_input: [
          { old_name: 'id', new_name: 'user_id', keep: true, is_available: true },
          { old_name: 'name', new_name: 'name', keep: true, is_available: true },
          { old_name: 'removed', new_name: 'removed', keep: false, is_available: true }
        ]
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.select([')
      expect(code).toContain('pl.col("id").alias("user_id")')
      expect(code).toContain('pl.col("name")')
      expect(code).not.toContain('removed')
    })

    it('should generate code for group_by node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'group_by', {
        groupby_input: {
          agg_cols: [
            { old_name: 'category', new_name: 'category', agg: 'groupby' },
            { old_name: 'amount', new_name: 'total', agg: 'sum' },
            { old_name: 'count', new_name: 'avg_count', agg: 'mean' }
          ]
        }
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.group_by(["category"])')
      expect(code).toContain('.agg([')
      expect(code).toContain('pl.col("amount").sum().alias("total")')
      expect(code).toContain('pl.col("count").mean().alias("avg_count")')
    })

    it('should generate code for sort node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'sort', {
        sort_input: [
          { column: 'date', how: 'desc' },
          { column: 'name', how: 'asc' }
        ]
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.sort(')
      expect(code).toContain('["date","name"]')
      expect(code).toContain('descending=[true,false]')
    })

    it('should generate code for unique node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'unique', {
        unique_input: {
          columns: ['id', 'name'],
          strategy: 'first'
        }
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.unique(')
      expect(code).toContain('["id","name"]')
      expect(code).toContain('keep="first"')
    })

    it('should generate code for head/sample node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'head', {
        sample_size: 100
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.head(100)')
    })

    it('should generate code for preview node (pass-through)', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'preview', {}, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('# Preview (pass-through)')
    })

    it('should generate code for output node with CSV', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'output', {
        output_settings: {
          name: 'result.csv',
          file_type: 'csv',
          table_settings: {
            delimiter: ','
          }
        }
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.sink_csv(')
      expect(code).toContain('"result.csv"')
    })

    it('should generate code for output node with Parquet', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'output', {
        output_settings: {
          name: 'result.parquet',
          file_type: 'parquet'
        }
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.sink_parquet(')
      expect(code).toContain('"result.parquet"')
    })

    it('should generate code for polars_code node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'polars_code', {
        polars_code_input: {
          polars_code: 'df.with_columns(pl.col("value") * 2)'
        }
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('# Custom Polars code')
      expect(code).toContain('.with_columns(pl.col("value") * 2)')
    })
  })

  describe('Join Node Generation', () => {
    it('should generate code for inner join', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', {
        received_table: { name: 'left.csv', table_settings: {} }
      }))
      nodes.set(2, createNode(2, 'read_csv', {
        received_table: { name: 'right.csv', table_settings: {} }
      }))

      const joinNode = createNode(3, 'join', {
        join_input: {
          how: 'inner',
          join_mapping: [
            { left_col: 'id', right_col: 'user_id' }
          ]
        }
      })
      joinNode.leftInputId = 1
      joinNode.rightInputId = 2
      nodes.set(3, joinNode)

      const code = generateCode({
        nodes,
        edges: [
          { id: 'e1-3', source: '1', target: '3', sourceHandle: 'output-0', targetHandle: 'input-0' },
          { id: 'e2-3', source: '2', target: '3', sourceHandle: 'output-0', targetHandle: 'input-1' }
        ]
      })

      expect(code).toContain('.join(')
      expect(code).toContain('left_on=["id"]')
      expect(code).toContain('right_on=["user_id"]')
      expect(code).toContain('how="inner"')
    })

    it('should generate code for left join', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'left.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'read_csv', { received_table: { name: 'right.csv', table_settings: {} } }))

      const joinNode = createNode(3, 'join', {
        join_input: {
          how: 'left',
          join_mapping: [{ left_col: 'id', right_col: 'id' }]
        }
      })
      joinNode.leftInputId = 1
      joinNode.rightInputId = 2
      nodes.set(3, joinNode)

      const code = generateCode({
        nodes,
        edges: [
          { id: 'e1-3', source: '1', target: '3', sourceHandle: 'output-0', targetHandle: 'input-0' },
          { id: 'e2-3', source: '2', target: '3', sourceHandle: 'output-0', targetHandle: 'input-1' }
        ]
      })

      expect(code).toContain('how="left"')
    })
  })

  describe('Pipeline Generation', () => {
    it('should generate code for multi-step pipeline', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', {
        received_table: { name: 'data.csv', table_settings: {} }
      }))
      nodes.set(2, createNode(2, 'filter', {
        filter_input: {
          mode: 'basic',
          basic_filter: { field: 'active', operator: 'equals', value: 'true' }
        }
      }, [1]))
      nodes.set(3, createNode(3, 'select', {
        select_input: [
          { old_name: 'id', new_name: 'id', keep: true, is_available: true },
          { old_name: 'name', new_name: 'name', keep: true, is_available: true }
        ]
      }, [2]))
      nodes.set(4, createNode(4, 'output', {
        output_settings: { name: 'result.csv', file_type: 'csv' }
      }, [3]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2], [2, 3], [3, 4]])
      })

      // Check execution order
      expect(code.indexOf('df_1')).toBeLessThan(code.indexOf('df_2'))
      expect(code.indexOf('df_2')).toBeLessThan(code.indexOf('df_3'))
      expect(code.indexOf('df_3')).toBeLessThan(code.indexOf('df_4'))

      // Check all operations present
      expect(code).toContain('scan_csv')
      expect(code).toContain('.filter(')
      expect(code).toContain('.select(')
      expect(code).toContain('.sink_csv(')
    })

    it('should generate proper function structure', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', {
        received_table: { name: 'data.csv', table_settings: {} }
      }))

      const code = generateCode({
        nodes,
        edges: [],
        flowName: 'My Test Pipeline'
      })

      expect(code).toContain('def run_etl_pipeline():')
      expect(code).toContain('ETL Pipeline: My Test Pipeline')
      expect(code).toContain('Generated from Flowfile WASM')
      expect(code).toContain('if __name__ == "__main__":')
      expect(code).toContain('pipeline_output = run_etl_pipeline()')
      expect(code).toContain('print(pipeline_output.collect())')
    })
  })

  describe('Error Handling', () => {
    it('should throw error for cyclic dependencies', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'filter', {}, [2]))
      nodes.set(2, createNode(2, 'filter', {}, [1]))

      expect(() =>
        generateCode({
          nodes,
          edges: createEdges([[1, 2], [2, 1]])
        })
      ).toThrow('cycle')
    })

    it('should warn about unsupported node types', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'unsupported_type', {}))

      expect(() =>
        generateCode({ nodes, edges: [] })
      ).toThrow(/cannot be converted/)
    })
  })

  describe('Aggregation Functions', () => {
    const aggTestCases = [
      { agg: 'sum', expected: '.sum()' },
      { agg: 'mean', expected: '.mean()' },
      { agg: 'min', expected: '.min()' },
      { agg: 'max', expected: '.max()' },
      { agg: 'count', expected: '.count()' },
      { agg: 'median', expected: '.median()' },
      { agg: 'first', expected: '.first()' },
      { agg: 'last', expected: '.last()' },
      { agg: 'n_unique', expected: '.n_unique()' }
    ]

    for (const { agg, expected } of aggTestCases) {
      it(`should generate correct code for ${agg} aggregation`, () => {
        const nodes = new Map<number, FlowNode>()
        nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
        nodes.set(2, createNode(2, 'group_by', {
          groupby_input: {
            agg_cols: [
              { old_name: 'category', new_name: 'category', agg: 'groupby' },
              { old_name: 'value', new_name: `value_${agg}`, agg }
            ]
          }
        }, [1]))

        const code = generateCode({
          nodes,
          edges: createEdges([[1, 2]])
        })

        expect(code).toContain(expected)
        expect(code).toContain(`alias("value_${agg}")`)
      })
    }
  })

  describe('Type Casting in Select', () => {
    it('should add cast for data type changes', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read_csv', { received_table: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'select', {
        select_input: [
          {
            old_name: 'value',
            new_name: 'value',
            keep: true,
            is_available: true,
            data_type_change: true,
            data_type: 'Integer'
          }
        ]
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.cast(pl.Int64)')
    })
  })
})
