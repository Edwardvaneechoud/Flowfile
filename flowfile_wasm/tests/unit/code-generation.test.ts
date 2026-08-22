/**
 * Code Generation Unit Tests
 * Tests for converting flow graphs to Python/Polars code
 */

import { describe, it, expect } from 'vitest'
import { useCodeGeneration } from '../../src/composables/useCodeGeneration'
import type { FlowNode, FlowEdge } from '../../src/types'

describe('Code Generation', () => {
  const { generateCode } = useCodeGeneration()

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
    it('should generate code for read node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', {
        received_file: {
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
      expect(code).toContain('has_header=True')
    })

    it('should emit infer_schema_length=0 when schema inference is off', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', {
        received_file: {
          name: 'data.csv',
          file_type: 'csv',
          table_settings: {
            delimiter: ',',
            has_headers: true,
            starting_from_line: 0,
            infer_schema: false
          }
        }
      }))

      const code = generateCode({ nodes, edges: [] })

      // pl.scan_csv has no infer_schema kwarg; the flag maps to infer_schema_length=0
      expect(code).toContain('infer_schema_length=0')
      expect(code).not.toContain('infer_schema=False')
    })

    it('should read from the source URL when the file was loaded from one', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', {
        file_name: 'sales.csv',
        received_file: {
          name: 'sales.csv',
          path: 'https://raw.githubusercontent.com/user/repo/main/sales.csv',
          file_type: 'csv',
          table_settings: { delimiter: ',', has_headers: true, starting_from_line: 0 }
        }
      }))

      const code = generateCode({ nodes, edges: [] })

      expect(code).toContain('pl.scan_csv(')
      expect(code).toContain('"https://raw.githubusercontent.com/user/repo/main/sales.csv"')
      expect(code).not.toContain('"sales.csv"')
    })

    it('should read remote parquet and excel from their source URLs', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', {
        file_name: 'data.parquet',
        received_file: {
          name: 'data.parquet',
          path: 'https://example.com/data.parquet',
          file_type: 'parquet',
          table_settings: {}
        }
      }))
      nodes.set(2, createNode(2, 'read', {
        file_name: 'book.xlsx',
        received_file: {
          name: 'book.xlsx',
          path: 'https://example.com/book.xlsx',
          file_type: 'excel',
          table_settings: { sheet_name: 'Sheet1', has_headers: true }
        }
      }))

      const code = generateCode({ nodes, edges: [] })

      expect(code).toContain('pl.scan_parquet("https://example.com/data.parquet")')
      expect(code).toContain('urlopen("https://example.com/book.xlsx")')
      expect(code).toContain('BytesIO(')
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
      expect(code).toContain('"id": [1, 2, 3]')
      expect(code).toContain('"name": ["Alice", "Bob", "Charlie"]')
    })

    it('should generate code for filter node with basic filter', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
        nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
      expect(code).toContain('descending=[True, False]')
    })

    it('should generate code for unique node', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'head', {
        sample_size: 100
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('.head(100)')
    })

    it('should elide explore_data node (transparent pass-through, no dead code)', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'explore_data', {}, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      // explore_data is interactive-only: it emits no statement and the pipeline
      // returns its upstream frame directly.
      expect(code).not.toContain('Preview')
      expect(code).not.toContain('explore_data')
      expect(code).toContain('source = pl.scan_csv')
      expect(code).toContain('return source')
    })

    it('should generate code for output node with CSV', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
      nodes.set(1, createNode(1, 'read', {
        received_file: { name: 'left.csv', table_settings: {} }
      }))
      nodes.set(2, createNode(2, 'read', {
        received_file: { name: 'right.csv', table_settings: {} }
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
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'left.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'read', { received_file: { name: 'right.csv', table_settings: {} } }))

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
      nodes.set(1, createNode(1, 'read', {
        received_file: { name: 'data.csv', table_settings: {} }
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

      // The linear read -> filter -> select chain fuses into one piped expression
      // named after its terminal operation; no df_1/df_2/df_3 ladder remains.
      expect(code).toContain('selected = (')
      expect(code).not.toContain('df_1')
      expect(code).not.toContain('df_2')
      expect(code).not.toContain('df_3')

      // scan_csv is the pipe root, so filter/select chain onto it (no re-assignment).
      expect(code).not.toContain('= pl.scan_csv')
      expect(code).toContain('pl.scan_csv')
      expect(code).toContain('.filter(')
      expect(code).toContain('.select(')
      expect(code).toContain('selected.sink_csv(')
    })

    it('should generate proper function structure', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', {
        received_file: { name: 'data.csv', table_settings: {} }
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
      // Type-aware footer: an external_output tail binds a DataFrame already.
      expect(code).toContain('print(pipeline_output)')
    })
  })

  describe('Chain Fusion & Naming', () => {
    it('fuses a linear single-use chain into one piped expression', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'filter', {
        filter_input: { mode: 'basic', basic_filter: { field: 'age', operator: 'greater_than', value: '18' } }
      }, [1]))
      nodes.set(3, createNode(3, 'sort', { sort_input: [{ column: 'age', how: 'asc' }] }, [2]))

      const code = generateCode({ nodes, edges: createEdges([[1, 2], [2, 3]]) })

      // Terminal operation names the pipe; intermediates are not re-bound.
      expect(code).toContain('ordered = (')
      expect(code).toContain('.filter(')
      expect(code).toContain('.sort(')
      expect(code).not.toContain('df_1')
      expect(code).not.toContain('df_2')
      expect(code).not.toContain('df_3')
      expect(code).toContain('return ordered')
    })

    it('keeps a named variable at a fan-out boundary', () => {
      // read feeds both a filter and a sort -> read must survive as its own var.
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'filter', {
        filter_input: { mode: 'basic', basic_filter: { field: 'age', operator: 'greater_than', value: '18' } }
      }, [1]))
      nodes.set(3, createNode(3, 'sort', { sort_input: [{ column: 'age', how: 'asc' }] }, [1]))

      const code = generateCode({ nodes, edges: createEdges([[1, 2], [1, 3]]) })

      expect(code).toContain('source = pl.scan_csv')
      expect(code).toContain('source.filter')
      expect(code).toContain('source.sort')
    })

    it('suffixes labels that collide across boundaries', () => {
      // Two independent sources both want the "source" label.
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'a.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'read', { received_file: { name: 'b.csv', table_settings: {} } }))
      const joinNode = createNode(3, 'join', {
        join_input: { how: 'inner', join_mapping: [{ left_col: 'id', right_col: 'id' }] }
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

      expect(code).toContain('source_1 = pl.scan_csv')
      expect(code).toContain('source_2 = pl.scan_csv')
      expect(code).toContain('joined = source_1.join')
    })

    it('does not fuse into a node that references its input variable elsewhere', () => {
      // dynamic_rename's expression references `<input>.columns`, so fusing would
      // drop the variable it relies on -> the source must survive as its own var.
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'dynamic_rename', {
        dynamic_rename_input: { selection_mode: 'all', rename_mode: 'prefix', prefix: 'x_' }
      }, [1]))

      const code = generateCode({ nodes, edges: createEdges([[1, 2]]) })

      expect(code).toContain('source = pl.scan_csv')
      expect(code).toContain('source.columns')
      expect(code).toContain('renamed = source.rename')
    })

    it('does not rewrite a provisional token that appears inside a string literal', () => {
      // node 1 fans out (so it survives as `source`, i.e. df_1 -> source is in the
      // rename map). A downstream filter compares against the literal value "df_1".
      // The variable reference must be renamed, but the string literal must not.
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
      nodes.set(2, createNode(2, 'filter', {
        filter_input: { mode: 'basic', basic_filter: { field: 'label', operator: 'equals', value: 'df_1' } }
      }, [1]))
      nodes.set(3, createNode(3, 'sort', { sort_input: [{ column: 'label', how: 'asc' }] }, [1]))

      const code = generateCode({ nodes, edges: createEdges([[1, 2], [1, 3]]) })

      expect(code).toContain('source = pl.scan_csv') // df_1 -> source (real rename)
      expect(code).toContain('source.filter') // variable reference renamed
      expect(code).toContain('== "df_1"') // string literal left verbatim
      expect(code).not.toContain('== "source"') // the naive-regex bug would produce this
    })

    it('names custom polars_code functions by node id', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
      nodes.set(7, createNode(7, 'polars_code', {
        polars_code_input: { polars_code: 'df.head(5)' }
      }, [1]))

      const code = generateCode({ nodes, edges: createEdges([[1, 7]]) })

      expect(code).toContain('def _polars_code_7(')
      expect(code).toContain('transformed = _polars_code_7(')
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
        nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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
      nodes.set(1, createNode(1, 'read', { received_file: { name: 'data.csv', table_settings: {} } }))
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

  describe('Node Reference', () => {
    it('should use node_reference as variable name when set on FlowNode', () => {
      const nodes = new Map<number, FlowNode>()
      const node = createNode(1, 'manual_input', {
        raw_data_format: {
          columns: [
            { name: 'id', data_type: 'Int64' },
            { name: 'name', data_type: 'String' }
          ],
          data: [[1, 2, 3], ['Alice', 'Bob', 'Charlie']]
        }
      })
      node.node_reference = 'my_data'
      nodes.set(1, node)

      const code = generateCode({ nodes, edges: [] })

      expect(code).toContain('my_data = pl.LazyFrame')
      expect(code).not.toContain('df_1 = ')
    })

    it('should use node_reference from settings when set', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'manual_input', {
        node_reference: 'source_data',
        raw_data_format: {
          columns: [
            { name: 'id', data_type: 'Int64' }
          ],
          data: [[1, 2, 3]]
        }
      }))

      const code = generateCode({ nodes, edges: [] })

      expect(code).toContain('source_data = pl.LazyFrame')
      expect(code).not.toContain('df_1 = ')
    })

    it('should use node_reference in downstream node references', () => {
      const nodes = new Map<number, FlowNode>()

      const inputNode = createNode(1, 'manual_input', {
        raw_data_format: {
          columns: [
            { name: 'price', data_type: 'Int64' },
            { name: 'quantity', data_type: 'Int64' }
          ],
          data: [[10, 20, 30], [2, 3, 4]]
        }
      })
      inputNode.node_reference = 'source_data'
      nodes.set(1, inputNode)

      const filterNode = createNode(2, 'filter', {
        filter_input: {
          mode: 'basic',
          basic_filter: {
            field: 'price',
            operator: 'greater_than',
            value: '15'
          }
        }
      }, [1])
      filterNode.node_reference = 'filtered_data'
      nodes.set(2, filterNode)

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('source_data = pl.LazyFrame')
      expect(code).toContain('filtered_data = source_data.filter')
      expect(code).not.toContain('df_1')
      expect(code).not.toContain('df_2')
    })

    it('should work with mixed nodes (with and without node_reference)', () => {
      const nodes = new Map<number, FlowNode>()

      const inputNode = createNode(1, 'manual_input', {
        raw_data_format: {
          columns: [{ name: 'id', data_type: 'Int64' }],
          data: [[1, 2]]
        }
      })
      inputNode.node_reference = 'custom_input'
      nodes.set(1, inputNode)

      nodes.set(2, createNode(2, 'filter', {
        filter_input: {
          mode: 'basic',
          basic_filter: { field: 'id', operator: 'greater_than', value: '1' }
        }
      }, [1]))

      const code = generateCode({
        nodes,
        edges: createEdges([[1, 2]])
      })

      expect(code).toContain('custom_input = pl.LazyFrame')
      // Unpinned boundary gets an operation-based name, not df_2.
      expect(code).toContain('filtered = custom_input.filter')
      expect(code).not.toContain('df_1')
      expect(code).not.toContain('df_2')
    })

    it('should use node_reference in join operations', () => {
      const nodes = new Map<number, FlowNode>()

      const leftNode = createNode(1, 'manual_input', {
        raw_data_format: {
          columns: [
            { name: 'id', data_type: 'Int64' },
            { name: 'name', data_type: 'String' }
          ],
          data: [[1, 2], ['Alice', 'Bob']]
        }
      })
      leftNode.node_reference = 'left_table'
      nodes.set(1, leftNode)

      const rightNode = createNode(2, 'manual_input', {
        raw_data_format: {
          columns: [
            { name: 'id', data_type: 'Int64' },
            { name: 'city', data_type: 'String' }
          ],
          data: [[1, 2], ['NYC', 'LA']]
        }
      })
      rightNode.node_reference = 'right_table'
      nodes.set(2, rightNode)

      const joinNode = createNode(3, 'join', {
        join_input: {
          how: 'inner',
          join_mapping: [{ left_col: 'id', right_col: 'id' }]
        }
      })
      joinNode.leftInputId = 1
      joinNode.rightInputId = 2
      joinNode.node_reference = 'joined_result'
      nodes.set(3, joinNode)

      const code = generateCode({
        nodes,
        edges: [
          { id: 'e1-3', source: '1', target: '3', sourceHandle: 'output-0', targetHandle: 'input-0' },
          { id: 'e2-3', source: '2', target: '3', sourceHandle: 'output-0', targetHandle: 'input-1' }
        ]
      })

      expect(code).toContain('left_table = pl.LazyFrame')
      expect(code).toContain('right_table = pl.LazyFrame')
      expect(code).toContain('joined_result = left_table.join')
      expect(code).not.toContain('df_1')
      expect(code).not.toContain('df_2')
      expect(code).not.toContain('df_3')
    })

    it('should fall back to df_{node_id} when node_reference is empty', () => {
      const nodes = new Map<number, FlowNode>()
      const node = createNode(1, 'manual_input', {
        node_reference: '',
        raw_data_format: {
          columns: [{ name: 'id', data_type: 'Int64' }],
          data: [[1, 2, 3]]
        }
      })
      node.node_reference = ''
      nodes.set(1, node)

      const code = generateCode({ nodes, edges: [] })

      // No user reference -> auto operation-based name (source), not df_1.
      expect(code).toContain('source = pl.LazyFrame')
      expect(code).not.toContain('df_1')
    })

    it('should use an operation-based name when node_reference is undefined', () => {
      const nodes = new Map<number, FlowNode>()
      nodes.set(1, createNode(1, 'manual_input', {
        raw_data_format: {
          columns: [{ name: 'id', data_type: 'Int64' }],
          data: [[1, 2, 3]]
        }
      }))

      const code = generateCode({ nodes, edges: [] })

      expect(code).toContain('source = pl.LazyFrame')
      expect(code).not.toContain('df_1')
    })

    it('should return correct node_reference in return statement', () => {
      const nodes = new Map<number, FlowNode>()
      const node = createNode(1, 'manual_input', {
        raw_data_format: {
          columns: [{ name: 'id', data_type: 'Int64' }],
          data: [[1, 2, 3]]
        }
      })
      node.node_reference = 'final_result'
      nodes.set(1, node)

      const code = generateCode({ nodes, edges: [] })

      expect(code).toContain('return final_result')
    })
  })

  describe('YAML-loaded null input ids', () => {
    it('rightInputId: null never registers a phantom right input', () => {
      // Flows loaded from YAML carry `right_input_id: null` on every node.
      const nodes = new Map<number, FlowNode>()
      const read = createNode(1, 'read', {
        received_file: { name: 'data.csv', path: 'https://example.com/data.csv', file_type: 'csv' }
      })
      read.rightInputId = null as unknown as undefined
      read.leftInputId = null as unknown as undefined
      const unique = createNode(2, 'unique', { unique_input: { columns: null, strategy: 'any' } }, [1])
      unique.rightInputId = null as unknown as undefined
      nodes.set(1, read)
      nodes.set(2, unique)

      const code = generateCode({ nodes, edges: createEdges([[1, 2]]) })
      expect(code).not.toContain('df_right')
      expect(code).not.toContain('df_left')
    })
  })
})
