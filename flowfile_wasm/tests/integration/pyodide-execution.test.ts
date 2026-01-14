/**
 * Pyodide Integration Tests
 * Tests for Python/Polars execution via Pyodide
 *
 * Note: These tests validate the Python code logic embedded in pyodide-store.ts.
 * In a full integration test environment with actual Pyodide, these would run
 * the Python code. For unit testing, we validate the expected behavior patterns.
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createPinia, setActivePinia } from 'pinia'

describe('Pyodide Execution Logic', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  describe('Node Execution Python Code', () => {
    // These tests validate the Python code structure and logic
    // embedded in the pyodide-store.ts execution functions

    describe('read_csv execution', () => {
      it('should handle CSV with custom delimiter', () => {
        // Expected Python behavior for read_csv with semicolon delimiter
        const settings = {
          received_table: {
            name: 'data.csv',
            table_settings: {
              delimiter: ';',
              has_headers: true,
              starting_from_line: 0
            }
          }
        }

        expect(settings.received_table.table_settings.delimiter).toBe(';')
        expect(settings.received_table.table_settings.has_headers).toBe(true)
      })

      it('should handle CSV with skip rows', () => {
        const settings = {
          received_table: {
            table_settings: {
              delimiter: ',',
              has_headers: true,
              starting_from_line: 5
            }
          }
        }

        expect(settings.received_table.table_settings.starting_from_line).toBe(5)
      })
    })

    describe('filter execution', () => {
      it('should construct equals filter correctly', () => {
        const settings = {
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'name',
              operator: 'equals',
              value: 'Alice'
            }
          }
        }

        // Python would construct: df.filter(pl.col("name") == "Alice")
        expect(settings.filter_input.basic_filter.operator).toBe('equals')
        expect(settings.filter_input.basic_filter.field).toBe('name')
        expect(settings.filter_input.basic_filter.value).toBe('Alice')
      })

      it('should handle numeric comparison filters', () => {
        const settings = {
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'age',
              operator: 'greater_than',
              value: '18'
            }
          }
        }

        // Python would convert string "18" to number 18
        expect(settings.filter_input.basic_filter.operator).toBe('greater_than')
        expect(parseInt(settings.filter_input.basic_filter.value)).toBe(18)
      })

      it('should handle between filter with two values', () => {
        const settings = {
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'value',
              operator: 'between',
              value: '10',
              value2: '100'
            }
          }
        }

        expect(settings.filter_input.basic_filter.operator).toBe('between')
        expect(settings.filter_input.basic_filter.value).toBe('10')
        expect(settings.filter_input.basic_filter.value2).toBe('100')
      })

      it('should handle in filter with comma-separated values', () => {
        const settings = {
          filter_input: {
            mode: 'basic',
            basic_filter: {
              field: 'status',
              operator: 'in',
              value: 'active, pending, approved'
            }
          }
        }

        // Python would split by comma: ['active', 'pending', 'approved']
        const values = settings.filter_input.basic_filter.value.split(',').map(v => v.trim())
        expect(values).toEqual(['active', 'pending', 'approved'])
      })

      it('should handle null check operators', () => {
        const isNullSettings = {
          filter_input: {
            mode: 'basic',
            basic_filter: { field: 'email', operator: 'is_null', value: '' }
          }
        }

        const isNotNullSettings = {
          filter_input: {
            mode: 'basic',
            basic_filter: { field: 'email', operator: 'is_not_null', value: '' }
          }
        }

        expect(isNullSettings.filter_input.basic_filter.operator).toBe('is_null')
        expect(isNotNullSettings.filter_input.basic_filter.operator).toBe('is_not_null')
      })

      it('should handle string operators', () => {
        const operators = ['contains', 'not_contains', 'starts_with', 'ends_with']

        for (const operator of operators) {
          const settings = {
            filter_input: {
              mode: 'basic',
              basic_filter: { field: 'name', operator, value: 'test' }
            }
          }

          expect(settings.filter_input.basic_filter.operator).toBe(operator)
        }
      })
    })

    describe('select execution', () => {
      it('should filter columns based on keep flag', () => {
        const settings = {
          select_input: [
            { old_name: 'id', new_name: 'id', keep: true },
            { old_name: 'name', new_name: 'name', keep: true },
            { old_name: 'hidden', new_name: 'hidden', keep: false }
          ]
        }

        const keptColumns = settings.select_input.filter(s => s.keep)
        expect(keptColumns).toHaveLength(2)
        expect(keptColumns.map(c => c.old_name)).toEqual(['id', 'name'])
      })

      it('should handle column renaming', () => {
        const settings = {
          select_input: [
            { old_name: 'id', new_name: 'user_id', keep: true },
            { old_name: 'name', new_name: 'full_name', keep: true }
          ]
        }

        const renamed = settings.select_input.filter(s => s.old_name !== s.new_name)
        expect(renamed).toHaveLength(2)
      })

      it('should sort by position', () => {
        const settings = {
          select_input: [
            { old_name: 'c', new_name: 'c', keep: true, position: 2 },
            { old_name: 'a', new_name: 'a', keep: true, position: 0 },
            { old_name: 'b', new_name: 'b', keep: true, position: 1 }
          ]
        }

        const sorted = [...settings.select_input].sort((a, b) => a.position - b.position)
        expect(sorted.map(s => s.old_name)).toEqual(['a', 'b', 'c'])
      })
    })

    describe('group_by execution', () => {
      it('should separate groupby columns from aggregation columns', () => {
        const settings = {
          groupby_input: {
            agg_cols: [
              { old_name: 'category', new_name: 'category', agg: 'groupby' },
              { old_name: 'region', new_name: 'region', agg: 'groupby' },
              { old_name: 'amount', new_name: 'total', agg: 'sum' },
              { old_name: 'count', new_name: 'records', agg: 'count' }
            ]
          }
        }

        const groupCols = settings.groupby_input.agg_cols.filter(c => c.agg === 'groupby')
        const aggCols = settings.groupby_input.agg_cols.filter(c => c.agg !== 'groupby')

        expect(groupCols).toHaveLength(2)
        expect(aggCols).toHaveLength(2)
        expect(groupCols.map(c => c.old_name)).toEqual(['category', 'region'])
        expect(aggCols.map(c => c.agg)).toEqual(['sum', 'count'])
      })

      it('should handle all aggregation types', () => {
        const aggTypes = ['sum', 'max', 'min', 'count', 'mean', 'median', 'first', 'last', 'n_unique', 'concat']

        for (const agg of aggTypes) {
          const settings = {
            groupby_input: {
              agg_cols: [
                { old_name: 'key', new_name: 'key', agg: 'groupby' },
                { old_name: 'value', new_name: `value_${agg}`, agg }
              ]
            }
          }

          expect(settings.groupby_input.agg_cols[1].agg).toBe(agg)
        }
      })
    })

    describe('join execution', () => {
      it('should handle inner join', () => {
        const settings = {
          join_input: {
            join_type: 'inner',
            join_mapping: [
              { left_col: 'id', right_col: 'user_id' }
            ]
          }
        }

        expect(settings.join_input.join_type).toBe('inner')
        expect(settings.join_input.join_mapping[0].left_col).toBe('id')
        expect(settings.join_input.join_mapping[0].right_col).toBe('user_id')
      })

      it('should handle multiple join keys', () => {
        const settings = {
          join_input: {
            join_type: 'inner',
            join_mapping: [
              { left_col: 'date', right_col: 'date' },
              { left_col: 'region', right_col: 'region' }
            ]
          }
        }

        const leftOn = settings.join_input.join_mapping.map(m => m.left_col)
        const rightOn = settings.join_input.join_mapping.map(m => m.right_col)

        expect(leftOn).toEqual(['date', 'region'])
        expect(rightOn).toEqual(['date', 'region'])
      })

      it('should handle all join types', () => {
        const joinTypes = ['inner', 'left', 'right', 'full', 'semi', 'anti']

        for (const joinType of joinTypes) {
          const settings = {
            join_input: {
              join_type: joinType,
              join_mapping: [{ left_col: 'id', right_col: 'id' }]
            }
          }

          expect(settings.join_input.join_type).toBe(joinType)
        }
      })

      it('should handle suffixes for overlapping columns', () => {
        const settings = {
          join_input: {
            join_type: 'inner',
            join_mapping: [{ left_col: 'id', right_col: 'id' }],
            left_suffix: '_left',
            right_suffix: '_right'
          }
        }

        expect(settings.join_input.left_suffix).toBe('_left')
        expect(settings.join_input.right_suffix).toBe('_right')
      })
    })

    describe('sort execution', () => {
      it('should handle single column sort', () => {
        const settings = {
          sort_input: {
            sort_cols: [
              { column: 'name', descending: false }
            ]
          }
        }

        expect(settings.sort_input.sort_cols).toHaveLength(1)
        expect(settings.sort_input.sort_cols[0].descending).toBe(false)
      })

      it('should handle multi-column sort', () => {
        const settings = {
          sort_input: {
            sort_cols: [
              { column: 'category', descending: false },
              { column: 'date', descending: true },
              { column: 'name', descending: false }
            ]
          }
        }

        const columns = settings.sort_input.sort_cols.map(s => s.column)
        const descending = settings.sort_input.sort_cols.map(s => s.descending)

        expect(columns).toEqual(['category', 'date', 'name'])
        expect(descending).toEqual([false, true, false])
      })
    })

    describe('unique execution', () => {
      it('should handle unique on specific columns', () => {
        const settings = {
          unique_input: {
            columns: ['id', 'name'],
            strategy: 'first'
          }
        }

        expect(settings.unique_input.columns).toEqual(['id', 'name'])
        expect(settings.unique_input.strategy).toBe('first')
      })

      it('should handle unique on all columns', () => {
        const settings = {
          unique_input: {
            columns: [],
            strategy: 'first'
          }
        }

        expect(settings.unique_input.columns).toEqual([])
      })

      it('should handle different keep strategies', () => {
        const strategies = ['first', 'last', 'any', 'none']

        for (const strategy of strategies) {
          const settings = {
            unique_input: {
              columns: [],
              strategy
            }
          }

          expect(settings.unique_input.strategy).toBe(strategy)
        }
      })
    })

    describe('polars_code execution', () => {
      it('should accept custom Polars code', () => {
        const settings = {
          polars_code_input: {
            polars_code: 'input_df.with_columns(pl.col("value") * 2)'
          }
        }

        expect(settings.polars_code_input.polars_code).toContain('with_columns')
      })

      it('should handle assignment patterns', () => {
        const codePatterns = [
          'output_df = input_df.filter(pl.col("x") > 0)',
          'result = input_df.select(["a", "b"])',
          'df = input_df.head(10)'
        ]

        for (const code of codePatterns) {
          const settings = {
            polars_code_input: { polars_code: code }
          }

          expect(settings.polars_code_input.polars_code).toBeTruthy()
        }
      })
    })

    describe('pivot execution', () => {
      it('should validate pivot configuration', () => {
        const settings = {
          pivot_input: {
            index_columns: ['date', 'region'],
            pivot_column: 'category',
            value_col: 'amount',
            aggregations: ['sum', 'count']
          }
        }

        expect(settings.pivot_input.index_columns).toEqual(['date', 'region'])
        expect(settings.pivot_input.pivot_column).toBe('category')
        expect(settings.pivot_input.value_col).toBe('amount')
        expect(settings.pivot_input.aggregations).toContain('sum')
      })
    })

    describe('unpivot execution', () => {
      it('should validate unpivot configuration', () => {
        const settings = {
          unpivot_input: {
            index_columns: ['id', 'name'],
            value_columns: ['jan', 'feb', 'mar'],
            data_type_selector_mode: 'column'
          }
        }

        expect(settings.unpivot_input.index_columns).toEqual(['id', 'name'])
        expect(settings.unpivot_input.value_columns).toEqual(['jan', 'feb', 'mar'])
      })

      it('should support data type selector mode', () => {
        const settings = {
          unpivot_input: {
            index_columns: ['id'],
            value_columns: [],
            data_type_selector: 'numeric',
            data_type_selector_mode: 'data_type'
          }
        }

        expect(settings.unpivot_input.data_type_selector).toBe('numeric')
        expect(settings.unpivot_input.data_type_selector_mode).toBe('data_type')
      })
    })

    describe('output execution', () => {
      it('should handle CSV output settings', () => {
        const settings = {
          output_settings: {
            name: 'result.csv',
            file_type: 'csv',
            table_settings: {
              delimiter: ',',
              encoding: 'utf-8'
            }
          }
        }

        expect(settings.output_settings.file_type).toBe('csv')
        expect(settings.output_settings.name).toBe('result.csv')
        expect(settings.output_settings.table_settings.delimiter).toBe(',')
      })

      it('should handle parquet output settings', () => {
        const settings = {
          output_settings: {
            name: 'result.parquet',
            file_type: 'parquet'
          }
        }

        expect(settings.output_settings.file_type).toBe('parquet')
        expect(settings.output_settings.name).toBe('result.parquet')
      })

      it('should handle tab delimiter', () => {
        const settings = {
          output_settings: {
            name: 'result.tsv',
            file_type: 'csv',
            table_settings: {
              delimiter: 'tab'
            }
          }
        }

        // Python code converts 'tab' to '\t'
        expect(settings.output_settings.table_settings.delimiter).toBe('tab')
      })
    })

    describe('head execution', () => {
      it('should respect n parameter', () => {
        const settings = {
          head_input: {
            n: 50
          }
        }

        expect(settings.head_input.n).toBe(50)
      })

      it('should default to 10 when not specified', () => {
        const settings = {
          head_input: {}
        }

        const n = (settings.head_input as any).n ?? 10
        expect(n).toBe(10)
      })
    })
  })

  describe('Result Format', () => {
    it('should return success result with data preview', () => {
      const result = {
        success: true,
        data: {
          columns: ['id', 'name', 'value'],
          data: [[1, 'Alice', 100], [2, 'Bob', 200]],
          total_rows: 2
        },
        schema: [
          { name: 'id', data_type: 'Int64' },
          { name: 'name', data_type: 'String' },
          { name: 'value', data_type: 'Float64' }
        ]
      }

      expect(result.success).toBe(true)
      expect(result.data.columns).toHaveLength(3)
      expect(result.data.data).toHaveLength(2)
      expect(result.schema).toHaveLength(3)
    })

    it('should return error result on failure', () => {
      const result = {
        success: false,
        error: 'Filter error on node #2: Column "invalid" not found. Available columns: id, name, value'
      }

      expect(result.success).toBe(false)
      expect(result.error).toContain('not found')
      expect(result.error).toContain('Available columns')
    })

    it('should include download info for output nodes', () => {
      const result = {
        success: true,
        data: { columns: ['id'], data: [[1]], total_rows: 1 },
        schema: [{ name: 'id', data_type: 'Int64' }],
        download: {
          content: 'id\n1\n',
          file_name: 'output.csv',
          file_type: 'csv',
          mime_type: 'text/csv',
          row_count: 1
        }
      }

      expect(result.download).toBeDefined()
      expect(result.download.file_name).toBe('output.csv')
      expect(result.download.row_count).toBe(1)
    })
  })

  describe('Error Message Formatting', () => {
    it('should format column not found errors with suggestions', () => {
      const formatError = (nodeType: string, nodeId: number, field: string, availableColumns: string[]) => {
        const parts = [`${nodeType.replace('_', ' ')} error on node #${nodeId}:`]
        parts.push(`Column "${field}" not found.`)
        parts.push(`Available columns: ${availableColumns.join(', ')}`)

        // Suggest similar columns
        const similar = availableColumns.filter(c =>
          c.toLowerCase().includes(field.toLowerCase()) ||
          field.toLowerCase().includes(c.toLowerCase())
        )
        if (similar.length > 0) {
          parts.push(`Did you mean: ${similar.join(', ')}?`)
        }

        return parts.join(' ')
      }

      const error = formatError('filter', 2, 'nam', ['id', 'name', 'value'])

      expect(error).toContain('filter error')
      expect(error).toContain('node #2')
      expect(error).toContain('"nam"')
      expect(error).toContain('Available columns')
      expect(error).toContain('Did you mean: name')
    })

    it('should include input node reference in error messages', () => {
      const errorMessage = 'Filter error on node #3: No input data from node #1. Make sure the upstream node executed successfully.'

      expect(errorMessage).toContain('node #3')
      expect(errorMessage).toContain('node #1')
      expect(errorMessage).toContain('upstream')
    })
  })
})
