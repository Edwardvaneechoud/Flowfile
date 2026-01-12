/**
 * Schema Inference Utilities
 * Pure TypeScript functions for inferring output schemas without Python execution
 */

import type {
  ColumnSchema,
  NodeSettings,
  NodeSelectSettings,
  NodeGroupBySettings,
  NodeJoinSettings,
  NodePivotSettings,
  NodeUnpivotSettings,
  AggType,
  SelectInput,
  AggCol,
  JoinSettings
} from '../types'

/**
 * Infer the output type for an aggregation function
 */
function inferAggOutputType(agg: AggType, inputType: string): string {
  switch (agg) {
    case 'count':
    case 'n_unique':
      return 'Int64'
    case 'sum':
      // Sum of integers stays integer, floats stay float
      if (inputType.toLowerCase().includes('int')) {
        return 'Int64'
      }
      return 'Float64'
    case 'mean':
    case 'median':
      return 'Float64'
    case 'first':
    case 'last':
    case 'max':
    case 'min':
      // These preserve the input type
      return inputType
    case 'concat':
      return 'String'
    case 'groupby':
      // Groupby columns preserve their type
      return inputType
    default:
      return inputType
  }
}

/**
 * Find a column in a schema by name
 */
function findColumn(schema: ColumnSchema[], name: string): ColumnSchema | undefined {
  return schema.find(col => col.name === name)
}

/**
 * Infer output schema for a SELECT node
 */
function inferSelectSchema(
  inputSchema: ColumnSchema[],
  settings: NodeSelectSettings
): ColumnSchema[] | null {
  const selectInput = settings.select_input || []

  if (selectInput.length === 0) {
    // No select configuration - return input schema as-is
    return inputSchema
  }

  // Filter to kept columns and sort by position
  const keptColumns = selectInput
    .filter((s: SelectInput) => s.keep !== false)
    .sort((a: SelectInput, b: SelectInput) => (a.position ?? 0) - (b.position ?? 0))

  const result: ColumnSchema[] = []

  for (const selectCol of keptColumns) {
    const inputCol = findColumn(inputSchema, selectCol.old_name)
    if (inputCol) {
      result.push({
        name: selectCol.new_name || selectCol.old_name,
        data_type: selectCol.data_type || inputCol.data_type
      })
    }
  }

  return result.length > 0 ? result : null
}

/**
 * Infer output schema for a GROUP_BY node
 */
function inferGroupBySchema(
  inputSchema: ColumnSchema[],
  settings: NodeGroupBySettings
): ColumnSchema[] | null {
  const groupbyInput = settings.groupby_input
  if (!groupbyInput?.agg_cols || groupbyInput.agg_cols.length === 0) {
    return null
  }

  const result: ColumnSchema[] = []

  // First add groupby columns (in order they appear)
  const groupCols = groupbyInput.agg_cols.filter((c: AggCol) => c.agg === 'groupby')
  for (const col of groupCols) {
    const inputCol = findColumn(inputSchema, col.old_name)
    if (inputCol) {
      result.push({
        name: col.new_name || col.old_name,
        data_type: inputCol.data_type
      })
    }
  }

  // Then add aggregation columns
  const aggCols = groupbyInput.agg_cols.filter((c: AggCol) => c.agg !== 'groupby')
  for (const col of aggCols) {
    const inputCol = findColumn(inputSchema, col.old_name)
    const inputType = inputCol?.data_type || 'Unknown'
    const outputType = col.output_type || inferAggOutputType(col.agg, inputType)

    result.push({
      name: col.new_name || `${col.old_name}_${col.agg}`,
      data_type: outputType
    })
  }

  return result.length > 0 ? result : null
}

/**
 * Infer output schema for a JOIN node
 */
function inferJoinSchema(
  leftSchema: ColumnSchema[],
  rightSchema: ColumnSchema[],
  settings: NodeJoinSettings | JoinSettings
): ColumnSchema[] | null {
  const joinInput = settings.join_input as any
  if (!joinInput) {
    return null
  }

  const joinType = joinInput.how || joinInput.join_type || 'inner'
  const joinMapping = joinInput.join_mapping || []
  // These properties exist in actual runtime settings even if not in base JoinInput type
  const leftSuffix = joinInput.left_suffix || '_left'
  const rightSuffix = joinInput.right_suffix || '_right'

  // For semi and anti joins, only return left schema
  if (joinType === 'semi' || joinType === 'anti') {
    return leftSchema
  }

  // Get join key column names
  const leftKeySet = new Set(joinMapping.map((m: { left_col: string; right_col: string }) => m.left_col))
  const rightKeySet = new Set(joinMapping.map((m: { left_col: string; right_col: string }) => m.right_col))

  // Get all column names from both schemas
  const leftColNames = new Set(leftSchema.map(c => c.name))
  const rightColNames = new Set(rightSchema.map(c => c.name))

  const result: ColumnSchema[] = []

  // Add all left columns
  for (const col of leftSchema) {
    // Check if this column exists in right schema (and is not a key column)
    const needsSuffix = rightColNames.has(col.name) && !leftKeySet.has(col.name)
    result.push({
      name: needsSuffix ? col.name + leftSuffix : col.name,
      data_type: col.data_type
    })
  }

  // Add right columns that are not join keys
  for (const col of rightSchema) {
    if (rightKeySet.has(col.name)) {
      // Skip right key columns as they duplicate left keys
      continue
    }

    // Check if this column exists in left schema
    const needsSuffix = leftColNames.has(col.name)
    result.push({
      name: needsSuffix ? col.name + rightSuffix : col.name,
      data_type: col.data_type
    })
  }

  return result.length > 0 ? result : null
}

/**
 * Infer output schema for an UNPIVOT node
 * Unpivot always produces: index columns + 'variable' (string) + 'value' (varies)
 */
function inferUnpivotSchema(
  inputSchema: ColumnSchema[],
  settings: NodeUnpivotSettings
): ColumnSchema[] | null {
  const unpivotInput = settings.unpivot_input
  if (!unpivotInput) {
    return null
  }

  const result: ColumnSchema[] = []

  // Add index columns (these are preserved as-is)
  const indexColumns = unpivotInput.index_columns || []
  for (const colName of indexColumns) {
    const col = findColumn(inputSchema, colName)
    if (col) {
      result.push({ name: col.name, data_type: col.data_type })
    }
  }

  // Add the 'variable' column (contains original column names)
  result.push({ name: 'variable', data_type: 'String' })

  // Add the 'value' column (type depends on unpivoted columns)
  // For simplicity, we'll use String as a safe default
  // since the actual type depends on the columns being unpivoted
  result.push({ name: 'value', data_type: 'String' })

  return result.length > 0 ? result : null
}

/**
 * Infer output schema for a PIVOT node
 * Pivot output depends on unique values in the pivot column, which we can't know without execution
 * We return null to indicate that lazy execution is needed
 */
function inferPivotSchema(
  _inputSchema: ColumnSchema[],
  _settings: NodePivotSettings
): ColumnSchema[] | null {
  // Pivot creates columns based on unique values in the pivot column
  // We can't infer these without executing the query
  // Return null to signal that lazy execution is needed
  return null
}

/**
 * Main schema inference function
 * Computes output schema from input schema + node settings
 *
 * @param nodeType - The type of node (filter, select, join, etc.)
 * @param inputSchema - The input schema from upstream node (null if no input)
 * @param settings - The node's settings
 * @param rightInputSchema - For join nodes, the right input schema
 * @returns The inferred output schema, or null if cannot be inferred
 */
export function inferOutputSchema(
  nodeType: string,
  inputSchema: ColumnSchema[] | null,
  settings: NodeSettings,
  rightInputSchema?: ColumnSchema[] | null
): ColumnSchema[] | null {
  // Source nodes - schema comes from actual data, not inference
  if (nodeType === 'read_csv' || nodeType === 'manual_input') {
    // These nodes get their schema from loaded data
    // Return null to indicate we should keep existing schema
    return null
  }

  // If no input schema, we can't infer output
  if (!inputSchema || inputSchema.length === 0) {
    return null
  }

  switch (nodeType) {
    // Pass-through nodes - return input schema unchanged
    case 'filter':
    case 'sort':
    case 'head':
    case 'sample':
    case 'unique':
    case 'preview':
      return inputSchema

    // Polars code - can't infer transformations without execution
    // Return null so downstream nodes don't get incorrect inferred schemas
    case 'polars_code':
    case 'formula':
      return null

    // Select - filter/reorder columns
    case 'select':
      return inferSelectSchema(inputSchema, settings as NodeSelectSettings)

    // Group by - return grouped + aggregated columns
    case 'group_by':
      return inferGroupBySchema(inputSchema, settings as NodeGroupBySettings)

    // Join - merge schemas
    case 'join':
      if (!rightInputSchema || rightInputSchema.length === 0) {
        return null
      }
      return inferJoinSchema(inputSchema, rightInputSchema, settings as NodeJoinSettings)

    // Pivot - can't infer without execution (depends on unique values)
    case 'pivot':
      return inferPivotSchema(inputSchema, settings as NodePivotSettings)

    // Unpivot - always produces index columns + variable + value
    case 'unpivot':
      return inferUnpivotSchema(inputSchema, settings as NodeUnpivotSettings)

    default:
      // Unknown node type - return input schema as fallback
      return inputSchema
  }
}

/**
 * Check if a node type is a source node (produces data rather than transforming it)
 */
export function isSourceNode(nodeType: string): boolean {
  return nodeType === 'read_csv' || nodeType === 'manual_input'
}

/**
 * Get downstream node IDs from a given node
 */
export function getDownstreamNodeIds(
  nodeId: number,
  edges: { source: string; target: string }[]
): number[] {
  return edges
    .filter(e => e.source === String(nodeId))
    .map(e => parseInt(e.target))
}

/**
 * Infer a data type from a sample of string values
 */
function inferDataTypeFromValues(values: string[]): string {
  // Filter out empty values
  const nonEmpty = values.filter(v => v !== '' && v !== null && v !== undefined)
  if (nonEmpty.length === 0) return 'String'

  // Check for booleans
  const allBooleans = nonEmpty.every(v =>
    v.toLowerCase() === 'true' || v.toLowerCase() === 'false'
  )
  if (allBooleans) return 'Boolean'

  // Check for integers
  const allIntegers = nonEmpty.every(v => /^-?\d+$/.test(v.trim()))
  if (allIntegers) return 'Int64'

  // Check for floats
  const allNumbers = nonEmpty.every(v => {
    const trimmed = v.trim()
    return !isNaN(parseFloat(trimmed)) && /^-?\d*\.?\d+$/.test(trimmed)
  })
  if (allNumbers) return 'Float64'

  return 'String'
}

/**
 * Parse CSV content and return the inferred schema
 * This is used for source nodes to provide immediate schema feedback
 */
export function inferSchemaFromCsv(
  csvContent: string,
  hasHeaders: boolean = true,
  delimiter: string = ','
): ColumnSchema[] | null {
  if (!csvContent || csvContent.trim().length === 0) {
    return null
  }

  const lines = csvContent.trim().split('\n')
  if (lines.length === 0) {
    return null
  }

  const actualDelimiter = delimiter === '\\t' ? '\t' : delimiter
  const schema: ColumnSchema[] = []

  if (hasHeaders) {
    const headerLine = lines[0]
    const columnNames = headerLine.split(actualDelimiter).map(c => c.trim())

    // Get sample data for type inference (up to 100 rows)
    const sampleLines = lines.slice(1, Math.min(101, lines.length))
    const sampleData: string[][] = sampleLines.map(line =>
      line.split(actualDelimiter).map(c => c.trim())
    )

    columnNames.forEach((name, colIndex) => {
      const columnValues = sampleData.map(row => row[colIndex] || '')
      const dataType = inferDataTypeFromValues(columnValues)

      schema.push({
        name: name || `column_${colIndex + 1}`,
        data_type: dataType
      })
    })
  } else {
    // No headers - infer column count from first line
    const firstLine = lines[0].split(actualDelimiter)
    const columnCount = firstLine.length

    // Get sample data for type inference
    const sampleLines = lines.slice(0, Math.min(100, lines.length))
    const sampleData: string[][] = sampleLines.map(line =>
      line.split(actualDelimiter).map(c => c.trim())
    )

    for (let colIndex = 0; colIndex < columnCount; colIndex++) {
      const columnValues = sampleData.map(row => row[colIndex] || '')
      const dataType = inferDataTypeFromValues(columnValues)

      schema.push({
        name: `column_${colIndex + 1}`,
        data_type: dataType
      })
    }
  }

  return schema.length > 0 ? schema : null
}

/**
 * Infer schema from RawData structure (for manual input nodes)
 */
export function inferSchemaFromRawData(
  fields: { name: string; data_type: string }[]
): ColumnSchema[] | null {
  if (!fields || fields.length === 0) {
    return null
  }

  return fields.map(field => ({
    name: field.name,
    data_type: field.data_type || 'String'
  }))
}
