/**
 * Code Generation Composable for WASM
 * Generates standalone Python/Polars code from flow graphs
 */

import type {
  FlowNode,
  FlowEdge,
  NodeFilterSettings,
  NodeSelectSettings,
  NodeGroupBySettings,
  NodeJoinSettings,
  NodeSortSettings,
  NodeUniqueSettings,
  NodeSampleSettings,
  NodeReadSettings,
  NodeManualInputSettings,
  NodeOutputSettings,
  PolarsCodeSettings,
  FilterOperator,
  AggType
} from '../types'

interface CodeGenerationOptions {
  nodes: Map<number, FlowNode>
  edges: FlowEdge[]
  flowName?: string
}

class FlowToPolarsConverter {
  private nodes: Map<number, FlowNode>
  private edges: FlowEdge[]
  private flowName: string
  private nodeVarMapping: Map<number, string>
  private imports: Set<string>
  private codeLines: string[]
  private lastNodeVar: string | null
  private unsupportedNodes: Array<{ id: number; type: string; reason: string }>

  constructor(options: CodeGenerationOptions) {
    this.nodes = options.nodes
    this.edges = options.edges
    this.flowName = options.flowName || 'Flow Pipeline'
    this.nodeVarMapping = new Map()
    this.imports = new Set(['import polars as pl'])
    this.codeLines = []
    this.lastNodeVar = null
    this.unsupportedNodes = []
  }

  convert(): string {
    // Get execution order (topological sort)
    const executionOrder = this.determineExecutionOrder()

    // Generate code for each node
    for (const nodeId of executionOrder) {
      const node = this.nodes.get(nodeId)
      if (node) {
        this.generateNodeCode(node)
      }
    }

    // Check for unsupported nodes
    if (this.unsupportedNodes.length > 0) {
      const errorMessages = this.unsupportedNodes
        .map(n => `  - Node ${n.id} (${n.type}): ${n.reason}`)
        .join('\n')
      throw new Error(
        `The flow contains ${this.unsupportedNodes.length} node(s) that cannot be converted to code:\n${errorMessages}`
      )
    }

    // Build final code
    return this.buildFinalCode()
  }

  private determineExecutionOrder(): number[] {
    const order: number[] = []
    const visited = new Set<number>()

    // Build adjacency list
    const adjacency = new Map<number, number[]>()
    const inDegree = new Map<number, number>()

    // Initialize
    for (const [nodeId] of this.nodes) {
      adjacency.set(nodeId, [])
      inDegree.set(nodeId, 0)
    }

    // Build graph from edges
    for (const edge of this.edges) {
      const from = parseInt(edge.source)
      const to = parseInt(edge.target)
      adjacency.get(from)?.push(to)
      inDegree.set(to, (inDegree.get(to) || 0) + 1)
    }

    // Kahn's algorithm for topological sort
    const queue: number[] = []
    for (const [nodeId, degree] of inDegree.entries()) {
      if (degree === 0) {
        queue.push(nodeId)
      }
    }

    while (queue.length > 0) {
      const nodeId = queue.shift()!
      order.push(nodeId)
      visited.add(nodeId)

      const neighbors = adjacency.get(nodeId) || []
      for (const neighbor of neighbors) {
        const newDegree = (inDegree.get(neighbor) || 0) - 1
        inDegree.set(neighbor, newDegree)
        if (newDegree === 0) {
          queue.push(neighbor)
        }
      }
    }

    // Check for cycles
    if (order.length !== this.nodes.size) {
      throw new Error('Flow contains a cycle - cannot generate code')
    }

    return order
  }

  private generateNodeCode(node: FlowNode): void {
    const varName = `df_${node.id}`
    this.nodeVarMapping.set(node.id, varName)
    this.lastNodeVar = varName

    // Get input variables
    const inputVars = this.getInputVars(node)

    // Route to appropriate handler
    switch (node.type) {
      case 'read_csv':
        this.handleReadCsv(node.settings as NodeReadSettings, varName)
        break
      case 'manual_input':
        this.handleManualInput(node.settings as NodeManualInputSettings, varName)
        break
      case 'filter':
        this.handleFilter(node.settings as NodeFilterSettings, varName, inputVars)
        break
      case 'select':
        this.handleSelect(node.settings as NodeSelectSettings, varName, inputVars)
        break
      case 'group_by':
        this.handleGroupBy(node.settings as NodeGroupBySettings, varName, inputVars)
        break
      case 'join':
        this.handleJoin(node.settings as NodeJoinSettings, varName, inputVars)
        break
      case 'sort':
        this.handleSort(node.settings as NodeSortSettings, varName, inputVars)
        break
      case 'unique':
        this.handleUnique(node.settings as NodeUniqueSettings, varName, inputVars)
        break
      case 'sample':
      case 'head':
        this.handleSample(node.settings as NodeSampleSettings, varName, inputVars)
        break
      case 'polars_code':
        this.handlePolarsCode(node.settings as PolarsCodeSettings, varName, inputVars)
        break
      case 'preview':
        // Preview is a pass-through node
        this.handlePreview(varName, inputVars)
        break
      case 'output':
        this.handleOutput(node.settings as NodeOutputSettings, varName, inputVars)
        break
      default:
        this.unsupportedNodes.push({
          id: node.id,
          type: node.type,
          reason: `Node type '${node.type}' not supported for code generation`
        })
        this.addComment(`# WARNING: Cannot generate code for node type '${node.type}' (node_id=${node.id})`)
    }
  }

  private getInputVars(node: FlowNode): { main?: string; left?: string; right?: string } {
    const inputVars: { main?: string; left?: string; right?: string } = {}

    if (node.leftInputId !== undefined) {
      inputVars.left = this.nodeVarMapping.get(node.leftInputId) || 'df_left'
    }

    if (node.rightInputId !== undefined) {
      inputVars.right = this.nodeVarMapping.get(node.rightInputId) || 'df_right'
    }

    if (node.inputIds && node.inputIds.length > 0) {
      inputVars.main = this.nodeVarMapping.get(node.inputIds[0]) || 'df'
    }

    return inputVars
  }

  private handleReadCsv(settings: NodeReadSettings, varName: string): void {
    const table = settings.received_table
    const fileName = settings.file_name || table?.name || 'data.csv'
    const tableSettings = table?.table_settings

    this.addCode(`${varName} = pl.scan_csv(`)
    this.addCode(`    "${fileName}",`)

    if (tableSettings) {
      this.addCode(`    separator="${tableSettings.delimiter || ','}",`)
      this.addCode(`    has_header=${tableSettings.has_headers ?? true},`)
      if (tableSettings.starting_from_line) {
        this.addCode(`    skip_rows=${tableSettings.starting_from_line},`)
      }
      if (tableSettings.encoding && tableSettings.encoding.toLowerCase() !== 'utf-8') {
        this.addCode(`    encoding="${tableSettings.encoding}",`)
      }
      if (tableSettings.ignore_errors) {
        this.addCode(`    ignore_errors=True,`)
      }
    } else {
      this.addCode(`    separator=",",`)
      this.addCode(`    has_header=True,`)
    }

    this.addCode(`)`)
    this.addCode('')
  }

  private handleManualInput(settings: NodeManualInputSettings, varName: string): void {
    const rawData = settings.raw_data_format
    if (!rawData) {
      this.addComment(`# Manual input node ${varName} has no data`)
      this.addCode(`${varName} = pl.LazyFrame()`)
      this.addCode('')
      return
    }

    // Generate dictionary for DataFrame creation
    const dataDict: Record<string, any[]> = {}
    for (let i = 0; i < rawData.columns.length; i++) {
      const col = rawData.columns[i]
      dataDict[col.name] = rawData.data.map(row => row[i])
    }

    this.addCode(`${varName} = pl.LazyFrame({`)
    for (const [colName, values] of Object.entries(dataDict)) {
      this.addCode(`    "${colName}": ${JSON.stringify(values)},`)
    }
    this.addCode(`})`)
    this.addCode('')
  }

  private handleFilter(settings: NodeFilterSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const filterInput = settings.filter_input

    if (filterInput.mode === 'advanced') {
      // Advanced filter with Polars expression
      this.addCode(`${varName} = ${inputDf}.filter(`)
      this.addCode(`    ${filterInput.advanced_filter}`)
      this.addCode(`)`)
    } else if (filterInput.basic_filter) {
      // Basic filter
      const filter = filterInput.basic_filter
      const filterExpr = this.createBasicFilterExpr(filter.field, filter.operator, filter.value, filter.value2)
      this.addCode(`${varName} = ${inputDf}.filter(${filterExpr})`)
    } else {
      // No filter
      this.addCode(`${varName} = ${inputDf}  # No filter applied`)
    }
    this.addCode('')
  }

  private createBasicFilterExpr(field: string, operator: FilterOperator, value: string, value2?: string): string {
    const col = `pl.col("${field}")`

    switch (operator) {
      case 'equals':
        return `${col} == "${value}"`
      case 'not_equals':
        return `${col} != "${value}"`
      case 'greater_than':
        return `${col} > ${value}`
      case 'greater_than_or_equals':
        return `${col} >= ${value}`
      case 'less_than':
        return `${col} < ${value}`
      case 'less_than_or_equals':
        return `${col} <= ${value}`
      case 'contains':
        return `${col}.str.contains("${value}")`
      case 'not_contains':
        return `~${col}.str.contains("${value}")`
      case 'starts_with':
        return `${col}.str.starts_with("${value}")`
      case 'ends_with':
        return `${col}.str.ends_with("${value}")`
      case 'is_null':
        return `${col}.is_null()`
      case 'is_not_null':
        return `${col}.is_not_null()`
      case 'in':
        const values = value.split(',').map(v => `"${v.trim()}"`).join(', ')
        return `${col}.is_in([${values}])`
      case 'not_in':
        const notValues = value.split(',').map(v => `"${v.trim()}"`).join(', ')
        return `~${col}.is_in([${notValues}])`
      case 'between':
        return `${col}.is_between(${value}, ${value2 || value})`
      default:
        return `${col} == "${value}"`
    }
  }

  private handleSelect(settings: NodeSelectSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const selectExprs: string[] = []

    for (const col of settings.select_input) {
      if (col.keep && col.is_available !== false) {
        let expr = `pl.col("${col.old_name}")`

        if (col.old_name !== col.new_name) {
          expr += `.alias("${col.new_name}")`
        }

        if (col.data_type_change && col.data_type) {
          const polarsType = this.getPolarsType(col.data_type)
          expr += `.cast(${polarsType})`
        }

        selectExprs.push(expr)
      }
    }

    if (selectExprs.length > 0) {
      this.addCode(`${varName} = ${inputDf}.select([`)
      for (const expr of selectExprs) {
        this.addCode(`    ${expr},`)
      }
      this.addCode(`])`)
    } else {
      this.addCode(`${varName} = ${inputDf}`)
    }
    this.addCode('')
  }

  private handleGroupBy(settings: NodeGroupBySettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const groupCols: string[] = []
    const aggExprs: string[] = []

    for (const aggCol of settings.groupby_input.agg_cols) {
      if (aggCol.agg === 'groupby') {
        groupCols.push(aggCol.old_name)
      } else {
        const aggFunc = this.getAggFunction(aggCol.agg)
        aggExprs.push(`pl.col("${aggCol.old_name}").${aggFunc}().alias("${aggCol.new_name}")`)
      }
    }

    this.addCode(`${varName} = ${inputDf}.group_by(${JSON.stringify(groupCols)}).agg([`)
    for (const expr of aggExprs) {
      this.addCode(`    ${expr},`)
    }
    this.addCode(`])`)
    this.addCode('')
  }

  private getAggFunction(agg: AggType): string {
    const mapping: Record<AggType, string> = {
      'groupby': '',
      'sum': 'sum',
      'max': 'max',
      'min': 'min',
      'count': 'count',
      'mean': 'mean',
      'median': 'median',
      'first': 'first',
      'last': 'last',
      'n_unique': 'n_unique',
      'concat': 'str.concat'
    }
    return mapping[agg] || 'sum'
  }

  private handleJoin(settings: NodeJoinSettings, varName: string, inputVars: { left?: string; right?: string }): void {
    const leftDf = inputVars.left || 'df_left'
    const rightDf = inputVars.right || 'df_right'
    const joinInput = settings.join_input

    const leftOn = joinInput.join_mapping.map(jm => jm.left_col)
    const rightOn = joinInput.join_mapping.map(jm => jm.right_col)

    this.addCode(`${varName} = ${leftDf}.join(`)
    this.addCode(`    ${rightDf},`)
    this.addCode(`    left_on=${JSON.stringify(leftOn)},`)
    this.addCode(`    right_on=${JSON.stringify(rightOn)},`)
    this.addCode(`    how="${joinInput.how}"`)
    this.addCode(`)`)
    this.addCode('')
  }

  private handleSort(settings: NodeSortSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const sortCols = settings.sort_input.map(s => s.column)
    const descending = settings.sort_input.map(s => s.how === 'desc')

    this.addCode(`${varName} = ${inputDf}.sort(`)
    this.addCode(`    ${JSON.stringify(sortCols)},`)
    this.addCode(`    descending=${JSON.stringify(descending)}`)
    this.addCode(`)`)
    this.addCode('')
  }

  private handleUnique(settings: NodeUniqueSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const uniqueInput = settings.unique_input

    if (uniqueInput.columns && uniqueInput.columns.length > 0) {
      this.addCode(`${varName} = ${inputDf}.unique(`)
      this.addCode(`    subset=${JSON.stringify(uniqueInput.columns)},`)
      this.addCode(`    keep="${uniqueInput.strategy}"`)
      this.addCode(`)`)
    } else {
      this.addCode(`${varName} = ${inputDf}.unique(keep="${uniqueInput.strategy}")`)
    }
    this.addCode('')
  }

  private handleSample(settings: NodeSampleSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const n = settings.sample_size || 10

    this.addCode(`${varName} = ${inputDf}.head(${n})`)
    this.addCode('')
  }

  private handlePolarsCode(settings: PolarsCodeSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const code = settings.polars_code_input?.polars_code || ''

    // Replace 'df' with actual input variable name
    const processedCode = code.replace(/\bdf\b/g, inputDf)

    this.addCode(`# Custom Polars code`)
    this.addCode(`${varName} = ${processedCode}`)
    this.addCode('')
  }

  private handlePreview(varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    this.addCode(`${varName} = ${inputDf}  # Preview (pass-through)`)
    this.addCode('')
  }

  private handleOutput(settings: NodeOutputSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const outputSettings = settings.output_settings

    if (!outputSettings) {
      this.addComment(`# Output node ${varName} has no settings configured`)
      this.addCode(`${varName} = ${inputDf}`)
      this.addCode('')
      return
    }

    const fileName = outputSettings.name || 'output.csv'
    const fileType = outputSettings.file_type || 'csv'
    const tableSettings = outputSettings.table_settings

    this.addComment(`# Write output to ${fileName}`)

    if (fileType === 'parquet') {
      // For parquet, use sink_parquet for lazy evaluation
      this.addCode(`${inputDf}.sink_parquet("${fileName}")`)
    } else {
      // For CSV, use sink_csv with options
      const delimiter = (tableSettings && 'delimiter' in tableSettings) ? tableSettings.delimiter : ','

      this.addCode(`${inputDf}.sink_csv(`)
      this.addCode(`    "${fileName}",`)
      this.addCode(`    separator="${delimiter}"`)
      this.addCode(`)`)
    }

    // Assign the input to varName so downstream code can reference it if needed
    this.addCode(`${varName} = ${inputDf}  # Reference for potential downstream use`)
    this.addCode('')
  }

  private getPolarsType(dataType: string): string {
    const mapping: Record<string, string> = {
      'String': 'pl.Utf8',
      'Integer': 'pl.Int64',
      'Float': 'pl.Float64',
      'Boolean': 'pl.Boolean',
      'Date': 'pl.Date',
      'Datetime': 'pl.Datetime',
      'Int32': 'pl.Int32',
      'Int64': 'pl.Int64',
      'Float32': 'pl.Float32',
      'Float64': 'pl.Float64',
      'Utf8': 'pl.Utf8'
    }
    return mapping[dataType] || 'pl.Utf8'
  }

  private addCode(line: string): void {
    this.codeLines.push(line)
  }

  private addComment(comment: string): void {
    this.codeLines.push(comment)
  }

  private buildFinalCode(): string {
    const lines: string[] = []

    // Add imports
    lines.push(...Array.from(this.imports).sort())
    lines.push('')
    lines.push('')

    // Add main function
    lines.push('def run_etl_pipeline():')
    lines.push('    """')
    lines.push(`    ETL Pipeline: ${this.flowName}`)
    lines.push('    Generated from Flowfile WASM')
    lines.push('    """')
    lines.push('    ')

    // Add generated code (indented)
    for (const line of this.codeLines) {
      if (line) {
        lines.push(`    ${line}`)
      } else {
        lines.push('')
      }
    }

    // Add return statement
    lines.push('')
    if (this.lastNodeVar) {
      lines.push(`    return ${this.lastNodeVar}`)
    } else {
      lines.push('    return None')
    }

    lines.push('')
    lines.push('')

    // Add main block
    lines.push('if __name__ == "__main__":')
    lines.push('    pipeline_output = run_etl_pipeline()')
    lines.push('    print(pipeline_output.collect())')

    return lines.join('\n')
  }
}

export function useCodeGeneration() {
  const generateCode = (options: CodeGenerationOptions): string => {
    try {
      const converter = new FlowToPolarsConverter(options)
      return converter.convert()
    } catch (error) {
      if (error instanceof Error) {
        throw error
      }
      throw new Error('Failed to generate code')
    }
  }

  return {
    generateCode
  }
}
