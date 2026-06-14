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
  NodeFormulaSettings,
  NodeCrossJoinSettings,
  NodeUnionSettings,
  NodeRecordIdSettings,
  NodeRenameSettings,
  NodePivotSettings,
  NodeUnpivotSettings,
  NodeSampleSettings,
  NodeReadSettings,
  InputCsvTable,
  InputExcelTable,
  NodeManualInputSettings,
  NodeExternalDataSettings,
  NodeOutputSettings,
  NodeWriteToCatalogSettings,
  PolarsCodeSettings,
  FilterOperator,
  AggType
} from '../types'

interface CodeGenerationOptions {
  nodes: Map<number, FlowNode>
  edges: FlowEdge[]
  flowName?: string
}


interface JoinInputVars {
  main: string;
  right: string;
}
function isJoinInputVars(input: unknown): input is JoinInputVars {
  return (
    typeof input === 'object' &&
    input !== null &&
    'main' in input &&
    typeof (input as any).main === 'string' &&
    'right' in input &&
    typeof (input as any).right === 'string'
  );
}

function toPythonValue(value: any): string {
  if (typeof value === 'boolean') {
    return value ? 'True' : 'False'
  }
  if (Array.isArray(value)) {
    return '[' + value.map(toPythonValue).join(', ') + ']'
  }
  if (typeof value === 'string') {
    return JSON.stringify(value)
  }
  return String(value)
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
    const executionOrder = this.determineExecutionOrder()

    for (const nodeId of executionOrder) {
      const node = this.nodes.get(nodeId)
      if (node) {
        this.generateNodeCode(node)
      }
    }

    if (this.unsupportedNodes.length > 0) {
      const errorMessages = this.unsupportedNodes
        .map(n => `  - Node ${n.id} (${n.type}): ${n.reason}`)
        .join('\n')
      throw new Error(
        `The flow contains ${this.unsupportedNodes.length} node(s) that cannot be converted to code:\n${errorMessages}`
      )
    }

    return this.buildFinalCode()
  }

  private determineExecutionOrder(): number[] {
    const order: number[] = []
    const visited = new Set<number>()

    const adjacency = new Map<number, number[]>()
    const inDegree = new Map<number, number>()

    for (const [nodeId] of this.nodes) {
      adjacency.set(nodeId, [])
      inDegree.set(nodeId, 0)
    }

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

    if (order.length !== this.nodes.size) {
      throw new Error('Flow contains a cycle - cannot generate code')
    }

    return order
  }

  private generateNodeCode(node: FlowNode): void {
    const nodeReference = node.node_reference || (node.settings as any)?.node_reference
    const varName = nodeReference || `df_${node.id}`
    this.nodeVarMapping.set(node.id, varName)
    this.lastNodeVar = varName

    const inputVars = this.getInputVars(node)

    switch (node.type) {
      case 'read':
        this.handleReadCsv(node.settings as NodeReadSettings, varName)
        break
      case 'manual_input':
        this.handleManualInput(node.settings as NodeManualInputSettings, varName)
        break
      case 'external_data':
        this.handleExternalData(node.settings as NodeExternalDataSettings, varName)
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
        if (isJoinInputVars(inputVars)) {
          this.handleJoin(node.settings as NodeJoinSettings, varName, inputVars);
      } else {
          console.error("Input does not match interface JoinInputVars");
      }
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
      case 'formula':
        this.handleFormula(node.settings as NodeFormulaSettings, varName, inputVars)
        break
      case 'cross_join':
        if (isJoinInputVars(inputVars)) {
          this.handleCrossJoin(node.settings as NodeCrossJoinSettings, varName, inputVars)
        } else {
          console.error('Cross join input does not match interface JoinInputVars')
        }
        break
      case 'union':
        this.handleUnion(node.settings as NodeUnionSettings, varName, inputVars)
        break
      case 'record_id':
        this.handleRecordId(node.settings as NodeRecordIdSettings, varName, inputVars)
        break
      case 'rename':
        this.handleRename(node.settings as NodeRenameSettings, varName, inputVars)
        break
      case 'pivot':
        this.handlePivot(node.settings as NodePivotSettings, varName, inputVars)
        break
      case 'unpivot':
        this.handleUnpivot(node.settings as NodeUnpivotSettings, varName, inputVars)
        break
      case 'explore_data':
        this.handlePreview(varName, inputVars)
        break
      case 'output':
        this.handleOutput(node.settings as NodeOutputSettings, varName, inputVars)
        break
      case 'external_output':
        this.handleExternalOutput(varName, inputVars)
        break
      case 'write_to_catalog':
        this.handleWriteToCatalog(node.settings as NodeWriteToCatalogSettings, varName, inputVars)
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

  private getInputVars(node: FlowNode): Record<string, string> {
    const inputVars: Record<string, string> = {}

    // For join nodes: leftInputId maps to 'main', rightInputId maps to 'right'
    if (node.leftInputId !== undefined) {
      inputVars.main = this.nodeVarMapping.get(node.leftInputId) || 'df_left'
    }

    if (node.rightInputId !== undefined) {
      inputVars.right = this.nodeVarMapping.get(node.rightInputId) || 'df_right'
    }

    if (node.inputIds && node.inputIds.length > 0) {
      if (node.inputIds.length === 1) {
        inputVars.main = this.nodeVarMapping.get(node.inputIds[0]) || 'df'
      } else {
        for (let i = 0; i < node.inputIds.length; i++) {
          inputVars[`main_${i}`] = this.nodeVarMapping.get(node.inputIds[i]) || `df_${i}`
        }
      }
    }

    return inputVars
  }

  private handleReadCsv(settings: NodeReadSettings, varName: string): void {
    const table = settings.received_file
    const fileName = settings.file_name || table?.name || 'data.csv'
    // Files loaded from a URL keep it as received_file.path — generated code
    // reads from the same source instead of a file name that only existed in
    // the browser session. scan_csv/scan_parquet accept http(s) directly.
    const path = table?.path ?? ''
    const isRemote = path.startsWith('http://') || path.startsWith('https://')
    const source = isRemote ? path : fileName

    if (table?.file_type === 'excel') {
      const ts = table.table_settings as InputExcelTable
      this.addCode(`# requires: pip install fastexcel (polars' default excel engine)`)
      if (isRemote) {
        this.addCode(`from io import BytesIO`)
        this.addCode(`from urllib.request import urlopen`)
        this.addCode(`${varName} = pl.read_excel(`)
        this.addCode(`    BytesIO(urlopen("${source}").read()),`)
      } else {
        this.addCode(`${varName} = pl.read_excel(`)
        this.addCode(`    "${source}",`)
      }
      if (ts?.sheet_name) {
        this.addCode(`    sheet_name="${ts.sheet_name}",`)
      }
      this.addCode(`    has_header=${(ts?.has_headers ?? true) ? 'True' : 'False'},`)
      this.addCode(`).lazy()`)
      if (ts?.start_row) {
        this.addCode(`# note: this flow skips the first ${ts.start_row} row(s) before the header;`)
        this.addCode(`# adjust read_options for your engine if needed`)
      }
      this.addCode('')
      return
    }

    if (table?.file_type === 'parquet') {
      this.addCode(`${varName} = pl.scan_parquet("${source}")`)
      this.addCode('')
      return
    }

    const tableSettings = table?.table_settings as InputCsvTable | undefined

    this.addCode(`${varName} = pl.scan_csv(`)
    this.addCode(`    "${source}",`)

    if (tableSettings) {
      this.addCode(`    separator="${tableSettings.delimiter || ','}",`)
      this.addCode(`    has_header=${(tableSettings.has_headers ?? true) ? 'True' : 'False'},`)
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

  private handleExternalData(settings: NodeExternalDataSettings, varName: string): void {
    const datasetName = settings.dataset_name || 'external_data'
    const fileName = `${datasetName}.csv`

    this.addComment(`# External dataset: "${datasetName}"`)
    this.addComment(`# Replace the path below with the actual location of your "${datasetName}" data`)
    this.addCode(`${varName} = pl.scan_csv("${fileName}")`)
    this.addCode('')
  }

  private handleExternalOutput(varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    this.addComment(`# External output — collect result for downstream use`)
    this.addCode(`${varName} = ${inputDf}.collect()`)
    this.addCode('')
  }

  private handleWriteToCatalog(settings: NodeWriteToCatalogSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const name = (settings.dataset_name || '').trim() || 'catalog_table'
    // The browser Catalog has no standalone equivalent; the closest is writing
    // the table to a CSV file named after it.
    this.addComment(`# Write to Catalog table "${name}" (browser catalog → CSV file in standalone code)`)
    this.addCode(`${inputDf}.sink_csv("${name}.csv", separator=",")`)
    this.addCode(`${varName} = ${inputDf}  # Reference for potential downstream use`)
    this.addCode('')
  }

  private handleFilter(settings: NodeFilterSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const filterInput = settings.filter_input

    if (filterInput.mode === 'advanced') {
      this.addCode(`${varName} = ${inputDf}.filter(`)
      this.addCode(`    ${filterInput.advanced_filter}`)
      this.addCode(`)`)
    } else if (filterInput.basic_filter) {
      const filter = filterInput.basic_filter
      const filterExpr = this.createBasicFilterExpr(filter.field, filter.operator, filter.value, filter.value2)
      this.addCode(`${varName} = ${inputDf}.filter(${filterExpr})`)
    } else {
      this.addCode(`${varName} = ${inputDf}  # No filter applied`)
    }
    this.addCode('')
  }

  private createBasicFilterExpr(field: string, operator: FilterOperator, value: string, value2?: string): string {
    const col = `pl.col("${field}")`
    
    const formatValue = (v: string) => {
      if (/^-?\d+(\.\d+)?$/.test(v)) {
        return v
      }
      const lower = v.toLowerCase()
      if (lower === 'true') return 'True'
      if (lower === 'false') return 'False'
      return `"${v}"`
    }
  
    switch (operator) {
      case 'equals':
        return `${col} == ${formatValue(value)}`
      case 'not_equals':
        return `${col} != ${formatValue(value)}`
      case 'greater_than':
        return `${col} > ${formatValue(value)}`
      case 'greater_than_or_equals':
        return `${col} >= ${formatValue(value)}`
      case 'less_than':
        return `${col} < ${formatValue(value)}`
      case 'less_than_or_equals':
        return `${col} <= ${formatValue(value)}`
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
        const values = value.split(',').map(v => formatValue(v.trim())).join(', ')
        return `${col}.is_in([${values}])`
      case 'not_in':
        const notValues = value.split(',').map(v => formatValue(v.trim())).join(', ')
        return `~${col}.is_in([${notValues}])`
      case 'between':
        return `${col}.is_between(${formatValue(value)}, ${formatValue(value2 || value)})`
      default:
        return `${col} == ${formatValue(value)}`
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

  private handleJoin(settings: NodeJoinSettings, varName: string, inputVars: { main: string; right: string }): void {
    const leftDf = inputVars.main
    const rightDf = inputVars.right
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

  private handleCrossJoin(settings: NodeCrossJoinSettings, varName: string, inputVars: { main: string; right: string }): void {
    const leftDf = inputVars.main
    const rightDf = inputVars.right
    const suffix = settings.cross_join_input?.right_suffix || '_right'
    this.addCode(`${varName} = ${leftDf}.join(`)
    this.addCode(`    ${rightDf},`)
    this.addCode(`    how="cross",`)
    this.addCode(`    suffix=${toPythonValue(suffix)}`)
    this.addCode(`)`)
    this.addCode('')
  }

  private collectMainInputs(inputVars: Record<string, string>): string[] {
    return Object.keys(inputVars)
      .filter(k => k === 'main' || k.startsWith('main_'))
      .sort((a, b) => (a === 'main' ? -1 : parseInt(a.slice(5))) - (b === 'main' ? -1 : parseInt(b.slice(5))))
      .map(k => inputVars[k])
  }

  private handleUnion(settings: NodeUnionSettings, varName: string, inputVars: Record<string, string>): void {
    const dfs = this.collectMainInputs(inputVars)
    const how = (settings.union_input?.mode || 'diagonal') === 'vertical' ? 'vertical_relaxed' : 'diagonal_relaxed'
    if (dfs.length <= 1) {
      this.addCode(`${varName} = ${dfs[0] || 'df'}`)
      this.addCode('')
      return
    }
    this.addCode(`${varName} = pl.concat([${dfs.join(', ')}], how="${how}")`)
    this.addCode('')
  }

  private handleFormula(settings: NodeFormulaSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const fn = settings.function
    const name = fn?.field?.name || 'new_column'
    const expr = (fn?.function || '').trim()
    if (!expr) {
      this.addCode(`${varName} = ${inputDf}`)
      this.addCode('')
      return
    }
    const dtype = fn?.field?.data_type
    this.addCode(`# requires: pip install polars-expr-transformer`)
    this.addCode(`from polars_expr_transformer import simple_function_to_expr`)
    const cast = dtype && dtype !== 'Auto' ? `.cast(pl.${dtype})` : ''
    this.addCode(`${varName} = ${inputDf}.with_columns(`)
    this.addCode(`    simple_function_to_expr(${toPythonValue(expr)})${cast}.alias(${toPythonValue(name)})`)
    this.addCode(`)`)
    this.addCode('')
  }

  private handleRecordId(settings: NodeRecordIdSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const name = settings.record_id_input?.name || 'record_id'
    const offset = settings.record_id_input?.offset ?? 1
    this.addCode(`${varName} = ${inputDf}.with_row_index(name=${toPythonValue(name)}, offset=${offset})`)
    this.addCode('')
  }

  private handleRename(settings: NodeRenameSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const entries = (settings.rename_input || []).filter(r => r.old_name && r.new_name && r.old_name !== r.new_name)
    if (entries.length === 0) {
      this.addCode(`${varName} = ${inputDf}`)
      this.addCode('')
      return
    }
    const mapping = entries.map(r => `${JSON.stringify(r.old_name)}: ${JSON.stringify(r.new_name)}`).join(', ')
    this.addCode(`${varName} = ${inputDf}.rename({${mapping}})`)
    this.addCode('')
  }

  private handleSort(settings: NodeSortSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    // sort_input is now a flat array matching flowfile_core: [{column, how}]
    const sortInput = settings.sort_input || []
    const sortCols = sortInput.map(s => s.column)
    const descending = sortInput.map(s => s.how === 'desc')

    this.addCode(`${varName} = ${inputDf}.sort(`)
    this.addCode(`    ${JSON.stringify(sortCols)},`)
    this.addCode(`    descending=${toPythonValue(descending)}`)
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

  private handlePivot(settings: NodePivotSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const pivotInput = settings.pivot_input
    
    // Handle multiple aggregations - just take first (same as core)
    const aggFunc = pivotInput.aggregations?.[0] || 'first'
    
    if (pivotInput.index_columns.length === 0) {
      this.addCode(`${varName} = (${inputDf}.collect()`)
      this.addCode(`    .with_columns(pl.lit(1).alias("__temp_index__"))`)
      this.addCode(`    .pivot(`)
      this.addCode(`        values="${pivotInput.value_col}",`)
      this.addCode(`        index=["__temp_index__"],`)
      this.addCode(`        columns="${pivotInput.pivot_column}",`)
      this.addCode(`        aggregate_function="${aggFunc}"`)
      this.addCode(`    )`)
      this.addCode(`    .drop("__temp_index__")`)
      this.addCode(`).lazy()`)
    } else {
      this.addCode(`${varName} = ${inputDf}.collect().pivot(`)
      this.addCode(`    values="${pivotInput.value_col}",`)
      this.addCode(`    index=${JSON.stringify(pivotInput.index_columns)},`)
      this.addCode(`    columns="${pivotInput.pivot_column}",`)
      this.addCode(`    aggregate_function="${aggFunc}"`)
      this.addCode(`).lazy()`)
    }
    this.addCode('')
  }

        private handleUnpivot(settings: NodeUnpivotSettings, varName: string, inputVars: { main?: string }): void {
          const inputDf = inputVars.main || 'df'
          const unpivotInput = settings.unpivot_input

      this.addCode(`${varName} = ${inputDf}.unpivot(`)

      if (unpivotInput.index_columns?.length > 0) {
        this.addCode(`    index=${JSON.stringify(unpivotInput.index_columns)},`)
      }

      if (unpivotInput.data_type_selector_mode === 'data_type' && unpivotInput.data_type_selector) {
        this.imports.add('import polars.selectors as cs')

        const selectorMap: Record<string, string> = {
          'numeric': 'cs.numeric()',
          'string': 'cs.string()',
          'float': 'cs.float()',
          'date': 'cs.temporal()',  // Note: 'date' maps to temporal() in Polars
          'all': 'cs.all()'
        }

        const selector = selectorMap[unpivotInput.data_type_selector] || 'cs.all()'

        this.addCode(`    on=${selector},`)
      } else if (unpivotInput.value_columns?.length > 0) {
        this.addCode(`    on=${JSON.stringify(unpivotInput.value_columns)},`)
      }

      this.addCode(`    variable_name="variable",`)
      this.addCode(`    value_name="value"`)
      this.addCode(`)`)
      this.addCode('')
    }

  private handleSample(settings: NodeSampleSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const n = settings.sample_size || 10

    this.addCode(`${varName} = ${inputDf}.head(${n})`)
    this.addCode('')
  }

  private handlePolarsCode(settings: PolarsCodeSettings, varName: string, inputVars: { main?: string; left?: string; right?: string }): void {
    const code = (settings.polars_code_input?.polars_code || '').trim()
    
    let params: string
    let args: string
    const inputKeys = Object.keys(inputVars)
    if (inputKeys.length === 0) {
      params = ''
      args = ''
    } else if (inputKeys.length === 1) {
      params = 'input_df: pl.LazyFrame'
      args = Object.values(inputVars)[0] || 'df'
    } else {
      const paramList: string[] = []
      const argList: string[] = []
      let i = 1
      for (const key of Object.keys(inputVars).sort()) {
        if (key.startsWith('main')) {
          paramList.push(`input_df_${i}: pl.LazyFrame`)
          argList.push(inputVars[key as keyof typeof inputVars] || `df_${i}`)
          i++
        }
      }
      params = paramList.join(', ')
      args = argList.join(', ')
    }
  
    this.addCode('# Custom Polars code')
    this.addCode(`def _polars_code_${varName.replace('df_', '')}(${params}):`)
  
    const hasOutputDf = /\boutput_df\s*=/.test(code)

    if (hasOutputDf) {
      for (const line of code.split('\n')) {
        if (line.trim()) {
          this.addCode(`    ${line}`)
        }
      }
      if (!code.trim().endsWith('return output_df')) {
        this.addCode(`    return output_df`)
      }

    } else {
      const isSingleLine = code.split('\n').filter(l => l.trim()).length === 1
      const isAssignment = code.includes('=')
      const hasReturn = code.includes('return')

      if (isSingleLine && !isAssignment && !hasReturn) {
        this.addCode(`    return ${code}`)
      } else {
        for (const line of code.split('\n')) {
          if (line.trim()) {
            this.addCode(`    ${line}`)
          }
        }
        if (!hasReturn) {
          const lines = code.split('\n').map(l => l.trim()).filter(l => l && l.includes('='))
          if (lines.length > 0) {
            const lastAssignment = lines[lines.length - 1]
            const outputVar = lastAssignment.split('=')[0].trim()
            // Basic validity check to ensure we don't return "df['col']" or similar
            if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(outputVar)) {
              this.addCode(`    return ${outputVar}`)
            }
          }
        }
      }
    }

    this.addCode('')

    this.addCode(`${varName} = _polars_code_${varName.replace('df_', '')}(${args})`)
    this.addCode('')
  }

  private handlePreview(varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    this.addCode(`${varName} = ${inputDf}  # Preview (pass-through)`)
    this.addCode('')
  }

  private handleOutput(settings: NodeOutputSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'

    // Handle both nested format (from UI) and flat format (from YAML import)
    // UI format: settings.output_settings.name, settings.output_settings.file_type, etc.
    // YAML format: settings.file_name, settings.file_type, settings.output_table, etc.
    const outputSettings = settings.output_settings
    const anySettings = settings as any

    const fileName = outputSettings?.name || anySettings.file_name || 'output.csv'
    const fileType = outputSettings?.file_type || anySettings.file_type || 'csv'
    const tableSettings = outputSettings?.table_settings || anySettings.output_table
    // file_type is authoritative: settings saved by older builds carry a stale
    // polars_method ('sink_csv' on parquet nodes)
    const polarsMethod = fileType === 'parquet' ? 'sink_parquet' : 'sink_csv'

    if (!outputSettings && !anySettings.file_name && !anySettings.file_type) {
      this.addComment(`# Output node ${varName} has no settings configured`)
      this.addCode(`${varName} = ${inputDf}`)
      this.addCode('')
      return
    }

    if (fileType === 'excel') {
      const sheetName = (tableSettings && 'sheet_name' in tableSettings && tableSettings.sheet_name) || 'Sheet1'
      this.addComment(`# Write output to ${fileName} (requires: pip install xlsxwriter)`)
      this.addCode(`${inputDf}.collect().write_excel("${fileName}", worksheet="${sheetName}")`)
      this.addCode(`${varName} = ${inputDf}  # Reference for potential downstream use`)
      this.addCode('')
      return
    }

    this.addComment(`# Write output to ${fileName} using ${polarsMethod}`)

    if (polarsMethod === 'sink_parquet') {
      this.addCode(`${inputDf}.sink_parquet("${fileName}")`)
    } else {
      const delimiter = (tableSettings && 'delimiter' in tableSettings) ? tableSettings.delimiter : ','

      this.addCode(`${inputDf}.sink_csv(`)
      this.addCode(`    "${fileName}",`)
      this.addCode(`    separator="${delimiter}"`)
      this.addCode(`)`)
    }

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

    lines.push(...Array.from(this.imports).sort())
    lines.push('')
    lines.push('')

    lines.push('def run_etl_pipeline():')
    lines.push('    """')
    lines.push(`    ETL Pipeline: ${this.flowName}`)
    lines.push('    Generated from Flowfile WASM')
    lines.push('    """')
    lines.push('    ')

    for (const line of this.codeLines) {
      if (line) {
        lines.push(`    ${line}`)
      } else {
        lines.push('')
      }
    }

    lines.push('')
    if (this.lastNodeVar) {
      lines.push(`    return ${this.lastNodeVar}`)
    } else {
      lines.push('    return None')
    }

    lines.push('')
    lines.push('')

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
