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
  NodeDynamicRenameSettings,
  DynamicRenameInput,
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
  NodeReadFromCatalogSettings,
  PolarsCodeSettings,
  FilterOperator,
  AggType
} from '../types'
import { selectCastTarget } from '../stores/schema-inference'

export interface CodeGenerationOptions {
  nodes: Map<number, FlowNode>
  edges: FlowEdge[]
  flowName?: string
  // Formula node id → translated Polars expression code (to_polars_code).
  formulaCode?: Record<number, string>
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

// Mirrors _str_to_dtype in the engine: match on the base name, else String.
const POLARS_DTYPES = new Set([
  'Int8', 'Int16', 'Int32', 'Int64', 'Int128',
  'UInt8', 'UInt16', 'UInt32', 'UInt64',
  'Float32', 'Float64',
  'Boolean', 'String', 'Utf8', 'Categorical', 'Date', 'Time', 'Datetime', 'Duration', 'Null'
])

export function polarsDtype(name: string | undefined): string {
  const base = (name || '').split('(')[0]
  return POLARS_DTYPES.has(base) ? `pl.${base}` : 'pl.String'
}

export function toPythonValue(value: any): string {
  if (value === null || value === undefined) {
    return 'None'
  }
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

// --- Chain fusion (ported from flowfile_core code_generator/chain_fusion.py) ---
// Collapse linear runs of single-use nodes into a single piped expression, keeping
// named variables only where the graph branches (fan-out) or merges (join/union).

interface NodeEmission {
  nodeId: number
  varName: string
  lines: string[] // non-empty code lines for this node (no trailing blanks)
  mainProducerId: number | null // sole input node id, or null for sources / multi-input
  numInputs: number // number of distinct (resolved) input nodes
  pinned: boolean // user named the node (node_reference) -> keep it a variable
}

const ASSIGNMENT_RE = /^[A-Za-z_]\w* = /

function assignmentCount(lines: string[]): number {
  return lines.filter(line => ASSIGNMENT_RE.test(line)).length
}

// True when the node is exactly one top-level assignment to its own var.
function isSimpleEmission(em: NodeEmission): boolean {
  return em.lines.length > 0 && em.lines[0].startsWith(`${em.varName} = `) && assignmentCount(em.lines) === 1
}

// Remove the common leading indentation shared by every non-blank line.
function dedentLines(lines: string[]): string[] {
  let common = Infinity
  for (const line of lines) {
    if (!line.trim()) continue
    const indent = line.length - line.replace(/^\s+/, '').length
    common = Math.min(common, indent)
  }
  if (!isFinite(common) || common === 0) return lines
  return lines.map(line => (line.trim() ? line.slice(common) : line))
}

// Method-chain lines of `consumer` applied to `producerVar`, or null when not fusable.
function linkSuffix(consumer: NodeEmission, producerVar: string): string[] | null {
  if (consumer.lines.length === 0) return null
  const head = consumer.lines[0]
  const direct = `${consumer.varName} = ${producerVar}`
  const paren = `${consumer.varName} = (${producerVar}`
  let suffix: string[]
  if (head.startsWith(direct + '.')) {
    suffix = [head.slice(direct.length), ...consumer.lines.slice(1)]
  } else if (head === paren) {
    const body = consumer.lines.slice(1)
    if (body.length === 0 || body[body.length - 1].trimEnd() !== ')') return null
    suffix = dedentLines(body.slice(0, -1))
  } else {
    return null
  }
  // Not fusable when producerVar is referenced anywhere but the chain base
  // (e.g. `... for c in producer.columns`), since fusing drops that variable.
  const token = new RegExp(`\\b${producerVar.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`)
  if (suffix.some(line => token.test(line))) return null
  return suffix
}

// The right-hand side of a chain-root statement (`var = ` stripped).
function rootRhs(em: NodeEmission): string[] {
  const head = em.lines[0]
  return [head.slice(`${em.varName} = `.length), ...em.lines.slice(1)]
}

function indentLine(line: string): string {
  return line.trim() ? `    ${line}` : ''
}

// Render a producer->consumer chain as one piped expression (or null to bail).
function renderChain(chain: NodeEmission[]): string[] | null {
  if (chain.length === 1) return [...chain[0].lines]
  const out = [`${chain[chain.length - 1].varName} = (`]
  for (const line of rootRhs(chain[0])) out.push(indentLine(line))
  for (let i = 0; i < chain.length - 1; i++) {
    const suffix = linkSuffix(chain[i + 1], chain[i].varName)
    if (suffix === null) return null
    for (const line of suffix) out.push(indentLine(line))
  }
  out.push(')')
  return out
}

// Render all node statements, fusing linear single-use chains into pipes.
// Returns [bodyLines, survivingBoundaryNodeIds].
function renderPipeline(
  emissions: NodeEmission[],
  consumers: Map<number, number[]>
): [string[], Set<number>] {
  const byId = new Map(emissions.map(em => [em.nodeId, em]))
  const simple = new Map(emissions.map(em => [em.nodeId, isSimpleEmission(em)]))

  const absorbedInto = new Map<number, number>()
  for (const producer of emissions) {
    if (!simple.get(producer.nodeId) || producer.numInputs > 1) continue
    if (producer.pinned) continue // user named it; keep it as its own variable
    const consuming = consumers.get(producer.nodeId) || []
    if (consuming.length !== 1) continue
    const consumer = byId.get(consuming[0])
    if (!consumer || !simple.get(consumer.nodeId)) continue
    if (consumer.numInputs !== 1 || consumer.mainProducerId !== producer.nodeId) continue
    if (linkSuffix(consumer, producer.varName) === null) continue
    absorbedInto.set(producer.nodeId, consumer.nodeId)
  }

  const rendered: string[] = []
  const survivors = new Set<number>()
  for (const em of emissions) {
    if (absorbedInto.has(em.nodeId)) continue // emitted as part of its consumer's chain
    const chain = [em]
    let cur = em
    while (cur.mainProducerId !== null && absorbedInto.get(cur.mainProducerId) === cur.nodeId) {
      cur = byId.get(cur.mainProducerId)!
      chain.unshift(cur)
    }
    let block = renderChain(chain)
    if (block === null) block = chain.flatMap(node => node.lines) // defensive fallback
    survivors.add(em.nodeId)
    if (rendered.length > 0) rendered.push('')
    rendered.push(...block)
  }
  return [rendered, survivors]
}

// Operation-based labels for the boundary variables that survive chain fusion.
const NODE_TYPE_VAR_LABEL: Record<string, string> = {
  read: 'source',
  manual_input: 'source',
  external_data: 'source',
  read_from_catalog: 'source',
  filter: 'filtered',
  formula: 'computed',
  select: 'selected',
  dynamic_rename: 'renamed',
  sort: 'ordered',
  group_by: 'grouped',
  pivot: 'pivoted',
  unpivot: 'unpivoted',
  join: 'joined',
  cross_join: 'joined',
  union: 'combined',
  unique: 'deduped',
  record_id: 'with_record_id',
  sample: 'sampled',
  head: 'sampled',
  polars_code: 'transformed'
}

// Python keywords + common builtins that generated names must never shadow.
// Exported for the plain-Python flavour, whose loop temporaries obey the same rule.
export const RESERVED_NAMES = new Set<string>([
  'False', 'None', 'True', 'and', 'as', 'assert', 'async', 'await', 'break', 'class',
  'continue', 'def', 'del', 'elif', 'else', 'except', 'finally', 'for', 'from', 'global',
  'if', 'import', 'in', 'is', 'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return',
  'try', 'while', 'with', 'yield',
  'abs', 'all', 'any', 'bool', 'bytes', 'dict', 'filter', 'float', 'format', 'frozenset',
  'getattr', 'hash', 'id', 'input', 'int', 'iter', 'len', 'list', 'map', 'max', 'min',
  'next', 'object', 'open', 'ord', 'print', 'range', 'repr', 'reversed', 'round', 'set',
  'slice', 'sorted', 'str', 'sum', 'tuple', 'type', 'vars', 'zip'
])

// Members are `protected` rather than `private` because usePlainPythonGeneration.ts
// subclasses this to emit the teaching flavour; see that file for the seam it uses.
export class FlowToPolarsConverter {
  protected nodes: Map<number, FlowNode>
  private edges: FlowEdge[]
  protected flowName: string
  protected nodeVarMapping: Map<number, string>
  protected imports: Set<string>
  protected codeLines: string[]
  protected lastNodeVar: string | null
  private unsupportedNodes: Array<{ id: number; type: string; reason: string }>
  private formulaCode: Record<number, string>
  // (node, effectiveVar, start, end) per emitting node; (start, end) slices codeLines.
  protected nodeSpans: Array<{ node: FlowNode; effectiveVar: string; start: number; end: number }>
  // node_id -> upstream node_id for nodes that emit nothing (passthroughs).
  private passthrough: Map<number, number>
  private currentNodeId: number
  // Final names for every emitting node (fused-away ones included); set by renderBody().
  private explainRename: Map<string, string> | null

  constructor(options: CodeGenerationOptions) {
    this.nodes = options.nodes
    this.edges = options.edges
    this.flowName = options.flowName || 'Flow Pipeline'
    this.formulaCode = options.formulaCode || {}
    this.nodeVarMapping = new Map()
    this.imports = new Set(['import polars as pl'])
    this.codeLines = []
    this.lastNodeVar = null
    this.unsupportedNodes = []
    this.nodeSpans = []
    this.passthrough = new Map()
    this.currentNodeId = -1
    this.explainRename = null
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
    this.currentNodeId = node.id

    const inputVars = this.getInputVars(node)

    const start = this.codeLines.length
    this.dispatchNodeCode(node, varName, inputVars)
    const end = this.codeLines.length

    // A handler may have remapped the node to a passthrough (e.g. empty select,
    // explore_data); read the effective var back so downstream/return honour it.
    const effectiveVar = this.nodeVarMapping.get(node.id) || varName
    this.lastNodeVar = effectiveVar

    if (end === start) {
      // Handler emitted nothing -> passthrough to its sole main input.
      const mainInputs = node.inputIds || []
      if (mainInputs.length === 1) {
        this.passthrough.set(node.id, mainInputs[0])
      }
    } else {
      this.nodeSpans.push({ node, effectiveVar, start, end })
    }
  }

  protected dispatchNodeCode(node: FlowNode, varName: string, inputVars: Record<string, string>): void {
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
        this.handleFormula(node.settings as NodeFormulaSettings, varName, inputVars, this.formulaCode[node.id])
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
      case 'dynamic_rename':
        this.handleDynamicRename(node.settings as NodeDynamicRenameSettings, varName, inputVars)
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
      case 'read_from_catalog':
        this.handleReadFromCatalog(node.settings as NodeReadFromCatalogSettings, varName)
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

    // For join nodes: leftInputId maps to 'main', rightInputId maps to 'right'.
    // != null: flows loaded from YAML carry an explicit `right_input_id: null`,
    // which must not fabricate a phantom df_left/df_right input.
    if (node.leftInputId != null) {
      inputVars.main = this.nodeVarMapping.get(node.leftInputId) || 'df_left'
    }

    if (node.rightInputId != null) {
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
      // As execute_read_csv derives it.
      if (tableSettings.infer_schema === false) {
        this.addCode(`    infer_schema_length=0,`)
      } else if (tableSettings.infer_schema_length != null) {
        this.addCode(`    infer_schema_length=${tableSettings.infer_schema_length},`)
      }
    } else {
      this.addCode(`    separator=",",`)
      this.addCode(`    has_header=True,`)
    }

    // The node parses dates on read, so date expressions get a temporal dtype.
    this.addCode(`    try_parse_dates=True,`)
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

    // rawData.data is columnar: data[i] holds all values for columns[i]
    const dataDict: Record<string, any[]> = {}
    for (let i = 0; i < rawData.columns.length; i++) {
      const col = rawData.columns[i]
      dataDict[col.name] = rawData.data[i] ?? []
    }

    // Cells are text next to a declared dtype; build with it like
    // execute_manual_input, or numbers arrive as String and sum as text.
    const declared = rawData.columns.map(col => (col as { data_type?: string }).data_type)
    const typed = declared.every(Boolean) && declared.length === rawData.data.length

    this.addCode(`${varName} = pl.LazyFrame({`)
    for (const [colName, values] of Object.entries(dataDict)) {
      // Not JSON.stringify: JSON's null/true/false are not Python literals.
      this.addCode(`    "${colName}": ${toPythonValue(values)},`)
    }
    if (typed) {
      this.addCode(`}, schema={`)
      for (const [index, col] of rawData.columns.entries()) {
        this.addCode(`    "${col.name}": ${polarsDtype(declared[index])},`)
      }
      this.addCode(`}, strict=False)`)
    } else {
      this.addCode(`})`)
    }
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

  private handleReadFromCatalog(settings: NodeReadFromCatalogSettings, varName: string): void {
    const name = (settings.dataset_name || '').trim() || 'catalog_table'
    // Mirror of handleWriteToCatalog: the table's standalone form is its CSV file.
    this.addComment(`# Read Catalog table "${name}" (browser catalog → CSV file in standalone code)`)
    this.addCode(`${varName} = pl.scan_csv("${name}.csv")`)
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

    // `position` order, then cast, then rename — the order build_select uses.
    const kept = settings.select_input
      .filter(col => col.keep && col.is_available !== false)
      .map((col, index) => ({ col, index }))
      .sort((a, b) => (a.col.position ?? 0) - (b.col.position ?? 0) || a.index - b.index)

    for (const { col } of kept) {
      let expr = `pl.col("${col.old_name}")`
      const target = selectCastTarget(col)
      if (target === 'Datetime' || target === 'Date') {
        // The engine parses temporal targets from text rather than casting.
        expr += `.str.to_${target.toLowerCase()}(strict=False)`
      } else if (target) {
        expr += `.cast(pl.${target}, strict=False)`
      }
      if (col.old_name !== col.new_name) {
        expr += `.alias("${col.new_name}")`
      }
      selectExprs.push(expr)
    }

    if (selectExprs.length > 0) {
      this.addCode(`${varName} = ${inputDf}.select([`)
      for (const expr of selectExprs) {
        this.addCode(`    ${expr},`)
      }
      this.addCode(`])`)
      this.addCode('')
    } else {
      // Nothing selected -> transparent passthrough; remap and emit nothing.
      this.nodeVarMapping.set(this.currentNodeId, inputDf)
    }
  }

  private handleGroupBy(settings: NodeGroupBySettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const groupCols: string[] = []
    const aggExprs: string[] = []

    for (const aggCol of settings.groupby_input.agg_cols) {
      if (aggCol.agg === 'groupby') {
        // A group column can be renamed too, so it is an expression, not a name.
        const newName = aggCol.new_name || aggCol.old_name
        groupCols.push(
          newName === aggCol.old_name
            ? `pl.col("${aggCol.old_name}")`
            : `pl.col("${aggCol.old_name}").alias("${newName}")`
        )
      } else {
        aggExprs.push(this.aggExpr(aggCol.old_name, aggCol.agg, aggCol.new_name))
      }
    }

    if (groupCols.length === 0) {
      // No grouping columns: the engine reduces the whole frame in one select.
      if (aggExprs.length === 0) {
        this.nodeVarMapping.set(this.currentNodeId, inputDf)
        return
      }
      this.addCode(`${varName} = ${inputDf}.select([`)
      for (const expr of aggExprs) {
        this.addCode(`    ${expr},`)
      }
      this.addCode(`])`)
      this.addCode('')
      return
    }

    this.addCode(`${varName} = ${inputDf}.group_by([${groupCols.join(', ')}]).agg([`)
    for (const expr of aggExprs) {
      this.addCode(`    ${expr},`)
    }
    this.addCode(`])`)
    this.addCode('')
  }

  // One aggregation expression, matching _build_agg_exprs in the engine.
  private aggExpr(oldName: string, agg: AggType, newName: string): string {
    const col = `pl.col("${oldName}")`
    const alias = `.alias("${newName || `${oldName}_${agg}`}")`
    if (agg === 'concat') {
      return `${col}.cast(pl.Utf8).str.join(",")${alias}`
    }
    const mapping: Partial<Record<AggType, string>> = {
      sum: 'sum',
      max: 'max',
      min: 'min',
      count: 'count',
      mean: 'mean',
      median: 'median',
      first: 'first',
      last: 'last',
      n_unique: 'n_unique'
    }
    return `${col}.${mapping[agg] || 'sum'}()${alias}`
  }

  private handleJoin(settings: NodeJoinSettings, varName: string, inputVars: { main: string; right: string }): void {
    const leftDf = inputVars.main
    const rightDf = inputVars.right
    const joinInput = settings.join_input

    const leftOn = joinInput.join_mapping.map(jm => jm.left_col)
    const rightOn = joinInput.join_mapping.map(jm => jm.right_col ?? jm.left_col)
    // build_join reads join_type; `how` is core's spelling for the same thing.
    const how = joinInput.join_type || joinInput.how || 'inner'
    const suffix = joinInput.right_suffix ?? '_right'

    this.addCode(`${varName} = ${leftDf}.join(`)
    this.addCode(`    ${rightDf},`)
    this.addCode(`    left_on=${JSON.stringify(leftOn)},`)
    this.addCode(`    right_on=${JSON.stringify(rightOn)},`)
    this.addCode(`    how="${how}",`)
    this.addCode(`    suffix=${toPythonValue(suffix)}`)
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

  protected collectMainInputs(inputVars: Record<string, string>): string[] {
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

  private handleFormula(
    settings: NodeFormulaSettings,
    varName: string,
    inputVars: { main?: string },
    translated?: string,
  ): void {
    const inputDf = inputVars.main || 'df'
    const fn = settings.function
    const name = fn?.field?.name || 'new_column'
    const expr = (fn?.function || '').trim()
    if (!expr) {
      this.addCode(`${varName} = ${inputDf}`)
      this.addCode('')
      return
    }
    const castType = this.formulaCastType(fn?.field?.data_type)
    const cast = castType ? `.cast(${castType})` : ''

    if (translated) {
      this.addCode(`${varName} = ${inputDf}.with_columns((${translated})${cast}.alias(${toPythonValue(name)}))`)
      this.addCode('')
      return
    }

    // Fallback: translate at runtime.
    this.addCode(`# requires: pip install polars-expr-transformer`)
    this.addCode(`from polars_expr_transformer import simple_function_to_expr`)
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

  // Python expression (over `df`) for the columns a dynamic-rename rule targets.
  private renameTargetsExpr(dr: DynamicRenameInput, df: string): string {
    if (dr.selection_mode === 'list') {
      return `[c for c in ${toPythonValue(dr.selected_columns || [])} if c in ${df}.columns]`
    }
    if (dr.selection_mode === 'data_type') {
      // Mirror readable_data_type_group on the base dtype name (matches the engine).
      const NUMERIC = '{"fixed_decimal","decimal","float","integer","boolean","double","Int8","Int16","Int32","Int64","Int128","Float16","Float32","Float64","Decimal","Binary","Boolean","Uint8","Uint16","Uint32","Uint64","UInt8","UInt16","UInt32","UInt64","UInt128"}'
      const STRING = '{"Utf8","VARCHAR","CHAR","NVARCHAR","String"}'
      const DATE = '{"datetime","date","Date","Datetime","Time"}'
      const base = 'str(dt).split("(")[0]'
      let pred: string
      switch (dr.selected_data_type) {
        case 'String': pred = `${base} in ${STRING}`; break
        case 'Numeric': pred = `${base} in ${NUMERIC}`; break
        case 'Date': pred = `${base} in ${DATE}`; break
        case 'Other': pred = `${base} not in ${STRING} | ${NUMERIC} | ${DATE}`; break
        default: pred = 'False'  // Boolean/Binary/Complex never occur as a group
      }
      return `[c for c, dt in ${df}.collect_schema().items() if ${pred}]`
    }
    return `${df}.collect_schema().names()`
  }

  private handleDynamicRename(settings: NodeDynamicRenameSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const dr = settings.dynamic_rename_input
    if (!dr) {
      this.addCode(`${varName} = ${inputDf}`)
      this.addCode('')
      return
    }
    const targets = this.renameTargetsExpr(dr, inputDf)

    if (dr.rename_mode === 'prefix' || dr.rename_mode === 'suffix') {
      const newName =
        dr.rename_mode === 'prefix' ? `${toPythonValue(dr.prefix || '')} + c` : `c + ${toPythonValue(dr.suffix || '')}`
      this.addCode(`${varName} = ${inputDf}.rename({c: ${newName} for c in ${targets}})`)
      this.addCode('')
      return
    }

    if (dr.rename_mode === 'formula') {
      this.imports.add('from polars_expr_transformer import simple_function_to_expr')
      this.addCode(`_targets = ${targets}`)
      this.addCode(`_expr = simple_function_to_expr(${toPythonValue(dr.formula || '')})`)
      this.addCode(`_new = pl.DataFrame({"column_name": _targets}).select(_expr.alias("n"))["n"].to_list()`)
      this.addCode(`${varName} = ${inputDf}.rename({o: str(n) for o, n in zip(_targets, _new) if o != str(n)})`)
      this.addCode('')
      return
    }

    if (dr.rename_mode === 'first_row') {
      this.addCode(`_targets = ${targets}`)
      this.addCode(`_first = dict(zip(${inputDf}.collect_schema().names(), ${inputDf}.head(1).collect().row(0)))`)
      this.addCode(`${varName} = ${inputDf}.rename({o: str(_first[o]) for o in _targets if o != str(_first[o])}).slice(1)`)
      this.addCode('')
      return
    }

    this.addCode(`${varName} = ${inputDf}`)
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
    // build_unique reads subset/keep or columns/strategy, so this does too.
    const uniqueInput = settings.unique_input || {}
    const subset = uniqueInput.subset?.length ? uniqueInput.subset : uniqueInput.columns
    const keep = uniqueInput.keep || uniqueInput.strategy || 'first'
    const maintainOrder = uniqueInput.maintain_order ?? true

    this.addCode(`${varName} = ${inputDf}.unique(`)
    if (subset && subset.length > 0) {
      this.addCode(`    subset=${JSON.stringify(subset)},`)
    }
    this.addCode(`    keep="${keep}",`)
    this.addCode(`    maintain_order=${maintainOrder ? 'True' : 'False'}`)
    this.addCode(`)`)
    this.addCode('')
  }

  // What execute_pivot knows; anything else falls back to sum under its own name.
  private static readonly PIVOT_AGGS = new Set(['sum', 'mean', 'min', 'max', 'count', 'first', 'last', 'median'])

  // Zero-filled by execute_pivot: an absent combination reads 0, not null.
  private static readonly PIVOT_ZERO_FILL_AGGS = new Set(['sum', 'count', 'len'])

  private handlePivot(settings: NodePivotSettings, varName: string, inputVars: { main?: string }): void {
    const inputDf = inputVars.main || 'df'
    const pivotInput = settings.pivot_input
    const indexColumns = pivotInput.index_columns || []
    const pivotColumn = pivotInput.pivot_column
    const valueCol = pivotInput.value_col
    const aggregations = pivotInput.aggregations?.length ? pivotInput.aggregations : ['sum']

    // Mirrors execute_pivot: a native pivot cannot do several aggregations at once.
    this.addCode(`_pivot_src = ${inputDf}.collect()`)
    this.addCode(`_pivot_aggs = ${toPythonValue(aggregations)}`)
    this.addCode(`_pivot_values = (`)
    this.addCode(`    _pivot_src.select(pl.col(${toPythonValue(pivotColumn)}).cast(pl.String))`)
    this.addCode(`    .unique()`)
    this.addCode(`    .sort(${toPythonValue(pivotColumn)})`)
    this.addCode(`    .limit(200)  # the node refuses a pivot column with more distinct values`)
    this.addCode(`    .to_series()`)
    this.addCode(`    .to_list()`)
    this.addCode(`)`)

    const groupCols = [...indexColumns, pivotColumn]
    this.addCode(`_pivot_grouped = _pivot_src.group_by(${toPythonValue(groupCols)}).agg([`)
    for (const agg of aggregations) {
      const fn = FlowToPolarsConverter.PIVOT_AGGS.has(agg) ? agg : 'sum'
      this.addCode(`    pl.col(${toPythonValue(valueCol)}).${fn}().alias(${toPythonValue(agg)}),`)
    }
    this.addCode(`])`)

    const indexExprs = indexColumns.length > 0 ? indexColumns : ['__temp_idx__']
    if (indexColumns.length === 0) {
      this.addCode(`_pivot_grouped = _pivot_grouped.with_columns(pl.lit(1).alias("__temp_idx__"))`)
    }

    const zeroFilled = aggregations.filter(agg => FlowToPolarsConverter.PIVOT_ZERO_FILL_AGGS.has(agg))
    if (zeroFilled.length > 0) {
      this.addCode(`# a combination that never occurred reads 0 for these, not null`)
      this.addCode(`_pivot_zero_fill = ${toPythonValue(zeroFilled)}`)
    }

    this.addCode(`${varName} = _pivot_grouped.group_by([`)
    for (const col of indexExprs) {
      this.addCode(`    pl.col(${toPythonValue(col)}),`)
    }
    this.addCode(`]).agg([`)
    const cell = `pl.col(_agg).filter(pl.col(${toPythonValue(pivotColumn)}) == _val).first()`
    if (zeroFilled.length > 0) {
      this.addCode(`    (`)
      this.addCode(`        ${cell}.fill_null(0)`)
      this.addCode(`        if _agg in _pivot_zero_fill`)
      this.addCode(`        else ${cell}`)
      this.addCode(`    )`)
    } else {
      this.addCode(`    ${cell}`)
    }
    this.addCode(`    .alias(f"{_val}_{_agg}" if len(_pivot_aggs) > 1 else str(_val))`)
    this.addCode(`    for _val in _pivot_values`)
    this.addCode(`    for _agg in _pivot_aggs`)
    this.addCode(`])`)
    if (indexColumns.length === 0) {
      this.addCode(`${varName} = ${varName}.drop("__temp_idx__")`)
    }
    this.addCode(`${varName} = ${varName}.lazy()`)
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
    // build_head reads sample_size first, then head_input.n.
    const n = settings.sample_size ?? settings.head_input?.n ?? 10
    const method = settings.sample_method || 'first'

    if (method !== 'random' && method !== 'random_fraction') {
      this.addCode(`${varName} = ${inputDf}.head(${n})`)
      this.addCode('')
      return
    }

    const seedArg = settings.seed === null || settings.seed === undefined ? '' : `seed=${settings.seed}`
    const threshold =
      method === 'random_fraction'
        ? `(pl.len() * ${(settings.fraction ?? 10) / 100}).round().cast(pl.Int64)`
        : `${Math.max(0, n)}`

    this.addCode(`${varName} = ${inputDf}.filter(pl.int_range(0, pl.len()).shuffle(${seedArg}) < ${threshold})`)
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
  
    const fnName = `_polars_code_${this.currentNodeId}`
    this.addCode('# Custom Polars code')
    this.addCode(`def ${fnName}(${params}):`)
  
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

    this.addCode(`${varName} = ${fnName}(${args})`)
    this.addCode('')
  }

  private handlePreview(_varName: string, inputVars: { main?: string }): void {
    // explore_data is interactive-only: remap to its input and emit nothing, so
    // no dead `var = input` passthrough appears in the exported script.
    this.nodeVarMapping.set(this.currentNodeId, inputVars.main || 'df')
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

  // _DTYPE_MAP in the engine; anything else (incl. "Auto") stays uncast.
  private formulaCastType(dataType: string | undefined): string | null {
    const mapping: Record<string, string> = {
      'String': 'pl.String',
      'Utf8': 'pl.String',
      'Int64': 'pl.Int64',
      'Int32': 'pl.Int32',
      'Float64': 'pl.Float64',
      'Float32': 'pl.Float32',
      'Boolean': 'pl.Boolean',
      'Date': 'pl.Date',
      'Datetime': 'pl.Datetime'
    }
    return (dataType && mapping[dataType]) || null
  }

  protected addCode(line: string): void {
    this.codeLines.push(line)
  }

  protected addComment(comment: string): void {
    this.codeLines.push(comment)
  }

  // Upstream node ids feeding `node` (main + left + right).
  private rawProducerIds(node: FlowNode): number[] {
    const ids = [...(node.inputIds || [])]
    if (node.leftInputId != null) ids.push(node.leftInputId)
    if (node.rightInputId != null) ids.push(node.rightInputId)
    return ids
  }

  // Hop through elided passthrough nodes to the real producing node.
  private resolveProducer(nodeId: number): number {
    const seen = new Set<number>()
    while (this.passthrough.has(nodeId) && !seen.has(nodeId)) {
      seen.add(nodeId)
      nodeId = this.passthrough.get(nodeId)!
    }
    return nodeId
  }

  private varLabel(node: FlowNode): string {
    return NODE_TYPE_VAR_LABEL[node.type] || 'df'
  }

  // Fuse linear single-use chains, then give the surviving boundaries clean names.
  protected renderBody(): string[] {
    const emissions: NodeEmission[] = []
    const nodeById = new Map<number, FlowNode>()
    for (const { node, effectiveVar, start, end } of this.nodeSpans) {
      let lines = this.codeLines.slice(start, end)
      while (lines.length > 0 && lines[lines.length - 1] === '') lines = lines.slice(0, -1)
      if (lines.length === 0) continue
      const resolved = new Set(this.rawProducerIds(node).map(pid => this.resolveProducer(pid)))
      const numInputs = resolved.size
      const mainProducerId = numInputs === 1 ? resolved.values().next().value! : null
      const pinned = Boolean(node.node_reference || (node.settings as any)?.node_reference)
      emissions.push({
        nodeId: node.id, varName: effectiveVar, lines, mainProducerId, numInputs, pinned
      })
      nodeById.set(node.id, node)
    }

    const consumers = new Map<number, number[]>()
    for (const em of emissions) consumers.set(em.nodeId, [])
    for (const em of emissions) {
      const node = nodeById.get(em.nodeId)!
      const resolved = new Set(this.rawProducerIds(node).map(pid => this.resolveProducer(pid)))
      for (const pid of resolved) {
        if (consumers.has(pid)) consumers.get(pid)!.push(em.nodeId)
      }
    }

    const [body, survivors] = renderPipeline(emissions, consumers)
    const rename = this.planBoundaryNames(emissions, survivors, nodeById)
    this.explainRename = this.planExplainNames(emissions, nodeById)
    return this.applyRenames(body, rename)
  }

  // Names for explain()'s single-node snippets. Deliberately NOT the fused
  // script's names: the drawer stacks each snippet under the plain flavour's
  // loop, and the plain flavour names every node from an unfused pass — doing
  // the same here (filtered_1/filtered_2, never a survivor's bare `filtered`)
  // keeps one identifier meaning one table across the two stacked blocks.
  private planExplainNames(
    emissions: NodeEmission[], nodeById: Map<number, FlowNode>
  ): Map<string, string> {
    return this.planBoundaryNames(emissions, new Set(emissions.map(em => em.nodeId)), nodeById)
  }

  // Renameable boundaries: single-output provisional `df_N` assignments (not pinned,
  // and only node types with a known operation label).
  private isRenameable(em: NodeEmission, node: FlowNode): boolean {
    if (em.pinned) return false
    if (!(node.type in NODE_TYPE_VAR_LABEL)) return false
    return em.varName === `df_${em.nodeId}`
  }

  // Map provisional `df_N` tokens to clean labels; suffix only on collision.
  protected planBoundaryNames(
    emissions: NodeEmission[], survivors: Set<number>, nodeById: Map<number, FlowNode>
  ): Map<string, string> {
    const ordered = emissions.filter(em => survivors.has(em.nodeId))
    const renameable = ordered.filter(em => this.isRenameable(em, nodeById.get(em.nodeId)!))
    const labels = new Map(renameable.map(em => [em.nodeId, this.varLabel(nodeById.get(em.nodeId)!)]))
    const counts = new Map<string, number>()
    for (const label of labels.values()) counts.set(label, (counts.get(label) || 0) + 1)

    const used = new Set<string>([
      ...ordered.filter(em => em.pinned).map(em => em.varName),
      ...RESERVED_NAMES,
      ...this.importedNames()
    ])
    const seen = new Map<string, number>()
    const rename = new Map<string, string>()
    for (const em of renameable) {
      const label = labels.get(em.nodeId)!
      let name: string
      if ((counts.get(label) || 0) > 1) {
        seen.set(label, (seen.get(label) || 0) + 1)
        name = `${label}_${seen.get(label)}`
      } else {
        name = label
      }
      name = this.uniquify(name, used)
      rename.set(em.varName, name)
    }
    return rename
  }

  // Names bound by imports so generated vars never shadow an import alias.
  private importedNames(): Set<string> {
    const names = new Set<string>()
    for (const statement of this.imports) {
      let m = statement.match(/^import\s+(.+?)(?:\s+as\s+(\w+))?$/)
      if (m) {
        names.add(m[2] || m[1].split('.')[0])
        continue
      }
      m = statement.match(/^from\s+\S+\s+import\s+(.+)$/)
      if (m) {
        for (const part of m[1].split(',')) {
          const alias = part.trim().match(/(\w+)(?:\s+as\s+(\w+))?/)
          if (alias) names.add(alias[2] || alias[1])
        }
      }
    }
    return names
  }

  private uniquify(name: string, used: Set<string>): string {
    let candidate = name
    let bump = 2
    while (used.has(candidate)) {
      candidate = `${name}_${bump}`
      bump++
    }
    used.add(candidate)
    return candidate
  }

  // Substitute provisional tokens with final names across body + return target.
  // Word-boundary regex, leaving `param=` kwargs untouched (mirrors core's fallback).
  // Only real code is rewritten — string literals and comments are left verbatim
  // (mirrors flowfile_core's _rename_tokens, which renames NAME tokens only), so a
  // column/filter value like "df_2" is never mangled into a renamed variable.
  protected applyRenames(body: string[], rename: Map<string, string>): string[] {
    if (rename.size === 0) return body
    const patterns = Array.from(rename.entries()).map(
      ([oldName, newName]) => [new RegExp(`\\b${oldName}\\b(?!=)`, 'g'), newName] as const
    )
    const out = body.map(element =>
      element
        .split('\n')
        .map(line => this.renameCodeSegments(line, patterns))
        .join('\n')
    )
    if (this.lastNodeVar !== null) {
      this.lastNodeVar = rename.get(this.lastNodeVar) ?? this.lastNodeVar
    }
    return out
  }

  // Apply the rename patterns to a single physical line, skipping Python string
  // literals and comments. The line is walked once, splitting it into code runs
  // vs. protected spans (quoted strings, `# ...` comments); only code runs are
  // rewritten. Generated code never emits f-strings, so string bodies are opaque.
  private renameCodeSegments(line: string, patterns: ReadonlyArray<readonly [RegExp, string]>): string {
    let out = ''
    let i = 0
    const n = line.length
    while (i < n) {
      const ch = line[i]
      if (ch === '"' || ch === "'") {
        // Consume a string literal verbatim (handles backslash escapes).
        const quote = ch
        let j = i + 1
        while (j < n) {
          if (line[j] === '\\') {
            j += 2
            continue
          }
          if (line[j] === quote) {
            j++
            break
          }
          j++
        }
        out += line.slice(i, j)
        i = j
      } else if (ch === '#') {
        // Comment: the rest of the line is protected.
        out += line.slice(i)
        i = n
      } else {
        // Code run up to the next quote or comment start.
        let j = i
        while (j < n && line[j] !== '"' && line[j] !== "'" && line[j] !== '#') j++
        let segment = line.slice(i, j)
        for (const [pat, newName] of patterns) segment = segment.replace(pat, newName)
        out += segment
        i = j
      }
    }
    return out
  }

  // One node's Polars statement, standalone and under its final script names — powers
  // the settings drawer's "same step in Polars" block. Null when the node emitted
  // nothing (passthrough/disconnected) or convert() never completed.
  explain(nodeId: number): string | null {
    if (!this.explainRename) return null
    const span = this.nodeSpans.find(s => s.node.id === nodeId)
    if (!span) return null
    let lines = this.codeLines.slice(span.start, span.end)
    while (lines.length > 0 && lines[lines.length - 1] === '') lines = lines.slice(0, -1)
    if (lines.length === 0) return null
    // applyRenames also remaps lastNodeVar; restore it so explain() is idempotent.
    const saved = this.lastNodeVar
    const renamed = this.applyRenames(lines, this.explainRename)
    this.lastNodeVar = saved
    return renamed.join('\n')
  }

  protected buildFinalCode(): string {
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

    // renderBody() fuses chains and (as a side effect) remaps this.lastNodeVar
    // to its final name, so it must run before the return line is emitted.
    const body = this.renderBody()
    for (const line of body) {
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
    // Type-aware: an external_output tail binds an already-collected DataFrame.
    lines.push('    if isinstance(pipeline_output, pl.LazyFrame):')
    lines.push('        pipeline_output = pipeline_output.collect()')
    lines.push('    print(pipeline_output)')

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

  // The Polars form of one node's actual settings, for the settings drawer.
  // Whole-flow convert then slice, mirroring usePlainPythonGeneration's explainNode.
  const explainNodePolars = (options: CodeGenerationOptions, nodeId: number): string | null => {
    try {
      const converter = new FlowToPolarsConverter(options)
      converter.convert()
      return converter.explain(nodeId)
    } catch {
      return null
    }
  }

  return {
    generateCode,
    explainNodePolars
  }
}
