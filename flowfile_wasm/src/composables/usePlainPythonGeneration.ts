/**
 * Plain-Python Code Generation for WASM
 *
 * A teaching flavour of the code generator: the same flow, rewritten with no
 * dataframe library at all. Every table is a `list[dict]` (one dict per row)
 * and every node is an explicit loop.
 *
 * Semantics are matched against the WASM execution engine (`src/pyodide/engine/`),
 * not against flowfile_core — the two differ in places (aggregate set, join
 * suffixing, unique strategies), and the engine is what the canvas actually ran.
 */

import { FlowToPolarsConverter, toPythonValue } from './useCodeGeneration'
import type { CodeGenerationOptions } from './useCodeGeneration'
import type {
  FlowNode,
  NodeReadSettings,
  NodeManualInputSettings,
  NodeExternalDataSettings,
  NodeReadFromCatalogSettings,
  NodeFilterSettings,
  NodeSelectSettings,
  NodeSortSettings,
  NodeUniqueSettings,
  NodeGroupBySettings,
  NodeJoinSettings,
  NodeCrossJoinSettings,
  NodeUnionSettings,
  NodeRecordIdSettings,
  NodeSampleSettings,
  NodeDynamicRenameSettings,
  NodeUnpivotSettings,
  NodeOutputSettings,
  NodeWriteToCatalogSettings,
  InputCsvTable,
  FilterOperator,
  AggType
} from '../types'

type InputVars = Record<string, string>

/** Thrown by an emitter that cannot honour this particular configuration. */
class PlainPythonUnsupported extends Error {}

/** Join strategies with an honest loop form. `right`/`full`/`outer` are left as exercises. */
const JOIN_HOWS_SUPPORTED = new Set(['inner', 'left', 'semi', 'anti'])

/** Plain-English "what this node does", shown above the snippet in the settings drawer. */
export const NODE_EXPLANATIONS: Record<string, string> = {
  manual_input:
    'Holds a small table you typed in by hand. In plain Python that is just a list of dictionaries written out as a literal — the starting point for everything below it.',
  read:
    'Reads a file into rows. A CSV has no types: every cell arrives as text, and something has to decide that "age" is a number. That decision is normally made for you; here it is written out.',
  external_data:
    'Takes a table the host application handed to the editor. Standalone code has no host, so the generated script reads the same data from a CSV file next to it.',
  read_from_catalog:
    'Reads a table you saved in the Catalog. Standalone code has no Catalog, so the generated script reads the same data from a CSV file next to it.',
  filter:
    'Keeps the rows that match a condition and drops the rest. The loop form is the one every language shares: walk the rows, test each one, collect the keepers.',
  select:
    'Chooses which columns survive, renames them, and fixes their order. Each row is rebuilt as a new dictionary with only the keys you asked for.',
  sort:
    'Puts the rows in order. Rather than comparing rows by hand, you hand the sort a *key function* that reduces each row to the value it should be ordered by.',
  unique:
    'Drops duplicate rows. The pattern is a `seen` set: remember every key you have already emitted, and skip a row whose key is in it.',
  group_by:
    'Collapses many rows into one row per group. Two passes: file every row under its key in a dictionary, then walk the groups and summarise each one. This accumulator-dict pattern is worth knowing by heart.',
  join:
    'Matches rows in one table against rows in another. Doing it with nested loops is O(n×m); building a dictionary from the right-hand table first — a hash index — makes it O(n+m). That difference is the lesson.',
  cross_join:
    'Pairs every row on the left with every row on the right. This is the one join that really is just two nested loops, and it is why the result gets big so fast.',
  union:
    'Stacks the rows of several tables into one. The only real question is what to do about columns that not every table has.',
  record_id:
    'Numbers the rows. `enumerate` gives you the counter and the row together, so you never have to maintain an index variable yourself.',
  head:
    'Keeps the first few rows. In Python that is a slice — and slicing a list never raises when you ask for more rows than exist.',
  sample:
    'Keeps the first few rows. In Python that is a slice — and slicing a list never raises when you ask for more rows than exist.',
  dynamic_rename:
    'Renames many columns with one rule instead of one at a time. The rows are rebuilt with new keys; the values are untouched.',
  unpivot:
    'Turns wide data into long data: one row per (row, column) pair. Note the loop order — column first, then rows — which is what fixes the order of the output.',
  explore_data: 'Opens the table in the data explorer. It changes nothing, so it generates no code.',
  output:
    'Writes the rows to a file. Python ships a `csv` module for exactly this, and it handles the quoting rules you would otherwise get wrong.',
  external_output: 'Hands the rows back to the host application. It changes nothing, so it generates no code.',
  write_to_catalog:
    'Saves the rows as a Catalog table. Standalone code has no Catalog, so the generated script writes the same rows to a CSV file.',
  formula:
    'Evaluates a Flowfile expression against every row. There is no short loop equivalent, because reproducing it in general means writing an expression evaluator — so this node becomes an exercise instead.',
  polars_code:
    'Runs Polars code you wrote yourself. It is already Python, and it is already a dataframe library, so there is nothing to translate.',
  pivot:
    'Turns long data into wide data, inventing one column per distinct value. It is left as an exercise: the loop is not hard, but the column bookkeeping is fiddly and worth doing yourself.'
}

/** Why a node type has no loop form, quoted into its exercise stub. */
const STUB_REASONS: Record<string, string> = {
  formula: 'Flowfile evaluates this with its expression engine, so there is no fixed loop to show.',
  polars_code: 'This node is already Python — and already a dataframe library.',
  pivot: 'Pivoting invents one column per distinct value; the column bookkeeping is the fiddly part.'
}

/** Module-level helper functions, emitted only when a node actually needs one. */
const HELPER_SOURCES: Record<string, string[]> = {
  read_csv_file: [
    'def read_csv_file(path, delimiter=",", has_header=True, skip_rows=0, infer_types=True):',
    '    """Read a CSV file into a list of dicts, one dict per row."""',
    '    with open(path, newline="", encoding="utf-8") as handle:',
    '        rows = list(csv.reader(handle, delimiter=delimiter))',
    '    rows = rows[skip_rows:]',
    '    if not rows:',
    '        return []',
    '    if has_header:',
    '        columns, rows = rows[0], rows[1:]',
    '    else:',
    '        columns = ["column_" + str(i + 1) for i in range(len(rows[0]))]',
    '    table = [dict(zip(columns, row)) for row in rows]',
    '    if infer_types:',
    '        for column in columns:',
    '            convert = pick_column_type([row.get(column, "") for row in table])',
    '            for row in table:',
    '                raw = row.get(column, "")',
    '                row[column] = None if raw == "" else convert(raw)',
    '    return table'
  ],
  pick_column_type: [
    'def pick_column_type(values):',
    '    """Pick one converter for a whole column, the way a CSV reader does.',
    '',
    '    The type belongs to the column, not to a single cell: one stray word in a',
    '    column of numbers makes the entire column text. Flowfile also recognises',
    '    dates here — adding that is a good exercise.',
    '    """',
    '    filled = [value for value in values if value != ""]',
    '    if not filled:',
    '        return str',
    '    for convert in (int, float):',
    '        try:',
    '            for value in filled:',
    '                convert(value)',
    '            return convert',
    '        except ValueError:',
    '            pass',
    '    if all(value.lower() in ("true", "false") for value in filled):',
    '        return lambda value: value.lower() == "true"',
    '    return str'
  ],
  match_type_of: [
    'def match_type_of(text, rows, column):',
    '    """Read a filter value written as text as the same type the column holds.',
    '',
    '    The settings panel stores every filter value as a string, but the column',
    '    may hold numbers — and in Python 5 != "5". A dataframe library converts',
    '    for you; this is that conversion, made visible.',
    '    """',
    '    sample = next((row[column] for row in rows if row.get(column) is not None), None)',
    '    if isinstance(sample, bool):',
    '        return text.strip().lower() == "true"',
    '    if isinstance(sample, int):',
    '        return int(text)',
    '    if isinstance(sample, float):',
    '        return float(text)',
    '    return text'
  ],
  as_text: [
    'def as_text(value):',
    '    """Render a value the way Polars renders it when casting to text."""',
    '    if value is None:',
    '        return None',
    '    if isinstance(value, bool):',
    '        return "true" if value else "false"',
    '    return str(value)'
  ],
  values_of: [
    'def values_of(rows, column):',
    '    """Every non-null value of one column.',
    '',
    '    Aggregates skip missing values rather than tripping over them, so this',
    '    filtering step happens on your behalf every time you sum a column.',
    '    """',
    '    return [row[column] for row in rows if row[column] is not None]'
  ],
  average: [
    'def average(values):',
    '    """Mean of the values given, or None when there are none left."""',
    '    return sum(values) / len(values) if values else None'
  ],
  middle_value: [
    'def middle_value(values):',
    '    """Median of the values given, or None when there are none left."""',
    '    return statistics.median(values) if values else None'
  ],
  write_csv_file: [
    'def write_csv_file(rows, path, delimiter=","):',
    '    """Write a list of dicts to a CSV file, taking the columns from the first row."""',
    '    with open(path, "w", newline="", encoding="utf-8") as handle:',
    '        writer = csv.DictWriter(handle, fieldnames=list(rows[0]) if rows else [], delimiter=delimiter)',
    '        writer.writeheader()',
    '        writer.writerows(rows)'
  ]
}

/** Helpers that pull in other helpers. */
const HELPER_DEPENDENCIES: Record<string, string[]> = {
  read_csv_file: ['pick_column_type']
}

export class FlowToPlainPythonConverter extends FlowToPolarsConverter {
  /** Module-level helper functions this flow ended up needing. */
  protected helpers = new Set<string>()
  /** Names already taken by a user's node_reference, so loop temps never shadow one. */
  protected takenNames = new Set<string>()
  /** node id -> [start, end) into the rendered body, for the per-node explainer. */
  protected renderedSpans = new Map<number, [number, number]>()

  constructor(options: CodeGenerationOptions) {
    super(options)
    // Plain Python imports nothing by default; helpers add what they need.
    this.imports = new Set<string>()
    for (const node of this.nodes.values()) {
      const reference = node.node_reference || (node.settings as any)?.node_reference
      if (reference) this.takenNames.add(reference)
    }
  }

  // --- small helpers -------------------------------------------------------

  /** A loop-local name that cannot collide with a user's node_reference. */
  protected temp(base: string, nodeId: number): string {
    let name = `${base}_${nodeId}`
    while (this.takenNames.has(name)) name = `_${name}`
    return name
  }

  protected useHelper(name: string): void {
    if (this.helpers.has(name)) return
    this.helpers.add(name)
    for (const dependency of HELPER_DEPENDENCIES[name] || []) this.useHelper(dependency)
  }

  /** A `# ---- Title ----` banner, so each node's block is findable at a glance. */
  protected section(title: string): void {
    const rule = '-'.repeat(Math.max(4, 68 - title.length))
    this.addComment(`# --- ${title} ${rule}`)
  }

  protected teach(...lines: string[]): void {
    for (const line of lines) this.addComment(`# ${line}`)
  }

  /** `row["col"]` for a column name. */
  protected cell(rowVar: string, column: string): string {
    return `${rowVar}[${toPythonValue(column)}]`
  }

  /** A tuple expression over the given columns; no columns means the whole row. */
  protected keyTuple(rowVar: string, columns: string[]): string {
    if (columns.length === 0) return `tuple(${rowVar}.items())`
    return '(' + columns.map(column => `${this.cell(rowVar, column)}`).join(', ') + ',)'
  }

  // --- overrides -----------------------------------------------------------

  protected dispatchNodeCode(node: FlowNode, varName: string, inputVars: InputVars): void {
    const method = plainHandlerFor(node.type)
    if (!method) {
      this.emitExerciseStub(node, varName, inputVars)
      return
    }
    const start = this.codeLines.length
    try {
      const emit = this[method] as unknown as (n: FlowNode, v: string, i: InputVars) => void
      emit.call(this, node, varName, inputVars)
    } catch (error) {
      if (!(error instanceof PlainPythonUnsupported)) throw error
      // Roll back a half-written block, then leave an exercise in its place.
      this.codeLines.length = start
      this.emitExerciseStub(node, varName, inputVars, error.message)
    }
  }

  /** Every node emits its own block; fusing two `for` loops into one pipe is not a thing. */
  protected renderBody(): string[] {
    const emissions: Array<{ nodeId: number; varName: string; lines: string[]; node: FlowNode }> = []
    for (const { node, effectiveVar, start, end } of this.nodeSpans) {
      let lines = this.codeLines.slice(start, end)
      while (lines.length > 0 && lines[lines.length - 1] === '') lines = lines.slice(0, -1)
      if (lines.length === 0) continue
      emissions.push({ nodeId: node.id, varName: effectiveVar, lines, node })
    }

    const body: string[] = []
    this.renderedSpans.clear()
    for (const emission of emissions) {
      if (body.length > 0) body.push('')
      const start = body.length
      body.push(...emission.lines)
      this.renderedSpans.set(emission.nodeId, [start, body.length])
    }

    const survivors = new Set(emissions.map(emission => emission.nodeId))
    const nodeById = new Map(emissions.map(emission => [emission.nodeId, emission.node]))
    const asEmissions = emissions.map(emission => ({
      nodeId: emission.nodeId,
      varName: emission.varName,
      lines: emission.lines,
      mainProducerId: null,
      numInputs: 0,
      pinned: Boolean(emission.node.node_reference || (emission.node.settings as any)?.node_reference)
    }))
    const rename = this.planBoundaryNames(asEmissions, survivors, nodeById)
    return this.applyRenames(body, rename)
  }

  protected buildFinalCode(): string {
    // renderBody() remaps lastNodeVar to its final name, so it has to run first.
    const body = this.renderBody()
    this.renderedBody = body

    const lines: string[] = []
    const helpers = this.orderedHelpers()
    if (helpers.length > 0) {
      // Otherwise the reader opens the file on CSV-parsing scaffolding and has
      // to go looking for their own flow.
      lines.push('# Your pipeline is in run_etl_pipeline() below.')
      lines.push('# Everything above it is the handful of helpers it leans on.')
      lines.push('')
    }
    lines.push(...Array.from(this.imports).sort())
    if (this.imports.size > 0) lines.push('', '')

    for (const helper of helpers) {
      lines.push(...HELPER_SOURCES[helper], '', '')
    }

    lines.push('def run_etl_pipeline():')
    lines.push('    """')
    lines.push(`    ${this.flowName}`)
    lines.push('    Generated from Flowfile — plain Python, no dataframe library.')
    lines.push('')
    lines.push('    Every table here is a list of dicts: one dict per row, keyed by column')
    lines.push('    name. Read it top to bottom; each block is one node on the canvas.')
    lines.push('    """')

    for (const line of body) lines.push(line ? `    ${line}` : '')

    lines.push('')
    lines.push(this.lastNodeVar ? `    return ${this.lastNodeVar}` : '    return []')
    lines.push('', '')
    lines.push('if __name__ == "__main__":')
    lines.push('    pipeline_output = run_etl_pipeline()')
    lines.push('    for row in pipeline_output or []:')
    lines.push('        print(row)')

    return lines.join('\n')
  }

  /** Helpers in definition order: a helper's dependencies come before it. */
  protected orderedHelpers(): string[] {
    const ordered: string[] = []
    const visit = (name: string): void => {
      if (ordered.includes(name)) return
      for (const dependency of HELPER_DEPENDENCIES[name] || []) visit(dependency)
      ordered.push(name)
    }
    for (const name of Object.keys(HELPER_SOURCES)) {
      if (this.helpers.has(name)) visit(name)
    }
    return ordered
  }

  /** The final body, kept so explain() can slice one node's block back out of it. */
  protected renderedBody: string[] = []

  /** The rendered lines for one node — powers the settings-drawer explainer. */
  explain(nodeId: number): string | null {
    const span = this.renderedSpans.get(nodeId)
    if (!span) return null
    const lines = this.renderedBody.slice(span[0], span[1])
    // The drawer already names the node, and the banner rule wraps at that width.
    return (lines[0]?.startsWith('# --- ') ? lines.slice(1) : lines).join('\n')
  }

  // --- exercise stubs ------------------------------------------------------

  protected emitExerciseStub(node: FlowNode, varName: string, inputVars: InputVars, reason?: string): void {
    const inputs = this.collectMainInputs(inputVars)
    const right = inputVars.right
    const args = right ? [inputVars.main || 'rows_left', right] : inputs
    const parameters = args.map((_, index) => `rows_${index}`)
    const functionName = this.temp(node.type, node.id)
    const why = reason || STUB_REASONS[node.type] || `Flowfile runs this node with a library, so there is no loop to show.`

    this.section(`${node.type} (node ${node.id}) — over to you`)
    this.teach(why)
    const detail = this.stubDetail(node)
    if (detail) this.teach('The rule it applies is:', `    ${detail}`)
    this.teach(
      'Writing this one by hand is the exercise. Replace the raise below with a',
      'loop that returns the new list of rows, and the rest of the script runs.'
    )
    this.addCode(`def ${functionName}(${parameters.join(', ')}):`)
    this.addCode(`    raise NotImplementedError(${toPythonValue(why)})`)
    this.addCode('')
    this.addCode(`${varName} = ${functionName}(${args.join(', ')})`)
    this.addCode('')
  }

  /** The one line of configuration worth quoting back in a stub. */
  protected stubDetail(node: FlowNode): string | null {
    const settings = node.settings as any
    if (node.type === 'formula') return settings?.function?.function?.trim() || null
    if (node.type === 'polars_code') return settings?.polars_code_input?.polars_code?.trim()?.split('\n')[0] || null
    if (node.type === 'pivot') {
      const pivot = settings?.pivot_input
      return pivot?.pivot_column ? `pivot "${pivot.pivot_column}" into columns, aggregating "${pivot.value_col}"` : null
    }
    return null
  }

  // --- sources -------------------------------------------------------------

  plainManualInput(node: FlowNode, varName: string): void {
    const raw = (node.settings as NodeManualInputSettings).raw_data_format
    this.section('Manual input')
    this.teach(
      'A table is a list of dicts: one dict per row, keyed by column name.',
      'That is the only data structure this whole script uses.'
    )
    if (!raw || !raw.columns?.length) {
      this.addCode(`${varName} = []`)
      this.addCode('')
      return
    }
    // raw.data is columnar: data[i] holds every value of columns[i].
    const names = raw.columns.map(column => column.name)
    const height = Math.max(0, ...names.map((_, index) => (raw.data[index] || []).length))
    this.addCode(`${varName} = [`)
    for (let row = 0; row < height; row++) {
      const pairs = names.map(
        (name, index) => `${toPythonValue(name)}: ${toPythonValue((raw.data[index] || [])[row] ?? null)}`
      )
      this.addCode(`    {${pairs.join(', ')}},`)
    }
    this.addCode(']')
    this.addCode('')
  }

  plainRead(node: FlowNode, varName: string): void {
    const settings = node.settings as NodeReadSettings
    const table = settings.received_file
    if (table?.file_type && table.file_type !== 'csv') {
      throw new PlainPythonUnsupported(
        `Reading ${table.file_type} needs a library that understands the format; only CSV is plain Python.`
      )
    }
    const path = table?.path ?? ''
    const remote = path.startsWith('http://') || path.startsWith('https://')
    if (remote) {
      throw new PlainPythonUnsupported('This file is loaded from a URL; fetching it is a separate exercise.')
    }
    const csvSettings = table?.table_settings as InputCsvTable | undefined
    this.useHelper('read_csv_file')
    this.imports.add('import csv')

    this.section('Read CSV')
    this.teach(
      'Every cell in a CSV file is text. read_csv_file (above) decides which',
      'columns are really numbers — that guess is normally hidden from you.'
    )
    const args = [toPythonValue(settings.file_name || table?.name || 'data.csv')]
    if (csvSettings?.delimiter && csvSettings.delimiter !== ',') {
      args.push(`delimiter=${toPythonValue(csvSettings.delimiter)}`)
    }
    if (csvSettings?.has_headers === false) args.push('has_header=False')
    if (csvSettings?.starting_from_line) args.push(`skip_rows=${csvSettings.starting_from_line}`)
    if (csvSettings?.infer_schema === false) args.push('infer_types=False')
    this.addCode(`${varName} = read_csv_file(${args.join(', ')})`)
    this.addCode('')
  }

  plainExternalData(node: FlowNode, varName: string): void {
    const name = (node.settings as NodeExternalDataSettings).dataset_name || 'external_data'
    this.emitCsvStandIn(varName, name, 'the host application')
  }

  plainReadFromCatalog(node: FlowNode, varName: string): void {
    const name = (node.settings as NodeReadFromCatalogSettings).dataset_name || 'catalog_table'
    this.emitCsvStandIn(varName, name, 'the Catalog')
  }

  protected emitCsvStandIn(varName: string, datasetName: string, origin: string): void {
    this.useHelper('read_csv_file')
    this.imports.add('import csv')
    this.section(`Read "${datasetName}"`)
    this.teach(
      `On the canvas this table comes from ${origin}, which a standalone`,
      'script has no access to. Export it once and this line reads the same rows.'
    )
    this.addCode(`${varName} = read_csv_file(${toPythonValue(`${datasetName}.csv`)})`)
    this.addCode('')
  }

  // --- row-wise transforms -------------------------------------------------

  plainFilter(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeFilterSettings
    const input = inputVars.main || 'rows'
    const filterInput = settings.filter_input

    if (filterInput?.mode === 'advanced') {
      throw new PlainPythonUnsupported(
        'This filter is written as a Polars expression, which needs Polars to evaluate.'
      )
    }
    const basic = filterInput?.basic_filter
    if (!basic?.field) {
      this.nodeVarMapping.set(node.id, input)
      return
    }

    const setup: string[] = []
    const condition = this.filterCondition(node, basic.field, basic.operator, basic.value, basic.value2, input, setup)
    this.section('Filter')
    this.teach(
      'Walk every row, test it, keep the ones that pass. This is the shape of',
      'a filter in any language — a dataframe library just hides the loop.'
    )
    for (const line of setup) this.addCode(line)
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    if ${condition}:`)
    this.addCode(`        ${varName}.append(row)`)
    this.addCode('')
  }

  /**
   * The `if` test for one basic filter, matching the engine's operator semantics.
   * Any lines pushed onto `setup` must be emitted before the loop.
   */
  protected filterCondition(
    node: FlowNode,
    field: string,
    operator: FilterOperator,
    value: string,
    value2: string | undefined,
    input: string,
    setup: string[]
  ): string {
    const target = this.cell('row', field)
    const present = `${target} is not None`

    if (operator === 'is_null') return `${target} is None`
    if (operator === 'is_not_null') return present

    // Read the filter value once, before the loop — not once per row.
    let literals = 0
    const literal = (text: string): string => {
      this.useHelper('match_type_of')
      const name = this.temp(literals === 0 ? 'wanted' : `wanted_${literals}`, node.id)
      literals += 1
      setup.push(`${name} = match_type_of(${toPythonValue(text)}, ${input}, ${toPythonValue(field)})`)
      return name
    }

    switch (operator) {
      case 'equals':
        return `${target} == ${literal(value)}`
      // `None != 5` is True in Python but null in Polars, which drops the row.
      case 'not_equals':
        return `${present} and ${target} != ${literal(value)}`
      // Comparing against None yields null in Polars, which drops the row.
      case 'greater_than':
        return `${present} and ${target} > ${literal(value)}`
      case 'greater_than_or_equals':
        return `${present} and ${target} >= ${literal(value)}`
      case 'less_than':
        return `${present} and ${target} < ${literal(value)}`
      case 'less_than_or_equals':
        return `${present} and ${target} <= ${literal(value)}`
      // Polars' str.contains takes a regular expression, so `re` it is.
      case 'contains':
        this.imports.add('import re')
        return `${present} and re.search(${toPythonValue(value)}, ${target}) is not None`
      case 'not_contains':
        this.imports.add('import re')
        return `${present} and re.search(${toPythonValue(value)}, ${target}) is None`
      case 'starts_with':
        return `${present} and ${target}.startswith(${toPythonValue(value)})`
      case 'ends_with':
        return `${present} and ${target}.endswith(${toPythonValue(value)})`
      case 'in':
      case 'not_in': {
        this.useHelper('match_type_of')
        const wanted = this.temp('wanted', node.id)
        const members = toPythonValue(value.split(',').map(part => part.trim()))
        setup.push(
          `${wanted} = [match_type_of(text, ${input}, ${toPythonValue(field)}) for text in ${members}]`
        )
        // Polars drops null rows for is_in and for its negation alike.
        const test = `${target} in ${wanted}`
        return operator === 'in' ? `${present} and ${test}` : `${present} and not (${test})`
      }
      case 'between':
        return `${present} and ${literal(value)} <= ${target} <= ${literal(value2 ?? value)}`
      default:
        throw new PlainPythonUnsupported(`The "${operator}" filter has no plain-Python form yet.`)
    }
  }

  plainSelect(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeSelectSettings
    const input = inputVars.main || 'rows'
    // The engine keeps `keep` columns in `position` order and only renames them.
    const kept = (settings.select_input || [])
      .filter(column => column.keep !== false && column.is_available !== false)
      .slice()
      .sort((a, b) => (a.position ?? 0) - (b.position ?? 0))

    if (kept.length === 0) {
      this.nodeVarMapping.set(node.id, input)
      return
    }

    this.section('Select columns')
    this.teach(
      'Rebuild each row as a new dict holding only the columns you asked for.',
      'Renaming is free here: you simply write the new key.'
    )
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    ${varName}.append({`)
    for (const column of kept) {
      const newName = column.new_name || column.old_name
      this.addCode(`        ${toPythonValue(newName)}: ${this.cell('row', column.old_name)},`)
    }
    this.addCode('    })')
    this.addCode('')
  }

  plainSort(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeSortSettings
    const input = inputVars.main || 'rows'
    const sortInput = (settings.sort_input || []).filter(entry => entry.column)
    if (sortInput.length === 0) {
      this.nodeVarMapping.set(node.id, input)
      return
    }

    // Polars sorts nulls FIRST in both directions (nulls_last defaults to False),
    // so each key leads with a "is this missing" flag rather than the value itself.
    const nullFlag = (target: string, descending: boolean): string =>
      descending ? `${target} is None` : `${target} is not None`

    const descendingFlags = sortInput.map(entry => entry.how === 'desc')
    const uniform = descendingFlags.every(flag => flag === descendingFlags[0])

    this.section('Sort')
    if (uniform) {
      const reverse = descendingFlags[0]
      const parts = sortInput.flatMap(entry => {
        const target = this.cell('row', entry.column)
        return [nullFlag(target, reverse), target]
      })
      this.teach(
        'sorted() takes a *key function*: reduce each row to the value it should',
        'be ordered by, and let the sort do the comparing.',
        'Missing values sort first either way, which is what the "is None" flag',
        'at the front of the key reproduces.'
      )
      this.addCode(`${varName} = sorted(`)
      this.addCode(`    ${input},`)
      this.addCode(`    key=lambda row: (${parts.join(', ')}),`)
      if (reverse) this.addCode('    reverse=True,')
      this.addCode(')')
      this.addCode('')
      return
    }

    // Mixed directions need one pass per column, least significant first —
    // which works only because Python's sort is stable.
    this.teach(
      'The columns sort in different directions, so one key function will not do.',
      'Instead: sort once per column, least significant first. That works only',
      "because Python's sort is *stable* — equal rows keep the order they had.",
      'Read the passes bottom to top to see the priority.'
    )
    this.addCode(`${varName} = list(${input})`)
    for (let index = sortInput.length - 1; index >= 0; index--) {
      const entry = sortInput[index]
      const target = this.cell('row', entry.column)
      const reverse = descendingFlags[index]
      const key = `(${nullFlag(target, reverse)}, ${target})`
      this.addCode(
        `${varName}.sort(key=lambda row: ${key}${reverse ? ', reverse=True' : ''})  # ${entry.column} ${reverse ? 'desc' : 'asc'}`
      )
    }
    this.addCode('')
  }

  plainUnique(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeUniqueSettings
    const input = inputVars.main || 'rows'
    const uniqueInput = (settings.unique_input || {}) as any
    const subset: string[] = uniqueInput.subset || uniqueInput.columns || []
    const keep: string = uniqueInput.keep || uniqueInput.strategy || 'first'

    const key = this.keyTuple('row', subset)
    const seen = this.temp('seen', node.id)

    this.section('Unique')
    if (keep === 'first' || keep === 'any') {
      this.teach(
        'The `seen` set is the whole trick: remember each key you have already',
        'emitted, and skip a row whose key is in it.'
      )
      this.addCode(`${seen} = set()`)
      this.addCode(`${varName} = []`)
      this.addCode(`for row in ${input}:`)
      this.addCode(`    key = ${key}`)
      this.addCode(`    if key in ${seen}:`)
      this.addCode('        continue')
      this.addCode(`    ${seen}.add(key)`)
      this.addCode(`    ${varName}.append(row)`)
      this.addCode('')
      return
    }

    const byKey = this.temp('by_key', node.id)
    if (keep === 'last') {
      this.teach(
        'Keeping the *last* of each key: a dict remembers insertion order, and',
        'overwriting a key does NOT move it to the end — so drop the old entry',
        'first. That puts each row where its kept copy actually sits.'
      )
      this.addCode(`${byKey} = {}`)
      this.addCode(`for row in ${input}:`)
      this.addCode(`    key = ${key}`)
      this.addCode(`    ${byKey}.pop(key, None)`)
      this.addCode(`    ${byKey}[key] = row`)
      this.addCode(`${varName} = list(${byKey}.values())`)
      this.addCode('')
      return
    }

    if (keep === 'none') {
      this.teach(
        'keep="none" drops every row whose key appears more than once, so you',
        'have to count the keys before you can decide — hence two passes.'
      )
      const counts = this.temp('counts', node.id)
      this.addCode(`${counts} = {}`)
      this.addCode(`for row in ${input}:`)
      this.addCode(`    key = ${key}`)
      this.addCode(`    ${counts}[key] = ${counts}.get(key, 0) + 1`)
      this.addCode(`${varName} = [row for row in ${input} if ${counts}[${key}] == 1]`)
      this.addCode('')
      return
    }

    throw new PlainPythonUnsupported(`The "${keep}" duplicate strategy has no plain-Python form yet.`)
  }

  plainRecordId(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeRecordIdSettings
    const input = inputVars.main || 'rows'
    const name = settings.record_id_input?.name || 'record_id'
    const offset = settings.record_id_input?.offset ?? 1

    this.section('Record ID')
    this.teach(
      'enumerate() hands you the counter and the row together, so there is no',
      'index variable to keep in step by hand. The new column goes first.'
    )
    this.addCode(`${varName} = []`)
    this.addCode(`for number, row in enumerate(${input}, start=${offset}):`)
    this.addCode(`    ${varName}.append({${toPythonValue(name)}: number, **row})`)
    this.addCode('')
  }

  plainHead(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeSampleSettings
    const input = inputVars.main || 'rows'
    const method = settings.sample_method || 'first'
    if (method !== 'first') {
      throw new PlainPythonUnsupported(
        'Random sampling uses a seeded shuffle, which plain Python cannot reproduce row-for-row.'
      )
    }
    const size = settings.sample_size ?? (settings as any).head_input?.n ?? 10

    this.section('Take sample')
    this.teach('A slice. Asking for more rows than exist is not an error — you just get fewer.')
    this.addCode(`${varName} = ${input}[:${size}]`)
    this.addCode('')
  }

  plainDynamicRename(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeDynamicRenameSettings
    const input = inputVars.main || 'rows'
    const rule = settings.dynamic_rename_input
    if (!rule) {
      this.nodeVarMapping.set(node.id, input)
      return
    }
    if (rule.rename_mode !== 'prefix' && rule.rename_mode !== 'suffix') {
      throw new PlainPythonUnsupported(
        `The "${rule.rename_mode}" rename mode needs Flowfile's expression engine or the data itself.`
      )
    }
    if (rule.selection_mode === 'data_type') {
      throw new PlainPythonUnsupported(
        'Selecting columns by data type needs the schema, which a list of dicts does not carry.'
      )
    }

    const affix = rule.rename_mode === 'prefix' ? rule.prefix || '' : rule.suffix || ''
    const newName =
      rule.rename_mode === 'prefix' ? `${toPythonValue(affix)} + column` : `column + ${toPythonValue(affix)}`
    const targets = this.temp('targets', node.id)

    this.section('Rename columns')
    this.teach(
      'One rule applied to many columns. The rows are rebuilt with new keys;',
      'the values never move.'
    )
    if (rule.selection_mode === 'list') {
      this.addCode(`${targets} = set(${toPythonValue(rule.selected_columns || [])})`)
    } else {
      this.addCode(`${targets} = set(${input}[0]) if ${input} else set()`)
    }
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    ${varName}.append({`)
    this.addCode(`        (${newName}) if column in ${targets} else column: value`)
    this.addCode('        for column, value in row.items()')
    this.addCode('    })')
    this.addCode('')
  }

  // --- reshape -------------------------------------------------------------

  plainGroupBy(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeGroupBySettings
    const input = inputVars.main || 'rows'
    const aggCols = settings.groupby_input?.agg_cols || []
    const groupCols = aggCols.filter(column => column.agg === 'groupby')
    const aggregations = aggCols.filter(column => column.agg !== 'groupby')

    if (aggCols.length === 0) {
      this.nodeVarMapping.set(node.id, input)
      return
    }

    const groups = this.temp('groups', node.id)
    const members = 'rows_in_group'

    this.section('Group by')
    if (groupCols.length === 0) {
      this.teach('No grouping columns, so the whole table is one group: a single summary row.')
      this.addCode(`${members} = ${input}`)
      this.addCode(`${varName} = [{`)
      for (const aggregation of aggregations) {
        this.addCode(`    ${toPythonValue(aggregation.new_name)}: ${this.aggExpression(aggregation, members)},`)
      }
      this.addCode('}]')
      this.addCode('')
      return
    }

    this.teach(
      'The accumulator-dict pattern, in two passes: file every row under its',
      'key, then walk the groups and summarise each one. Worth knowing by heart.'
    )
    this.addCode(`${groups} = {}`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    key = ${this.keyTuple('row', groupCols.map(column => column.old_name))}`)
    this.addCode(`    ${groups}.setdefault(key, []).append(row)`)
    this.addCode('')
    this.addCode(`${varName} = []`)
    this.addCode(`for key, ${members} in ${groups}.items():`)
    if (aggregations.length === 0) {
      // group_by with no aggregations is just the distinct set of keys.
      this.addCode(`    ${varName}.append({`)
      groupCols.forEach((column, index) => {
        this.addCode(`        ${toPythonValue(column.new_name)}: key[${index}],`)
      })
      this.addCode('    })')
      this.addCode('')
      return
    }
    this.addCode(`    ${varName}.append({`)
    groupCols.forEach((column, index) => {
      this.addCode(`        ${toPythonValue(column.new_name)}: key[${index}],`)
    })
    for (const aggregation of aggregations) {
      this.addCode(`        ${toPythonValue(aggregation.new_name)}: ${this.aggExpression(aggregation, members)},`)
    }
    this.addCode('    })')
    this.addCode('')
  }

  /** One aggregation over `members`, matching Polars' null handling. */
  protected aggExpression(aggregation: { old_name: string; agg: AggType }, members: string): string {
    const column = toPythonValue(aggregation.old_name)
    // Polars skips nulls in every aggregate except first / last / n_unique.
    const needsValues = aggregation.agg !== 'first' && aggregation.agg !== 'last' && aggregation.agg !== 'n_unique'
    if (needsValues) this.useHelper('values_of')
    const values = `values_of(${members}, ${column})`

    switch (aggregation.agg) {
      case 'sum':
        return `sum(${values})`
      case 'min':
        return `min(${values}, default=None)`
      case 'max':
        return `max(${values}, default=None)`
      // count() counts values, not rows: nulls do not count.
      case 'count':
        return `len(${values})`
      case 'mean':
        this.useHelper('average')
        return `average(${values})`
      case 'median':
        this.useHelper('middle_value')
        this.imports.add('import statistics')
        return `middle_value(${values})`
      // first / last are positional, so a null counts as a value here.
      case 'first':
        return `${members}[0][${column}] if ${members} else None`
      case 'last':
        return `${members}[-1][${column}] if ${members} else None`
      // A null is one of the distinct values, the same way Polars counts it.
      case 'n_unique':
        return `len({item[${column}] for item in ${members}})`
      case 'concat':
        this.useHelper('as_text')
        return `",".join(as_text(value) for value in ${values})`
      default:
        throw new PlainPythonUnsupported(`The "${aggregation.agg}" aggregation has no plain-Python form yet.`)
    }
  }

  plainUnpivot(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeUnpivotSettings
    const input = inputVars.main || 'rows'
    const unpivot = settings.unpivot_input || ({} as any)
    if (unpivot.data_type_selector_mode === 'data_type' && unpivot.data_type_selector) {
      throw new PlainPythonUnsupported(
        'Selecting the columns to unpivot by data type needs the schema, which a list of dicts does not carry.'
      )
    }
    const indexColumns: string[] = unpivot.index_columns || []
    const valueColumns: string[] = unpivot.value_columns || []
    const targets = this.temp('value_columns', node.id)

    this.section('Unpivot')
    this.teach(
      'Wide to long: one output row per (row, column) pair. The column loop is',
      'on the outside — that is what puts all of one column together in the result.'
    )
    if (valueColumns.length > 0) {
      this.addCode(`${targets} = ${toPythonValue(valueColumns)}`)
    } else {
      this.addCode(
        `${targets} = [column for column in (${input}[0] if ${input} else {}) if column not in ${toPythonValue(indexColumns)}]`
      )
    }
    this.addCode(`${varName} = []`)
    this.addCode(`for column in ${targets}:`)
    this.addCode(`    for row in ${input}:`)
    this.addCode(`        ${varName}.append({`)
    for (const indexColumn of indexColumns) {
      this.addCode(`            ${toPythonValue(indexColumn)}: ${this.cell('row', indexColumn)},`)
    }
    this.addCode('            "variable": column,')
    this.addCode('            "value": row.get(column),')
    this.addCode('        })')
    this.addCode('')
  }

  // --- combine -------------------------------------------------------------

  plainJoin(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeJoinSettings
    const joinInput = (settings.join_input || {}) as any
    const how: string = joinInput.join_type || joinInput.how || 'inner'
    const mapping = joinInput.join_mapping || []
    const suffix: string = joinInput.right_suffix ?? '_right'

    if (!JOIN_HOWS_SUPPORTED.has(how)) {
      throw new PlainPythonUnsupported(
        `A "${how}" join is a left join with the tables the other way round — worth writing out yourself.`
      )
    }
    if (mapping.length === 0) {
      throw new PlainPythonUnsupported('This join has no columns to match on yet.')
    }

    const left = inputVars.main || 'rows_left'
    const right = inputVars.right || 'rows_right'
    const index = this.temp('index', node.id)
    const rightKeys = this.temp('right_keys', node.id)
    const leftKey = this.keyTuple('row', mapping.map((entry: any) => entry.left_col))
    const indexKey = this.keyTuple('other', mapping.map((entry: any) => entry.right_col))

    this.section(`Join (${how})`)
    this.teach(
      'Index the right-hand table by its key first, then walk the left table',
      'once. Nested loops would be rows x rows; this is rows + rows.',
      'A missing key matches nothing at all — not even another missing key.'
    )
    const extras = this.temp('right_columns', node.id)

    this.addCode(`${rightKeys} = ${toPythonValue(mapping.map((entry: any) => entry.right_col))}`)
    this.addCode(`${index} = {}`)
    this.addCode(`for other in ${right}:`)
    this.addCode(`    key = ${indexKey}`)
    this.addCode('    if any(value is None for value in key):')
    this.addCode('        continue')
    this.addCode(`    ${index}.setdefault(key, []).append(other)`)
    if (how === 'left') {
      this.addCode(
        `${extras} = [column for column in (${right}[0] if ${right} else {}) if column not in ${rightKeys}]`
      )
    }
    this.addCode('')
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${left}:`)
    this.addCode(`    key = ${leftKey}`)
    this.addCode(`    matches = [] if any(value is None for value in key) else ${index}.get(key, [])`)

    if (how === 'semi') {
      this.addCode('    if matches:')
      this.addCode(`        ${varName}.append(row)`)
      this.addCode('')
      return
    }
    if (how === 'anti') {
      this.addCode('    if not matches:')
      this.addCode(`        ${varName}.append(row)`)
      this.addCode('')
      return
    }

    this.addCode('    for other in matches:')
    this.addCode('        combined = dict(row)')
    this.addCode('        for column, value in other.items():')
    this.addCode(`            if column in ${rightKeys}:`)
    this.addCode('                continue')
    this.addCode(`            combined[column + ${toPythonValue(suffix)} if column in row else column] = value`)
    this.addCode(`        ${varName}.append(combined)`)

    if (how === 'left') {
      this.addCode('    if not matches:')
      this.addCode(
        `        ${varName}.append({**row, **{column + ${toPythonValue(suffix)} if column in row else column: None for column in ${extras}}})`
      )
    }
    this.addCode('')
  }

  plainCrossJoin(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeCrossJoinSettings
    const suffix = settings.cross_join_input?.right_suffix || '_right'
    const left = inputVars.main || 'rows_left'
    const right = inputVars.right || 'rows_right'

    this.section('Cross join')
    this.teach(
      'Every row on the left paired with every row on the right. This is the',
      'one join that really is two nested loops — and why the output gets big.'
    )
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${left}:`)
    this.addCode(`    for other in ${right}:`)
    this.addCode('        combined = dict(row)')
    this.addCode('        for column, value in other.items():')
    this.addCode(`            combined[column + ${toPythonValue(suffix)} if column in row else column] = value`)
    this.addCode(`        ${varName}.append(combined)`)
    this.addCode('')
  }

  plainUnion(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeUnionSettings
    const inputs = this.collectMainInputs(inputVars)
    if (inputs.length === 0) {
      this.addCode(`${varName} = []`)
      this.addCode('')
      return
    }
    if (inputs.length === 1) {
      this.nodeVarMapping.set(node.id, inputs[0])
      return
    }
    const mode = settings.union_input?.mode || 'diagonal'

    this.section('Union')
    if (mode === 'vertical') {
      this.teach('Same columns in every table, so stacking the rows is all there is to it.')
      this.addCode(`${varName} = ${inputs.join(' + ')}`)
      this.addCode('')
      return
    }

    const columns = this.temp('all_columns', node.id)
    this.teach(
      'The tables need not share columns. Collect every column name first, then',
      'rebuild each row with None wherever its table had nothing to say.'
    )
    this.addCode(`${columns} = []`)
    this.addCode(`for table in (${inputs.join(', ')}):`)
    this.addCode('    for column in (table[0] if table else {}):')
    this.addCode(`        if column not in ${columns}:`)
    this.addCode(`            ${columns}.append(column)`)
    this.addCode('')
    this.addCode(`${varName} = []`)
    this.addCode(`for table in (${inputs.join(', ')}):`)
    this.addCode('    for row in table:')
    this.addCode(`        ${varName}.append({column: row.get(column) for column in ${columns}})`)
    this.addCode('')
  }

  // --- sinks ---------------------------------------------------------------

  /** Emitting nothing makes the base class alias this node straight to its input. */
  plainExploreData(): void {}

  plainExternalOutput(): void {}

  plainOutput(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeOutputSettings
    const input = inputVars.main || 'rows'
    const output = settings.output_settings
    const fileType = output?.file_type || 'csv'
    if (fileType !== 'csv') {
      throw new PlainPythonUnsupported(
        `Writing ${fileType} needs a library that understands the format; only CSV is plain Python.`
      )
    }
    const fileName = output?.name || 'output.csv'
    const delimiter = (output?.table_settings as any)?.delimiter
    this.useHelper('write_csv_file')
    this.imports.add('import csv')

    this.section('Write CSV')
    this.teach("Python's csv module handles the quoting and escaping rules for you.")
    const args = [input, toPythonValue(fileName)]
    if (delimiter && delimiter !== ',') {
      args.push(`delimiter=${toPythonValue(delimiter === 'tab' ? '\t' : delimiter)}`)
    }
    this.addCode(`write_csv_file(${args.join(', ')})`)
    this.addCode('')
    // Writing a file produces no new table, so downstream keeps using the input.
    void varName
    this.nodeVarMapping.set(node.id, input)
  }

  plainWriteToCatalog(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeWriteToCatalogSettings
    const input = inputVars.main || 'rows'
    const name = (settings.dataset_name || '').trim() || 'catalog_table'
    this.useHelper('write_csv_file')
    this.imports.add('import csv')

    this.section(`Write "${name}"`)
    this.teach('A standalone script has no Catalog, so the rows go to a CSV file of the same name.')
    this.addCode(`write_csv_file(${input}, ${toPythonValue(`${name}.csv`)})`)
    this.addCode('')
    void varName
    this.nodeVarMapping.set(node.id, input)
  }
}

/**
 * Node type -> emitter method. This table is the single source of truth: the
 * allowlist below is derived from it, so a node type can never be "supported"
 * without an emitter, and `satisfies` fails the build if a method name is wrong.
 */
const PLAIN_HANDLERS = {
  manual_input: 'plainManualInput',
  read: 'plainRead',
  external_data: 'plainExternalData',
  read_from_catalog: 'plainReadFromCatalog',
  filter: 'plainFilter',
  select: 'plainSelect',
  sort: 'plainSort',
  unique: 'plainUnique',
  record_id: 'plainRecordId',
  head: 'plainHead',
  sample: 'plainHead',
  dynamic_rename: 'plainDynamicRename',
  group_by: 'plainGroupBy',
  unpivot: 'plainUnpivot',
  join: 'plainJoin',
  cross_join: 'plainCrossJoin',
  union: 'plainUnion',
  explore_data: 'plainExploreData',
  external_output: 'plainExternalOutput',
  output: 'plainOutput',
  write_to_catalog: 'plainWriteToCatalog'
} as const satisfies Record<string, keyof FlowToPlainPythonConverter>

function plainHandlerFor(nodeType: string): keyof FlowToPlainPythonConverter | undefined {
  return (PLAIN_HANDLERS as Record<string, keyof FlowToPlainPythonConverter>)[nodeType]
}

/** Node types with a plain-Python form. Derived — never hand-written. */
export const PLAIN_PYTHON_NODE_TYPES: ReadonlySet<string> = new Set(Object.keys(PLAIN_HANDLERS))

export interface NodeExplanation {
  nodeId: number
  nodeType: string
  explanation: string
  code: string | null
  supported: boolean
  /** Helper functions the snippet calls but does not define; they live in the full script. */
  helpers: string[]
}

export function usePlainPythonGeneration() {
  /** The whole flow as one standalone plain-Python script. Never throws on an unsupported node. */
  const generatePlainPython = (options: CodeGenerationOptions): string =>
    new FlowToPlainPythonConverter(options).convert()

  /** Prose + the plain-Python form of one node's actual settings, for the settings drawer. */
  const explainNode = (options: CodeGenerationOptions, nodeId: number): NodeExplanation => {
    const node = options.nodes.get(nodeId)
    const nodeType = node?.type ?? 'unknown'
    const explanation = NODE_EXPLANATIONS[nodeType] ?? ''
    let code: string | null = null
    try {
      const converter = new FlowToPlainPythonConverter(options)
      converter.convert()
      code = converter.explain(nodeId)
    } catch {
      code = null
    }
    const helpers = code ? Object.keys(HELPER_SOURCES).filter(name => code!.includes(`${name}(`)) : []
    return { nodeId, nodeType, explanation, code, helpers, supported: PLAIN_PYTHON_NODE_TYPES.has(nodeType) }
  }

  return { generatePlainPython, explainNode, PLAIN_PYTHON_NODE_TYPES, NODE_EXPLANATIONS }
}
