/**
 * The Polars flavour of the walkthrough: the same flow as an UNFUSED Polars
 * script — one statement per node, named from the same unfused pass the plain
 * flavour uses — plus the per-step metadata the walkthrough screen needs.
 *
 * Lives in its own file because usePlainPythonGeneration imports from
 * useCodeGeneration; importing PlainStep back into useCodeGeneration would be
 * a cycle.
 */

import { FlowToPolarsConverter, type CodeGenerationOptions } from './useCodeGeneration'
import type { PlainStep, PlainWalkthrough } from './usePlainPythonGeneration'
import type { FlowNode } from '../types'

/** One entry in the official Polars API reference. */
export interface PolarsDoc {
  label: string
  href: string
  /** One sentence on what the operation does — the whole Why card for this flavour. */
  blurb: string
}

const API = 'https://docs.pola.rs/api/python/stable/reference'
const scan = (blurb: string): PolarsDoc => ({
  label: 'polars.scan_csv',
  href: `${API}/api/polars.scan_csv.html`,
  blurb
})
const lazy = (method: string, blurb: string): PolarsDoc => ({
  label: `LazyFrame.${method}`,
  href: `${API}/lazyframe/api/polars.LazyFrame.${method}.html`,
  blurb
})

/**
 * The Polars reference entry for the operation each node type renders as —
 * one sentence plus the docs link, which is the whole Why card in the Polars
 * flavour, where the plain flavour shows its concept card instead.
 */
export const POLARS_DOC_FOR_NODE: Record<string, PolarsDoc> = {
  read: scan('Reads a CSV lazily — nothing loads until the pipeline actually runs.'),
  external_data: scan('Reads a CSV lazily — nothing loads until the pipeline actually runs.'),
  read_from_catalog: scan('Reads a CSV lazily — nothing loads until the pipeline actually runs.'),
  manual_input: {
    label: 'polars.LazyFrame',
    href: `${API}/lazyframe/index.html`,
    blurb: 'Builds a table straight from literal values, one list per column.'
  },
  filter: lazy('filter', 'Keeps only the rows where the condition is true.'),
  select: lazy('select', 'Keeps, reorders or renames columns; the rows themselves never change.'),
  formula: lazy('with_columns', 'Adds or replaces columns computed from the existing ones.'),
  group_by: lazy('group_by', 'Folds the rows that share a key into one summary row per group.'),
  join: lazy('join', 'Combines two tables by matching rows on their key columns.'),
  cross_join: lazy('join', 'Pairs every row of one table with every row of the other.'),
  union: {
    label: 'polars.concat',
    href: `${API}/api/polars.concat.html`,
    blurb: 'Stacks the tables on top of each other into one.'
  },
  sort: lazy('sort', 'Reorders the rows by one or more columns.'),
  unique: lazy('unique', 'Drops every row that duplicates an earlier one.'),
  head: lazy('head', 'Takes just the first N rows.'),
  sample: lazy('filter', 'Keeps a random subset of the rows.'),
  record_id: lazy('with_row_index', 'Numbers the rows with a counter column.'),
  dynamic_rename: lazy('rename', 'Renames columns without touching the data.'),
  pivot: {
    label: 'DataFrame.pivot',
    href: `${API}/dataframe/api/polars.DataFrame.pivot.html`,
    blurb: "Spreads one column's values out into new columns."
  },
  unpivot: lazy('unpivot', 'Melts wide columns back into name/value rows.'),
  output: lazy('sink_csv', 'Streams the result into a CSV file.'),
  write_to_catalog: lazy('sink_csv', 'Streams the result into a CSV file.'),
  polars_code: {
    label: 'the Polars user guide',
    href: 'https://docs.pola.rs/',
    blurb: 'Runs the Polars code you wrote in the node, as-is.'
  }
}

// Write nodes bind `df_N = <input>` as a reference line; the step is ABOUT the
// input's table, which is also how the plain flavour names it.
const SINK_PASSTHROUGH = new Set(['output', 'write_to_catalog'])

export class FlowToPolarsWalkthroughConverter extends FlowToPolarsConverter {
  steps: PlainStep[] = []
  protected renderedSpans = new Map<number, [number, number]>()
  protected stepInputs = new Map<number, string[]>()

  protected dispatchNodeCode(node: FlowNode, varName: string, inputVars: Record<string, string>): void {
    const reads = [...this.collectMainInputs(inputVars)]
    if (inputVars.right) reads.push(inputVars.right)
    this.stepInputs.set(node.id, reads)
    super.dispatchNodeCode(node, varName, inputVars)
  }

  /** Every node keeps its own statement; a step cannot highlight half a fused chain. */
  protected renderBody(): string[] {
    const emissions: Array<{ nodeId: number; varName: string; lines: string[]; node: FlowNode }> = []
    for (const { node, effectiveVar, start, end } of this.nodeSpans) {
      // Split on newlines: the line-range math needs one PHYSICAL line per
      // element, and injected code (formulaCode, advanced filters) may not be.
      let lines = this.codeLines.slice(start, end).flatMap(line => line.split('\n'))
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

    const final = (name: string): string => rename.get(name) ?? name
    this.steps = emissions.map(emission => {
      const span = this.renderedSpans.get(emission.nodeId) ?? [0, 0]
      const inputs = (this.stepInputs.get(emission.nodeId) ?? []).map(final)
      // Sink steps keep a provisional df_N binding (no NODE_TYPE_VAR_LABEL entry);
      // the plain flavour names that step after its input — agree with it, so the
      // Data tab and the cross-flavour name contract hold for sinks too.
      const varName =
        SINK_PASSTHROUGH.has(emission.node.type) && inputs.length === 1
          ? inputs[0]
          : final(emission.varName)
      return {
        nodeId: emission.nodeId,
        nodeType: emission.node.type,
        varName,
        inputVars: inputs,
        // The plain flavour's concept cards teach plain Python — wrong flavour
        // here. The Why tab shows POLARS_DOC_FOR_NODE for these steps instead.
        concept: '',
        // Resolved against the finished script in buildFinalCode, once the
        // header above the body is known.
        lineStart: span[0],
        lineEnd: span[1]
      }
    })

    return this.applyRenames(body, rename)
  }

  protected buildFinalCode(): string {
    // renderBody() remaps lastNodeVar to its final name, so it has to run first —
    // and imports can grow during dispatch, so the offset is computed inline.
    const body = this.renderBody()

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

    // Body line i lands on script line (offset + i), 1-based — which is what
    // lets the walkthrough highlight a step inside the whole script.
    const offset = lines.length + 1
    for (const step of this.steps) {
      step.lineStart += offset
      step.lineEnd += offset - 1
    }

    for (const line of body) lines.push(line ? `    ${line}` : '')

    lines.push('')
    lines.push(this.lastNodeVar ? `    return ${this.lastNodeVar}` : '    return None')

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

/**
 * Everything the step-by-step view needs, from a single conversion.
 * Throws on an unsupported node type, like generateCode — the caller's catch
 * shows the error.
 */
export function buildPolarsWalkthrough(options: CodeGenerationOptions): PlainWalkthrough {
  const converter = new FlowToPolarsWalkthroughConverter(options)
  const script = converter.convert()
  return { script, steps: converter.steps }
}
