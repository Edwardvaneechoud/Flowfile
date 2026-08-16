/**
 * Everything the margin says about the step you are looking at.
 *
 * Lifted out of the old walkthrough component so the panel shell and the margin
 * can read the same derived values without either owning them.
 */

import { computed, type Ref } from 'vue'
import { CONCEPTS, type PlainStep } from './usePlainPythonGeneration'
import { getNodeDescription } from '../config/nodeDescriptions'

export interface StepTable {
  label: string
  /** The rows that crossed the bridge — capped, so `total` is the real count. */
  rows: Record<string, unknown>[]
  total: number
}

/** What ▶ came back with: the buffer's own output, not the traced tables. */
export interface PlainRunResult {
  rows: Record<string, unknown>[]
  failed: boolean
  error?: string
}

/**
 * What a trace run captured: per-variable tables capped at the bridge, plus
 * the true row counts — so the counts never lie about a big table.
 */
export interface CapturedTrace {
  tables: Record<string, Record<string, unknown>[]>
  counts: Record<string, number>
}

type Captured = CapturedTrace | null

/** Steps that collapse rows rather than discarding them. */
const AGGREGATING = new Set(['accumulator-dict'])

export const stepLabel = (step: PlainStep): string => getNodeDescription(step.nodeType).title

export function usePlainStep(steps: Ref<PlainStep[]>, current: Ref<number>, captured: Ref<Captured>) {
  const step = computed<PlainStep | undefined>(() => steps.value[current.value])

  const concept = computed(() => CONCEPTS[step.value?.concept ?? ''] ?? null)

  /** The tables to show: each input this step reads, then what it produced. */
  const tables = computed<StepTable[]>(() => {
    const active = step.value
    const trace = captured.value
    if (!active || !trace) return []
    const out: StepTable[] = []
    for (const name of active.inputVars) {
      const rows = trace.tables[name]
      if (rows) out.push({ label: `in · ${name}`, rows, total: trace.counts[name] ?? rows.length })
    }
    const produced = trace.tables[active.varName]
    // A sink (write to file) binds its input straight through, so showing the
    // same table twice as "in" and "out" is noise, not a before/after.
    if (produced && !out.some(table => table.rows === produced)) {
      out.push({
        label: `out · ${active.varName}`,
        rows: produced,
        total: trace.counts[active.varName] ?? produced.length
      })
    }
    return out
  })

  /** The headline row-count fact, small enough to live on a tab label. */
  const deltaCounts = computed<{ before: number; after: number } | null>(() => {
    const active = step.value
    const trace = captured.value
    if (!active || !trace || active.inputVars.length !== 1) return null
    const before = trace.counts[active.inputVars[0]]
    const after = trace.counts[active.varName]
    return before !== undefined && after !== undefined ? { before, after } : null
  })

  /** A plain-English "what just happened to the row count". */
  const delta = computed(() => {
    const active = step.value
    const counts = deltaCounts.value
    if (!active || !counts) return ''
    if (counts.before === counts.after) return `Same ${counts.after} rows going out as came in.`
    if (counts.after < counts.before) {
      // "Dropped 992" would be a lie about a group by: nothing was discarded,
      // the rows were folded together.
      return AGGREGATING.has(active.concept)
        ? `${counts.before} rows in, ${counts.after} out — every row is still accounted for, folded into ${counts.after} groups.`
        : `${counts.before} rows in, ${counts.after} out — this step dropped ${counts.before - counts.after}.`
    }
    return `${counts.before} rows in, ${counts.after} out — this step produced ${counts.after - counts.before} more.`
  })

  /** Why this step has no data, when an earlier one does. */
  const blockedReason = computed(() => {
    const stopped = steps.value.findIndex(entry => !captured.value?.tables[entry.varName])
    if (stopped > 0 && steps.value[stopped]?.concept === 'exercise') {
      return `The script stopped at step ${stopped + 1}, which is still an unfilled exercise — so nothing past it has run yet.`
    }
    if (stopped > 0 && stopped < current.value) {
      return `The script stopped at step ${stopped + 1}, so nothing past it has run yet.`
    }
    return 'No data captured for this step.'
  })

  return { step, concept, tables, delta, deltaCounts, blockedReason, stepLabel }
}
