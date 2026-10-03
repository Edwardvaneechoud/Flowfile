import { usePyodideStore } from '../stores/pyodide-store'
import type { FlowNode, NodeFormulaSettings } from '../types'
import { activeFormulaEntries } from '../utils/formulaEntries'

/**
 * The single polars-expr-transformer pin for src/ — every micropip install of
 * this package must use it, so two nodes can never race different versions into
 * one Pyodide runtime. Keep in sync with tests/python/requirements.txt.
 */
export const EXPR_TRANSFORMER_PACKAGE = 'polars-expr-transformer==0.6.4'

/**
 * Translate each formula node's active entries to Polars code (to_polars_code),
 * one slot per active entry in order; an untranslated entry's slot is undefined.
 *
 * Best-effort: returns {} when Pyodide is not ready (it never triggers Pyodide
 * initialization) and on any failure — the Polars emitter then falls back to a
 * runtime `simple_function_to_expr(...)` call, which is always correct.
 */
export const translateFormulaNodes = async (
  nodes: Map<number, FlowNode>
): Promise<Record<number, Array<string | undefined>>> => {
  const pyodideStore = usePyodideStore()
  const out: Record<number, Array<string | undefined>> = {}
  const formulaNodes = [...nodes.values()].filter(n => n.type === 'formula')
  if (formulaNodes.length === 0 || !pyodideStore.isReady) return out
  try {
    await pyodideStore.ensurePyPackages([EXPR_TRANSFORMER_PACKAGE])
    for (const node of formulaNodes) {
      const entries = activeFormulaEntries(node.settings as NodeFormulaSettings)
      const translated: Array<string | undefined> = []
      for (const entry of entries) {
        const polars = await pyodideStore.runPythonWithResult(
          'import json\n' +
            'from polars_expr_transformer.process.polars_expr_transformer import to_polars_code\n' +
            `to_polars_code(json.loads(${JSON.stringify(JSON.stringify(entry.function.trim()))}))`
        )
        translated.push(typeof polars === 'string' && polars.trim() ? polars.trim() : undefined)
      }
      if (translated.some(Boolean)) out[node.id] = translated
    }
  } catch {
    // caller's emitter falls back to runtime translation
  }
  return out
}

/** Stable key of every formula entry's expression, for "re-translate when a formula changed" watchers. */
export const formulaNodesKey = (nodes: Map<number, FlowNode>): string =>
  [...nodes.values()]
    .filter(n => n.type === 'formula')
    .map(n => `${n.id}:${activeFormulaEntries(n.settings as NodeFormulaSettings).map(e => e.function).join('\u0000')}`)
    .join('|')
