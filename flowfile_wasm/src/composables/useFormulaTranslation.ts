import { usePyodideStore } from '../stores/pyodide-store'
import type { FlowNode, NodeFormulaSettings } from '../types'

/** One pinned spec, shared with the run/trace harnesses so it cannot drift. */
export const EXPR_TRANSFORMER_PACKAGE = 'polars-expr-transformer==0.5.6'

/**
 * Translate each formula node's expression to Polars code (to_polars_code).
 *
 * Best-effort: returns {} when Pyodide is not ready (it never triggers Pyodide
 * initialization) and on any failure — the Polars emitter then falls back to a
 * runtime `simple_function_to_expr(...)` call, which is always correct.
 */
export const translateFormulaNodes = async (
  nodes: Map<number, FlowNode>
): Promise<Record<number, string>> => {
  const pyodideStore = usePyodideStore()
  const out: Record<number, string> = {}
  const formulaNodes = [...nodes.values()].filter(n => n.type === 'formula')
  if (formulaNodes.length === 0 || !pyodideStore.isReady) return out
  try {
    await pyodideStore.ensurePyPackages([EXPR_TRANSFORMER_PACKAGE])
    for (const node of formulaNodes) {
      const expr = (node.settings as NodeFormulaSettings)?.function?.function?.trim()
      if (!expr) continue
      const polars = await pyodideStore.runPythonWithResult(
        'import json\n' +
          'from polars_expr_transformer.process.polars_expr_transformer import to_polars_code\n' +
          `to_polars_code(json.loads(${JSON.stringify(JSON.stringify(expr))}))`
      )
      if (typeof polars === 'string' && polars.trim()) out[node.id] = polars.trim()
    }
  } catch {
    // caller's emitter falls back to runtime translation
  }
  return out
}
