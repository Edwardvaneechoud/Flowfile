import type { FunctionInput, NodeFormulaSettings } from '../types'

/**
 * The formula node's ordered entries, matching flowfile_core's NodeFormula: `functions`
 * is authoritative (core omits `function` for zero or 2+ entries), the legacy single
 * `function` is the fallback for flows saved before multi-entry.
 */
export function formulaEntries(settings: NodeFormulaSettings | undefined | null): FunctionInput[] {
  if (!settings) return []
  if (Array.isArray(settings.functions)) return settings.functions.filter(Boolean)
  return settings.function ? [settings.function] : []
}

/** Entries with a non-blank expression; blank rows produce no column (core's `active_entries`). */
export function activeFormulaEntries(settings: NodeFormulaSettings | undefined | null): FunctionInput[] {
  return formulaEntries(settings).filter(entry => (entry.function || '').trim().length > 0)
}

/** Settings carrying `entries`, with `function` mirrored only for exactly one entry (core's dump shape). */
export function withFormulaEntries(
  settings: NodeFormulaSettings,
  entries: FunctionInput[],
): NodeFormulaSettings {
  const next: NodeFormulaSettings = { ...settings, functions: entries }
  if (entries.length === 1) next.function = entries[0]
  else delete next.function
  return next
}
