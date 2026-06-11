/**
 * Client-side dashboard filtering. flowfile_frontend pushes filter rules into
 * the GraphicWalker workflow that the worker applies (polars-gw); the WASM build
 * has the rows in memory, so we apply the same semantics here before handing the
 * data to the read-only renderer. Mirrors `filterToRule` in the full app's
 * useDashboardComputation.ts (categorical "one of", numeric range, date range).
 */

import type { DashboardFilter, DashboardTile } from '../types/visuals'
import type { IRow } from '../components/nodes/exploreData/interfaces'

/** Which filters apply to a tile, given the tile's datasource (dataset) name. */
export function filtersTargetingTile(
  filters: DashboardFilter[],
  tile: DashboardTile,
  tileDatasourceName: string | null,
): DashboardFilter[] {
  return filters.filter((f) => {
    if (f.target === 'tiles' && !f.target_tile_ids.includes(tile.id)) return false
    if (f.datasource_name != null && f.datasource_name !== tileDatasourceName) return false
    return true
  })
}

function matchesCategorical(value: unknown, selected: unknown[]): boolean {
  return selected.some((s) => String(s) === String(value))
}

function inNumericRange(value: unknown, min: number | null, max: number | null): boolean {
  const n = typeof value === 'number' ? value : Number(value)
  if (Number.isNaN(n)) return false
  if (min != null && n < min) return false
  if (max != null && n > max) return false
  return true
}

function inDateRange(value: unknown, start: string | null, end: string | null): boolean {
  if (value == null) return false
  const t = new Date(String(value)).getTime()
  if (Number.isNaN(t)) return false
  if (start && t < new Date(start).getTime()) return false
  if (end && t > new Date(end).getTime()) return false
  return true
}

/** A filter only narrows the data when its widget state is non-empty. */
function isActive(f: DashboardFilter): boolean {
  if (f.kind === 'categorical') {
    const sel = f.state.selected as unknown[]
    return Array.isArray(sel) && sel.length > 0
  }
  if (f.kind === 'numeric_range') return f.state.min != null || f.state.max != null
  if (f.kind === 'date_range') return !!f.state.start || !!f.state.end
  return false
}

function rowPasses(row: IRow, f: DashboardFilter): boolean {
  const value = (row as Record<string, unknown>)[f.field_name]
  if (f.kind === 'categorical') {
    return matchesCategorical(value, (f.state.selected as unknown[]) ?? [])
  }
  if (f.kind === 'numeric_range') {
    return inNumericRange(
      value,
      (f.state.min as number | null) ?? null,
      (f.state.max as number | null) ?? null,
    )
  }
  if (f.kind === 'date_range') {
    return inDateRange(
      value,
      (f.state.start as string | null) ?? null,
      (f.state.end as string | null) ?? null,
    )
  }
  return true
}

/**
 * Apply dashboard filters to rows. Returns the original array unchanged when
 * nothing is active, to avoid needless copies of large datasets.
 */
export function applyDashboardFilters(rows: IRow[], filters: DashboardFilter[]): IRow[] {
  const active = filters.filter(isActive)
  if (!active.length) return rows
  return rows.filter((row) => active.every((f) => rowPasses(row, f)))
}
