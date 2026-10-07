/**
 * Where the notebook's cells stand. Pure functions of the cells: the store decides what the cells are.
 */

export interface OrderedCell {
  cell_id: string
  defines: string[]
  uses: string[]
}

/**
 * The cells in the order the notebook showed them before, so a canvas change does not move the cells
 * the user is looking at. A cell that was not shown before follows the cell it follows in `ordered`.
 * A cell may only read names defined above it (a sync reads cells in this order); when the old order
 * would leave a cell above a name it reads, or the name is defined nowhere, `ordered` stands.
 */
export function stableOrder<T extends OrderedCell>(previous: string[], ordered: T[]): T[] {
  const rank = new Map(previous.map((id, index) => [id, index]))
  if (!ordered.some(cell => rank.has(cell.cell_id))) return ordered
  const followers = new Map<string | null, T[]>()
  let lead: string | null = null
  for (const cell of ordered) {
    if (rank.has(cell.cell_id)) {
      lead = cell.cell_id
      continue
    }
    const after = followers.get(lead)
    if (after) after.push(cell)
    else followers.set(lead, [cell])
  }
  const known = ordered.filter(cell => rank.has(cell.cell_id)).sort((a, b) => rank.get(a.cell_id)! - rank.get(b.cell_id)!)
  const result = [...(followers.get(null) ?? [])]
  for (const cell of known) result.push(cell, ...(followers.get(cell.cell_id) ?? []))
  const defined = new Set<string>()
  for (const cell of result) {
    if (cell.uses.some(name => !defined.has(name))) return ordered
    for (const name of cell.defines) defined.add(name)
  }
  return result
}
