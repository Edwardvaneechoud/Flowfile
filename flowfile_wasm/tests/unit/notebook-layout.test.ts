import { describe, it, expect } from 'vitest'
import { stableOrder } from '../../src/utils/notebookLayout'

const cell = (id: string, defines: string[] = [], uses: string[] = []) => ({ cell_id: id, defines, uses })
const ids = (cells: { cell_id: string }[]) => cells.map(each => each.cell_id)

describe('stableOrder', () => {
  it('keeps the order the cells were shown in', () => {
    const ordered = [cell('imports'), cell('a'), cell('c'), cell('b')]
    expect(ids(stableOrder(['imports', 'a', 'b', 'c'], ordered))).toEqual(['imports', 'a', 'b', 'c'])
  })

  it('puts a cell not shown before after the cell it follows in the render', () => {
    const ordered = [cell('imports'), cell('a'), cell('new'), cell('c'), cell('b')]
    expect(ids(stableOrder(['imports', 'a', 'b', 'c'], ordered))).toEqual(['imports', 'a', 'new', 'b', 'c'])
  })

  it('takes the render order when the old one would put a cell above a name it reads', () => {
    const ordered = [cell('imports'), cell('a', ['x']), cell('c', ['y']), cell('b', [], ['y'])]
    expect(ids(stableOrder(['imports', 'a', 'b', 'c'], ordered))).toEqual(['imports', 'a', 'c', 'b'])
  })

  it('takes the render order when nothing was shown before', () => {
    const ordered = [cell('imports'), cell('b'), cell('a')]
    expect(stableOrder([], ordered)).toBe(ordered)
  })
})
