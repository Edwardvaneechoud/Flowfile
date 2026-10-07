/**
 * What a notebook cell offers while the user types: the `ff.` names and methods the engine's
 * dialect has, the frames the cells above name, and the columns of the frame a string is read on.
 */

import { describe, it, expect } from 'vitest'
import { CompletionContext, type CompletionResult, type CompletionSource } from '@codemirror/autocomplete'
import { python } from '@codemirror/lang-python'
import { EditorState } from '@codemirror/state'
import {
  chainBefore,
  classify,
  notebookCompletionSources,
  type CellCompletionContext,
  type NotebookSurface
} from '../../src/components/notebook/notebookCompletions'

const SURFACE: NotebookSurface = {
  ff: ['Int64', 'String', 'col', 'concat', 'from_raw_data', 'lit', 'scan_csv', 'when'],
  methods: {
    FlowFrame: ['drop', 'filter', 'group_by', 'join', 'select', 'sort'],
    GroupByFrame: ['agg'],
    Expr: ['alias', 'cast', 'dt', 'str', 'sum'],
    StringNS: ['contains', 'to_uppercase'],
    DateTimeNS: ['year'],
    datetime: ['date', 'datetime']
  },
  pushable: ['FlowFrame.drop', 'FlowFrame.filter', 'FlowFrame.group_by', 'FlowFrame.select', 'FlowFrame.sort', 'ff.from_raw_data']
}
const SALES = [
  { name: 'product', dtype: 'String' },
  { name: 'revenue', dtype: 'Int64' }
]

function context(overrides: Partial<CellCompletionContext> = {}): CellCompletionContext {
  return {
    surface: SURFACE,
    cellId: 'cell-2',
    priorCells: [{ id: 'cell-1', code: 'sales = ff.scan_csv("sales.csv")' }],
    frameColumns: name => (name === 'sales' ? SALES : null),
    cellColumns: () => [{ name: 'cell_column', dtype: 'Float64' }],
    ...overrides
  }
}

/** What the sources offer with the cursor at `|`. */
async function offered(text: string, overrides: Partial<CellCompletionContext> = {}): Promise<CompletionResult | null> {
  const pos = text.indexOf('|')
  const doc = text.slice(0, pos) + text.slice(pos + 1)
  const state = EditorState.create({ doc, extensions: [python()] })
  for (const source of notebookCompletionSources(() => context(overrides)) as CompletionSource[]) {
    const result = await source(new CompletionContext(state, pos, false))
    if (result) return result
  }
  return null
}

const labels = (result: CompletionResult | null) => result?.options.map(option => option.label) ?? []
const option = (result: CompletionResult | null, label: string) => result?.options.find(each => each.label === label)

describe('ff. names', () => {
  it('offers only what the dialect has, the calls a push reads first', async () => {
    const result = await offered('x = ff.|')
    expect(labels(result)).toEqual(SURFACE.ff)
    expect(option(result, 'from_raw_data')?.boost).toBe(2)
    expect(option(result, 'col')?.detail).toMatch(/^\(/)
  })

  it('offers nothing before the engine has said what the dialect is', async () => {
    expect(await offered('x = ff.|', { surface: null })).toBeNull()
  })
})

describe('methods after a dot', () => {
  it('offers a frame its methods, marking the ones a push cannot read', async () => {
    const result = await offered('x = sales.|')
    expect(labels(result)).toEqual(SURFACE.methods.FlowFrame)
    expect(option(result, 'group_by')).toMatchObject({ detail: 'a step', boost: 2 })
    expect(option(result, 'join')?.detail).toBe('change on the canvas')
  })

  it('knows a group_by only aggregates, and an expression its own methods and namespaces', async () => {
    expect(labels(await offered('x = sales.group_by("product").|'))).toEqual(['agg'])
    expect(labels(await offered('x = sales.filter(ff.col("revenue").|'))).toEqual(SURFACE.methods.Expr)
    expect(labels(await offered('x = sales.select(ff.col("product").str.|'))).toEqual(SURFACE.methods.StringNS)
    expect(labels(await offered('x = sales.group_by("product").agg(ff.col("revenue").sum()).|'))).toEqual(
      SURFACE.methods.FlowFrame
    )
  })

  it('follows a chain over lines, as the render writes one', async () => {
    const cell = 'out = (\n    sales\n    .filter(ff.col("revenue") > 1)\n    .|'
    expect(labels(await offered(cell))).toEqual(SURFACE.methods.FlowFrame)
  })

  it('takes a frame a line above in the same cell as a frame', async () => {
    expect(labels(await offered('big = ff.from_raw_data({})\nbig.|'))).toEqual(SURFACE.methods.FlowFrame)
  })

  it('offers nothing on a name it does not know as a frame', async () => {
    expect(await offered('x = unknown.|')).toBeNull()
  })
})

describe('column names inside a string', () => {
  it('offers the columns the canvas knows for the frame', async () => {
    const result = await offered('x = sales.select("|")')
    expect(labels(result)).toEqual(['product', 'revenue'])
    expect(option(result, 'revenue')?.detail).toBe('Int64 · sales (last run)')
  })

  it('follows a rename written in a cell above', async () => {
    const priorCells = [
      { id: 'cell-1', code: 'sales = ff.scan_csv("sales.csv")' },
      { id: 'cell-2', code: 'renamed = sales.rename({"product": "item"})' }
    ]
    expect(labels(await offered('x = renamed.sort("|")', { priorCells, cellId: 'cell-3' }))).toEqual(['item', 'revenue'])
  })

  it('reads a column inside ff.col on the frame method around it', async () => {
    expect(labels(await offered('x = sales.filter(ff.col("|") > 1)'))).toEqual(['product', 'revenue'])
  })

  it("falls back to this cell's columns when the frame cannot be worked out", async () => {
    const result = await offered('x = ff.scan_csv("a.csv").filter(ff.col("|") > 1)')
    expect(labels(result)).toEqual(['cell_column'])
    expect(option(result, 'cell_column')?.detail).toBe('Float64 · this cell')
  })
})

describe('frame names', () => {
  it('offers the frames named above and ff', async () => {
    expect(labels(await offered('x = sa|'))).toEqual(['sales', 'ff'])
  })
})

describe('reading the chain before a dot', () => {
  it('splits it into its calls, past strings and lines', () => {
    const doc = 'x = sales.filter(ff.col("a)").is_in([1, 2]))\n    .sort("b")'
    expect(chainBefore(doc, doc.length)).toEqual([
      { name: 'sales', call: false },
      { name: 'filter', call: true },
      { name: 'sort', call: true }
    ])
  })

  it('classifies only the chains the dialect has', () => {
    const isFrame = (name: string) => name === 'sales'
    expect(classify([{ name: 'ff', call: false }, { name: 'Int64', call: false }], isFrame)).toBeNull()
    expect(classify([{ name: 'datetime', call: false }], isFrame)).toBe('datetime')
    expect(classify([{ name: 'sales', call: false }, { name: 'group_by', call: true }], isFrame)).toBe('GroupByFrame')
    expect(classify([{ name: 'ff', call: false }, { name: 'col', call: true }, { name: 'dt', call: false }], isFrame)).toBe(
      'DateTimeNS'
    )
  })
})
