/**
 * Completions in a notebook cell: the `ff.` names, the methods of the value before a dot,
 * the frames the cells above name, and column names inside a string a frame method reads.
 *
 * Names and methods come from the engine's allowlist (`notebook_surface()`), so the editor never
 * offers a call the browser would refuse to read; the calls a push turns into settings rank first.
 * Columns are found the full app's way (`dataframeColumnContext.ts` + `dataframeSchemaInference.ts`,
 * copied byte for byte), with this editor's node schemas standing in for a kernel's last run.
 */

import type { Completion, CompletionResult, CompletionSource } from '@codemirror/autocomplete'
import { resolveColumnContext, type ColumnContext } from './dataframeColumnContext'
import { inferSchema, type CellSource, type SchemaSources } from './dataframeSchemaInference'
import type { SchemaColumn } from './dataframeSchemaTypes'
import flCompletions from './flCompletions.json'

/** What the engine lets a cell write (`notebook_cells.notebook_surface`). */
export interface NotebookSurface {
  ff: string[]
  methods: Record<string, string[]>
  /** `"<kind>.<method>"` for each call a push reads back into settings. */
  pushable: string[]
  /** The node types (flowfile_core's names) a push can change. */
  node_types: string[]
}

export interface CellCompletionContext {
  surface: NotebookSurface | null
  /** The cells above this one, as they read now. */
  priorCells: CellSource[]
  cellId: string
  /** The columns of the frame a name holds on the canvas, or null. */
  frameColumns(name: string): SchemaColumn[] | null
  /** Columns to offer when the frame a string is read on cannot be worked out: this cell's steps and their inputs. */
  cellColumns(): SchemaColumn[]
}

type Kind = 'FlowFrame' | 'GroupByFrame' | 'Expr' | 'StringNS' | 'DateTimeNS' | 'datetime'

interface Segment {
  name: string
  call: boolean
}

const WORD = /^\w*$/
const VALID_DOUBLE = /^[^"\\]*$/
const VALID_SINGLE = /^[^'\\]*$/
const EXPR_HEADS = new Set(['col', 'lit', 'len', 'when'])
const FL_ENTRIES = new Map(flCompletions.ff.map(entry => [entry.name, entry]))
const NAMESPACE_LABEL: Record<Kind, string> = {
  FlowFrame: 'frame',
  GroupByFrame: 'grouped frame',
  Expr: 'expression',
  StringNS: 'text',
  DateTimeNS: 'date',
  datetime: 'datetime'
}

/** Every completion source a cell's editor uses. */
export function notebookCompletionSources(getContext: () => CellCompletionContext): CompletionSource[] {
  return [ffNames(getContext), members(getContext), columns(getContext), frameNames(getContext)]
}

function ffNames(getContext: () => CellCompletionContext): CompletionSource {
  return context => {
    const match = context.matchBefore(/\bff\.\w*$/)
    const surface = getContext().surface
    if (!match || !surface) return null
    const pushable = new Set(surface.pushable.filter(call => call.startsWith('ff.')).map(call => call.slice(3)))
    const options = surface.ff.map(name => {
      const entry = FL_ENTRIES.get(name)
      const option: Completion = { label: name, type: entry?.kind ?? (/^[A-Z]/.test(name) ? 'class' : 'function') }
      if (entry?.signature) option.detail = entry.signature
      if (pushable.has(name)) option.boost = 2
      return option
    })
    return { from: match.from + 3, options, validFor: WORD }
  }
}

function members(getContext: () => CellCompletionContext): CompletionSource {
  return context => {
    const match = context.matchBefore(/\.\w*$/)
    const ctx = getContext()
    if (!match || !ctx.surface) return null
    const doc = context.state.doc.toString()
    const segments = chainBefore(doc, match.from)
    if (!segments || (segments.length === 1 && segments[0].name === 'ff')) return null
    const frames = new Set([...ctx.priorCells.flatMap(cell => assignedNames(cell.code)), ...assignedNames(doc)])
    const kind = classify(segments, name => frames.has(name) || ctx.frameColumns(name) !== null)
    const methods = kind ? ctx.surface.methods[kind] : undefined
    if (!kind || !methods?.length) return null
    const pushable = new Set(ctx.surface.pushable)
    const options = methods.map(method => {
      const reads = pushable.has(`${kind}.${method}`)
      const option: Completion = { label: method, type: kind === 'Expr' && method === 'str' ? 'namespace' : 'method' }
      if (kind === 'FlowFrame') option.detail = reads ? 'a step' : 'change on the canvas'
      else option.detail = NAMESPACE_LABEL[kind]
      if (reads) option.boost = 2
      return option
    })
    return { from: match.from + 1, options, validFor: WORD }
  }
}

function frameNames(getContext: () => CellCompletionContext): CompletionSource {
  return context => {
    const match = context.matchBefore(/[A-Za-z_]\w*$/)
    if (!match || (match.from === match.to && !context.explicit)) return null
    if (match.from > 0 && context.state.doc.sliceString(match.from - 1, match.from) === '.') return null
    const ctx = getContext()
    const names = new Set(ctx.priorCells.flatMap(cell => assignedNames(cell.code)))
    const above = context.state.doc.sliceString(0, context.state.doc.lineAt(match.from).from)
    for (const name of assignedNames(above)) names.add(name)
    if (names.size === 0) return null
    const options: Completion[] = [...names].map(name => ({ label: name, type: 'variable', detail: 'frame' }))
    options.push({ label: 'ff', type: 'namespace', detail: 'import flowfile as ff' })
    return { from: match.from, options, validFor: WORD }
  }
}

function columns(getContext: () => CellCompletionContext): CompletionSource {
  return context => {
    const column = resolveColumnContext(context.state, context.pos)
    if (!column) return null
    const ctx = getContext()
    if (column.receiver.kind === 'bare-col') return toResult(column, fallback(ctx, column.quote))
    const sources: SchemaSources = {
      runtime: name => {
        const found = ctx.frameColumns(name)
        return found ? { columns: found, kind: 'LazyFrame' } : null
      },
      catalogRef: () => null,
      input: () => null,
      isCellOutdated: () => false
    }
    const { schema } = inferSchema(
      column.receiver.node,
      context.state.doc.toString(),
      context.pos,
      ctx.priorCells,
      ctx.cellId,
      sources
    )
    // A subscript on an unknown receiver may not be a frame at all, so it never guesses.
    if (!schema) return column.method === '__getitem__' ? null : toResult(column, fallback(ctx, column.quote))
    return toResult(
      column,
      schema.columns.map(each => toOption(each.name, each.dtype, schema.sourceLabel, column.quote))
    )
  }
}

function fallback(ctx: CellCompletionContext, quote: '"' | "'"): Completion[] {
  return ctx.cellColumns().map(each => toOption(each.name, each.dtype, 'this cell', quote))
}

function toOption(name: string, dtype: string, source: string, quote: '"' | "'"): Completion {
  const option: Completion = {
    label: name,
    type: 'property',
    detail: dtype ? `${dtype} · ${source}` : `· ${source}`,
    boost: 6
  }
  if (name.includes(quote) || name.includes('\\')) option.apply = name.split('\\').join('\\\\').split(quote).join(`\\${quote}`)
  return option
}

function toResult(column: ColumnContext, options: Completion[]): CompletionResult | null {
  if (options.length === 0) return null
  return {
    from: column.contentFrom,
    to: column.contentTo,
    options,
    validFor: column.quote === '"' ? VALID_DOUBLE : VALID_SINGLE
  }
}

/** Top-level names a cell assigns, in order. */
export function assignedNames(code: string): string[] {
  return [...code.matchAll(/^([A-Za-z_]\w*)\s*=(?!=)/gm)].map(match => match[1])
}

/** What a value of `kind` becomes after `segment` is called or read on it. */
function step(kind: Kind, segment: Segment): Kind | null {
  if (kind === 'Expr') {
    if (!segment.call && segment.name === 'str') return 'StringNS'
    if (!segment.call && segment.name === 'dt') return 'DateTimeNS'
    return segment.call ? 'Expr' : null
  }
  if (kind === 'StringNS' || kind === 'DateTimeNS') return segment.call ? 'Expr' : null
  if (kind === 'GroupByFrame') return segment.call && segment.name === 'agg' ? 'FlowFrame' : null
  if (kind === 'FlowFrame' && segment.call) return segment.name === 'group_by' ? 'GroupByFrame' : 'FlowFrame'
  return null
}

/** The kind of value a method chain ends on, or null when it is not one the dialect knows. */
export function classify(segments: Segment[], isFrame: (name: string) => boolean): Kind | null {
  const [root, ...rest] = segments
  if (!root || root.call) return null
  let kind: Kind
  let chain = rest
  if (root.name === 'ff') {
    const head = rest[0]
    if (!head?.call) return null
    kind = EXPR_HEADS.has(head.name) ? 'Expr' : 'FlowFrame'
    chain = rest.slice(1)
  } else if (root.name === 'datetime') {
    return rest.length === 0 ? 'datetime' : null
  } else if (isFrame(root.name)) {
    kind = 'FlowFrame'
  } else {
    return null
  }
  for (const segment of chain) {
    const next = step(kind, segment)
    if (!next) return null
    kind = next
  }
  return kind
}

/**
 * The method chain that ends right before the dot at `dot`, root first: `a.b(...).c` is three
 * segments. Calls may span lines and a chain may break lines around its dots; anything else ends it.
 */
export function chainBefore(doc: string, dot: number): Segment[] | null {
  const segments: Segment[] = []
  let i = skipSpace(doc, dot - 1)
  while (i >= 0) {
    let call = false
    if (doc[i] === ')') {
      const open = openingBracket(doc, i)
      if (open < 0) return null
      call = true
      i = open - 1
    }
    const end = i
    while (i >= 0 && /\w/.test(doc[i])) i--
    if (i === end) return null
    segments.unshift({ name: doc.slice(i + 1, end + 1), call })
    const before = skipSpace(doc, i)
    if (before < 0 || doc[before] !== '.') break
    i = skipSpace(doc, before - 1)
  }
  return segments.length ? segments : null
}

function skipSpace(doc: string, from: number): number {
  let i = from
  while (i >= 0 && /\s/.test(doc[i])) i--
  return i
}

/** Where the bracket closed at `close` opens, skipping strings; -1 when it does not. */
function openingBracket(doc: string, close: number): number {
  let depth = 0
  for (let i = close; i >= 0; i--) {
    const ch = doc[i]
    if (ch === '"' || ch === "'") {
      let j = i - 1
      while (j >= 0 && (doc[j] !== ch || doc[j - 1] === '\\')) j--
      if (j < 0) return -1
      i = j
    } else if (ch === ')' || ch === ']' || ch === '}') {
      depth++
    } else if (ch === '(' || ch === '[' || ch === '{') {
      depth--
      if (depth === 0) return i
    }
  }
  return -1
}
