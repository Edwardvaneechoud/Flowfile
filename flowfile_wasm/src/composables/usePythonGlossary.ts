/**
 * Hover glossary for the generated plain Python.
 *
 * Reading unfamiliar code, the thing that stops you is rarely the shape — it is
 * one name you do not recognise. This puts a one-line answer under the cursor
 * instead of sending you to a search engine, and it only covers identifiers the
 * generator actually emits, so there is nothing here you will not meet.
 */

import { hoverTooltip, tooltips } from '@codemirror/view'
import type { EditorView } from '@codemirror/view'
import type { Extension } from '@codemirror/state'

export interface GlossaryEntry {
  /** What it is, in one line. */
  summary: string
  /** A tiny concrete example; rendered monospace under the summary. */
  example?: string
}

export const PYTHON_GLOSSARY: Record<string, GlossaryEntry> = {
  // --- the shapes this script is built from ---
  sorted: {
    summary: 'Returns a NEW list in order. Give it key= to say what to order by.',
    example: 'sorted(rows, key=lambda r: r["age"])'
  },
  sort: {
    summary: 'Orders a list IN PLACE and returns None. Use sorted() if you want a copy.',
    example: 'rows.sort(key=..., reverse=True)'
  },
  lambda: {
    summary: 'A function with no name, written inline. The bit after the colon is what it returns.',
    example: 'lambda r: r["age"]   is   def f(r): return r["age"]'
  },
  enumerate: {
    summary: 'Walks a list handing you the position and the item together, so you never maintain a counter.',
    example: 'for i, row in enumerate(rows, start=1):'
  },
  setdefault: {
    summary: 'Reads a key, inserting a default first if it is missing. The one-liner for "add to the list at this key".',
    example: 'groups.setdefault(key, []).append(row)'
  },
  get: {
    summary: 'Reads a key, returning None instead of raising when it is absent. row["x"] would raise.',
    example: 'row.get("maybe_missing")   ->  None'
  },
  append: { summary: 'Adds one item to the end of a list.', example: 'out.append(row)' },
  pop: {
    summary: 'Removes a key and returns its value. pop(key, None) will not raise when it is missing.',
    example: 'by_key.pop(key, None)'
  },
  add: { summary: 'Puts one item into a set. Adding something already there does nothing.', example: 'seen.add(key)' },
  items: {
    summary: 'Walks a dict handing you each key and value as a pair.',
    example: 'for column, value in row.items():'
  },
  values: { summary: 'The values of a dict, without their keys.', example: 'list(by_key.values())' },
  set: {
    summary: 'An unordered collection with no duplicates. Checking membership is instant however big it gets.',
    example: 'seen = set();  if key in seen: ...'
  },
  dict: { summary: 'Maps keys to values, and remembers the order keys were first inserted.' },
  tuple: {
    summary: 'An immutable sequence. Unlike a list it can be a dict key or a set member — which is why group keys are tuples.',
    example: '(row["a"],)   <- the comma is what makes it a tuple'
  },
  len: { summary: 'How many items. Works on lists, dicts, sets and strings.' },
  sum: { summary: 'Adds up a sequence of numbers. Summing nothing gives 0.', example: 'sum([])  ->  0' },
  min: {
    summary: 'The smallest item. Pass default= so an empty sequence returns that instead of raising.',
    example: 'min(values, default=None)'
  },
  max: { summary: 'The largest item. Takes the same default= as min().' },
  next: {
    summary: 'Pulls the first item off a generator; the second argument is what to return when there is none.',
    example: 'next((r for r in rows if ...), None)'
  },
  zip: { summary: 'Walks two sequences in step, pairing them up.', example: 'dict(zip(columns, row))' },
  open: {
    summary: 'Opens a file. Used with "with", so it is closed for you even if something raises.',
    example: 'with open(path) as handle:'
  },
  continue: { summary: 'Skip the rest of this loop pass and move to the next item.' },
  None: { summary: 'Python\'s "no value". It is not 0 and not "" — and comparing it to a number raises.' },
  isinstance: { summary: 'Asks whether a value is of a given type.', example: 'isinstance(v, bool)' },
  str: { summary: 'The text type, and the function that converts a value to text.' },
  int: { summary: 'The whole-number type. int("42") converts; int("4.2") raises ValueError.' },
  float: { summary: 'The decimal-number type. float("4.2") converts.' },
  bool: { summary: 'True or False. Note that bool is a kind of int, so check it before int in a type test.' },

  // --- modules ---
  csv: { summary: 'The standard library CSV reader/writer. Handles the quoting rules you would get wrong by hand.' },
  re: { summary: 'Regular expressions. Polars\' str.contains is a regex search, so the loop uses re.search.' },
  statistics: { summary: 'Standard library maths helpers — mean, median and friends.' },
  search: { summary: 'Looks for a regex anywhere in a string; returns None when it does not match.', example: 're.search("W.dget", text)' },
  median: { summary: 'The middle value once sorted. Raises on an empty sequence, hence the wrapper.' },
  startswith: { summary: 'Literal prefix test on a string — not a regex.' },
  endswith: { summary: 'Literal suffix test on a string.' },
  join: {
    summary: 'Glues a sequence of strings together with this string between them.',
    example: '",".join(["a", "b"])  ->  "a,b"'
  },

  // --- helpers this generator writes for you ---
  read_csv_file: {
    summary: 'Generated helper: reads a CSV into a list of dicts and decides each column\'s type.',
    example: 'defined at the top of this script'
  },
  pick_column_type: {
    summary: 'Generated helper: picks one converter for a whole column, the way a CSV reader does.'
  },
  match_type_of: {
    summary: 'Generated helper: reads a filter value written as text as the same type the column holds, so 5 and "5" do not miss each other.'
  },
  values_of: {
    summary: 'Generated helper: the non-null values of one column. Aggregates skip missing values, and this is where that happens.'
  },
  as_text: { summary: 'Generated helper: renders a value the way Polars does when casting to text (True becomes "true").' },
  average: { summary: 'Generated helper: the mean, or None when there is nothing to average.' },
  middle_value: { summary: 'Generated helper: the median, or None when there is nothing to take it of.' },
  write_csv_file: { summary: 'Generated helper: writes a list of dicts to a CSV, taking the columns from the first row.' },
  run_etl_pipeline: { summary: 'The whole pipeline. Every node on the canvas is one block inside it.' }
}

/** Word under the cursor, or null. */
function wordAt(text: string, offset: number): { word: string; from: number; to: number } | null {
  const isWord = (character: string) => /[A-Za-z0-9_]/.test(character)
  if (!isWord(text[offset] ?? '')) return null
  let from = offset
  let to = offset
  while (from > 0 && isWord(text[from - 1])) from--
  while (to < text.length - 1 && isWord(text[to + 1])) to++
  return { word: text.slice(from, to + 1), from, to: to + 1 }
}

// The panel clips anything drawn above the code, so bound the space at the
// editor: on the top visible line CodeMirror then flips the tooltip below
// instead of into the clipped strip, where it renders but cannot be seen.
const editorSpace = (view: EditorView) => ({
  top: Math.max(0, view.scrollDOM.getBoundingClientRect().top),
  left: 0,
  bottom: window.innerHeight,
  right: window.innerWidth
})

/** A CodeMirror extension that explains a known identifier on hover. */
export function glossaryTooltip(): Extension {
  const hover = hoverTooltip((view, pos) => {
    const line = view.state.doc.lineAt(pos)
    const hit = wordAt(line.text, pos - line.from)
    if (!hit) return null
    const entry = PYTHON_GLOSSARY[hit.word]
    if (!entry) return null

    return {
      pos: line.from + hit.from,
      end: line.from + hit.to,
      above: true,
      create() {
        const dom = document.createElement('div')
        dom.className = 'cm-glossary'
        const name = document.createElement('div')
        name.className = 'cm-glossary-name'
        name.textContent = hit.word
        const summary = document.createElement('div')
        summary.className = 'cm-glossary-summary'
        summary.textContent = entry.summary
        dom.append(name, summary)
        if (entry.example) {
          const example = document.createElement('pre')
          example.className = 'cm-glossary-example'
          example.textContent = entry.example
          dom.append(example)
        }
        return { dom }
      }
    }
  })
  return [tooltips({ tooltipSpace: editorSpace }), hover]
}

/** Identifiers the glossary can explain — used to tell the reader it is there. */
export const GLOSSARY_TERMS: ReadonlySet<string> = new Set(Object.keys(PYTHON_GLOSSARY))
