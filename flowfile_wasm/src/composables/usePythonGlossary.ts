/**
 * Hover glossary for the generated plain Python.
 *
 * Reading unfamiliar code, the thing that stops you is rarely the shape — it is
 * one name you do not recognise. This puts a one-line answer under the cursor
 * instead of sending you to a search engine, and it only covers identifiers the
 * generator actually emits, so there is nothing here you will not meet.
 */

import { EditorView, hoverTooltip, showTooltip, tooltips, type Tooltip } from '@codemirror/view'
import { StateField, type EditorState, type Extension } from '@codemirror/state'

export interface GlossaryEntry {
  /** What it is, in one line. */
  summary: string
  /** A tiny concrete example; rendered monospace under the summary. */
  example?: string
}

export const PYTHON_GLOSSARY: Record<string, GlossaryEntry> = {
  // --- keywords and structure ---
  def: {
    summary: 'Defines a function: a named block you can call later. Nothing runs until it is called.',
    example: 'def run_etl_pipeline():'
  },
  return: { summary: 'Hands a value back to whoever called the function, and ends it.' },
  for: {
    summary: 'Walks a sequence one item at a time, running the indented block for each.',
    example: 'for row in rows:'
  },
  if: { summary: 'Runs the indented block only when the condition is true.' },
  else: { summary: 'The branch taken when the if (or the loop) did not fire.' },
  in: {
    summary: 'Two jobs: "walk this sequence" in a for loop, and "is this a member?" as a test.',
    example: 'if key in seen:'
  },
  not: { summary: 'Flips a truth value. `not matches` is true when the list is empty.' },
  and: { summary: 'True when both sides are. Python stops at the first false side, so the left test can guard the right one.' },
  or: { summary: 'True when either side is. `rows or []` also means "rows, unless it is empty/None".' },
  is: {
    summary: 'Identity test. `x is None` is THE way to check for a missing value — == can lie.',
    example: 'row["age"] is not None'
  },
  import: { summary: 'Loads a module so its names can be used, written once at the top of the file.' },
  with: {
    summary: 'Runs a block and guarantees the cleanup after it — here: the file is closed even if something raises.',
    example: 'with open(path) as handle:'
  },
  as: { summary: 'Names the thing just introduced — the opened file in "with open(...) as handle".' },
  try: { summary: 'Attempt the block; if it raises, jump to the except instead of crashing.' },
  except: {
    summary: 'What to do when the try block raised. Naming an error type catches only that kind.',
    example: 'except ValueError:'
  },
  pass: { summary: 'Do nothing. A placeholder where Python demands a statement.' },
  raise: {
    summary: 'Throws an error on purpose, stopping the function right here.',
    example: 'raise NotImplementedError("...")'
  },
  NotImplementedError: {
    summary: 'The error meaning "this part is not written yet".'
  },
  ValueError: { summary: 'Raised when the type is right but the value is not — int("4.2") for instance.' },
  True: { summary: 'The boolean yes. Capitalised — true is a NameError.' },
  False: { summary: 'The boolean no. Note False < True, which is why True/False flags can be sorted.' },
  __name__: {
    summary: 'The module\'s name — "__main__" when the file is run directly, so this guard means "only when run as a script, not when imported".',
    example: 'if __name__ == "__main__":'
  },
  __main__: {
    summary: 'The name Python gives the file you actually ran. The guard around the bottom of this script tests for it.'
  },

  // --- the shapes this script is built from ---
  sorted: {
    summary: 'Returns a NEW list in order. Give it key= to say what to order by.',
    example: 'sorted(rows, key=lambda r: r["age"])'
  },
  sort: {
    summary: 'Orders a list IN PLACE and returns None — use sorted() for a copy. On a Polars frame, the opposite: .sort() returns a NEW sorted frame.',
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
  values: {
    summary: 'The values of a dict, without their keys. In a Polars pivot, values= names the column that fills the grid.',
    example: 'list(by_key.values())'
  },
  set: {
    summary: 'An unordered collection with no duplicates. Checking membership is instant however big it gets.',
    example: 'seen = set();  if key in seen: ...'
  },
  dict: { summary: 'Maps keys to values, and remembers the order keys were first inserted.' },
  tuple: {
    summary: 'An immutable sequence. Unlike a list it can be a dict key or a set member — which is why group keys are tuples.',
    example: '(row["a"],)   <- the comma is what makes it a tuple'
  },
  len: { summary: 'How many items — lists, dicts, sets and strings. Inside a Polars expression, pl.len() is the row count.' },
  sum: {
    summary: 'Adds up a sequence of numbers; summing nothing gives 0. As a Polars aggregation, .sum() totals a column per group, skipping nulls.',
    example: 'sum([])  ->  0'
  },
  min: {
    summary: 'The smallest item; pass default= so an empty sequence returns that instead of raising. In Polars, .min() is the per-group smallest, nulls skipped.',
    example: 'min(values, default=None)'
  },
  max: { summary: 'The largest item — same default= as min(). In Polars, .max() is the per-group largest.' },
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
  str: {
    summary: 'The text type, and the function that converts a value to text. On a Polars column, .str opens the string methods.',
    example: 'pl.col("x").str.contains("...")'
  },
  int: { summary: 'The whole-number type. int("42") converts; int("4.2") raises ValueError.' },
  float: { summary: 'The decimal-number type. float("4.2") converts.' },
  bool: { summary: 'True or False. Note that bool is a kind of int, so check it before int in a type test.' },
  list: { summary: 'An ordered sequence. list(x) also copies — handy before sorting in place.' },
  range: { summary: 'The numbers 0, 1, 2, … up to (not including) the end you give it.', example: 'range(3)  ->  0, 1, 2' },
  any: {
    summary: 'True if at least one element is truthy. Empty in, False out.',
    example: 'any(value is None for value in key)'
  },
  all: { summary: 'True only if every element is truthy. Empty in, True out.' },
  print: { summary: 'Writes a value to the terminal. How the bottom of this script shows its rows.' },

  // --- modules ---
  csv: { summary: 'The standard library CSV reader/writer. Handles the quoting rules you would get wrong by hand.' },
  re: { summary: 'Regular expressions. Polars\' str.contains is a regex search, so the loop uses re.search.' },
  statistics: { summary: 'Standard library maths helpers — mean, median and friends.' },
  search: { summary: 'Looks for a regex anywhere in a string; returns None when it does not match.', example: 're.search("W.dget", text)' },
  median: {
    summary: 'The middle value once sorted. statistics.median raises on nothing (hence the wrapper); Polars\' .median() just gives null.'
  },
  startswith: { summary: 'Literal prefix test on a string — not a regex.' },
  endswith: { summary: 'Literal suffix test on a string.' },
  strip: { summary: 'A copy of the string with the whitespace shaved off both ends.' },
  lower: { summary: 'A copy of the string in lowercase — the usual first step of a case-insensitive compare.' },
  reader: { summary: 'csv.reader walks a CSV file handing you each row as a list of strings.' },
  DictWriter: {
    summary: 'csv writer that takes each row as a dict. fieldnames fixes the column order, so header and rows cannot drift apart.',
    example: 'csv.DictWriter(handle, fieldnames=[...])'
  },
  writeheader: { summary: 'Writes the header row, taken from the fieldnames the writer was given.' },
  writerows: { summary: 'Writes every row in the list, one CSV line each.' },
  fieldnames: { summary: 'The column names the CSV writer will use, in the order they should appear.' },
  join: {
    summary: 'Two meanings: on a string, glues a sequence together with it between the parts; on a Polars frame, matches the rows of two tables on key columns.',
    example: 'left.join(right, left_on=["id"], right_on=["id"], how="left")'
  },

  // --- the Polars flavour (bare-word keys: the tokenizer splits on dots) ---
  pl: {
    summary: 'The Polars library, imported as pl — the dataframe engine the canvas itself runs on.',
    example: 'import polars as pl'
  },
  LazyFrame: {
    summary: 'A recipe for a table, not the table. Steps chain onto it instantly; nothing computes until .collect().'
  },
  lazy: { summary: 'Turns an eager DataFrame into a LazyFrame, so later steps extend the recipe instead of running now.' },
  collect: {
    summary: 'Runs the whole recipe and returns the actual table — the one moment Polars does the work.',
    example: 'run_etl_pipeline().collect()'
  },
  scan_csv: {
    summary: 'Starts a lazy CSV read. Nothing loads yet, and only the columns the recipe needs ever will.',
    example: 'pl.scan_csv("data.csv")'
  },
  scan_parquet: { summary: 'The lazy Parquet read — same idea as scan_csv, for a columnar file format.' },
  read_excel: { summary: 'Reads an Excel sheet into a DataFrame — eager, because the file format has no lazy path.' },
  col: {
    summary: 'Names a column inside an expression. It is a reference, not the data — the data arrives at collect().',
    example: 'pl.col("age") > 30'
  },
  lit: { summary: 'A plain value lifted into an expression, so it can sit beside columns.', example: 'pl.lit(1)' },
  alias: {
    summary: 'Names the result of an expression — the column it will appear as.',
    example: 'pl.col("a").sum().alias("total")'
  },
  cast: { summary: 'Converts a column to another type.', example: 'pl.col("a").cast(pl.Int64)' },
  filter: {
    summary: 'Keeps the rows where the expression is true — the whole guard-loop in one call.',
    example: 'source.filter(pl.col("age") > 30)'
  },
  select: { summary: 'Keeps exactly the columns you name, in that order — the rebuild-each-row loop in one call.' },
  with_columns: {
    summary: 'Adds or replaces columns, keeping the rest of the table as it is.',
    example: 'df.with_columns((pl.col("a") * 2).alias("b"))'
  },
  group_by: {
    summary: 'One output row per distinct key — the buckets dict, built for you.',
    example: 'df.group_by(["product"]).agg([...])'
  },
  agg: { summary: 'The summaries to compute per group — the group-by loop\'s second pass, as expressions.' },
  unique: { summary: 'Drops duplicate rows — the seen set, built in. subset= names the columns that count as "the same".' },
  head: { summary: 'The first n rows of the table.', example: 'df.head(5)' },
  with_row_index: {
    summary: 'Adds a counting column — enumerate for tables.',
    example: 'df.with_row_index(name="record_id", offset=1)'
  },
  concat: { summary: 'Stacks tables on top of each other — the union. (On strings in an aggregation, it joins values.)' },
  sink_csv: {
    summary: 'Runs the recipe and writes the result to a CSV file in chunks — no whole-table detour through memory when the plan can stream.',
    example: 'filtered.sink_csv("out.csv")'
  },
  sink_parquet: { summary: 'Runs the recipe and streams the result into a Parquet file.' },
  write_excel: { summary: 'Writes a DataFrame to an Excel file.' },
  rename: { summary: 'Renames columns from an {old: new} mapping.', example: 'df.rename({"a": "b"})' },
  drop: { summary: 'Removes the named columns and keeps everything else.' },
  pivot: { summary: 'Turns one column\'s values into new columns — the grid of buckets, in one call.' },
  unpivot: { summary: 'The reverse of pivot: melts wide columns back into long (variable, value) rows.' },
  descending: {
    summary: 'sort\'s per-column direction — True means largest first.',
    example: 'df.sort(["age"], descending=[True])'
  },
  subset: { summary: 'Which columns unique() compares — rows equal on just these count as duplicates.' },
  keep: { summary: 'Which duplicate unique() keeps: "first", "last", "any" or "none".' },
  how: { summary: 'The join strategy: "inner" keeps matches only, "left" keeps every left row, "semi"/"anti" filter by match.' },
  left_on: { summary: 'The join key column(s) on the left table; right_on is its partner.' },
  right_on: { summary: 'The join key column(s) on the right table; pairs with left_on.' },
  suffix: { summary: 'Text appended to clashing column names — how a join tells the right table\'s columns apart.' },
  is_null: { summary: 'True where the value is missing.' },
  is_not_null: { summary: 'True where the value is present.' },
  is_in: { summary: 'Membership test against a list of values.', example: 'pl.col("x").is_in(["a", "b"])' },
  is_between: { summary: 'True where the value lies inside the range, ends included.' },
  contains: {
    summary: 'Regex search inside a string column — the reason the plain loop reaches for re.search.',
    example: 'pl.col("x").str.contains("W.dget")'
  },
  starts_with: { summary: 'Literal prefix test on a string column — not a regex.' },
  ends_with: { summary: 'Literal suffix test on a string column.' },
  mean: { summary: 'The per-group average. Nulls are skipped; averaging nothing gives null.' },
  count: { summary: 'How many values in the group — a missing value does not count. (pl.len() is the row count.)' },
  first: { summary: 'The first value in the group — positional, so a null counts as a value.' },
  last: { summary: 'The last value in the group, same rule as first.' },
  n_unique: { summary: 'How many distinct values — "missing" counts as one of them.' },
  int_range: { summary: 'A counting expression: the numbers 0, 1, 2, … as a column.' },
  shuffle: { summary: 'Randomly reorders values — how a random sample picks its rows.' },
  simple_function_to_expr: {
    summary: 'Translates a Flowfile formula string into a Polars expression at run time.',
    example: 'from polars_expr_transformer import simple_function_to_expr'
  },
  cs: { summary: 'Polars column selectors — ways to name groups of columns at once, like cs.all() or cs.numeric().' },
  Utf8: { summary: 'Polars\' text column type.' },
  Int64: { summary: 'Polars\' whole-number column type.' },
  Float64: { summary: 'Polars\' decimal-number column type.' },
  Boolean: { summary: 'Polars\' true/false column type.' },
  Date: { summary: 'Polars\' calendar-date column type — a day, no time attached.' },
  Datetime: { summary: 'Polars\' date-and-time column type.' },

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

/** Hover never fires on a touch screen, so there the glossary rides the cursor instead. */
export const COARSE_POINTER =
  typeof matchMedia !== 'undefined' && matchMedia('(pointer: coarse)').matches

function glossaryDom(word: string, entry: GlossaryEntry): HTMLElement {
  const dom = document.createElement('div')
  dom.className = 'cm-glossary'
  const name = document.createElement('div')
  name.className = 'cm-glossary-name'
  name.textContent = word
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
  return dom
}

function entryAt(state: EditorState, pos: number): { word: string; from: number; to: number; entry: GlossaryEntry } | null {
  const line = state.doc.lineAt(pos)
  const hit = wordAt(line.text, pos - line.from)
  if (!hit) return null
  // hasOwnProperty guard: a plain object lookup would also "find" inherited
  // names like constructor or toString in a user-edited script.
  if (!Object.prototype.hasOwnProperty.call(PYTHON_GLOSSARY, hit.word)) return null
  return { word: hit.word, from: line.from + hit.from, to: line.from + hit.to, entry: PYTHON_GLOSSARY[hit.word] }
}

// Tapping a word puts the cursor in it; that is the touch equivalent of hover.
const cursorGlossary = StateField.define<Tooltip | null>({
  create: state => cursorTooltipAt(state),
  update(value, tr) {
    if (!tr.docChanged && !tr.selection) return value
    return cursorTooltipAt(tr.state)
  },
  provide: field => showTooltip.from(field)
})

function cursorTooltipAt(state: EditorState): Tooltip | null {
  const range = state.selection.main
  if (!range.empty) return null
  const hit = entryAt(state, Math.min(range.head, Math.max(0, state.doc.length - 1)))
  if (!hit) return null
  return {
    pos: hit.from,
    end: hit.to,
    above: true,
    create: () => ({ dom: glossaryDom(hit.word, hit.entry) })
  }
}

const glossaryTheme = EditorView.baseTheme({
  '.cm-glossary': {
    maxWidth: '340px',
    padding: '8px 10px',
    borderRadius: '6px',
    border: '1px solid rgba(148, 163, 184, 0.35)',
    background: '#1e293b',
    color: '#e2e8f0',
    fontFamily: 'system-ui, sans-serif',
    lineHeight: '1.5'
  },
  '.cm-glossary-name': {
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '12px',
    fontWeight: '600',
    color: '#7dd3fc',
    marginBottom: '3px'
  },
  '.cm-glossary-summary': { fontSize: '12px' },
  '.cm-glossary-example': {
    margin: '6px 0 0',
    padding: '5px 7px',
    borderRadius: '4px',
    background: 'rgba(15, 23, 42, 0.85)',
    fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
    fontSize: '11px',
    whiteSpace: 'pre-wrap',
    color: '#cbd5e1'
  }
})

/** A CodeMirror extension that explains a known identifier on hover — or, on touch devices, at the tap. */
export function glossaryTooltip(): Extension {
  const hover = hoverTooltip((view, pos) => {
    const hit = entryAt(view.state, pos)
    if (!hit) return null
    return {
      pos: hit.from,
      end: hit.to,
      above: true,
      create: () => ({ dom: glossaryDom(hit.word, hit.entry) })
    }
  })
  const extensions: Extension[] = [tooltips({ tooltipSpace: editorSpace }), hover, glossaryTheme]
  if (COARSE_POINTER) extensions.push(cursorGlossary)
  return extensions
}
