/**
 * Plain-Python Code Generation for WASM
 *
 * A teaching flavour of the code generator: the same flow, rewritten with no
 * dataframe library at all. Every table is a `list[dict]` (one dict per row)
 * and every node is an explicit loop.
 *
 * Semantics are matched against the WASM execution engine (`src/pyodide/engine/`),
 * not against flowfile_core — the two differ in places (aggregate set, join
 * suffixing, unique strategies), and the engine is what the canvas actually ran.
 */

import { FlowToPolarsConverter, toPythonValue } from './useCodeGeneration'
import type { CodeGenerationOptions } from './useCodeGeneration'
import type {
  FlowNode,
  NodeReadSettings,
  NodeManualInputSettings,
  NodeExternalDataSettings,
  NodeReadFromCatalogSettings,
  NodeFilterSettings,
  NodeSelectSettings,
  NodeSortSettings,
  NodeUniqueSettings,
  NodeGroupBySettings,
  NodeJoinSettings,
  NodeCrossJoinSettings,
  NodeUnionSettings,
  NodeRecordIdSettings,
  NodeSampleSettings,
  NodeDynamicRenameSettings,
  NodePivotSettings,
  NodeUnpivotSettings,
  NodeOutputSettings,
  NodeWriteToCatalogSettings,
  InputCsvTable,
  FilterOperator,
  AggType
} from '../types'

type InputVars = Record<string, string>

/** Thrown by an emitter that cannot honour this particular configuration. */
class PlainPythonUnsupported extends Error {}

/** Join strategies with an honest loop form. `right`/`full`/`outer` are left as exercises. */
const JOIN_HOWS_SUPPORTED = new Set(['inner', 'left', 'semi', 'anti'])

/**
 * The aggregations pivot really implements. The settings panel also offers
 * n_unique and concat, but the engine has no case for them and quietly sums
 * instead — so those become an exercise rather than a lie.
 */
const PIVOT_AGGS_SUPPORTED = new Set(['sum', 'mean', 'min', 'max', 'count', 'first', 'last', 'median'])

/** Plain-English "what this node does", shown above the snippet in the settings drawer. */
export const NODE_EXPLANATIONS: Record<string, string> = {
  manual_input:
    'Holds a small table you typed in by hand. In plain Python that is just a list of dictionaries written out as a literal — the starting point for everything below it.',
  read:
    'Reads a file into rows. A CSV has no types: every cell arrives as text, and something has to decide that "age" is a number. That decision is normally made for you; here it is written out.',
  external_data:
    'Takes a table the host application handed to the editor. Standalone code has no host, so the generated script reads the same data from a CSV file next to it.',
  read_from_catalog:
    'Reads a table you saved in the Catalog. Standalone code has no Catalog, so the generated script reads the same data from a CSV file next to it.',
  filter:
    'Keeps the rows that match a condition and drops the rest. The loop form is the one every language shares: walk the rows, test each one, collect the keepers.',
  select:
    'Chooses which columns survive, renames them, and fixes their order. Each row is rebuilt as a new dictionary with only the keys you asked for.',
  sort:
    'Puts the rows in order. Rather than comparing rows by hand, you hand the sort a *key function* that reduces each row to the value it should be ordered by.',
  unique:
    'Drops duplicate rows. The pattern is a `seen` set: remember every key you have already emitted, and skip a row whose key is in it.',
  group_by:
    'Collapses many rows into one row per group. Two passes: file every row under its key in a dictionary, then walk the groups and summarise each one. This accumulator-dict pattern is worth knowing by heart.',
  join:
    'Matches rows in one table against rows in another. Doing it with nested loops is O(n×m); building a dictionary from the right-hand table first — a hash index — makes it O(n+m). That difference is the lesson.',
  cross_join:
    'Pairs every row on the left with every row on the right. This is the one join that really is just two nested loops, and it is why the result gets big so fast.',
  union:
    'Stacks the rows of several tables into one. The only real question is what to do about columns that not every table has.',
  record_id:
    'Numbers the rows. `enumerate` gives you the counter and the row together, so you never have to maintain an index variable yourself.',
  head:
    'Takes a sample of the rows. Set to "first", that is a Python slice — and slicing a list never raises when you ask for more rows than exist. Set to one of the random methods it has no plain-Python form, because the engine uses a seeded shuffle this script cannot reproduce row for row.',
  sample:
    'Takes a sample of the rows. Set to "first", that is a Python slice — and slicing a list never raises when you ask for more rows than exist. Set to one of the random methods it has no plain-Python form, because the engine uses a seeded shuffle this script cannot reproduce row for row.',
  dynamic_rename:
    'Renames many columns with one rule instead of one at a time. The rows are rebuilt with new keys; the values are untouched.',
  unpivot:
    'Turns wide data into long data: one row per (row, column) pair. Note the loop order — column first, then rows — which is what fixes the order of the output.',
  explore_data: 'Opens the table in the data explorer. It changes nothing, so it generates no code.',
  output:
    'Writes the rows to a file. Python ships a `csv` module for exactly this, and it handles the quoting rules you would otherwise get wrong.',
  external_output: 'Hands the rows back to the host application. It changes nothing, so it generates no code.',
  write_to_catalog:
    'Saves the rows as a Catalog table. Standalone code has no Catalog, so the generated script writes the same rows to a CSV file.',
  formula:
    'Evaluates a Flowfile expression against every row. There is no short loop equivalent, because reproducing it in general means writing an expression evaluator — so this node becomes an exercise instead.',
  polars_code:
    'Runs Polars code you wrote yourself. It is already Python, and it is already a dataframe library, so there is nothing to translate.',
  pivot:
    'Turns long data into wide data, inventing one column per distinct value. It is a group-by with two keys instead of one — which row, and which column — so the accumulator gains a second level, and the column names have to be collected before any row can be written.'
}

export interface PlainStep {
  nodeId: number
  nodeType: string
  /** The variable this step binds, after boundary renaming. */
  varName: string
  /** The variables this step reads, after boundary renaming. */
  inputVars: string[]
  concept: string
  /** 1-based inclusive line range of this step's block in the finished script. */
  lineStart: number
  lineEnd: number
}

/**
 * Background for the *pattern*, not the node.
 *
 * A code comment can say what a line does; it cannot say why the shape is what
 * it is, or where else you will meet it. `sketch` is a tiny worked example,
 * rendered in a monospace block — small enough to hold in your head.
 */
export interface Concept {
  title: string
  /** One idea per paragraph. Keep them short; this is read standing up. */
  body: string[]
  sketch?: string[]
  /** The thing to take away and reuse elsewhere. */
  takeaway?: string
}

export const CONCEPTS: Record<string, Concept> = {
  'list-of-dicts': {
    title: 'A table is a list of dicts',
    body: [
      'Every table in this script is a list, and every row in it is a dictionary keyed by column name. That is the whole data structure — there is nothing else to learn.',
      'A dataframe stores each column together instead. That is what makes it fast, and it is why its operations are described a column at a time rather than a row at a time.'
    ],
    sketch: [
      'rows = [',
      '    {"product": "Widget", "revenue": 100},',
      '    {"product": "Gadget", "revenue": 200},',
      ']',
      '',
      'rows[0]["product"]   ->  "Widget"',
      'len(rows)            ->  2'
    ],
    takeaway: 'Row-shaped data is easy to read and slow to compute on. That trade is the reason dataframes exist.'
  },
  'csv-typing': {
    title: 'A CSV has no types',
    body: [
      'Every cell in a CSV file is text. The number 42 arrives as the two characters "4" and "2". Something has to decide it is a number, and that something is normally hidden from you.',
      'The decision belongs to the whole column, not to one cell: a single stray word in a column of numbers makes the entire column text. That is why the helper scans every value before choosing.'
    ],
    sketch: [
      'file:      age',
      '           30',
      '           41',
      '',
      'as text:   ["30", "41"]       "30" < "41"  ...but "9" > "41"',
      'as ints:   [30, 41]           correct ordering',
      '',
      'one bad cell changes everything:',
      '           ["30", "41", "n/a"]  ->  stays text'
    ],
    takeaway: 'Reading a file is never just reading. Somebody guessed the types for you.'
  },
  'guard-loop': {
    title: 'Filtering is walk, test, keep',
    body: [
      'Start with an empty list. Walk the rows one at a time, test each one, and append the ones that pass. Three lines, and the same three lines in every language you will ever use.',
      'The fiddly part is missing values. In Python None > 5 is an error and None != 5 is True; in a dataframe both quietly produce "unknown", and the row is dropped. Where the loop above carries an extra "is not None" test, that rule is what put it there — and where the comparison is already safe, it does not.'
    ],
    sketch: [
      'kept = []',
      'for row in rows:',
      '    if row["revenue"] > 100:',
      '        kept.append(row)',
      '',
      'same thing, shorter:',
      'kept = [r for r in rows if r["revenue"] > 100]'
    ],
    takeaway: 'Build a new list rather than deleting from the one you are walking. Mutating a list while looping over it skips elements.'
  },
  'rebuild-row': {
    title: 'Selecting rebuilds each row',
    body: [
      'You are not deleting columns; you are making a new dictionary that holds only the keys you asked for. Renaming comes free, because you simply write a different key.',
      'It also fixes the order. A dict remembers the order you inserted keys, so the order you write them here is the order they come out.'
    ],
    sketch: [
      'out = []',
      'for row in rows:',
      '    out.append({',
      '        "name": row["full_name"],   # renamed',
      '        "age":  row["age"],',
      '    })                              # everything else is gone'
    ]
  },
  'key-function': {
    title: 'Sorting by a key, not a comparison',
    body: [
      'Most languages make you supply a comparator: given two rows, work out which comes first, and return -1, 0 or 1. It is tedious and easy to get subtly wrong. Python asks for something smaller — a function that turns one row into the value it should be ordered by. You say what to sort on; the sort does the comparing.',
      'Because the key can return anything, several columns cost nothing extra. Return a tuple and you are done: Python compares tuples left to right and only looks at the next item when the previous one ties. No nesting, no comparator.',
      'The "is None" in the key looks like noise until you delete it. None < 5 does not come back False — it raises TypeError, because Python refuses to order a missing value against a number. Booleans, though, compare happily: False < True. So the key puts the flag first, the flag decides, and the value is never asked.',
      'Stability is the quiet one. Python never reorders rows whose keys tie, so a sort by name followed by a sort by department leaves each department alphabetical. That is what lets you build any ordering you like out of one-line sorts — and it is the only reason sorting one column at a time works at all.'
    ],
    sketch: [
      'one column',
      '    sorted(rows, key=lambda r: r["age"])',
      '',
      'two columns — free, because a tuple is a value',
      '    sorted(rows, key=lambda r: (r["city"], r["age"]))',
      '    ties on city?  then compare age',
      '',
      'why the flag is there',
      '    None < 5      TypeError: \'<\' not supported',
      '    False < True  True            <- booleans do compare',
      '    key=lambda r: (r["age"] is None, r["age"])',
      '',
      'stability, and what it buys you',
      '    [b1, a1, b2]  sorted by letter',
      ' -> [a1, b1, b2]  b1 is still before b2',
      '',
      '    so: sort by age, then by city',
      '     -> cities in order, ages in order within each city'
    ],
    takeaway:
      'Sort by the least important column first and the most important last. Stability preserves the earlier passes, so you never need a comparator.'
  },
  'seen-set': {
    title: 'The `seen` set',
    body: [
      'To drop duplicates, remember what you have already emitted. A set is the right container because asking "is this in it" costs the same whether it holds ten items or ten million.',
      'A list would also work and would be catastrophically slower: checking membership in a list means looking at every element. On 100k rows that is the difference between instant and a coffee break.'
    ],
    sketch: [
      'seen = set()',
      'out = []',
      'for row in rows:',
      '    key = (row["email"],)',
      '    if key in seen:',
      '        continue',
      '    seen.add(key)',
      '    out.append(row)'
    ],
    takeaway: 'set for "have I seen this", dict for "what did I see with it". Both are ~instant lookups; a list is not.'
  },
  'accumulator-dict': {
    title: 'The accumulator dict',
    body: [
      'You have many rows and you want one row per group. You cannot know a group is finished until you have looked at every row — so you make two passes.',
      'First pass: file each row under its key in a dictionary. Second pass: walk the keys and summarise each list. Nothing clever, and it is the shape of every group-by, word count and tally you will ever write.',
      'Most aggregates skip missing values, which is why they run over values_of() rather than the rows. The positional ones do not: first and last hand back whatever is actually there, missing or not, and n_unique counts "missing" as one of the distinct values.',
      'Summing an empty list gives 0, but the average of nothing is not 0 — it is undefined, so it comes back as None.',
      'The key is a tuple — note the trailing comma in (row["x"],), which is what makes a one-item tuple rather than just brackets. Tuples are used because they can be dictionary keys and lists cannot, and because grouping by two columns then needs no extra code at all.'
    ],
    sketch: [
      'rows:  Widget 100 | Gadget 200 | Widget 150',
      '',
      'pass 1 — file every row under its key',
      '    groups = {}',
      '    for row in rows:',
      '        groups.setdefault(row["product"], []).append(row)',
      '',
      '    "Widget" -> [Widget 100, Widget 150]',
      '    "Gadget" -> [Gadget 200]',
      '',
      'pass 2 — summarise each list',
      '    "Widget" -> 100 + 150 = 250',
      '    "Gadget" ->       200 = 200'
    ],
    takeaway: 'setdefault(key, []).append(x) is the one-liner for "add to the list at this key, making it if needed".'
  },
  'nested-accumulator': {
    title: 'A dict of dicts is a grid',
    body: [
      'A group-by files each row under one key. A pivot files it under two — the row it belongs to and the column it belongs to — so the accumulator grows a second level: a dict of rows, each holding a dict of cells, each cell the list of rows that landed on that square.',
      'You cannot write the first output row until you have read the last input row. Any row is free to invent a column no earlier row mentioned, and every row of a table has to carry the same keys, so the labels get a pass of their own before a single output row is built.',
      'An empty cell and an absent cell are different answers. Summing no values gives 0; a combination that never occurred is not 0 but unknown, and comes back as None. The loop can only tell the two apart by asking whether the key is there — never by counting what is under it.',
      'The column names depend on how many boxes you ticked. One aggregation names each column after the value alone; ask for two and the names have to say which is which, so they become value_sum and value_mean.'
    ],
    sketch: [
      'rows:  r1 colour 3 | r1 size 5 | r2 size 1 | r2 size 7 | r3 colour -',
      '       one row per id, one column per label, each cell = sum',
      '',
      '1. collect the labels first — any row may invent one',
      '       labels = ["colour", "size"]',
      '',
      '2. file every row under both of its keys',
      '       cells = {"r1": {"colour": [3], "size": [5]},',
      '                "r2": {               "size": [1, 7]},',
      '                "r3": {"colour": [-]}}',
      '',
      '3. now every row can be written, and they all line up',
      '            colour   size',
      '       r1        3      5',
      '       r2     None      8   <- 1 + 7; r2 never mentioned colour',
      '       r3        0   None   <- a cell holding nothing sums to 0,',
      '                               a cell that was never there is None'
    ],
    takeaway:
      'Collect the column labels in a pass of their own. A table whose rows disagree about their keys is not a table.'
  },
  'hash-index': {
    title: 'A join is a hash index',
    body: [
      'The obvious way to match two tables is a loop inside a loop: for every row on the left, scan the whole right table. That is rows x rows of work — 1000 against 1000 is a million comparisons.',
      'Instead, walk the right table once and file it into a dictionary by its key. Now each left row is a single lookup. That is rows + rows: 2000 instead of 1000000. Same answer, and it is the reason real databases talk about "hash joins".',
      'Missing keys match nothing at all — not even another missing key. Two unknowns are not the same unknown, so the loop skips them on both sides.'
    ],
    sketch: [
      'slow — rows x rows',
      '    for row in left:',
      '        for other in right:',
      '            if row["id"] == other["id"]: ...',
      '',
      'fast — rows + rows',
      '    index = {}',
      '    for other in right:',
      '        index.setdefault(other["id"], []).append(other)',
      '',
      '    for row in left:',
      '        for other in index.get(row["id"], []): ...'
    ],
    takeaway: 'When you catch yourself scanning one list inside a loop over another, build a dict instead.'
  },
  'nested-loop': {
    title: 'The one join that really is two loops',
    body: [
      'A cross join pairs every row on the left with every row on the right, so there is no key to index and nothing to be clever about. Two nested loops is the honest implementation.',
      'It is also the one to be careful with: 1000 rows against 1000 rows is a million rows out. The output size is the product, not the sum.'
    ],
    sketch: ['for row in left:          # 3 rows', '    for other in right:   # 4 rows', '        ...               # 12 rows out']
  },
  'column-union': {
    title: 'Stacking tables that disagree',
    body: [
      'If both tables have the same columns, stacking them is just list addition. The interesting case is when they do not.',
      'Then you collect every column name that appears anywhere, and rebuild each row against that full set — filling in None wherever a table had nothing to say. row.get(column) does the work, because .get returns None instead of raising when a key is missing.'
    ],
    sketch: [
      'table A: id, name',
      'table B: id, score',
      '',
      'all columns: id, name, score',
      '',
      'A row ->  {"id": 1, "name": "x", "score": None}',
      'B row ->  {"id": 2, "name": None, "score": 10}'
    ],
    takeaway: 'row["k"] raises when the key is missing; row.get("k") returns None. Pick deliberately.'
  },
  enumerate: {
    title: 'enumerate, not a counter',
    body: [
      'You could keep an integer, add one at the bottom of the loop, and hope you never forget. enumerate() hands you the counter and the item together, so there is nothing to keep in step.',
      'It takes a start value, which is how the numbering begins wherever the node says it should.'
    ],
    sketch: [
      "for i, row in enumerate(rows, start=1):",
      '    ...',
      '',
      '{**row}  copies a dict;  {"nr": i, **row}  puts nr first'
    ]
  },
  slice: {
    title: 'Slicing never overruns',
    body: [
      'rows[:5] gives the first five. If there are only two, you get two — asking a slice for more than exists is not an error, unlike indexing past the end.',
      'That forgiving behaviour is why "take a sample" needs no length check.'
    ],
    sketch: ['rows[:5]    first five (or fewer)', 'rows[-5:]   last five', 'rows[5]     IndexError if it is not there']
  },
  'key-rewrite': {
    title: 'Renaming is rewriting keys',
    body: [
      'The values never move. You walk each row and build a new dictionary where some keys have been rewritten and the rest are copied through.',
      'A dict comprehension says that in one line: for every key and value, decide what the key should be, keep the value as it is.'
    ],
    sketch: [
      '{',
      '    ("x_" + column) if column in targets else column: value',
      '    for column, value in row.items()',
      '}'
    ]
  },
  'wide-to-long': {
    title: 'Wide to long',
    body: [
      'One row with four quarterly columns becomes four rows, each naming the quarter it came from. The identifying columns are repeated on every one.',
      'Watch the loop order: columns on the outside, rows on the inside. That is what puts all of Q1 together before any of Q2, and it is the only reason the output order is what it is.'
    ],
    sketch: [
      'before:  id  q1  q2',
      '          1  10  30',
      '          2  20  40',
      '',
      'after:   id  variable  value',
      '          1  q1        10',
      '          2  q1        20      <- all of q1 first',
      '          1  q2        30',
      '          2  q2        40'
    ]
  },
  'write-csv': {
    title: 'Writing a CSV is not print',
    body: [
      'You could join values with commas, and it would work until a value contains a comma, a quote or a newline. Then it silently produces a broken file.',
      'The csv module knows the quoting rules. DictWriter also takes the column order from the fieldnames you give it, so the header and the rows cannot drift apart.'
    ],
    sketch: [
      'writer = csv.DictWriter(handle, fieldnames=list(rows[0]))',
      'writer.writeheader()',
      'writer.writerows(rows)',
      '',
      'value:   he said "hi", loudly',
      'written: "he said ""hi"", loudly"'
    ],
    takeaway: 'Never hand-roll CSV output. The edge cases are not worth it.'
  },
  exercise: {
    title: 'Over to you',
    body: [
      'This node has no short loop equivalent, so the script leaves you a function that raises, plus the rule it is supposed to apply.',
      'The steps before this one still show their data, so you can see exactly what is coming in. The script above is editable — fill in the function, then press ▶ to run it.',
      'Everything downstream of here has no data yet, for the obvious reason: the script stopped at this line.'
    ],
    takeaway: 'Return a new list of dicts. The rest of the script does not care how you build it.'
  }
}

/** Which pattern a node type is an instance of. */
export const CONCEPT_FOR_NODE: Record<string, string> = {
  manual_input: 'list-of-dicts',
  read: 'csv-typing',
  external_data: 'csv-typing',
  read_from_catalog: 'csv-typing',
  filter: 'guard-loop',
  select: 'rebuild-row',
  sort: 'key-function',
  unique: 'seen-set',
  group_by: 'accumulator-dict',
  pivot: 'nested-accumulator',
  join: 'hash-index',
  cross_join: 'nested-loop',
  union: 'column-union',
  record_id: 'enumerate',
  head: 'slice',
  sample: 'slice',
  dynamic_rename: 'key-rewrite',
  unpivot: 'wide-to-long',
  output: 'write-csv',
  write_to_catalog: 'write-csv'
}

/** Why a node type has no loop form, quoted into its exercise stub. */
const STUB_REASONS: Record<string, string> = {
  formula: 'Flowfile evaluates this with its expression engine, so there is no fixed loop to show.',
  polars_code: 'This node is already Python — and already a dataframe library.'
}

/** Module-level helper functions, emitted only when a node actually needs one. */
const HELPER_SOURCES: Record<string, string[]> = {
  read_csv_file: [
    'def read_csv_file(path, delimiter=",", has_header=True, skip_rows=0, infer_types=True):',
    '    """Read a CSV file into a list of dicts, one dict per row."""',
    '    with open(path, newline="", encoding="utf-8") as handle:',
    '        rows = list(csv.reader(handle, delimiter=delimiter))',
    '    rows = rows[skip_rows:]',
    '    if not rows:',
    '        return []',
    '    if has_header:',
    '        columns, rows = rows[0], rows[1:]',
    '    else:',
    '        columns = ["column_" + str(i + 1) for i in range(len(rows[0]))]',
    '    table = [dict(zip(columns, row)) for row in rows]',
    '    if infer_types:',
    '        for column in columns:',
    '            convert = pick_column_type([row.get(column, "") for row in table])',
    '            for row in table:',
    '                raw = row.get(column, "")',
    '                row[column] = None if raw == "" else convert(raw)',
    '    return table'
  ],
  pick_column_type: [
    'def pick_column_type(values):',
    '    """Pick one converter for a whole column, the way a CSV reader does.',
    '',
    '    The type belongs to the column, not to a single cell: one stray word in a',
    '    column of numbers makes the entire column text. Flowfile also recognises',
    '    dates here — adding that is a good exercise.',
    '    """',
    '    filled = [value for value in values if value != ""]',
    '    if not filled:',
    '        return str',
    '    for convert in (int, float):',
    '        try:',
    '            for value in filled:',
    '                convert(value)',
    '            return convert',
    '        except ValueError:',
    '            pass',
    '    if all(value.lower() in ("true", "false") for value in filled):',
    '        return lambda value: value.lower() == "true"',
    '    return str'
  ],
  match_type_of: [
    'def match_type_of(text, rows, column):',
    '    """Read a filter value written as text as the same type the column holds.',
    '',
    '    The settings panel stores every filter value as a string, but the column',
    '    may hold numbers — and in Python 5 != "5". A dataframe library converts',
    '    for you; this is that conversion, made visible.',
    '    """',
    '    sample = next((row[column] for row in rows if row.get(column) is not None), None)',
    '    if isinstance(sample, bool):',
    '        return text.strip().lower() == "true"',
    '    if isinstance(sample, int):',
    '        return int(text)',
    '    if isinstance(sample, float):',
    '        return float(text)',
    '    return text'
  ],
  as_text: [
    'def as_text(value):',
    '    """Render a value the way Polars renders it when casting to text."""',
    '    if value is None:',
    '        return None',
    '    if isinstance(value, bool):',
    '        return "true" if value else "false"',
    '    return str(value)'
  ],
  values_of: [
    'def values_of(rows, column):',
    '    """Every non-null value of one column.',
    '',
    '    Aggregates skip missing values rather than tripping over them, so this',
    '    filtering step happens on your behalf every time you sum a column.',
    '    """',
    '    return [row[column] for row in rows if row[column] is not None]'
  ],
  average: [
    'def average(values):',
    '    """Mean of the values given, or None when there are none left."""',
    '    return sum(values) / len(values) if values else None'
  ],
  middle_value: [
    'def middle_value(values):',
    '    """Median of the values given, or None when there are none left."""',
    '    return statistics.median(values) if values else None'
  ],
  write_csv_file: [
    'def write_csv_file(rows, path, delimiter=","):',
    '    """Write a list of dicts to a CSV file, taking the columns from the first row."""',
    '    with open(path, "w", newline="", encoding="utf-8") as handle:',
    '        writer = csv.DictWriter(handle, fieldnames=list(rows[0]) if rows else [], delimiter=delimiter)',
    '        writer.writeheader()',
    '        writer.writerows(rows)'
  ]
}

/** Helpers that pull in other helpers. */
const HELPER_DEPENDENCIES: Record<string, string[]> = {
  read_csv_file: ['pick_column_type']
}

export class FlowToPlainPythonConverter extends FlowToPolarsConverter {
  /** Module-level helper functions this flow ended up needing. */
  protected helpers = new Set<string>()
  /** Names already taken by a user's node_reference, so loop temps never shadow one. */
  protected takenNames = new Set<string>()
  /** node id -> [start, end) into the rendered body, for the per-node explainer. */
  protected renderedSpans = new Map<number, [number, number]>()
  /** node id -> the variables it read, recorded pre-rename at dispatch time. */
  protected stepInputs = new Map<number, string[]>()
  /** Nodes that ended up as an exercise rather than a loop. */
  protected stubbed = new Set<number>()
  /** One entry per emitting node, in pipeline order. Populated by renderBody(). */
  steps: PlainStep[] = []

  constructor(options: CodeGenerationOptions) {
    super(options)
    // Plain Python imports nothing by default; helpers add what they need.
    this.imports = new Set<string>()
    for (const node of this.nodes.values()) {
      const reference = node.node_reference || (node.settings as any)?.node_reference
      if (reference) this.takenNames.add(reference)
    }
  }

  // --- small helpers -------------------------------------------------------

  /** A loop-local name that cannot collide with a user's node_reference. */
  protected temp(base: string, nodeId: number): string {
    let name = `${base}_${nodeId}`
    while (this.takenNames.has(name)) name = `_${name}`
    return name
  }

  protected useHelper(name: string): void {
    if (this.helpers.has(name)) return
    this.helpers.add(name)
    for (const dependency of HELPER_DEPENDENCIES[name] || []) this.useHelper(dependency)
  }

  /** A `# ---- Title ----` banner, so each node's block is findable at a glance. */
  protected section(title: string): void {
    const rule = '-'.repeat(Math.max(4, 68 - title.length))
    this.addComment(`# --- ${title} ${rule}`)
  }

  protected teach(...lines: string[]): void {
    for (const line of lines) this.addComment(`# ${line}`)
  }

  /** `row["col"]` for a column name. */
  protected cell(rowVar: string, column: string): string {
    return `${rowVar}[${toPythonValue(column)}]`
  }

  /** A tuple expression over the given columns; no columns means the whole row. */
  protected keyTuple(rowVar: string, columns: string[]): string {
    if (columns.length === 0) return `tuple(${rowVar}.items())`
    return '(' + columns.map(column => `${this.cell(rowVar, column)}`).join(', ') + ',)'
  }

  // --- overrides -----------------------------------------------------------

  protected dispatchNodeCode(node: FlowNode, varName: string, inputVars: InputVars): void {
    const reads = [...this.collectMainInputs(inputVars)]
    if (inputVars.right) reads.push(inputVars.right)
    this.stepInputs.set(node.id, reads)

    const method = plainHandlerFor(node.type)
    if (!method) {
      this.stubbed.add(node.id)
      this.emitExerciseStub(node, varName, inputVars)
      return
    }
    const start = this.codeLines.length
    try {
      const emit = this[method] as unknown as (n: FlowNode, v: string, i: InputVars) => void
      emit.call(this, node, varName, inputVars)
    } catch (error) {
      if (!(error instanceof PlainPythonUnsupported)) throw error
      // Roll back a half-written block, then leave an exercise in its place.
      this.codeLines.length = start
      this.stubbed.add(node.id)
      this.emitExerciseStub(node, varName, inputVars, error.message)
    }
  }

  /** Every node emits its own block; fusing two `for` loops into one pipe is not a thing. */
  protected renderBody(): string[] {
    const emissions: Array<{ nodeId: number; varName: string; lines: string[]; node: FlowNode }> = []
    for (const { node, effectiveVar, start, end } of this.nodeSpans) {
      let lines = this.codeLines.slice(start, end)
      while (lines.length > 0 && lines[lines.length - 1] === '') lines = lines.slice(0, -1)
      if (lines.length === 0) continue
      emissions.push({ nodeId: node.id, varName: effectiveVar, lines, node })
    }

    const body: string[] = []
    this.renderedSpans.clear()
    for (const emission of emissions) {
      if (body.length > 0) body.push('')
      const start = body.length
      body.push(...emission.lines)
      this.renderedSpans.set(emission.nodeId, [start, body.length])
    }

    const survivors = new Set(emissions.map(emission => emission.nodeId))
    const nodeById = new Map(emissions.map(emission => [emission.nodeId, emission.node]))
    const asEmissions = emissions.map(emission => ({
      nodeId: emission.nodeId,
      varName: emission.varName,
      lines: emission.lines,
      mainProducerId: null,
      numInputs: 0,
      pinned: Boolean(emission.node.node_reference || (emission.node.settings as any)?.node_reference)
    }))
    const rename = this.planBoundaryNames(asEmissions, survivors, nodeById)

    const final = (name: string): string => rename.get(name) ?? name
    this.steps = emissions.map(emission => {
      const span = this.renderedSpans.get(emission.nodeId) ?? [0, 0]
      return {
        nodeId: emission.nodeId,
        nodeType: emission.node.type,
        varName: final(emission.varName),
        inputVars: (this.stepInputs.get(emission.nodeId) ?? []).map(final),
        concept: this.stubbed.has(emission.nodeId)
          ? 'exercise'
          : (CONCEPT_FOR_NODE[emission.node.type] ?? 'exercise'),
        // Resolved against the finished script in buildFinalCode, once the
        // header above the body is known.
        lineStart: span[0],
        lineEnd: span[1]
      }
    })

    return this.applyRenames(body, rename)
  }

  /**
   * The same pipeline, instrumented to record every intermediate table.
   *
   * `__steps__` is module-level rather than a local, so a raising exercise stub
   * still leaves everything computed before it readable — which is exactly the
   * case the walkthrough needs to keep working.
   */
  buildTraceCode(): string {
    const full = this.buildFinalCode()
    const body = this.renderedBody

    const spans = [...this.renderedSpans.entries()].sort((a, b) => a[1][0] - b[1][0])
    const traced: string[] = []
    let cursor = 0
    for (const [nodeId, [, end]] of spans) {
      traced.push(...body.slice(cursor, end))
      const step = this.steps.find(entry => entry.nodeId === nodeId)
      if (step) traced.push(`__steps__[${toPythonValue(step.varName)}] = ${step.varName}`)
      cursor = end
    }
    traced.push(...body.slice(cursor))

    const header = full.slice(0, full.indexOf('def run_etl_pipeline():'))
    const lines = [header + '__steps__ = {}', '', '', 'def run_etl_pipeline():']
    for (const line of traced) lines.push(line ? `    ${line}` : '')
    lines.push('')
    lines.push(this.lastNodeVar ? `    return ${this.lastNodeVar}` : '    return []')
    return lines.join('\n')
  }

  protected buildFinalCode(): string {
    // renderBody() remaps lastNodeVar to its final name, so it has to run first.
    const body = this.renderBody()
    this.renderedBody = body

    const lines: string[] = []
    const helpers = this.orderedHelpers()
    if (helpers.length > 0) {
      // Otherwise the reader opens the file on CSV-parsing scaffolding and has
      // to go looking for their own flow.
      lines.push('# Your pipeline is in run_etl_pipeline() below.')
      lines.push('# Everything above it is the handful of helpers it leans on.')
      lines.push('')
    }
    lines.push(...Array.from(this.imports).sort())
    if (this.imports.size > 0) lines.push('', '')

    for (const helper of helpers) {
      lines.push(...HELPER_SOURCES[helper], '', '')
    }

    lines.push('def run_etl_pipeline():')
    lines.push('    """')
    lines.push(`    ${this.flowName}`)
    lines.push('    Generated from Flowfile — plain Python, no dataframe library.')
    lines.push('')
    lines.push('    Every table here is a list of dicts: one dict per row, keyed by column')
    lines.push('    name. Read it top to bottom; each block is one node on the canvas.')
    lines.push('    """')

    // Body line i lands on script line (offset + i), 1-based — which is what
    // lets the walkthrough highlight a step inside the whole script.
    const offset = lines.length + 1
    for (const step of this.steps) {
      step.lineStart += offset
      step.lineEnd += offset - 1
    }

    for (const line of body) lines.push(line ? `    ${line}` : '')

    lines.push('')
    lines.push(this.lastNodeVar ? `    return ${this.lastNodeVar}` : '    return []')
    lines.push('', '')
    lines.push('if __name__ == "__main__":')
    lines.push('    pipeline_output = run_etl_pipeline()')
    lines.push('    for row in pipeline_output or []:')
    lines.push('        print(row)')

    return lines.join('\n')
  }

  /** Helpers in definition order: a helper's dependencies come before it. */
  protected orderedHelpers(): string[] {
    const ordered: string[] = []
    const visit = (name: string): void => {
      if (ordered.includes(name)) return
      for (const dependency of HELPER_DEPENDENCIES[name] || []) visit(dependency)
      ordered.push(name)
    }
    for (const name of Object.keys(HELPER_SOURCES)) {
      if (this.helpers.has(name)) visit(name)
    }
    return ordered
  }

  /** The final body, kept so explain() can slice one node's block back out of it. */
  protected renderedBody: string[] = []

  /** The rendered lines for one node — powers the settings-drawer explainer. */
  explain(nodeId: number): string | null {
    const span = this.renderedSpans.get(nodeId)
    if (!span) return null
    const lines = this.renderedBody.slice(span[0], span[1])
    // The drawer already names the node, and the banner rule wraps at that width.
    return (lines[0]?.startsWith('# --- ') ? lines.slice(1) : lines).join('\n')
  }

  // --- exercise stubs ------------------------------------------------------

  protected emitExerciseStub(node: FlowNode, varName: string, inputVars: InputVars, reason?: string): void {
    const inputs = this.collectMainInputs(inputVars)
    const right = inputVars.right
    const args = right ? [inputVars.main || 'rows_left', right] : inputs
    const parameters = args.map((_, index) => `rows_${index}`)
    const functionName = this.temp(node.type, node.id)
    const why = reason || STUB_REASONS[node.type] || `Flowfile runs this node with a library, so there is no loop to show.`

    this.section(`${node.type} (node ${node.id}) — over to you`)
    this.teach(why)
    const detail = this.stubDetail(node)
    if (detail) this.teach('The rule it applies is:', ...detail.split('\n').map(line => `    ${line}`))
    this.teach(
      'Writing this one by hand is the exercise. Replace the raise below with a',
      'loop that returns the new list of rows, and the rest of the script runs.'
    )
    this.addCode(`def ${functionName}(${parameters.join(', ')}):`)
    this.addCode(`    raise NotImplementedError(${toPythonValue(why)})`)
    this.addCode('')
    this.addCode(`${varName} = ${functionName}(${args.join(', ')})`)
    this.addCode('')
  }

  /** The one line of configuration worth quoting back in a stub. */
  protected stubDetail(node: FlowNode): string | null {
    const settings = node.settings as any
    if (node.type === 'formula') return settings?.function?.function?.trim() || null
    if (node.type === 'polars_code') return settings?.polars_code_input?.polars_code?.trim()?.split('\n')[0] || null
    // Only reached by a pivot the emitter refused (an aggregation it will not fake).
    if (node.type === 'pivot') {
      const pivot = settings?.pivot_input
      if (!pivot?.pivot_column) return null
      // Without the index and the aggregation the exercise cannot be solved.
      const index = pivot.index_columns?.length ? pivot.index_columns.join(', ') : '(none — one output row)'
      const aggregations = pivot.aggregations?.length ? pivot.aggregations.join(', ') : '(none selected)'
      return [
        `one row per: ${index}`,
        `one column per distinct value of: ${pivot.pivot_column}`,
        `each cell = ${aggregations} of: ${pivot.value_col}`
      ].join('\n')
    }
    return null
  }

  // --- sources -------------------------------------------------------------

  plainManualInput(node: FlowNode, varName: string): void {
    const raw = (node.settings as NodeManualInputSettings).raw_data_format
    this.section('Manual input')
    this.teach(
      'A table is a list of dicts: one dict per row, keyed by column name.',
      'That is the only data structure this whole script uses.'
    )
    if (!raw || !raw.columns?.length) {
      this.addCode(`${varName} = []`)
      this.addCode('')
      return
    }
    // raw.data is columnar: data[i] holds every value of columns[i].
    const names = raw.columns.map(column => column.name)
    const height = Math.max(0, ...names.map((_, index) => (raw.data[index] || []).length))
    this.addCode(`${varName} = [`)
    for (let row = 0; row < height; row++) {
      const pairs = names.map(
        (name, index) => `${toPythonValue(name)}: ${toPythonValue((raw.data[index] || [])[row] ?? null)}`
      )
      this.addCode(`    {${pairs.join(', ')}},`)
    }
    this.addCode(']')
    this.addCode('')
  }

  plainRead(node: FlowNode, varName: string): void {
    const settings = node.settings as NodeReadSettings
    const table = settings.received_file
    if (table?.file_type && table.file_type !== 'csv') {
      throw new PlainPythonUnsupported(
        `Reading ${table.file_type} needs a library that understands the format; only CSV is plain Python.`
      )
    }
    const path = table?.path ?? ''
    const remote = path.startsWith('http://') || path.startsWith('https://')
    if (remote) {
      throw new PlainPythonUnsupported('This file is loaded from a URL; fetching it is a separate exercise.')
    }
    const csvSettings = table?.table_settings as InputCsvTable | undefined
    this.useHelper('read_csv_file')
    this.imports.add('import csv')

    this.section('Read CSV')
    this.teach(
      'Every cell in a CSV file is text. read_csv_file (above) decides which',
      'columns are really numbers — that guess is normally hidden from you.'
    )
    const args = [toPythonValue(settings.file_name || table?.name || 'data.csv')]
    if (csvSettings?.delimiter && csvSettings.delimiter !== ',') {
      args.push(`delimiter=${toPythonValue(csvSettings.delimiter)}`)
    }
    if (csvSettings?.has_headers === false) args.push('has_header=False')
    if (csvSettings?.starting_from_line) args.push(`skip_rows=${csvSettings.starting_from_line}`)
    if (csvSettings?.infer_schema === false) args.push('infer_types=False')
    this.addCode(`${varName} = read_csv_file(${args.join(', ')})`)
    this.addCode('')
  }

  plainExternalData(node: FlowNode, varName: string): void {
    const name = (node.settings as NodeExternalDataSettings).dataset_name || 'external_data'
    this.emitCsvStandIn(varName, name, 'the host application')
  }

  plainReadFromCatalog(node: FlowNode, varName: string): void {
    const name = (node.settings as NodeReadFromCatalogSettings).dataset_name || 'catalog_table'
    this.emitCsvStandIn(varName, name, 'the Catalog')
  }

  protected emitCsvStandIn(varName: string, datasetName: string, origin: string): void {
    this.useHelper('read_csv_file')
    this.imports.add('import csv')
    this.section(`Read "${datasetName}"`)
    this.teach(
      `On the canvas this table comes from ${origin}, which a standalone`,
      'script has no access to. Export it once and this line reads the same rows.'
    )
    this.addCode(`${varName} = read_csv_file(${toPythonValue(`${datasetName}.csv`)})`)
    this.addCode('')
  }

  // --- row-wise transforms -------------------------------------------------

  plainFilter(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeFilterSettings
    const input = inputVars.main || 'rows'
    const filterInput = settings.filter_input

    if (filterInput?.mode === 'advanced') {
      throw new PlainPythonUnsupported(
        'This filter is written as a Polars expression, which needs Polars to evaluate.'
      )
    }
    const basic = filterInput?.basic_filter
    if (!basic?.field) {
      this.nodeVarMapping.set(node.id, input)
      return
    }

    const setup: string[] = []
    const condition = this.filterCondition(node, basic.field, basic.operator, basic.value, basic.value2, input, setup)
    this.section('Filter')
    this.teach(
      'Walk every row, test it, keep the ones that pass. This is the shape of',
      'a filter in any language — a dataframe library just hides the loop.'
    )
    for (const line of setup) this.addCode(line)
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    if ${condition}:`)
    this.addCode(`        ${varName}.append(row)`)
    this.addCode('')
  }

  /**
   * The `if` test for one basic filter, matching the engine's operator semantics.
   * Any lines pushed onto `setup` must be emitted before the loop.
   */
  protected filterCondition(
    node: FlowNode,
    field: string,
    operator: FilterOperator,
    value: string,
    value2: string | undefined,
    input: string,
    setup: string[]
  ): string {
    const target = this.cell('row', field)
    const present = `${target} is not None`

    if (operator === 'is_null') return `${target} is None`
    if (operator === 'is_not_null') return present

    // Read the filter value once, before the loop — not once per row.
    let literals = 0
    const literal = (text: string): string => {
      this.useHelper('match_type_of')
      const name = this.temp(literals === 0 ? 'wanted' : `wanted_${literals}`, node.id)
      literals += 1
      setup.push(`${name} = match_type_of(${toPythonValue(text)}, ${input}, ${toPythonValue(field)})`)
      return name
    }

    switch (operator) {
      case 'equals':
        return `${target} == ${literal(value)}`
      // `None != 5` is True in Python but null in Polars, which drops the row.
      case 'not_equals':
        return `${present} and ${target} != ${literal(value)}`
      // Comparing against None yields null in Polars, which drops the row.
      case 'greater_than':
        return `${present} and ${target} > ${literal(value)}`
      case 'greater_than_or_equals':
        return `${present} and ${target} >= ${literal(value)}`
      case 'less_than':
        return `${present} and ${target} < ${literal(value)}`
      case 'less_than_or_equals':
        return `${present} and ${target} <= ${literal(value)}`
      // Polars' str.contains takes a regular expression, so `re` it is.
      case 'contains':
        this.imports.add('import re')
        return `${present} and re.search(${toPythonValue(value)}, ${target}) is not None`
      case 'not_contains':
        this.imports.add('import re')
        return `${present} and re.search(${toPythonValue(value)}, ${target}) is None`
      case 'starts_with':
        return `${present} and ${target}.startswith(${toPythonValue(value)})`
      case 'ends_with':
        return `${present} and ${target}.endswith(${toPythonValue(value)})`
      case 'in':
      case 'not_in': {
        this.useHelper('match_type_of')
        const wanted = this.temp('wanted', node.id)
        const members = toPythonValue(value.split(',').map(part => part.trim()))
        setup.push(
          `${wanted} = [match_type_of(text, ${input}, ${toPythonValue(field)}) for text in ${members}]`
        )
        // Polars drops null rows for is_in and for its negation alike.
        const test = `${target} in ${wanted}`
        return operator === 'in' ? `${present} and ${test}` : `${present} and not (${test})`
      }
      case 'between':
        return `${present} and ${literal(value)} <= ${target} <= ${literal(value2 ?? value)}`
      default:
        throw new PlainPythonUnsupported(`The "${operator}" filter has no plain-Python form yet.`)
    }
  }

  plainSelect(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeSelectSettings
    const input = inputVars.main || 'rows'
    // The engine keeps `keep` columns in `position` order and only renames them.
    const kept = (settings.select_input || [])
      .filter(column => column.keep !== false && column.is_available !== false)
      .slice()
      .sort((a, b) => (a.position ?? 0) - (b.position ?? 0))

    if (kept.length === 0) {
      this.nodeVarMapping.set(node.id, input)
      return
    }

    this.section('Select columns')
    this.teach(
      'Rebuild each row as a new dict holding only the columns you asked for.',
      'Renaming is free here: you simply write the new key.'
    )
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    ${varName}.append({`)
    for (const column of kept) {
      const newName = column.new_name || column.old_name
      this.addCode(`        ${toPythonValue(newName)}: ${this.cell('row', column.old_name)},`)
    }
    this.addCode('    })')
    this.addCode('')
  }

  plainSort(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeSortSettings
    const input = inputVars.main || 'rows'
    const sortInput = (settings.sort_input || []).filter(entry => entry.column)
    if (sortInput.length === 0) {
      this.nodeVarMapping.set(node.id, input)
      return
    }

    // Polars sorts nulls FIRST in both directions (nulls_last defaults to False),
    // so each key leads with a "is this missing" flag rather than the value itself.
    const nullFlag = (target: string, descending: boolean): string =>
      descending ? `${target} is None` : `${target} is not None`

    const descendingFlags = sortInput.map(entry => entry.how === 'desc')
    const uniform = descendingFlags.every(flag => flag === descendingFlags[0])

    this.section('Sort')
    if (uniform) {
      const reverse = descendingFlags[0]
      const parts = sortInput.flatMap(entry => {
        const target = this.cell('row', entry.column)
        return [nullFlag(target, reverse), target]
      })
      this.teach(
        'sorted() takes a *key function*: reduce each row to the value it should',
        'be ordered by, and let the sort do the comparing.',
        'Missing values sort first either way, which is what the "is None" flag',
        'at the front of the key reproduces.'
      )
      this.addCode(`${varName} = sorted(`)
      this.addCode(`    ${input},`)
      this.addCode(`    key=lambda row: (${parts.join(', ')}),`)
      if (reverse) this.addCode('    reverse=True,')
      this.addCode(')')
      this.addCode('')
      return
    }

    // Mixed directions need one pass per column, least significant first —
    // which works only because Python's sort is stable.
    this.teach(
      'The columns sort in different directions, so one key function will not do.',
      'Instead: sort once per column, least significant first. That works only',
      "because Python's sort is *stable* — equal rows keep the order they had.",
      'Read the passes bottom to top to see the priority.'
    )
    this.addCode(`${varName} = list(${input})`)
    for (let index = sortInput.length - 1; index >= 0; index--) {
      const entry = sortInput[index]
      const target = this.cell('row', entry.column)
      const reverse = descendingFlags[index]
      const key = `(${nullFlag(target, reverse)}, ${target})`
      this.addCode(
        `${varName}.sort(key=lambda row: ${key}${reverse ? ', reverse=True' : ''})  # ${entry.column} ${reverse ? 'desc' : 'asc'}`
      )
    }
    this.addCode('')
  }

  plainUnique(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeUniqueSettings
    const input = inputVars.main || 'rows'
    const uniqueInput = (settings.unique_input || {}) as any
    const subset: string[] = uniqueInput.subset || uniqueInput.columns || []
    const keep: string = uniqueInput.keep || uniqueInput.strategy || 'first'

    const key = this.keyTuple('row', subset)
    const seen = this.temp('seen', node.id)

    this.section('Unique')
    if (keep === 'first' || keep === 'any') {
      this.teach(
        'The `seen` set is the whole trick: remember each key you have already',
        'emitted, and skip a row whose key is in it.'
      )
      this.addCode(`${seen} = set()`)
      this.addCode(`${varName} = []`)
      this.addCode(`for row in ${input}:`)
      this.addCode(`    key = ${key}`)
      this.addCode(`    if key in ${seen}:`)
      this.addCode('        continue')
      this.addCode(`    ${seen}.add(key)`)
      this.addCode(`    ${varName}.append(row)`)
      this.addCode('')
      return
    }

    const byKey = this.temp('by_key', node.id)
    if (keep === 'last') {
      this.teach(
        'Keeping the *last* of each key: a dict remembers insertion order, and',
        'overwriting a key does NOT move it to the end — so drop the old entry',
        'first. That puts each row where its kept copy actually sits.'
      )
      this.addCode(`${byKey} = {}`)
      this.addCode(`for row in ${input}:`)
      this.addCode(`    key = ${key}`)
      this.addCode(`    ${byKey}.pop(key, None)`)
      this.addCode(`    ${byKey}[key] = row`)
      this.addCode(`${varName} = list(${byKey}.values())`)
      this.addCode('')
      return
    }

    if (keep === 'none') {
      this.teach(
        'keep="none" drops every row whose key appears more than once, so you',
        'have to count the keys before you can decide — hence two passes.'
      )
      const counts = this.temp('counts', node.id)
      this.addCode(`${counts} = {}`)
      this.addCode(`for row in ${input}:`)
      this.addCode(`    key = ${key}`)
      this.addCode(`    ${counts}[key] = ${counts}.get(key, 0) + 1`)
      this.addCode(`${varName} = [row for row in ${input} if ${counts}[${key}] == 1]`)
      this.addCode('')
      return
    }

    throw new PlainPythonUnsupported(`The "${keep}" duplicate strategy has no plain-Python form yet.`)
  }

  plainRecordId(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeRecordIdSettings
    const input = inputVars.main || 'rows'
    const name = settings.record_id_input?.name || 'record_id'
    const offset = settings.record_id_input?.offset ?? 1

    this.section('Record ID')
    this.teach(
      'enumerate() hands you the counter and the row together, so there is no',
      'index variable to keep in step by hand. The new column goes first.'
    )
    this.addCode(`${varName} = []`)
    this.addCode(`for number, row in enumerate(${input}, start=${offset}):`)
    this.addCode(`    ${varName}.append({${toPythonValue(name)}: number, **row})`)
    this.addCode('')
  }

  plainHead(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeSampleSettings
    const input = inputVars.main || 'rows'
    const method = settings.sample_method || 'first'
    if (method !== 'first') {
      throw new PlainPythonUnsupported(
        'Random sampling uses a seeded shuffle, which plain Python cannot reproduce row-for-row.'
      )
    }
    const size = settings.sample_size ?? (settings as any).head_input?.n ?? 10

    this.section('Take sample')
    this.teach('A slice. Asking for more rows than exist is not an error — you just get fewer.')
    this.addCode(`${varName} = ${input}[:${size}]`)
    this.addCode('')
  }

  plainDynamicRename(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeDynamicRenameSettings
    const input = inputVars.main || 'rows'
    const rule = settings.dynamic_rename_input
    if (!rule) {
      this.nodeVarMapping.set(node.id, input)
      return
    }
    if (rule.rename_mode !== 'prefix' && rule.rename_mode !== 'suffix') {
      throw new PlainPythonUnsupported(
        `The "${rule.rename_mode}" rename mode needs Flowfile's expression engine or the data itself.`
      )
    }
    if (rule.selection_mode === 'data_type') {
      throw new PlainPythonUnsupported(
        'Selecting columns by data type needs the schema, which a list of dicts does not carry.'
      )
    }

    const affix = rule.rename_mode === 'prefix' ? rule.prefix || '' : rule.suffix || ''
    const newName =
      rule.rename_mode === 'prefix' ? `${toPythonValue(affix)} + column` : `column + ${toPythonValue(affix)}`
    const targets = this.temp('targets', node.id)

    this.section('Rename columns')
    this.teach(
      'One rule applied to many columns. The rows are rebuilt with new keys;',
      'the values never move.'
    )
    if (rule.selection_mode === 'list') {
      this.addCode(`${targets} = set(${toPythonValue(rule.selected_columns || [])})`)
    } else {
      this.addCode(`${targets} = set(${input}[0]) if ${input} else set()`)
    }
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    ${varName}.append({`)
    this.addCode(`        (${newName}) if column in ${targets} else column: value`)
    this.addCode('        for column, value in row.items()')
    this.addCode('    })')
    this.addCode('')
  }

  // --- reshape -------------------------------------------------------------

  plainGroupBy(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeGroupBySettings
    const input = inputVars.main || 'rows'
    const aggCols = settings.groupby_input?.agg_cols || []
    const groupCols = aggCols.filter(column => column.agg === 'groupby')
    const aggregations = aggCols.filter(column => column.agg !== 'groupby')

    if (aggCols.length === 0) {
      this.nodeVarMapping.set(node.id, input)
      return
    }

    const groups = this.temp('groups', node.id)
    const members = 'rows_in_group'

    this.section('Group by')
    if (groupCols.length === 0) {
      this.teach('No grouping columns, so the whole table is one group: a single summary row.')
      this.addCode(`${members} = ${input}`)
      this.addCode(`${varName} = [{`)
      for (const aggregation of aggregations) {
        this.addCode(`    ${toPythonValue(aggregation.new_name)}: ${this.aggExpression(aggregation, members)},`)
      }
      this.addCode('}]')
      this.addCode('')
      return
    }

    this.teach(
      'The accumulator-dict pattern, in two passes: file every row under its',
      'key, then walk the groups and summarise each one. Worth knowing by heart.'
    )
    this.addCode(`${groups} = {}`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    key = ${this.keyTuple('row', groupCols.map(column => column.old_name))}`)
    this.addCode(`    ${groups}.setdefault(key, []).append(row)`)
    this.addCode('')
    this.addCode(`${varName} = []`)
    this.addCode(`for key, ${members} in ${groups}.items():`)
    if (aggregations.length === 0) {
      // group_by with no aggregations is just the distinct set of keys.
      this.addCode(`    ${varName}.append({`)
      groupCols.forEach((column, index) => {
        this.addCode(`        ${toPythonValue(column.new_name)}: key[${index}],`)
      })
      this.addCode('    })')
      this.addCode('')
      return
    }
    this.addCode(`    ${varName}.append({`)
    groupCols.forEach((column, index) => {
      this.addCode(`        ${toPythonValue(column.new_name)}: key[${index}],`)
    })
    for (const aggregation of aggregations) {
      this.addCode(`        ${toPythonValue(aggregation.new_name)}: ${this.aggExpression(aggregation, members)},`)
    }
    this.addCode('    })')
    this.addCode('')
  }

  /** One aggregation over `members`, matching Polars' null handling. */
  protected aggExpression(aggregation: { old_name: string; agg: AggType }, members: string): string {
    const column = toPythonValue(aggregation.old_name)
    // Polars skips nulls in every aggregate except first / last / n_unique.
    const needsValues = aggregation.agg !== 'first' && aggregation.agg !== 'last' && aggregation.agg !== 'n_unique'
    if (needsValues) this.useHelper('values_of')
    const values = `values_of(${members}, ${column})`

    switch (aggregation.agg) {
      case 'sum':
        return `sum(${values})`
      case 'min':
        return `min(${values}, default=None)`
      case 'max':
        return `max(${values}, default=None)`
      // count() counts values, not rows: nulls do not count.
      case 'count':
        return `len(${values})`
      case 'mean':
        this.useHelper('average')
        return `average(${values})`
      case 'median':
        this.useHelper('middle_value')
        this.imports.add('import statistics')
        return `middle_value(${values})`
      // first / last are positional, so a null counts as a value here.
      case 'first':
        return `${members}[0][${column}] if ${members} else None`
      case 'last':
        return `${members}[-1][${column}] if ${members} else None`
      // A null is one of the distinct values, the same way Polars counts it.
      case 'n_unique':
        return `len({item[${column}] for item in ${members}})`
      case 'concat':
        this.useHelper('as_text')
        return `",".join(as_text(value) for value in ${values})`
      default:
        throw new PlainPythonUnsupported(`The "${aggregation.agg}" aggregation has no plain-Python form yet.`)
    }
  }

  plainPivot(node: FlowNode, varName: string, inputVars: InputVars): void {
    const pivot = (node.settings as NodePivotSettings).pivot_input
    const input = inputVars.main || 'rows'
    if (!pivot?.pivot_column) {
      throw new PlainPythonUnsupported('This pivot has no pivot column yet — nothing to turn into columns.')
    }
    if (!pivot.value_col) {
      throw new PlainPythonUnsupported('This pivot has no value column yet — nothing to put in the cells.')
    }
    const aggregations = pivot.aggregations || []
    if (aggregations.length === 0) {
      throw new PlainPythonUnsupported('This pivot has no aggregation selected yet.')
    }
    for (const aggregation of aggregations) {
      if (!PIVOT_AGGS_SUPPORTED.has(aggregation)) {
        throw new PlainPythonUnsupported(
          `Pivot has no case for the "${aggregation}" aggregation — the canvas quietly sums instead of applying it.`
        )
      }
    }

    const indexColumns = pivot.index_columns || []
    const labels = this.temp('labels', node.id)
    const cells = this.temp('cells', node.id)
    const out = this.temp('out', node.id)
    this.useHelper('as_text')

    /** One cell of the output. An absent cell is None; an empty one still aggregates. */
    const cellValue = (aggregation: string): string => {
      const expression = this.aggExpression({ old_name: pivot.value_col, agg: aggregation as AggType }, 'cell')
      // first / last already guard on `cell`; the rest would trip over a missing one.
      return aggregation === 'first' || aggregation === 'last' ? expression : `${expression} if cell else None`
    }
    /** The engine only spells the aggregation out when there is more than one. */
    const columnName = (aggregation: string): string =>
      aggregations.length > 1 ? `str(label) + ${toPythonValue(`_${aggregation}`)}` : 'str(label)'

    this.section('Pivot')
    this.teach(
      'One row per index key, one column per distinct value — which needs two',
      'levels of bookkeeping: a dict of rows, each holding a dict of cells.',
      'The labels get a pass of their own, because any row can invent a column',
      'and every output row has to end up carrying the same keys.'
    )
    // Polars takes the labels as text, sorted, nulls first — and Python refuses
    // to order None against a string, so the key leads with a flag.
    this.addCode(`${labels} = []`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    label = as_text(${this.cell('row', pivot.pivot_column)})`)
    this.addCode(`    if label not in ${labels}:`)
    this.addCode(`        ${labels}.append(label)`)
    this.addCode(`${labels}.sort(key=lambda label: (label is not None, label))`)
    this.addCode('')

    if (indexColumns.length === 0) {
      this.teach('No index columns, so every row lands in the same single output row.')
      this.addCode(`${cells} = {}`)
      this.addCode(`for row in ${input}:`)
      this.addCode(`    label = as_text(${this.cell('row', pivot.pivot_column)})`)
      this.addCode('    if label is None:')
      this.addCode('        continue')
      this.addCode(`    ${cells}.setdefault(label, []).append(row)`)
      this.addCode('')
      this.addCode(`${out} = {}`)
      this.addCode(`for label in ${labels}:`)
      this.addCode(`    cell = ${cells}.get(label)`)
      for (const aggregation of aggregations) {
        this.addCode(`    ${out}[${columnName(aggregation)}] = ${cellValue(aggregation)}`)
      }
      this.addCode(`${varName} = [${out}] if ${input} else []`)
      this.addCode('')
      return
    }

    this.addCode(`${cells} = {}`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    key = ${this.keyTuple('row', indexColumns)}`)
    this.addCode(`    row_cells = ${cells}.setdefault(key, {})`)
    this.addCode(`    label = as_text(${this.cell('row', pivot.pivot_column)})`)
    this.addCode('    if label is None:')
    // The row still earns an output row; it just belongs to no column.
    this.addCode('        continue')
    this.addCode('    row_cells.setdefault(label, []).append(row)')
    this.addCode('')
    this.addCode(`${varName} = []`)
    this.addCode(`for key, row_cells in ${cells}.items():`)
    this.addCode(`    ${out} = {`)
    indexColumns.forEach((column, index) => {
      this.addCode(`        ${toPythonValue(column)}: key[${index}],`)
    })
    this.addCode('    }')
    this.addCode(`    for label in ${labels}:`)
    this.addCode('        cell = row_cells.get(label)')
    for (const aggregation of aggregations) {
      this.addCode(`        ${out}[${columnName(aggregation)}] = ${cellValue(aggregation)}`)
    }
    this.addCode(`    ${varName}.append(${out})`)
    this.addCode('')
  }

  plainUnpivot(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeUnpivotSettings
    const input = inputVars.main || 'rows'
    const unpivot = settings.unpivot_input || ({} as any)
    if (unpivot.data_type_selector_mode === 'data_type' && unpivot.data_type_selector) {
      throw new PlainPythonUnsupported(
        'Selecting the columns to unpivot by data type needs the schema, which a list of dicts does not carry.'
      )
    }
    const indexColumns: string[] = unpivot.index_columns || []
    const valueColumns: string[] = unpivot.value_columns || []
    const targets = this.temp('value_columns', node.id)

    this.section('Unpivot')
    this.teach(
      'Wide to long: one output row per (row, column) pair. The column loop is',
      'on the outside — that is what puts all of one column together in the result.'
    )
    if (valueColumns.length > 0) {
      this.addCode(`${targets} = ${toPythonValue(valueColumns)}`)
    } else {
      this.addCode(
        `${targets} = [column for column in (${input}[0] if ${input} else {}) if column not in ${toPythonValue(indexColumns)}]`
      )
    }
    this.addCode(`${varName} = []`)
    this.addCode(`for column in ${targets}:`)
    this.addCode(`    for row in ${input}:`)
    this.addCode(`        ${varName}.append({`)
    for (const indexColumn of indexColumns) {
      this.addCode(`            ${toPythonValue(indexColumn)}: ${this.cell('row', indexColumn)},`)
    }
    this.addCode('            "variable": column,')
    this.addCode('            "value": row.get(column),')
    this.addCode('        })')
    this.addCode('')
  }

  // --- combine -------------------------------------------------------------

  plainJoin(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeJoinSettings
    const joinInput = (settings.join_input || {}) as any
    const how: string = joinInput.join_type || joinInput.how || 'inner'
    const mapping = joinInput.join_mapping || []
    const suffix: string = joinInput.right_suffix ?? '_right'

    if (!JOIN_HOWS_SUPPORTED.has(how)) {
      throw new PlainPythonUnsupported(
        `A "${how}" join is a left join with the tables the other way round — worth writing out yourself.`
      )
    }
    if (mapping.length === 0) {
      throw new PlainPythonUnsupported('This join has no columns to match on yet.')
    }

    const left = inputVars.main || 'rows_left'
    const right = inputVars.right || 'rows_right'
    const index = this.temp('index', node.id)
    const rightKeys = this.temp('right_keys', node.id)
    const leftKey = this.keyTuple('row', mapping.map((entry: any) => entry.left_col))
    const indexKey = this.keyTuple('other', mapping.map((entry: any) => entry.right_col))

    this.section(`Join (${how})`)
    this.teach(
      'Index the right-hand table by its key first, then walk the left table',
      'once. Nested loops would be rows x rows; this is rows + rows.',
      'A missing key matches nothing at all — not even another missing key.'
    )
    const extras = this.temp('right_columns', node.id)

    this.addCode(`${rightKeys} = ${toPythonValue(mapping.map((entry: any) => entry.right_col))}`)
    this.addCode(`${index} = {}`)
    this.addCode(`for other in ${right}:`)
    this.addCode(`    key = ${indexKey}`)
    this.addCode('    if any(value is None for value in key):')
    this.addCode('        continue')
    this.addCode(`    ${index}.setdefault(key, []).append(other)`)
    if (how === 'left') {
      this.addCode(
        `${extras} = [column for column in (${right}[0] if ${right} else {}) if column not in ${rightKeys}]`
      )
    }
    this.addCode('')
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${left}:`)
    this.addCode(`    key = ${leftKey}`)
    this.addCode(`    matches = [] if any(value is None for value in key) else ${index}.get(key, [])`)

    if (how === 'semi') {
      this.addCode('    if matches:')
      this.addCode(`        ${varName}.append(row)`)
      this.addCode('')
      return
    }
    if (how === 'anti') {
      this.addCode('    if not matches:')
      this.addCode(`        ${varName}.append(row)`)
      this.addCode('')
      return
    }

    this.addCode('    for other in matches:')
    this.addCode('        combined = dict(row)')
    this.addCode('        for column, value in other.items():')
    this.addCode(`            if column in ${rightKeys}:`)
    this.addCode('                continue')
    this.addCode(`            combined[column + ${toPythonValue(suffix)} if column in row else column] = value`)
    this.addCode(`        ${varName}.append(combined)`)

    if (how === 'left') {
      this.addCode('    if not matches:')
      this.addCode(
        `        ${varName}.append({**row, **{column + ${toPythonValue(suffix)} if column in row else column: None for column in ${extras}}})`
      )
    }
    this.addCode('')
  }

  plainCrossJoin(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeCrossJoinSettings
    const suffix = settings.cross_join_input?.right_suffix || '_right'
    const left = inputVars.main || 'rows_left'
    const right = inputVars.right || 'rows_right'

    this.section('Cross join')
    this.teach(
      'Every row on the left paired with every row on the right. This is the',
      'one join that really is two nested loops — and why the output gets big.'
    )
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${left}:`)
    this.addCode(`    for other in ${right}:`)
    this.addCode('        combined = dict(row)')
    this.addCode('        for column, value in other.items():')
    this.addCode(`            combined[column + ${toPythonValue(suffix)} if column in row else column] = value`)
    this.addCode(`        ${varName}.append(combined)`)
    this.addCode('')
  }

  plainUnion(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeUnionSettings
    const inputs = this.collectMainInputs(inputVars)
    if (inputs.length === 0) {
      this.addCode(`${varName} = []`)
      this.addCode('')
      return
    }
    if (inputs.length === 1) {
      this.nodeVarMapping.set(node.id, inputs[0])
      return
    }
    const mode = settings.union_input?.mode || 'diagonal'

    this.section('Union')
    if (mode === 'vertical') {
      this.teach('Same columns in every table, so stacking the rows is all there is to it.')
      this.addCode(`${varName} = ${inputs.join(' + ')}`)
      this.addCode('')
      return
    }

    const columns = this.temp('all_columns', node.id)
    this.teach(
      'The tables need not share columns. Collect every column name first, then',
      'rebuild each row with None wherever its table had nothing to say.'
    )
    this.addCode(`${columns} = []`)
    this.addCode(`for table in (${inputs.join(', ')}):`)
    this.addCode('    for column in (table[0] if table else {}):')
    this.addCode(`        if column not in ${columns}:`)
    this.addCode(`            ${columns}.append(column)`)
    this.addCode('')
    this.addCode(`${varName} = []`)
    this.addCode(`for table in (${inputs.join(', ')}):`)
    this.addCode('    for row in table:')
    this.addCode(`        ${varName}.append({column: row.get(column) for column in ${columns}})`)
    this.addCode('')
  }

  // --- sinks ---------------------------------------------------------------

  /** Emitting nothing makes the base class alias this node straight to its input. */
  plainExploreData(): void {}

  plainExternalOutput(): void {}

  plainOutput(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeOutputSettings
    const input = inputVars.main || 'rows'
    const output = settings.output_settings
    const fileType = output?.file_type || 'csv'
    if (fileType !== 'csv') {
      throw new PlainPythonUnsupported(
        `Writing ${fileType} needs a library that understands the format; only CSV is plain Python.`
      )
    }
    const fileName = output?.name || 'output.csv'
    const delimiter = (output?.table_settings as any)?.delimiter
    this.useHelper('write_csv_file')
    this.imports.add('import csv')

    this.section('Write CSV')
    this.teach("Python's csv module handles the quoting and escaping rules for you.")
    const args = [input, toPythonValue(fileName)]
    if (delimiter && delimiter !== ',') {
      args.push(`delimiter=${toPythonValue(delimiter === 'tab' ? '\t' : delimiter)}`)
    }
    this.addCode(`write_csv_file(${args.join(', ')})`)
    this.addCode('')
    // Writing a file produces no new table, so downstream keeps using the input.
    void varName
    this.nodeVarMapping.set(node.id, input)
  }

  plainWriteToCatalog(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeWriteToCatalogSettings
    const input = inputVars.main || 'rows'
    const name = (settings.dataset_name || '').trim() || 'catalog_table'
    this.useHelper('write_csv_file')
    this.imports.add('import csv')

    this.section(`Write "${name}"`)
    this.teach('A standalone script has no Catalog, so the rows go to a CSV file of the same name.')
    this.addCode(`write_csv_file(${input}, ${toPythonValue(`${name}.csv`)})`)
    this.addCode('')
    void varName
    this.nodeVarMapping.set(node.id, input)
  }
}

/** The module-level helpers a snippet may call without defining them nearby. */
export const HELPER_NAMES: string[] = Object.keys(HELPER_SOURCES)

/**
 * Node type -> emitter method. This table is the single source of truth: the
 * allowlist below is derived from it, so a node type can never be "supported"
 * without an emitter, and `satisfies` fails the build if a method name is wrong.
 */
const PLAIN_HANDLERS = {
  manual_input: 'plainManualInput',
  read: 'plainRead',
  external_data: 'plainExternalData',
  read_from_catalog: 'plainReadFromCatalog',
  filter: 'plainFilter',
  select: 'plainSelect',
  sort: 'plainSort',
  unique: 'plainUnique',
  record_id: 'plainRecordId',
  head: 'plainHead',
  sample: 'plainHead',
  dynamic_rename: 'plainDynamicRename',
  group_by: 'plainGroupBy',
  pivot: 'plainPivot',
  unpivot: 'plainUnpivot',
  join: 'plainJoin',
  cross_join: 'plainCrossJoin',
  union: 'plainUnion',
  explore_data: 'plainExploreData',
  external_output: 'plainExternalOutput',
  output: 'plainOutput',
  write_to_catalog: 'plainWriteToCatalog'
} as const satisfies Record<string, keyof FlowToPlainPythonConverter>

function plainHandlerFor(nodeType: string): keyof FlowToPlainPythonConverter | undefined {
  return (PLAIN_HANDLERS as Record<string, keyof FlowToPlainPythonConverter>)[nodeType]
}

/** Node types with a plain-Python form. Derived — never hand-written. */
export const PLAIN_PYTHON_NODE_TYPES: ReadonlySet<string> = new Set(Object.keys(PLAIN_HANDLERS))

export interface NodeExplanation {
  nodeId: number
  nodeType: string
  explanation: string
  code: string | null
  supported: boolean
  /** Helper functions the snippet calls but does not define; they live in the full script. */
  helpers: string[]
}

export interface PlainWalkthrough {
  /** The script the learner reads. */
  script: string
  /** The same pipeline instrumented to record every intermediate table. */
  traceScript: string
  steps: PlainStep[]
  /** Per step, the lines of that node's own block. */
  snippets: Record<number, string>
}

export function usePlainPythonGeneration() {
  /** The whole flow as one standalone plain-Python script. Never throws on an unsupported node. */
  const generatePlainPython = (options: CodeGenerationOptions): string =>
    new FlowToPlainPythonConverter(options).convert()

  /** Everything the step-by-step view needs, from a single conversion. */
  const buildWalkthrough = (options: CodeGenerationOptions): PlainWalkthrough => {
    const converter = new FlowToPlainPythonConverter(options)
    const script = converter.convert()
    const snippets: Record<number, string> = {}
    for (const step of converter.steps) {
      const snippet = converter.explain(step.nodeId)
      if (snippet) snippets[step.nodeId] = snippet
    }
    return { script, traceScript: converter.buildTraceCode(), steps: converter.steps, snippets }
  }

  /** Prose + the plain-Python form of one node's actual settings, for the settings drawer. */
  const explainNode = (options: CodeGenerationOptions, nodeId: number): NodeExplanation => {
    const node = options.nodes.get(nodeId)
    const nodeType = node?.type ?? 'unknown'
    const explanation = NODE_EXPLANATIONS[nodeType] ?? ''
    let code: string | null = null
    try {
      const converter = new FlowToPlainPythonConverter(options)
      converter.convert()
      code = converter.explain(nodeId)
    } catch {
      code = null
    }
    const helpers = code ? Object.keys(HELPER_SOURCES).filter(name => code!.includes(`${name}(`)) : []
    return { nodeId, nodeType, explanation, code, helpers, supported: PLAIN_PYTHON_NODE_TYPES.has(nodeType) }
  }

  return {
    generatePlainPython,
    buildWalkthrough,
    explainNode,
    PLAIN_PYTHON_NODE_TYPES,
    NODE_EXPLANATIONS,
    CONCEPTS
  }
}
