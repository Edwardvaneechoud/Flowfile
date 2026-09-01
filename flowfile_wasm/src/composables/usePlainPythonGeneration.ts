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

import { FlowToPolarsConverter, RESERVED_NAMES, toPythonValue } from './useCodeGeneration'
import type { CodeGenerationOptions } from './useCodeGeneration'
import { getNodeDescription } from '../config/nodeDescriptions'
import { selectCastTarget } from '../stores/schema-inference'
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

/** Join strategies with an honest loop form. The rest become done-by-the-canvas notes. */
const JOIN_HOWS_SUPPORTED = new Set(['inner', 'left', 'semi', 'anti'])

/** Honest per-join reasons — a full join is NOT a swapped left join. */
const JOIN_EXERCISE_REASONS: Record<string, string> = {
  right: 'A "right" join is a left join with the two tables swapped.',
  full: 'A "full" join is a left join plus the right-hand rows that found no match.',
  outer: 'An "outer" join is a left join plus the right-hand rows that found no match.',
  cross: 'A cross join pairs every row with every row — two nested loops, like the Cross join node.'
}

/**
 * The aggregations pivot really implements. The settings panel also offers
 * n_unique and concat, but the engine has no case for them and quietly sums
 * instead — so those become a done-by-the-canvas note rather than a lie.
 */
const PIVOT_AGGS_SUPPORTED = new Set(['sum', 'mean', 'min', 'max', 'count', 'first', 'last', 'median'])

/** The counting aggregations the engine zero-fills, so an absent cell reads 0 rather than None. */
const PIVOT_ZERO_FILL_AGGS = new Set(['sum', 'count', 'len'])

/** Plain-English "what this node does", shown above the snippet in the settings drawer. */
export const NODE_EXPLANATIONS: Record<string, string> = {
  manual_input:
    'Holds a small table you typed in by hand. In plain Python that is a list of dictionaries, written out literally — one dict per row, ready for everything below it.',
  read:
    'Reads a CSV file into a list of rows. Everything in a CSV is text, so the code also has to work out which columns are really numbers — a guess that other tools make silently, written out here as a function you can read.',
  external_data:
    'Takes a table the host application handed to the editor. A standalone script has no host, so the generated code reads the same data from a CSV file next to it.',
  read_from_catalog:
    'Reads a table you saved in the Catalog. A standalone script has no Catalog, so the generated code reads the same data from a CSV file next to it.',
  filter:
    'Keeps the rows that match a condition and drops the rest. In plain Python that is a loop: test each row, and append the ones that pass to a new list.',
  select:
    'Chooses which columns to keep, renames them, and sets their order. Each row is rebuilt as a new dictionary holding only the keys you asked for.',
  sort:
    "Puts the rows in order. Python's sorted() takes a key function — it turns each row into the value to sort it by, and the sort does the comparing.",
  unique:
    'Drops duplicate rows. The code keeps a set of the row keys it has already kept, and skips any row whose key is already in it.',
  group_by:
    'Collapses many rows into one row per group, in two passes: first put every row into the bucket for its group key, then turn each bucket into one summary row.',
  join:
    'Matches rows in one table against rows in another. Instead of scanning the right table once per left row, the code puts the right table into a dictionary first, so each left row is a single lookup — the same trick databases call a hash join.',
  cross_join:
    'Pairs every row on the left with every row on the right. That really is just two nested loops — and the reason the output grows so fast: the row counts multiply.',
  union:
    'Stacks the rows of several tables into one. Columns that only some tables have are filled with None in the rows that lack them.',
  record_id:
    'Numbers the rows. enumerate() hands you the number and the row together on every pass of the loop, so there is no counter variable to maintain.',
  head:
    'Takes a sample of the rows. Set to "first", that is a Python slice — rows[:n] — and a slice never complains when you ask for more rows than exist. The random methods have no plain-Python form, because the canvas samples with a seeded shuffle this script cannot reproduce row for row.',
  sample:
    'Takes a sample of the rows. Set to "first", that is a Python slice — rows[:n] — and a slice never complains when you ask for more rows than exist. The random methods have no plain-Python form, because the canvas samples with a seeded shuffle this script cannot reproduce row for row.',
  dynamic_rename:
    'Renames many columns with one rule instead of one at a time. Each row is rebuilt with the new keys; the values are copied through untouched.',
  unpivot:
    'Turns wide data into long data: one output row for each row-and-column pair. The column loop sits outside the row loop, which is what groups the output by column.',
  explore_data: 'Opens the table in the data explorer. It changes nothing, so it generates no code.',
  output:
    "Writes the rows to a file. Python's csv module handles the quoting and escaping rules you would otherwise get wrong.",
  external_output: 'Hands the rows back to the host application. It changes nothing, so it generates no code.',
  write_to_catalog:
    'Saves the rows as a Catalog table. A standalone script has no Catalog, so the generated code writes the same rows to a CSV file.',
  formula:
    'Evaluates a Flowfile expression on every row. Translating any possible expression would mean writing a whole expression evaluator, so the generated script cannot reproduce it — the canvas applies this step for you.',
  polars_code:
    'Runs Polars code you wrote yourself. It is already Python, and it already uses a dataframe library, so there is nothing to translate.',
  pivot:
    'Turns long data into wide data, creating one column per distinct value. It works like a group-by with two keys per row: which output row, and which output column. So the dictionary of buckets gains a second level, and the column names are collected before any row is built.'
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
  /**
   * A standard-library road to the same shape — a thought and a link, never a
   * third code path. Sparingly: it only lands if most cards do not have one.
   */
  another?: { text: string; linkLabel: string; href: string }
}

export const CONCEPTS: Record<string, Concept> = {
  'list-of-dicts': {
    title: 'A table is a list of dicts',
    body: [
      'In this script a table is a Python list, and each row in it is a dictionary: the column names are the keys, the cell contents are the values. rows[0] is the first row, and rows[0]["product"] is one cell. That is the only data structure the whole script uses.',
      'A dataframe library like Polars stores the same table the other way around — one array per column — which is far faster on big data. But row by row is easier to read and to write loops over, so that is the shape this script uses.'
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
    takeaway: 'A list of dicts is easy to read but slow at scale. That trade-off is why dataframe libraries exist.'
  },
  'csv-typing': {
    title: 'A CSV file is all text',
    body: [
      'Open a CSV file in a text editor and you can see it: everything in it is text. The cell that looks like the number 42 is really just the characters "4" and "2" — the file has no way to say "this column holds numbers".',
      'Text compares badly as numbers: Python compares strings character by character, so "9" sorts after "41" — and strings cannot be summed at all. Before the script can filter or total a column, the values have to be converted to real numbers.',
      'The conversion is done per column, not per cell. If the "age" column held "30", "41" and "n/a", converting only the numeric ones would leave a column that is half numbers and half text — and the first comparison would crash. So pick_column_type checks every value in the column first: if they all convert, the whole column becomes numbers; otherwise it stays text. (One case in between: a column that is entirely "true"/"false" becomes booleans.)',
      'Every tool that reads CSVs — Polars, pandas, a spreadsheet import — makes this same guess. They just do it silently. Here it is a function you can read.'
    ],
    sketch: [
      'file:      age',
      '           30',
      '           41',
      '',
      'as text:   ["30", "41"]       "30" < "41"  ...but "9" > "41"',
      'as ints:   [30, 41]           correct ordering',
      '',
      'a value like "n/a" keeps the whole column text:',
      '           ["30", "41", "n/a"]  ->  stays text'
    ],
    takeaway: 'When a CSV loads with wrong types — numbers as text, IDs as numbers — this guessing step is where it happened.'
  },
  'guard-loop': {
    title: 'A filter is a loop with a test',
    body: [
      'A filter takes three moves: start with an empty list, loop over the rows, and append every row that passes the test. This is how a filter looks in every programming language — a dataframe library just runs the same loop for you, out of sight.',
      'Missing values need one extra guard. If row["revenue"] is None, then None > 100 raises an error in Python — it refuses to order "no value" against a number. Polars instead treats that comparison as unknown and drops the row. To give the same answer, the loop checks is not None before comparing. Where a test is already safe with None (like equals on text), the guard is left out.'
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
    takeaway: 'Always build a new list instead of deleting from the one you are looping over — removing items mid-loop makes Python skip elements.'
  },
  'rebuild-row': {
    title: 'Selecting builds a new row',
    body: [
      'To keep only some columns, the loop does not delete anything. It builds a fresh dictionary for each row, holding just the keys you asked for — every column you did not name is simply not copied over.',
      'Renaming costs nothing extra: write the new name as the key, read the value from the old one. And because Python dicts keep their keys in insertion order, the order the keys are written here is the column order that comes out.'
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
    title: 'Sorting with a key function',
    body: [
      'sorted() does not ask you to compare two rows. It asks for something simpler: a key function that turns one row into the value to sort it by. You point at the column; Python does all the comparing.',
      'Sorting by two columns costs nothing extra — return a tuple. key=lambda r: (r["city"], r["age"]) means "sort by city, break ties by age", because Python compares tuples left to right and only looks at the next item when the earlier ones are equal.',
      'The (row["age"] is not None, row["age"]) pattern handles missing values. None < 5 does not return False in Python — it raises an error, because "no value" cannot be ordered against a number. But False < True compares fine. So the key leads with a True/False flag: missing rows get False and sort together at the front — which is also where Polars puts them — and the real values are only compared between rows that have one.',
      "One more property does real work here: Python's sort is stable, meaning rows with equal keys keep the order they already had. Sort by name, then sort by department, and each department's names stay alphabetical. That is what lets the script sort mixed ascending/descending columns one pass at a time."
    ],
    sketch: [
      'one column',
      '    sorted(rows, key=lambda r: r["age"])',
      '',
      'two columns — a tuple compares left to right',
      '    sorted(rows, key=lambda r: (r["city"], r["age"]))',
      '    ties on city?  then compare age',
      '',
      'why the flag is there',
      '    None < 5      TypeError: \'<\' not supported',
      '    False < True  True            <- booleans do compare',
      '    key=lambda r: (r["age"] is not None, r["age"])',
      '    missing rows -> False -> they sort first',
      '',
      'stable: equal keys keep their order',
      '    [b1, a1, b2]  sorted by letter',
      ' -> [a1, b1, b2]  b1 is still before b2',
      '',
      '    so: sort by age, then by city',
      '     -> cities in order, ages in order within each city'
    ],
    takeaway:
      'To sort by several columns one pass at a time: sort by the least important column first and the most important last. Stability keeps the earlier passes intact.',
    another: {
      text: 'operator.itemgetter("city", "age") builds the same key function without a lambda. It cannot do the (is not None, value) trick, though — that still needs the lambda.',
      linkLabel: 'Sorting HOW-TO — Python docs',
      href: 'https://docs.python.org/3/howto/sorting.html'
    }
  },
  'seen-set': {
    title: 'The `seen` set',
    body: [
      'To drop duplicates, keep a set of the row keys you have already kept. For each row: if its key is in seen, skip it; otherwise add the key and keep the row.',
      'A set, not a list, because of speed. Asking a set "is this in it?" takes the same tiny time whether it holds ten keys or ten million; the same question to a list means reading the whole list, every time. On 100k rows that is the difference between instant and a coffee break.'
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
    takeaway: 'Use a set for "have I seen this?" and a dict for "what did I see with it?". Both answer instantly; a list does not.',
    another: {
      text: 'For a flat list of values, list(dict.fromkeys(values)) drops duplicates in one line, keeping first appearances — a dict holds one entry per key. Rows need a detour: a key must be a value that cannot change, so each dict becomes a tuple first — exactly what the loop\'s key = (row["email"],) line is quietly doing.',
      linkLabel: 'dict.fromkeys — Python docs',
      href: 'https://docs.python.org/3/library/stdtypes.html#dict.fromkeys'
    }
  },
  'accumulator-dict': {
    title: 'Group by: sort into buckets, then summarise',
    body: [
      'A group-by cannot finish any group early — the last row of the input might still belong to the first group. So the code makes two passes over the data.',
      'Pass one puts each row into the bucket for its group: a dictionary where each key is a group and each value is the list of rows in it. groups.setdefault(key, []).append(row) does this in one line — "get the list for this key, creating an empty one if this is its first row, and append". Pass two walks the finished buckets and turns each one into a single summary row.',
      'This two-pass shape comes up constantly: word counts, tallies, and any "one result per category" problem all use this same code.',
      'Most aggregates ignore missing values — that is why they run over values_of(...), which drops the Nones, rather than the raw rows. first and last are positional, so they return whatever sits there, None included; and n_unique counts "missing" as one of the distinct values.',
      'Two small details: sum of an empty list is 0, but the average of nothing is undefined, so it comes back as None. And the group key is a tuple like (row["x"],) — the trailing comma is what makes a one-item tuple. Tuples can be dictionary keys (lists cannot), and grouping by two columns then just means a longer tuple.'
    ],
    sketch: [
      'rows:  Widget 100 | Gadget 200 | Widget 150',
      '',
      'pass 1 — put every row into its bucket',
      '    groups = {}',
      '    for row in rows:',
      '        groups.setdefault(row["product"], []).append(row)',
      '',
      '    "Widget" -> [Widget 100, Widget 150]',
      '    "Gadget" -> [Gadget 200]',
      '',
      'pass 2 — summarise each bucket',
      '    "Widget" -> 100 + 150 = 250',
      '    "Gadget" ->       200 = 200'
    ],
    takeaway: 'setdefault(key, []).append(x) is the one-liner for "add to the list at this key, creating the list if needed".',
    another: {
      text: 'itertools.groupby collapses the two passes into one — but it only groups rows that are already next to each other, so you must sort by the group key first. The buckets dict never needs the sort.',
      linkLabel: 'itertools.groupby — Python docs',
      href: 'https://docs.python.org/3/library/itertools.html#itertools.groupby'
    }
  },
  'nested-accumulator': {
    title: 'Pivot: a grid of buckets',
    body: [
      'A pivot is a group-by with two keys per row: which output ROW it belongs to, and which output COLUMN. So the dictionary of buckets from the group-by pattern gains a second level — a dict of rows, each holding a dict of cells, each cell collecting the input rows that landed on that spot of the grid.',
      'The column names have to be collected before any output row can be built. Any input row may bring a value nobody has seen yet, and every row of a table must end up with the same keys — so the loop gathers all the labels first, then builds each output row against that full list.',
      'Two kinds of "nothing" come out differently. A cell that exists but sums no values is 0; a row-and-column combination that never occurred is unknown, and comes back as None. The code tells them apart by asking whether the cell\'s key exists — not by looking at what is stored under it.',
      'The column names depend on how many aggregations you picked. One aggregation names each column after the pivot value alone ("colour", "size"); pick two and the names must say which is which ("colour_sum", "colour_mean").'
    ],
    sketch: [
      'rows:  r1 colour 3 | r1 size 5 | r2 size 1 | r2 size 7 | r3 colour -',
      '       one row per id, one column per label, each cell = sum',
      '',
      '1. collect the labels first — any row may invent one',
      '       labels = ["colour", "size"]',
      '',
      '2. put every row under both of its keys',
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
      'Collect all the column names first. Every row of a table has to end up with the same keys.'
  },
  'hash-index': {
    title: 'The fast way to join: a dict',
    body: [
      'The obvious join is a loop inside a loop: for every row on the left, scan the whole right table for matches. It works, but the cost multiplies — 1,000 rows against 1,000 rows is a million comparisons.',
      'The fast join walks the right table once first, putting each of its rows into a dictionary under its join key. Now each left row finds its matches with one dictionary lookup. Cost: about 2,000 steps instead of a million, for the same answer. Databases use the same trick and call it a hash join.',
      'Missing join keys match nothing — not even each other. Polars treats two unknowns as not equal, so the loop skips None keys on both sides.'
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
    takeaway: 'When you catch yourself scanning one list inside a loop over another, build a dict instead.',
    another: {
      text: 'collections.defaultdict(list) makes the setdefault line unnecessary: index[key].append(other) creates the empty list itself the first time a key appears.',
      linkLabel: 'collections.defaultdict — Python docs',
      href: 'https://docs.python.org/3/library/collections.html#collections.defaultdict'
    }
  },
  'nested-loop': {
    title: 'Cross join: every pair',
    body: [
      'A cross join pairs every row on the left with every row on the right. There is no key to match on, so two nested loops is exactly the right code — nothing here to speed up.',
      'Watch the output size: it multiplies. 1,000 rows against 1,000 rows makes 1,000,000 rows out.'
    ],
    sketch: ['for row in left:          # 3 rows', '    for other in right:   # 4 rows', '        ...               # 12 rows out'],
    another: {
      text: 'itertools.product(left, right) yields exactly these pairs — the two nested loops as one call. Merging each pair into one row is still yours to write.',
      linkLabel: 'itertools.product — Python docs',
      href: 'https://docs.python.org/3/library/itertools.html#itertools.product'
    }
  },
  'column-union': {
    title: 'Stacking tables that disagree',
    body: [
      'When every table has the same columns, stacking them is just adding the lists together.',
      'When they do not, the code first collects every column name that appears in any table, then rebuilds each row against that full list. row.get(column) fills the gaps: .get returns None for a key the row does not have, where row[column] would raise an error.'
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
    takeaway: 'row["k"] raises an error if the key is missing; row.get("k") returns None instead. Choose on purpose.'
  },
  enumerate: {
    title: 'enumerate counts for you',
    body: [
      'You could number rows with a counter variable — set it to 1, add 1 at the bottom of the loop, and hope you never forget. enumerate(rows, start=1) removes the bookkeeping: each pass of the loop hands you the number and the row together.',
      'start= sets where the numbering begins.'
    ],
    sketch: [
      "for i, row in enumerate(rows, start=1):",
      '    ...',
      '',
      '{**row}  copies a dict;  {"nr": i, **row}  puts nr first'
    ]
  },
  slice: {
    title: 'A slice takes what is there',
    body: [
      'rows[:5] means "the first five rows". If the list only has two, you get two — asking a slice for more than exists is not an error. A single index is stricter: rows[5] raises IndexError when there is no row 5.',
      'That forgiving behaviour is why taking a sample needs no length check.'
    ],
    sketch: ['rows[:5]    first five (or fewer)', 'rows[-5:]   last five', 'rows[5]     IndexError if it is not there']
  },
  'key-rewrite': {
    title: 'Renaming rewrites the keys',
    body: [
      'Renaming columns never touches the values. The loop rebuilds each row as a new dictionary: every key that matches the rule gets its new name, every other key is copied through unchanged — and each value comes along as it was.',
      'The {key: value for ...} form is a dict comprehension: a one-line loop that builds a dictionary. Here it reads "for each column and value in the row, decide what the column should now be called, and keep the value".'
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
      'Unpivot turns one wide row into several long ones: a row with four quarter columns becomes four rows, each naming its quarter under "variable" and carrying its number under "value". The id columns repeat on every one.',
      'The loop order decides the output order. With columns on the outside and rows on the inside, all of Q1 comes out before any of Q2 — swap the two loops and the output would be grouped by row instead.'
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
    title: 'Writing a CSV needs the csv module',
    body: [
      'It is tempting to write a CSV by joining values with commas. That works right up until a value contains a comma, a quote or a newline — then the file is silently broken, and whatever reads it next sees the wrong columns.',
      "Python's csv module exists for exactly this. DictWriter quotes and escapes the tricky values correctly, and it takes the column order from the fieldnames you give it — so the header and the data rows can never disagree."
    ],
    sketch: [
      'writer = csv.DictWriter(handle, fieldnames=list(rows[0]))',
      'writer.writeheader()',
      'writer.writerows(rows)',
      '',
      'value:   he said "hi", loudly',
      'written: "he said ""hi"", loudly"'
    ],
    takeaway: 'Never write CSV by joining strings yourself — the edge cases will break the file.'
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

/** Why a node type has no loop form, quoted into its done-by-the-canvas note. */
const STUB_REASONS: Record<string, string> = {
  formula: 'The canvas evaluates this with its expression engine, so there is no fixed loop to show.',
  polars_code: 'This node is already Python, and it already uses a dataframe library — there is nothing to translate.'
}

/** Module-level helper functions, emitted only when a node actually needs one. */
const HELPER_SOURCES: Record<string, string[]> = {
  read_csv_file: [
    'def read_csv_file(path, delimiter=",", has_header=True, skip_rows=0, infer_types=True):',
    '    """Read a CSV file — or an http(s) URL — into a list of dicts, one dict per row."""',
    '    if path.startswith(("http://", "https://")):',
    '        import io, urllib.request',
    '        with urllib.request.urlopen(path) as response:',
    '            handle = io.StringIO(response.read().decode("utf-8"))',
    '    else:',
    '        handle = open(path, newline="", encoding="utf-8")',
    '    with handle:',
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
    '    """Work out one type for a whole column, the way a CSV reader does.',
    '',
    '    If every filled cell converts to a number, the column becomes numbers;',
    '    if every cell is "true"/"false", it becomes booleans; anything else',
    '    keeps the entire column text, because a half-converted column would',
    '    crash on its first comparison. Flowfile also recognises dates here;',
    '    adding that is a good exercise.',
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
    '    """Convert a filter value written as text to the type the column holds.',
    '',
    '    The settings panel stores every filter value as a string, but the column',
    '    may hold numbers — and in Python 5 != "5". This does the conversion a',
    '    dataframe library would do for you silently.',
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
  as_whole_number: [
    'def as_whole_number(value, low=-(2 ** 63), high=2 ** 63 - 1):',
    '    """Read a value as a whole number the way Polars converts a column to one.',
    '',
    '    Polars converts without complaining: anything that will not parse — or',
    '    will not fit the width asked for — comes out missing rather than raising.',
    '    Decimals are truncated toward zero, never rounded.',
    '    """',
    '    if value is None:',
    '        return None',
    '    if isinstance(value, (bool, int, float)):',
    '        try:',
    '            number = int(value)',
    '        except (ValueError, OverflowError):',
    '            return None',
    '    else:',
    '        text = str(value)',
    '        if text != text.strip():',
    '            return None  # Polars will not parse padded text',
    '        try:',
    '            number = int(text)',
    '        except ValueError:',
    '            return None',
    '    return number if low <= number <= high else None'
  ],
  as_decimal_number: [
    'def as_decimal_number(value):',
    '    """Read a value as a decimal number, the way Polars converts a column to one.',
    '',
    '    Same rule as as_whole_number: text that will not parse comes out missing.',
    '    """',
    '    if value is None:',
    '        return None',
    '    if isinstance(value, (bool, int, float)):',
    '        return float(value)',
    '    text = str(value)',
    '    if text != text.strip():',
    '        return None',
    '    try:',
    '        return float(text)',
    '    except ValueError:',
    '        return None'
  ],
  values_of: [
    'def values_of(rows, column):',
    '    """One column\'s values, with the missing ones (None) dropped.',
    '',
    '    Aggregations like sum and mean skip missing values — this function is',
    '    where that happens.',
    '    """',
    '    return [row[column] for row in rows if row[column] is not None]'
  ],
  average: [
    'def average(values):',
    '    """Mean of the values given, or None when the list is empty."""',
    '    return sum(values) / len(values) if values else None'
  ],
  middle_value: [
    'def middle_value(values):',
    '    """Median of the values given, or None when the list is empty."""',
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

/**
 * The plain-Python conversion for a select node's cast target.
 *
 * Only the conversions a `list[dict]` can honour exactly are here. Polars parses
 * text into temporals by sniffing formats, refuses text→Boolean outright, and
 * loses precision on Float32 — none of which a loop reproduces, so those targets
 * throw and the node becomes a done-by-the-canvas note.
 */
const PLAIN_CAST_CONVERSIONS: Record<string, { helper: string; bounds?: string }> = {
  String: { helper: 'as_text' },
  Float64: { helper: 'as_decimal_number' },
  Int64: { helper: 'as_whole_number' },
  Int32: { helper: 'as_whole_number', bounds: '-(2 ** 31), 2 ** 31 - 1' },
  Int16: { helper: 'as_whole_number', bounds: '-(2 ** 15), 2 ** 15 - 1' },
  Int8: { helper: 'as_whole_number', bounds: '-(2 ** 7), 2 ** 7 - 1' },
  UInt64: { helper: 'as_whole_number', bounds: '0, 2 ** 64 - 1' },
  UInt32: { helper: 'as_whole_number', bounds: '0, 2 ** 32 - 1' },
  UInt16: { helper: 'as_whole_number', bounds: '0, 2 ** 16 - 1' },
  UInt8: { helper: 'as_whole_number', bounds: '0, 2 ** 8 - 1' }
}

/** One manual-input cell as its declared dtype, or unchanged when it will not convert. */
function coerceCell(value: unknown, dataType: string | undefined): unknown {
  if (value === null || value === undefined || value === '') return null
  if (typeof value !== 'string' || !dataType) return value
  if (dataType === 'Boolean') {
    const lowered = value.trim().toLowerCase()
    return lowered === 'true' ? true : lowered === 'false' ? false : null
  }
  if (!/^(Int|UInt|Float)/.test(dataType)) return value
  const parsed = Number(value)
  if (value.trim() === '' || Number.isNaN(parsed)) return null
  return dataType.startsWith('Float') ? parsed : Math.trunc(parsed)
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
  /** Nodes that ended up as a done-by-the-canvas note rather than a real loop. */
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

  /**
   * A loop-local name that cannot collide with a node_reference or another temp.
   *
   * The plain name wherever possible — `groups`, not `groups_4`. A node-id
   * suffix appears only when a second node needs the same base, because a
   * machine-looking name is exactly what this flavour is trying not to emit.
   */
  protected temp(base: string, nodeId: number): string {
    const taken = (name: string) => this.takenNames.has(name) || RESERVED_NAMES.has(name)
    let name = taken(base) ? `${base}_${nodeId}` : base
    while (taken(name)) name = `_${name}`
    this.takenNames.add(name)
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
      // Roll back a half-written block, then leave a done-by-the-canvas note.
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
          ? ''
          : (CONCEPT_FOR_NODE[emission.node.type] ?? ''),
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
   * `__steps__` is module-level rather than a local, so a script the learner
   * has edited into raising mid-way still leaves everything computed before
   * the raise readable.
   */
  buildTraceCode(): string {
    // Renders the body and finalises imports/helpers; the text itself is unused.
    void this.buildFinalCode()
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

    const lines: string[] = []
    lines.push(...Array.from(this.imports).sort())
    if (this.imports.size > 0) lines.push('', '')
    lines.push('__steps__ = {}', '', '')
    lines.push('def run_etl_pipeline():')
    for (const line of traced) lines.push(line ? `    ${line}` : '')
    lines.push('')
    lines.push(this.lastNodeVar ? `    return ${this.lastNodeVar}` : '    return []')
    for (const helper of this.orderedHelpers()) {
      lines.push('', '')
      lines.push(...HELPER_SOURCES[helper])
    }
    return lines.join('\n')
  }

  /**
   * Newspaper order: the reader's own pipeline first, the helpers it calls
   * after it. Python resolves names when the pipeline *runs*, not when it is
   * defined, so nothing is used before it exists — and the file opens on the
   * flow the reader built rather than on CSV-parsing scaffolding.
   */
  protected buildFinalCode(): string {
    // renderBody() remaps lastNodeVar to its final name, so it has to run first.
    const body = this.renderBody()
    this.renderedBody = body

    const lines: string[] = []
    lines.push(...Array.from(this.imports).sort())
    if (this.imports.size > 0) lines.push('', '')

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

    const helpers = this.orderedHelpers()
    if (helpers.length > 0) {
      lines.push('# The helper functions the pipeline calls. Python only looks these names up')
      lines.push('# when the pipeline runs, so it is fine that they live below their callers.')
      for (const helper of helpers) {
        lines.push(...HELPER_SOURCES[helper], '', '')
      }
    }

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

  /** Whether the node was emitted as a passthrough note rather than a real loop. */
  isStubbed(nodeId: number): boolean {
    return this.stubbed.has(nodeId)
  }

  // --- steps the canvas keeps to itself -------------------------------------

  /**
   * A node with no honest plain-Python form gets a short note, never a fake
   * implementation: the rule is quoted, and the rows pass through unchanged
   * (or start empty for a source) so every later name stays defined.
   */
  protected emitExerciseStub(node: FlowNode, varName: string, inputVars: InputVars, reason?: string): void {
    const main = inputVars.main || this.collectMainInputs(inputVars)[0]
    const why = reason || STUB_REASONS[node.type] || `The canvas runs this node with a library, so there is no loop to show.`

    this.section(`${getNodeDescription(node.type).title} — done by the canvas`)
    this.teach(why)
    const detail = this.stubDetail(node)
    if (detail) this.teach('The rule it applies is:', ...detail.split('\n').map(line => `    ${line}`))
    if (main) {
      this.teach('No plain-Python form for it, so the rows continue unchanged here.')
      this.addCode(`${varName} = ${main}`)
    } else {
      this.teach('No plain-Python form for it, so this table starts out empty here.')
      this.addCode(`${varName} = []`)
    }
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
      const pairs = names.map((name, index) => {
        // Cells are text next to a declared dtype; a list of dicts has no dtypes
        // to convert by later, so convert here.
        const declared = (raw.columns[index] as { data_type?: string }).data_type
        const value = coerceCell((raw.data[index] || [])[row] ?? null, declared)
        return `${toPythonValue(name)}: ${toPythonValue(value)}`
      })
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
        `Reading ${table.file_type} needs a library that understands the format. CSV is the only format plain Python can read on its own.`
      )
    }
    const path = table?.path ?? ''
    const remote = path.startsWith('http://') || path.startsWith('https://')
    const csvSettings = table?.table_settings as InputCsvTable | undefined
    this.useHelper('read_csv_file')
    this.imports.add('import csv')

    this.section('Read CSV')
    this.teach(
      'Everything in a CSV file is text. read_csv_file (defined below the',
      'pipeline) works out which columns are really numbers — the same guess',
      'other tools make silently.'
    )
    if (remote) this.teach('Given a URL, the helper fetches the file with urllib first.')
    const args = [toPythonValue(remote ? path : settings.file_name || table?.name || 'data.csv')]
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
    const name = ((node.settings as NodeReadFromCatalogSettings).dataset_name || '').trim() || 'catalog_table'
    this.emitCsvStandIn(varName, name, 'the Catalog')
  }

  protected emitCsvStandIn(varName: string, datasetName: string, origin: string): void {
    this.useHelper('read_csv_file')
    this.imports.add('import csv')
    this.section(`Read "${datasetName}"`)
    this.teach(
      `On the canvas this table comes from ${origin}, which a standalone`,
      'script cannot reach. Download it as a CSV once, put it next to this',
      'script, and this line reads the same rows.'
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
      'Test every row, append the ones that pass to a new list. A filter looks',
      'like this in every language — a dataframe library just runs the loop for you.'
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
    const literal = (text: string, base = 'wanted'): string => {
      this.useHelper('match_type_of')
      const name = this.temp(base, node.id)
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
        return `${present} and ${literal(value, 'lower')} <= ${target} <= ${literal(value2 ?? value, 'upper')}`
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

    // Resolve every conversion before emitting: a refusal must roll back cleanly,
    // and a helper registered for a rolled-back block would still be written out.
    const needed = new Set<string>()
    const cells = kept.map(column => {
      const value = this.cell('row', column.old_name)
      const target = selectCastTarget(column)
      if (!target) return value
      const conversion = PLAIN_CAST_CONVERSIONS[target]
      if (!conversion) {
        throw new PlainPythonUnsupported(
          `Converting a column to ${target} follows Polars' own parsing rules, which a list of dicts cannot reproduce.`
        )
      }
      needed.add(conversion.helper)
      return `${conversion.helper}(${value}${conversion.bounds ? `, ${conversion.bounds}` : ''})`
    })
    for (const helper of needed) this.useHelper(helper)

    this.section('Select columns')
    this.teach(
      'Rebuild each row as a new dict holding only the columns you asked for.',
      'Renaming is free here: you simply write the new key.'
    )
    if (needed.size > 0) {
      this.teach('A column whose type changed is converted cell by cell on the way in.')
    }
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${input}:`)
    this.addCode(`    ${varName}.append({`)
    kept.forEach((column, index) => {
      const newName = column.new_name || column.old_name
      this.addCode(`        ${toPythonValue(newName)}: ${cells[index]},`)
    })
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
        'sorted() takes a key function: it turns each row into the value to',
        'sort it by, and the sort does the comparing.',
        'Missing values sort first either way (as they do in Polars) — that is',
        'what the True/False flag at the front of the key is for.'
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
      'Instead: sort once per column, least important first. That works only',
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
        'kept, and skip any row whose key is already in it.'
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
        'first. Each row then ends up in the position of its last occurrence.'
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
      'counter variable to update yourself. The new column goes first.'
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
        "Random sampling shuffles with a fixed seed, and plain Python cannot reproduce the canvas's exact shuffle — the rows would not match."
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
        `The "${rule.rename_mode}" rename mode works its new names out at run time — from a formula or from the data — which a pre-written script cannot do.`
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
      'the values are copied through untouched.'
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
      'Two passes: put every row into the bucket for its group key, then turn',
      'each bucket into one summary row. This pattern comes up constantly.'
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
          `The canvas does not truly support the "${aggregation}" aggregation in a pivot — it quietly sums instead — so this script will not pretend to.`
        )
      }
    }

    const indexColumns = pivot.index_columns || []
    const labels = this.temp('labels', node.id)
    const cells = this.temp('cells', node.id)
    const out = this.temp('out', node.id)
    this.useHelper('as_text')

    /** One cell of the output. An absent cell is 0 for the counting aggregations, else None. */
    const cellValue = (aggregation: string): string => {
      const expression = this.aggExpression({ old_name: pivot.value_col, agg: aggregation as AggType }, 'cell')
      // first / last already guard on `cell`; the rest would trip over a missing one.
      if (aggregation === 'first' || aggregation === 'last') return expression
      const missing = PIVOT_ZERO_FILL_AGGS.has(aggregation) ? '0' : 'None'
      return `${expression} if cell else ${missing}`
    }
    /** The engine only spells the aggregation out when there is more than one. */
    const columnName = (aggregation: string): string =>
      aggregations.length > 1 ? `str(label) + ${toPythonValue(`_${aggregation}`)}` : 'str(label)'

    this.section('Pivot')
    this.teach(
      'One row per index key, one column per distinct value — which needs two',
      'levels of bookkeeping: a dict of rows, each holding a dict of cells.',
      'The column names are collected first, because any row may introduce a',
      'new one and every output row must end up with the same keys.'
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
        JOIN_EXERCISE_REASONS[how] ?? `A "${how}" join has no plain-Python form here yet — worth writing out yourself.`
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
      'Put each right-hand row into a dictionary under its key first; then each',
      'left row finds its matches with one lookup. Nested loops would cost',
      'rows x rows of work; this costs rows + rows.',
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
    this.addCode(`            new_column = column + ${toPythonValue(suffix)} if column in row else column`)
    this.addCode('            combined[new_column] = value')
    this.addCode(`        ${varName}.append(combined)`)

    if (how === 'left') {
      // A left row with no partner still comes through, with the right-hand
      // columns filled in as None — spelled out as a loop, not a one-liner.
      this.addCode('    if not matches:')
      this.addCode('        combined = dict(row)')
      this.addCode(`        for column in ${extras}:`)
      this.addCode(`            new_column = column + ${toPythonValue(suffix)} if column in row else column`)
      this.addCode('            combined[new_column] = None')
      this.addCode(`        ${varName}.append(combined)`)
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
      'Every row on the left paired with every row on the right — two nested',
      'loops, no key to match. The output size multiplies: rows x rows.'
    )
    this.addCode(`${varName} = []`)
    this.addCode(`for row in ${left}:`)
    this.addCode(`    for other in ${right}:`)
    this.addCode('        combined = dict(row)')
    this.addCode('        for column, value in other.items():')
    this.addCode(`            new_column = column + ${toPythonValue(suffix)} if column in row else column`)
    this.addCode('            combined[new_column] = value')
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
      "The tables don't have to share columns. Collect every column name first,",
      'then rebuild each row with None wherever it lacks one of the columns.'
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
  plainExploreData(node: FlowNode, _varName: string, inputVars: InputVars): void {
    // Interactive-only: remap to the input (like the base handlePreview) so the
    // return line and downstream readers resolve to a defined name.
    if (inputVars.main) this.nodeVarMapping.set(node.id, inputVars.main)
  }

  plainExternalOutput(node: FlowNode, _varName: string, inputVars: InputVars): void {
    if (inputVars.main) this.nodeVarMapping.set(node.id, inputVars.main)
  }

  plainOutput(node: FlowNode, varName: string, inputVars: InputVars): void {
    const settings = node.settings as NodeOutputSettings
    const input = inputVars.main || 'rows'
    const output = settings.output_settings
    const fileType = output?.file_type || 'csv'
    if (fileType !== 'csv') {
      throw new PlainPythonUnsupported(
        `Writing ${fileType} needs a library that understands the format. CSV is the only format plain Python can write on its own.`
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
  explanation: string
  code: string | null
  /** Helper functions the snippet calls but does not define; they live in the full script. */
  helpers: string[]
  /** True when the snippet is an exercise stub, not a real loop. */
  isStub: boolean
}

export interface PlainWalkthrough {
  /** The script the learner reads. */
  script: string
  steps: PlainStep[]
}

export function usePlainPythonGeneration() {
  /** The whole flow as one standalone plain-Python script. Never throws on an unsupported node. */
  const generatePlainPython = (options: CodeGenerationOptions): string =>
    new FlowToPlainPythonConverter(options).convert()

  /** Everything the step-by-step view needs, from a single conversion. */
  const buildWalkthrough = (options: CodeGenerationOptions): PlainWalkthrough => {
    const converter = new FlowToPlainPythonConverter(options)
    const script = converter.convert()
    return { script, steps: converter.steps }
  }

  /** Prose + the plain-Python form of one node's actual settings, for the settings drawer. */
  const explainNode = (options: CodeGenerationOptions, nodeId: number): NodeExplanation => {
    const node = options.nodes.get(nodeId)
    const nodeType = node?.type ?? 'unknown'
    const explanation = NODE_EXPLANATIONS[nodeType] ?? ''
    let code: string | null = null
    let isStub = false
    try {
      const converter = new FlowToPlainPythonConverter(options)
      converter.convert()
      code = converter.explain(nodeId)
      isStub = converter.isStubbed(nodeId)
    } catch {
      code = null
    }
    const helpers = code ? Object.keys(HELPER_SOURCES).filter(name => code!.includes(`${name}(`)) : []
    return { explanation, code, helpers, isStub }
  }

  return { generatePlainPython, buildWalkthrough, explainNode }
}
