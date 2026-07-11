/**
 * Composable for Polars autocompletion in CodeMirror
 */
import { EditorView, keymap } from "@codemirror/view";
import { EditorState, Extension, Prec } from "@codemirror/state";
import { python } from "@codemirror/lang-python";
import { indentUnit } from "@codemirror/language";
import { oneDark } from "@codemirror/theme-one-dark";
import {
  autocompletion,
  Completion,
  CompletionContext,
  CompletionResult,
  acceptCompletion,
  completionStatus,
} from "@codemirror/autocomplete";
import { indentMore, indentLess } from "@codemirror/commands";
import { bodyTooltips } from "@/utils/codemirrorTooltips";
import {
  catalogRefChainCompletions,
  createRefVariableCompletions,
  FLOWFILE_CTX_GLOBAL_ENTRIES,
  flowfileApiCompletions,
} from "@/utils/flowfileCtxCompletions";
import { makeProcessBodyLinter } from "./processBodyLint";
import type { ComponentState, SectionState } from "../designerState";
import { toSnakeCase } from "../mappers";

// The shared flowfile_ctx sources are synchronous; CompletionSource's return
// type merely permits a Promise. Alias them so the gated wrapper below stays
// typed as a plain synchronous source.
type SyncCompletionSource = (context: CompletionContext) => CompletionResult | null;
const ffApiSource = flowfileApiCompletions as SyncCompletionSource;
const ffCatalogChainSource = catalogRefChainCompletions as SyncCompletionSource;

// LazyFrame methods for autocompletion
const lazyFrameMethods = [
  // Selection & Filtering
  { label: "select", type: "method", info: "Select columns", apply: "select()" },
  { label: "filter", type: "method", info: "Filter rows by condition", apply: "filter()" },
  { label: "with_columns", type: "method", info: "Add or modify columns", apply: "with_columns()" },
  { label: "drop", type: "method", info: "Drop columns", apply: "drop()" },
  { label: "rename", type: "method", info: "Rename columns", apply: "rename({})" },
  { label: "cast", type: "method", info: "Cast column types", apply: "cast({})" },

  // Sorting & Limiting
  { label: "sort", type: "method", info: "Sort by columns", apply: 'sort("")' },
  { label: "head", type: "method", info: "Get first n rows", apply: "head()" },
  { label: "tail", type: "method", info: "Get last n rows", apply: "tail()" },
  { label: "limit", type: "method", info: "Limit number of rows", apply: "limit()" },
  { label: "slice", type: "method", info: "Slice rows by offset and length", apply: "slice()" },
  { label: "unique", type: "method", info: "Get unique rows", apply: "unique()" },

  // Grouping & Aggregation
  { label: "group_by", type: "method", info: "Group by columns", apply: "group_by().agg()" },
  { label: "agg", type: "method", info: "Aggregate expressions", apply: "agg()" },
  { label: "rolling", type: "method", info: "Rolling window operations", apply: "rolling()" },
  {
    label: "group_by_dynamic",
    type: "method",
    info: "Dynamic time-based grouping",
    apply: "group_by_dynamic()",
  },

  // Joins
  {
    label: "join",
    type: "method",
    info: "Join with another LazyFrame",
    apply: 'join(other, on="", how="left")',
  },
  { label: "join_asof", type: "method", info: "As-of join for time series", apply: "join_asof()" },
  {
    label: "cross_join",
    type: "method",
    info: "Cross join (cartesian product)",
    apply: "cross_join()",
  },

  // Reshaping
  { label: "explode", type: "method", info: "Explode list column to rows", apply: 'explode("")' },
  { label: "unpivot", type: "method", info: "Unpivot wide to long format", apply: "unpivot()" },
  { label: "pivot", type: "method", info: "Pivot long to wide format", apply: "pivot()" },
  { label: "unnest", type: "method", info: "Unnest struct column", apply: 'unnest("")' },

  // Missing data
  { label: "fill_null", type: "method", info: "Fill null values", apply: "fill_null()" },
  { label: "fill_nan", type: "method", info: "Fill NaN values", apply: "fill_nan()" },
  { label: "drop_nulls", type: "method", info: "Drop rows with nulls", apply: "drop_nulls()" },
  { label: "interpolate", type: "method", info: "Interpolate null values", apply: "interpolate()" },

  // Other
  {
    label: "with_row_index",
    type: "method",
    info: "Add row index column",
    apply: 'with_row_index("index")',
  },
  { label: "reverse", type: "method", info: "Reverse row order", apply: "reverse()" },
  {
    label: "collect",
    type: "method",
    info: "Execute and collect to DataFrame",
    apply: "collect()",
  },
  { label: "lazy", type: "method", info: "Convert to LazyFrame", apply: "lazy()" },

  // Expression methods (chainable)
  { label: "alias", type: "method", info: "Rename expression result", apply: 'alias("")' },
  { label: "is_null", type: "method", info: "Check for null", apply: "is_null()" },
  { label: "is_not_null", type: "method", info: "Check for not null", apply: "is_not_null()" },
  { label: "sum", type: "method", info: "Sum values", apply: "sum()" },
  { label: "mean", type: "method", info: "Calculate mean", apply: "mean()" },
  { label: "min", type: "method", info: "Get minimum", apply: "min()" },
  { label: "max", type: "method", info: "Get maximum", apply: "max()" },
  { label: "count", type: "method", info: "Count values", apply: "count()" },
  { label: "first", type: "method", info: "Get first value", apply: "first()" },
  { label: "last", type: "method", info: "Get last value", apply: "last()" },
  { label: "str", type: "property", info: "String operations namespace", apply: "str." },
  { label: "dt", type: "property", info: "Datetime operations namespace", apply: "dt." },
  { label: "list", type: "property", info: "List operations namespace", apply: "list." },
  { label: "over", type: "method", info: "Window function over groups", apply: 'over("")' },
];

// Common Polars completions
const polarsCompletions = [
  { label: "self", type: "keyword", info: "Access node instance" },
  { label: "inputs[0]", type: "variable", info: "First input LazyFrame" },
  { label: "inputs[1]", type: "variable", info: "Second input LazyFrame" },
  {
    label: "logging",
    type: "module",
    info: "Python logging module — output shows in the Test panel's Logs",
    apply: "logging.",
  },

  // Polars expressions
  { label: "pl.col", type: "function", info: "Select a column by name", apply: 'pl.col("")' },
  { label: "pl.lit", type: "function", info: "Create a literal value", apply: "pl.lit()" },
  { label: "pl.all", type: "function", info: "Select all columns", apply: "pl.all()" },
  {
    label: "pl.exclude",
    type: "function",
    info: "Select all except specified",
    apply: 'pl.exclude("")',
  },
  {
    label: "pl.when",
    type: "function",
    info: "Start conditional expression",
    apply: "pl.when().then().otherwise()",
  },
  { label: "pl.concat", type: "function", info: "Concatenate LazyFrames", apply: "pl.concat([])" },
  { label: "pl.struct", type: "function", info: "Create struct column", apply: "pl.struct([])" },

  // LazyFrame methods with lf. prefix
  { label: "lf.select", type: "method", info: "Select columns", apply: "lf.select()" },
  { label: "lf.filter", type: "method", info: "Filter rows by condition", apply: "lf.filter()" },
  {
    label: "lf.with_columns",
    type: "method",
    info: "Add or modify columns",
    apply: "lf.with_columns()",
  },
  { label: "lf.drop", type: "method", info: "Drop columns", apply: "lf.drop()" },
  { label: "lf.rename", type: "method", info: "Rename columns", apply: "lf.rename({})" },
  { label: "lf.cast", type: "method", info: "Cast column types", apply: "lf.cast({})" },
  { label: "lf.sort", type: "method", info: "Sort by columns", apply: 'lf.sort("")' },
  { label: "lf.head", type: "method", info: "Get first n rows", apply: "lf.head()" },
  { label: "lf.tail", type: "method", info: "Get last n rows", apply: "lf.tail()" },
  { label: "lf.limit", type: "method", info: "Limit number of rows", apply: "lf.limit()" },
  {
    label: "lf.slice",
    type: "method",
    info: "Slice rows by offset and length",
    apply: "lf.slice()",
  },
  { label: "lf.unique", type: "method", info: "Get unique rows", apply: "lf.unique()" },
  { label: "lf.group_by", type: "method", info: "Group by columns", apply: "lf.group_by().agg()" },
  { label: "lf.agg", type: "method", info: "Aggregate expressions", apply: "lf.agg()" },
  { label: "lf.rolling", type: "method", info: "Rolling window operations", apply: "lf.rolling()" },
  {
    label: "lf.group_by_dynamic",
    type: "method",
    info: "Dynamic time-based grouping",
    apply: "lf.group_by_dynamic()",
  },
  {
    label: "lf.join",
    type: "method",
    info: "Join with another LazyFrame",
    apply: 'lf.join(other, on="", how="left")',
  },
  {
    label: "lf.join_asof",
    type: "method",
    info: "As-of join for time series",
    apply: "lf.join_asof()",
  },
  {
    label: "lf.cross_join",
    type: "method",
    info: "Cross join (cartesian product)",
    apply: "lf.cross_join()",
  },
  {
    label: "lf.explode",
    type: "method",
    info: "Explode list column to rows",
    apply: 'lf.explode("")',
  },
  {
    label: "lf.unpivot",
    type: "method",
    info: "Unpivot wide to long format",
    apply: "lf.unpivot()",
  },
  { label: "lf.pivot", type: "method", info: "Pivot long to wide format", apply: "lf.pivot()" },
  { label: "lf.unnest", type: "method", info: "Unnest struct column", apply: 'lf.unnest("")' },
  { label: "lf.fill_null", type: "method", info: "Fill null values", apply: "lf.fill_null()" },
  { label: "lf.fill_nan", type: "method", info: "Fill NaN values", apply: "lf.fill_nan()" },
  {
    label: "lf.drop_nulls",
    type: "method",
    info: "Drop rows with nulls",
    apply: "lf.drop_nulls()",
  },
  {
    label: "lf.interpolate",
    type: "method",
    info: "Interpolate null values",
    apply: "lf.interpolate()",
  },
  {
    label: "lf.with_row_index",
    type: "method",
    info: "Add row index column",
    apply: 'lf.with_row_index("index")',
  },
  { label: "lf.reverse", type: "method", info: "Reverse row order", apply: "lf.reverse()" },
  {
    label: "lf.collect",
    type: "method",
    info: "Execute and collect to DataFrame",
    apply: "lf.collect()",
  },
  { label: "lf.lazy", type: "method", info: "Convert to LazyFrame", apply: "lf.lazy()" },

  // Expression methods
  { label: ".alias", type: "method", info: "Rename expression result", apply: '.alias("")' },
  { label: ".cast", type: "method", info: "Cast to type", apply: ".cast(pl.Utf8)" },
  { label: ".is_null", type: "method", info: "Check for null", apply: ".is_null()" },
  { label: ".is_not_null", type: "method", info: "Check for not null", apply: ".is_not_null()" },
  { label: ".fill_null", type: "method", info: "Fill null values", apply: ".fill_null()" },
  { label: ".sum", type: "method", info: "Sum values", apply: ".sum()" },
  { label: ".mean", type: "method", info: "Calculate mean", apply: ".mean()" },
  { label: ".min", type: "method", info: "Get minimum", apply: ".min()" },
  { label: ".max", type: "method", info: "Get maximum", apply: ".max()" },
  { label: ".count", type: "method", info: "Count values", apply: ".count()" },
  { label: ".first", type: "method", info: "Get first value", apply: ".first()" },
  { label: ".last", type: "method", info: "Get last value", apply: ".last()" },
  { label: ".str", type: "property", info: "String operations namespace", apply: ".str." },
  { label: ".dt", type: "property", info: "Datetime operations namespace", apply: ".dt." },
  { label: ".list", type: "property", info: "List operations namespace", apply: ".list." },
  { label: ".over", type: "method", info: "Window function over groups", apply: '.over("")' },
];

// SecretStr methods for autocompletion
const secretStrMethods = [
  {
    label: "get_secret_value",
    type: "method",
    info: "Get the decrypted secret value as a string",
    apply: "get_secret_value()",
    detail: "SecretStr",
  },
];

// nd.ColumnActionValue attributes (the `.value` of a ColumnActionInput).
const columnActionValueMembers = [
  {
    label: "rows",
    type: "property",
    info: "list[nd.ColumnActionRow] — each has .column / .action / .output_name",
    detail: "list[ColumnActionRow]",
  },
  {
    label: "group_by_columns",
    type: "property",
    info: "Selected group-by columns ([] unless show_group_by=True)",
    detail: "list[str]",
  },
  {
    label: "order_by_column",
    type: "property",
    info: "Selected order-by column (None unless show_order_by=True)",
    detail: "str | None",
  },
];

// Component type -> Python type of its `.value` (`.secret_value` for secrets),
// shown as the completion `detail` so suggestions carry a type hint.
export const PY_TYPE: Record<string, string> = {
  SliderInput: "float",
  NumericInput: "float",
  TextInput: "str",
  ToggleSwitch: "bool",
  SingleSelect: "str",
  MultiSelect: "list",
  ColumnSelector: "str | list[str]",
  SecretSelector: "SecretStr",
  ColumnActionInput: "ColumnActionValue",
};

export const pyType = (componentType: string): string => PY_TYPE[componentType] ?? "Any";

// Must match FormFieldsOverview.vue's accessor() so click-insert and
// autocomplete-insert produce identical code.
export const settingsFieldAccessor = (
  sectionName: string,
  fieldName: string,
  componentType: string,
): string => {
  const attribute = componentType === "SecretSelector" ? "secret_value" : "value";
  return `self.settings_schema.${sectionName}.${fieldName}.${attribute}`;
};

const PY_KEYWORDS = new Set([
  "False",
  "None",
  "True",
  "and",
  "as",
  "assert",
  "async",
  "await",
  "break",
  "class",
  "continue",
  "def",
  "del",
  "elif",
  "else",
  "except",
  "finally",
  "for",
  "from",
  "global",
  "if",
  "import",
  "in",
  "is",
  "lambda",
  "nonlocal",
  "not",
  "or",
  "pass",
  "raise",
  "return",
  "try",
  "while",
  "with",
  "yield",
  "self",
]);

const INDENT = "    ";
const DEDENT_RE = /^(return|pass|raise|break|continue|\.\.\.)\b/;

// The Code tab edits only the function BODY (dedented, no enclosing `def`), so
// Lezer's Python indent service sees top-level "unexpected indent" and returns 0
// — insertNewlineAndIndent then drops to column 0. Compute the new-line indent
// ourselves from the current line's leading whitespace instead.
export function computeNewlineIndent(lineText: string, cursorOffsetInLine: number): string {
  const beforeCursor = lineText.slice(0, cursorOffsetInLine);
  const leadingWs = /^[ \t]*/.exec(lineText)?.[0] ?? "";
  const codeBefore = beforeCursor.trim().replace(/#.*$/, "").trimEnd();
  if (codeBefore.endsWith(":")) return leadingWs + INDENT;
  if (DEDENT_RE.test(beforeCursor.trim())) {
    return leadingWs.length >= INDENT.length ? leadingWs.slice(INDENT.length) : "";
  }
  return leadingWs;
}

function fragmentSafeNewline(view: EditorView): boolean {
  // Yield to an open completion popup so Enter still accepts a suggestion.
  if (completionStatus(view.state) === "active") return false;
  const { state } = view;
  if (state.selection.ranges.length > 1) return false; // let the stock command handle multi-cursor
  const range = state.selection.main;
  const line = state.doc.lineAt(range.head);
  const indent = computeNewlineIndent(line.text, range.head - line.from);
  const insert = "\n" + indent;
  view.dispatch(
    state.update(
      {
        changes: { from: range.from, to: range.to, insert },
        selection: { anchor: range.from + insert.length },
        scrollIntoView: true,
      },
      { userEvent: "input" },
    ),
  );
  return true;
}

const newlineKeymap = keymap.of([
  { key: "Enter", run: fragmentSafeNewline },
  { key: "Shift-Enter", run: fragmentSafeNewline },
]);

// Bare-word sources must defer to the dotted-path source (schemaCompletions),
// which owns everything after a `.`.
export function precededByDot(context: CompletionContext): boolean {
  const before = context.matchBefore(/\.\w*/);
  return before !== null && before.text.startsWith(".");
}

// True when the cursor sits inside a `def name(...)` parameter list; returns the
// param text typed so far (before the cursor). Single-paren-level match, so it
// stops being true once a nested `(` (e.g. a default value) appears — fine, since
// we only use it to offer `self` at the first-parameter position.
export function insideDefParams(context: CompletionContext): { before: string } | null {
  const text = context.state.doc.sliceString(0, context.pos);
  const m = text.match(/\bdef\s+[A-Za-z_]\w*\s*\(([^()]*)$/);
  return m ? { before: m[1] } : null;
}

// Offer `self` as the first parameter of a def — closing the loop with the lint
// hint that a helper method needs `self` (called as self.<name>(...)).
export function selfParamSource(context: CompletionContext): CompletionResult | null {
  const params = insideDefParams(context);
  if (!params) return null;
  const word = context.matchBefore(/[A-Za-z_]\w*/);
  const typed = word ? word.text : "";
  // self only belongs as the FIRST parameter, and only if it prefixes what's typed.
  if (params.before.slice(0, params.before.length - typed.length).trim() !== "") return null;
  if (!"self".startsWith(typed)) return null;
  if (!word && !context.explicit) return null;
  return {
    from: word ? word.from : context.pos,
    options: [
      {
        label: "self",
        type: "keyword",
        detail: "method parameter",
        info: "Make this a method: 'self' as the first parameter lets you call it with self.<name>(...).",
        boost: 99,
      },
    ],
    validFor: /^\w*$/,
  };
}

interface LocalSymbol {
  name: string;
  type: string;
}

// Heuristic (not a parser): find names the user has bound so their own
// variables autocomplete. Anchored at line start; false positives inside
// strings are harmless as suggestions.
export function scanLocalSymbols(doc: string): LocalSymbol[] {
  const found = new Map<string, LocalSymbol>();
  const add = (name: string, type = "variable") => {
    if (!name || /^\d/.test(name) || PY_KEYWORDS.has(name)) return;
    const existing = found.get(name);
    if (!existing) found.set(name, { name, type });
    else if (type !== "variable") existing.type = type;
  };
  for (const raw of doc.split("\n")) {
    const line = raw.trim();
    // name = ...  or  name: type = ...   (=(?!=) so ==/<=/>=/!= don't match)
    let m = line.match(/^([A-Za-z_]\w*)\s*(?::\s*[^=]+)?=(?!=)/);
    if (m) {
      add(m[1]);
      continue;
    }
    // tuple unpacking:  a, b = ...   /   (a, b) = ...
    m = line.match(/^\(?\s*([A-Za-z_]\w*(?:\s*,\s*[A-Za-z_]\w*)+)\s*\)?\s*=(?!=)/);
    if (m) {
      m[1].split(",").forEach((t) => add(t.trim()));
      continue;
    }
    // for a in ...   /   for a, b in ...
    m = line.match(/^for\s+\(?\s*([A-Za-z_]\w*(?:\s*,\s*[A-Za-z_]\w*)*)\s*\)?\s+in\b/);
    if (m) {
      m[1].split(",").forEach((t) => add(t.trim()));
      continue;
    }
    // with expr as name:  (comma-separated `as` targets)
    for (const wm of line.matchAll(/\bas\s+([A-Za-z_]\w*)/g)) add(wm[1]);
    m = line.match(/^(?:async\s+)?def\s+([A-Za-z_]\w*)/);
    if (m) {
      add(m[1], "function");
      continue;
    }
    m = line.match(/^class\s+([A-Za-z_]\w*)/);
    if (m) {
      add(m[1], "class");
      continue;
    }
  }
  return [...found.values()];
}

// ---- Offline type inference for local variables --------------------------
// A best-effort, regex-based inferrer (no real parser). It powers two things:
// the type shown next to a local-variable suggestion, and — more importantly —
// which member methods appear after `var.` (so a float doesn't get LazyFrame
// methods). Unknown types deliberately yield no member suggestions rather than
// a misleading guess.

const meth = (label: string, info: string, apply?: string): Completion => ({
  label,
  type: "method",
  info,
  apply: apply ?? `${label}()`,
});

const strMethods: Completion[] = [
  meth("upper", "Uppercase copy"),
  meth("lower", "Lowercase copy"),
  meth("strip", "Trim surrounding whitespace"),
  meth("lstrip", "Trim left"),
  meth("rstrip", "Trim right"),
  meth("replace", "Replace a substring", 'replace("", "")'),
  meth("split", "Split into a list", "split()"),
  meth("rsplit", "Split from the right"),
  meth("splitlines", "Split on line breaks"),
  meth("join", "Join an iterable of strings", "join()"),
  meth("startswith", "Prefix test", 'startswith("")'),
  meth("endswith", "Suffix test", 'endswith("")'),
  meth("find", "Index of a substring, or -1", 'find("")'),
  meth("index", "Index of a substring (raises)", 'index("")'),
  meth("count", "Count occurrences", 'count("")'),
  meth("format", "Format placeholders", "format()"),
  meth("encode", "Encode to bytes", "encode()"),
  meth("title", "Title-case copy"),
  meth("capitalize", "Capitalize the first character"),
  meth("casefold", "Aggressive lowercase"),
  meth("zfill", "Left-pad with zeros", "zfill()"),
  meth("removeprefix", "Drop a prefix", 'removeprefix("")'),
  meth("removesuffix", "Drop a suffix", 'removesuffix("")'),
  meth("isdigit", "All characters digits?"),
  meth("isnumeric", "All characters numeric?"),
  meth("isalpha", "All characters letters?"),
  meth("isalnum", "All characters alphanumeric?"),
];

const listMethods: Completion[] = [
  meth("append", "Add one item", "append()"),
  meth("extend", "Add items from an iterable", "extend()"),
  meth("insert", "Insert at an index", "insert()"),
  meth("remove", "Remove the first matching value", "remove()"),
  meth("pop", "Remove & return by index"),
  meth("clear", "Remove all items"),
  meth("index", "Index of a value", "index()"),
  meth("count", "Count a value", "count()"),
  meth("sort", "Sort in place"),
  meth("reverse", "Reverse in place"),
  meth("copy", "Shallow copy"),
];

const dictMethods: Completion[] = [
  meth("keys", "View of keys"),
  meth("values", "View of values"),
  meth("items", "View of (key, value) pairs"),
  meth("get", "Value for a key, or a default", "get()"),
  meth("pop", "Remove & return by key", "pop()"),
  meth("update", "Merge another mapping", "update()"),
  meth("setdefault", "Get, inserting a default", "setdefault()"),
  meth("popitem", "Remove & return the last pair"),
  meth("clear", "Remove all items"),
  meth("copy", "Shallow copy"),
];

const setMethods: Completion[] = [
  meth("add", "Add an element", "add()"),
  meth("remove", "Remove (raises if absent)", "remove()"),
  meth("discard", "Remove if present", "discard()"),
  meth("pop", "Remove & return an element"),
  meth("union", "Union", "union()"),
  meth("intersection", "Intersection", "intersection()"),
  meth("difference", "Difference", "difference()"),
  meth("issubset", "Subset test", "issubset()"),
  meth("issuperset", "Superset test", "issuperset()"),
  meth("update", "In-place union", "update()"),
  meth("clear", "Remove all elements"),
  meth("copy", "Shallow copy"),
];

const tupleMethods: Completion[] = [
  meth("count", "Count a value", "count()"),
  meth("index", "Index of a value", "index()"),
];

const floatMethods: Completion[] = [
  meth("is_integer", "Is this a whole number?"),
  meth("hex", "Hexadecimal string form"),
  meth("as_integer_ratio", "(numerator, denominator)"),
];

const intMethods: Completion[] = [
  meth("bit_length", "Bits needed to represent"),
  meth("bit_count", "Number of set bits"),
  meth("to_bytes", "Encode to bytes", "to_bytes()"),
  meth("as_integer_ratio", "(numerator, denominator)"),
];

const bytesMethods: Completion[] = [
  meth("decode", "Decode to str", "decode()"),
  meth("hex", "Hexadecimal string form"),
  meth("split", "Split into a list", "split()"),
  meth("replace", "Replace a subsequence", "replace()"),
  meth("startswith", "Prefix test", "startswith()"),
  meth("endswith", "Suffix test", "endswith()"),
];

const exprMethods: Completion[] = [
  meth("alias", "Rename the expression", 'alias("")'),
  meth("cast", "Cast to a dtype", "cast()"),
  meth("is_null", "Null mask"),
  meth("is_not_null", "Non-null mask"),
  meth("fill_null", "Replace nulls", "fill_null()"),
  meth("fill_nan", "Replace NaNs", "fill_nan()"),
  meth("sum", "Sum"),
  meth("mean", "Mean"),
  meth("min", "Minimum"),
  meth("max", "Maximum"),
  meth("median", "Median"),
  meth("std", "Standard deviation"),
  meth("count", "Count"),
  meth("n_unique", "Distinct count"),
  meth("first", "First value"),
  meth("last", "Last value"),
  meth("abs", "Absolute value"),
  meth("round", "Round to n decimals", "round()"),
  meth("clip", "Clip to bounds", "clip()"),
  meth("cum_sum", "Cumulative sum"),
  meth("diff", "First difference"),
  meth("shift", "Shift values", "shift()"),
  meth("rank", "Ranks"),
  meth("sort", "Sort values"),
  meth("unique", "Distinct values"),
  meth("over", "Window over groups", 'over("")'),
  meth("filter", "Filter by a predicate", "filter()"),
  meth("map_elements", "Apply a Python function", "map_elements()"),
  { label: "str", type: "property", info: "String namespace", apply: "str." },
  { label: "dt", type: "property", info: "Datetime namespace", apply: "dt." },
  { label: "list", type: "property", info: "List namespace", apply: "list." },
];

const plModuleMethods: Completion[] = [
  meth("col", "Reference a column", 'col("")'),
  meth("lit", "A literal value", "lit()"),
  meth("when", "Start a conditional", "when()"),
  meth("all", "All columns"),
  meth("exclude", "All columns except", 'exclude("")'),
  meth("sum", "Sum a column", 'sum("")'),
  meth("min", "Min of a column", 'min("")'),
  meth("max", "Max of a column", 'max("")'),
  meth("mean", "Mean of a column", 'mean("")'),
  meth("count", "Row count"),
  meth("first", "First value"),
  meth("last", "Last value"),
  meth("struct", "Struct expression", "struct()"),
  meth("concat", "Concatenate frames", "concat()"),
  meth("concat_str", "Concatenate strings", "concat_str()"),
  meth("format", "Format a string expression", "format()"),
  meth("int_range", "Integer range", "int_range()"),
  meth("date", "Date literal", "date()"),
  meth("datetime", "Datetime literal", "datetime()"),
  { label: "DataFrame", type: "class", info: "Eager frame", apply: "DataFrame()" },
  { label: "LazyFrame", type: "class", info: "Lazy frame", apply: "LazyFrame()" },
  { label: "Series", type: "class", info: "One-dimensional series", apply: "Series()" },
];

// Methods shared by the `logging` module and a `logging.getLogger()` Logger.
// Output lands in the Test panel's Logs (captured at DEBUG on dry-run).
const loggerMethods: Completion[] = [
  meth("debug", "Log a DEBUG message", 'debug("")'),
  meth("info", "Log an INFO message", 'info("")'),
  meth("warning", "Log a WARNING message", 'warning("")'),
  meth("error", "Log an ERROR message", 'error("")'),
  meth("critical", "Log a CRITICAL message", 'critical("")'),
  meth("exception", "Log an ERROR with the current traceback", 'exception("")'),
  meth("log", "Log at an explicit level", "log()"),
];

const loggingMethods: Completion[] = [
  ...loggerMethods,
  meth("getLogger", "Get a named logger", "getLogger()"),
];

interface Assignment {
  name: string;
  annotation: string | null;
  rhs: string;
}

// Line-anchored scan of single-target assignments, capturing any annotation and
// the right-hand side (=(?!=) so ==/<=/>= aren't treated as assignments).
export function scanAssignments(doc: string): Assignment[] {
  const out: Assignment[] = [];
  for (const raw of doc.split("\n")) {
    const line = raw.trim();
    const m = line.match(/^([A-Za-z_]\w*)\s*(?::\s*([^=]+?))?\s*=(?!=)\s*(.*)$/);
    if (m && !PY_KEYWORDS.has(m[1])) {
      out.push({ name: m[1], annotation: m[2] ? m[2].trim() : null, rhs: (m[3] ?? "").trim() });
    }
  }
  return out;
}

// Reduce an annotation / class name to a canonical bare type token.
export function normalizePyType(raw: string): string {
  let s = raw
    .trim()
    .replace(/\s*\|\s*None\b/g, "")
    .trim();
  const opt = s.match(/^Optional\[(.+)\]$/);
  if (opt) s = opt[1].trim();
  s = s.replace(/\[.*\]$/, "").trim(); // drop generic params: list[str] -> list
  s = s.replace(/^[\w.]+\./, ""); // drop a module prefix: pl.LazyFrame -> LazyFrame
  const lower = s.toLowerCase();
  if (lower === "lazyframe") return "LazyFrame";
  if (lower === "dataframe") return "DataFrame";
  if (s === "Expr" || s === "Series" || s === "SecretStr") return s;
  if (["str", "int", "float", "bool", "bytes", "list", "dict", "set", "tuple"].includes(lower)) {
    return lower;
  }
  return s;
}

function findSettingsComponent(
  sections: SectionState[],
  sectionName: string,
  fieldName: string,
): ComponentState | null {
  for (const section of sections) {
    const sName = section.name || toSnakeCase(section.title || "section");
    if (sName !== sectionName) continue;
    for (const comp of section.components) {
      if (toSnakeCase(comp.name) === fieldName) return comp;
    }
  }
  return null;
}

interface SelfMethod {
  name: string;
  returnType: string | null;
}

// Methods the user defined on the node — `def name(self, ...)` — so `self.` can
// suggest them. Best-effort infers each return type from the first top-level
// `return`, so `x = self.foo()` can be typed too. `process` is excluded (you
// don't call self.process()).
export function scanSelfMethods(doc: string, sections: SectionState[] = []): SelfMethod[] {
  const lines = doc.split("\n");
  const out: SelfMethod[] = [];
  const seen = new Set<string>();
  for (let i = 0; i < lines.length; i++) {
    const def = lines[i].match(/^(\s*)(?:async\s+)?def\s+([A-Za-z_]\w*)\s*\(\s*self\b/);
    if (!def || def[2] === "process" || seen.has(def[2])) continue;
    seen.add(def[2]);
    const defIndent = def[1].length;
    let returnType: string | null = null;
    for (let j = i + 1; j < lines.length; j++) {
      if (lines[j].trim() === "") continue;
      const indent = /^\s*/.exec(lines[j])![0].length;
      if (indent <= defIndent) break; // left the method body
      const ret = lines[j].trim().match(/^return\s+(\S.*)$/);
      if (ret) {
        returnType = inferFromRhs(ret[1].trim(), new Map(), sections);
        break;
      }
    }
    out.push({ name: def[2], returnType });
  }
  return out;
}

function inferFromRhs(
  rhs: string,
  types: Map<string, string>,
  sections: SectionState[],
  selfReturns: Map<string, string> = new Map(),
): string | null {
  const s = rhs.trim();
  if (!s) return null;
  if (/^inputs\s*\[\s*\d+\s*\]/.test(s)) {
    // inputs[0] -> LazyFrame; inputs[0].collect() -> DataFrame; other chains stay lazy.
    if (/^inputs\s*\[\s*\d+\s*\]\s*$/.test(s)) return "LazyFrame";
    return /\.collect\s*\(/.test(s) ? "DataFrame" : "LazyFrame";
  }
  // flowfile_ctx kernel reads (kernel-environment nodes): frame readers return
  // LazyFrames (DataFrame once .collect()ed); read_inputs() yields a dict.
  const ctxRead = s.match(/^(?:flowfile_ctx|flowfile)\.(\w+)\s*\(/);
  if (ctxRead) {
    const fn = ctxRead[1];
    if (fn === "read_input" || fn === "read_first" || fn === "read_catalog_table") {
      return /\.collect\s*\(/.test(s) ? "DataFrame" : "LazyFrame";
    }
    if (fn === "read_inputs") return "dict";
    return null;
  }
  const settings = s.match(/^self\.settings_schema\.(\w+)\.(\w+)\.(value|secret_value)\b/);
  if (settings) {
    if (settings[3] === "secret_value") return "SecretStr";
    const comp = findSettingsComponent(sections, settings[1], settings[2]);
    return comp ? normalizePyType(pyType(comp.component_type)) : null;
  }
  // x = self.my_method(...) -> the method's inferred return type
  const selfCall = s.match(/^self\.([A-Za-z_]\w*)\s*\(/);
  if (selfCall) return selfReturns.get(selfCall[1]) ?? null;
  if (/\.get_secret_value\s*\(\s*\)\s*$/.test(s)) return "str";
  if (/^[frbuFRBU]{0,2}['"]/.test(s)) return "str";
  if (/^(True|False)\b/.test(s)) return "bool";
  if (/^-?\d[\d_]*\.\d*([eE][+-]?\d+)?$/.test(s) || /^-?\.\d+/.test(s)) return "float";
  if (/^-?\d[\d_]*([eE][+-]?\d+)?$/.test(s)) return "int";
  if (/^\[/.test(s)) return "list";
  if (/^\{/.test(s)) return /:/.test(s) ? "dict" : "set";
  if (/^\(/.test(s)) return "tuple";
  const ctor = s.match(/^(str|int|float|bool|list|dict|set|tuple|bytes)\s*\(/);
  if (ctor) return ctor[1];
  if (/^pl\./.test(s)) {
    if (/^pl\.(LazyFrame|scan_\w+)\b/.test(s)) return "LazyFrame";
    if (/^pl\.(DataFrame|read_\w+)\b/.test(s)) return "DataFrame";
    if (/^pl\.Series\b/.test(s)) return "Series";
    if (/^pl\.concat\b/.test(s)) return "LazyFrame";
    return "Expr";
  }
  if (/^logging\.getLogger\b/.test(s)) return "Logger";
  // Chain off a known variable: alias, or a method that preserves the frame kind.
  const root = s.match(/^([A-Za-z_]\w*)\b/);
  if (root) {
    const rootType = types.get(root[1]);
    if (rootType) {
      if (/^[A-Za-z_]\w*\s*$/.test(s)) return rootType; // y = x
      if (rootType === "LazyFrame") return /\.collect\s*\(/.test(s) ? "DataFrame" : "LazyFrame";
      if (rootType === "DataFrame") return /\.lazy\s*\(/.test(s) ? "LazyFrame" : "DataFrame";
      if (rootType === "Expr" || rootType === "Series") return rootType;
    }
  }
  return null;
}

// Map every assigned local to its best-guess Python type. Annotations win; the
// rest is inferred to a fixed point so aliases/chains that depend on earlier
// variables resolve (e.g. lf = inputs[0]; out = lf.filter(...) -> both LazyFrame).
export function inferLocalTypes(doc: string, sections: SectionState[]): Map<string, string> {
  const bindings = scanAssignments(doc);
  const selfReturns = new Map<string, string>();
  for (const sm of scanSelfMethods(doc, sections)) {
    if (sm.returnType) selfReturns.set(sm.name, sm.returnType);
  }
  const annotated = new Map<string, string>();
  for (const b of bindings) if (b.annotation) annotated.set(b.name, normalizePyType(b.annotation));
  const types = new Map(annotated);
  for (let pass = 0; pass < 4; pass++) {
    let changed = false;
    for (const b of bindings) {
      if (annotated.has(b.name)) continue;
      const inferred = inferFromRhs(b.rhs, types, sections, selfReturns);
      if (inferred && types.get(b.name) !== inferred) {
        types.set(b.name, inferred);
        changed = true;
      }
    }
    if (!changed) break;
  }
  return types;
}

// Methods to offer after `var.`, chosen by the variable's inferred type. `pl` is
// the polars module. Unknown/valueless types return null (no member suggestions).
export function memberCompletionsFor(
  varName: string,
  types: Map<string, string>,
): Completion[] | null {
  if (varName === "pl") return plModuleMethods;
  switch (types.get(varName)) {
    case "LazyFrame":
    case "DataFrame":
      return lazyFrameMethods;
    case "Expr":
    case "Series":
      return exprMethods;
    case "str":
      return strMethods;
    case "int":
      return intMethods;
    case "float":
      return floatMethods;
    case "list":
      return listMethods;
    case "dict":
      return dictMethods;
    case "set":
      return setMethods;
    case "tuple":
      return tupleMethods;
    case "bytes":
      return bytesMethods;
    case "SecretStr":
      return secretStrMethods;
    case "ColumnActionValue":
      return columnActionValueMembers;
    case "Logger":
      return loggerMethods;
    default:
      return null;
  }
}

// Suggest the user's own bound names, tagging each with its inferred type when
// known (so `test` reads as `float`, not just `local`).
export function makeLocalVariableSource(
  getSections: () => SectionState[],
): (context: CompletionContext) => CompletionResult | null {
  return (context) => {
    if (precededByDot(context) || insideDefParams(context)) return null;
    const word = context.matchBefore(/[A-Za-z_]\w*/);
    if (!word && !context.explicit) return null;
    if (word && word.from === word.to) return null;
    const current = word ? word.text : "";
    const doc = context.state.doc.toString();
    const symbols = scanLocalSymbols(doc).filter((s) => s.name !== current);
    if (!symbols.length) return null;
    const types = inferLocalTypes(doc, getSections());
    return {
      from: word ? word.from : context.pos,
      options: symbols.map((s) => ({
        label: s.name,
        type: s.type,
        detail: types.get(s.name) ?? (s.type === "variable" ? "local" : s.type),
      })),
      validFor: /^\w*$/,
    };
  };
}

// Offer each settings field as a top-level suggestion whose `apply` inserts the
// full `self.settings_schema.<section>.<field>.value` accessor, with the Python
// type shown as detail — so users don't have to type the whole path. Fields the
// user has already bound to a local of the same name are skipped: the local var
// (offered by localVariableSource) is the thing to reference, so the accessor
// snippet would just be a redundant duplicate entry in the popup.
export function makeSettingsFieldSnippetSource(
  getSections: () => SectionState[],
): (context: CompletionContext) => CompletionResult | null {
  return (context) => {
    if (precededByDot(context) || insideDefParams(context)) return null;
    const word = context.matchBefore(/[A-Za-z_]\w*/);
    if (!word && !context.explicit) return null;
    const boundLocals = new Set(scanLocalSymbols(context.state.doc.toString()).map((s) => s.name));
    const options: Completion[] = [];
    for (const section of getSections()) {
      const sectionName = section.name || toSnakeCase(section.title || "section");
      for (const comp of section.components) {
        const fieldName = toSnakeCase(comp.name);
        if (boundLocals.has(fieldName)) continue;
        const accessor = settingsFieldAccessor(sectionName, fieldName, comp.component_type);
        options.push({
          label: fieldName,
          type: "property",
          detail: pyType(comp.component_type),
          info: `${comp.label ?? fieldName} — inserts ${accessor}`,
          apply: accessor,
          boost: 1,
        });
      }
    }
    if (!options.length) return null;
    return { from: word ? word.from : context.pos, options, validFor: /^\w*$/ };
  };
}

export function usePolarsAutocompletion(
  getSections: () => SectionState[],
  isCodeOnly: () => boolean = () => false,
  isKernel: () => boolean = () => false,
) {
  function findSecretStrVariables(doc: string): string[] {
    const variables: string[] = [];
    // Match patterns like: var_name: SecretStr = ... or var_name: SecretStr=...
    const typeAnnotationPattern = /(\w+)\s*:\s*SecretStr\s*=/g;
    let match;
    while ((match = typeAnnotationPattern.exec(doc)) !== null) {
      variables.push(match[1]);
    }
    return variables;
  }

  function schemaCompletions(context: CompletionContext): CompletionResult | null {
    // Inside a `def name(...)` param list, selfParamSource handles it — no Polars noise.
    if (insideDefParams(context)) return null;
    const beforeCursor = context.state.doc.sliceString(0, context.pos);
    const fullDoc = context.state.doc.toString();
    const sections = getSections();

    const secretStrVars = findSecretStrVariables(fullDoc);
    for (const varName of secretStrVars) {
      const varMethodMatch = beforeCursor.match(new RegExp(`\\b${varName}\\.(\\w*)$`));
      if (varMethodMatch) {
        const typed = varMethodMatch[1];
        return {
          from: context.pos - typed.length,
          options: secretStrMethods,
          validFor: /^\w*$/,
        };
      }
    }

    const secretStrMethodMatch = beforeCursor.match(/\.secret_value\.(\w*)$/);
    if (secretStrMethodMatch) {
      const typed = secretStrMethodMatch[1];
      return {
        from: context.pos - typed.length,
        options: secretStrMethods,
        validFor: /^\w*$/,
      };
    }

    for (const section of sections) {
      const sectionName = section.name || toSnakeCase(section.title || "section");
      for (const comp of section.components) {
        const fieldName = toSnakeCase(comp.name);
        const valueMatch = beforeCursor.match(
          new RegExp(`self\\.settings_schema\\.${sectionName}\\.${fieldName}\\.(\\w*)$`),
        );
        if (valueMatch) {
          const typed = valueMatch[1];
          // For SecretSelector, show secret_value instead of value
          if (comp.component_type === "SecretSelector") {
            return {
              from: context.pos - typed.length,
              options: [
                {
                  label: "secret_value",
                  type: "property",
                  info: "Get the decrypted secret value (SecretStr)",
                  detail: pyType(comp.component_type),
                },
              ],
              validFor: /^\w*$/,
            };
          }
          return {
            from: context.pos - typed.length,
            options: [
              {
                label: "value",
                type: "property",
                info: "Get the setting value",
                detail: pyType(comp.component_type),
              },
            ],
            validFor: /^\w*$/,
          };
        }
      }
    }

    for (const section of sections) {
      const sectionName = section.name || toSnakeCase(section.title || "section");
      const sectionMatch = beforeCursor.match(
        new RegExp(`self\\.settings_schema\\.${sectionName}\\.(\\w*)$`),
      );

      if (sectionMatch) {
        const typed = sectionMatch[1];
        const componentOptions = section.components.map((comp) => {
          const fieldName = toSnakeCase(comp.name);
          return {
            label: fieldName,
            type: "property",
            info: `${comp.component_type}: ${comp.label}`,
            detail: comp.component_type,
          };
        });

        return {
          from: context.pos - typed.length,
          options: componentOptions,
          validFor: /^\w*$/,
        };
      }
    }

    const settingsMatch = beforeCursor.match(/self\.settings_schema\.(\w*)$/);
    if (settingsMatch) {
      const typed = settingsMatch[1];
      const sectionOptions = sections.map((section) => {
        const sectionName = section.name || toSnakeCase(section.title || "section");
        const sectionTitle = section.title || section.name || "Section";
        return {
          label: sectionName,
          type: "property",
          info: `Section: ${sectionTitle}`,
          detail: "Section",
        };
      });

      return {
        from: context.pos - typed.length,
        options: sectionOptions,
        validFor: /^\w*$/,
      };
    }

    const selfDotMatch = beforeCursor.match(/self\.(\w*)$/);
    if (selfDotMatch) {
      const typed = selfDotMatch[1];
      const options: Completion[] = [
        { label: "settings_schema", type: "property", info: "Access node settings" },
      ];
      for (const sm of scanSelfMethods(fullDoc, sections)) {
        options.push({
          label: sm.name,
          type: "method",
          info: sm.returnType ? `Method — returns ${sm.returnType}` : "Method on this node",
          apply: `${sm.name}()`,
        });
      }
      return { from: context.pos - typed.length, options, validFor: /^\w*$/ };
    }

    // inputs[0]. etc. are always LazyFrames (the identifier match below can't see
    // through the `[...]` subscript).
    const inputsMemberMatch = beforeCursor.match(/inputs\s*\[\s*\d+\s*\]\.(\w*)$/);
    if (inputsMemberMatch) {
      const typed = inputsMemberMatch[1];
      return { from: context.pos - typed.length, options: lazyFrameMethods, validFor: /^\w*$/ };
    }

    // logging.<method> — the module and a getLogger()-returned Logger share members.
    const loggingMatch = beforeCursor.match(/\blogging\.(\w*)$/);
    if (loggingMatch) {
      const typed = loggingMatch[1];
      return { from: context.pos - typed.length, options: loggingMethods, validFor: /^\w*$/ };
    }

    // Member access on a plain identifier: offer methods for its inferred type,
    // or nothing when the type is unknown (rather than misleading LazyFrame ones).
    const memberMatch = beforeCursor.match(/([A-Za-z_]\w*)\.(\w*)$/);
    if (memberMatch) {
      const typed = memberMatch[2];
      const options = memberCompletionsFor(memberMatch[1], inferLocalTypes(fullDoc, sections));
      if (!options) return null;
      return { from: context.pos - typed.length, options, validFor: /^\w*$/ };
    }

    const wordMatch = context.matchBefore(/\w+/);
    if (!wordMatch && !context.explicit) return null;

    return {
      from: wordMatch ? wordMatch.from : context.pos,
      options: polarsCompletions,
      validFor: /^\w*$/,
    };
  }

  const localVariableSource = makeLocalVariableSource(getSections);
  const settingsFieldSnippetSource = makeSettingsFieldSnippetSource(getSections);
  const refVariableSource = createRefVariableCompletions(() => []) as SyncCompletionSource;

  // `flowfile_ctx` is injected only into kernel-executed process() bodies — the
  // local/worker path (custom_node_runner.py) never binds it — so gate every
  // suggestion on the kernel environment. Offering it for a local node would
  // autocomplete an API that NameErrors at run time. Delegates to the same shared
  // catalog the Python Script editor uses (@/utils/flowfileCtxCompletions).
  function flowfileCtxSource(context: CompletionContext): CompletionResult | null {
    if (!isKernel()) return null;
    const api = ffApiSource(context); // after `flowfile_ctx.` / `flowfile.`
    if (api) return api;
    const chain = ffCatalogChainSource(context); // after `.get_catalog(...)` etc.
    if (chain) return chain;
    const refVar = refVariableSource(context); // locals bound to a catalog/schema/table ref
    if (refVar) return refVar;
    // Bare-word: suggest `flowfile_ctx` (and the deprecated `flowfile`) as the
    // user starts an identifier — but not for member access or def-param lists.
    if (precededByDot(context) || insideDefParams(context)) return null;
    const word = context.matchBefore(/[A-Za-z_]\w*/);
    if (!word && !context.explicit) return null;
    if (!context.explicit && word && word.text.length < 2) return null;
    return {
      from: word ? word.from : context.pos,
      options: FLOWFILE_CTX_GLOBAL_ENTRIES,
      validFor: /^\w*$/,
    };
  }

  const tabKeymap = keymap.of([
    {
      key: "Tab",
      run: (view: EditorView): boolean => {
        if (acceptCompletion(view)) {
          return true;
        }
        return indentMore(view);
      },
    },
    {
      key: "Shift-Tab",
      run: (view: EditorView): boolean => {
        return indentLess(view);
      },
    },
  ]);

  // Note: Prec.highest ensures completion keybindings take priority over other keymaps
  const extensions: Extension[] = [
    python(),
    oneDark,
    EditorState.tabSize.of(4),
    indentUnit.of(INDENT),
    autocompletion({
      override: [
        flowfileCtxSource,
        selfParamSource,
        schemaCompletions,
        localVariableSource,
        settingsFieldSnippetSource,
      ],
      defaultKeymap: true,
      closeOnBlur: false,
      activateOnTyping: true,
    }),
    bodyTooltips(),
    makeProcessBodyLinter(() => !isCodeOnly()),
    Prec.highest(newlineKeymap),
    Prec.highest(tabKeymap),
  ];

  const readOnlyExtensions: Extension[] = [
    python(),
    oneDark,
    EditorState.tabSize.of(4),
    indentUnit.of(INDENT),
    EditorView.editable.of(false),
    EditorState.readOnly.of(true),
  ];

  return {
    extensions,
    readOnlyExtensions,
    schemaCompletions,
    flowfileCtxSource,
  };
}
