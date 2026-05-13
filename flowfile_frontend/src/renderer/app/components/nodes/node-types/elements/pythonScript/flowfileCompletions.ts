import type { Completion, CompletionSource } from "@codemirror/autocomplete";

import type { UpstreamColumn } from "./useUpstreamColumns";

// ─── Static entries ──────────────────────────────────────────────────────────

const FLOWFILE_API_ENTRIES: Completion[] = [
  {
    label: "read_input",
    type: "function",
    info: "Read input DataFrame. Optional name parameter for named inputs.",
    detail: "flowfile.read_input(name?)",
    apply: "read_input()",
  },
  {
    label: "read_inputs",
    type: "function",
    info: "Read all inputs as a dict of LazyFrame lists (one per connection).",
    detail: "flowfile.read_inputs() -> dict[str, list[LazyFrame]]",
    apply: "read_inputs()",
  },
  {
    label: "publish_output",
    type: "function",
    info: "Write output DataFrame. Optional name parameter for named outputs.",
    detail: "flowfile.publish_output(df, name?)",
    apply: "publish_output(df)",
  },
  {
    label: "display",
    type: "function",
    info: "Display a rich object (matplotlib figure, plotly figure, PIL image, HTML string) in the output panel.",
    detail: "flowfile.display(obj, title?)",
    apply: "display(obj)",
  },
  {
    label: "publish_artifact",
    type: "function",
    info: "Store a Python object as a named artifact in kernel memory.",
    detail: 'flowfile.publish_artifact("name", obj)',
    apply: 'publish_artifact("name", obj)',
  },
  {
    label: "read_artifact",
    type: "function",
    info: "Retrieve a Python object from a named artifact.",
    detail: 'flowfile.read_artifact("name")',
    apply: 'read_artifact("name")',
  },
  {
    label: "delete_artifact",
    type: "function",
    info: "Remove a named artifact from kernel memory.",
    detail: 'flowfile.delete_artifact("name")',
    apply: 'delete_artifact("name")',
  },
  {
    label: "list_artifacts",
    type: "function",
    info: "List all artifacts available in the kernel.",
    detail: "flowfile.list_artifacts() -> list[ArtifactInfo]",
    apply: "list_artifacts()",
  },
  {
    label: "publish_global",
    type: "function",
    info: "Persist a Python object to the global artifact store (survives across sessions).",
    detail: 'flowfile.publish_global("name", obj, description?, tags?, namespace_id?, fmt?)',
    apply: 'publish_global("name", obj)',
  },
  {
    label: "get_global",
    type: "function",
    info: "Retrieve a Python object from the global artifact store.",
    detail: 'flowfile.get_global("name", version?, namespace_id?)',
    apply: 'get_global("name")',
  },
  {
    label: "list_global_artifacts",
    type: "function",
    info: "List available global artifacts with optional namespace/tag filters.",
    detail: "flowfile.list_global_artifacts(namespace_id?, tags?) -> list[GlobalArtifactInfo]",
    apply: "list_global_artifacts()",
  },
  {
    label: "get_shared_location",
    type: "function",
    info: "Get the shared location to make objects available to other processes.",
    detail: "flowfile.get_shared_location() -> str",
    apply: "get_shared_location()",
  },
  {
    label: "delete_global_artifact",
    type: "function",
    info: "Delete a global artifact by name, optionally a specific version.",
    detail: 'flowfile.delete_global_artifact("name", version?, namespace_id?)',
    apply: 'delete_global_artifact("name")',
  },
  {
    label: "log",
    type: "function",
    info: "Send a log message to the FlowFile log viewer.",
    detail: 'flowfile.log("message", level?)',
    apply: 'log("message")',
  },
  {
    label: "log_info",
    type: "function",
    info: "Send an INFO log message to the FlowFile log viewer.",
    detail: 'flowfile.log_info("message")',
    apply: 'log_info("message")',
  },
  {
    label: "log_warning",
    type: "function",
    info: "Send a WARNING log message to the FlowFile log viewer.",
    detail: 'flowfile.log_warning("message")',
    apply: 'log_warning("message")',
  },
  {
    label: "log_error",
    type: "function",
    info: "Send an ERROR log message to the FlowFile log viewer.",
    detail: 'flowfile.log_error("message")',
    apply: 'log_error("message")',
  },
];

// Polars module-level (after `pl.`)
const POLARS_MODULE_ENTRIES: Completion[] = [
  { label: "col", type: "function", info: "Reference a column by name", apply: 'col("")' },
  { label: "lit", type: "function", info: "Literal value", apply: "lit()" },
  { label: "when", type: "function", info: "Conditional expression: when(...).then(...).otherwise(...)", apply: "when()" },
  { label: "concat", type: "function", info: "Concatenate DataFrames/LazyFrames", apply: "concat()" },
  { label: "struct", type: "function", info: "Create a struct expression", apply: "struct()" },
  { label: "format", type: "function", info: "String format expression", apply: "format()" },
  { label: "sum_horizontal", type: "function", apply: "sum_horizontal()" },
  { label: "min_horizontal", type: "function", apply: "min_horizontal()" },
  { label: "max_horizontal", type: "function", apply: "max_horizontal()" },
  { label: "mean_horizontal", type: "function", apply: "mean_horizontal()" },
  { label: "coalesce", type: "function", apply: "coalesce()" },
  { label: "date_range", type: "function", apply: "date_range()" },
  { label: "datetime_range", type: "function", apply: "datetime_range()" },
  { label: "from_dict", type: "function", apply: "from_dict()" },
  { label: "from_records", type: "function", apply: "from_records()" },
  { label: "from_dicts", type: "function", apply: "from_dicts()" },
  { label: "from_pandas", type: "function", apply: "from_pandas()" },
  { label: "read_csv", type: "function", apply: 'read_csv("")' },
  { label: "read_parquet", type: "function", apply: 'read_parquet("")' },
  { label: "read_json", type: "function", apply: 'read_json("")' },
  { label: "read_ndjson", type: "function", apply: 'read_ndjson("")' },
  { label: "read_excel", type: "function", apply: 'read_excel("")' },
  { label: "read_database", type: "function", apply: "read_database()" },
  { label: "scan_csv", type: "function", apply: 'scan_csv("")' },
  { label: "scan_parquet", type: "function", apply: 'scan_parquet("")' },
  { label: "scan_ndjson", type: "function", apply: 'scan_ndjson("")' },
  { label: "DataFrame", type: "class" },
  { label: "LazyFrame", type: "class" },
  { label: "Series", type: "class" },
  { label: "Expr", type: "class" },
  { label: "Datetime", type: "class" },
  { label: "Date", type: "class" },
  { label: "Duration", type: "class" },
  { label: "Time", type: "class" },
  { label: "Int8", type: "class" },
  { label: "Int16", type: "class" },
  { label: "Int32", type: "class" },
  { label: "Int64", type: "class" },
  { label: "UInt8", type: "class" },
  { label: "UInt16", type: "class" },
  { label: "UInt32", type: "class" },
  { label: "UInt64", type: "class" },
  { label: "Float32", type: "class" },
  { label: "Float64", type: "class" },
  { label: "Utf8", type: "class" },
  { label: "String", type: "class" },
  { label: "Boolean", type: "class" },
  { label: "Object", type: "class" },
  { label: "List", type: "class" },
  { label: "Struct", type: "class" },
  { label: "Categorical", type: "class" },
];

// Common Polars Expr / DataFrame / LazyFrame methods (after any `.`)
const POLARS_METHOD_ENTRIES: Completion[] = [
  // DataFrame / LazyFrame
  { label: "select", type: "method", apply: "select()" },
  { label: "filter", type: "method", apply: "filter()" },
  { label: "with_columns", type: "method", apply: "with_columns()" },
  { label: "with_row_count", type: "method", apply: "with_row_count()" },
  { label: "drop", type: "method", apply: "drop()" },
  { label: "rename", type: "method", apply: "rename()" },
  { label: "sort", type: "method", apply: "sort()" },
  { label: "group_by", type: "method", apply: "group_by()" },
  { label: "agg", type: "method", apply: "agg()" },
  { label: "join", type: "method", apply: "join()" },
  { label: "join_asof", type: "method", apply: "join_asof()" },
  { label: "head", type: "method", apply: "head()" },
  { label: "tail", type: "method", apply: "tail()" },
  { label: "sample", type: "method", apply: "sample()" },
  { label: "slice", type: "method", apply: "slice()" },
  { label: "limit", type: "method", apply: "limit()" },
  { label: "unique", type: "method", apply: "unique()" },
  { label: "drop_nulls", type: "method", apply: "drop_nulls()" },
  { label: "fill_null", type: "method", apply: "fill_null()" },
  { label: "fill_nan", type: "method", apply: "fill_nan()" },
  { label: "collect", type: "method", apply: "collect()" },
  { label: "lazy", type: "method", apply: "lazy()" },
  { label: "clone", type: "method", apply: "clone()" },
  { label: "explode", type: "method", apply: "explode()" },
  { label: "melt", type: "method", apply: "melt()" },
  { label: "pivot", type: "method", apply: "pivot()" },
  { label: "unpivot", type: "method", apply: "unpivot()" },
  { label: "transpose", type: "method", apply: "transpose()" },
  { label: "hstack", type: "method", apply: "hstack()" },
  { label: "vstack", type: "method", apply: "vstack()" },
  { label: "partition_by", type: "method", apply: "partition_by()" },
  { label: "rolling", type: "method", apply: "rolling()" },
  { label: "describe", type: "method", apply: "describe()" },
  { label: "schema", type: "property" },
  { label: "columns", type: "property" },
  { label: "dtypes", type: "property" },
  { label: "shape", type: "property" },
  { label: "height", type: "property" },
  { label: "width", type: "property" },
  { label: "is_empty", type: "method", apply: "is_empty()" },
  { label: "write_csv", type: "method", apply: 'write_csv("")' },
  { label: "write_parquet", type: "method", apply: 'write_parquet("")' },
  { label: "write_json", type: "method", apply: 'write_json("")' },
  { label: "write_excel", type: "method", apply: 'write_excel("")' },
  { label: "write_database", type: "method", apply: "write_database()" },
  { label: "to_pandas", type: "method", apply: "to_pandas()" },
  { label: "to_dict", type: "method", apply: "to_dict()" },
  { label: "to_numpy", type: "method", apply: "to_numpy()" },
  { label: "to_arrow", type: "method", apply: "to_arrow()" },

  // Expr
  { label: "alias", type: "method", apply: 'alias("")' },
  { label: "cast", type: "method", apply: "cast()" },
  { label: "is_null", type: "method", apply: "is_null()" },
  { label: "is_not_null", type: "method", apply: "is_not_null()" },
  { label: "is_nan", type: "method", apply: "is_nan()" },
  { label: "is_not_nan", type: "method", apply: "is_not_nan()" },
  { label: "is_in", type: "method", apply: "is_in()" },
  { label: "is_unique", type: "method", apply: "is_unique()" },
  { label: "is_duplicated", type: "method", apply: "is_duplicated()" },
  { label: "is_first_distinct", type: "method", apply: "is_first_distinct()" },
  { label: "is_last_distinct", type: "method", apply: "is_last_distinct()" },
  { label: "abs", type: "method", apply: "abs()" },
  { label: "ceil", type: "method", apply: "ceil()" },
  { label: "floor", type: "method", apply: "floor()" },
  { label: "round", type: "method", apply: "round()" },
  { label: "sign", type: "method", apply: "sign()" },
  { label: "exp", type: "method", apply: "exp()" },
  { label: "log", type: "method", apply: "log()" },
  { label: "sqrt", type: "method", apply: "sqrt()" },
  { label: "sum", type: "method", apply: "sum()" },
  { label: "mean", type: "method", apply: "mean()" },
  { label: "median", type: "method", apply: "median()" },
  { label: "min", type: "method", apply: "min()" },
  { label: "max", type: "method", apply: "max()" },
  { label: "std", type: "method", apply: "std()" },
  { label: "var", type: "method", apply: "var()" },
  { label: "count", type: "method", apply: "count()" },
  { label: "n_unique", type: "method", apply: "n_unique()" },
  { label: "and_", type: "method", apply: "and_()" },
  { label: "or_", type: "method", apply: "or_()" },
  { label: "not_", type: "method", apply: "not_()" },
  { label: "when", type: "method", apply: "when()" },
  { label: "then", type: "method", apply: "then()" },
  { label: "otherwise", type: "method", apply: "otherwise()" },
  { label: "over", type: "method", apply: "over()" },
  { label: "rank", type: "method", apply: "rank()" },
  { label: "cum_sum", type: "method", apply: "cum_sum()" },
  { label: "cum_min", type: "method", apply: "cum_min()" },
  { label: "cum_max", type: "method", apply: "cum_max()" },
  { label: "cum_prod", type: "method", apply: "cum_prod()" },
  { label: "shift", type: "method", apply: "shift()" },
  { label: "diff", type: "method", apply: "diff()" },
  { label: "pct_change", type: "method", apply: "pct_change()" },
  { label: "rolling_sum", type: "method", apply: "rolling_sum()" },
  { label: "rolling_mean", type: "method", apply: "rolling_mean()" },
  { label: "rolling_min", type: "method", apply: "rolling_min()" },
  { label: "rolling_max", type: "method", apply: "rolling_max()" },
  { label: "map_elements", type: "method", apply: "map_elements()" },
  { label: "map_batches", type: "method", apply: "map_batches()" },
  { label: "replace", type: "method", apply: "replace()" },
  { label: "value_counts", type: "method", apply: "value_counts()" },
  { label: "top_k", type: "method", apply: "top_k()" },
  { label: "bottom_k", type: "method", apply: "bottom_k()" },

  // Accessor namespaces (no parens)
  { label: "str", type: "namespace", info: "String operations (.contains, .split, .replace, ...)" },
  { label: "dt", type: "namespace", info: "Datetime operations (.year, .month, .strftime, ...)" },
  { label: "list", type: "namespace", info: "List operations (.get, .len, .sum, ...)" },
  { label: "struct", type: "namespace", info: "Struct operations (.field, .unnest, ...)" },
  { label: "cat", type: "namespace", info: "Categorical operations" },
];

// ─── Scoped completion sources ────────────────────────────────────────────────

/**
 * Completions after `flowfile.` — Flowfile API functions.
 */
export const flowfileApiCompletions: CompletionSource = (context) => {
  const match = context.matchBefore(/flowfile\.\w*/);
  if (!match) return null;
  // Only fire if there's actually a dot in the match
  if (!match.text.includes(".")) return null;
  const dotPos = match.from + match.text.indexOf(".") + 1;
  return {
    from: dotPos,
    options: FLOWFILE_API_ENTRIES,
    validFor: /^\w*$/,
  };
};

/**
 * Completions after `pl.` — Polars module exports.
 */
export const polarsModuleCompletions: CompletionSource = (context) => {
  const match = context.matchBefore(/\bpl\.\w*/);
  if (!match) return null;
  if (!match.text.includes(".")) return null;
  const dotPos = match.from + match.text.indexOf(".") + 1;
  return {
    from: dotPos,
    options: POLARS_MODULE_ENTRIES,
    validFor: /^\w*$/,
  };
};

/**
 * Completions after any `.` — common Polars Expr / DataFrame / LazyFrame methods.
 * Skipped when the preceding identifier is `flowfile` or `pl` (those have dedicated sources).
 */
export const polarsExprCompletions: CompletionSource = (context) => {
  const match = context.matchBefore(/\.\w*/);
  if (!match) return null;

  // Skip if preceded by `flowfile` or `pl` — those have dedicated, more specific sources
  const lookback = context.state.doc.sliceString(Math.max(0, match.from - 10), match.from);
  if (/\bflowfile$/.test(lookback) || /\bpl$/.test(lookback)) return null;

  return {
    from: match.from + 1,
    options: POLARS_METHOD_ENTRIES,
    validFor: /^\w*$/,
  };
};

/**
 * Build a completion source that suggests connected input names inside `read_input("...")`.
 * Accepts a getter so the source picks up the latest names without rebuilding the editor.
 */
export function createNamedInputCompletions(getInputNames: () => string[]): CompletionSource {
  return (context) => {
    const inputNames = getInputNames();
    if (inputNames.length === 0) return null;
    const match = context.matchBefore(/read_input\(\s*"[^"]*/);
    if (!match) return null;
    const quoteIdx = match.text.lastIndexOf('"');
    const partialStart = match.from + quoteIdx + 1;
    return {
      from: partialStart,
      options: inputNames.map((name) => ({
        label: name,
        type: "text",
        detail: "input",
        info: "Connected input name",
        boost: 10,
      })),
      validFor: /^[^"]*$/,
    };
  };
}

/**
 * Build a completion source that suggests upstream column names inside `col("...")`
 * or `pl.col("...")`. Each column carries its data type as `detail` and its source
 * input as `info`. Accepts a getter so the source picks up newly-fetched schemas.
 */
export function createUpstreamColumnCompletions(
  getColumns: () => UpstreamColumn[],
): CompletionSource {
  return (context) => {
    const columns = getColumns();
    if (columns.length === 0) return null;
    const match = context.matchBefore(/(?:pl\.)?col\(\s*"[^"]*/);
    if (!match) return null;
    const quoteIdx = match.text.lastIndexOf('"');
    const partialStart = match.from + quoteIdx + 1;

    // Dedupe by column name; if multiple inputs share a column name keep the first
    const seen = new Map<string, UpstreamColumn>();
    for (const col of columns) {
      if (!seen.has(col.name)) seen.set(col.name, col);
    }

    return {
      from: partialStart,
      options: Array.from(seen.values()).map((col) => ({
        label: col.name,
        type: "property",
        detail: col.data_type,
        info: `Column from "${col.source_input}"`,
        boost: 5,
      })),
      validFor: /^[^"]*$/,
    };
  };
}

// ─── Scope-aware variable extraction ─────────────────────────────────────────

type ScopeSymbol = { name: string; type: string };

function extractSymbols(code: string): ScopeSymbol[] {
  const symbols = new Map<string, string>();
  // Strip comments to reduce false positives in regex matches
  const stripped = code.replace(/#.*$/gm, "");

  const ASSIGN_RE = /^\s*([A-Za-z_]\w*)\s*=(?!=)/gm;
  const DEF_RE = /^\s*def\s+([A-Za-z_]\w*)/gm;
  const CLASS_RE = /^\s*class\s+([A-Za-z_]\w*)/gm;
  const IMPORT_RE = /^\s*import\s+([A-Za-z_][\w.]*(?:\s+as\s+[A-Za-z_]\w*)?(?:\s*,\s*[A-Za-z_][\w.]*(?:\s+as\s+[A-Za-z_]\w*)?)*)/gm;
  const FROM_IMPORT_RE = /^\s*from\s+\S+\s+import\s+([^\n]+)/gm;

  let m: RegExpExecArray | null;
  while ((m = ASSIGN_RE.exec(stripped)) !== null) {
    symbols.set(m[1], "variable");
  }
  while ((m = DEF_RE.exec(stripped)) !== null) {
    symbols.set(m[1], "function");
  }
  while ((m = CLASS_RE.exec(stripped)) !== null) {
    symbols.set(m[1], "class");
  }
  while ((m = IMPORT_RE.exec(stripped)) !== null) {
    for (const part of m[1].split(/\s*,\s*/)) {
      const asMatch = part.match(/\s+as\s+([A-Za-z_]\w*)$/);
      const name = asMatch ? asMatch[1] : part.split(".")[0].trim();
      if (name) symbols.set(name, "namespace");
    }
  }
  while ((m = FROM_IMPORT_RE.exec(stripped)) !== null) {
    // Strip parens (allowed in `from x import (a, b)`)
    const inner = m[1].replace(/[()]/g, "");
    for (const part of inner.split(/\s*,\s*/)) {
      const trimmed = part.trim();
      if (!trimmed || trimmed === "*") continue;
      const asMatch = trimmed.match(/^(\S+)\s+as\s+([A-Za-z_]\w*)$/);
      const name = asMatch ? asMatch[2] : trimmed;
      if (/^[A-Za-z_]\w*$/.test(name)) symbols.set(name, "variable");
    }
  }

  return Array.from(symbols, ([name, type]) => ({ name, type }));
}

/**
 * Build a completion source that suggests identifiers defined in earlier cells
 * (top-level assignments, defs, classes, imports). Cheap regex-based parse.
 * Accepts a getter so it re-parses on each completion request rather than at
 * extension-creation time.
 */
export function createScopeCompletions(getPriorCellCodes: () => string[]): CompletionSource {
  return (context) => {
    const codes = getPriorCellCodes();
    if (codes.length === 0) return null;
    const symbols = extractSymbols(codes.join("\n"));
    if (symbols.length === 0) return null;
    const match = context.matchBefore(/[A-Za-z_]\w*/);
    if (!match || (match.from === match.to && !context.explicit)) return null;
    return {
      from: match.from,
      options: symbols.map((s) => ({
        label: s.name,
        type: s.type,
        info: "Defined in earlier cell",
      })),
      validFor: /^\w*$/,
    };
  };
}
