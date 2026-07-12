/**
 * Canonical CodeMirror completions for the kernel-runtime `flowfile_ctx` global.
 *
 * `flowfile_ctx` is injected into the exec namespace only for code that runs on
 * an isolated kernel — the Python Script node's notebook cells and custom-node
 * `process()` bodies whose execution environment is "kernel". The entries below
 * mirror `kernel_runtime/kernel_runtime/flowfile_client.py` (the source of truth
 * for the API). Both editors import from here so the two never drift apart.
 */
import type { Completion, CompletionSource } from "@codemirror/autocomplete";

// ─── flowfile_ctx API (after `flowfile_ctx.` / legacy `flowfile.`) ───────────

export const FLOWFILE_API_ENTRIES: Completion[] = [
  {
    label: "read_input",
    type: "function",
    info: "Read input DataFrame. Optional name parameter for named inputs.",
    detail: "flowfile_ctx.read_input(name?)",
    apply: "read_input()",
  },
  {
    label: "read_first",
    type: "function",
    info: "Read only the first input file for a name (skips concatenation).",
    detail: "flowfile_ctx.read_first(name?) -> LazyFrame",
    apply: "read_first()",
  },
  {
    label: "read_inputs",
    type: "function",
    info: "Read all inputs as a dict of LazyFrame lists (one per connection).",
    detail: "flowfile_ctx.read_inputs() -> dict[str, list[LazyFrame]]",
    apply: "read_inputs()",
  },
  {
    label: "publish_output",
    type: "function",
    info: "Write output DataFrame. Optional name parameter for named outputs.",
    detail: "flowfile_ctx.publish_output(df, name?)",
    apply: "publish_output(df)",
  },
  {
    label: "display",
    type: "function",
    info: "Display a rich object inline. Polars DataFrames/LazyFrames render as an interactive table; also matplotlib/plotly figures, PIL images, and HTML strings.",
    detail: "flowfile_ctx.display(obj, title?)",
    apply: "display(obj)",
  },
  {
    label: "explore",
    type: "function",
    info: "Open the full Graphic Walker explorer (data grid + drag-to-chart visualization) for a Polars DataFrame/LazyFrame.",
    detail: "flowfile_ctx.explore(obj, title?)",
    apply: "explore(obj)",
  },
  {
    label: "publish_artifact",
    type: "function",
    info: "Store a Python object as a named artifact in kernel memory.",
    detail: 'flowfile_ctx.publish_artifact("name", obj)',
    apply: 'publish_artifact("name", obj)',
  },
  {
    label: "read_artifact",
    type: "function",
    info: "Retrieve a Python object from a named artifact.",
    detail: 'flowfile_ctx.read_artifact("name")',
    apply: 'read_artifact("name")',
  },
  {
    label: "delete_artifact",
    type: "function",
    info: "Remove a named artifact from kernel memory.",
    detail: 'flowfile_ctx.delete_artifact("name")',
    apply: 'delete_artifact("name")',
  },
  {
    label: "list_artifacts",
    type: "function",
    info: "List all artifacts available in the kernel.",
    detail: "flowfile_ctx.list_artifacts() -> list[ArtifactInfo]",
    apply: "list_artifacts()",
  },
  {
    label: "publish_global",
    type: "function",
    info: "Persist a Python object to the global artifact store (survives across sessions).",
    detail: 'flowfile_ctx.publish_global("name", obj, description?, tags?, namespace_id?, fmt?)',
    apply: 'publish_global("name", obj)',
  },
  {
    label: "get_global",
    type: "function",
    info: "Retrieve a Python object from the global artifact store.",
    detail: 'flowfile_ctx.get_global("name", version?, namespace_id?)',
    apply: 'get_global("name")',
  },
  {
    label: "list_global_artifacts",
    type: "function",
    info: "List available global artifacts with optional namespace/tag filters.",
    detail: "flowfile_ctx.list_global_artifacts(namespace_id?, tags?) -> list[GlobalArtifactInfo]",
    apply: "list_global_artifacts()",
  },
  {
    label: "get_shared_location",
    type: "function",
    info: "Get the shared location to make objects available to other processes.",
    detail: "flowfile_ctx.get_shared_location() -> str",
    apply: "get_shared_location()",
  },
  {
    label: "delete_global_artifact",
    type: "function",
    info: "Delete a global artifact by name, optionally a specific version.",
    detail: 'flowfile_ctx.delete_global_artifact("name", version?, namespace_id?)',
    apply: 'delete_global_artifact("name")',
  },
  {
    label: "log",
    type: "function",
    info: "Send a log message to the FlowFile log viewer.",
    detail: 'flowfile_ctx.log("message", level?)',
    apply: 'log("message")',
  },
  {
    label: "log_info",
    type: "function",
    info: "Send an INFO log message to the FlowFile log viewer.",
    detail: 'flowfile_ctx.log_info("message")',
    apply: 'log_info("message")',
  },
  {
    label: "log_warning",
    type: "function",
    info: "Send a WARNING log message to the FlowFile log viewer.",
    detail: 'flowfile_ctx.log_warning("message")',
    apply: 'log_warning("message")',
  },
  {
    label: "log_error",
    type: "function",
    info: "Send an ERROR log message to the FlowFile log viewer.",
    detail: 'flowfile_ctx.log_error("message")',
    apply: 'log_error("message")',
  },
  {
    label: "read_catalog_table",
    type: "function",
    info: "Read a catalog table as a lazy Polars LazyFrame. Pass schema=/namespace_id= to disambiguate; delta_version= for time travel.",
    detail: 'flowfile_ctx.read_catalog_table("name", schema?, namespace_id?, delta_version?)',
    apply: 'read_catalog_table("name")',
  },
  {
    label: "write_catalog_table",
    type: "function",
    info: 'Write a Polars DataFrame/LazyFrame to a catalog table. write_mode: "overwrite" | "append" | "upsert" | "update" | "delete" | "error". Merge modes require merge_keys=.',
    detail: 'flowfile_ctx.write_catalog_table(df, "name", schema?, write_mode?, merge_keys?)',
    apply: 'write_catalog_table(df, "name")',
  },
  {
    label: "list_catalog_tables",
    type: "function",
    info: "List catalog tables available to the kernel as TableRef objects. Optionally filter by schema= or namespace_id=.",
    detail: "flowfile_ctx.list_catalog_tables(schema?, namespace_id?) -> list[TableRef]",
    apply: "list_catalog_tables()",
  },
  {
    label: "list_catalogs",
    type: "function",
    info: "List top-level catalog namespaces as CatalogRef objects. Navigate further with .get_schema() / .list_schemas() / .get_table_ref().",
    detail: "flowfile_ctx.list_catalogs() -> list[CatalogRef]",
    apply: "list_catalogs()",
  },
  {
    label: "list_schemas",
    type: "function",
    info: "List schemas as SchemaRef objects, optionally filtered by catalog= or catalog_id=.",
    detail: "flowfile_ctx.list_schemas(catalog?, catalog_id?) -> list[SchemaRef]",
    apply: "list_schemas()",
  },
  {
    label: "get_catalog",
    type: "function",
    info: "Return a CatalogRef for the named top-level catalog. Raises LookupError if missing.",
    detail: 'flowfile_ctx.get_catalog("name") -> CatalogRef',
    apply: 'get_catalog("")',
  },
  {
    label: "default_schema",
    type: "function",
    info: "Return the seeded default schema (General/default) as a SchemaRef.",
    detail: "flowfile_ctx.default_schema() -> SchemaRef",
    apply: "default_schema()",
  },
  {
    label: "CatalogRef",
    type: "class",
    info: "Typed handle to a top-level catalog. Methods: get_schema, list_schemas, list_tables, get_table_ref(schema_name=, table_name=).",
    detail: "flowfile_ctx.CatalogRef",
  },
  {
    label: "SchemaRef",
    type: "class",
    info: "Typed handle to a schema. Methods: get_table_ref, list_tables, read_table, write_table.",
    detail: "flowfile_ctx.SchemaRef",
  },
  {
    label: "TableRef",
    type: "class",
    info: "Typed handle to a catalog table. Methods: read, write, exists, refresh.",
    detail: "flowfile_ctx.TableRef",
  },
];

// ─── Catalog ref method entries ──────────────────────────────────────────────

const CATALOG_REF_METHODS: Completion[] = [
  {
    label: "get_schema",
    type: "method",
    info: "Return the named child schema as a SchemaRef. Raises LookupError if missing.",
    detail: "CatalogRef.get_schema(name) -> SchemaRef",
    apply: 'get_schema("")',
  },
  {
    label: "list_schemas",
    type: "method",
    info: "All schemas (level-1 namespaces) under this catalog.",
    detail: "CatalogRef.list_schemas() -> list[SchemaRef]",
    apply: "list_schemas()",
  },
  {
    label: "list_tables",
    type: "method",
    info: "All tables across every schema in this catalog (flat list).",
    detail: "CatalogRef.list_tables() -> list[TableRef]",
    apply: "list_tables()",
  },
  {
    label: "get_table_ref",
    type: "method",
    info: "Shortcut for self.get_schema(schema_name).get_table_ref(table_name).",
    detail: "CatalogRef.get_table_ref(schema_name=, table_name=) -> TableRef",
    apply: 'get_table_ref(schema_name="", table_name="")',
  },
  { label: "id", type: "property", info: "Catalog namespace id." },
  { label: "name", type: "property", info: "Catalog name as stored in Core." },
];

const SCHEMA_REF_METHODS: Completion[] = [
  {
    label: "get_table_ref",
    type: "method",
    info: "Get a TableRef for the named table. Returns a lazy ref (id=None) if the table doesn't exist yet.",
    detail: "SchemaRef.get_table_ref(name) -> TableRef",
    apply: 'get_table_ref("")',
  },
  {
    label: "list_tables",
    type: "method",
    info: "All tables registered in this schema.",
    detail: "SchemaRef.list_tables() -> list[TableRef]",
    apply: "list_tables()",
  },
  {
    label: "read_table",
    type: "method",
    info: "Read a table from this schema as a Polars LazyFrame.",
    detail: "SchemaRef.read_table(name, delta_version?) -> LazyFrame",
    apply: 'read_table("")',
  },
  {
    label: "write_table",
    type: "method",
    info: 'Write a DataFrame/LazyFrame into this schema. write_mode: "overwrite"|"append"|"upsert"|"update"|"delete"|"error".',
    detail: "SchemaRef.write_table(df, name, write_mode=, merge_keys=, description=) -> TableRef",
    apply: 'write_table(df, "")',
  },
  {
    label: "read_catalog_table",
    type: "method",
    info: "Alias for read_table — name mirrors flowfile_ctx.read_catalog_table.",
    detail: "SchemaRef.read_catalog_table(name, delta_version?) -> LazyFrame",
    apply: 'read_catalog_table("")',
  },
  {
    label: "write_catalog_table",
    type: "method",
    info: "Alias for write_table — name mirrors flowfile_ctx.write_catalog_table.",
    detail:
      "SchemaRef.write_catalog_table(df, name, write_mode=, merge_keys=, description=) -> TableRef",
    apply: 'write_catalog_table(df, "")',
  },
  {
    label: "publish_artifact",
    type: "method",
    info: "Persist a Python object to the global artifact store under this schema. Requires a registered catalog flow.",
    detail: "SchemaRef.publish_artifact(name, obj, description=, tags=, fmt=) -> int",
    apply: 'publish_artifact("", obj)',
  },
  {
    label: "read_artifact",
    type: "method",
    info: "Retrieve an artifact from this schema's namespace.",
    detail: "SchemaRef.read_artifact(name, version?) -> Any",
    apply: 'read_artifact("")',
  },
  {
    label: "list_artifacts",
    type: "method",
    info: "List artifacts in this schema's namespace.",
    detail: "SchemaRef.list_artifacts(tags?) -> list[GlobalArtifactInfo]",
    apply: "list_artifacts()",
  },
  {
    label: "delete_artifact",
    type: "method",
    info: "Delete an artifact from this schema's namespace.",
    detail: "SchemaRef.delete_artifact(name, version?)",
    apply: 'delete_artifact("")',
  },
  { label: "id", type: "property", info: "Schema namespace id." },
  { label: "name", type: "property", info: "Schema name." },
  { label: "catalog", type: "property", info: "Parent CatalogRef." },
];

const TABLE_REF_METHODS: Completion[] = [
  {
    label: "read",
    type: "method",
    info: "Read this table as a Polars LazyFrame.",
    detail: "TableRef.read(delta_version?) -> LazyFrame",
    apply: "read()",
  },
  {
    label: "write",
    type: "method",
    info: 'Write df into this table. Creates it if it doesn\'t exist yet. write_mode: "overwrite"|"append"|"upsert"|"update"|"delete"|"error".',
    detail: "TableRef.write(df, write_mode=, merge_keys=, description=) -> TableRef",
    apply: "write(df)",
  },
  {
    label: "exists",
    type: "method",
    info: "True if this ref points at an existing catalog table.",
    detail: "TableRef.exists() -> bool",
    apply: "exists()",
  },
  {
    label: "refresh",
    type: "method",
    info: "Return a fresh TableRef with re-fetched metadata from Core.",
    detail: "TableRef.refresh() -> TableRef",
    apply: "refresh()",
  },
  { label: "name", type: "property", info: "Table name." },
  { label: "schema", type: "property", info: "Parent SchemaRef." },
  { label: "id", type: "property", info: "Catalog table id (None until the table is created)." },
  { label: "file_path", type: "property", info: "Absolute path of the Delta directory on disk." },
  { label: "row_count", type: "property", info: "Last-recorded row count." },
  { label: "column_count", type: "property", info: "Last-recorded column count." },
  { label: "size_bytes", type: "property", info: "Last-recorded size on disk." },
];

// Regex fragments shared by the catalog chain-completion source.
// ``[^()]*`` is intentionally simple — handles the common case of a single
// string/kwarg argument. Nested parens (`get_table_ref(name=foo("x"))`) won't
// match; users with that shape will fall through to the Polars completions.
export const RE_CATALOG_CALL = /\.get_catalog\s*\([^()]*\)\s*$/;
export const RE_SCHEMA_CALL = /\.(?:get_schema|default_schema)\s*\([^()]*\)\s*$/;
export const RE_TABLE_CALL = /\.get_table_ref\s*\([^()]*\)\s*$/;

// Variable-assignment regexes anchored to the final ref-producing method.
// Greedy `.+` covers any base expression — `cat = flowfile_ctx.get_catalog("x")`
// AND `tref = schema.get_table_ref("t")` both match the table-assignment rule
// because the final `.method(...)` segment is what determines the resulting
// ref's type. Multi-line bases aren't supported (the `m` flag anchors `^`/`$`
// to line boundaries).
const RE_ASSIGN_CATALOG = /^\s*([A-Za-z_]\w*)\s*=\s*.+\.get_catalog\s*\([^()]*\)\s*$/gm;
const RE_ASSIGN_SCHEMA =
  /^\s*([A-Za-z_]\w*)\s*=\s*.+\.(?:get_schema|default_schema)\s*\([^()]*\)\s*$/gm;
const RE_ASSIGN_TABLE = /^\s*([A-Za-z_]\w*)\s*=\s*.+\.get_table_ref\s*\([^()]*\)\s*$/gm;

export type RefKind = "catalog" | "schema" | "table";

const REF_METHODS_BY_KIND: Record<RefKind, Completion[]> = {
  catalog: CATALOG_REF_METHODS,
  schema: SCHEMA_REF_METHODS,
  table: TABLE_REF_METHODS,
};

/**
 * Scan code for `name = <expr>.get_catalog|get_schema|default_schema|get_table_ref(...)`
 * lines and return a map of varname → ref kind. Re-built on each completion
 * request because the edited code changes frequently.
 */
export function buildRefVarMap(code: string): Map<string, RefKind> {
  const map = new Map<string, RefKind>();
  for (const m of code.matchAll(RE_ASSIGN_CATALOG)) map.set(m[1], "catalog");
  for (const m of code.matchAll(RE_ASSIGN_SCHEMA)) map.set(m[1], "schema");
  for (const m of code.matchAll(RE_ASSIGN_TABLE)) map.set(m[1], "table");
  return map;
}

// ─── Kernel-global bare-name entries ─────────────────────────────────────────

/**
 * The kernel-only globals every kernel-executed cell / process() body can use.
 * `pl` is deliberately excluded — editors that also want it (the notebook)
 * append it via KERNEL_GLOBAL_ENTRIES; editors with their own Polars completions
 * (the node designer) use just these two.
 */
export const FLOWFILE_CTX_GLOBAL_ENTRIES: Completion[] = [
  {
    label: "flowfile_ctx",
    type: "namespace",
    info: "Kernel-runtime context. Provides read_input/publish_output, display, log, artifacts, catalog tables — type `flowfile_ctx.` to see methods.",
    detail: "kernel global",
  },
  {
    label: "flowfile",
    type: "namespace",
    info: "Deprecated alias for `flowfile_ctx`. Still works but emits a DeprecationWarning on first attribute access.",
    detail: "kernel global (deprecated → flowfile_ctx)",
  },
];

/**
 * Globals injected into the kernel exec namespace. Suggested as bare-name
 * completions so typing `flowfile_ct…` offers `flowfile_ctx` before the user
 * has typed the dot. `flowfile` is also suggested but flagged as deprecated;
 * `pl` is included for editors that don't have their own Polars completions.
 */
const KERNEL_GLOBAL_ENTRIES: Completion[] = [
  ...FLOWFILE_CTX_GLOBAL_ENTRIES,
  {
    label: "pl",
    type: "namespace",
    info: "Polars module (when imported as `import polars as pl`). Type `pl.` to see DataFrame/LazyFrame/Series constructors and read_* / scan_* helpers.",
    detail: "polars",
  },
];

// ─── Completion sources ──────────────────────────────────────────────────────

/**
 * Completions for bare global identifiers — fires when the user is typing a
 * word that is NOT preceded by a dot (so it doesn't interfere with the
 * scoped member-access completions below). Suggests the kernel globals
 * (`flowfile_ctx`, `flowfile`, `pl`).
 */
export const globalIdentifierCompletions: CompletionSource = (context) => {
  const match = context.matchBefore(/\w+/);
  if (!match) return null;
  // Skip if preceded by a dot — that's member access, handled elsewhere.
  if (match.from > 0) {
    const prev = context.state.doc.sliceString(match.from - 1, match.from);
    if (prev === ".") return null;
  }
  // Don't fire on explicit-trigger-only contexts (avoid stomping the language's
  // builtin keyword/identifier completions when the user is just typing).
  if (!context.explicit && match.text.length < 2) return null;
  return {
    from: match.from,
    options: KERNEL_GLOBAL_ENTRIES,
    validFor: /^\w*$/,
  };
};

/**
 * Completions after `flowfile_ctx.` (canonical) or `flowfile.` (legacy alias) —
 * Flowfile kernel-runtime API functions. Suggestion `detail` strings always
 * show the canonical `flowfile_ctx.` form to nudge migration.
 */
export const flowfileApiCompletions: CompletionSource = (context) => {
  const match = context.matchBefore(/(?:flowfile_ctx|flowfile)\.\w*/);
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
 * Completions after a `.` that follows a catalog-ref-producing call —
 * `…get_catalog(…).`, `…get_schema(…).`, `…default_schema().`,
 * `…get_table_ref(…).`. Suggests the corresponding ref's methods so chained
 * calls (`flowfile_ctx.get_catalog("X").get_schema("Y").read_table(...)`)
 * autocomplete the way users expect even though CodeMirror doesn't do
 * type inference.
 *
 * Limitation: detection is purely textual with a ~200-char lookback. Chains
 * that span more than that, or that nest parentheses inside the arg list,
 * won't be recognised and fall back to the generic Polars completions.
 */
export const catalogRefChainCompletions: CompletionSource = (context) => {
  const match = context.matchBefore(/\.\w*/);
  if (!match) return null;
  const lookback = context.state.doc.sliceString(Math.max(0, match.from - 200), match.from);
  let options: Completion[] | null = null;
  if (RE_TABLE_CALL.test(lookback)) {
    options = TABLE_REF_METHODS;
  } else if (RE_SCHEMA_CALL.test(lookback)) {
    options = SCHEMA_REF_METHODS;
  } else if (RE_CATALOG_CALL.test(lookback)) {
    options = CATALOG_REF_METHODS;
  }
  if (!options) return null;
  return {
    from: match.from + 1,
    options,
    validFor: /^\w*$/,
  };
};

/**
 * Completions after `<varname>.` where `<varname>` is locally assigned to a
 * catalog/schema/table ref. Scans the current doc (plus any extra code the
 * getter provides, e.g. prior notebook cells) for assignment patterns like
 * `schema = flowfile_ctx.default_schema()` so the popup offers the right ref
 * methods on the next line instead of generic Polars items.
 */
export function createRefVariableCompletions(getExtraCode: () => string[]): CompletionSource {
  return (context) => {
    const match = context.matchBefore(/\.\w*/);
    if (!match) return null;
    // Identifier immediately before the dot.
    const before = context.state.doc.sliceString(Math.max(0, match.from - 64), match.from);
    const idMatch = before.match(/([A-Za-z_]\w*)$/);
    if (!idMatch) return null;
    const varName = idMatch[1];
    // These have their own dedicated sources — don't shadow them.
    if (varName === "flowfile_ctx" || varName === "flowfile" || varName === "pl") return null;

    const allCode = [...getExtraCode(), context.state.doc.toString()].join("\n");
    const kind = buildRefVarMap(allCode).get(varName);
    if (!kind) return null;

    return {
      from: match.from + 1,
      options: REF_METHODS_BY_KIND[kind],
      validFor: /^\w*$/,
    };
  };
}
