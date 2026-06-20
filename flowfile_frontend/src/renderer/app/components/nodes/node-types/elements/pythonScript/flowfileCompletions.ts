import type { Completion, CompletionSource } from "@codemirror/autocomplete";

import type { UpstreamColumn } from "./useUpstreamColumns";

// ─── Static entries ──────────────────────────────────────────────────────────

const FLOWFILE_API_ENTRIES: Completion[] = [
  {
    label: "read_input",
    type: "function",
    info: "Read input DataFrame. Optional name parameter for named inputs.",
    detail: "flowfile_ctx.read_input(name?)",
    apply: "read_input()",
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
  {
    label: "write_catalog_table",
    type: "function",
    info: 'Write a Polars DataFrame/LazyFrame to a catalog table. write_mode: "overwrite" | "append" | "upsert" | "update" | "delete" | "error". Merge modes require merge_keys=.',
    detail: 'flowfile_ctx.write_catalog_table(df, "name", schema?, write_mode?, merge_keys?)',
    apply: 'write_catalog_table(df, "name")',
  },
];

// Polars module-level (after `pl.`)
const POLARS_MODULE_ENTRIES: Completion[] = [
  { label: "col", type: "function", info: "Reference a column by name", apply: 'col("")' },
  { label: "lit", type: "function", info: "Literal value", apply: "lit()" },
  {
    label: "when",
    type: "function",
    info: "Conditional expression: when(...).then(...).otherwise(...)",
    apply: "when()",
  },
  {
    label: "concat",
    type: "function",
    info: "Concatenate DataFrames/LazyFrames",
    apply: "concat()",
  },
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
const _RE_CATALOG_CALL = /\.get_catalog\s*\([^()]*\)\s*$/;
const _RE_SCHEMA_CALL = /\.(?:get_schema|default_schema)\s*\([^()]*\)\s*$/;
const _RE_TABLE_CALL = /\.get_table_ref\s*\([^()]*\)\s*$/;

// Variable-assignment regexes anchored to the final ref-producing method.
// Greedy `.+` covers any base expression — `cat = flowfile_ctx.get_catalog("x")`
// AND `tref = schema.get_table_ref("t")` both match the table-assignment rule
// because the final `.method(...)` segment is what determines the resulting
// ref's type. Multi-line bases aren't supported (the `m` flag anchors `^`/`$`
// to line boundaries).
const _RE_ASSIGN_CATALOG = /^\s*([A-Za-z_]\w*)\s*=\s*.+\.get_catalog\s*\([^()]*\)\s*$/gm;
const _RE_ASSIGN_SCHEMA =
  /^\s*([A-Za-z_]\w*)\s*=\s*.+\.(?:get_schema|default_schema)\s*\([^()]*\)\s*$/gm;
const _RE_ASSIGN_TABLE = /^\s*([A-Za-z_]\w*)\s*=\s*.+\.get_table_ref\s*\([^()]*\)\s*$/gm;

type RefKind = "catalog" | "schema" | "table";

const _REF_METHODS_BY_KIND: Record<RefKind, Completion[]> = {
  catalog: CATALOG_REF_METHODS,
  schema: SCHEMA_REF_METHODS,
  table: TABLE_REF_METHODS,
};

/**
 * Scan code for `name = <expr>.get_catalog|get_schema|default_schema|get_table_ref(...)`
 * lines and return a map of varname → ref kind. Re-built on each completion
 * request because notebook cells change frequently.
 */
function buildRefVarMap(code: string): Map<string, RefKind> {
  const map = new Map<string, RefKind>();
  for (const m of code.matchAll(_RE_ASSIGN_CATALOG)) map.set(m[1], "catalog");
  for (const m of code.matchAll(_RE_ASSIGN_SCHEMA)) map.set(m[1], "schema");
  for (const m of code.matchAll(_RE_ASSIGN_TABLE)) map.set(m[1], "table");
  return map;
}

// ─── Scoped completion sources ────────────────────────────────────────────────

/**
 * Globals injected into the kernel exec namespace. Suggested as bare-name
 * completions so typing `flowfile_ct…` offers `flowfile_ctx` before the user
 * has typed the dot. `flowfile` is also suggested but flagged as deprecated.
 */
const KERNEL_GLOBAL_ENTRIES: Completion[] = [
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
  {
    label: "pl",
    type: "namespace",
    info: "Polars module (when imported as `import polars as pl`). Type `pl.` to see DataFrame/LazyFrame/Series constructors and read_* / scan_* helpers.",
    detail: "polars",
  },
];

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
  if (_RE_TABLE_CALL.test(lookback)) {
    options = TABLE_REF_METHODS;
  } else if (_RE_SCHEMA_CALL.test(lookback)) {
    options = SCHEMA_REF_METHODS;
  } else if (_RE_CATALOG_CALL.test(lookback)) {
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
 * catalog/schema/table ref. Scans the current cell doc + prior cells for
 * assignment patterns like `schema = flowfile_ctx.default_schema()` so the
 * popup offers the right ref methods on the next line instead of generic
 * Polars items.
 */
export function createRefVariableCompletions(getPriorCellCodes: () => string[]): CompletionSource {
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

    const allCode = [...getPriorCellCodes(), context.state.doc.toString()].join("\n");
    const kind = buildRefVarMap(allCode).get(varName);
    if (!kind) return null;

    return {
      from: match.from + 1,
      options: _REF_METHODS_BY_KIND[kind],
      validFor: /^\w*$/,
    };
  };
}

/**
 * Completions after any `.` — common Polars Expr / DataFrame / LazyFrame methods.
 * Skipped when the preceding identifier is `flowfile_ctx`, the legacy `flowfile`
 * alias, `pl`, any catalog-ref-producing call, or a variable locally bound to a
 * catalog/schema/table ref (those have dedicated sources).
 */
export function createPolarsExprCompletions(getPriorCellCodes: () => string[]): CompletionSource {
  return (context) => {
    const match = context.matchBefore(/\.\w*/);
    if (!match) return null;

    // Skip if preceded by `flowfile_ctx`, `flowfile`, or `pl` — those have dedicated, more specific sources
    const lookback = context.state.doc.sliceString(Math.max(0, match.from - 200), match.from);
    if (/\b(?:flowfile_ctx|flowfile)$/.test(lookback) || /\bpl$/.test(lookback)) return null;
    // Also step aside for catalog-ref chains so the Polars list doesn't pollute
    // ref completions.
    if (
      _RE_CATALOG_CALL.test(lookback) ||
      _RE_SCHEMA_CALL.test(lookback) ||
      _RE_TABLE_CALL.test(lookback)
    ) {
      return null;
    }
    // Step aside for ref-bound variables (handled by createRefVariableCompletions).
    const idMatch = lookback.match(/([A-Za-z_]\w*)$/);
    if (idMatch) {
      const allCode = [...getPriorCellCodes(), context.state.doc.toString()].join("\n");
      if (buildRefVarMap(allCode).has(idMatch[1])) return null;
    }

    return {
      from: match.from + 1,
      options: POLARS_METHOD_ENTRIES,
      validFor: /^\w*$/,
    };
  };
}

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
  const IMPORT_RE =
    /^\s*import\s+([A-Za-z_][\w.]*(?:\s+as\s+[A-Za-z_]\w*)?(?:\s*,\s*[A-Za-z_][\w.]*(?:\s+as\s+[A-Za-z_]\w*)?)*)/gm;
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
