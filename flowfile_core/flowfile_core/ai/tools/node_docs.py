"""Per-node-type narrative documentation.

Two parallel dicts, one per audience:

* :data:`NODE_LONG_DESCRIPTIONS` — agent-shaped prose ("when to call this
  tool"). Used by tool-calling surfaces (``agent`` / ``agent_complex`` /
  ``cmd_k`` / ``ghost_node``) so the model picks the right tool.
* :data:`NODE_USER_INSTRUCTIONS` — user-shaped prose ("how does the user
  do this in the UI"). Used by chat / advisory surfaces (``explain`` /
  ``lineage`` / ``docgen``) so the chat answers cite real palette labels
  ("Group by"), real settings field names ("Field", "Action", "Output
  Field Name"), and real sidebar sections ("Aggregations") instead of
  inventing UI elements like "transform node" / "expression editor".

Structure per :data:`NODE_LONG_DESCRIPTIONS` entry (agent audience):

* One-sentence summary of what the node does.
* "Use when" — the shape of input + the user's goal.
* "Don't use when" — the alternative node that fits better.
* One or two compact example settings snippets.
* Cross-reference one or two commonly-paired nodes.

Structure per :data:`NODE_USER_INSTRUCTIONS` entry (user audience):

* Settings panel — the 2-4 fields that actually matter, using the real
  label strings.
* Worked example — a concrete ETL ask the user is plausibly making.
* Pitfalls — common mistakes (e.g. "count needs a Field even though the
  value doesn't matter").

The palette label and sidebar section are read at runtime from
``flowfile_core.configs.node_store.nodes`` (single source of truth) so
they don't drift here when the UI is renamed. See
:func:`palette_label_for` / :func:`sidebar_section_for` for the lookup
helpers.

The minimum length floor (80 chars) is enforced by the test suite; if you
add a new node type, write something substantive — not a placeholder.
"""

from __future__ import annotations

from typing import Final

# Sidebar section label → human-readable heading. Mirrors the hardcoded
# map in ``flowfile_frontend/src/renderer/app/views/DesignerView/NodeList.vue``
# (the only place the frontend stores these). The chat surface needs the
# real heading text so the model says "drag from the Aggregations sidebar"
# instead of inventing names. If the frontend renames a section, update
# both here and in ``NodeList.vue``; the cross-check test catches drift.
NODE_GROUP_TO_SIDEBAR_LABEL: Final[dict[str, str]] = {
    "input": "Input Sources",
    "transform": "Transformations",
    "combine": "Combine Operations",
    "aggregate": "Aggregations",
    "ml": "Machine Learning",
    "output": "Output Operations",
    "custom": "User Defined Operations",
}


def palette_label_for(node_type: str) -> str:
    """Return the palette label for ``node_type`` from ``nodes.py``.

    Falls back to the raw node-type string when the node doesn't appear
    in the palette (``promise`` / ``user_defined`` are internal — no
    palette entry). Lazy-imports the node store so this module stays
    cheap to import.
    """

    from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

    _, nodes_dict, _ = get_all_standard_nodes()
    template = nodes_dict.get(node_type)
    if template is None:
        return node_type
    return template.name


def sidebar_section_for(node_type: str) -> str:
    """Return the sidebar section heading for ``node_type``.

    Falls back to the raw ``node_group`` value (or the empty string for
    nodes not in the palette) so the test suite can flag missing maps
    without crashing the prompt build.
    """

    from flowfile_core.configs.node_store.nodes import get_all_standard_nodes

    _, nodes_dict, _ = get_all_standard_nodes()
    template = nodes_dict.get(node_type)
    if template is None:
        return ""
    return NODE_GROUP_TO_SIDEBAR_LABEL.get(template.node_group, template.node_group)


NODE_LONG_DESCRIPTIONS: Final[dict[str, str]] = {
    "manual_input": (
        "Inline literal data — type a small table directly into the flow with no "
        "external file. Use for tiny lookup dimensions (≤ ~50 rows), test fixtures, "
        "or a constant join target like a country-code map. Don't use for anything "
        "you'd otherwise load from disk; use 'read' for files and 'manual_input' "
        "only when there is no external source. Example settings: "
        '{"raw_data_format": {"columns": [{"name": "code"}, {"name": "label"}], '
        '"data": [["US", "United States"], ["NL", "Netherlands"]]}}. '
        "Often paired upstream of 'join' (small lookup) or 'union' (constants)."
    ),
    "filter": (
        "Keep only rows matching a predicate. Use when the user wants 'rows where X', "
        "'only Y', 'last 30 days', 'remove nulls', etc. The predicate runs row-wise "
        "over the upstream schema; the output schema is identical. Don't use for "
        "column-level transforms — use 'formula' to derive a new value, or 'select' "
        "to drop columns. Don't use to remove duplicates — use 'unique'. Modes: "
        '"basic" (column op value) and "advanced" (a Polars expression string). '
        "Basic-mode operator vocabulary is snake_case (equals, not_equals, "
        "greater_than, greater_than_or_equals, less_than, less_than_or_equals, "
        "contains, not_contains, starts_with, ends_with, is_null, is_not_null, "
        "in, not_in, between) — do NOT use the symbol form (>, <, ==). "
        "Basic-mode `value` is ALWAYS a JSON string, even for numeric comparisons: "
        'write "1" not 1, "0.5" not 0.5. Polars handles the str→numeric coercion '
        "downstream when the column dtype is numeric. "
        'Basic-mode example: {"filter_input": {"mode": "basic", "basic_filter": '
        '{"field": "email_count", "operator": "greater_than", "value": "1"}}}. '
        'Advanced-mode example: {"filter_input": {"mode": "advanced", '
        "\"advanced_filter\": \"pl.col('status') == 'active'\"}}. "
        "Often the first step after 'read' / 'manual_input'."
    ),
    "formula": (
        "**ROW-WISE ONLY — CANNOT aggregate, count, sum, average, min, max, "
        "or compute across rows.** For aggregation use ``group_by``; for raw "
        "row count use ``record_count``; for window / multi-column / "
        "aggregation logic that ``[col]`` syntax can't express use "
        "``polars_code``. Pick ``formula`` ONLY when the new column can be "
        "derived from the SAME row's existing columns (string concat, "
        "arithmetic, conditional, type cast). "
        "Adds or replaces a single column using Flowfile's expression "
        "language. **Syntax is NOT raw Polars.** Column references are "
        "SQL-style ``[column_name]`` (square brackets), not "
        "``pl.col('column_name')``. Operators ``+``, ``-``, ``*``, ``/``, "
        "``==``, ``!=``, ``and``, ``or`` work; the canonical function "
        "library is documented at stage 3 (fill_settings) when you pick this "
        "node type. Examples: ``[first] + ' ' + [last]`` (string concat), "
        "``[amount] * 1.21`` (arithmetic). Often paired upstream of "
        "``group_by`` (after deriving a key) or after ``join`` (combining "
        "columns from both sides)."
    ),
    "select": (
        "Project, rename, drop, or reorder columns; can also cast types. Use when "
        "the user says 'only keep X', 'rename A to B', 'drop the email column', or "
        "after a join when half the columns are no longer needed. Don't use to "
        "compute new values — use 'formula'; don't use to filter rows — use "
        "'filter'. Example: "
        '{"select_input": [{"old_name": "id", "new_name": "user_id", "keep": true}, '
        '{"old_name": "email", "keep": false}]}. '
        "Often the last step before 'output' or 'database_writer' to shape the "
        "final column list."
    ),
    "sort": (
        "Order rows by one or more columns ascending or descending. Use when the "
        "user wants 'sorted by X', 'top N by Y' (sort then 'sample'), or 'order by'. "
        "The output schema is identical — only row order changes. Don't use sort "
        "as a substitute for 'group_by' — sorting does not deduplicate or aggregate. "
        'Example: {"sort_input": [{"column": "created_at", "how": "desc"}]}. '
        "Often paired upstream of 'sample' (top-N) or 'unique' (keep-first dedupe)."
    ),
    "record_id": (
        "Add a sequential 1-based row-number column. Use when the user wants "
        "'add a row id', 'number the rows', or to assign a stable ordering before "
        "a downstream split. Don't confuse with 'record_count' — that produces a "
        "single scalar count row, this adds a per-row id. Don't use as a join key "
        "across two unrelated inputs — record ids are local to one input. Example: "
        '{"output_column_name": "row_id"}. '
        "Often paired with 'sort' upstream (so the id reflects a meaningful order)."
    ),
    "sample": (
        "Take a random or first-N subset of rows. Use to spot-check a large dataset, "
        "build a development sample, or grab the top-N after a 'sort'. Two modes: "
        "'first' (head-N) and 'random' (uniform sample with seed). Don't use to "
        "deduplicate — use 'unique'. Don't use for stratified sampling — that needs "
        "'group_by' upstream and a per-group sample expression in 'polars_code'. "
        'Example: {"sample_size": 1000, "sample_type": "random", "seed": 42}. '
        "Often paired downstream of 'sort' (top-N) or upstream of 'output' (preview)."
    ),
    "random_split": (
        "Split rows into two outputs by ratio (e.g. 80/20 train/test). Emits two "
        "named outputs: output-0 (the kept fraction) and output-1 (the rest). "
        "Use for ML train/test splits or A/B sampling. Don't use for filter-style "
        "binary partitions — 'filter' with split_mode=true gives a deterministic "
        'pass/fail. Example: {"ratio": 0.8, "seed": 42}. '
        "Often paired upstream of 'train_model' (output-0) and 'evaluate_model' "
        "(output-1)."
    ),
    "unique": (
        "Deduplicate rows on a column subset. Use when the user says 'remove "
        "duplicates', 'distinct', 'unique by X', 'keep one row per id'. The 'first' "
        "/ 'last' strategy controls which row is kept when duplicates exist; "
        "combine with 'sort' upstream to make the choice deterministic. Don't use "
        "to count duplicates — that's 'group_by' with a count aggregation. "
        'Example: {"unique_input": {"columns": ["customer_id"], "strategy": "first"}}. '
        "Often paired with 'sort' upstream (recency-based dedupe)."
    ),
    "group_by": (
        "Aggregate rows by one or more group keys. Use whenever the user asks for "
        "'total by X', 'average per Y', 'count per group', 'sum/min/max'. Output "
        "is one row per unique combination of group keys, with the chosen "
        "aggregation columns. Don't use 'formula' for this — formulas are row-wise "
        "and cannot collapse rows. Don't use 'pivot' unless the user explicitly "
        "wants a long-to-wide reshape on top. Group keys and aggregations share "
        "a single `agg_cols` list — group keys are entries with `agg=\"groupby\"`. "
        "Example: "
        '{"groupby_input": {"agg_cols": ['
        '{"old_name": "region", "agg": "groupby"}, '
        '{"old_name": "amount", "agg": "sum", "new_name": "total"}'
        "]}}. "
        "Often paired downstream of 'filter' (then aggregate) and upstream of "
        "'sort' (rank by aggregate)."
    ),
    "window_functions": (
        "Add rolling, cumulative, rank, or tile columns over ordered rows, "
        "optionally reset per partition (Polars `.over(...)`). Use for running "
        "totals, moving averages, row ranks within a group, or N-tile buckets. "
        "Don't use for plain group aggregation (one row per group) — that's "
        "'group_by'; window functions keep every input row and add columns. "
        "Rolling and tile functions REQUIRE at least one order_by column; "
        "rolling needs window_size; each op needs a unique new_column_name. "
        "Example: "
        '{"window_input": {"partition_by": ["region"], '
        '"order_by": [{"column": "date", "how": "asc"}], '
        '"window_functions": [{"column": "amount", "function": "rolling_mean", '
        '"new_column_name": "amount_7d_avg", "window_size": 7}]}}. '
        "Often paired downstream of 'sort'/'filter' and upstream of 'select'."
    ),
    "pivot": (
        "Long-to-wide reshape: turn distinct values of one column into new columns "
        "with an aggregate per cell. Use for cross-tabs, monthly summaries (rows "
        "by category, columns by month), or any 'one column per X' presentation. "
        "Don't use to 'group by' — that's 'group_by'; pivot is a presentation step "
        "downstream of aggregation. Don't use when the cardinality of the pivoted "
        "column is high (> ~100); the result blows up. Example: "
        '{"pivot_input": {"index_columns": ["region"], "pivot_column": "month", '
        '"value_col": "amount", "aggregations": ["sum"]}}. '
        "Often paired downstream of 'group_by' or upstream of 'output' (reporting)."
    ),
    "unpivot": (
        "Wide-to-long reshape: collapse multiple value columns into a single "
        "(name, value) pair of columns. Use to normalise a wide schema where "
        "monthly / category columns are separate, so downstream nodes can "
        "'group_by' across them. The inverse of 'pivot'. Don't use when only one "
        "of the wide columns is wanted — use 'select' to keep that one column. "
        'Example: {"unpivot_input": {"index_columns": ["region"], '
        '"value_columns": ["jan", "feb", "mar"]}}. '
        "Often paired upstream of 'group_by' or 'filter' (after the long form)."
    ),
    "text_to_rows": (
        "Split a delimited string column into multiple rows — one row per token. "
        "Use to explode tags, comma-separated lists, or CSV-in-a-cell. Don't use "
        "to split into multiple columns — that's a 'formula' with a string-split "
        "expression. Example: "
        '{"text_to_rows_input": {"column_to_split": {"name": "tags"}, '
        '"split_by_fixed_value": true, "split_value": ","}}. '
        "Often paired downstream of 'read' (raw CSVs with packed cells)."
    ),
    "graph_solver": (
        "Find connected components across two id columns: e.g. for record-linkage "
        "or clustering of related identifiers. Use when the user says 'find groups "
        "of connected ids', 'cluster matching records', 'union-find', or after a "
        "'fuzzy_match' to coalesce match pairs into clusters. Don't use for "
        "general-purpose graph traversal — only connected-component output is "
        'returned. Example: {"graph_solver_input": {"col_from": "id_a", '
        '"col_to": "id_b", "output_column_name": "cluster_id"}}. '
        "Often paired downstream of 'fuzzy_match' or two 'join's that produce "
        "id-pair output."
    ),
    "python_script": (
        "Run a Python function in the isolated kernel_runtime sandbox; receives "
        "upstream inputs as polars DataFrames, returns a DataFrame. Use only when "
        "no other node fits — custom enrichment with an external library, "
        "imperative logic that doesn't translate to a Polars expression, or a "
        "complex multi-step transform. Don't use for things 'formula' / "
        "'polars_code' / 'sql_query' can express; those are typed and faster. "
        "Don't use for I/O — the sandbox blocks network unless explicitly "
        "approved. The output schema must be re-discovered by a 1-row "
        "dry-run. Often paired with the codegen tool "
        "'flowfile.codegen.generate_python_script' to author the script body."
    ),
    "polars_code": (
        "Run a Polars expression body against the upstream LazyFrame. Use for "
        "complex multi-column transforms or window functions where 'formula' "
        "(single-column) is too narrow but 'python_script' (full sandbox) is "
        "overkill. The body must end with a returnable LazyFrame / DataFrame. "
        "Don't use to issue arbitrary Python — use 'python_script' for that. "
        "Don't use for SQL-shaped joins/aggregations — 'sql_query' is clearer. "
        "1-row dry-run discovers the prospective output schema. "
        "Often paired with 'flowfile.codegen.generate_polars_code' to author the body. "
        "``pl`` is already available — do NOT write ``import polars as pl``; "
        "imports are rejected by the sandbox."
    ),
    "sql_query": (
        "Run a SQL SELECT against upstream inputs via Polars' embedded SQL "
        "engine. Use when the user already thinks in SQL or for multi-input "
        "queries that are clearer expressed as JOINs in SQL than as a chain "
        "of 'join' nodes. **Upstream inputs are registered POSITIONALLY as "
        "``input_1``, ``input_2``, ... — the table name is NEVER the upstream "
        "node's id, type, or display name. Always write ``FROM input_1`` "
        "(single upstream) or ``FROM input_1 a JOIN input_2 b ON ...`` "
        "(multiple upstreams, in connect order).** Don't use for write "
        "operations — Polars SQL is read-only. Don't use for window/CTE-heavy "
        "queries that aren't supported by Polars SQL; fall back to "
        "'polars_code'. Often paired with "
        "'flowfile.codegen.generate_sql_query' to author the SELECT."
    ),
    "join": (
        "**KEY-BASED join. REQUIRES at least one equality key pair in "
        "``join_mapping``. If you don't have a key column to match on, "
        "use ``cross_join`` instead — `join` will have nothing to "
        "match on and is the wrong tool.** Strategies: left, inner, "
        "right, outer, full, semi, anti — `how` does NOT include "
        "``\"cross\"`` (the enum is inner/left/right/full/semi/anti/"
        "outer; cross/Cartesian goes through the dedicated "
        "``cross_join`` node). Use whenever the user says 'lookup', "
        "'merge', 'attach', 'enrich on column X', or 'combine A with "
        "B on column'. Takes TWO inputs: the LEFT side (driving "
        "table — its rows are preserved for left-joins; its columns "
        "appear first in the output) and the RIGHT side (joining "
        "table). Match column names exactly; cast types with "
        "'formula' upstream if they don't match. Don't use "
        "'fuzzy_match' if the keys are exact — fuzzy is slower and "
        "approximate. Example: "
        '{"join_input": {"join_mapping": [{"left_col": "user_id", '
        '"right_col": "id"}], "how": "left"}}. '
        "Often paired with 'select' downstream to drop redundant key columns."
    ),
    "cross_join": (
        "**Cartesian / NO-KEY join. The ONLY way to combine two "
        "inputs WITHOUT a key column.** Every LEFT row paired with "
        "every RIGHT row. Order-symmetric (A×B has the same rows as "
        "B×A) — but the LEFT columns appear first in the output, "
        "then the RIGHT columns. **Triggers**: any task where the "
        "user wants to combine two streams and there's no shared "
        "key — the canonical pattern is *broadcasting a single-row "
        "total* onto every row of a larger table (e.g. 'percentage "
        "of customers per city vs the total': group_by → "
        "record_count gives the total → cross_join attaches the "
        "total to every per-city row → formula computes the "
        "percentage). Other uses: every-combination expansion "
        "(calendar × dimensions, scenario grids). DO NOT use "
        "`join` for these patterns — `join` requires a key "
        "(``join_mapping``) and will fail or produce nothing if "
        "you don't have one. No key columns needed here — just "
        "connect both inputs. Pitfall: output is N×M rows; for a "
        "1k × 1k input you get 1M rows. Restrict by broadcasting "
        "a single-row side, or pair with 'filter' downstream to "
        "prune. Often paired upstream of 'formula' (compute ratios "
        "using the broadcast value)."
    ),
    "fuzzy_match": (
        "Approximate-match join: pair LEFT rows with RIGHT rows by "
        "similarity rather than exact key equality. Use for record-linkage "
        "on names / addresses / free-text where exact join would miss "
        "obvious matches. Slower than 'join' and produces match scores. "
        "Don't use when keys are exact identifiers (UUIDs, ints, stable "
        "codes) — 'join' is faster and exact. Don't use as a substitute "
        "for cleaning — normalise casing / whitespace upstream with "
        "'formula' first. Output adds a similarity score column; LEFT "
        "columns appear before RIGHT in the output. Often paired "
        "downstream of 'graph_solver' (cluster the matched pairs) or with "
        "'filter' to keep only high-confidence matches."
    ),
    "record_count": (
        "Output a single row containing the row count of the upstream input. Use "
        "for sanity checks ('how many rows survived the filter?') or to feed a "
        "downstream 'formula' that needs a total. Don't confuse with 'record_id' "
        "— that adds a per-row id; this collapses to one count row. Example: "
        "no settings beyond connection — emits one row, one column. "
        "Often paired downstream of 'filter' (count survivors) or as a debug tap."
    ),
    "explore_data": (
        "**NO settings required — emit an empty object ``{}`` for the "
        "settings (the planner injects flow_id / node_id / upstream "
        "automatically).** Profile the upstream input — column types, "
        "null counts, distinct counts, summary statistics. Use for "
        "data discovery: 'what does this dataset look like?', or as a "
        "lightweight 'now you can inspect the result' inspector at "
        "the end of a transformation chain. Output is a wide "
        "diagnostic table; not typically connected downstream. Don't "
        "fabricate field values — there's nothing to fill. The LLM "
        "should announce something like *\"Added an explore_data "
        "node; you can inspect the data on the canvas\"* and stop. "
        "Don't use as a transform — it does not change the data, it "
        "produces a profile artefact."
    ),
    "union": (
        "Concatenate two or more inputs row-wise, optionally aligning by name or "
        "by position. Use when the user says 'append', 'stack', 'combine A and B', "
        "or to merge multi-source data with the same logical schema. Don't use "
        "to combine columns side-by-side — that's 'join'. Don't use when schemas "
        "differ — align them with 'select' / 'formula' upstream first. Modes: "
        "'relaxed' (align by name, fill missing with null) or 'strict' (require "
        'identical schemas). Example: {"union_input": {"mode": "relaxed"}}. '
        "Often paired upstream of a single 'output' that consolidates multi-source "
        "input."
    ),
    "output": (
        "Write the upstream output to a file (csv, parquet, excel, json). Use as "
        "the terminal step of a flow that produces a local artefact. The format "
        "is inferred from the path extension or set explicitly. Don't use to "
        "write to a database — use 'database_writer'. Don't use to upload to "
        "cloud — use 'cloud_storage_writer'. Example: "
        '{"output_settings": {"name": "report.parquet", "directory": '
        '"/tmp/outputs", "output_csv_table": null}}. '
        "Often the last node in a flow."
    ),
    "api_response": (
        "Mark the upstream output as the body of an HTTP API response. Use as the "
        "terminal node of a flow you intend to publish as an API endpoint — the "
        "data reaching this node is serialized and returned to the caller. During "
        "interactive runs it is a pass-through (output equals input), so previews "
        "keep working. Don't use to write a file — use 'output'; don't use to "
        "persist to a database — use 'database_writer'. Settings: orientation "
        "('records' = a list of row objects, the default; 'columns' = "
        "column-oriented arrays) and an optional max_rows cap on the payload. "
        'Example: {"orientation": "records", "max_rows": 100}. '
        "Often the last node in a flow that is published as an endpoint."
    ),
    "read": (
        "Read a local file (csv, parquet, excel, json) into the flow. Use as the "
        "first step when the data lives on disk. Schema is inferred (csv) or "
        "self-describing (parquet). Don't use to read from a database — use "
        "'database_reader'. Don't use to read from cloud storage — use "
        "'cloud_storage_reader'. Example: "
        '{"received_file": {"name": "users.csv", "file_type": "csv"}}. '
        "Often paired downstream with 'filter' / 'select' (initial cleanup)."
    ),
    "database_reader": (
        "Read from a SQL database via a stored connection. Use to pull the result "
        "of a SELECT directly into a flow without a local file. Specify the "
        "connection (a stored credential) and either a table name or a custom "
        "query string. Don't use 'sql_query' for this — that runs Polars SQL "
        "against upstream nodes, not against a remote DB. Often paired downstream "
        "with 'filter' / 'join' (combine DB data with local data)."
    ),
    "database_writer": (
        "Write the upstream output to a SQL database table via a stored "
        "connection. Use as a terminal step that persists results. Append, "
        "replace, or create-if-missing semantics depending on the chosen mode. "
        "Don't use 'output' for this — that writes a file. Don't use without "
        "considering schema mismatch — cast types upstream with 'select'/'formula' "
        "to align with the target table."
    ),
    "cloud_storage_reader": (
        "Read from S3 / GCS / Azure Blob / Azure Data Lake. Use when the source "
        "data lives in cloud object storage. Specify the auth (a stored cloud "
        "connection or env credentials) and the path / glob. Format inferred "
        "from extension. Don't use 'read' for cloud paths — use this. "
        "Often paired downstream with 'filter' / 'select' (initial cleanup)."
    ),
    "cloud_storage_writer": (
        "Write the upstream output to S3 / GCS / Azure Blob. Use as a terminal "
        "step for cloud-persisted artefacts. Format chosen by extension or "
        "explicit setting. Don't use 'output' for cloud paths. Often the last "
        "node when results are consumed by downstream cloud systems."
    ),
    "catalog_reader": (
        "Read from a data catalog table (e.g. Iceberg, Delta) via a registered "
        "catalog connection. Use to pull governed data without managing object "
        "paths directly. Don't use 'cloud_storage_reader' when a catalog binding "
        "exists — the catalog reader handles schema, partitioning, and time-travel "
        "on the user's behalf. Often the first step in a governed pipeline."
    ),
    "catalog_writer": (
        "Write the upstream output to a catalog table. Use as a terminal step "
        "for governed datasets. Append / overwrite / merge semantics depending "
        "on the catalog binding. Don't use 'cloud_storage_writer' when the target "
        "is a catalog table — the catalog writer maintains the metadata correctly."
    ),
    "kafka_source": (
        "Stream-read from a Kafka topic. Use when the data source is a Kafka "
        "topic and the flow is consuming a bounded slice (offset / time range). "
        "Don't use for unbounded streaming in a non-streaming flow — Flowfile "
        "executes flows as a finite DAG; treat this as a windowed read."
    ),
    "google_analytics_reader": (
        "Read from Google Analytics 4 via a stored credential. Use when the user "
        "wants GA4 metrics / dimensions inside the flow. Don't use for ad-platform "
        "data — those need their own connectors / 'external_source'."
    ),
    "external_source": (
        "Generic plug-in source: invokes a registered external connector. Use "
        "for data sources that don't have a first-class node yet. Schema and "
        "behaviour depend on the chosen connector. Don't use when a specific "
        "node already exists for the source — prefer the typed node."
    ),
    "promise": (
        "An empty placeholder node — a node id that exists but has no settings "
        "yet. Created internally by editor flows that reserve an id before "
        "attaching settings. The agent has no tool to create one (the generic "
        "'flowfile.graph.add_node' was removed because it confused the model "
        "into emitting `node_type=\"node\"`); use the typed "
        "'flowfile.graph.add_<type>' tools instead."
    ),
    "user_defined": (
        "A user-defined node (UDF) — runs custom code registered in the flow's "
        "node store. Use when the workspace has a registered UDF that fits the "
        "task; the per-UDF tool name is 'flowfile.graph.add_<udf_type>'. Don't "
        "use this generic placeholder — pick the named UDF tool instead."
    ),
    "train_model": (
        "Train a machine-learning model on the upstream input. Use as the first "
        "ML step after preparing features (with 'select' / 'formula') and "
        "splitting into train/test ('random_split'). Output is a model artefact "
        "consumed by 'apply_model' / 'evaluate_model'. Don't use to score new "
        "data — that's 'apply_model' downstream of training."
    ),
    "apply_model": (
        "Score data with a trained model. Use after 'train_model' to predict on "
        "new (or held-out) data. Input must have the same feature columns the "
        "model was trained on. Don't use without a 'train_model' upstream — the "
        "model artefact is required."
    ),
    "evaluate_model": (
        "Compute evaluation metrics for a trained model on labelled data (e.g. "
        "the test split from 'random_split'). Use after 'train_model' to assess "
        "quality. Don't use to score unlabelled data — that's 'apply_model'."
    ),
    "wait_for": (
        "Synchronisation barrier: wait for one or more upstream nodes to finish "
        "before downstream nodes run. Use when downstream sequencing matters "
        "but data doesn't flow through the barrier. Don't use as a data-passing "
        "node — its output is the unmodified primary input. Often used to enforce "
        "side-effect ordering (write A before reading B)."
    ),
}


NODE_USER_INSTRUCTIONS: Final[dict[str, str]] = {
    "manual_input": (
        "Settings: a small inline grid where the user types column names + "
        "rows directly. No file path, no connection — the data is embedded "
        "in the node settings. Worked example: 'I need a country-code lookup "
        "table': drag 'Manual input' from the Input Sources sidebar, click "
        "'Add column' to define `code` and `label`, then add rows like `US, "
        "United States` and `NL, Netherlands`. Pitfall: this is for tiny "
        "tables only (≤ ~50 rows); for anything larger use 'Read data' "
        "with a CSV / Parquet file."
    ),
    "filter": (
        "Settings panel: 'Column' (the column to test), 'Operator' "
        "(==, !=, >, contains, etc.), 'Value' (the literal to compare "
        "against), and an 'And' / 'Or' chain to add more clauses. There's "
        "also an Advanced mode that takes a single Polars expression "
        "string. Worked example: 'keep only EU customers' → drag 'Filter "
        "data' from Transformations, connect it to the upstream Read node, "
        "set Column=region, Operator==, Value=EU. Pitfall: filter does NOT "
        "drop columns or compute new ones — to drop columns use 'Select "
        "data', to compute use 'Formula'. Filter only removes rows."
    ),
    "formula": (
        "**ROW-WISE ONLY — CANNOT count, sum, average, or aggregate across "
        "rows.** For aggregation use 'Group by' or 'Record count'. Pick "
        "Formula only when the new column comes from the SAME row's "
        "existing values (concat, arithmetic, conditional, type cast). "
        "Settings panel: a single 'Output column' name, a Flowfile "
        "expression editor for the formula body, and a 'Data type' "
        "selector for the output. **Syntax**: SQL-style ``[column_name]`` "
        "references (square brackets), NOT Polars-Python. Worked example: "
        "'add a full_name column from first + last' → drag 'Formula' from "
        "Transformations, set Output column=full_name, expression="
        "``[first] + ' ' + [last]``. Pitfall 1: NOT Polars syntax — "
        "``pl.col('first')`` doesn't work; use ``[first]``. Pitfall 2: "
        "Formula is row-wise — it cannot aggregate across rows. To compute "
        "totals or per-group statistics use 'Group by' first, then a "
        "Formula on the aggregated result. Pitfall 3: For multi-column "
        "transforms or window functions use 'Polars code' instead — that "
        "node accepts real Polars-Python."
    ),
    "select": (
        "Settings panel: a row per upstream column with checkboxes for "
        "'Keep' / 'Drop', a 'New name' field for renames, a 'Position' "
        "field for reorder, and an optional data-type cast. Worked "
        "example: 'rename id to user_id and drop email' → drag 'Select "
        "data' from Transformations, find the `id` row, set New name="
        "user_id, find the `email` row, uncheck Keep. Pitfall: 'Select "
        "data' does not compute new columns — for that use 'Formula'. It "
        "also doesn't filter rows — that's 'Filter data'."
    ),
    "sort": (
        "Settings panel: a 'Columns' list where the user adds sort keys, "
        "each with an Ascending / Descending toggle. Multiple keys are "
        "applied in order (primary, secondary, …). Worked example: 'sort "
        "by created_at descending' → drag 'Sort data' from Transformations, "
        "click Add column, pick `created_at`, toggle to Descending. "
        "Pitfall: sort does NOT deduplicate. If the user wants 'most "
        "recent record per customer', sort + 'Drop duplicates' (with Keep "
        "strategy = first) is the combo."
    ),
    "record_id": (
        "Settings panel: an 'Offset' field (1-based by default) and an "
        "'Output name' for the new column (defaults to `record_id`). "
        "Worked example: 'add a row id starting at 1' → drag 'Add record "
        "Id' from Transformations, leave Offset=1, set Output name=row_id. "
        "Pitfall: this is for adding a per-row identifier; if the user "
        "wants the *count* of rows (a single number), they want 'Count "
        "records' instead."
    ),
    "sample": (
        "Settings panel: a sample size, a sample method ('First N' or "
        "'Random N'), and a seed for the random method. Worked example: "
        "'preview 100 random rows' → drag 'Take Sample' from "
        "Transformations, set size=100, method=Random, seed=42. Pitfall: "
        "for stratified sampling (e.g. 100 random rows *per region*) "
        "this node alone won't do it — combine with 'Group by' upstream "
        "or use 'Polars code' for a custom sampling expression."
    ),
    "random_split": (
        "Settings panel: a 'Ratio' for the split (e.g. 0.8 for 80/20) and "
        "a seed. The node has TWO outputs in the canvas — output-0 is the "
        "kept fraction, output-1 is the rest. Worked example: 'split data "
        "for ML 80/20 train/test' → drag 'Random Split' from Machine "
        "Learning, set Ratio=0.8, seed=42, then connect output-0 to "
        "'Train Model' and output-1 to 'Evaluate Model'. Pitfall: don't "
        "connect both outputs to the same downstream node — that's a "
        "Cross join you didn't ask for."
    ),
    "unique": (
        "Settings panel: a 'Columns' list (the user picks the dedup keys) "
        "and a 'Keep strategy' (first / last / none). Worked example: "
        "'one row per customer, most recent first' → first add 'Sort data' "
        "by `created_at` descending, then 'Drop duplicates' (the palette "
        "name) with Columns=[customer_id], Keep strategy=first. Pitfall: "
        "'first' / 'last' depend on the upstream row order — sort first "
        "if you care which row survives. To *count* duplicates instead "
        "of dropping them, use 'Group by' with Action=count."
    ),
    "group_by": (
        "Settings panel: pick the 'group columns' (using the Group by "
        "button on the toolbar) then add aggregation rows in the Settings "
        "table. Each row has 'Field' (the column to aggregate), 'Action' "
        "(count, sum, mean, min, max, …), and 'Output Field Name'. Worked "
        "example: 'how many customers per city?' → drag 'Group by' from "
        "the Aggregations sidebar, mark `city` as a group column, then "
        "add a row: Field=customer_id, Action=count, Output Field Name="
        "customer_count. Pitfall: the count action still needs a Field — "
        "the value doesn't matter for count, but the panel won't validate "
        "without one. Pick any non-null column (the id is fine)."
    ),
    "window_functions": (
        "Settings panel: 'Partition by' columns (optional — leave empty for a "
        "single window over all rows), 'Order by' columns (sort key applied "
        "before windowing), and a 'Window functions' list. Each row picks a "
        "Function (rolling_mean, rolling_sum, cum_sum, rank, tile, …), a source "
        "Column, an output column name, and function-specific options (window "
        "size, rank method, number of tiles). Worked example: '7-day moving "
        "average of sales per region' → drag 'Window functions' from the "
        "Aggregations section, set Partition by=[region], Order by=[date], add a "
        "row: Function=rolling_mean, Column=sales, output=sales_7d_avg, window "
        "size=7. Pitfall 1: rolling and tile functions need an Order by column — "
        "the panel won't compute a meaningful window without one. Pitfall 2: for "
        "a single total/count per group (collapsing rows) use 'Group by' instead; "
        "window functions keep every row and just add columns."
    ),
    "pivot": (
        "Settings panel: 'Index columns' (the rows to keep as-is), a "
        "'Pivot column' (its distinct values become new columns), a "
        "'Value column' (what to put in the cells), and 'Select "
        "aggregations' for the per-cell aggregator. Worked example: "
        "'monthly revenue per region — wide format' → drag 'Pivot data' "
        "from Aggregations, set Index columns=[region], Pivot column="
        "month, Value column=revenue, aggregation=sum. Pitfall: if the "
        "Pivot column has high cardinality (> 100 distinct values), the "
        "result is unreadable — group / bin it upstream first."
    ),
    "unpivot": (
        "Settings panel: 'Index columns' (the rows to keep) and 'Value "
        "selector' (the columns to collapse into one (variable, value) "
        "pair). Worked example: 'jan/feb/mar columns are awkward, give "
        "me one row per month' → drag 'Unpivot data' from Aggregations, "
        "Index columns=[region], value columns=[jan, feb, mar]. Pitfall: "
        "the inverse of 'Pivot data'; don't use it if you only want one "
        "of the wide columns — 'Select data' to keep just that one column "
        "is simpler."
    ),
    "text_to_rows": (
        "Settings panel: 'Column to split', 'Split by a fixed value' vs "
        "'Split by a column' radios, 'Split by value' (the delimiter "
        "literal) or 'Column that contains the value to split', and an "
        "'Output column name'. Worked example: 'tags column has "
        "comma-separated values; explode them' → drag 'Text to rows' "
        "from Transformations, Column to split=tags, Split by fixed "
        "value=true, Split by value=','. Pitfall: this produces multiple "
        "*rows* per input row — to split into multiple *columns* use a "
        "'Formula' with a string-split Polars expression."
    ),
    "graph_solver": (
        "Settings panel: 'col_from' and 'col_to' to define the edge pair, "
        "and an 'Output column name' for the resulting cluster id. Worked "
        "example: 'cluster matched record pairs from a fuzzy match' → "
        "drag 'Graph solver' from Combine Operations, connect to the "
        "fuzzy match output, col_from=id_a, col_to=id_b, Output column="
        "cluster_id. Pitfall: this only computes *connected components*; "
        "for general-purpose graph traversal you'd need a 'Polars code' "
        "or 'Python Script' node."
    ),
    "python_script": (
        "Settings panel: 'Kernel' (the runtime the script executes in), "
        "'Available Inputs' (the upstream node names exposed inside the "
        "script), 'Output Names' (what the script returns), and an "
        "'Artifacts' section for files the script publishes. The script "
        "body is a code editor below. Worked example: 'enrich orders "
        "with a third-party API' → drag 'Python Script' from "
        "Transformations, write `def transform(orders): ...`, declare "
        "Output Names=enriched. Pitfall: the kernel is sandboxed — "
        "network egress is OFF by default. The user must explicitly "
        "approve network access in the kernel settings before any "
        "outbound HTTP works."
    ),
    "polars_code": (
        "Settings panel: a code editor for the Polars expression body. "
        "The upstream input is available as `input_df`; the script must "
        "assign to `output_df`. `pl` is already available — do NOT write "
        "`import polars as pl`. Worked example: 'compute a 30-day rolling "
        "average per region' → drag 'Polars code' from Transformations, "
        "write `output_df = input_df.with_columns(pl.col('amount').mean()."
        "over('region').alias('avg_30d'))`. Pitfall: the script is not "
        "Python in general — it's a Polars expression. Imperative loops "
        "don't fit; use 'Python Script' for that."
    ),
    "sql_query": (
        "Settings panel: a SQL editor under a 'SQL Query' heading. "
        "**Upstream inputs are registered POSITIONALLY as ``input_1``, "
        "``input_2``, ... — the table name is NOT the upstream node's "
        "id, type, or display name. With one upstream connected, write "
        "``FROM input_1``; with two, ``FROM input_1`` and "
        "``JOIN input_2`` (positional, in connect order).** Worked "
        "example: 'two-way join I'd rather write as SQL' → drag 'SQL "
        "Query' from Transformations, connect the orders node first "
        "and the customers node second, then write "
        "``SELECT o.*, c.name FROM input_1 o JOIN input_2 c USING "
        "(customer_id) WHERE o.amount > 100``. Pitfall: this is "
        "Polars' embedded SQL — it's read-only and not every "
        "PostgreSQL feature works. CTE / window queries that fail "
        "here usually work in 'Polars code'."
    ),
    "join": (
        "**KEY-BASED join — REQUIRES equality keys (`join_mapping`)**. "
        "If you don't have a key column to match on, you need "
        "`cross_join` instead, NOT this node. Settings panel: 'Join "
        "columns' on the left (the user adds key pairs: left column "
        "= right column — required, at least one pair) and a 'Join "
        "Type' selector with these options ONLY: left / inner / "
        "right / outer / full / semi / anti. There is NO `cross` "
        "option here — Cartesian / cross-product joins are the "
        "dedicated `cross_join` node's job. Two inputs are expected "
        "— LEFT (its rows / columns come first) and RIGHT. Worked "
        "example: 'enrich orders with customer info' → drag 'Join' "
        "from Combine Operations, connect orders to LEFT and "
        "customers to RIGHT, add Join columns row: customer_id = "
        "id, set Join Type to left. Pitfall: column data types must "
        "match — if left.customer_id is an integer but right.id is "
        "a string, the join produces no matches; cast types upstream "
        "with 'Formula'. Pitfall 2: trying to broadcast a single-row "
        "total onto every row of a larger table is NOT a job for "
        "join — there's no key to match on. Use `cross_join` for "
        "that pattern."
    ),
    "cross_join": (
        "**Cartesian / NO-KEY join — every LEFT row × every RIGHT "
        "row.** This is the ONLY way to combine two inputs without "
        "a key column. Use cases: broadcasting a single-row total "
        "onto every row of a larger table (e.g. attach the global "
        "customer count to per-city counts so a downstream Formula "
        "can compute percentages), exhaustive combinations "
        "(calendar × dimensions, scenario expansion), or any "
        "'every A with every B' pattern. Settings panel: no key "
        "columns — just left/right column selection. Two inputs: "
        "LEFT (its columns appear first in the output), RIGHT "
        "(columns appear second). Worked example: 'compute the "
        "percentage of customers per city vs the total' → group_by "
        "city → record_count (total in 1 row) → cross_join the "
        "per-city output (LEFT) with the total (RIGHT) → formula "
        "= [customer_count] / [total] * 100. DON'T use 'Join' for "
        "this — Join requires a key column you don't have. Pitfall: "
        "the output is N×M rows; for a 1k × 1k input you get 1M "
        "rows. Use 'Filter data' immediately downstream to prune "
        "to a meaningful subset, or restrict to broadcasting a "
        "single-row side."
    ),
    "fuzzy_match": (
        "Settings panel: a list of column pairs to compare (left column "
        "vs. right column) with a per-pair similarity threshold and "
        "weight. The output adds a similarity score column. Worked "
        "example: 'match customer names against a vendor list with "
        "typos' → drag 'Fuzzy match' from Combine Operations, connect "
        "customers to input-0 and vendors to input-1, add column pair: "
        "left.name vs right.company_name with a threshold. Pitfall: "
        "fuzzy matching is slow and approximate — if the keys are "
        "stable identifiers (UUIDs, ids), use 'Join' which is faster "
        "and exact."
    ),
    "record_count": (
        "Settings panel: empty — no configuration; the node always "
        "outputs a single row containing the input row count. Worked "
        "example: 'how many rows passed the filter?' → drag 'Count "
        "records' from Aggregations, connect it to the filter's "
        "output. Pitfall: don't confuse with 'Add record Id' which "
        "adds a per-row id; this collapses to one count row."
    ),
    "explore_data": (
        "**No settings — just connect it.** The node produces a wide "
        "diagnostic table with per-column statistics: type, null "
        "count, distinct count, min / max / mean. Worked example: 'I "
        "just loaded a CSV; what does the data look like?' → drag "
        "'Explore data' from Output Operations, connect to the Read "
        "node, run. The agent doesn't need to configure anything for "
        "this node — it can just announce that it was added and let "
        "you inspect the data on the canvas. Pitfall: this is "
        "diagnostic only — it doesn't transform the data and isn't "
        "typically connected to downstream transformation nodes."
    ),
    "union": (
        "Settings panel: a 'mode' selector — 'relaxed' aligns by name "
        "and fills missing columns with null; 'strict' requires identical "
        "schemas across both inputs. Worked example: 'combine 2024 and "
        "2025 data files' → drag 'Union data' from Combine Operations, "
        "connect 2024 to input-0 and 2025 to input-1, mode=relaxed. "
        "Pitfall: this stacks rows; to combine columns side-by-side use "
        "'Join'. If schemas differ unintentionally (a typo in a column "
        "name), strict mode catches it where relaxed silently nulls."
    ),
    "output": (
        "Settings panel: a 'Folder' picker, a file name, and a format "
        "(CSV / Excel / Parquet) — chosen via tabs at the top, each "
        "with format-specific options (delimiter for CSV, sheet for "
        "Excel). Worked example: 'save the result as a Parquet file' "
        "→ drag 'Write data' from Output Operations, click the Folder "
        "label to pick a directory, set name=report.parquet, choose "
        "the Parquet tab. Pitfall: 'Write data' is for local files. "
        "For S3 / GCS / Azure use 'Write to cloud provider'; for a "
        "database use 'Write to Database'."
    ),
    "api_response": (
        "Settings panel: an 'Orientation' choice (Records — a list of "
        "row objects, the default; or Columns — column-oriented arrays) "
        "and an optional 'Max rows' cap on the returned payload. Worked "
        "example: 'expose the cleaned orders as a JSON API' → drag 'API "
        "response' from Output Operations onto the final node, leave "
        "orientation=Records, optionally set Max rows=1000, then publish "
        "the flow as an endpoint. Pitfall: this is for flows served over "
        "HTTP — it does not save anything to disk. To write a file use "
        "'Write data'; to persist to a database use 'Write to Database'. "
        "In normal interactive runs it just passes its input through, so "
        "previews still work."
    ),
    "read": (
        "Settings panel: a 'File Specs' section where the user picks a "
        "file path and the format (CSV / Excel / Parquet via tabs). "
        "Each format has its own options — CSV has delimiter / "
        "encoding / has-header; Parquet is self-describing so options "
        "are minimal. Worked example: 'load customers.csv from disk' → "
        "drag 'Read data' from Input Sources, set the file path to "
        "customers.csv, leave format=csv. Pitfall: this is for local "
        "files. For cloud paths (s3://, gs://) use 'Read from cloud "
        "provider'; for databases use 'Read from Database'."
    ),
    "database_reader": (
        "Settings panel: a 'Connection' dropdown (lists previously-saved "
        "DB connections from the AI / Connections settings), then either "
        "a 'Table' picker or a custom 'Query' textarea. Worked example: "
        "'pull yesterday's orders from Postgres' → drag 'Read from "
        "Database' from Input Sources, pick the production-db "
        "connection, write a SQL query. Pitfall: don't confuse with "
        "'SQL Query' — that runs Polars SQL against upstream nodes; "
        "this issues a real network query against a remote database "
        "via the saved connection."
    ),
    "database_writer": (
        "Settings panel: 'Connection', 'Table' (target), and a 'Mode' "
        "(append / replace / create-if-missing). Worked example: 'save "
        "the cleaned orders to the warehouse' → drag 'Write to "
        "Database' from Output Operations, pick the warehouse "
        "connection, target table=clean_orders, mode=replace. Pitfall: "
        "'replace' drops the existing table — make sure the column "
        "schema upstream matches the target's expected types, casting "
        "upstream with 'Formula' or 'Select data' if needed."
    ),
    "cloud_storage_reader": (
        "Settings panel: a 'Cloud connection' dropdown (S3 / GCS / Azure), "
        "a 'Path / glob' field, and a format (CSV / Parquet / Excel / "
        "JSON). Worked example: 'load all orders/*.parquet from S3' → "
        "drag 'Read from cloud provider' from Input Sources, pick the "
        "S3 connection, path=s3://my-bucket/orders/*.parquet. Pitfall: "
        "use this rather than 'Read data' for any cloud URL — 'Read "
        "data' is local-disk only."
    ),
    "cloud_storage_writer": (
        "Settings panel: a 'Cloud connection', a target path, and a "
        "format. Worked example: 'export the report to GCS' → drag "
        "'Write to cloud provider' from Output Operations, pick the "
        "GCS connection, path=gs://my-bucket/report.parquet. Pitfall: "
        "use this rather than 'Write data' for cloud destinations — "
        "'Write data' writes to local disk."
    ),
    "catalog_reader": (
        "Settings panel: a 'Catalog' dropdown (Iceberg / Delta), a "
        "table picker (often namespaced), and an optional time-travel "
        "version selector. Worked example: 'read the latest customers "
        "snapshot from Iceberg' → drag 'Read from Catalog' from Input "
        "Sources, pick the catalog, navigate to db.customers. Pitfall: "
        "use this rather than 'Read from cloud provider' when the "
        "destination is a *catalog table* (managed metadata) instead "
        "of bare files — the catalog handles partitioning and schema "
        "evolution."
    ),
    "catalog_writer": (
        "Settings panel: a 'Catalog' dropdown, a target table, and a "
        "mode (append / overwrite / merge). Worked example: 'persist "
        "the cleaned customers to Iceberg' → drag 'Write to Catalog' "
        "from Output Operations, pick the catalog, table=db.clean_customers, "
        "mode=overwrite. Pitfall: 'merge' needs a key — define it via "
        "the upstream schema; without a key, only append / overwrite "
        "are valid."
    ),
    "kafka_source": (
        "Settings panel: a Kafka connection / broker URL, a 'Topic' "
        "name, and offset / time-range options. Worked example: 'read "
        "the last 10 minutes of events from the orders topic' → drag "
        "'Kafka Source' from Input Sources, pick the broker, topic="
        "orders, time range = last 10m. Pitfall: Flowfile flows are "
        "finite DAGs — this reads a *bounded slice*, not an unbounded "
        "stream. For continuous streaming use a different platform."
    ),
    "google_analytics_reader": (
        "Settings panel: a Google Analytics connection / property id, a "
        "Date range, and Metrics + Dimensions selectors. Worked example: "
        "'last 30 days, sessions by country' → drag 'Google Analytics' "
        "from Input Sources, pick the property, date range=30 days, "
        "metric=sessions, dimension=country. Pitfall: GA4 has hard rate "
        "limits — large date ranges with high-cardinality dimensions "
        "may be sampled."
    ),
    "external_source": (
        "Settings panel: depends on the connector — each external "
        "source plug-in defines its own settings shape. Worked example: "
        "'pull data from a custom REST API the team built' → drag "
        "'External source' from Input Sources, pick the registered "
        "connector. Pitfall: prefer a typed node ('Read from Database', "
        "'Read from cloud provider', etc.) when one exists for the "
        "source — 'External source' is the catch-all."
    ),
    "promise": (
        "Internal placeholder — a node id that exists with no settings. "
        "Not in the palette; the user shouldn't create one directly. "
        "Mention only if the user explicitly asks about an unfinished "
        "node. Suggest replacing it with a typed node from the palette."
    ),
    "user_defined": (
        "User-defined node (UDF) — runs custom code registered in the "
        "workspace's node store. The palette label varies per-UDF (it's "
        "registered by the team). Worked example: 'use the customer-"
        "scoring UDF the team registered' → look for the named UDF in "
        "the User Defined Operations sidebar section. Pitfall: this "
        "generic 'user_defined' entry is the placeholder; the actual "
        "UDF appears under its registered name."
    ),
    "train_model": (
        "Settings panel: a 'Target column' (the y), a 'Model type' "
        "(regression / classification), a 'Features' multi-select, and "
        "model-specific hyperparameters. Worked example: 'train a "
        "linear regression to predict sales from features' → drag "
        "'Train Model' from Machine Learning, target=sales, features=["
        "price, region, season]. Pitfall: split your data first with "
        "'Random Split' so you can evaluate on held-out data via "
        "'Evaluate Model' downstream."
    ),
    "apply_model": (
        "Settings panel: a 'Model' selector (the upstream Train Model "
        "node, or a saved model from the catalog) and a 'Predictions "
        "column' name. Worked example: 'score next quarter's data' → "
        "drag 'Apply Model' from Machine Learning, connect the trained "
        "model and the new data, predictions column=predicted_sales. "
        "Pitfall: the new data must have the same feature columns the "
        "model was trained on — no missing columns, same data types."
    ),
    "evaluate_model": (
        "Settings panel: an 'Actual column' and a 'Predicted column'. "
        "Outputs metrics (R² / MAE / accuracy / F1, depending on model "
        "type). Worked example: 'how good is the predictor on test "
        "data?' → connect the test split (Random Split output-1) to a "
        "downstream Apply Model, then to Evaluate Model with "
        "actual=sales, predicted=predicted_sales. Pitfall: the data "
        "must include both the true value AND the prediction — apply "
        "the model first, then evaluate."
    ),
    "wait_for": (
        "Settings panel: empty — no configuration; the node passes "
        "input-0 through unchanged and uses input-1 only as a "
        "synchronisation barrier. Worked example: 'make sure the "
        "warehouse write finishes before reading from it' → drag 'Wait "
        "For' from Combine Operations, connect the read query to "
        "input-0 and the write node's success signal to input-1. "
        "Pitfall: this is for ordering side effects, not data — "
        "input-1's data is discarded; only its completion is observed."
    ),
}


# --------------------------------------------------------------------------- #
# Agent payload examples                                                       #
# --------------------------------------------------------------------------- #
#
# Only for node types whose Pydantic settings shape diverges from what an LLM
# would naively guess from the field names. The agent prompt injects this
# JSON verbatim under the per-tool catalog entry so the model copies the
# correct shape rather than re-deriving it from the JSON Schema each time.
#
# Each entry MUST validate via the corresponding ``NodeXxx.model_validate``
# (the CI test ``test_w56v2_agent_payload_examples_validate`` enforces this).
# If a Pydantic field is renamed or the nested shape changes, the test fails
# before any user hits the agent retry loop.
#
# Why these seven specifically:
#   - group_by  — agg_cols list mixes group-keys (agg="groupby") and
#                 aggregations; the natural LLM guess is two separate lists.
#   - pivot     — index_columns / pivot_column / value_col / aggregations is
#                 a four-key shape, easy to flatten incorrectly.
#   - join      — left_select / right_select are nested dicts wrapping
#                 ``renames``; the natural guess is column name lists.
#   - fuzzy_match — same as join + extra threshold_score / fuzzy_type per pair.
#   - select    — keep/drop/rename in a single list, not three lists.
#   - unpivot   — index_columns vs value_columns naming.
#   - text_to_rows — split_by_fixed_value / split_fixed_value (not "split_by"
#                 or "delimiter" as the LLM would guess).
#
# Simple nodes (filter, sort, formula, unique, sample, …) don't get an example
# because the JSON Schema alone is unambiguous. Adding one would burn tokens
# without improving accuracy.
NODE_AGENT_PAYLOAD_EXAMPLES: Final[dict[str, str]] = {
    # explore_data takes NO settings; the planner injects flow_id /
    # node_id / upstream and the LLM emits an empty inner object.
    # Listed so ``_trim_example_to_inner_shape`` produces a bare ``{}``
    # at fill_settings, signalling unambiguously that nothing needs
    # filling. The LLM is encouraged to announce it was added rather
    # than fabricate config.
    "explore_data": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "graphic_walker_input": {}\n'
        "}"
    ),
    "group_by": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "groupby_input": {\n'
        '    "agg_cols": [\n'
        '      {"old_name": "city", "agg": "groupby"},\n'
        '      {"old_name": "customer_id", "agg": "count", "new_name": "customer_count"}\n'
        "    ]\n"
        "  }\n"
        "}"
    ),
    "pivot": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "pivot_input": {\n'
        '    "index_columns": ["region"],\n'
        '    "pivot_column": "month",\n'
        '    "value_col": "revenue",\n'
        '    "aggregations": ["sum"]\n'
        "  }\n"
        "}"
    ),
    "join": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "join_input": {\n'
        '    "join_mapping": [{"left_col": "customer_id", "right_col": "id"}],\n'
        '    "left_select": {"renames": [\n'
        '      {"old_name": "order_id"}, {"old_name": "amount"}, {"old_name": "customer_id"}\n'
        "    ]},\n"
        '    "right_select": {"renames": [\n'
        '      {"old_name": "id"}, {"old_name": "name"}\n'
        "    ]},\n"
        '    "how": "left"\n'
        "  }\n"
        "}"
    ),
    "fuzzy_match": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "join_input": {\n'
        '    "join_mapping": [\n'
        '      {"left_col": "name", "right_col": "company_name", '
        '"threshold_score": 80.0, "fuzzy_type": "levenshtein"}\n'
        "    ],\n"
        '    "left_select": {"renames": [{"old_name": "name"}]},\n'
        '    "right_select": {"renames": [{"old_name": "company_name"}]},\n'
        '    "how": "left"\n'
        "  }\n"
        "}"
    ),
    "select": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "select_input": [\n'
        '    {"old_name": "id", "new_name": "user_id", "keep": true},\n'
        '    {"old_name": "email", "keep": false}\n'
        "  ]\n"
        "}"
    ),
    "unpivot": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "unpivot_input": {\n'
        '    "index_columns": ["region"],\n'
        '    "value_columns": ["jan", "feb", "mar"]\n'
        "  }\n"
        "}"
    ),
    "text_to_rows": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "text_to_rows_input": {\n'
        '    "column_to_split": "tags",\n'
        '    "split_by_fixed_value": true,\n'
        '    "split_fixed_value": ","\n'
        "  }\n"
        "}"
    ),
    # formula uses Flowfile expression language (SQL-style
    # ``[column_name]`` references), NOT raw Polars. The Pydantic
    # shape is small (one ``function`` field with a name + the
    # expression string) but the SYNTAX is the divergence the LLM
    # trips on. The full canonical function list is appended at
    # stage-3 fill_settings
    # via ``_build_single_node_block`` ONLY when ``picked_node_type ==
    # "formula"`` so the catalog stays cheap.
    "formula": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "function": {\n'
        '    "field": {"name": "full_name", "data_type": "String"},\n'
        '    "function": "[first] + \' \' + [last]"\n'
        "  }\n"
        "}"
    ),
    # RawData.data is COLUMNAR — data[i] is the values for columns[i].
    # The LLM defaults to row-oriented and silently corrupts alignment
    # because both layouts validate as list[list]. Two rows of
    # {name, age}: data=[["Alice","Bob"],[30,25]] not
    # [["Alice",30],["Bob",25]].
    "manual_input": (
        "{\n"
        '  "flow_id": 1,\n'
        '  "node_id": 99,\n'
        '  "raw_data_format": {\n'
        '    "columns": [\n'
        '      {"name": "name", "data_type": "String"},\n'
        '      {"name": "age", "data_type": "Int64"}\n'
        "    ],\n"
        '    "data": [\n'
        '      ["Alice", "Bob"],\n'
        "      [30, 25]\n"
        "    ]\n"
        "  }\n"
        "}"
    ),
}


__all__ = [
    "NODE_LONG_DESCRIPTIONS",
    "NODE_USER_INSTRUCTIONS",
    "NODE_GROUP_TO_SIDEBAR_LABEL",
    "NODE_AGENT_PAYLOAD_EXAMPLES",
    "palette_label_for",
    "sidebar_section_for",
]
