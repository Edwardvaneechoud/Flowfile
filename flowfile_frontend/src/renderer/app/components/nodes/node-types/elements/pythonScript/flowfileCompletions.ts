export const flowfileCompletionVals = [
  // ff_kernel module
  {
    label: "ff_kernel",
    type: "variable",
    info: "FlowFile kernel API for data I/O and artifacts",
  },

  // Data I/O functions
  {
    label: "read_input",
    type: "function",
    info: "Read input DataFrame. Optional name parameter for named inputs.",
    detail: "ff_kernel.read_input(name?)",
    apply: "read_input()",
  },
  {
    label: "read_inputs",
    type: "function",
    info: "Read all inputs as a dict of LazyFrame lists (one per connection).",
    detail: "ff_kernel.read_inputs() -> dict[str, list[LazyFrame]]",
    apply: "read_inputs()",
  },
  {
    label: "publish_output",
    type: "function",
    info: "Write output DataFrame. Optional name parameter for named outputs.",
    detail: "ff_kernel.publish_output(df, name?)",
    apply: "publish_output(df)",
  },

  // Display function
  {
    label: "display",
    type: "function",
    info: "Display a rich object (matplotlib figure, plotly figure, PIL image, HTML string) in the output panel.",
    detail: "ff_kernel.display(obj, title?)",
    apply: "display(obj)",
  },

  // Artifact functions
  {
    label: "publish_artifact",
    type: "function",
    info: "Store a Python object as a named artifact in kernel memory.",
    detail: 'ff_kernel.publish_artifact("name", obj)',
    apply: 'publish_artifact("name", obj)',
  },
  {
    label: "read_artifact",
    type: "function",
    info: "Retrieve a Python object from a named artifact.",
    detail: 'ff_kernel.read_artifact("name")',
    apply: 'read_artifact("name")',
  },
  {
    label: "delete_artifact",
    type: "function",
    info: "Remove a named artifact from kernel memory.",
    detail: 'ff_kernel.delete_artifact("name")',
    apply: 'delete_artifact("name")',
  },
  {
    label: "list_artifacts",
    type: "function",
    info: "List all artifacts available in the kernel.",
    detail: "ff_kernel.list_artifacts()",
    apply: "list_artifacts()",
  },

  // Global Artifact functions
  {
    label: "publish_global",
    type: "function",
    info: "Persist a Python object to the global artifact store (survives across sessions).",
    detail: 'ff_kernel.publish_global("name", obj, description?, tags?, namespace_id?, fmt?)',
    apply: 'publish_global("name", obj)',
  },
  {
    label: "get_global",
    type: "function",
    info: "Retrieve a Python object from the global artifact store.",
    detail: 'ff_kernel.get_global("name", version?, namespace_id?)',
    apply: 'get_global("name")',
  },
  {
    label: "list_global_artifacts",
    type: "function",
    info: "List available global artifacts with optional namespace/tag filters.",
    detail: "ff_kernel.list_global_artifacts(namespace_id?, tags?)",
    apply: "list_global_artifacts()",
  },
  {
    label: "delete_global_artifact",
    type: "function",
    info: "Delete a global artifact by name, optionally a specific version.",
    detail: 'ff_kernel.delete_global_artifact("name", version?, namespace_id?)',
    apply: 'delete_global_artifact("name")',
  },

  // Logging functions
  {
    label: "log",
    type: "function",
    info: "Send a log message to the FlowFile log viewer.",
    detail: 'ff_kernel.log("message", level?)',
    apply: 'log("message")',
  },
  {
    label: "log_info",
    type: "function",
    info: "Send an INFO log message to the FlowFile log viewer.",
    detail: 'ff_kernel.log_info("message")',
    apply: 'log_info("message")',
  },
  {
    label: "log_warning",
    type: "function",
    info: "Send a WARNING log message to the FlowFile log viewer.",
    detail: 'ff_kernel.log_warning("message")',
    apply: 'log_warning("message")',
  },
  {
    label: "log_error",
    type: "function",
    info: "Send an ERROR log message to the FlowFile log viewer.",
    detail: 'ff_kernel.log_error("message")',
    apply: 'log_error("message")',
  },

  // Polars basics (also useful in python_script context)
  { label: "pl", type: "variable", info: "Polars main module" },
  { label: "col", type: "function", info: "Polars column selector" },
  { label: "lit", type: "function", info: "Polars literal value" },

  // Common Polars operations
  { label: "select", type: "method", info: "Select columns" },
  { label: "filter", type: "method", info: "Filter rows" },
  { label: "group_by", type: "method", info: "Group by columns" },
  { label: "with_columns", type: "method", info: "Add/modify columns" },
  { label: "join", type: "method", info: "Join operations" },
  { label: "sort", type: "method", info: "Sort DataFrame" },
  { label: "collect", type: "method", info: "Collect LazyFrame to DataFrame" },

  // Basic Python
  { label: "print", type: "function" },
  { label: "len", type: "function" },
  { label: "range", type: "function" },
  { label: "import", type: "keyword" },
];
