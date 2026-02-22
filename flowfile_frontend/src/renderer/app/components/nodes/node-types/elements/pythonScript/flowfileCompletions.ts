export const flowfileCompletionVals = [
  // flowfile module
  {
    label: "flowfile",
    type: "variable",
    info: "FlowFile API module for data I/O and artifacts",
  },

  // Data I/O functions
  {
    label: "read_input",
    type: "function",
    info: 'Read input DataFrame by name. Use the source node\'s reference name, e.g. read_input("orders"). Defaults to "main" or auto-resolves when there is exactly one input.',
    detail: 'flowfile.read_input(name="main")',
    apply: "read_input()",
  },
  {
    label: "read_first",
    type: "function",
    info: "Read only the first input file for a named input (convenience shortcut).",
    detail: 'flowfile.read_first(name="main")',
    apply: "read_first()",
  },
  {
    label: "read_inputs",
    type: "function",
    info: "Read all named inputs as a dict of LazyFrame lists. Each key is the source node reference.",
    detail: "flowfile.read_inputs() -> dict[str, list[LazyFrame]]",
    apply: "read_inputs()",
  },
  {
    label: "publish_output",
    type: "function",
    info: 'Write output DataFrame. Use name to publish to a specific output handle, e.g. publish_output(df, "valid"). Configure output names in the Outputs section.',
    detail: 'flowfile.publish_output(df, name="main")',
    apply: "publish_output(df)",
  },

  // Display function
  {
    label: "display",
    type: "function",
    info: "Display a rich object (matplotlib figure, plotly figure, PIL image, HTML string) in the output panel.",
    detail: "flowfile.display(obj, title?)",
    apply: "display(obj)",
  },

  // Artifact functions
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
    info: "List all artifacts available in the kernel. Returns list[ArtifactInfo] with .name, .type_name, .module, .node_id, .flow_id, .created_at, .size_bytes, .persisted fields.",
    detail: "flowfile.list_artifacts() -> list[ArtifactInfo]",
    apply: "list_artifacts()",
  },

  // Global Artifact functions
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
    info: "List available global artifacts with optional namespace/tag filters. Returns list[GlobalArtifactInfo] with .id, .name, .version, .status, .python_type, .size_bytes, .created_at, .tags, .owner_id fields.",
    detail: "flowfile.list_global_artifacts(namespace_id?, tags?) -> list[GlobalArtifactInfo]",
    apply: "list_global_artifacts()",
  },
  {label: "get_shared_location",
    type: "function",
    info: "Get the shared location to make objects available to other processes",
    detail: "flowfile.get_shared_location()->str",
    apply: "get_shared_location()",
  },
  {
    label: "delete_global_artifact",
    type: "function",
    info: "Delete a global artifact by name, optionally a specific version.",
    detail: 'flowfile.delete_global_artifact("name", version?, namespace_id?)',
    apply: 'delete_global_artifact("name")',
  },

  // Logging functions
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
