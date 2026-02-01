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
    info: "Read input DataFrame. Optional name parameter for named inputs.",
    detail: "flowfile.read_input(name?)",
    apply: "read_input()",
  },
  {
    label: "read_inputs",
    type: "function",
    info: "Read all inputs as a dict of DataFrames.",
    detail: "flowfile.read_inputs()",
    apply: "read_inputs()",
  },
  {
    label: "publish_output",
    type: "function",
    info: "Write output DataFrame. Optional name parameter for named outputs.",
    detail: "flowfile.publish_output(df, name?)",
    apply: "publish_output(df)",
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
    info: "List all artifacts available in the kernel.",
    detail: "flowfile.list_artifacts()",
    apply: "list_artifacts()",
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
