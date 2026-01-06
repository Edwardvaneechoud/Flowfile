export const polarsCompletionVals = [
  // Polars basics
  { label: "pl", type: "variable", info: "Polars main module" },
  { label: "col", type: "function", info: "Column selector" },
  { label: "lit", type: "function", info: "Literal value" },
  { label: "expr", type: "function", info: "Expression builder" },

  // Common Polars operations
  { label: "select", type: "method", info: "Select columns" },
  { label: "filter", type: "method", info: "Filter rows" },
  { label: "group_by", type: "method", info: "Group by columns" },
  { label: "agg", type: "method", info: "Aggregate operations" },
  { label: "sort", type: "method", info: "Sort DataFrame" },
  { label: "with_columns", type: "method", info: "Add/modify columns" },
  { label: "join", type: "method", info: "Join operations" },

  // Aggregation functions
  { label: "sum", type: "method", info: "Sum values" },
  { label: "mean", type: "method", info: "Calculate mean" },
  { label: "min", type: "method", info: "Find minimum" },
  { label: "max", type: "method", info: "Find maximum" },
  { label: "count", type: "method", info: "Count records" },

  // Common variables
  { label: "input_df", type: "variable", info: "Input DataFrame" },
  { label: "output_df", type: "variable", info: "Output DataFrame" },

  // Basic Python
  { label: "print", type: "function" },
  { label: "len", type: "function" },
  { label: "range", type: "function" },
  { label: "list", type: "type" },
  { label: "dict", type: "type" },
  { label: "set", type: "type" },
  { label: "str", type: "type" },
  { label: "int", type: "type" },
  { label: "float", type: "type" },
  { label: "bool", type: "type" },
];
