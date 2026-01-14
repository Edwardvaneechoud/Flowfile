// Polars code editor autocompletion values
export const polarsCompletionVals = [
  // Polars basics
  { label: 'pl', type: 'variable', info: 'Polars main module' },
  { label: 'pl.col', type: 'function', info: 'Column selector - pl.col("name")' },
  { label: 'pl.lit', type: 'function', info: 'Literal value - pl.lit(value)' },
  { label: 'pl.when', type: 'function', info: 'Conditional expression - pl.when(condition).then(value)' },
  { label: 'pl.concat', type: 'function', info: 'Concatenate DataFrames' },
  { label: 'pl.DataFrame', type: 'class', info: 'Create a DataFrame' },

  // Common DataFrame methods
  { label: 'select', type: 'method', info: 'Select columns - df.select([cols])' },
  { label: 'filter', type: 'method', info: 'Filter rows - df.filter(condition)' },
  { label: 'group_by', type: 'method', info: 'Group by columns - df.group_by(cols)' },
  { label: 'agg', type: 'method', info: 'Aggregate after group_by' },
  { label: 'sort', type: 'method', info: 'Sort DataFrame - df.sort(col)' },
  { label: 'with_columns', type: 'method', info: 'Add/modify columns' },
  { label: 'join', type: 'method', info: 'Join DataFrames' },
  { label: 'drop', type: 'method', info: 'Drop columns' },
  { label: 'rename', type: 'method', info: 'Rename columns' },
  { label: 'head', type: 'method', info: 'First n rows' },
  { label: 'tail', type: 'method', info: 'Last n rows' },
  { label: 'unique', type: 'method', info: 'Remove duplicates' },
  { label: 'drop_nulls', type: 'method', info: 'Remove null values' },
  { label: 'fill_null', type: 'method', info: 'Fill null values' },
  { label: 'cast', type: 'method', info: 'Cast column type' },
  { label: 'alias', type: 'method', info: 'Rename expression result' },

  // Aggregation functions
  { label: 'sum', type: 'method', info: 'Sum values' },
  { label: 'mean', type: 'method', info: 'Calculate mean' },
  { label: 'median', type: 'method', info: 'Calculate median' },
  { label: 'min', type: 'method', info: 'Find minimum' },
  { label: 'max', type: 'method', info: 'Find maximum' },
  { label: 'count', type: 'method', info: 'Count records' },
  { label: 'n_unique', type: 'method', info: 'Count unique values' },
  { label: 'first', type: 'method', info: 'First value' },
  { label: 'last', type: 'method', info: 'Last value' },
  { label: 'std', type: 'method', info: 'Standard deviation' },
  { label: 'var', type: 'method', info: 'Variance' },

  // String operations
  { label: 'str.contains', type: 'method', info: 'Check if string contains pattern' },
  { label: 'str.starts_with', type: 'method', info: 'Check if string starts with' },
  { label: 'str.ends_with', type: 'method', info: 'Check if string ends with' },
  { label: 'str.to_lowercase', type: 'method', info: 'Convert to lowercase' },
  { label: 'str.to_uppercase', type: 'method', info: 'Convert to uppercase' },
  { label: 'str.strip', type: 'method', info: 'Strip whitespace' },
  { label: 'str.replace', type: 'method', info: 'Replace string pattern' },
  { label: 'str.split', type: 'method', info: 'Split string' },
  { label: 'str.len_chars', type: 'method', info: 'String length' },

  // Date/time operations
  { label: 'dt.year', type: 'method', info: 'Extract year' },
  { label: 'dt.month', type: 'method', info: 'Extract month' },
  { label: 'dt.day', type: 'method', info: 'Extract day' },
  { label: 'dt.hour', type: 'method', info: 'Extract hour' },
  { label: 'dt.minute', type: 'method', info: 'Extract minute' },
  { label: 'dt.weekday', type: 'method', info: 'Day of week (0=Monday)' },

  // Comparison operators
  { label: 'is_null', type: 'method', info: 'Check if null' },
  { label: 'is_not_null', type: 'method', info: 'Check if not null' },
  { label: 'is_in', type: 'method', info: 'Check if value in list' },
  { label: 'is_between', type: 'method', info: 'Check if between values' },

  // Common variables
  { label: 'input_df', type: 'variable', info: 'Input DataFrame (read-only)' },
  { label: 'output_df', type: 'variable', info: 'Assign result to this variable' },
  { label: 'result', type: 'variable', info: 'Alternative result variable' },
  { label: 'df', type: 'variable', info: 'Alternative result variable' },

  // Data types
  { label: 'pl.Utf8', type: 'type', info: 'String type' },
  { label: 'pl.Int64', type: 'type', info: '64-bit integer' },
  { label: 'pl.Float64', type: 'type', info: '64-bit float' },
  { label: 'pl.Boolean', type: 'type', info: 'Boolean type' },
  { label: 'pl.Date', type: 'type', info: 'Date type' },
  { label: 'pl.Datetime', type: 'type', info: 'Datetime type' },

  // Basic Python
  { label: 'print', type: 'function', info: 'Print to console' },
  { label: 'len', type: 'function', info: 'Get length' },
  { label: 'range', type: 'function', info: 'Generate range' },
  { label: 'list', type: 'type', info: 'List type' },
  { label: 'dict', type: 'type', info: 'Dictionary type' },
  { label: 'str', type: 'type', info: 'String type' },
  { label: 'int', type: 'type', info: 'Integer type' },
  { label: 'float', type: 'type', info: 'Float type' },
  { label: 'True', type: 'keyword', info: 'Boolean true' },
  { label: 'False', type: 'keyword', info: 'Boolean false' },
  { label: 'None', type: 'keyword', info: 'Null value' },
]
