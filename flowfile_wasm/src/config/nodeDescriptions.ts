// Node descriptions for settings panel titles
export interface NodeDescription {
  title: string
  intro: string
}

export const nodeDescriptions: Record<string, NodeDescription> = {
  read: {
    title: 'Read CSV',
    intro: 'Load data from a CSV file. Drag and drop a file or paste CSV content directly.'
  },
  manual_input: {
    title: 'Manual Input',
    intro: 'Enter data manually in CSV format. Great for creating small datasets or test data.'
  },
  external_data: {
    title: 'External Data',
    intro: 'Load data provided by the host application. Select an available dataset from the dropdown.'
  },
  filter: {
    title: 'Filter',
    intro: 'Filter rows based on conditions. Use basic mode for simple filters or advanced mode for custom Polars expressions.'
  },
  select: {
    title: 'Select',
    intro: 'Choose which columns to keep, rename columns, or reorder them. Drag to reorder, click to toggle visibility.'
  },
  sort: {
    title: 'Sort',
    intro: 'Sort your data by one or more columns. Choose ascending or descending order for each column.'
  },
  group_by: {
    title: 'Group By',
    intro: 'Group rows by columns and calculate aggregations like sum, count, mean, min, max, and more.'
  },
  join: {
    title: 'Join',
    intro: 'Combine two datasets based on matching column values. Supports inner, left, right, full, semi, and anti joins.'
  },
  unique: {
    title: 'Unique',
    intro: 'Remove duplicate rows from your data. Optionally specify which columns to consider for uniqueness.'
  },
  head: {
    title: 'Take Sample',
    intro: 'Limit the number of rows in your dataset. Useful for previewing large datasets or taking samples.'
  },
  polars_code: {
    title: 'Polars Code',
    intro: 'Write custom Polars code for advanced transformations. Access input_df and return a DataFrame. Supports full Python/Polars syntax.'
  },
  explore_data: {
    title: 'Preview',
    intro: 'Preview your data at this point in the flow. Does not modify the data.'
  },
  pivot: {
    title: 'Pivot',
    intro: 'Transform data from long to wide format. Values in the pivot column become new column headers, with aggregated values.'
  },
  unpivot: {
    title: 'Unpivot',
    intro: 'Transform data from wide to long format. Multiple columns are melted into variable/value pairs (also known as melt or gather).'
  },
  output: {
    title: 'Write Data',
    intro: 'Export your data as CSV. Run the flow to prepare the data, then download the file to your computer.'
  },
  external_output: {
    title: 'External Output',
    intro: 'Send the result data back to the host application via the output callback. Use this to return processed data from the embedded editor.'
  }
}

export function getNodeDescription(type: string): NodeDescription {
  return nodeDescriptions[type] || { title: type, intro: '' }
}
