# Export Flows to YAML

Flowfile allows you to save your visual data pipelines as human-readable YAML files. This makes your flows portable, version-controllable, and easy to share with your team.

## Key Benefits

* **Human-Readable**: YAML format is easy to read and understand, making it simple to review pipeline changes
* **Version Control Friendly**: Track changes to your flows in Git with meaningful diffs
* **Portable**: Share flows between team members or environments without compatibility issues
* **Lossless Round-Trip**: Save and reload flows without losing any configuration
* **JSON Support**: Alternatively export to JSON if you prefer that format

## Saving a Flow

### From the Visual Editor

In the visual editor, use the save functionality to export your flow. When saving, choose a filename with the `.yaml` or `.yml` extension:

- `my_pipeline.yaml` - Saves as YAML format
- `my_pipeline.json` - Saves as JSON format

### From Python Code

You can also save flows programmatically using the Python API:

```python
from flowfile_core.flowfile.flow_graph import FlowGraph

# After building your flow...
graph.save_flow("my_pipeline.yaml")

# Or save as JSON
graph.save_flow("my_pipeline.json")
```

## YAML File Structure

When you export a flow, Flowfile creates a structured YAML file containing all the information needed to recreate your pipeline.

### Basic Structure

```yaml
flowfile_version: "0.6.0"
flowfile_id: 12345
flowfile_name: my_pipeline
flowfile_settings:
  description: My data transformation pipeline
  execution_mode: Performance
  execution_location: local
  auto_save: false
  show_detailed_progress: true
nodes:
  - id: 1
    type: manual_input
    is_start_node: true
    x_position: 100
    y_position: 200
    setting_input:
      # Node-specific configuration
```

### Flow Settings

The `flowfile_settings` section contains flow-level configuration:

| Setting | Description | Values |
|---------|-------------|--------|
| `description` | Optional description of the flow | Any text |
| `execution_mode` | Optimization mode | `Development` or `Performance` |
| `execution_location` | Where to run the flow | `local` or `remote` |
| `auto_save` | Automatically save changes | `true` or `false` |
| `show_detailed_progress` | Show detailed execution progress | `true` or `false` |

### Node Structure

Each node in your flow is represented with its configuration:

```yaml
nodes:
  - id: 1
    type: manual_input
    is_start_node: true
    description: Sample customer data
    x_position: 100
    y_position: 200
    input_ids: []
    outputs: [2]
    setting_input:
      raw_data_format:
        columns:
          - name: customer_id
            data:
              - "C001"
              - "C002"
          - name: name
            data:
              - "Alice"
              - "Bob"
```

## Examples

### Example 1: Simple Select Pipeline

A pipeline that reads data and selects/renames columns:

<details markdown="1">
<summary>YAML Output</summary>

```yaml
flowfile_version: "0.6.0"
flowfile_id: 100
flowfile_name: select_example
flowfile_settings:
  execution_mode: Development
nodes:
  - id: 1
    type: manual_input
    is_start_node: true
    x_position: 100
    y_position: 200
    outputs: [2]
    setting_input:
      raw_data_format:
        columns:
          - name: customer_id
            data: ["C001", "C002", "C003"]
          - name: full_name
            data: ["Alice", "Bob", "Charlie"]
          - name: city
            data: ["NYC", "LA", "Chicago"]

  - id: 2
    type: select
    is_start_node: false
    x_position: 300
    y_position: 200
    input_ids: [1]
    setting_input:
      select_input:
        - old_name: customer_id
          new_name: id
        - old_name: full_name
          new_name: name
        # city is omitted (not selected)
```

</details>

### Example 2: Filter and Group By Pipeline

A pipeline that filters data and performs aggregation:

<details markdown="1">
<summary>YAML Output</summary>

```yaml
flowfile_version: "0.6.0"
flowfile_id: 101
flowfile_name: filter_groupby_example
flowfile_settings:
  execution_mode: Performance
nodes:
  - id: 1
    type: manual_input
    is_start_node: true
    x_position: 100
    y_position: 200
    outputs: [2]
    setting_input:
      raw_data_format:
        columns:
          - name: product
            data: ["Apple", "Banana", "Orange", "Apple"]
          - name: price
            data: [1.50, 0.75, 2.00, 1.50]
          - name: quantity
            data: [100, 150, 80, 120]

  - id: 2
    type: filter
    is_start_node: false
    x_position: 300
    y_position: 200
    input_ids: [1]
    outputs: [3]
    setting_input:
      filter_input:
        filter_type: basic
        basic_filter:
          field: price
          filter_type: ">"
          filter_value: "1.0"

  - id: 3
    type: group_by
    is_start_node: false
    x_position: 500
    y_position: 200
    input_ids: [2]
    setting_input:
      groupby_input:
        agg_cols:
          - column: product
            agg: groupby
          - column: quantity
            agg: sum
            new_name: total_quantity
```

</details>

### Example 3: Join Pipeline

A pipeline that joins two data sources:

<details markdown="1">
<summary>YAML Output</summary>

```yaml
flowfile_version: "0.6.0"
flowfile_id: 103
flowfile_name: join_example
flowfile_settings:
  execution_mode: Performance
nodes:
  - id: 1
    type: manual_input
    is_start_node: true
    x_position: 100
    y_position: 100
    outputs: [3]
    setting_input:
      raw_data_format:
        columns:
          - name: id
            data: [1, 2, 3]
          - name: name
            data: ["Alice", "Bob", "Charlie"]

  - id: 2
    type: manual_input
    is_start_node: true
    x_position: 100
    y_position: 300
    outputs: [3]
    setting_input:
      raw_data_format:
        columns:
          - name: id
            data: [1, 2, 4]
          - name: department
            data: ["Sales", "Engineering", "Marketing"]

  - id: 3
    type: join
    is_start_node: false
    x_position: 400
    y_position: 200
    left_input_id: 1
    right_input_id: 2
    setting_input:
      join_input:
        how: inner
        join_mapping:
          - left_col: id
            right_col: id
        left_select:
          select:
            - old_name: id
            - old_name: name
        right_select:
          select:
            - old_name: department
```

</details>

## Loading Saved Flows

### From the Visual Editor

Open a saved YAML or JSON file directly in the visual editor. The flow will be fully restored with all nodes, connections, and configurations.

### From Python Code

```python
from flowfile_core.flowfile.manage.io_flowfile import open_flow

# Load a YAML flow
flow = open_flow("my_pipeline.yaml")

# Or load a JSON flow
flow = open_flow("my_pipeline.json")

# Execute the loaded flow
result = flow.run_graph()
```

## Supported Node Types

The YAML export supports all Flowfile node types:

| Category | Node Types |
|----------|------------|
| **Input** | `manual_input`, `read`, `database_reader`, `cloud_storage_reader` |
| **Transform** | `filter`, `formula`, `select`, `sort`, `record_id`, `sample`, `unique`, `text_to_rows` |
| **Combine** | `join`, `cross_join`, `fuzzy_match`, `union` |
| **Aggregate** | `group_by`, `pivot`, `unpivot` |
| **Output** | `output`, `database_writer`, `cloud_storage_writer` |
| **Advanced** | `polars_code`, `graph_solver`, `user_defined` |

## Best Practices

1. **Use Descriptive Names**: Give your flows meaningful names that describe their purpose
2. **Add Descriptions**: Use the `description` field to document what your flow does
3. **Version Control**: Commit your YAML files to Git to track changes over time
4. **Organize by Project**: Keep related flows in the same directory structure
5. **Review Before Sharing**: Check your YAML files for sensitive data like credentials before sharing

## Migrating from Legacy Format

If you have flows saved in the legacy `.flowfile` format, you can convert them to YAML:

```python
from flowfile_core.flowfile.manage.io_flowfile import open_flow

# Load legacy format
flow = open_flow("old_pipeline.flowfile")

# Save as YAML
flow.save_flow("new_pipeline.yaml")
```

!!! note "Legacy Format Deprecated"
    The `.flowfile` format is deprecated starting from version 0.5.0. Please migrate your flows to YAML or JSON format for continued support.

## Troubleshooting

### Flow Won't Load

- Verify the YAML syntax is valid (use a YAML validator)
- Check that `flowfile_version` matches your Flowfile version
- Ensure all referenced node types are available

### Missing Connections After Load

- Connections are derived from `input_ids`, `outputs`, `left_input_id`, and `right_input_id` fields
- Make sure these fields correctly reference existing node IDs

### Settings Not Preserved

- Some internal/computed fields are excluded from YAML export
- These are automatically reconstructed when loading the flow
