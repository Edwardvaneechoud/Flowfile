# Node Designer

The Node Designer is a visual interface for creating user-defined nodes without writing Python files manually.

## Accessing the Node Designer

Open the Node Designer from the sidebar menu. Click the "Node Designer" option to launch the editor.

## Interface Layout

The Node Designer uses a three-panel layout:

### Left Panel: Component Palette

The left panel contains draggable UI components. Drag components onto the canvas to add them to your node's settings interface.

Available components:

| Component | Description |
|-----------|-------------|
| **Section** | Groups related components together with a title |
| **TextInput** | Single-line text field for string values |
| **NumberInput** | Numeric input with optional min/max validation |
| **Checkbox** | Boolean toggle for on/off settings |
| **Dropdown** | Single-select dropdown list |
| **ColumnSelector** | Picker for selecting input columns with type filtering |
| **MultiColumnSelector** | Select multiple columns at once |
| **SecretSelector** | Access stored secrets (API keys, credentials) |

### Center Panel: Canvas

The canvas displays your node's settings structure. Components are organized within sections. Drag components to reorder them or move them between sections.

The canvas shows a preview of how your node's settings panel will appear in the visual editor.

### Right Panel: Property Editor

When you select a component on the canvas, the right panel shows its configurable properties:

- **Label** - Display name shown in the UI
- **Name** - Internal identifier used in code
- **Default value** - Pre-filled value
- **Required** - Whether the field must be filled
- **Placeholder** - Hint text for empty fields
- Component-specific options (min/max for numbers, options for dropdowns, etc.)

## Process Code Editor

The bottom section contains a code editor for writing your node's processing logic. This is where you define the `process(self, *dfs)` function that transforms input data.

```python
def process(self, *dfs):
    # Access the first input dataframe
    df = dfs[0]

    # Get values from your UI components
    column = self.settings_schema.main_config.column_selector.value

    # Apply your transformation
    result = df.with_columns([
        pl.col(column).str.to_uppercase().alias("upper_" + column)
    ])

    return result
```

The editor provides:

- Syntax highlighting for Python
- Access to component values via `self.settings_schema`
- Standard Polars imports available

## Toolbar Actions

The toolbar at the top provides these actions:

| Action | Description |
|--------|-------------|
| **New** | Create a blank node definition |
| **Save** | Save the current node to your user-defined nodes directory |
| **Load** | Open an existing node definition for editing |
| **Validate** | Check for configuration errors before saving |
| **Preview Code** | View the generated Python code |

## Custom Icons

You can upload a custom icon for your node:

1. Click the icon placeholder in the node metadata section
2. Select a PNG or SVG file (recommended size: 48x48 pixels)
3. The icon appears in the node palette and on the canvas

## Creating a Node

1. Open the Node Designer
2. Set the node name, category, and description in the metadata section
3. Drag a Section component onto the canvas
4. Add input components (TextInput, ColumnSelector, etc.) to the section
5. Write your processing logic in the code editor
6. Click Validate to check for errors
7. Click Save to create your node

After saving, restart Flowfile to load the new node. It appears in the node palette under your specified category.

## Example: Text Uppercase Node

Here's a complete example of creating a simple text transformation node:

**Metadata:**

- Node Name: Text Uppercase
- Category: Text Processing
- Description: Converts text column to uppercase

**Components:**

- Section: "Column Settings"
    - ColumnSelector: name=`text_column`, label="Text Column", types=String

**Process Code:**

```python
def process(self, *dfs):
    df = dfs[0]
    col_name = self.settings_schema.column_settings.text_column.value
    return df.with_columns([
        pl.col(col_name).str.to_uppercase().alias(col_name + "_upper")
    ])
```

## Tips

- Use descriptive names for components to make code more readable
- Group related settings into sections
- Test your node with sample data before using in production flows
- Use the Validate action to catch errors early

## Programmatic Alternative

For more control or to create nodes without the visual interface, see [Creating Custom Nodes](../../for-developers/creating-custom-nodes.md) for the Python-based approach.
