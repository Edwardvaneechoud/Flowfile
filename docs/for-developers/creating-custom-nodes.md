# Creating Custom Nodes

This page is the reference for building a custom node in Python with the Node Designer API — its class structure, the UI components you can put in the settings panel, type filtering, and the `process()` contract. After reading it you can write a node that shows up in the editor with a generated settings form. For a guided end-to-end build, see the [Custom Node Tutorial](custom-node-tutorial.md).

!!! tip "Visual alternative"
    You can also create custom nodes in the browser with the [Node Designer](../users/visual-editor/node-designer.md), without writing Python files directly.

!!! warning "Beta feature"
    Custom nodes are in beta. Some features (such as changing the node icon) are still in development.

## What are custom nodes?

Custom nodes extend Flowfile with your own data transformations that appear alongside the built-in nodes in the visual editor. Each one has:

- **A settings panel** — generated automatically from your schema (dropdowns, inputs, toggles).
- **Processing logic** — Polars code that transforms the data.
- **Palette placement** — under **User Defined Operations** in the node palette.

## Quick Start

### 1. Create Your First Node

Create a new Python file in your custom nodes directory:

```bash
~/.flowfile/user_defined_nodes/my_first_node.py
```

!!! info "Custom Node Location"
    The `~/.flowfile/user_defined_nodes/` directory is automatically created when you first run Flowfile. Place all your custom nodes here.

Here's a simple example that adds a greeting column. Note the `process` signature — it receives one or more eager `pl.DataFrame` inputs (variadic `*inputs`) and returns a `pl.DataFrame`:

```python
--8<-- "docs/examples/custom_node.py:example"
```

This example is tested against the current API. The node-designer symbols are imported through `flowfile.node_designer` (aliased `nd` above); they are the same classes as `from flowfile_core.flowfile.node_designer import ...`.

### 2. Use Your Node

1. **Restart Flowfile** to load your new node.
2. **Open the visual editor.**
3. **Find your node** under **User Defined Operations** in the node palette — every custom node lands there (see [The palette section](#the-palette-section)).
4. **Drag it** onto the canvas.
5. **Configure the settings** in the right panel.
6. **Run your flow.**

<details markdown="1">
<summary>Visual overview of the result</summary>

![Flow visualized](../assets/images/developers/basic_overview.png)


</details>

## Understanding the Architecture

### Node Structure

Every custom node has three main parts:

```python
class MyCustomNode(CustomNodeBase):
    # 1. Metadata - how the node appears in Flowfile
    node_name: str = "My Node"
    node_category: str = "Data Enhancement"
    title: str = "Add greetings"
    intro: str = "Prefix a name column with a greeting."

    # 2. Settings schema - the UI configuration
    settings_schema: MySettings = MySettings()

    # 3. Processing logic - what the node does
    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        df = inputs[0]
        # ... transformation logic ...
        return df
```

#### 3. Process

The `process` method is the engine of your node — where you write Polars code to transform the data.

- **Input:** the method receives its inputs as eager `pl.DataFrame` objects, passed as a variadic `*inputs`. A single-input node reads `inputs[0]`; a node with multiple inputs indexes further.
- **Accessing settings:** read current UI values with `self.settings_schema.<section_name>.<component_name>.value`.
- **Output:** return a single `pl.DataFrame` (or, for a multi-output node, a `dict[str, pl.DataFrame]` keyed by output name).

### The palette section

Every custom node appears in the node palette under one section: **User Defined Operations**. That placement is driven by `node_group`, which defaults to `"custom"` on `CustomNodeBase` — you don't need to set it.

`node_category` is separate, descriptive metadata (shown in the node header and browser modal). It does **not** create or select a palette section, so a value like `"Text Processing"` or `"Data Validation"` organizes nothing in the palette — all custom nodes still sit under User Defined Operations.


### Settings Architecture

Settings are organized in sections for clean UI organization:

```python
class MyNodeSettings(NodeSettings):
    # Each section becomes a collapsible panel in the UI
    basic_config: Section = Section(
        title="Basic Settings",
        description="Core functionality options",
        # Components go here as keyword arguments
        input_column=ColumnSelector(...),
        operation_type=SingleSelect(...)
    )
    
    advanced_options: Section = Section(
        title="Advanced Options",
        description="Fine-tune behavior",
        enable_caching=ToggleSwitch(...),
        max_iterations=NumericInput(...)
    )
```

## Available UI Components

### Text Input
For capturing string values:

```python
text_field = TextInput(
    label="Enter a value",
    default="Default text",
    placeholder="Hint text here..."
)
```

### Numeric Input
For numbers with optional validation:

```python
number_field = NumericInput(
    label="Count",
    default=10,
    min_value=1,
    max_value=100
)
```

### Single Select
Dropdown for one choice:

```python
choice_field = SingleSelect(
    label="Choose option",
    options=[
        ("value1", "Display Name 1"),
        ("value2", "Display Name 2"),
        ("simple", "Simple String Option")
    ],
    default="value1"
)
```

### Multi Select
For selecting multiple options:

```python
multi_field = MultiSelect(
    label="Select multiple",
    options=[
        ("opt1", "Option 1"),
        ("opt2", "Option 2"),
        ("opt3", "Option 3")
    ],
    default=["opt1", "opt2"]
)
```

### Toggle Switch
Boolean on/off control:

```python
toggle_field = ToggleSwitch(
    label="Enable feature",
    default=True,
    description="Turn this on to enable the feature"
)
```

### Column Selector
Smart column picker with type filtering:

```python
# Select any column
any_column = ColumnSelector(
    label="Pick a column",
    data_types=Types.All
)

# Select only numeric columns
numeric_column = ColumnSelector(
    label="Numeric column only",
    data_types=Types.Numeric,
    required=True
)

# Select multiple string columns
text_columns = ColumnSelector(
    label="Text columns",
    data_types=[Types.String, Types.Categorical],
    multiple=True
)
```

### Secret Selector
Access stored secrets (API keys, credentials, tokens). `SecretSelector`, like every other component, is added inside a `Section` as a keyword argument (the keyword is the field name). It has no `name=` field — only `label`:

```python
from flowfile_core.flowfile.node_designer import (
    CustomNodeBase, NodeSettings, Section, SecretSelector
)

class ApiSettings(NodeSettings):
    connection: Section = Section(
        title="Connection",
        api_key=SecretSelector(label="API Key"),
    )

class MyNode(CustomNodeBase):
    node_name: str = "API Reader"
    settings_schema: ApiSettings = ApiSettings()
```

The dropdown lists the secrets configured by the current user. Read the decrypted value inside `process()` with `self.settings_schema.connection.api_key.secret_value` — it is only accessible during execution. This is useful for nodes that connect to external APIs or services.

### Dynamic Column Options
Use `IncomingColumns` for dropdowns that populate with input columns:

```python
column_dropdown = SingleSelect(
    label="Choose input column",
    options=IncomingColumns  # Automatically filled with column names
)
```

## Type Filtering in Column Selector

The `Types` object provides convenient type filtering:

```python
from flowfile_core.flowfile.node_designer import Types

# Type groups
Types.Numeric    # All numeric types
Types.String     # String and categorical
Types.AnyDate    # Date, datetime, time, duration
Types.Boolean    # Boolean columns
Types.All        # All column types

# Specific types
Types.Int64      # 64-bit integers
Types.Float      # Float64
Types.Decimal    # Decimal type
Types.Date       # The Date type specifically (not the date group)

# Mix and match
data_types=[Types.Numeric, Types.AnyDate]  # Numbers and dates only
```

## Real-World Examples

### Data Quality Node

```python
class DataQualityNode(CustomNodeBase):
    node_name: str = "Data Quality Checker"
    node_category: str = "Data Validation"

    settings_schema: NodeSettings = NodeSettings(
        validation_rules=Section(
            title="Validation Rules",
            columns_to_check=ColumnSelector(
                label="Columns to Validate",
                data_types=Types.All,
                multiple=True
            ),
            null_threshold=NumericInput(
                label="Max Null Percentage",
                default=5.0,
                min_value=0,
                max_value=100
            ),
            add_summary=ToggleSwitch(
                label="Add Quality Summary",
                default=True
            )
        )
    )

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        df = inputs[0]
        columns = self.settings_schema.validation_rules.columns_to_check.value
        threshold = self.settings_schema.validation_rules.null_threshold.value

        row_count = df.height
        result = df
        for col in columns:
            null_pct = (df[col].null_count() / row_count) * 100 if row_count else 0.0
            if null_pct > threshold:
                # Flag the failing column with a per-row null indicator
                result = result.with_columns(
                    pl.col(col).is_null().alias(f"{col}_has_issues")
                )
        return result
```

### Text Processing Node

```python
class TextCleanerNode(CustomNodeBase):
    node_name: str = "Text Cleaner"
    node_category: str = "Text Processing"

    settings_schema: NodeSettings = NodeSettings(
        cleaning_options=Section(
            title="Cleaning Options",
            text_column=ColumnSelector(
                label="Text Column",
                data_types=Types.String,
                required=True
            ),
            operations=MultiSelect(
                label="Cleaning Operations",
                options=[
                    ("lowercase", "Convert to lowercase"),
                    ("remove_punctuation", "Remove punctuation"),
                    ("remove_extra_spaces", "Remove extra spaces"),
                    ("remove_numbers", "Remove numbers"),
                    ("trim", "Trim whitespace")
                ],
                default=["lowercase", "trim"]
            ),
            output_column=TextInput(
                label="Output Column Name",
                default="cleaned_text"
            )
        )
    )

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        df = inputs[0]
        text_col = self.settings_schema.cleaning_options.text_column.value
        operations = self.settings_schema.cleaning_options.operations.value
        output_col = self.settings_schema.cleaning_options.output_column.value

        # Start with the original text
        expr = pl.col(text_col)

        # Apply selected operations
        if "lowercase" in operations:
            expr = expr.str.to_lowercase()
        if "remove_punctuation" in operations:
            expr = expr.str.replace_all(r"[^\w\s]", "")
        if "remove_extra_spaces" in operations:
            expr = expr.str.replace_all(r"\s+", " ")
        if "remove_numbers" in operations:
            expr = expr.str.replace_all(r"\d+", "")
        if "trim" in operations:
            expr = expr.str.strip_chars()

        return df.with_columns(expr.alias(output_col))
```

## Performance note

Express transformations as Polars expressions on the incoming `pl.DataFrame` rather than iterating rows in Python. `process()` runs against an eager DataFrame, so keep per-row Python work (like `map_elements`) to cases that genuinely need it.

## Troubleshooting

### Node doesn't appear
1. Check the file is in `~/.flowfile/user_defined_nodes/`.
2. Restart Flowfile completely.
3. Check for Python syntax errors in the terminal.
4. Ensure your class inherits from `CustomNodeBase`.
5. Look under **User Defined Operations** in the palette — not a section named after your `node_category`.

### Settings don't work
1. Verify `settings_schema` is assigned.
2. Check component imports.
3. Ensure the section structure is correct (components are keyword arguments inside a `Section`).
4. Use `.value` to read component values in `process()`.

### Processing errors
1. Check the input DataFrame has the expected columns.
2. Ensure `process()` returns a `pl.DataFrame`.

---

For a step-by-step walkthrough, see the [Custom Node Tutorial](custom-node-tutorial.md).