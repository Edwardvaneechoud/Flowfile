# User-Defined Nodes Management

Create, edit, and manage custom nodes directly from the Flowfile UI without writing files manually or restarting the application.

!!! info "New in v0.5.2"
    The Node Designer provides a visual interface for creating custom nodes. Nodes appear immediately in the node panel after creation.

## Overview

The Node Designer lets you:

- **Create custom nodes** visually with a drag-and-drop interface
- **Configure UI components** for your node settings
- **Write Python logic** with syntax validation
- **Upload custom icons** to brand your nodes
- **Edit and delete** existing nodes without touching files

!!! tip "Looking for the Python API?"
    For advanced custom node development and programmatic workflows, see the [Creating Custom Nodes](../../for-developers/creating-custom-nodes.md) developer guide.

## Accessing the Node Designer

1. Open Flowfile and navigate to the main sidebar
2. Click on **Node Designer** (puzzle piece icon) in the navigation menu
3. The Node Designer canvas opens with a blank template

## Creating a New Node

### Step 1: Define Node Metadata

Fill in the basic information about your node:

| Field | Description | Required |
|-------|-------------|----------|
| **Node Name** | Display name shown in the node panel | Yes |
| **Category** | Category grouping (e.g., "Transform", "Input") | Yes |
| **Title** | Heading shown in the settings drawer | No |
| **Description** | Brief explanation shown in the node drawer | No |
| **Number of Inputs** | How many input connections the node accepts | No (default: 1) |
| **Number of Outputs** | How many output connections the node provides | No (default: 1) |
| **Icon** | Custom icon from the icon selector | No |

### Step 2: Design the Settings UI

Build your node's configuration interface by adding **Sections** and **Components**:

1. Click **Add Section** to create a settings group
2. Drag components from the **Component Palette** onto your section
3. Configure each component's properties in the right panel

**Available Components:**

| Component | Use Case | Example |
|-----------|----------|---------|
| **Text Input** | String values | Column names, labels |
| **Numeric Input** | Numbers with min/max validation | Thresholds, counts |
| **Slider Input** | Numeric range selection | Percentages |
| **Toggle Switch** | Boolean on/off settings | Enable features |
| **Single Select** | Dropdown single choice | Operation type |
| **Multi Select** | Multiple selections | Column list |
| **Column Selector** | Select columns from input data | Target column |
| **Secret Selector** | Select stored secrets | API credentials |

### Step 3: Write the Process Logic

Enter your Python processing code in the code editor. Your code has access to:

- `self.settings_schema` - Access component values via `self.settings_schema.<section>.<component>.value`
- `inputs` - Tuple of input DataFrames (Polars LazyFrames)

```python
def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
    # Access settings
    target_column = self.settings_schema.main_config.column_name.value
    threshold = self.settings_schema.main_config.threshold.value

    # Process data
    df = inputs[0]
    result = df.filter(pl.col(target_column) > threshold)

    return result
```

!!! warning "Syntax Validation"
    The editor validates Python syntax before saving. Fix any highlighted errors before proceeding.

### Step 4: Save Your Node

1. Click the **Save** button in the header
2. Your node is validated and saved
3. The node immediately appears in the node panel under your chosen category

## Node Metadata Reference

These class attributes control how your node appears and behaves:

| Attribute | Description | Default |
|-----------|-------------|---------|
| `node_name` | Display name in the node panel and canvas | Required |
| `node_category` | Category for grouping in the node panel | `"Custom"` |
| `title` | Drawer title when the node is selected | Same as `node_name` |
| `intro` | Description shown in the node drawer | `"A custom node for data processing"` |
| `node_icon` | Icon filename from the icons directory | `"user-defined-icon.png"` |

## Managing Custom Icons

Personalize your nodes with custom icons.

### Uploading Icons

1. In the Node Designer, click the **Icon** dropdown
2. Click **Upload Icon** at the bottom of the dropdown
3. Select an image file from your computer
4. The icon is uploaded and automatically selected

**Supported Formats:**

- PNG, JPG, JPEG
- SVG
- GIF, WebP

**Limits:**

- Maximum file size: 5MB

### Where Icons Are Stored

Icons are saved to:
```
~/.flowfile/user_defined_nodes/icons/
```

In Docker deployments:
```
/data/user/user_defined_nodes/icons/
```

### Selecting an Icon

1. Open your node in the Node Designer
2. Click the **Icon** dropdown
3. Select from your uploaded icons or the default icon
4. Save your node to apply the change

## Editing Existing Nodes

1. In the Node Designer, click the **Browse** button
2. Select a node from the grid to view its code
3. Click **Load** to edit the node (or just close to view only)
4. Make your changes and click **Save**

!!! tip "Non-destructive Preview"
    Clicking a node in the browser shows a read-only preview. You must explicitly load it to edit.

## Deleting Nodes

1. Click **Browse** to open the node browser
2. Select the node you want to delete
3. Click the **Delete** button
4. Confirm the deletion

!!! warning "Permanent Deletion"
    Deleting a node removes the file from disk and unregisters it from Flowfile. This cannot be undone.

## Using Secrets in Custom Nodes

For nodes that need API keys, passwords, or other sensitive data, use the `SecretSelector` component.

### Adding a Secret Selector

1. Drag a **Secret Selector** component onto your section
2. Configure the label and optionally a name prefix filter
3. Save your node

### Accessing Secrets in Code

```python
from flowfile_core.flowfile.node_designer.ui_components import SecretSelector, Section
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings
import polars as pl

class MyAPINode(CustomNodeBase):
    node_name: str = "My API Node"
    node_category: str = "Integration"
    title: str = "API Integration"
    intro: str = "Connect to external APIs securely"

    settings_schema = NodeSettings(
        api_config = Section(
            title="API Configuration",
            api_key = SecretSelector(
                label="API Key",
                required=True,
                name_prefix="GITHUB_"  # Optional: filter secrets by prefix
            )
        )
    )

    def process(self, *inputs: pl.DataFrame) -> pl.DataFrame:
        # Access the secret value during execution
        api_key = self.settings_schema.api_config.api_key.secret_value

        # Use api_key.get_secret_value() to get the actual string
        actual_key = api_key.get_secret_value()

        # ... use the key for API calls ...
        return inputs[0]
```

!!! info "Security Note"
    Secrets are decrypted only at execution time and are never exposed in the UI or logs. The `secret_value` property is only accessible within the `process()` method.

**See also:** [Secrets Management](../python-api/reference/cloud-connections.md) for storing and managing secrets.

## Troubleshooting

### Node Doesn't Appear in the Panel

1. **Check the class inheritance** - Your node must extend `CustomNodeBase`
2. **Verify syntax** - Ensure there are no Python syntax errors
3. **Check required fields** - `node_name` and `node_category` are required
4. **Refresh the page** - Sometimes a browser refresh is needed

### Syntax Errors in the Code Editor

The editor highlights syntax errors in red. Common issues:

- Missing colons after `def` or `class` statements
- Incorrect indentation (use 4 spaces)
- Unclosed parentheses or brackets
- Missing import statements

### Icon Not Loading

1. **Check the file format** - Only PNG, JPG, JPEG, SVG, GIF, and WebP are supported
2. **Check file size** - Icons must be under 5MB
3. **Verify the filename** - Ensure the `node_icon` attribute matches the uploaded filename
4. **Check browser cache** - Try a hard refresh (Ctrl+Shift+R / Cmd+Shift+R)

### Settings Not Working

1. **Access values correctly** - Use `.value` to get component values
2. **Check section names** - Ensure section names match between schema and access
3. **Verify component types** - Each component type returns different value types

---

[← Visual Editor Guide](index.md) | [Building Flows →](building-flows.md)
