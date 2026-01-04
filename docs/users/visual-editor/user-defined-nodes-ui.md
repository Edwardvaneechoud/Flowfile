# User-Defined Nodes Management

Create, edit, and manage custom nodes directly from the Flowfile UI without writing code in external files or restarting the application.

## Overview

The User-Defined Nodes Manager provides a visual interface for creating custom data transformation nodes. Key benefits include:

- **No file editing required** - Write node code directly in the browser
- **Instant availability** - Nodes appear immediately in the node panel after saving
- **Built-in validation** - Python syntax errors are shown before you save
- **Icon management** - Upload and assign custom icons to your nodes

!!! tip "New to Custom Nodes?"
    If you're new to creating custom nodes, check out the [Creating Custom Nodes](../../for-developers/creating-custom-nodes.md) guide first to understand the Python API and available components.

## Accessing the Node Manager

1. Open the Flowfile application
2. Navigate to the **User-Defined Nodes** page from the main navigation menu
3. You'll see a list of your existing custom nodes (if any) and options to create new ones

## Creating a New Node

### Step 1: Open the Editor

Click the **Create New Node** button to open the code editor interface.

### Step 2: Write Your Node Code

The editor provides a Python environment where you can write your custom node. Your node class must extend `CustomNodeBase`:

```python
import polars as pl
from flowfile_core.flowfile.node_designer import (
    CustomNodeBase,
    Section,
    NodeSettings,
    TextInput,
    ColumnSelector,
    Types
)

class MyNodeSettings(NodeSettings):
    main_config: Section = Section(
        title="Configuration",
        description="Configure your node",
        input_column=ColumnSelector(
            label="Input Column",
            data_types=Types.All,
            required=True
        )
    )

class MyCustomNode(CustomNodeBase):
    node_name: str = "My Custom Node"
    node_category: str = "Transform"
    title: str = "My Custom Node"
    intro: str = "A custom transformation node"

    settings_schema: MyNodeSettings = MyNodeSettings()

    def process(self, input_df: pl.LazyFrame) -> pl.LazyFrame:
        column = self.settings_schema.main_config.input_column.value
        # Your transformation logic here
        return input_df
```

### Step 3: Validate Your Code

The editor automatically validates your Python code as you type. Syntax errors are highlighted and must be fixed before saving.

!!! warning "Fix Errors Before Saving"
    If your code contains syntax errors, you won't be able to save the node. Review the error messages and fix any issues before proceeding.

### Step 4: Save Your Node

Click **Save** to register your node. Once saved:

- The node appears immediately in the left sidebar node panel
- It's placed in the category you specified with `node_category`
- You can drag it onto any flow canvas and start using it

## Node Metadata

Every custom node requires specific class attributes that control how it appears in the UI:

| Attribute | Description | Example |
|-----------|-------------|---------|
| `node_name` | Display name shown in the node panel | `"Data Cleaner"` |
| `node_category` | Category grouping in the sidebar | `"Transform"`, `"Input"`, `"Validation"` |
| `title` | Title displayed in the settings drawer | `"Clean and Validate Data"` |
| `intro` | Description shown in the node drawer | `"Remove nulls and validate formats"` |
| `node_icon` | Icon filename from the icons directory | `"cleaner.svg"` |

```python
class MyNode(CustomNodeBase):
    node_name: str = "Data Cleaner"
    node_category: str = "Transform"
    title: str = "Clean and Validate Data"
    intro: str = "Remove nulls, fix formats, and validate your data"
    node_icon: str = "cleaner.svg"  # Optional - uses default if not specified
```

## Managing Custom Icons

You can upload custom icons to give your nodes a distinctive appearance in the node panel.

### Uploading Icons

1. Navigate to the **Icons** section of the Node Manager
2. Click **Upload Icon**
3. Select an image file from your computer
4. The icon is uploaded and available for use

### Supported Formats

| Format | Extension |
|--------|-----------|
| PNG | `.png` |
| JPEG | `.jpg`, `.jpeg` |
| SVG | `.svg` |
| GIF | `.gif` |
| WebP | `.webp` |

!!! note "Size Limit"
    Icon files must be under **5MB** in size.

### Where Icons Are Stored

Uploaded icons are stored in the `user_defined_nodes/icons` directory within your Flowfile configuration folder.

### Assigning an Icon to a Node

Set the `node_icon` attribute in your node class to the filename of your uploaded icon:

```python
class MyNode(CustomNodeBase):
    node_name: str = "Weather Data"
    node_icon: str = "weather.svg"  # Must match an uploaded icon filename
    # ... rest of node definition
```

### Deleting Icons

1. Navigate to the **Icons** section
2. Find the icon you want to remove
3. Click the **Delete** button
4. Confirm the deletion

!!! warning "Icon References"
    If you delete an icon that's referenced by a node, that node will fall back to the default icon.

## Editing Existing Nodes

### Viewing Node Code

1. Open the Node Manager
2. Find your node in the list
3. Click on the node to view its details and code

### Making Changes

1. Click **Edit** to open the code editor
2. Make your modifications
3. The editor validates your changes in real-time
4. Click **Save** to apply updates

### Seeing Changes

After saving edits:

- Changes take effect immediately
- Any flows using the node will use the updated logic on next run
- The node's appearance in the panel updates if you changed metadata

## Deleting Nodes

To remove a custom node:

1. Open the Node Manager
2. Find the node you want to delete
3. Click the **Delete** button
4. Confirm the deletion

!!! warning "Deletion is Permanent"
    Deleting a node:

    - Unregisters the node from Flowfile
    - Deletes the underlying Python file
    - Cannot be undone

    Flows that use the deleted node will show errors when opened.

## Using Secrets in Custom Nodes

Custom nodes can securely access stored secrets using the `SecretSelector` component. This is useful for API keys, passwords, and other sensitive credentials.

### Adding a Secret Selector

```python
from flowfile_core.flowfile.node_designer.ui_components import SecretSelector, Section
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings

class APINodeSettings(NodeSettings):
    api_credentials: Section = Section(
        title="API Credentials",
        description="Configure API authentication",
        api_key=SecretSelector(
            label="API Key",
            required=True
        ),
        api_secret=SecretSelector(
            label="API Secret",
            required=False
        )
    )

class MyAPINode(CustomNodeBase):
    node_name: str = "My API Node"
    node_category: str = "External"
    title: str = "Connect to External API"
    intro: str = "Fetch data from an external API"

    settings_schema: APINodeSettings = APINodeSettings()

    def process(self, input_df):
        # Access the secret value during execution
        api_key = self.settings_schema.api_credentials.api_key.secret_value

        # Use get_secret_value() to get the actual string
        actual_key = api_key.get_secret_value()

        # Use the key to authenticate with your API
        # ... your API logic here

        return input_df
```

### How Secrets Work

- **Selection at design time** - Users select from stored secrets in the node settings
- **Decryption at runtime** - Secret values are only decrypted when the flow executes
- **Secure storage** - Secrets are never exposed in flow files or logs

!!! info "Managing Secrets"
    Secrets must be created in the Secrets Manager before they can be selected in custom nodes. See the Secrets management documentation for details on storing and managing secrets.

## Troubleshooting

### Node Not Appearing in Panel

**Problem**: You saved your node but it doesn't show up in the sidebar.

**Solutions**:

1. **Check class inheritance** - Ensure your class extends `CustomNodeBase`
2. **Verify the save succeeded** - Check for any error messages
3. **Check the category** - Look in the category specified by `node_category`
4. **Refresh the page** - Try refreshing the browser

### Syntax Errors

**Problem**: The editor shows syntax errors and won't let you save.

**Solutions**:

1. Check for missing colons, parentheses, or quotes
2. Verify proper indentation (Python requires consistent indentation)
3. Ensure all imports are correct
4. Check that class definitions are properly formatted

### Icon Not Loading

**Problem**: Your custom icon doesn't appear on the node.

**Solutions**:

1. **Check file format** - Ensure it's PNG, JPG, SVG, GIF, or WebP
2. **Verify filename** - The `node_icon` value must exactly match the uploaded filename
3. **Check file size** - Icons must be under 5MB
4. **Re-upload the icon** - Try deleting and uploading again

### Process Method Errors

**Problem**: The node appears but throws errors during execution.

**Solutions**:

1. Ensure `process()` returns a `pl.LazyFrame`
2. Check that column names accessed exist in the input data
3. Handle potential null values in your logic
4. Use `.value` to access component values from settings

## Next Steps

- Learn the full Python API in [Creating Custom Nodes](../../for-developers/creating-custom-nodes.md)
- Explore advanced patterns in [Custom Node Tutorial](../../for-developers/custom-node-tutorial.md)
- See available UI components and their options in the API reference
