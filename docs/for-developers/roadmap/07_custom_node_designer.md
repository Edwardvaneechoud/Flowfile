# Feature 7: Standardized Custom Node Designer

## Motivation

Flowfile has a custom node framework (`CustomNodeBase`) that allows developers to create new node types with UI components and kernel execution. However, creating a custom node today requires:

- Writing Python code that subclasses `CustomNodeBase`.
- Knowing the UI component API (`TextInput`, `ColumnSelector`, `Section`, etc.).
- Understanding kernel code generation and the `process()` method pattern.
- Manual packaging and distribution (copy files, no versioning).

This makes custom nodes inaccessible to non-developers and difficult to share across teams or the community. A standardized designer would let users create, test, package, and share custom nodes visually.

## Current State

- **`CustomNodeBase`** (`custom_node.py`): Base class for user-defined nodes. Provides:
  - `node_name`, `node_category`, `node_icon`: Display properties.
  - `number_of_inputs` / `number_of_outputs`: I/O configuration.
  - `requires_kernel` / `kernel_id`: Kernel execution control.
  - `settings_schema`: `NodeSettings` instance defining the UI.
  - `process()`: Abstract method for data processing logic.
  - `generate_kernel_code()`: Converts settings + process() into standalone Python for kernel execution.
  - `get_frontend_schema()`: Returns UI definition + current values to frontend.
  - `from_frontend_schema()`: Reconstructs node from UI data.

- **UI components** (`ui_components.py`): `TextInput`, `NumericInput`, `SliderInput`, `ToggleSwitch`, `SingleSelect`, `MultiSelect`, `ColumnSelector`, `SecretSelector`. Plus layout components: `Section`, `SectionBuilder`, `NodeSettingsBuilder`.

- **Node templates** (`nodes.py`): `user_defined` node type exists. `to_node_template()` on `CustomNodeBase` converts a custom node to the standard `NodeTemplate` format.

- **Kernel runtime** (`kernel_runtime/main.py`): Sandboxed execution with namespace persistence, artifact management, and display output capture. Custom nodes can target the kernel for execution.

- **No visual designer**: Custom nodes are created by writing Python code. No GUI for building them.
- **No packaging format**: Custom nodes are loose Python files. No manifest, no versioning, no dependency declaration.
- **No sharing mechanism**: No marketplace, no import/export, no registry.

## Proposed Design

### Part 1: Standardized Node Package Format

**Package structure** (`.flownode` archive — a ZIP with structure):

```
my_custom_node.flownode/
├── manifest.json            # metadata, version, dependencies
├── node.py                  # CustomNodeBase subclass
├── requirements.txt         # Python dependencies (optional)
├── icon.svg                 # Custom icon (optional)
├── README.md                # Documentation (optional)
└── tests/                   # Test cases (optional)
    └── test_node.py
```

**`manifest.json`**:

```json
{
  "name": "my_custom_node",
  "display_name": "My Custom Node",
  "version": "1.0.0",
  "author": "user@example.com",
  "description": "Does something useful",
  "category": "transform",
  "flowfile_version_min": "0.6.3",
  "entry_point": "node.py",
  "class_name": "MyCustomNode",
  "requires_kernel": true,
  "tags": ["text", "nlp"],
  "inputs": 1,
  "outputs": 1
}
```

### Part 2: Visual Node Designer

A frontend page where users can build custom nodes without writing code (for simple cases) or with minimal code (for complex logic).

**Designer workflow**:

1. **Configure metadata**: Name, category, icon, description, input/output count.

2. **Design settings UI**: Drag-and-drop UI component builder.
   - Component palette: TextInput, NumericInput, Slider, Toggle, Select, ColumnSelector, SecretSelector.
   - Section grouping: Drag components into sections with titles.
   - Live preview: See the node's settings panel as users would see it.
   - Property editor: Configure each component (label, default value, validation, placeholder).

3. **Write processing logic**: CodeMirror editor with:
   - Pre-populated template based on settings schema.
   - Access to `self.settings_schema.section.component.value` for each configured component.
   - Input DataFrame available as `df` (or `df_left`, `df_right` for multi-input).
   - Polars autocomplete and documentation.
   - Syntax checking and error highlighting.

4. **Test in-place**:
   - Upload or select a sample dataset.
   - Run the node's `process()` method against the sample.
   - View output DataFrame, errors, and display outputs.
   - Iterate on settings and logic.

5. **Package and export**:
   - Generate the `.flownode` package.
   - Register in the local catalog.
   - Optionally publish to a shared registry (future).

### Part 3: Node Registry & Discovery

**Local registry** (managed by catalog service):

```python
class RegisteredCustomNode(BaseModel):
    node_id: int
    name: str
    display_name: str
    version: str
    author: str
    category: str
    description: str
    package_path: str           # path to .flownode archive
    installed_at: datetime
    tags: list[str] = []
    is_enabled: bool = True
```

**API endpoints**:

```python
@router.post("/nodes/custom/install")
async def install_custom_node(package: UploadFile) -> RegisteredCustomNode:
    """Install a .flownode package."""
    ...

@router.get("/nodes/custom")
async def list_custom_nodes() -> list[RegisteredCustomNode]:
    """List installed custom nodes."""
    ...

@router.delete("/nodes/custom/{node_id}")
async def uninstall_custom_node(node_id: int):
    """Uninstall a custom node."""
    ...

@router.post("/nodes/custom/{node_id}/enable")
async def toggle_custom_node(node_id: int, enabled: bool):
    """Enable/disable a custom node."""
    ...
```

**Discovery**: Installed custom nodes appear in the frontend's node palette alongside built-in nodes, organized by category.

### Part 4: Enhanced `CustomNodeBase`

Improvements to the base class to support the designer:

```python
class CustomNodeBase:
    # ... existing fields ...

    # NEW: schema-driven validation
    def validate_settings(self) -> list[str]:
        """Return list of validation errors, empty if valid."""
        ...

    # NEW: output schema inference without execution
    def infer_output_schema(self, input_schema: list[FlowfileColumn]) -> list[FlowfileColumn]:
        """Predict output schema from input schema and settings."""
        ...

    # NEW: generate template code from settings schema
    @classmethod
    def generate_template_code(cls, settings: NodeSettings) -> str:
        """Generate a process() method template with typed access to all settings."""
        ...

    # NEW: serialize to/from .flownode format
    def to_package(self, output_path: str) -> str:
        """Export as .flownode package."""
        ...

    @classmethod
    def from_package(cls, package_path: str) -> "CustomNodeBase":
        """Import from .flownode package."""
        ...
```

### Frontend Changes

**Node Designer page** (new route: `/nodes/designer`):
- Tabbed interface: Metadata | Settings UI | Processing Logic | Test | Export.
- Component palette with drag-and-drop.
- Live settings panel preview.
- CodeMirror editor for `process()` logic.
- Test runner with sample data input and output viewer.

**Node palette enhancement**:
- Custom nodes section in the sidebar.
- Category grouping matching built-in categories.
- Custom icons from `.flownode` packages.
- Tooltip showing description and version.

**Node management page** (new route: `/nodes/manage`):
- List installed custom nodes with enable/disable toggles.
- Install from file upload.
- Version info and update detection (future).

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/flowfile/node_designer/custom_node.py` | Add validation, schema inference, package export/import |
| `flowfile_core/flowfile_core/flowfile/node_designer/ui_components.py` | Add component metadata for designer (drag targets, property editors) |
| `flowfile_core/flowfile_core/configs/node_store/nodes.py` | Dynamic registration of custom node templates |
| `flowfile_core/flowfile_core/main.py` | Add custom node management endpoints |
| `flowfile_core/flowfile_core/catalog/` | Store custom node registrations |
| `flowfile_frontend/` | Node designer page, component builder, node management |

## Open Questions

1. **Dependency isolation**: Custom nodes may require Python packages not in Flowfile's environment. Should each node get its own virtual environment? Or use the kernel runtime for isolation?
2. **Security**: User-uploaded code runs in the kernel. Is the current kernel sandbox sufficient? Should there be a review/approval step for shared nodes?
3. **Multi-output nodes**: The designer should support nodes with multiple named outputs (e.g., "valid" and "invalid" rows). How does this map to VueFlow's edge model?
4. **Versioning**: When a custom node is updated, what happens to flows that use the old version? Proposed: version pinning with upgrade prompts.
5. **Community registry**: Future work — a centralized registry where users publish and discover nodes. What's the governance model?
