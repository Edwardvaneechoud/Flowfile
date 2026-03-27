# Feature 7: Standardized Custom Node Designer

## Motivation

The custom node framework is one of Flowfile's strengths. Users write clean Python code — subclass `CustomNodeBase`, define a `settings_schema` with UI components, implement a `process()` method, and they have a fully functional node with a visual settings panel. The API is intuitive: `self.settings_schema.section.component.value` gives you the user's input, and you return a Polars DataFrame. That's it.

The problem is the **kernel code generation layer**. When a custom node runs in the kernel (sandboxed execution), `generate_kernel_code()` translates the clean `process()` method into a fundamentally different syntax:

- The `self.settings_schema.section.component.value` access pattern gets replaced by auto-generated proxy classes (`_V`, `_Self`) that replicate the structure.
- `return` statements are rewritten into `result = ...` assignments.
- Input DataFrames come from `flowfile.read_inputs()` instead of method arguments.
- Outputs go through `flowfile.publish_output()` instead of `return`.

This generated kernel code is hard to read, hard to debug, and doesn't look like the clean Python the user wrote. When something goes wrong in kernel execution, users see error traces through the proxy code, not their original logic.

The visual Node Designer in the frontend is already excellent — a full drag-and-drop builder with component palette, property editor, code editor with Polars autocompletion, and code preview. Custom nodes always use Polars code in the `process()` method, which is a clean design choice.

**The goal is to standardize the kernel execution layer so that the code users write is the code that runs** — same syntax whether executing locally or in the kernel. Plus: packaging and sharing nodes across teams.

## Current State

- **`CustomNodeBase`** (`custom_node.py`): Clean, well-designed base class. The `process()` method pattern is intuitive — pure Python with Polars DataFrames in and out.
- **UI components** (`ui_components.py`): Rich set — `TextInput`, `NumericInput`, `SliderInput`, `ToggleSwitch`, `SingleSelect`, `MultiSelect`, `ColumnSelector`, `SecretSelector`. Section-based layout with builders.
- **`generate_kernel_code()`** (`custom_node.py:504`): This is the pain point. It:
  1. Extracts `process()` source via `inspect.getsource()`.
  2. Strips the `def process(...):` header to get the body.
  3. Builds proxy classes (`_V` for `.value` access, `_Self` for `self` access) to replicate settings access.
  4. Rewrites `return expr` → `result = expr`.
  5. Wraps everything with `flowfile.read_inputs()` and `flowfile.publish_output()`.
  6. Assembles a standalone script that looks nothing like the original `process()` method.
- **Kernel runtime** (`kernel_runtime/main.py`): Sandboxed execution with namespace persistence. Works well, but receives the transformed code, not the original.
- **Visual Node Designer** (frontend — fully implemented): A complete 3-panel designer at `/nodes/designer` with:
  - **Component Palette**: Drag-and-drop 9 UI component types (TextInput, NumericInput, ToggleSwitch, SingleSelect, MultiSelect, ColumnSelector, ColumnActionInput, SliderInput, SecretSelector).
  - **Design Canvas**: Node metadata (name, category, icon, inputs/outputs), section-based layout with drop zones, and a CodeMirror Python editor for `process()` logic with Polars autocompletion.
  - **Property Editor**: Right panel to configure selected component properties (defaults, validation, options source).
  - **Code Preview**: Shows the generated `CustomNodeBase` subclass.
  - **Node Browser**: Browse, view, and delete saved custom nodes.
  - **Auto-save**: Session storage persistence.
  - **Validation**: Name rules, duplicate checks, process method structure.
  - Custom nodes always use Polars code in the `process()` method — this is a core design choice.
- **No packaging format**: Custom nodes are Python files, no manifest or versioning.
- **No sharing mechanism**: No import/export, no registry.

## Proposed Design

### Part 1: Unified Execution Syntax

The core improvement: **the `process()` method should run as-is in the kernel**, without proxy class generation or return-statement rewriting.

**Approach**: Instead of deconstructing `process()` into a flat script, inject the actual `CustomNodeBase` subclass into the kernel environment and call `process()` directly.

```python
# What the kernel should receive (conceptual):

# 1. The user's actual class definition (transmitted as source)
class MyNode(CustomNodeBase):
    settings_schema = NodeSettings(...)

    def process(self, df: pl.DataFrame) -> pl.DataFrame:
        threshold = self.settings_schema.config.threshold.value
        return df.filter(pl.col("amount") > threshold)

# 2. Instantiate with populated settings
node = MyNode()
node.populate_values(settings_data)

# 3. Read inputs, call process(), publish outputs
inputs = flowfile.read_inputs()
result = node.process(*inputs)
flowfile.publish_output(result)
```

**Key changes to `generate_kernel_code()`**:
- Instead of extracting and rewriting the `process()` body, serialize the entire class definition.
- Inject `CustomNodeBase` (or a lightweight shim) into the kernel namespace.
- Populate settings values on the instance, then call `process()` normally.
- `self.settings_schema.section.component.value` works because it's the real object, not a proxy.
- `return` works because it's a real method call, not a flat script.
- Error traces show the user's original line numbers and code.

**Fallback**: For cases where the full class can't be transmitted (dynamic classes, closures), keep the current proxy generation as a fallback but mark it as legacy.

### Part 2: Standardized Node Package Format

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

### Part 3: Node Registry & Discovery

**Local registry**:

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
```

**Discovery**: Installed custom nodes appear in the frontend's node palette alongside built-in nodes, organized by category.

### Part 4: Enhanced `CustomNodeBase`

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
| `flowfile_core/flowfile_core/flowfile/node_designer/custom_node.py` | Rewrite `generate_kernel_code()` to transmit the actual class; add validation, schema inference, packaging |
| `kernel_runtime/kernel_runtime/main.py` | Support receiving and instantiating `CustomNodeBase` subclasses |
| `flowfile_core/flowfile_core/flowfile/node_designer/ui_components.py` | Ensure all components are serializable for kernel transmission |
| `flowfile_core/flowfile_core/configs/node_store/nodes.py` | Dynamic registration of custom node templates |
| `flowfile_core/flowfile_core/main.py` | Add custom node management endpoints |
| `flowfile_frontend/` | Node management page, package install UI |

## Open Questions

1. **Kernel shim size**: Transmitting the full `CustomNodeBase` + UI component classes to the kernel adds overhead. Should the kernel pre-install a lightweight `flowfile_node_designer` package, or receive everything per-execution?
2. **Dependency isolation**: Custom nodes may require Python packages not in Flowfile's environment. Should each node get its own virtual environment? Or use the kernel runtime for isolation?
3. **Multi-output nodes**: The designer should support nodes with multiple named outputs (e.g., "valid" and "invalid" rows). How does this map to VueFlow's edge model?
4. **Versioning**: When a custom node is updated, what happens to flows that use the old version? Proposed: version pinning with upgrade prompts.
5. **Community registry**: Future work — a centralized registry where users publish and discover nodes. What's the governance model?
