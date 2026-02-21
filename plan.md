# Plan: Kernel Integration in Custom Node Designer

## Overview

Integrate the flowfile kernel (Docker-based Python execution) into the custom node designer, allowing users to:
1. Select a kernel for their custom node (for external package support)
2. Have the process code execute on the kernel instead of locally
3. Define multiple named outputs that downstream nodes can connect to independently

## Design Principles

- **Same process code style**: Users write `def process(self, *inputs)` as today; the backend transparently wraps it for kernel execution
- **Settings access preserved**: `self.settings_schema.section.component.value` works identically in kernel mode via lightweight proxy classes injected into the kernel script
- **Multi-output via existing infrastructure**: Output handles (`output-0`, `output-1`) are already supported in the frontend and schema — we add backend routing

---

## Step 1: Extend `CustomNodeBase` with kernel fields

**File**: `flowfile_core/flowfile_core/flowfile/node_designer/custom_node.py`

Add two new fields:
```python
kernel_id: str | None = None         # When set, process() runs on this kernel
output_names: list[str] = ["main"]   # Named outputs the kernel code produces
```

Add a method `generate_kernel_code(self) -> str` that:
- Extracts the `process()` method source code body using `inspect.getsource()`
- Generates a self-contained kernel script with:
  - Lightweight `_SettingsProxy` classes that replicate `self.settings_schema.section.component.value`
  - Input reading via `flowfile.read_input()`
  - The process body injected in the middle
  - Output publishing via `flowfile.publish_output()` for each output name

Update `get_frontend_schema()` and `to_node_template()` to include `kernel_id` and `output_names`.

---

## Step 2: Extend `UserDefinedNode` schema

**File**: `flowfile_core/flowfile_core/schemas/input_schema.py`

```python
class UserDefinedNode(NodeMultiInput):
    settings: Any
    kernel_id: str | None = None
    output_names: list[str] = Field(default_factory=lambda: ["main"])
```

---

## Step 3: Add multi-output support to `FlowNode`

**File**: `flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py`

- Add `_named_outputs: dict[str, FlowDataEngine] = {}` to `FlowNode.__init__`
- Add `_input_output_handles: dict[int, str] = {}` to track which output handle each input connection uses
- Modify `add_node_connection()` signature to accept `output_handle: str = "output-0"` and store in `_input_output_handles`
- Add `get_output(handle: str = "output-0") -> FlowDataEngine | None` method
- Modify `get_resulting_data()`: in the input collection loop, check if the source node has `_named_outputs` and if the connection specifies a non-default handle; if so, use `get_output(handle)` instead of the default result

**File**: `flowfile_core/flowfile_core/flowfile/flow_node/models.py`

- Add `output_handle_map` to `NodeStepInputs` (or keep it on FlowNode directly)

---

## Step 4: Update connection handling to pass output handle

**File**: `flowfile_core/flowfile_core/flowfile/flow_graph.py`

In `add_connection()`:
```python
to_node.add_node_connection(
    from_node,
    node_connection.input_connection.get_node_input_connection_type(),
    output_handle=node_connection.output_connection.connection_class,
)
```

---

## Step 5: Modify `add_user_defined_node` for kernel routing

**File**: `flowfile_core/flowfile_core/flowfile/flow_graph.py`

In `add_user_defined_node()`, check `custom_node.kernel_id`:

**If kernel_id is set** (kernel mode):
1. Write input DataFrames to parquet in shared directory (same pattern as `add_python_script`)
2. Call `custom_node.generate_kernel_code()` to get the self-contained script
3. Build `ExecuteRequest` with the generated code
4. Call `manager.execute_sync(kernel_id, request)`
5. Read output parquet files from output dir: for each name in `output_names`, read `{output_dir}/{name}.parquet`
6. Store all outputs in the node's `_named_outputs` dict
7. Return the first output (mapped to `output-0`) as backward-compatible result

**If kernel_id is not set** (local mode):
- Execute as today — call `custom_node.process()` directly

---

## Step 6: Frontend — Add kernel fields to Node Designer types and state

**File**: `flowfile_frontend/src/renderer/app/pages/nodeDesigner/types.ts`
- Add `kernel_id?: string | null` and `output_names?: string[]` to `NodeMetadata`

**File**: `flowfile_frontend/src/renderer/app/pages/nodeDesigner/constants.ts`
- Add `kernel_id: null` and `output_names: ["main"]` to `defaultNodeMetadata`
- Add a `defaultKernelProcessCode` template that shows users the kernel-aware pattern

**File**: `flowfile_frontend/src/renderer/app/pages/nodeDesigner/composables/useNodeDesignerState.ts`
- Include kernel state in `getState()` / `setState()` serialization

---

## Step 7: Frontend — Kernel selector UI in Node Designer page

**File**: `flowfile_frontend/src/renderer/app/pages/NodeDesigner.vue`

Add a new "Execution" section in the metadata panel:
- **Kernel selector**: Dropdown populated by `GET /api/kernels` (list available kernels). "Local (default)" option for no kernel.
- **Output names editor**: Visible when kernel is selected. A list of strings with add/remove buttons. Each output name maps to an output handle.
- When kernel is selected and `number_of_outputs` changes based on output names count, auto-sync.

---

## Step 8: Frontend — Update code generation for kernel mode

**File**: `flowfile_frontend/src/renderer/app/pages/nodeDesigner/composables/useCodeGeneration.ts`

When generating code:
- If `kernel_id` is set, add `kernel_id: str = "{kernel_id}"` and `output_names: list[str] = [...]` to the generated class
- The process method signature stays the same (`def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame`)
- For multi-output, the return type hint becomes `dict[str, pl.LazyFrame]`

---

## Key Design: Kernel Code Generation (the "elegant" part)

When `generate_kernel_code()` is called, it produces a script like:

```python
import polars as pl
import flowfile

# --- Settings proxy (auto-generated) ---
class _V:
    def __init__(self, v): self.value = v

class _Self:
    class settings_schema:
        class config:
            threshold = _V(42)
            column_name = _V("amount")

self = _Self()

# --- Read inputs ---
inputs = [flowfile.read_input()]

# --- Process body (from user's def process) ---
lf = inputs[0]
threshold = self.settings_schema.config.threshold.value
lf = lf.filter(pl.col(self.settings_schema.config.column_name.value) > threshold)

# --- Publish outputs ---
flowfile.publish_output(lf, name="main")
```

This preserves the exact `self.settings_schema.section.component.value` access pattern that users write in their process code, making kernel execution fully transparent.

---

## Files Modified (Summary)

### Backend (Python)
1. `flowfile_core/flowfile_core/flowfile/node_designer/custom_node.py` — kernel_id, output_names, generate_kernel_code()
2. `flowfile_core/flowfile_core/schemas/input_schema.py` — UserDefinedNode extension
3. `flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py` — multi-output support
4. `flowfile_core/flowfile_core/flowfile/flow_graph.py` — kernel routing in add_user_defined_node, output handle in add_connection

### Frontend (TypeScript/Vue)
5. `flowfile_frontend/src/renderer/app/pages/nodeDesigner/types.ts` — NodeMetadata extension
6. `flowfile_frontend/src/renderer/app/pages/nodeDesigner/constants.ts` — defaults
7. `flowfile_frontend/src/renderer/app/pages/nodeDesigner/composables/useNodeDesignerState.ts` — state tracking
8. `flowfile_frontend/src/renderer/app/pages/nodeDesigner/composables/useCodeGeneration.ts` — kernel code gen
9. `flowfile_frontend/src/renderer/app/pages/NodeDesigner.vue` — kernel selector UI
