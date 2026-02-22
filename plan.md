# Design Plan: Named Node Inputs & Outputs for Kernel Execution Nodes

## Executive Summary

This plan introduces **named inputs** and **multiple named outputs** for kernel execution nodes (Python Script and Custom Nodes) in the Flowfile visual editor. Today, all inputs arrive under the key `"main"` and the Python Script node can only produce a single `"main"` output. The proposed changes let users reference inputs by their source node's `node_reference` (e.g., `flowfile.read_input("orders")`) and publish multiple distinct outputs from a single node.

---

## 1. Current Architecture (Status Quo)

### 1.1 How inputs flow to a kernel node

```
FlowGraph.add_python_script()
  └─ _func(*flowfile_tables: FlowDataEngine)
       └─ write_inputs_to_parquet(flowfile_tables, ...) → {"main": [path0, path1, ...]}
            └─ ExecuteRequest.input_paths = {"main": ["...main_0.parquet", "...main_1.parquet"]}
```

**Key file:** `flowfile_core/flowfile_core/kernel/execution.py:19-44`

`write_inputs_to_parquet` writes every input DataFrame to `main_{idx}.parquet` and puts all paths under the single key `"main"`. The kernel's `flowfile.read_input("main")` concatenates them.

### 1.2 How outputs flow from a kernel node

The Python Script node reads exactly one output file: `main.parquet` from the output directory (`flow_graph.py:1345-1348`). Custom nodes with `output_names` already support multiple named outputs via `flowfile.publish_output(df, name="...")` and read them back in a loop (`flow_graph.py:996-1013`), but the Python Script node doesn't use this.

### 1.3 Node connections in the frontend

- Handles use numbered IDs: `input-0`..`input-9`, `output-0`..`output-9`
- `NodeTemplate.multi = true` means a single `input-0` handle accepts many connections (union-style)
- The Python Script node (`NodePythonScript`) extends `NodeMultiInput` with `depending_on_ids: list[int]`
- `FlowNode._input_output_handles: dict[int, str]` maps source node ID → output handle used

### 1.4 Kernel client API (already exists)

```python
# kernel_runtime/kernel_runtime/flowfile_client.py
flowfile.read_input(name="main")      # Returns pl.LazyFrame (concatenates multiple paths)
flowfile.read_first(name="main")      # Returns first input only
flowfile.read_inputs()                 # Returns dict[str, list[pl.LazyFrame]]
flowfile.publish_output(df, name="main")
```

The `read_input(name)` and `publish_output(df, name)` APIs **already support arbitrary names**. The limitation is entirely in how `flowfile_core` constructs `input_paths` and reads outputs.

---

## 2. Design: Named Inputs

### 2.1 Goal

When a user connects node "orders" (node_reference=`orders`) and node "customers" (node_reference=`customers`) to a Python Script node, they can write:

```python
orders = flowfile.read_input("orders")
customers = flowfile.read_input("customers")
joined = orders.join(customers, on="customer_id")
flowfile.publish_output(joined)
```

### 2.2 Input naming strategy

Each input is named by the **source node's `node_reference`**. The `node_reference` field already exists on every node (`NodeBase.node_reference`) and defaults to `df_{node_id}` in the code generator. This is a user-visible, editable label on the General Settings tab.

**Rules:**
1. If the source node has a non-empty `node_reference`, use it as the input name
2. Otherwise, fall back to `"input_{source_node_id}"` (e.g., `"input_3"`)
3. If two source nodes share the same `node_reference`, deduplicate with a `_{idx}` suffix
4. The key `"main"` is **reserved for backward compatibility**: when there is exactly one source node with no `node_reference`, or when the node uses `multi=true` union semantics, all inputs are grouped under `"main"`

### 2.3 Changes to `write_inputs_to_parquet`

**File:** `flowfile_core/flowfile_core/kernel/execution.py`

The function signature changes to accept named metadata:

```python
def write_inputs_to_parquet(
    flowfile_tables: tuple[FlowDataEngine, ...],
    manager: KernelManager,
    input_dir: str,
    flow_id: int,
    node_id: int,
    input_names: list[str] | None = None,  # NEW: one name per input
) -> dict[str, list[str]]:
```

- When `input_names` is `None` (backward-compatible default), all inputs go under `"main"` as today
- When provided, each `flowfile_tables[i]` is written to `{input_names[i]}.parquet` and grouped by name in the result dict. Multiple inputs with the same name get appended to the same list (union-style behavior for that name).

### 2.4 Resolving input names in `add_python_script`

**File:** `flowfile_core/flowfile_core/flowfile/flow_graph.py` (around line 1273)

Before calling `write_inputs_to_parquet`, the execution function resolves the input names:

```python
def _func(*flowfile_tables: FlowDataEngine) -> FlowDataEngine:
    node = self.get_node(node_id)
    main_inputs = node.node_inputs.main_inputs or []

    # Build input names from source node references
    input_names = []
    seen_names: dict[str, int] = {}
    for input_node in main_inputs:
        ref = getattr(input_node.setting_input, 'node_reference', None)
        name = ref if ref else f"input_{input_node.node_id}"
        # Deduplicate
        if name in seen_names:
            seen_names[name] += 1
            name = f"{name}_{seen_names[name]}"
        else:
            seen_names[name] = 0
        input_names.append(name)

    input_paths = write_inputs_to_parquet(
        flowfile_tables, manager, input_dir, flow_id, node_id,
        input_names=input_names,
    )
    ...
```

The same logic applies to `_make_kernel_user_defined_func` for custom nodes.

### 2.5 Backward compatibility

- `flowfile.read_input()` (no argument) already defaults to `name="main"`
- When the new code generates named inputs, calling `read_input("main")` will raise a `KeyError` with a helpful message listing available inputs
- We add a `"main"` alias that points to the first input when there's only one connection:

```python
# In write_inputs_to_parquet, after building named paths:
if len(result) == 1 and "main" not in result:
    # Single input: also register under "main" for backward compat
    only_name = next(iter(result))
    result["main"] = result[only_name]
```

- `flowfile.read_inputs()` returns all named inputs, giving full flexibility

### 2.6 `read_input()` auto-resolution improvement

**File:** `kernel_runtime/kernel_runtime/flowfile_client.py`

Improve the default behavior so `read_input()` (no argument) works when a single named input is present:

```python
def read_input(name: str = "main") -> pl.LazyFrame:
    input_paths = _get_context_value("input_paths")
    if name == "main" and "main" not in input_paths and len(input_paths) == 1:
        # Auto-resolve to the only available input
        name = next(iter(input_paths))
    paths = _check_input_available(input_paths, name)
    ...
```

This ensures `read_input()` works even when the single input has a custom name.

---

## 3. Design: Multiple Named Outputs

### 3.1 Goal

A Python Script node can publish multiple named outputs, each routable to a different downstream node via separate output handles.

```python
valid = df.filter(pl.col("amount") > 0)
invalid = df.filter(pl.col("amount") <= 0)
flowfile.publish_output(valid, "valid")
flowfile.publish_output(invalid, "invalid")
```

### 3.2 Node configuration

Add an `output_names` field to `PythonScriptInput`:

**File:** `flowfile_core/flowfile_core/schemas/input_schema.py`

```python
class PythonScriptInput(BaseModel):
    code: str = ""
    kernel_id: str | None = None
    cells: list[NotebookCell] | None = None
    output_names: list[str] = Field(default_factory=lambda: ["main"])  # NEW
```

When `output_names = ["main"]`, the node has 1 output handle (current behavior). When `output_names = ["valid", "invalid"]`, the node has 2 output handles.

### 3.3 Dynamic output handle count

The Python Script node's `NodeTemplate.output` is currently `1`. This needs to become dynamic based on `output_names`:

**File:** `flowfile_core/flowfile_core/flowfile/flow_graph.py` in `add_python_script`

```python
# After setting up the node:
node = self.get_node(node_python_script.node_id)
output_names = node_python_script.python_script_input.output_names or ["main"]
if node is not None:
    node.node_template.output = len(output_names)
```

### 3.4 Reading multiple outputs back

Currently `add_python_script` only reads `main.parquet`. Reuse the pattern from `_make_kernel_user_defined_func` (`flow_graph.py:996-1013`):

```python
# After kernel execution succeeds:
output_names = node_python_script.python_script_input.output_names or ["main"]
node = self.get_node(node_id)
primary_result: FlowDataEngine | None = None

for i, name in enumerate(output_names):
    output_path = os.path.join(output_dir, f"{name}.parquet")
    if os.path.exists(output_path):
        fde = FlowDataEngine(pl.scan_parquet(output_path))
        handle = f"output-{i}"
        if node is not None:
            node._named_outputs[handle] = fde
        if i == 0:
            primary_result = fde

if primary_result is not None:
    return primary_result
return flowfile_tables[0] if flowfile_tables else FlowDataEngine(pl.LazyFrame())
```

### 3.5 Downstream consumption

The existing `FlowNode._resolve_input_result` already handles `_named_outputs`:

```python
# flow_node.py:686-698
def _resolve_input_result(self, input_node):
    handle = self._input_output_handles.get(input_node.node_id, "output-0")
    if handle != "output-0" and input_node._named_outputs:
        return input_node.get_output(handle)
    return input_node.get_resulting_data()
```

Downstream nodes connected to `output-1` will automatically receive the second named output. No changes needed here.

### 3.6 Backward compatibility

- Default `output_names = ["main"]` preserves current behavior
- Existing flows that don't set `output_names` continue to work unchanged
- `publish_output(df)` with no name argument publishes to `"main"` — works with both old and new

---

## 4. Frontend Changes

### 4.1 Dynamic output handles based on `output_names`

**File:** `flowfile_frontend/src/renderer/app/composables/useDragAndDrop.ts`

When creating a node, if the node's settings include `output_names`, the number of output handles should reflect `len(output_names)` rather than the static `NodeTemplate.output`:

```typescript
// In getNodeToAdd or addNode:
const outputCount = nodeData?.python_script_input?.output_names?.length
  ?? node.output;
outputs: Array.from({ length: outputCount }, (_, i) => ({
  id: `output-${i}`,
  position: Position.Right,
  label: nodeData?.python_script_input?.output_names?.[i] ?? undefined,
})),
```

### 4.2 Handle labels

Add optional labels to handles in `NodeWrapper.vue` to show names like "valid" / "invalid" next to output handles, and source names like "orders" / "customers" next to input handles:

```vue
<!-- Output handles with labels -->
<div v-for="(output, index) in data.outputs" :key="output.id" class="handle-output" ...>
  <span v-if="output.label" class="handle-label">{{ output.label }}</span>
  <Handle :id="output.id" type="source" :position="output.position" />
</div>
```

Handle labels should appear as small, unobtrusive text next to the handle dot. Styling:
- Font size: 0.65rem
- Color: secondary text color
- Position: offset left of the handle for outputs, offset right for inputs
- Truncated with ellipsis if longer than ~8 characters

### 4.3 Input handle labels from source `node_reference`

Input handles should display the name of the connected source node (its `node_reference`) so users know which name to pass to `read_input()`. This can be resolved from the edge data:

```typescript
// Computed property in NodeWrapper.vue:
const inputLabels = computed(() => {
  const labels: Record<string, string> = {};
  for (const edge of edges.value) {
    if (edge.target === String(props.data.id)) {
      const sourceNode = findNode(edge.source);
      const ref = sourceNode?.data?.nodeReference;
      if (ref && edge.targetHandle) {
        labels[edge.targetHandle] = ref;
      }
    }
  }
  return labels;
});
```

### 4.4 Output names editor in PythonScript.vue

Add a UI section in the Python Script settings panel to manage output names:

```
┌──────────────────────────────────┐
│ Outputs                          │
│  [main        ] [×]              │
│  [+ Add output]                  │
└──────────────────────────────────┘
```

This is a simple list editor where users can:
- Edit output names (validated for uniqueness and valid Python identifiers)
- Add new outputs
- Remove outputs (minimum 1)

When the list changes, the node's output handle count updates dynamically.

### 4.5 Autocomplete / helper in code editor

**File:** `flowfile_frontend/.../pythonScript/flowfileCompletions.ts`

Update the CodeMirror completions to suggest available input names:

```typescript
// When completing flowfile.read_input("..., suggest connected input names
{
  label: `flowfile.read_input("${inputName}")`,
  type: "function",
  detail: `Read input from ${sourceNodeName}`,
}
```

### 4.6 API Help dialog update

**File:** `flowfile_frontend/.../pythonScript/FlowfileApiHelp.vue`

Update the documentation to mention named inputs/outputs more prominently, with examples:

```html
<div class="api-item">
  <code>flowfile.read_input("orders")</code>
  <p>Read a named input by its source node reference.</p>
</div>
```

---

## 5. Serialization & Persistence

### 5.1 YAML flow file format

The `FlowfileNode` already stores `setting_input` which serializes `PythonScriptInput`. Adding `output_names` to `PythonScriptInput` will be picked up automatically by the YAML serializer. Existing flows without `output_names` will use the Pydantic default `["main"]`.

### 5.2 Edge metadata

No changes to edge serialization. Edges already store `sourceHandle` and `targetHandle` (e.g., `output-0`, `input-0`). Named outputs map to handle indices: `output_names[0]` → `output-0`, `output_names[1]` → `output-1`.

---

## 6. Implementation Phases

### Phase 1: Named Inputs (Backend)
1. Update `write_inputs_to_parquet` to accept `input_names` parameter
2. Update `add_python_script` to resolve `node_reference` from input nodes
3. Update `_make_kernel_user_defined_func` similarly
4. Add backward-compatible `"main"` alias in `read_input()` for single-input case
5. Add tests for named input resolution

### Phase 2: Multiple Outputs (Backend)
1. Add `output_names` to `PythonScriptInput`
2. Update `add_python_script` to read multiple output files
3. Update `node_template.output` dynamically
4. Ensure `_named_outputs` is populated correctly
5. Add tests for multi-output Python Script nodes

### Phase 3: Frontend — Handle Labels & Dynamic Outputs
1. Add label support to handles in `NodeWrapper.vue`
2. Show input labels from connected source `node_reference`
3. Show output labels from `output_names`
4. Update `useDragAndDrop.ts` to create dynamic output handle count

### Phase 4: Frontend — Output Names Editor & UX
1. Add output names editor to `PythonScript.vue`
2. Update CodeMirror completions to suggest input names
3. Update `FlowfileApiHelp.vue` with named I/O examples
4. Handle node reconnection: refresh input labels when edges change

---

## 7. Files Changed (Summary)

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/kernel/execution.py` | Add `input_names` param to `write_inputs_to_parquet` |
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | Resolve input names from `node_reference`; read multi outputs |
| `flowfile_core/flowfile_core/schemas/input_schema.py` | Add `output_names` to `PythonScriptInput` |
| `kernel_runtime/kernel_runtime/flowfile_client.py` | Auto-resolve single-input to `"main"` fallback |
| `flowfile_frontend/.../NodeWrapper.vue` | Handle labels (input & output) |
| `flowfile_frontend/.../PythonScript.vue` | Output names editor UI |
| `flowfile_frontend/.../useDragAndDrop.ts` | Dynamic output handle count |
| `flowfile_frontend/.../flowfileCompletions.ts` | Input name suggestions |
| `flowfile_frontend/.../FlowfileApiHelp.vue` | Updated documentation |
| `flowfile_frontend/src/renderer/app/types/canvas.types.ts` | Optional `label` on handle types |

---

## 8. API Examples (After Implementation)

### Single input (backward-compatible)
```python
# One upstream node connected — works exactly as before
df = flowfile.read_input()
flowfile.publish_output(df.filter(pl.col("active")))
```

### Named inputs
```python
# Two upstream nodes: "orders" and "customers" (set via node_reference)
orders = flowfile.read_input("orders")
customers = flowfile.read_input("customers")
result = orders.join(customers, on="customer_id")
flowfile.publish_output(result)
```

### Multiple outputs
```python
# Configure output_names: ["valid", "invalid"] in settings
df = flowfile.read_input()
flowfile.publish_output(df.filter(pl.col("amount") > 0), "valid")
flowfile.publish_output(df.filter(pl.col("amount") <= 0), "invalid")
```

### All inputs at once
```python
# Access all inputs as a dict
inputs = flowfile.read_inputs()
for name, frames in inputs.items():
    print(f"Input '{name}': {len(frames)} frames")
```

---

## 9. Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Existing flows break when inputs get new names | Auto-alias `"main"` for single-input nodes; `read_input()` falls back to sole available input |
| `node_reference` conflicts (two sources with same name) | Dedup with `_{idx}` suffix; validate at connection time |
| Output names contain invalid characters | Validate as valid Python identifiers in UI and backend |
| Custom nodes using `generate_kernel_code()` | Already supports named inputs via `read_inputs()`; just needs updated `write_inputs_to_parquet` call |
| Performance: extra parquet writes | Negligible — same number of files, just different names |
