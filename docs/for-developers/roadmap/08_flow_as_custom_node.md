# Feature 8: Flow as Custom Node

## Motivation

As Flowfile users build more pipelines, common patterns emerge — a standardized data cleaning sequence, a specific join-and-enrich pattern, a validation pipeline. Today these must be duplicated across flows. There is no way to encapsulate a flow and reuse it as a building block inside other flows.

Flow as Custom Node enables **composition**: register a flow in the catalog, then use it as a node in other flows. This turns flows into reusable, versioned components with defined inputs and outputs.

## Current State

- **Flows are standalone**: `FlowfileData` is a self-contained definition with no referencing mechanism. Each flow is independent.
- **Catalog flow registration**: Flows can be registered in the catalog (`catalog/service.py`), with metadata, versioning, and namespace organization. But registration is for management/discovery, not for embedding in other flows.
- **`user_defined` node type**: Exists in `nodes.py` for custom nodes. Could potentially be extended for flow-as-node, but currently targets `CustomNodeBase` subclasses only.
- **`FlowGraph` execution**: Instantiated from `FlowfileData` and executed as a unit. No support for injecting data into arbitrary entry points or extracting data from arbitrary exit points.
- **Code generator**: Generates standalone scripts. No support for generating a callable function from a flow.

## Proposed Design

### Storage Model: Option C (Referenced Flow)

The parent flow contains a `sub_flow_node` that references a catalog-registered flow by ID. The referenced flow is a completely separate `FlowfileData` file — not embedded, not copied.

```yaml
# Parent flow
nodes:
  - id: 1
    type: read
    setting_input:
      file_path: "/data/raw_orders.csv"
    outputs: [10]

  - id: 10
    type: sub_flow_node
    input_ids: [1]
    outputs: [20]
    setting_input:
      referenced_flow_id: 42             # catalog-registered flow
      referenced_flow_version: 3         # pinned version (optional)
      input_mapping:
        - flow_input_name: "raw_data"    # sub-flow's input port name
          source_node_id: 1              # parent flow's source
      output_mapping:
        - flow_output_name: "cleaned"    # sub-flow's output port name
          output_index: 0                # which output port of this node
      parameter_values:                  # sub-flow parameters (Feature 4)
        threshold: 100
        region: "EMEA"

  - id: 20
    type: output
    input_ids: [10]
```

### Referenced Flow Structure

A flow intended for reuse must declare its **input and output ports**:

```yaml
# Referenced flow (catalog ID: 42)
flowfile_version: "0.6.3"
flowfile_id: 42
flowfile_name: "Data Cleaning Pipeline"
flowfile_settings:
  parameters:
    - name: "threshold"
      type: "float"
      default_value: 50
    - name: "region"
      type: "string"
      required: false
  flow_ports:                            # NEW
    inputs:
      - name: "raw_data"
        node_id: 1                       # which node receives the input
        description: "Raw data to clean"
    outputs:
      - name: "cleaned"
        node_id: 5                       # which node produces the output
        description: "Cleaned data"
nodes:
  - id: 1
    type: polars_lazy_frame              # input port node (receives data from parent)
    is_start_node: true
    outputs: [2]
  # ... transformation nodes ...
  - id: 5
    type: output
    input_ids: [4]
```

### Schema Changes

**`FlowfileSettings`** extension:

```python
class FlowPort(BaseModel):
    name: str                     # port identifier
    node_id: int                  # which node this port maps to
    description: str = ""
    schema: list[FlowfileColumn] | None = None  # expected schema (optional)

class FlowPorts(BaseModel):
    inputs: list[FlowPort] = []
    outputs: list[FlowPort] = []

class FlowfileSettings(BaseModel):
    # ... existing fields ...
    flow_ports: FlowPorts | None = None   # NEW: declared I/O ports for reuse
```

**New node settings** (`input_schema.py`):

```python
class InputMapping(BaseModel):
    flow_input_name: str          # name of the sub-flow's input port
    source_node_id: int           # node in the parent flow providing data

class OutputMapping(BaseModel):
    flow_output_name: str         # name of the sub-flow's output port
    output_index: int = 0         # which output slot of the sub_flow_node

class NodeSubFlow(NodeBase):
    referenced_flow_id: int
    referenced_flow_version: int | None = None   # None = latest
    input_mapping: list[InputMapping] = []
    output_mapping: list[OutputMapping] = []
    parameter_values: dict[str, Any] = {}
```

**New node template** (`nodes.py`):

```python
NodeTemplate(
    name="Sub-flow",
    item="sub_flow_node",
    input=10,                    # multi-input (up to 10)
    output=1,                    # configurable via output_mapping
    transform_type="wide",
    node_type="process",
    node_group="control_flow",
    prod_ready=False,
    multi=True,
    can_be_start=False,
)
```

### Execution Engine Changes

**`flow_graph.py`** — sub-flow execution:

```
1. Load the referenced flow from catalog (by ID + optional version)
2. Resolve parameter_values against the sub-flow's parameter definitions
3. Build a FlowGraph from the referenced FlowfileData
4. For each input_mapping:
   a. Get the parent node's output DataFrame
   b. Inject it as the data for the sub-flow's input port node
5. Execute the sub-flow graph
6. For each output_mapping:
   a. Extract the output DataFrame from the sub-flow's output port node
   b. Pass it to the corresponding output slot of the sub_flow_node
7. Continue parent flow execution with the sub-flow's outputs
```

**Caching**: The sub-flow's execution result can be cached at the `sub_flow_node` level. If the input data and parameters haven't changed, skip re-execution.

**Schema inference**: The sub-flow's output schema is inferred by loading the flow definition and tracing schema through its nodes. If the sub-flow's output ports have declared schemas, use those directly.

### Frontend Changes

**Flow port configuration** (flow settings panel):
- "Ports" tab for declaring input and output ports.
- Map each port to a node in the flow.
- Port names, descriptions, and optional schema constraints.
- Visual indicators on port nodes (e.g., colored border, port icon).

**Sub-flow node configuration**:
- Flow picker: Browse catalog flows, select one.
- Version selector: Pin to a specific version or use "latest".
- Input mapping grid: Map parent flow nodes to sub-flow input ports.
- Output mapping grid: Map sub-flow output ports to this node's outputs.
- Parameter values form: Auto-generated from the referenced flow's parameter definitions (uses same UI as Feature 4's parameter prompt).

**Visual representation**:
- The sub-flow node shows a compact preview of the referenced flow's structure.
- Double-click to open the referenced flow in a new tab.
- Badge showing the flow name and version.

**Catalog integration**:
- Flows with declared ports show a "Reusable" badge in the catalog.
- "Use as Node" action in the catalog to insert a `sub_flow_node` into the current flow.

### Code Generation

The code generator should emit the sub-flow as a function call:

```python
# Generated code for the referenced flow
def data_cleaning_pipeline(raw_data: pl.LazyFrame, threshold: float = 50, region: str = None) -> pl.LazyFrame:
    df = raw_data
    df = df.filter(pl.col("amount") > threshold)
    # ... rest of the pipeline ...
    return df

# Parent flow usage
df_raw = pl.scan_csv("/data/raw_orders.csv")
df_cleaned = data_cleaning_pipeline(df_raw, threshold=100, region="EMEA")
df_cleaned.sink_parquet("/data/cleaned_orders.parquet")
```

### Integration with Other Features

- **Feature 4 (Flow Parameters)**: Sub-flow parameters become the node's configuration inputs. The parameter form is reused.
- **Feature 7 (Custom Node Designer)**: A flow-as-node appears in the node palette similarly to custom nodes. The designer could offer a "Create from Flow" option.
- **Feature 9 (Code Generation)**: The referenced flow's code is generated as a function, and the `sub_flow_node` emits a function call.

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/schemas/schemas.py` | Add `FlowPort`, `FlowPorts`; extend `FlowfileSettings` |
| `flowfile_core/flowfile_core/schemas/input_schema.py` | Add `NodeSubFlow`, `InputMapping`, `OutputMapping` |
| `flowfile_core/flowfile_core/configs/node_store/nodes.py` | Add `sub_flow_node` template |
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | Sub-flow loading, execution, data injection/extraction |
| `flowfile_core/flowfile_core/flowfile/flow_node/executor.py` | Execution strategy for `sub_flow_node` |
| `flowfile_core/flowfile_core/catalog/service.py` | Load flow by ID + version; port metadata queries |
| `flowfile_core/flowfile_core/flowfile/code_generator/code_generator.py` | Emit sub-flows as functions |
| `flowfile_frontend/` | Flow port config, sub-flow node UI, catalog integration |

## Open Questions

1. **Circular references**: Flow A uses Flow B as a node, and Flow B uses Flow A. This must be detected and prevented. Proposed: catalog enforces a DAG of flow references.
2. **Version management**: When a referenced flow is updated, should flows using it auto-update or stay pinned? Proposed: pin by default, offer "update available" notification.
3. **Schema compatibility**: If the referenced flow's input schema changes, parent flows may break. Should port schemas be enforced contracts? Proposed: warn on mismatch, fail on incompatible types.
4. **Multi-output**: A sub-flow can have multiple output ports. How does VueFlow handle a node with dynamic output count? Proposed: output handles are generated based on `output_mapping` length.
5. **Performance**: Loading and building a sub-flow graph adds overhead. For hot paths, should the sub-flow's execution plan be cached across parent flow runs?
6. **Recursive depth**: Should there be a limit on nesting depth (sub-flow inside sub-flow inside sub-flow)? Proposed: limit to 5 levels, configurable.
