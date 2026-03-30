# Feature 1: Iterative Nodes

## Motivation

Many ETL workflows need to process data partitions independently — running the same transformation pipeline per customer, per region, per date, or per file. Today Flowfile has `group_by` for aggregation, but no way to execute a full sub-graph per group. Users must either duplicate nodes manually for each partition or fall back to custom Python code.

Iterative nodes introduce a **ForEach** container that partitions input data and executes an embedded sub-flow once per partition, then collects the results.

## Current State

- **No iteration construct**: The execution engine (`execution_orderer.py`) treats all nodes as a flat DAG. There is no concept of a node containing other nodes or executing a sub-graph multiple times.
- **`group_by` node**: Performs aggregation but does not allow arbitrary per-group transformation pipelines.
- **`FlowfileNode`** (`schemas.py:227`): Flat model — no `sub_flow` field, no parent concept.
- **`ExecutionStage`** (`execution_orderer.py:10`): A frozen dataclass holding a list of `FlowNode` objects for parallel execution. No variant for iterative execution.
- **`FlowfileData`** (`schemas.py:271`): Root serialization model with a flat `nodes` list.

## Proposed Design

### Storage Model: Option B (Embedded Sub-flow)

The `for_each` container node's `setting_input` holds a nested `FlowfileData` — a complete, self-contained sub-flow. The outer flat node list only contains the container node itself; its interior is opaque to the outer graph.

```yaml
nodes:
  - id: 5
    type: read
    outputs: [10]

  - id: 10
    type: for_each
    input_ids: [5]
    outputs: [20]
    setting_input:
      iterate_over: "region"          # column to partition by
      collect_mode: "concat"          # how to combine results: concat | list
      max_parallel: 4                 # concurrent partition executions
      sub_flow:
        flowfile_version: "0.6.3"
        flowfile_id: -1               # internal, not registered
        flowfile_name: "for_each_body"
        flowfile_settings: { ... }
        nodes:
          - id: 1
            type: filter
            setting_input: { ... }
            outputs: [2]
          - id: 2
            type: formula
            input_ids: [1]
            setting_input: { ... }
            outputs: []

  - id: 20
    type: output
    input_ids: [10]
```

### Schema Changes

**New Pydantic model** (`input_schema.py`):

```python
class NodeForEach(NodeSingleInput):
    iterate_over: str                          # column name or expression to partition by
    collect_mode: Literal["concat", "list"] = "concat"
    max_parallel: int = 4
    sub_flow: FlowfileData                     # embedded sub-flow definition
```

**New node template** (`nodes.py`):

```python
NodeTemplate(
    name="For Each",
    item="for_each",
    input=1,
    output=1,
    transform_type="wide",
    node_type="process",
    node_group="control_flow",
    prod_ready=False,
    multi=False,
    can_be_start=False,
)
```

### Execution Engine Changes

**`execution_orderer.py`**:

1. Add `IterativeExecutionStage` dataclass:

```python
@dataclass(frozen=True)
class IterativeExecutionStage:
    """A stage that executes an embedded sub-flow once per partition."""
    container_node: FlowNode
    sub_flow_data: FlowfileData
    iterate_over: str
    collect_mode: str
    max_parallel: int
```

2. In `compute_execution_plan()`, detect `for_each` nodes and emit an `IterativeExecutionStage` instead of including them in a regular `ExecutionStage`.

**`flow_graph.py`** — new execution path:

```
1. Evaluate container node inputs (get the input DataFrame)
2. Partition the DataFrame by `iterate_over` column
3. For each partition:
   a. Build a temporary FlowGraph from `sub_flow`
   b. Inject the partition DataFrame as the sub-flow's start node input
   c. Execute the sub-flow graph
   d. Collect the output DataFrame
4. Combine results based on `collect_mode`:
   - "concat": pl.concat(results)
   - "list": wrap results as a list column
5. Pass combined result to downstream nodes
```

**Concurrency**: Use the existing `ThreadPoolExecutor` with `max_parallel` controlling how many partitions execute simultaneously.

### Frontend Changes

**VueFlow rendering**:
- The `for_each` node renders as an expanded container with a visible boundary.
- The embedded sub-flow nodes render inside the container using VueFlow's native `parentNode` field for visual grouping.
- Positions of inner nodes are relative to the container's position.
- Users can drag nodes into/out of the container to add/remove them from the sub-flow.

**Node configuration panel**:
- `iterate_over`: Column selector dropdown populated from input schema.
- `collect_mode`: Radio toggle (Concat / List).
- `max_parallel`: Numeric slider (1–16).

### Serialization Round-trip

- **Save**: The `for_each` node's `setting_input` serializes the `sub_flow` field as a nested YAML/JSON object (standard `FlowfileData` serialization).
- **Load**: `NodeForEach.sub_flow` is deserialized as a `FlowfileData` model, which the frontend reconstructs into visual child nodes inside the container.
- The sub-flow's `flowfile_id` is set to `-1` (or a sentinel) to indicate it's not an independently registered flow.

### Schema Inference

The container node's output schema is inferred by:
1. Taking the input schema
2. Running schema inference on the embedded sub-flow (tracing column transformations)
3. Wrapping the result based on `collect_mode`

This allows downstream nodes to see correct column types without executing the iteration.

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/schemas/schemas.py` | No change (FlowfileData already works as nested model) |
| `flowfile_core/flowfile_core/schemas/input_schema.py` | Add `NodeForEach` model |
| `flowfile_core/flowfile_core/configs/node_store/nodes.py` | Add `for_each` node template |
| `flowfile_core/flowfile_core/flowfile/util/execution_orderer.py` | Add `IterativeExecutionStage`, detect `for_each` in plan computation |
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | Add scatter-gather execution logic for `IterativeExecutionStage` |
| `flowfile_core/flowfile_core/flowfile/flow_node/executor.py` | Handle `for_each` node execution strategy |
| `flowfile_frontend/` | VueFlow container rendering, node config panel |

## Open Questions

1. **Nested iteration**: Should `for_each` nodes be allowed inside other `for_each` nodes? The embedded sub-flow model supports this naturally, but execution complexity grows.
2. **Error handling per partition**: If one partition fails, should the entire iteration fail or continue with partial results?
3. **Schema mismatch**: What happens if different partitions produce different output schemas? Strict mode (fail) vs. lenient mode (union with nulls)?
4. **Progress tracking**: How to report per-partition progress to the frontend?
