# Feature 2: Conditional Execution

## Motivation

Real-world ETL pipelines need flow-level branching: "if the data meets this condition, run these nodes; otherwise, run those nodes." This is a **Python if/else at the flow level**, not a row-level filter. Examples:

- If `df.count() == 12` (expected monthly records), proceed with processing; otherwise, run an error-handling branch.
- If a required column exists in the schema, run enrichment; otherwise, skip it.
- If the data passes validation checks, write to production; otherwise, write to a quarantine table.

Today Flowfile's `filter` node can subset rows, but there's no way to route the **entire DataFrame** down one path or another based on a dataset-level condition. Users must create separate flows or manually check conditions outside Flowfile.

Conditional execution introduces a **Condition** container node that evaluates a condition against the whole DataFrame and executes **only the matching branch** — the entire DataFrame goes to one branch, not both.

## Current State

- **`filter` node**: Row-level subsetting. Outputs rows where the expression is true. This is fundamentally different from flow-level branching.
- **No branching**: The execution engine executes all nodes in topological order. There is no mechanism to skip a subset of nodes based on a runtime condition.
- **`FlowfileNode`** (`schemas.py:227`): No `parent_node_id` field — all nodes are peers.
- **`ExecutionStage`** (`execution_orderer.py:10`): Flat list of nodes, no conditional logic.
- **VueFlow frontend**: Supports `parentNode` natively for visual containment, but the backend has no counterpart.

## Proposed Design

### Core Semantics: Flow-Level If/Else

The condition node evaluates an expression against the **entire input DataFrame** (or its properties like count, schema, column values). The result is a boolean. The entire DataFrame is routed to exactly one branch:

```python
# Conceptual execution:
if df.count() == 12:
    result = if_branch.process(df)       # entire df goes here
else:
    result = else_branch.process(df)     # OR here — never both
```

This is NOT row-level splitting. The `filter` node already handles that. Condition nodes are control flow.

### Storage Model: Option A (Parent Pointer)

Add `parent_node_id: int | None = None` to `FlowfileNode`. The condition container is a normal node with type `condition`. Its child nodes carry `parent_node_id` pointing back to it, plus a `branch_id` to identify which branch they belong to.

```yaml
nodes:
  - id: 5
    type: read
    outputs: [10]

  - id: 10
    type: condition
    input_ids: [5]
    outputs: [20]
    setting_input:
      condition_expression: "df.count() == 12"
      branches:
        - branch_id: "if"
          label: "Expected row count"
        - branch_id: "else"
          label: "Unexpected data"

  # Children of condition node 10, "if" branch
  # These receive the ENTIRE input df when the condition is True
  - id: 11
    type: formula
    parent_node_id: 10
    branch_id: "if"
    x_position: 140        # relative to parent
    y_position: 50
    input_ids: []           # implicit: receives data from parent
    outputs: [12]

  - id: 12
    type: select
    parent_node_id: 10
    branch_id: "if"
    x_position: 300
    y_position: 50
    input_ids: [11]
    outputs: []             # terminal node of this branch

  # Children of condition node 10, "else" branch
  # These receive the ENTIRE input df when the condition is False
  - id: 13
    type: catalog_writer
    parent_node_id: 10
    branch_id: "else"
    x_position: 140
    y_position: 200
    input_ids: []
    setting_input:
      catalog_table_name: "quarantine_data"
    outputs: []

  # Downstream of condition (receives output from whichever branch ran)
  - id: 20
    type: output
    input_ids: [10]
```

### Schema Changes

**`FlowfileNode`** (`schemas.py`):

```python
class FlowfileNode(BaseModel):
    id: int
    type: str
    # ... existing fields ...
    parent_node_id: int | None = None    # NEW: container node this belongs to
    branch_id: str | None = None         # NEW: which branch inside the container
```

**New Pydantic model** (`input_schema.py`):

```python
class ConditionBranch(BaseModel):
    branch_id: str                        # "if", "else", or user-defined
    label: str = ""                       # display label

class NodeCondition(NodeSingleInput):
    condition_expression: str             # expression evaluated against the DataFrame
    branches: list[ConditionBranch] = [
        ConditionBranch(branch_id="if", label="True"),
        ConditionBranch(branch_id="else", label="False"),
    ]
```

**Condition expression examples**:

```python
# Row count checks
"df.count() == 12"
"df.count() > 0"

# Schema checks
"'expected_column' in df.columns"

# Aggregate checks
"df.select(pl.col('amount').sum()).item() > 10000"

# Null checks
"df.select(pl.col('id').null_count()).item() == 0"
```

**New node template** (`nodes.py`):

```python
NodeTemplate(
    name="Condition",
    item="condition",
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

1. When building the execution plan, identify nodes with `parent_node_id` and group them by their container.
2. Exclude child nodes from the main topological sort — they are executed as part of their container's stage.
3. Add `ConditionalExecutionStage`:

```python
@dataclass(frozen=True)
class ConditionalExecutionStage:
    """A stage that evaluates a condition and executes only the matching branch."""
    container_node: FlowNode
    branches: dict[str, list[FlowNode]]   # branch_id → ordered child nodes
```

**`flow_graph.py`** — conditional execution path:

```
1. Get the input DataFrame
2. Evaluate condition_expression against it → boolean result
3. Determine which branch to execute:
   - True  → execute the "if" branch's child nodes
   - False → execute the "else" branch's child nodes
4. The NON-matching branch is skipped entirely (no execution, no resource use)
5. Inject the ENTIRE input DataFrame as input to the first child node of the matching branch
6. Execute the matching branch's child nodes in order
7. Collect the terminal node's output
8. Pass result to downstream nodes via the container's outputs
```

**Key difference from filter**: The entire DataFrame goes to one branch. The other branch does NOT execute. This is true conditional control flow.

**Condition evaluation**: The expression runs in a controlled context where `df` refers to the input DataFrame and `pl` refers to polars. The expression must evaluate to a boolean scalar.

```python
def evaluate_condition(df: pl.DataFrame, expression: str) -> bool:
    context = {"df": df, "pl": pl}
    result = eval(expression, {"__builtins__": {}}, context)
    if not isinstance(result, bool):
        raise ValueError(f"Condition must evaluate to bool, got {type(result)}")
    return result
```

### Code Generation

Condition nodes generate Python if/else with the branch logic in separate modules:

```python
# main_flow.py
from pipeline_name.branch_if_validation import process as handle_valid
from pipeline_name.branch_else_validation import process as handle_invalid

df = read_from_catalog("production", "raw_data")

if df.count() == 12:
    result = handle_valid(df)
else:
    result = handle_invalid(df)

write_to_catalog(result, "production", "output")
```

```python
# branch_if_validation.py
def process(df: pl.DataFrame) -> pl.DataFrame:
    """Branch: Expected row count."""
    df = df.with_columns(pl.col("amount").round(2))
    return df.select(["id", "name", "amount"])
```

```python
# branch_else_validation.py
def process(df: pl.DataFrame) -> pl.DataFrame:
    """Branch: Unexpected data — write to quarantine."""
    write_to_catalog(df, "production", "quarantine_data")
    return df
```

### Frontend Changes

**VueFlow rendering**:
- The `condition` node renders as a container with labeled branch zones ("If" / "Else").
- Child nodes are rendered inside their branch zone using VueFlow's `parentNode` field.
- Positions are relative to the container.
- Drag-and-drop: users can drag nodes into a branch zone to assign them to that branch.

**Visual indicators**:
- The condition expression is displayed on the container header.
- Branch labels are shown above each zone.
- During execution, the active branch highlights while the skipped branch dims.

**Node configuration panel**:
- `condition_expression`: Code editor (CodeMirror) with Python expression syntax. Autocomplete for `df.count()`, `df.columns`, `df.select(...)`, etc.
- Branch list: Add/remove/rename branches.
- Expression help: examples of common conditions (count checks, schema checks, aggregate checks).

### Serialization Round-trip

- **Save**: `parent_node_id` and `branch_id` are persisted on each child `FlowfileNode` in the flat list. The condition node's `setting_input` stores the expression and branch definitions.
- **Load**: The frontend reconstructs the visual grouping by reading `parent_node_id` and `branch_id` from each node, then setting VueFlow's `parentNode` accordingly.
- This is minimal schema impact — only two nullable fields added to `FlowfileNode`.

### Schema Inference

- Only the "if" branch's output schema is used for downstream schema inference (it's the "expected" path).
- If the else branch produces a different schema, a warning is surfaced.
- Schema inference does not require executing the condition — it traces the "if" branch statically.

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/schemas/schemas.py` | Add `parent_node_id`, `branch_id` to `FlowfileNode` |
| `flowfile_core/flowfile_core/schemas/input_schema.py` | Add `NodeCondition`, `ConditionBranch` models |
| `flowfile_core/flowfile_core/configs/node_store/nodes.py` | Add `condition` node template |
| `flowfile_core/flowfile_core/flowfile/util/execution_orderer.py` | Add `ConditionalExecutionStage`, exclude children from main sort |
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | Add condition evaluation and single-branch execution logic |
| `flowfile_core/flowfile_core/flowfile/flow_node/executor.py` | Handle `condition` execution strategy |
| `flowfile_core/flowfile_core/flowfile/code_generator/` | Generate if/else with branch modules |
| `flowfile_frontend/` | VueFlow container with branch zones, parentNode mapping |

## Open Questions

1. **Multi-way branching**: Should conditions support more than if/else? E.g., a switch/case: "if count == 12, elif count == 6 (semi-annual), else error." The `branches` list model supports this — each branch would need its own condition expression except the last (default/else).
2. **Expression safety**: Using `eval()` for condition expressions is flexible but risky. Should conditions be restricted to a safe subset (e.g., only `df.count()`, `df.columns`, `df.select().item()`)? Or is kernel-level sandboxing sufficient?
3. **Branch with no output**: If the else branch writes to quarantine but doesn't return data (a dead end), what does the condition node pass downstream? `None`? An empty DataFrame?
4. **Nested conditions**: A condition node inside another condition node is technically possible with parent pointers. Should this be supported from day one?
5. **Parameter integration**: Flow Parameters has shipped — condition expressions should support `${param}` references for parameter-driven branching (e.g., `df.count() == ${expected_count}`).
