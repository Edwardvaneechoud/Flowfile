# Feature 2: Conditional Execution

## Motivation

Real-world ETL pipelines need branching logic: route data differently based on conditions, skip processing steps when criteria aren't met, or apply different transformations to different subsets. Today Flowfile's `filter` node can subset rows, but there is no way to express "if this condition, run these nodes; otherwise, run those nodes." Users must create separate flows or use workarounds with filters and unions.

Conditional execution introduces a **Condition** container node with inline branch sub-graphs, enabling if/else routing within a single flow.

## Current State

- **`filter` node**: Subsets rows based on an expression, but outputs a single stream (no else branch).
- **No branching**: The execution engine executes all nodes in topological order. There is no mechanism to skip a subset of nodes based on a runtime condition.
- **`FlowfileNode`** (`schemas.py:227`): No `parent_node_id` field — all nodes are peers.
- **`ExecutionStage`** (`execution_orderer.py:10`): Flat list of nodes, no conditional logic.
- **VueFlow frontend**: Supports `parentNode` natively for visual containment, but the backend has no counterpart.

## Proposed Design

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
      condition_expression: "col('status') == 'active'"
      branches:
        - branch_id: "if"
          label: "Active records"
        - branch_id: "else"
          label: "Inactive records"

  # Children of condition node 10, "if" branch
  - id: 11
    type: formula
    parent_node_id: 10
    branch_id: "if"
    x_position: 140        # relative to parent
    y_position: 50
    input_ids: []           # implicit: receives data from parent's "if" route
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
  - id: 13
    type: filter
    parent_node_id: 10
    branch_id: "else"
    x_position: 140
    y_position: 200
    input_ids: []
    outputs: []

  # Downstream of condition (receives merged output)
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
    condition_expression: str             # Polars expression string
    branches: list[ConditionBranch] = [
        ConditionBranch(branch_id="if", label="True"),
        ConditionBranch(branch_id="else", label="False"),
    ]
    merge_output: bool = True             # whether to concat branch outputs
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
    """A stage that evaluates a condition and executes the matching branch."""
    container_node: FlowNode
    branches: dict[str, list[FlowNode]]   # branch_id → ordered child nodes
```

**`flow_graph.py`** — conditional execution path:

```
1. Evaluate condition_expression against the input DataFrame
2. Split data:
   - "if" branch: rows where condition is True
   - "else" branch: rows where condition is False
3. For each branch with data:
   a. Identify the branch's child nodes (filter by parent_node_id + branch_id)
   b. Build a mini execution plan from those nodes
   c. Inject the branch's data subset as input to the first child node
   d. Execute the child nodes in order
   e. Collect the terminal node's output
4. If merge_output is True:
   - pl.concat() the branch outputs
   - Pass to downstream nodes via the container's outputs
5. If merge_output is False:
   - Each branch's terminal nodes connect directly to different downstream nodes
```

### Frontend Changes

**VueFlow rendering**:
- The `condition` node renders as a container with labeled branch zones ("If" / "Else").
- Child nodes are rendered inside their branch zone using VueFlow's `parentNode` field.
- Positions are relative to the container.
- Drag-and-drop: users can drag nodes into a branch zone to assign them to that branch.

**Visual indicators**:
- The condition expression is displayed on the container header.
- Branch labels ("Active records" / "Inactive records") are shown above each zone.
- During execution, the active branch highlights while the inactive branch dims.

**Node configuration panel**:
- `condition_expression`: Code editor (CodeMirror) with Polars expression syntax.
- Branch list: Add/remove/rename branches.
- `merge_output`: Toggle switch.

### Serialization Round-trip

- **Save**: `parent_node_id` and `branch_id` are persisted on each child `FlowfileNode` in the flat list. The condition node's `setting_input` stores the expression and branch definitions.
- **Load**: The frontend reconstructs the visual grouping by reading `parent_node_id` and `branch_id` from each node, then setting VueFlow's `parentNode` accordingly.
- This is minimal schema impact — only two nullable fields added to `FlowfileNode`.

### Schema Inference

- Each branch's output schema is inferred independently by tracing the child nodes.
- If `merge_output` is True, the container's output schema is the union of all branch schemas (with nulls for missing columns).
- If branches produce incompatible schemas, a warning is surfaced in the UI.

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/schemas/schemas.py` | Add `parent_node_id`, `branch_id` to `FlowfileNode` |
| `flowfile_core/flowfile_core/schemas/input_schema.py` | Add `NodeCondition`, `ConditionBranch` models |
| `flowfile_core/flowfile_core/configs/node_store/nodes.py` | Add `condition` node template |
| `flowfile_core/flowfile_core/flowfile/util/execution_orderer.py` | Add `ConditionalExecutionStage`, exclude children from main sort |
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | Add branch evaluation and per-branch execution logic |
| `flowfile_core/flowfile_core/flowfile/flow_node/executor.py` | Handle `condition` execution strategy |
| `flowfile_frontend/` | VueFlow container with branch zones, parentNode mapping |

## Open Questions

1. **Multi-way branching**: Should conditions support more than if/else (e.g., switch/case with multiple expressions)? The `branches` list model supports this, but the UI and expression evaluation would need to handle N conditions.
2. **Empty branches**: What happens when a branch receives zero rows? Skip execution entirely, or execute with an empty DataFrame?
3. **Cross-branch references**: Can a node in the "else" branch reference data from the "if" branch? (Proposed: no — branches are isolated.)
4. **Nested conditions**: A condition node inside another condition node is technically possible with parent pointers. Should this be supported from day one?
5. **Parameter integration**: Once Flow Parameters (feature 4) exists, condition expressions should support `{{param}}` references for parameter-driven branching.
