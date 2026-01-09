# Node Enhancement Features - Requirements

This document defines requirements for three interconnected node enhancements:
1. Error Handling with Output Behavior
2. Multiple Node Outputs
3. Schema Enforcement

---

## 1. Error Handling with Output Behavior

### Overview
Allow nodes to define what happens when an error occurs during execution. Instead of always failing the flow, nodes can optionally output a structured error dataframe that downstream nodes can process.

### Current Behavior
- When a node errors, the exception is caught and stored in `NodeResults.errors`
- All downstream nodes are added to a skip list
- The flow continues but dependent paths are not executed

### Proposed Behavior
Each node can configure an `on_error` behavior with three modes:

| Mode | Behavior |
|------|----------|
| `fail` | Current behavior. Raise error, skip downstream nodes. (Default) |
| `output_error` | Output a structured error dataframe, continue flow normally |
| `skip` | Output empty dataframe, continue flow normally |

### Data Models

```python
# Add to input_schema.py

class OnErrorBehavior(BaseModel):
    """Defines what happens when a node encounters an error."""

    action: Literal["fail", "output_error", "skip"] = "fail"
    include_input_data: bool = False  # When True, append error columns to input data

# Add to NodeBase
class NodeBase(BaseModel):
    # ... existing fields ...
    on_error: OnErrorBehavior | None = None  # None means use default (fail)
```

### Error Output Schema

When `action="output_error"`, the node outputs a dataframe with these columns:

| Column | Type | Description |
|--------|------|-------------|
| `_error` | Boolean | Always `True` for error rows |
| `_error_message` | String | The exception message |
| `_error_type` | String | The exception class name (e.g., `ValueError`) |
| `_error_node_id` | Int64 | The node_id where error occurred |
| `_error_node_type` | String | The node type (e.g., `filter`, `join`) |

When `include_input_data=True`:
- If input data is available, append error columns to the first input dataframe
- Each row gets the same error information
- Useful for debugging which data caused the error

When `include_input_data=False`:
- Output a single-row dataframe with only error columns
- Lightweight, just indicates an error occurred

### Implementation Changes

#### flow_node.py - execute_node method
```python
def execute_node(self, ...):
    try:
        result = self._function(*input_data)
        self.results.resulting_data = result
    except Exception as e:
        on_error = getattr(self.setting_input, 'on_error', None)

        if on_error is None or on_error.action == "fail":
            # Current behavior
            self.results.errors = str(e)
            raise e

        elif on_error.action == "output_error":
            self.results.resulting_data = self._create_error_dataframe(
                exception=e,
                input_data=input_data if on_error.include_input_data else None
            )
            self.results.errors = None  # Don't mark as failed

        elif on_error.action == "skip":
            self.results.resulting_data = FlowDataEngine()  # Empty
            self.results.errors = None
```

#### flow_graph.py - run_graph method
```python
# Modify the success check
node_result.success = node.results.errors is None

# Remove from skip_nodes logic when on_error handles the error
if not node_result.success:
    skip_nodes.extend(list(node.get_all_dependent_nodes()))
# When on_error is set and handles error, success=True, no skipping
```

### Frontend Requirements

#### Node Settings Panel
- Add "Error Handling" section/tab in node settings
- Dropdown for action: "Fail on Error" / "Output Error Data" / "Skip (Empty Output)"
- Checkbox for "Include input data in error output" (only visible when action is "Output Error Data")

#### Visual Indicators
- Nodes with custom error handling could show a small indicator icon
- During execution, nodes that output error data should show warning state (yellow) not error state (red)

### Usage Example

```
[Read CSV] → [Transform (on_error=output_error)] → [Filter] → [Write DB]
                                                       ↓
                                              Filters out _error=True rows
                                                       ↓
                                              [Write Error Log]
```

User creates a Filter node after Transform that routes error rows to a logging output.

---

## 2. Multiple Node Outputs

### Overview
Allow specific node types to produce multiple distinct output dataframes that route to different downstream nodes via separate output ports.

### Current State
- `NodeTemplate.output` is always 0 or 1
- Infrastructure exists: `OutputConnectionClass` supports "output-0" through "output-9"
- `NodeData` has `left_output` and `right_output` fields (unused)
- Only one result stored in `NodeResults.resulting_data`

### Proposed Behavior
Certain node types can define multiple outputs. Each output port can connect to different downstream nodes.

### Use Cases

| Node Type | Outputs | Description |
|-----------|---------|-------------|
| `split` | 2 | Routes rows based on condition: matches → output-0, non-matches → output-1 |
| `sample_split` | 2 | Random split for ML: train → output-0, test → output-1 |
| `validate` | 2 | Schema/data validation: valid → output-0, invalid → output-1 |
| `partition` | N | Route to N outputs based on column value |

### Data Models

```python
# Modify NodeTemplate in schemas.py
class NodeTemplate(BaseModel):
    name: str
    item: str
    input: int
    output: int  # Change interpretation: 0, 1, 2, 3... (not just 0/1)
    output_names: list[str] | None = None  # Optional friendly names
    multi: bool
    node_group: str
    can_be_start: bool
    transform_type: Literal["narrow", "wide", "other"]

# Example templates
split_template = NodeTemplate(
    name="Split",
    item="split",
    input=1,
    output=2,
    output_names=["Matches", "No Match"],
    node_group="transform",
    ...
)

# Modify NodeResults in models.py
class NodeResults:
    resulting_data: FlowDataEngine | None = None  # Primary output (output-0)
    additional_outputs: dict[str, FlowDataEngine] = Field(default_factory=dict)
    # Keys: "output-1", "output-2", etc.
    errors: str | None = None
    warnings: str | None = None
```

### Node Settings for Split

```python
# Add to input_schema.py
class NodeSplit(NodeSingleInput):
    """Settings for a node that splits data based on a condition."""

    filter_input: transform_schema.FilterInput  # Reuse existing filter logic
    # Rows matching condition → output-0
    # Rows not matching → output-1
```

### Implementation Changes

#### flow_node.py

```python
def get_resulting_data(self, output_port: str = "output-0") -> FlowDataEngine | None:
    """Get result data for a specific output port."""
    if output_port == "output-0":
        return self.results.resulting_data
    return self.results.additional_outputs.get(output_port)

def add_node_connection(self, from_node, insert_type: InputType, source_output: str = "output-0"):
    """Modified to track which output port the connection comes from."""
    # Store source_output in connection metadata
    ...
```

#### Node function return type
```python
# Multi-output nodes return a dict
def split_function(input_data: FlowDataEngine, settings: NodeSplit) -> dict[str, FlowDataEngine]:
    condition = build_filter_expression(settings.filter_input)
    matching = input_data.filter(condition)
    not_matching = input_data.filter(~condition)
    return {
        "output-0": matching,
        "output-1": not_matching
    }
```

#### flow_graph.py - execution
```python
# When collecting input data, specify which output port to pull from
for i, (input_node, source_port) in enumerate(self.get_inputs_with_ports()):
    input_data.append(input_node.get_resulting_data(output_port=source_port))
```

### Frontend Requirements

#### Node Visual
- Multi-output nodes show multiple output handles on right side
- Each handle labeled with output name or number
- Different colored handles for different outputs (optional)

#### Connection Creation
- When dragging from multi-output node, connection originates from specific handle
- Connection stores source output port in edge data

#### Node Settings
- For split: Show filter condition builder (reuse existing filter UI)
- For sample_split: Show split ratio (e.g., 80/20)
- For partition: Show column selector and value mappings

### Edge Schema Update

```python
# In schemas.py or wherever edges are defined
class NodeEdge(BaseModel):
    source_id: int
    target_id: int
    source_handle: OutputConnectionClass = "output-0"  # NEW
    target_handle: InputConnectionClass = "input-0"
```

---

## 3. Schema Enforcement

### Overview
Allow nodes to define an expected output schema. The system can then validate, transform, or warn when actual output doesn't match.

### Current State
- `MinimalFieldInfo` exists with `name` and `data_type` fields
- Used in input nodes to define expected fields
- `NodeSchemaInformation` tracks `predicted_schema` and `result_schema`
- No enforcement mechanism exists

### Proposed Behavior
Each node can optionally define an `output_schema` with enforcement rules.

### Enforcement Modes

| Mode | Behavior |
|------|----------|
| `strict` | Raise error if schema doesn't match exactly |
| `warn` | Log warning but continue with actual schema |
| `add_missing` | Add missing columns with null/default values |
| `drop_extra` | Remove columns not in defined schema |
| `conform` | Add missing + drop extra (most flexible) |

### Data Models

```python
# Add to input_schema.py

class SchemaEnforcement(BaseModel):
    """Optional schema enforcement for node output."""

    enabled: bool = False
    mode: Literal["strict", "warn", "add_missing", "drop_extra", "conform"] = "warn"
    fields: list[MinimalFieldInfo] = Field(default_factory=list)
    defaults: dict[str, Any] = Field(default_factory=dict)  # Column name → default value

    # Optional: type coercion
    coerce_types: bool = False  # Attempt to cast columns to specified types

# Add to NodeBase
class NodeBase(BaseModel):
    # ... existing fields ...
    output_schema: SchemaEnforcement | None = None
```

### MinimalFieldInfo Extension (Optional)

```python
class MinimalFieldInfo(BaseModel):
    """Represents the most basic information about a data field (column)."""

    name: str
    data_type: str = "String"
    nullable: bool = True  # NEW: Whether null values are allowed
    default: Any = None    # NEW: Default value for add_missing mode
```

Or keep `MinimalFieldInfo` as-is and use the `defaults` dict in `SchemaEnforcement`.

### Implementation

#### New utility function

```python
# Add to flowfile_core/flowfile/utils/ or flow_node.py

def enforce_schema(
    data: FlowDataEngine,
    enforcement: SchemaEnforcement,
    node_id: int  # For error messages
) -> FlowDataEngine:
    """Apply schema enforcement rules to output data."""

    if not enforcement.enabled:
        return data

    expected_cols = {f.name: f.data_type for f in enforcement.fields}
    actual_cols = set(data.columns)
    expected_col_names = set(expected_cols.keys())

    missing = expected_col_names - actual_cols
    extra = actual_cols - expected_col_names

    if enforcement.mode == "strict":
        if missing or extra:
            raise SchemaValidationError(
                f"Node {node_id}: Schema mismatch. "
                f"Missing: {missing or 'none'}. Extra: {extra or 'none'}."
            )

    elif enforcement.mode == "warn":
        if missing or extra:
            logger.warning(
                f"Node {node_id}: Schema mismatch. "
                f"Missing: {missing or 'none'}. Extra: {extra or 'none'}."
            )

    elif enforcement.mode in ("add_missing", "conform"):
        for col_name in missing:
            dtype = expected_cols[col_name]
            default = enforcement.defaults.get(col_name)
            data = data.with_column(col_name, pl.lit(default).cast(dtype))

    if enforcement.mode in ("drop_extra", "conform"):
        data = data.drop(list(extra))

    # Optional: reorder columns to match schema order
    if enforcement.mode in ("strict", "conform"):
        ordered_cols = [f.name for f in enforcement.fields if f.name in data.columns]
        # Add any remaining columns at the end (for non-strict modes)
        remaining = [c for c in data.columns if c not in ordered_cols]
        data = data.select(ordered_cols + remaining)

    # Optional: type coercion
    if enforcement.coerce_types:
        for field in enforcement.fields:
            if field.name in data.columns:
                data = data.with_column(
                    field.name,
                    pl.col(field.name).cast(map_dtype(field.data_type))
                )

    return data
```

#### Integration in flow_node.py

```python
def execute_node(self, ...):
    try:
        result = self._function(*input_data)

        # Apply schema enforcement if configured
        if hasattr(self.setting_input, 'output_schema') and self.setting_input.output_schema:
            result = enforce_schema(
                data=result,
                enforcement=self.setting_input.output_schema,
                node_id=self.node_id
            )

        self.results.resulting_data = result

    except SchemaValidationError as e:
        # Handle as regular error (respects on_error setting)
        ...
    except Exception as e:
        ...
```

### Frontend Requirements

#### Node Settings Panel
- Add "Output Schema" section/tab
- Toggle: "Enable Schema Enforcement"
- Dropdown: Enforcement mode
- Schema builder table:
  - Column name (text input)
  - Data type (dropdown: String, Int64, Float64, Boolean, Date, Datetime, etc.)
  - Default value (text input, optional)
  - Add/remove row buttons
- "Import from previous run" button - populates fields from last execution's actual schema
- "Import from upstream" button - populates from predicted schema of input node

#### Visual Indicators
- Nodes with schema enforcement show a small schema icon
- After execution, show green checkmark if schema matched, yellow warning if adjusted, red X if failed

### Type Mapping

```python
# Map MinimalFieldInfo.data_type to Polars types
DTYPE_MAP = {
    "String": pl.Utf8,
    "Int64": pl.Int64,
    "Int32": pl.Int32,
    "Float64": pl.Float64,
    "Float32": pl.Float32,
    "Boolean": pl.Boolean,
    "Date": pl.Date,
    "Datetime": pl.Datetime,
    "Time": pl.Time,
    "Duration": pl.Duration,
    "Binary": pl.Binary,
    "Null": pl.Null,
}
```

---

## Integration Between Features

### Error Handling + Schema Enforcement
When schema enforcement fails in `strict` mode:
- If `on_error.action == "fail"`: Raise `SchemaValidationError`
- If `on_error.action == "output_error"`: Output error dataframe with schema mismatch details
- If `on_error.action == "skip"`: Output empty dataframe

### Multiple Outputs + Schema Enforcement
- Each output port can have its own schema enforcement
- Schema defined per output in node settings

```python
class NodeSplit(NodeSingleInput):
    filter_input: transform_schema.FilterInput
    output_schemas: dict[str, SchemaEnforcement] = Field(default_factory=dict)
    # Keys: "output-0", "output-1"
```

### Multiple Outputs + Error Handling
- Error during multi-output node affects all outputs
- `on_error` applies to the node as a whole, not per-output

---

## Migration & Compatibility

### Backward Compatibility
- All new fields have defaults (`None` or sensible defaults)
- Existing flows continue to work without modification
- `on_error=None` means use current behavior (`fail`)
- `output_schema=None` means no enforcement

### Schema Migration
No database migration needed - Pydantic models handle missing fields with defaults.

### API Compatibility
- New optional fields in node settings API
- No breaking changes to existing endpoints

---

## Testing Requirements

### Error Handling Tests
1. Node with `on_error.action="fail"` raises exception on error
2. Node with `on_error.action="output_error"` produces error dataframe
3. Node with `on_error.action="skip"` produces empty dataframe
4. Error dataframe has correct schema (`_error`, `_error_message`, etc.)
5. `include_input_data=True` appends error columns to input
6. Downstream nodes receive error output and can filter/process it
7. Flow continues after error when `on_error` is configured

### Multiple Output Tests
1. Split node routes matching rows to output-0, non-matching to output-1
2. Connections from different output ports reach correct downstream nodes
3. Empty output on one port doesn't affect other port
4. Schema prediction works for each output port
5. Execution order handles multi-output nodes correctly

### Schema Enforcement Tests
1. `strict` mode raises error on mismatch
2. `warn` mode logs warning but continues
3. `add_missing` adds columns with defaults
4. `drop_extra` removes unlisted columns
5. `conform` does both add and drop
6. Type coercion works when enabled
7. Integration with `on_error` - schema errors respect error handling

---

## Implementation Order

### Phase 1: Schema Enforcement
- Add `SchemaEnforcement` and `OnErrorBehavior` models to `input_schema.py`
- Add fields to `NodeBase`
- Implement `enforce_schema()` utility
- Integrate into `execute_node()`
- Frontend: Schema builder UI

### Phase 2: Error Handling
- Implement error dataframe creation
- Modify `execute_node()` error handling
- Modify `run_graph()` skip logic
- Frontend: Error handling settings UI

### Phase 3: Multiple Outputs
- Extend `NodeTemplate.output` interpretation
- Add `additional_outputs` to `NodeResults`
- Modify connection handling for output ports
- Implement `split` node
- Frontend: Multi-output node visuals and connections

---

## Open Questions

1. **Error column prefix**: Use `_error` or `__error__` or configurable?
2. **Schema enforcement on error output**: Should error dataframes also be schema-enforced?
3. **Multi-output + error**: Should each output port have independent error handling?
4. **Default values**: Store in `MinimalFieldInfo` or separate `defaults` dict?
5. **Column ordering**: Should `conform` mode also reorder columns to match schema?
