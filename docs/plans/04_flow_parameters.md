# Feature 4: Flow Parameters

## Motivation

Flows today are static — every value (file paths, filter expressions, column names, thresholds) is hardcoded in node settings. This makes flows fragile and non-reusable:

- A daily pipeline that reads `sales_2024_01.csv` must be manually edited each month.
- A flow shared between teams cannot adapt to different connection strings or schema names.
- Scheduled flows cannot receive runtime inputs (e.g., date range, environment).

Flow parameters allow users to define named, typed inputs on a flow that are resolved at execution time. Parameters can be supplied via the UI, CLI arguments, API calls, or parent flows (when used as a sub-flow in Feature 8).

## Current State

- **No parameter support**: `FlowfileSettings` (`schemas.py:205`) contains execution configuration but no parameter definitions. `FlowfileData` has no parameter field.
- **Node settings are static**: Models like `NodeFilter.filter_expression` or `NodeRead` file paths are plain strings with no template/variable resolution.
- **CLI entry point** (`flowfile/__main__.py`): Runs flows but has no mechanism to pass runtime arguments.
- **Code generator** (`code_generator.py`): Generates hardcoded values — no variable injection.

## Proposed Design

### Parameter Model

**New Pydantic models** (`schemas.py` or new `parameters.py`):

```python
class FlowParameterType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    FILE_PATH = "file_path"
    SECRET = "secret"           # encrypted at rest, masked in UI

class FlowParameter(BaseModel):
    name: str                   # unique identifier, e.g. "input_date"
    display_name: str = ""      # human-readable label
    type: FlowParameterType = FlowParameterType.STRING
    default_value: Any | None = None
    required: bool = True
    description: str = ""
    validation: str | None = None  # optional regex or range expression
```

**`FlowfileSettings` extension**:

```python
class FlowfileSettings(BaseModel):
    # ... existing fields ...
    parameters: list[FlowParameter] = Field(default_factory=list)
```

**`FlowfileData` extension**:

```python
class FlowfileData(BaseModel):
    # ... existing fields ...
    # Parameter values are NOT stored in the flow file — they are runtime inputs.
    # The flow file only stores parameter definitions (in flowfile_settings.parameters).
```

### Parameter Resolution

**Syntax**: Parameters are referenced in node settings using `{{param_name}}` template syntax.

```yaml
nodes:
  - id: 1
    type: read
    setting_input:
      file_path: "/data/sales_{{input_date}}.csv"

  - id: 2
    type: filter
    setting_input:
      filter_expression: "col('amount') > {{min_amount}}"
```

**Resolution engine** (new module: `flowfile_core/flowfile_core/flowfile/parameters.py`):

```python
def resolve_parameters(setting_input: Any, parameter_values: dict[str, Any]) -> Any:
    """Deep-walk a node's setting_input, replacing {{param}} references with values."""
    # 1. Serialize setting_input to dict
    # 2. Recursively walk all string values
    # 3. Replace {{param_name}} with parameter_values[param_name]
    # 4. Cast to the parameter's declared type
    # 5. Deserialize back to the setting_input model
```

**Resolution timing**: Parameters are resolved **before** execution starts, during `FlowGraph` initialization. This means:
- Schema inference can use resolved values (e.g., to determine file schema).
- The execution plan sees concrete values, not templates.
- Parameter values are passed as a `dict[str, Any]` alongside the flow definition.

### Execution Integration

**`flow_graph.py`**:

```python
class FlowGraph:
    def __init__(self, flow_data: FlowfileData, parameter_values: dict[str, Any] | None = None):
        # Validate parameter_values against flow_data.flowfile_settings.parameters
        # Resolve all node setting_inputs with parameter values
        # Continue with normal graph construction
```

**CLI** (`flowfile/__main__.py`):

```bash
# Pass parameters via CLI
flowfile run my_flow.yaml --param input_date=2024-03-15 --param min_amount=100

# Pass parameters via JSON file
flowfile run my_flow.yaml --params-file params.json
```

**API** (new endpoint in `main.py`):

```
POST /flow/run
{
    "flow_id": 42,
    "parameter_values": {
        "input_date": "2024-03-15",
        "min_amount": 100
    }
}
```

### Frontend Changes

**Parameter definition UI** (flow settings panel):
- Table/list editor for defining parameters: name, type, default, required, description.
- Type-specific default value editors (date picker for `date`, file browser for `file_path`, etc.).

**Parameter prompt on execution**:
- When a flow has parameters, clicking "Run" opens a parameter form dialog.
- Fields are generated from parameter definitions with type-appropriate inputs.
- Default values are pre-filled.
- Required parameters must be filled before execution can start.
- `secret` type parameters show a password input and are not stored in run history.

**Template syntax highlighting**:
- In node setting text fields, `{{param_name}}` is highlighted distinctly.
- Autocomplete suggests available parameter names.

### Code Generation

The code generator should emit parameters as function arguments:

```python
# Generated code
def run_flow(input_date: str = "2024-01-01", min_amount: float = 0):
    df = pl.read_csv(f"/data/sales_{input_date}.csv")
    df = df.filter(pl.col("amount") > min_amount)
    ...
```

### Integration with Other Features

- **Feature 1 (Iterative Nodes)**: The iteration variable could be a parameter.
- **Feature 2 (Conditional Execution)**: Condition expressions can reference `{{param}}` values.
- **Feature 8 (Flow as Custom Node)**: When a flow is used as a sub-node, its parameters become the node's input configuration — the parent flow maps values to the child flow's parameters.

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/schemas/schemas.py` | Add `FlowParameter`, `FlowParameterType`; extend `FlowfileSettings` |
| `flowfile_core/flowfile_core/flowfile/parameters.py` | NEW: parameter resolution engine |
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | Accept and resolve parameters during initialization |
| `flowfile_core/flowfile_core/main.py` | Add parameter-aware run endpoint |
| `flowfile/flowfile/__main__.py` | Add `--param` / `--params-file` CLI arguments |
| `flowfile_core/flowfile_core/flowfile/code_generator/code_generator.py` | Emit parameters as function arguments |
| `flowfile_frontend/` | Parameter definition UI, execution prompt dialog, template highlighting |

## Open Questions

1. **Nested parameters**: Should parameters support expressions like `{{base_path}}/{{file_name}}`? Proposed: yes, simple string interpolation.
2. **Parameter validation**: Beyond regex, should we support enum constraints (dropdown of allowed values)?
3. **Environment variable fallback**: Should `{{param}}` fall back to `os.environ["param"]` if no value is provided? This would enable CI/CD integration without explicit parameter passing.
4. **Parameter history**: Should the UI remember recently used parameter values per flow?
5. **Secret handling**: How are `secret` type parameters passed to the worker? They should be encrypted in transit and never logged.
