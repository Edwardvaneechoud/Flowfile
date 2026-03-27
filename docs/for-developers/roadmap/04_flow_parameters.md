# Feature 4: Flow Parameters

## Motivation

Flows today are static — every value (file paths, filter expressions, column names, thresholds) is hardcoded in node settings. This makes flows fragile and non-reusable:

- A daily pipeline that reads `sales_2024_01.csv` must be manually edited each month.
- A flow shared between teams cannot adapt to different connection strings or schema names.
- Scheduled flows cannot receive runtime inputs (e.g., date range, environment).

Flow parameters allow users to define named inputs on a flow that are resolved at execution time via `${param_name}` syntax.

## Current State: In Progress (`feature/add-flow-parameters`)

This feature has substantial implementation on the `feature/add-flow-parameters` branch. Here is what exists:

### Backend — Implemented

**`FlowParameter` model** (`schemas.py`):

```python
class FlowParameter(BaseModel):
    """A single flow-level parameter that can be referenced via ${name} syntax."""
    name: str
    default_value: str = ""
    description: str = ""
```

Parameters are stored on `FlowGraphConfig`:

```python
class FlowGraphConfig(BaseModel):
    # ... existing fields ...
    parameters: list[FlowParameter] = Field(default_factory=list)
```

**Parameter resolver** (`parameter_resolver.py` — 177 lines, fully implemented):

- `resolve_parameters(text, params)` — replaces `${name}` patterns in strings.
- `apply_parameters_in_place(obj, params)` — recursively mutates Pydantic model string fields, returning restoration triples so originals can be restored after execution.
- `restore_parameters(restorations)` — restores original field values post-execution.
- `find_unresolved_in_model(obj)` — detects unresolved `${...}` references.
- Validates that all references resolve; raises `ValueError` with unresolved names if not.
- Uses regex pattern: `\$\{([a-zA-Z_][a-zA-Z0-9_]*)\}`

**`flow_graph.py` integration**:

- `_execute_single_node()` applies parameters in-place before execution, restores after.
- Saves and restores node `_hash` to prevent spurious cache invalidation from resolved values.
- On parameter change (via `flow_settings` setter), nodes with `${...}` references are reset.
- Each node gets a `_params_getter` callable that reads the latest parameters from the graph (lazy, not a copy).

**CLI support** (`flowfile/__main__.py`):

- Parameter passing via command-line arguments (implementation on branch).

### Frontend — Implemented

**`FlowParametersPanel.vue`** (223 lines):

- Panel with name / value / description columns per parameter.
- Add parameter button, delete button per row.
- Saves to backend via `FlowApi.updateFlowSettings()` on change.
- Empty state with explanation of `${param_name}` syntax.
- Integrated into the header layout.

### Tests — Implemented

- **`test_parameter_resolver.py`** (154 lines): Unit tests for the resolver — string substitution, nested Pydantic models, unresolved detection, edge cases.
- **`test_parameter_integration.py`** (609 lines): Integration tests for end-to-end parameter flow — graph execution with parameters, cache invalidation on parameter change, multiple nodes referencing same parameter, etc.

## What Remains

The core infrastructure is built. Remaining work to get this feature to production:

### 1. Type Support

Currently all parameters are strings (`default_value: str`). The original plan proposed typed parameters:

```python
class FlowParameterType(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    FILE_PATH = "file_path"
    SECRET = "secret"
```

**Decision needed**: Is string-only sufficient for v1? Many use cases (file paths, dates, thresholds) work as strings since they're substituted into string fields. Typed parameters would add validation and type-specific UI inputs but also complexity.

### 2. Parameter Prompt on Execution

When a flow has parameters, clicking "Run" should open a dialog where users fill in values before execution starts. Currently the panel lets you set default values, but there's no execution-time prompt for overriding them.

### 3. Code Generation

The code generator (`code_generator.py`) should emit parameters as function arguments:

```python
def run_flow(input_date: str = "2024-01-01", min_amount: str = "0"):
    df = pl.read_csv(f"/data/sales_{input_date}.csv")
    ...
```

### 4. Validation UX

- Highlight `${param_name}` references in node setting text fields.
- Show warnings when a parameter is referenced but not defined.
- Autocomplete parameter names in settings fields.

### 5. Integration with Feature 8 (Flow as Custom Node)

When a flow is used as a sub-node, its parameters become the node's configuration inputs. The parent flow maps values to the child flow's parameters.

## Key Files (Existing on Branch)

| File | Status | Description |
|------|--------|-------------|
| `flowfile_core/flowfile_core/schemas/schemas.py` | Modified | `FlowParameter` model, `parameters` on `FlowGraphConfig` |
| `flowfile_core/flowfile_core/flowfile/parameter_resolver.py` | New | Full resolver with in-place mutation and restoration |
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | Modified | Per-node parameter injection, reset on change |
| `flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py` | Modified | `_params_getter` callable |
| `flowfile/flowfile/__main__.py` | Modified | CLI parameter support |
| `flowfile_frontend/.../FlowParametersPanel/FlowParametersPanel.vue` | New | Parameter management UI |
| `flowfile_frontend/.../Header/HeaderButtons.vue` | Modified | Panel integration |
| `flowfile_core/tests/test_parameter_resolver.py` | New | Unit tests (154 lines) |
| `flowfile_core/tests/test_parameter_integration.py` | New | Integration tests (609 lines) |

## Open Questions

1. **String-only vs typed**: Is `default_value: str` sufficient, or should v1 include typed parameters?
3. **Secret parameters**: Should there be a `secret` type that uses the existing secrets system and masks values in the UI?
4. **Environment variable fallback**: Should `${param}` fall back to `os.environ["param"]` if no value is provided? Useful for CI/CD without explicit parameter passing.
5. **Parameter history**: Should the UI remember recently used parameter values per flow?
