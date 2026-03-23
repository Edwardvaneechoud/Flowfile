Context
Currently, Flowfile flows have no way to parameterize values like file paths, filter thresholds, SQL queries, or code snippets. Users must hardcode all values, making flows inflexible and not reusable. This change adds flow-level parameters that can be defined in the UI and referenced via ${param_name} syntax throughout node configurations. This is also the foundation for future "flow-in-flow" (sub-flow) support where parameters can be overridden at invocation time.
Design
Parameter Model
A FlowParameter is a simple name-value pair with an optional type and description:
```python
class FlowParameter(BaseModel):
    name: str                    # e.g. "input_dir", "threshold"
    default_value: str           # always stored as string
    description: str = ""        # optional documentation
```

Parameters live on the flow settings and are persisted in the flow file. At execution time, `${param_name}` references in node settings are resolved to their current values via simple string substitution.

### Substitution Strategy

Rather than modifying every node type individually, we implement a **single recursive substitution function** that walks a Pydantic model's serialized dict, replaces `${...}` patterns in all string values, and reconstructs the model. This is applied **at execution time** in `run_graph()` — saved flow files retain the `${...}` references.

---

## Implementation Steps

### Step 1: Backend — Add `FlowParameter` model and update schemas ✅ DONE

**Files:**
- `flowfile_core/flowfile_core/schemas/schemas.py`

Changes made:
1. Added `FlowParameter` Pydantic model (name, default_value, description)
2. Added `parameters: list[FlowParameter] = []` to `FlowGraphConfig`
3. Added `parameters: list[FlowParameter] = []` to `FlowfileSettings`

### Step 2: Backend — Parameter substitution engine ✅ DONE

**File:** `flowfile_core/flowfile_core/flowfile/parameter_resolver.py` (new file)

Created:
1. `resolve_parameters(text, params)` — replaces `${name}` patterns
2. `resolve_node_settings(setting_input, params)` — deep-clones a Pydantic model, resolves all string values, reconstructs model
3. `_find_unresolved(obj)` — detects remaining unresolved refs and raises `ValueError`

### Step 3: Backend — Integrate substitution into `FlowGraph.run_graph()` ✅ DONE

**File:** `flowfile_core/flowfile_core/flowfile/flow_graph.py`

Changes made:
1. Import `resolve_node_settings` from `parameter_resolver`
2. In `run_graph()`: build `params` dict once from `self.flow_settings.parameters`
3. `_execute_single_node()` now accepts `params: dict[str, str] | None`
4. Before `execute_node()`, temporarily swap `node.setting_input` with resolved copy
5. `finally` block restores original `setting_input`
6. `get_flowfile_data()` now passes `parameters=self.flow_settings.parameters` to `FlowfileSettings`

### Step 4: Backend — API endpoints for parameter CRUD ✅ DONE (automatic)

No new endpoints needed — existing `GET /flow_settings` and `POST /flow_settings` automatically include `parameters` via the updated `FlowSettings` model.

### Step 5: Backend — Persist parameters in flow files ✅ DONE

**File:** `flowfile_core/flowfile_core/flowfile/manage/io_flowfile.py`

Updated `_flowfile_data_to_flow_information()` to pass `parameters=flowfile_data.flowfile_settings.parameters` when constructing `FlowSettings`. Old flow files without `parameters` default to `[]`.

### Step 6: Backend — CLI parameter overrides ✅ DONE

**File:** `flowfile/flowfile/__main__.py`

Added `--param KEY=VALUE` argument:
- Parsed by `argparse` with `action="append"`
- `run_flow()` now accepts `param_overrides: list[str] | None`
- Overrides are applied before `run_graph()` by updating matching `FlowParameter` objects (or adding new ones)

Usage: `flowfile run flow my_flow.yaml --param input_dir=/data/prod --param threshold=100`

### Step 7: Frontend — Update TypeScript types ✅ DONE

**File:** `flowfile_frontend/src/renderer/app/types/flow.types.ts`

Added:
- `FlowParameter` interface (name, default_value, description)
- `parameters?: FlowParameter[]` to `FlowSettings`

### Step 8: Frontend — Parameters management UI in flow settings dialog ✅ DONE

**File:** `flowfile_frontend/src/renderer/app/components/layout/Header/HeaderButtons.vue`

Changes:
- Settings dialog width widened to 40% to fit parameter rows
- Added "Parameters" section with table of param rows (name / default value / description / delete button)
- "Add Parameter" button
- `addParameter()` and `removeParameter()` functions call `pushFlowSettings()` on change
- `loadFlowSettings()` initializes `parameters` to `[]` if null (backwards compat)
- CSS for `.param-row`, `.param-name-input`, `.param-value-input`, `.param-desc-input`, `.param-empty`

### Step 9: Frontend — Dedicated Parameters Panel ✅ DONE

A live, draggable panel in the canvas for quick parameter management (in addition to the Settings modal).

**Files changed:**
- `flowfile_frontend/src/renderer/app/stores/editor-store.ts` — added `showParametersPanel` state + `toggleParametersPanel()` / `setParametersPanelVisibility()` actions
- `flowfile_frontend/src/renderer/app/components/layout/FlowParametersPanel/FlowParametersPanel.vue` — **new component**: loads `FlowSettings`, displays parameters in a table (name / value / description / delete), add/remove/edit all auto-save via `updateFlowSettings`
- `flowfile_frontend/src/renderer/app/views/DesignerView/Canvas.vue` — added `<draggable-item id="flowParameters">` panel (520px wide, right group, minimizable)
- `flowfile_frontend/src/renderer/app/components/layout/Header/HeaderButtons.vue` — added "Parameters" toggle button (with `active` class when panel is open, uses `tune` icon)

### Step 10: Frontend — Parameter hint in text inputs (optional enhancement)

**Status:** Deferred — can be done as a follow-up.

---

## Tests Written ✅

**File:** `flowfile_core/tests/test_parameter_resolver.py`

13 unit tests covering:
- Simple substitution
- Multiple params in one string
- Unresolved references left as-is
- Empty params dict passthrough
- Returns new model instance (original unchanged)
- Nested Pydantic model resolution
- Raises `ValueError` on unresolved params
- Non-Pydantic objects returned unchanged

All 13 tests pass.

---

## Backwards Compatibility

- Old flow files without `parameters` field load fine — defaults to `[]`
- No parameter → no substitution → behavior identical to before
- Empty `params` dict → `resolve_node_settings` returns original object unchanged

---

## Files Changed

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/schemas/schemas.py` | Added `FlowParameter`, `parameters` to `FlowGraphConfig` and `FlowfileSettings` |
| `flowfile_core/flowfile_core/flowfile/parameter_resolver.py` | **New file** — substitution engine |
| `flowfile_core/flowfile_core/flowfile/flow_graph.py` | Integrated parameter resolution in `run_graph()` / `_execute_single_node()`, updated `get_flowfile_data()` |
| `flowfile_core/flowfile_core/flowfile/manage/io_flowfile.py` | Pass `parameters` when loading flow from file |
| `flowfile/flowfile/__main__.py` | Added `--param` CLI support |
| `flowfile_frontend/src/renderer/app/types/flow.types.ts` | Added `FlowParameter`, updated `FlowSettings` |
| `flowfile_frontend/src/renderer/app/components/layout/Header/HeaderButtons.vue` | Added parameters UI section |
| `flowfile_core/tests/test_parameter_resolver.py` | **New file** — 13 unit tests |
