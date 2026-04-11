# Flow Templates — Improvement Plan

This document outlines concrete improvements for the flow templates feature across architecture, backend, frontend, dark mode, and testing.

---

## 1. Architecture Improvements

### 1.1 Cache invalidation strategy

Currently, downloaded CSVs and YAMLs are cached forever in `~/.flowfile/template_data/`. If templates are updated on GitHub, users never get the new versions.

**Fix:** Write a small version manifest (`templates_version.json`) alongside cached files. On `ensure_available`, compare the local version against a remote version file. Re-download only when the version bumps.

```
~/.flowfile/template_data/
├── templates_version.json   # {"version": "0.8.2", "fetched_at": "..."}
├── flows/
│   └── *.yaml
└── *.csv
```

### 1.2 Temp YAML file cleanup after template instantiation

`create_from_template` writes a YAML to `flows_dir` and never removes it. Repeated instantiations accumulate orphan files, and concurrent calls for the same template can collide on the filename.

**Fix:** Use a unique filename (e.g. include a UUID or timestamp) and delete it after `import_flow` returns, or write to a `tempfile.NamedTemporaryFile` that is cleaned up automatically.

```python
import tempfile

with tempfile.NamedTemporaryFile(
    dir=flows_dir, suffix=".yaml", delete=True, mode="w", encoding="utf-8"
) as f:
    yaml.dump(flowfile_data.model_dump(), f, default_flow_style=False, allow_unicode=True)
    f.flush()
    flow_id = flow_file_handler.import_flow(Path(f.name), user_id=user_id)
```

### 1.3 Move `template_id` into the URL path

The `create_from_template` endpoint currently takes `template_id` as a query parameter on a POST with an empty body. This is non-standard REST.

**Fix:** Change the route to `POST /templates/{template_id}/create` and update the frontend API client accordingly.

```python
# routes.py
@router.post("/templates/{template_id}/create", tags=["templates"])
def create_from_template(template_id: str, current_user=Depends(get_current_active_user)) -> int:
    ...
```

```typescript
// templates.api.ts
static async createFromTemplate(templateId: string): Promise<number> {
    const response = await axios.post(`/templates/${templateId}/create`);
    return response.data;
}
```

---

## 2. Backend Improvements

### 2.1 Guard against missing YAML fields

`_load_template_yaml` calls `.pop()` on `_template_meta` and `_required_csv_files` without checking they exist. A malformed YAML causes an unhelpful `KeyError`.

**Fix:** Add explicit checks before popping.

```python
def _load_template_yaml(yaml_path: Path) -> tuple[FlowTemplateMeta, list[str], dict]:
    with open(yaml_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if "_template_meta" not in data:
        raise ValueError(f"Missing '_template_meta' in {yaml_path.name}")
    if "_required_csv_files" not in data:
        raise ValueError(f"Missing '_required_csv_files' in {yaml_path.name}")

    meta = FlowTemplateMeta.model_validate(data.pop("_template_meta"))
    required_files = data.pop("_required_csv_files")

    if not isinstance(required_files, list):
        raise TypeError(f"'_required_csv_files' must be a list in {yaml_path.name}")

    return meta, required_files, data
```

### 2.2 Add download timeout

`urllib.request.urlretrieve` has no timeout. A slow or unreachable GitHub will block the API thread indefinitely.

**Fix:** Replace `urlretrieve` with `urlopen` + explicit timeout.

```python
def _download_file(url: str, local_path: Path, timeout: int = 30) -> None:
    logger.info("Downloading template file: %s -> %s", url, local_path)
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
            local_path.write_bytes(resp.read())
    except Exception as e:
        # Clean up partial downloads
        local_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Failed to download '{local_path.name}' from {url}. "
            f"Please check your internet connection. Error: {e}"
        ) from e
```

### 2.3 Use `Literal` type for `category` on the Pydantic model

The backend accepts any string for `category`, while the frontend enforces `"Beginner" | "Intermediate" | "Advanced"`. Validation should happen server-side too.

**Fix:**

```python
from typing import Literal

class FlowTemplateMeta(BaseModel):
    template_id: str
    name: str
    description: str
    category: Literal["Beginner", "Intermediate", "Advanced"]
    tags: list[str]
    node_count: int
    icon: str
```

### 2.4 Reduce repeated `ensure_available` work

`ensure_available` checks file existence for all 8 YAMLs on every page visit. After the first successful call in a process, it does no useful work.

**Fix:** Add a module-level flag in the registry so subsequent calls are a no-op.

```python
class _TemplateRegistry:
    def __init__(self):
        self._loaded = False
        self._yamls_ensured = False
        ...

    def mark_yamls_ensured(self):
        self._yamls_ensured = True

    @property
    def yamls_ensured(self) -> bool:
        return self._yamls_ensured
```

---

## 3. Frontend Improvements

### 3.1 Remove dead code in API client

In `templates.api.ts`, the status check is unreachable — axios throws on non-2xx by default.

**Fix:**

```typescript
static async createFromTemplate(templateId: string): Promise<number> {
    const response = await axios.post(
        `/templates/${templateId}/create`,
        {},
        { headers: { accept: "application/json" } },
    );
    return response.data;
}
```

### 3.2 Prevent duplicate click on TemplateCard

Both the card wrapper (`@click`) and the button inside emit `use-template`. A button click bubbles up and fires the handler twice.

**Fix:** Either stop propagation on the button or remove the card-level click handler. Removing the card click is simpler and keeps the clickable area explicit.

```html
<!-- Option A: remove card click, keep only button -->
<div class="template-card">
  ...
  <el-button @click="$emit('use-template', template.template_id)" ...>
    Use Template
  </el-button>
</div>

<!-- Option B: keep card click, stop propagation on button -->
<div class="template-card" @click="$emit('use-template', template.template_id)">
  ...
  <el-button @click.stop ...>
    Use Template
  </el-button>
</div>
```

### 3.3 Distinguish empty states

"No templates available" is shown whether templates failed to load, none exist, or the filter has no matches.

**Fix:** Track error state separately and show different messages.

```typescript
const loadError = ref(false);

// In loadTemplates catch block:
loadError.value = true;
```

```html
<div v-if="isLoading" class="templates-view__loading">
  <p>Loading templates...</p>
</div>
<div v-else-if="loadError" class="templates-view__empty">
  <p>Could not load templates. Please check your connection and try again.</p>
  <el-button @click="loadTemplates">Retry</el-button>
</div>
<div v-else-if="filteredTemplates.length === 0 && selectedCategory !== 'All'" class="templates-view__empty">
  <p>No {{ selectedCategory }} templates found.</p>
</div>
<div v-else-if="filteredTemplates.length === 0" class="templates-view__empty">
  <p>No templates available.</p>
</div>
```

### 3.4 Cache `ensureAvailable` in the frontend session

Avoid calling the backend endpoint on every page visit after the first success.

```typescript
let templatesEnsured = false;

static async ensureAvailable(): Promise<void> {
    if (templatesEnsured) return;
    await axios.get("/templates/ensure_available/");
    templatesEnsured = true;
}
```

---

## 4. Dark Mode Compatibility

### Problem

`TemplateCard.vue` uses hardcoded hex colors for category badges. These are invisible or low-contrast in dark mode:

```css
/* Light green background on dark canvas = invisible */
.template-card__badge--beginner { background: #e8f5e9; color: #2e7d32; }
```

### Fix

Use the existing semantic CSS variables from `_variables.css` and `_status-badges.css`. Map difficulty levels to status colors (beginner=success, intermediate=warning, advanced=danger) and use semi-transparent backgrounds for dark mode.

```css
.template-card__badge--beginner {
  background: var(--color-success-light);
  color: var(--color-success);
}

.template-card__badge--intermediate {
  background: var(--color-warning-light);
  color: var(--color-warning);
}

.template-card__badge--advanced {
  background: var(--color-danger-light);
  color: var(--color-danger);
}

/* Dark mode: use transparent backgrounds so they work on dark surfaces */
[data-theme="dark"] .template-card__badge--beginner {
  background: rgba(16, 185, 129, 0.2);
  color: var(--color-success);
}

[data-theme="dark"] .template-card__badge--intermediate {
  background: rgba(245, 158, 11, 0.2);
  color: var(--color-warning);
}

[data-theme="dark"] .template-card__badge--advanced {
  background: rgba(239, 68, 68, 0.2);
  color: var(--color-danger);
}
```

This matches the pattern used by `KernelStatusBadge.vue` and the global `_status-badges.css` component and respects the `[data-theme="dark"]` selector used throughout the codebase.

---

## 5. Template Flow Tests

### Goal

Every YAML flow in `data/templates/flows/` should be loadable and executable. This catches breakage early when node schemas, serialization formats, or template data change.

### 5.1 Test location

```
flowfile_core/tests/templates/
├── __init__.py
├── conftest.py
└── test_template_flows.py
```

### 5.2 Shared fixtures (`conftest.py`)

```python
import pytest
from pathlib import Path

TEMPLATE_FLOWS_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "templates" / "flows"
TEMPLATE_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "templates"

def get_template_yaml_files() -> list[Path]:
    """Collect all template YAML files for parametrization."""
    return sorted(TEMPLATE_FLOWS_DIR.glob("*.yaml"))

@pytest.fixture(params=get_template_yaml_files(), ids=lambda p: p.stem)
def template_yaml_path(request) -> Path:
    """Parametrized fixture that yields each template YAML path."""
    return request.param

@pytest.fixture
def template_data_dir() -> Path:
    return TEMPLATE_DATA_DIR
```

### 5.3 Test cases (`test_template_flows.py`)

```python
"""Tests that verify all template flows in data/templates/flows/ load and run correctly."""

import yaml
import pytest
from pathlib import Path

from flowfile_core.templates.models import FlowTemplateMeta
from flowfile_core.templates.template_definitions import (
    _load_template_yaml,
    _replace_data_dir_placeholder,
    TEMPLATE_PATH_PLACEHOLDER,
)
from flowfile_core.schemas.schemas import FlowfileData
from flowfile_core.flowfile.manage.io_flowfile import open_flow


class TestTemplateYamlValidity:
    """Verify that every template YAML is structurally valid."""

    def test_yaml_parses(self, template_yaml_path: Path):
        """Template YAML can be parsed without errors."""
        with open(template_yaml_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict)

    def test_has_required_meta_fields(self, template_yaml_path: Path):
        """Template contains _template_meta and _required_csv_files."""
        with open(template_yaml_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        assert "_template_meta" in data, f"Missing _template_meta in {template_yaml_path.name}"
        assert "_required_csv_files" in data, f"Missing _required_csv_files in {template_yaml_path.name}"

    def test_meta_validates(self, template_yaml_path: Path):
        """_template_meta validates against FlowTemplateMeta model."""
        meta, required_files, flow_dict = _load_template_yaml(template_yaml_path)
        assert isinstance(meta, FlowTemplateMeta)
        assert meta.template_id
        assert meta.name
        assert meta.category in ("Beginner", "Intermediate", "Advanced")
        assert meta.node_count > 0

    def test_required_csv_files_exist(self, template_yaml_path: Path, template_data_dir: Path):
        """All CSV files listed in _required_csv_files exist in data/templates/."""
        _, required_files, _ = _load_template_yaml(template_yaml_path)
        for csv_file in required_files:
            csv_path = template_data_dir / csv_file
            assert csv_path.exists(), f"Required CSV '{csv_file}' not found at {csv_path}"

    def test_node_count_matches_meta(self, template_yaml_path: Path):
        """node_count in metadata matches actual number of nodes."""
        meta, _, flow_dict = _load_template_yaml(template_yaml_path)
        actual_nodes = len(flow_dict.get("nodes", []))
        assert actual_nodes == meta.node_count, (
            f"{template_yaml_path.name}: meta says {meta.node_count} nodes "
            f"but YAML contains {actual_nodes}"
        )

    def test_flowfile_data_validates(self, template_yaml_path: Path, template_data_dir: Path):
        """Flow dict with resolved paths validates against FlowfileData schema."""
        _, _, flow_dict = _load_template_yaml(template_yaml_path)
        resolved = _replace_data_dir_placeholder(flow_dict, template_data_dir)
        flowfile_data = FlowfileData.model_validate(resolved)
        assert flowfile_data.flowfile_name
        assert len(flowfile_data.nodes) > 0


class TestTemplateFlowExecution:
    """Verify that every template flow can be loaded and executed end-to-end."""

    def test_open_flow_from_template(self, template_yaml_path: Path, template_data_dir: Path, tmp_path: Path):
        """Template can be opened as a FlowGraph via the standard import path."""
        _, _, flow_dict = _load_template_yaml(template_yaml_path)
        resolved = _replace_data_dir_placeholder(flow_dict, template_data_dir)
        flowfile_data = FlowfileData.model_validate(resolved)

        # Write resolved flow to a temp YAML and open it
        temp_yaml = tmp_path / f"{template_yaml_path.stem}.yaml"
        with open(temp_yaml, "w", encoding="utf-8") as f:
            yaml.dump(flowfile_data.model_dump(), f, default_flow_style=False, allow_unicode=True)

        flow = open_flow(temp_yaml)
        assert flow is not None
        assert len(flow.nodes) > 0

    def test_run_template_flow(self, template_yaml_path: Path, template_data_dir: Path, tmp_path: Path):
        """Template flow runs successfully with sample data."""
        _, _, flow_dict = _load_template_yaml(template_yaml_path)
        resolved = _replace_data_dir_placeholder(flow_dict, template_data_dir)
        flowfile_data = FlowfileData.model_validate(resolved)

        temp_yaml = tmp_path / f"{template_yaml_path.stem}.yaml"
        with open(temp_yaml, "w", encoding="utf-8") as f:
            yaml.dump(flowfile_data.model_dump(), f, default_flow_style=False, allow_unicode=True)

        flow = open_flow(temp_yaml)
        flow.execution_location = "local"

        # Remove explore_data nodes — they require a worker service
        explore_data_nodes = [n.node_id for n in flow.nodes if n.node_type == "explore_data"]
        for node_id in explore_data_nodes:
            flow.delete_node(node_id)

        result = flow.run_graph()

        assert result is not None, f"run_graph() returned None for {template_yaml_path.name}"
        assert result.success, (
            f"Flow {template_yaml_path.name} failed: "
            + "; ".join(
                f"node {s.node_id} ({s.node_name}): {s.error}"
                for s in result.node_step_result
                if not s.success and s.error
            )
        )
        assert result.nodes_completed > 0
```

### 5.4 Running the tests

```bash
# Run only template tests
poetry run pytest flowfile_core/tests/templates/ -v

# Run as part of the full core test suite
poetry run pytest flowfile_core/tests/ -v
```

### 5.5 CI integration

Add a step to the existing `test.yaml` workflow so template tests run on every PR that touches `data/templates/` or `flowfile_core/flowfile_core/templates/`:

```yaml
- name: Run template flow tests
  run: poetry run pytest flowfile_core/tests/templates/ -v
```

---

## Summary

| Area | Items | Priority |
|------|-------|----------|
| **Architecture** | Cache invalidation, temp file cleanup, REST path convention | High |
| **Backend** | YAML field guards, download timeout, `Literal` category, ensure flag | High |
| **Frontend** | Dead code removal, double-click fix, error/empty states, session cache | Medium |
| **Dark mode** | Badge colors via CSS variables + `[data-theme="dark"]` overrides | Medium |
| **Testing** | Parametrized YAML validation + flow execution tests in CI | High |
