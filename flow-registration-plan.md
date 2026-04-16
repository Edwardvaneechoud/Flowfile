
# Flow Registration & Save Improvements — Detailed Plan

## Context

The current flow save/registration system has several UX friction points:

1. **Save always opens file browser** — even when re-saving to the same path, forcing users to navigate the dialog every time. Users expect Ctrl+S / clicking Save to silently overwrite the current file.
2. **Quick Create stores flows in a temp directory** (`~/.flowfile/temp/flows/`) that gets auto-cleaned after 24h, with no catalog visibility. Users lose quick-created flows.
3. **Close tab always asks "Save changes?"** — even when the flow hasn't changed at all, because there's zero dirty-state tracking.
4. **Catalog registration is invisible** — flows auto-register to `General > default` namespace without user awareness or control over namespace placement.
5. **Save doesn't reliably overwrite** — the file browser may send slightly different paths (relative vs absolute, trailing slashes), causing the backend to interpret it as "Save As" and create a duplicate flow identity.

### Intended Outcome
- Clicking "Save" on an already-saved flow silently overwrites the file (no dialog)
- A "Save As" option exists for saving to a new location
- The save dialog has a catalog sidebar so users can choose where to register in the catalog
- Quick Create immediately persists to `~/.flowfile/flows/unnamed_flows/` and registers in a dedicated "Unnamed Flows" catalog namespace
- The close-tab dialog only appears when the flow actually has unsaved changes

---

## 1. Silent Save (Ctrl+S Behavior)

### Problem
`openSaveModal()` in `HeaderButtons.vue:433` always fetches settings and opens the file browser modal, regardless of whether the flow already has a saved path.

### Current Flow
```
User clicks Save → openSaveModal() → fetches FlowSettings → opens modalVisibleForSave
→ file browser appears → user picks path → saveFlowAction() → GET /save_flow
```

### New Flow
```
User clicks Save → fetchFlowSettings →
  IF path exists AND not a temp path:
    → call saveFlowSilent(flowId) directly → show success toast
  ELSE (no path or temp path):
    → open SaveDialog (with catalog sidebar, see section 2)
```

### Backend Changes

**File: `flowfile_core/flowfile_core/routes/routes.py` (line ~1084)**

The `save_flow` endpoint currently accepts `flow_path: str = None`. When `flow_path` is `None`, it passes `None` to `flow.save_flow()` which would fail. Fix this and accept an optional `namespace_id`:

```python
@router.get("/save_flow", tags=["editor"])
def save_flow(
    flow_id: int,
    flow_path: str = None,
    namespace_id: int = None,
    current_user=Depends(get_current_active_user),
):
    if flow_path is not None:
        flow_path = validate_path_under_cwd(flow_path)
    flow = flow_file_handler.get_flow(flow_id)
    current_path = flow.flow_settings.path or flow.flow_settings.save_location

    # If no explicit path provided, use the current path (silent save)
    if flow_path is None:
        flow_path = current_path
    if not flow_path:
        raise HTTPException(422, "No save path specified and flow has no existing path")

    # Normalize for robust comparison (handles ., .., trailing slash, symlinks)
    is_new_path = (
        current_path is None
        or str(Path(flow_path).resolve()) != str(Path(current_path).resolve())
    )

    if is_new_path:
        user_id = current_user.id if current_user else None
        return flow_file_handler.save_as_flow(
            flow_id=flow_id,
            new_path=flow_path,
            user_id=user_id,
            on_catalog_register=lambda fp, n, uid: register_flow_in_namespace(fp, n, uid, namespace_id),
            on_resolve_registration=resolve_source_registration_id,
        )

    resolve_source_registration_id(flow)
    flow.save_flow(flow_path=flow_path)
    # Mark-as-saved is already done inside FlowGraph.save_flow (see section 4)

    # If user explicitly selected a namespace for an existing flow, update its registration
    if namespace_id is not None:
        user_id = current_user.id if current_user else None
        register_flow_in_namespace(flow_path, flow.flow_settings.name, user_id, namespace_id)

    return flow_id
```

Also add the import at the top:
```python
from flowfile_core.flowfile.catalog_helpers import (
    auto_register_flow,
    register_flow_in_namespace,   # NEW
    resolve_source_registration_id,
)
```

### Frontend Changes

**File: `flowfile_frontend/src/renderer/app/components/layout/Header/utils.ts`** — already updated ✓

Contains:
- `isTemporaryPath(path)` helper
- `saveFlow(flowId, flowPath, namespaceId?)` with optional namespace
- `saveFlowSilent(flowId)` — GET `/save_flow` without `flow_path`

**File: `flowfile_frontend/src/renderer/app/components/layout/Header/HeaderButtons.vue`**

Rework `openSaveModal()` (~line 433) to prefer silent save:
```typescript
import { saveFlow, saveFlowSilent, isTemporaryPath } from "./utils";

const openSaveModal = async () => {
  const settings = await getFlowSettings(nodeStore.flow_id);
  if (!settings) return;

  // If the flow already has a real (non-temp) path, save silently.
  if (settings.path && !isTemporaryPath(settings.path)) {
    try {
      await saveFlowSilent(nodeStore.flow_id);
      ElMessage.success("Flow saved");
      // Advance tutorial if on save-flow step
      if (tutorialStore.isActive && tutorialStore.currentStep?.id === "save-flow") {
        setTimeout(() => tutorialStore.nextStep(), 300);
      }
    } catch (error: any) {
      ElMessage.error({ message: error?.message || "Failed to save flow", duration: 5000 });
    }
    return;
  }

  // Otherwise, open the Save dialog (File System / Catalog picker).
  savePath.value = settings.path;
  modalVisibleForSave.value = true;
  await fileBrowserRef.value?.handleInitialFileSelection();
};
```

Add a dedicated "Save As" button in the template (next to the Save button):
```html
<button class="action-btn" data-tutorial="save-as-btn" @click="openSaveAsModal">
  <span class="material-icons btn-icon">save_as</span>
  <span class="btn-text">Save As</span>
</button>
```

With its handler:
```typescript
const openSaveAsModal = async () => {
  const settings = await getFlowSettings(nodeStore.flow_id);
  if (!settings) return;
  savePath.value = settings.path;
  modalVisibleForSave.value = true;
  await fileBrowserRef.value?.handleInitialFileSelection();
};
```

> Note: The existing `saveFlowAction()` already handles the flow-ID switch when `save_as_flow` returns a new ID, so Save As continues to work correctly.

---

## 2. Enhanced Save Dialog with Catalog Sidebar

### Problem
No UI affordance for catalog placement; registration is invisible.

### Design
```
┌──────────────────────────────────────────────────┐
│ Save Flow                                    [X] │
├─────────────┬────────────────────────────────────┤
│ [📁 File]   │  File Browser                      │
│ [📋 Catalog]│  ┌──────────────────────────────┐  │
│             │  │ path / navigation / files    │  │
│             │  └──────────────────────────────┘  │
│             │  ☑ Also register in catalog         │
│             │  Namespace: [tree picker]           │
└─────────────┴────────────────────────────────────┘

When "Catalog" tab is selected:
┌──────────────────────────────────────────────────┐
│ [📁 File]   │  Flow name: [_______________]       │
│ [📋 Catalog]│  Namespace: [tree picker]           │
│             │  Path preview: ~/.flowfile/flows/…  │
│             │                    [Save to Catalog]│
└─────────────┴────────────────────────────────────┘
```

### Frontend Changes

**File: `flowfile_frontend/src/renderer/app/features/designer/components/SaveDialog.vue`** — already updated ✓

Contains:
- `saveMode: "file" | "catalog"` sidebar toggle
- File System panel with `<CatalogNamespacePicker>` + "Also register in catalog" checkbox
- Catalog panel with flow name input, namespace picker, path preview, "Save to Catalog" button
- `handleSaveFlow()` passes `namespaceId` to `saveFlow()`
- `handleCatalogSave()` builds path from `flowsDirectory + catalogFlowName` and registers with namespace

**New file: `flowfile_frontend/src/renderer/app/features/designer/components/CatalogNamespacePicker.vue`** — already exists ✓

Tree-style namespace selector emitting `v-model` (namespace_id). Default pre-selection: General > default.

**File: `flowfile_frontend/src/renderer/app/features/designer/components/NamespaceTreeItem.vue`** — already exists ✓

Recursive tree item used by `CatalogNamespacePicker`.

**File: `flowfile_frontend/src/renderer/app/api/catalog.api.ts`**

Ensure the namespace fetch method exists (used by the picker). If not already present, add:
```typescript
static async getNamespaceTree(): Promise<CatalogNamespace[]> {
  const response = await axios.get("/catalog/namespaces");
  return response.data;
}
```

### Backend Changes

**File: `flowfile_core/flowfile_core/flowfile/catalog_helpers.py`** — already updated ✓

Contains `register_flow_in_namespace(flow_path, name, user_id, namespace_id)` that:
- Falls back to `auto_register_flow` if `namespace_id is None`
- Updates existing registration's namespace if the flow_path is already registered
- Creates a new registration otherwise
- Uses `service.update_flow(registration_id=, requesting_user_id=, namespace_id=)`

**File: `flowfile_core/flowfile_core/routes/routes.py`**

- Add `namespace_id: int = None` to `save_flow` signature (see section 1 code above)
- Replace `auto_register_flow` callback with `register_flow_in_namespace(..., namespace_id)` in the save-as branch

---

## 3. Quick Create → Catalog with "Unnamed Flows" Namespace

### Problem
Quick-created flows are stored at `~/.flowfile/temp/flows/YYYYMMDD_H_M_S_flow.yaml` — a temp directory that gets cleaned up. They auto-register in `General > default` which clutters the main namespace.

### Solution
- Store quick-created flows at `~/.flowfile/flows/unnamed_flows/` (persistent)
- Register them under a dedicated `General > Unnamed Flows` catalog namespace
- Create this namespace on app startup if it doesn't exist

### Backend Changes

**File: `shared/storage_config.py`**

Add property (near `flows_directory` at ~line 86):
```python
@property
def unnamed_flows_directory(self) -> Path:
    """Directory for quick-created (unnamed) flows — persistent, user-accessible."""
    return self.flows_directory / "unnamed_flows"
```

Add it to the list of directories ensured by `_ensure_directories()` (~line 217):
```python
self.unnamed_flows_directory,
```

**File: `flowfile_core/flowfile_core/flowfile/handler.py`** — already updated ✓

`get_flow_save_location()` now returns `storage.unnamed_flows_directory / flow_name` instead of the temp directory.

**File: `flowfile_core/flowfile_core/catalog/service.py`**

Add method (near `auto_register_flow` at line ~843):
```python
def ensure_unnamed_flows_namespace(self) -> CatalogNamespace | None:
    """Ensure 'General > Unnamed Flows' namespace exists, creating it if missing."""
    general = self.repo.get_namespace_by_name("General", parent_id=None)
    if general is None:
        logger.info("Cannot create 'Unnamed Flows' namespace: 'General' not found")
        return None
    existing = self.repo.get_namespace_by_name("Unnamed Flows", parent_id=general.id)
    if existing:
        return existing
    ns = CatalogNamespace(name="Unnamed Flows", parent_id=general.id)
    return self.repo.create_namespace(ns)
```

Update `auto_register_flow()` to route unnamed flows to the correct namespace:
```python
def auto_register_flow(self, flow_path: str, name: str, user_id: int) -> FlowRegistration | None:
    general = self.repo.get_namespace_by_name("General", parent_id=None)
    if general is None:
        logger.info("Auto-registration skipped: 'General' namespace not found")
        return None

    # Route unnamed flows to their dedicated namespace
    is_unnamed = "/unnamed_flows/" in flow_path or "\\unnamed_flows\\" in flow_path
    target_name = "Unnamed Flows" if is_unnamed else "default"

    target_ns = self.repo.get_namespace_by_name(target_name, parent_id=general.id)
    if target_ns is None and is_unnamed:
        target_ns = self.ensure_unnamed_flows_namespace()
    if target_ns is None:
        logger.info(f"Auto-registration skipped: '{target_name}' namespace not found")
        return None

    if self.repo.get_flow_by_path(flow_path):
        return None  # already registered

    reg = FlowRegistration(
        name=name or Path(flow_path).stem,
        flow_path=flow_path,
        namespace_id=target_ns.id,
        owner_id=user_id,
    )
    return self.repo.create_flow(reg)
```

**File: `flowfile_core/flowfile_core/main.py`** (or wherever the startup/lifespan lives)

On app startup, make sure the namespace exists:
```python
# In startup/lifespan:
from flowfile_core.catalog import CatalogService
from flowfile_core.catalog.repository import SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context

try:
    with get_db_context() as db:
        CatalogService(SQLAlchemyCatalogRepository(db)).ensure_unnamed_flows_namespace()
except Exception:
    logger.info("Could not ensure 'Unnamed Flows' namespace on startup", exc_info=True)
```

### Frontend Changes

No UI changes needed: `handleQuickCreateAction()` already calls `createFlow(null, fileName)`. The backend changes above are sufficient.

---

## 4. Dirty Tracking (Smart Close Dialog)

### Problem
`confirmCloseTab()` in `FlowSelectorView.vue` unconditionally opens the save confirmation modal.

### Solution
Add a `_saved_snapshot_hash` to `HistoryManager` that records state at save time, and compare against current state.

### Backend Changes

**File: `flowfile_core/flowfile_core/flowfile/history_manager.py`**

Add to `__slots__` (line 44):
```python
__slots__ = (
    "_config", "_undo_stack", "_redo_stack",
    "_is_restoring", "_last_snapshot_hash",
    "_saved_snapshot_hash",  # NEW
)
```

In `__init__` (line 52) add:
```python
self._saved_snapshot_hash: int | None = None
```

Add methods:
```python
def mark_saved(self, flow_graph) -> None:
    """Record the current state as the saved baseline."""
    snapshot = flow_graph.get_flowfile_data()
    data = snapshot.model_dump(mode="json")
    import json as _json
    self._saved_snapshot_hash = hash(_json.dumps(data, sort_keys=True))

def has_unsaved_changes(self, flow_graph) -> bool:
    """Check if the flow state differs from the last save point."""
    snapshot = flow_graph.get_flowfile_data()
    import json as _json
    current_hash = hash(_json.dumps(snapshot.model_dump(mode="json"), sort_keys=True))
    if self._saved_snapshot_hash is None:
        # Never saved: dirty only if the flow has any nodes
        return len(snapshot.nodes) > 0
    return current_hash != self._saved_snapshot_hash
```

Do **not** reset `_saved_snapshot_hash` inside `clear()` — the save point should survive history clears.

**File: `flowfile_core/flowfile_core/flowfile/flow_graph.py`**

Add instance methods:
```python
def has_unsaved_changes(self) -> bool:
    """Whether the flow has changes since last save."""
    return self._history_manager.has_unsaved_changes(self)

def mark_as_saved(self) -> None:
    """Record the current state as the saved baseline."""
    self._history_manager.mark_saved(self)
```

In `save_flow()` (~line 3736, after the existing `self.flow_settings.path = flow_path` and `_sync_catalog_read_links()`):
```python
self.mark_as_saved()
```

In `__init__()` (after flow is fully built, around line 668 — after `self._history_manager = HistoryManager(...)`):
```python
# Clean baseline for freshly-created flows (empty), so close-tab works correctly
self._history_manager.mark_saved(self)
```

When importing an existing flow (`open_flow`/`_flowfile_data_to_flow_information`), also call `mark_as_saved()` at the end — otherwise a freshly-opened flow appears dirty immediately.

**File: `flowfile_core/flowfile_core/schemas/schemas.py`**

Add to `FlowSettings` (near `track_history` at line 186):
```python
has_unsaved_changes: bool = False
```

**File: `flowfile_core/flowfile_core/flowfile/handler.py`** — already updated ✓

`get_flow_info()` now populates `flow.flow_settings.has_unsaved_changes = flow.has_unsaved_changes()` with a try/except fallback.

### Frontend Changes

**File: `flowfile_frontend/src/renderer/app/views/FlowSelectorView/FlowSelectorView.vue`** — already updated ✓

`confirmCloseTab()` now calls `FlowApi.getFlowSettings(flowId)` and, when `has_unsaved_changes === false`, emits `close-tab` immediately without showing the modal. Fallback to showing the modal on error.

**File: `flowfile_frontend/src/renderer/app/types/flow.types.ts`**

Add the field to the `FlowSettings` TypeScript interface:
```typescript
export interface FlowSettings {
  // ...existing fields...
  has_unsaved_changes?: boolean;
}
```

---

## 5. Save Reliability (Path Normalization Fix)

### Problem
The `is_new_path` check at `routes.py:1097` uses `Path.absolute()`, which doesn't resolve `..`, `.`, or symlinks. Minor differences (trailing `/`, `./` prefix) can falsely trigger the save-as branch and create a duplicate flow identity.

### Fix

**File: `flowfile_core/flowfile_core/routes/routes.py`** (~line 1097):

Change:
```python
is_new_path = (
    flow_path is not None and current_path
    and str(Path(flow_path).absolute()) != str(Path(current_path).absolute())
)
```
to:
```python
is_new_path = (
    flow_path is not None and current_path
    and str(Path(flow_path).resolve()) != str(Path(current_path).resolve())
)
```

Already folded into the updated `save_flow` in section 1.

---

## File Change Summary

| File | Section | Status |
|------|---------|--------|
| `flowfile_core/.../routes/routes.py` | 1, 2, 5 | **Modify** `save_flow` endpoint (silent save, `namespace_id`, Path.resolve) |
| `flowfile_core/.../flowfile/handler.py` | 1, 3, 4 | ✓ already updated (`get_flow_save_location`, `get_flow_info`) |
| `flowfile_core/.../flowfile/flow_graph.py` | 4 | **Modify** add `has_unsaved_changes()`, `mark_as_saved()`; call in `save_flow()` and `__init__()` |
| `flowfile_core/.../flowfile/history_manager.py` | 4 | **Modify** add `_saved_snapshot_hash`, `mark_saved()`, `has_unsaved_changes()` |
| `flowfile_core/.../flowfile/catalog_helpers.py` | 2 | ✓ already updated (`register_flow_in_namespace`) |
| `flowfile_core/.../catalog/service.py` | 3 | **Modify** `auto_register_flow`, add `ensure_unnamed_flows_namespace` |
| `flowfile_core/.../schemas/schemas.py` | 4 | **Modify** add `has_unsaved_changes: bool = False` to `FlowSettings` |
| `flowfile_core/.../main.py` | 3 | **Modify** startup hook to ensure Unnamed Flows namespace |
| `flowfile_core/.../flowfile/manage/io_flowfile.py` | 4 | **Modify** `open_flow` to call `mark_as_saved()` after loading |
| `shared/storage_config.py` | 3 | **Modify** add `unnamed_flows_directory` property + ensure in `_ensure_directories` |
| `flowfile_frontend/.../Header/HeaderButtons.vue` | 1 | **Modify** silent-save logic in `openSaveModal()`, add `openSaveAsModal()` + button |
| `flowfile_frontend/.../Header/utils.ts` | 1, 2 | ✓ already updated (`isTemporaryPath`, `saveFlow`, `saveFlowSilent`) |
| `flowfile_frontend/.../designer/components/SaveDialog.vue` | 2 | ✓ already updated (sidebar, catalog tab, namespace picker) |
| `flowfile_frontend/.../designer/components/CatalogNamespacePicker.vue` | 2 | ✓ already exists |
| `flowfile_frontend/.../designer/components/NamespaceTreeItem.vue` | 2 | ✓ already exists |
| `flowfile_frontend/.../FlowSelectorView/FlowSelectorView.vue` | 4 | ✓ already updated (dirty check before modal) |
| `flowfile_frontend/.../types/flow.types.ts` | 4 | **Modify** add `has_unsaved_changes?: boolean` |
| `flowfile_frontend/.../api/catalog.api.ts` | 2 | **Modify** ensure `getNamespaceTree()` exists |

---

## Implementation Order

1. **Backend dirty tracking** (section 4) — `history_manager.py`, `flow_graph.py`, `schemas.py`. This is already partially wired (handler + FlowSelectorView updated); just need the actual tracking logic.
2. **Routes `save_flow`** (sections 1, 2, 5) — silent save, `namespace_id`, path normalization.
3. **HeaderButtons silent save** (section 1) — use `saveFlowSilent` when path is known and non-temp; add "Save As" button.
4. **Unnamed Flows namespace** (section 3) — `storage_config.py`, `catalog/service.py`, startup hook.
5. **Frontend type** (section 4) — add `has_unsaved_changes?` to TS `FlowSettings`.
6. **API method** (section 2) — add `getNamespaceTree()` if missing.

---

## Verification Plan

### Manual Testing
1. **Silent save** — Create flow via file browser → add a node → click Save → verify no dialog, success toast, file updated on disk.
2. **Save As** — Click new Save As button → file browser opens → save to new path → verify new flow tab with new ID.
3. **First-time save** — Quick Create → click Save → verify save dialog appears (path is in unnamed_flows).
4. **Dirty tracking, no changes** — Open existing saved flow → immediately close tab → verify **no** save dialog.
5. **Dirty tracking, with changes** — Open flow → add a node → close tab → verify save dialog appears.
6. **Dirty tracking, save then close** — Open flow → add node → Save → close → verify no dialog (clean baseline reset).
7. **Quick Create catalog** — Click Quick Create → check catalog sidebar → verify flow under General > Unnamed Flows.
8. **Catalog save** — Click Save on unsaved flow → switch to Catalog tab → pick namespace → Save → verify registration in chosen namespace.
9. **Path normalization** — Save → Save again with trailing slash → verify same flow ID (no duplicate registration or flow tab).

### Automated Tests
```bash
poetry run pytest flowfile_core/tests   # existing tests should still pass
poetry run pytest flowfile_frame/tests
```

New tests to add:
- `test_history_manager_dirty_tracking` — `mark_saved` + `has_unsaved_changes` (before/after save, before/after edit)
- `test_ensure_unnamed_flows_namespace` — idempotent creation
- `test_register_flow_in_namespace` — new registration + update-existing
- `test_save_flow_silent_no_path` — `GET /save_flow?flow_id=X` with no `flow_path` reuses current path
- `test_save_flow_path_normalization` — same path with/without trailing slash returns same flow_id
