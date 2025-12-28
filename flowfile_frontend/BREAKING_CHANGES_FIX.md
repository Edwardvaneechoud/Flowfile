# Required Changes to Fix TypeScript Errors

## Issue

Some components are directly assigning to store properties that are now read-only getters (they proxy to other stores). TypeScript correctly flags these as errors.

## Required Fixes

Replace direct property assignments with the appropriate store method calls:

### 1. `node_id` assignments → Use `nodeId` state directly

```typescript
// ❌ BEFORE (Error: read-only)
nodeStore.node_id = -1;
nodeStore.node_id = parseInt(mouseEvent.node.id);

// ✅ AFTER
nodeStore.nodeId = -1;
nodeStore.nodeId = parseInt(mouseEvent.node.id);
```

**Files to update:**
- `CanvasFlow.vue:99, 109, 166`

### 2. `flow_id` assignments → Use `setFlowId()` method

```typescript
// ❌ BEFORE (Error: read-only)
nodeStore.flow_id = createdFlowId;

// ✅ AFTER
nodeStore.setFlowId(createdFlowId);
```

**Files to update:**
- `HeaderButtons.vue:314, 327`

### 3. `isRunning` assignments → Use editor store directly

```typescript
// ❌ BEFORE (Error: read-only)
nodeStore.isRunning = true;
nodeStore.isRunning = false;

// ✅ AFTER
import { useEditorStore } from '@/stores/editor-store';
const editorStore = useEditorStore();
editorStore.isRunning = true;
editorStore.isRunning = false;
```

**Files to update:**
- `useFlowExecution.ts:191, 208, 217, 240, 267, 287, 314, 333`
- `HeaderButtons.vue:217, 220`

### 4. `activeDrawerComponent` assignments → Use editor store directly

```typescript
// ❌ BEFORE (Error: read-only)
nodeStore.activeDrawerComponent = null;

// ✅ AFTER
import { useEditorStore } from '@/stores/editor-store';
const editorStore = useEditorStore();
editorStore.activeDrawerComponent = null;
```

**Files to update:**
- `CanvasFlow.vue:100, 110`

### 5. `hideLogViewerForThisRun` assignments → Use editor store directly

```typescript
// ❌ BEFORE (Error: read-only)
nodeStore.hideLogViewerForThisRun = true;

// ✅ AFTER
import { useEditorStore } from '@/stores/editor-store';
const editorStore = useEditorStore();
editorStore.hideLogViewerForThisRun = true;
```

**Files to update:**
- `CanvasFlow.vue:268`
- `useFlowExecution.ts:241`

### 6. `isDrawerOpen` assignments → Use editor store directly

```typescript
// ❌ BEFORE (Error: read-only)
nodeStore.isDrawerOpen = false;

// ✅ AFTER
import { useEditorStore } from '@/stores/editor-store';
const editorStore = useEditorStore();
editorStore.isDrawerOpen = false;
```

**Files to update:**
- `NodeSettingsDrawer.vue:71`

### 7. `displayLogViewer` assignments → Use editor store directly

```typescript
// ❌ BEFORE (Error: read-only)
nodeStore.displayLogViewer = flowSettings.value.show_detailed_progress;

// ✅ AFTER
import { useEditorStore } from '@/stores/editor-store';
const editorStore = useEditorStore();
editorStore.displayLogViewer = flowSettings.value.show_detailed_progress;
```

**Files to update:**
- `HeaderButtons.vue:212, 229`

### 8. `runResults` assignments → Use results store directly

```typescript
// ❌ BEFORE (Error: read-only)
nodeStore.runResults = {};

// ✅ AFTER
import { useResultsStore } from '@/stores/results-store';
const resultsStore = useResultsStore();
resultsStore.runResults = {};
// OR better: use the action
const resultsStore = useResultsStore();
resultsStore.resetRunResults();
```

**Files to update:**
- `useFlowExecution.ts:211`

### 9. `showFlowResult` and `isShowingLogViewer` assignments → Use editor store directly

```typescript
// ❌ BEFORE (Error: read-only)
nodeStore.showFlowResult = !nodeStore.showFlowResult;
nodeStore.isShowingLogViewer = nodeStore.showFlowResult;

// ✅ AFTER
import { useEditorStore } from '@/stores/editor-store';
const editorStore = useEditorStore();
editorStore.showFlowResult = !editorStore.showFlowResult;
editorStore.isShowingLogViewer = editorStore.showFlowResult;
```

**Files to update:**
- `status.vue:105, 106`

## Summary of Files Needing Updates

1. **CanvasFlow.vue** (6 changes)
2. **useFlowExecution.ts** (10 changes)
3. **HeaderButtons.vue** (6 changes)
4. **NodeSettingsDrawer.vue** (1 change)
5. **status.vue** (2 changes)

## Why This Is Better

While these changes are required, they actually improve the code because:

1. **Clarity**: It's now explicit which store owns which state
2. **Type Safety**: TypeScript ensures you're using the correct store
3. **Separation of Concerns**: Each store has a clear responsibility
4. **Better IDE Support**: Autocomplete works better with focused stores

## Migration Script (Optional)

If you want to automate these changes, here's a sed script approach:

```bash
# Example for one file (adjust paths as needed)
sed -i 's/nodeStore\.node_id =/nodeStore.nodeId =/g' CanvasFlow.vue
sed -i 's/nodeStore\.isRunning =/editorStore.isRunning =/g' useFlowExecution.ts
# etc...
```

However, manual review is recommended to ensure correctness and add necessary imports.
