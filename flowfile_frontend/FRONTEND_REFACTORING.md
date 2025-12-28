# Frontend Structure Refactoring - Migration Guide

## Overview

This document describes the major refactoring of the frontend structure completed on 2025-12-26.

## What Changed

### 1. Store Architecture - Split Monolithic Store

**Before:**
- Single `column-store.ts` with 17KB of mixed responsibilities

**After:**
- **`flow-store.ts`** - Flow ID and VueFlow instance management
- **`node-store.ts`** - Node data, validation, descriptions, and settings
- **`results-store.ts`** - Run results, node results caching, and validation caching
- **`editor-store.ts`** - Drawer UI, editor state, log viewer, code generator
- **`column-store.ts`** - Simple column tracking (preserved)

### 2. API/Repository Layer

**Created new services layer:**
```
services/api/
├── node.api.ts          # Node CRUD and operations
├── expressions.api.ts   # Expression documentation
└── index.ts             # Central exports
```

**Benefits:**
- Separation of concerns between state management and data fetching
- Easier to test and mock
- Centralized API error handling
- Reusable API methods across stores

### 3. CSS Design Token System

**Consolidated CSS variables:**
- Removed duplicate color definitions from `main.css`
- Created comprehensive design token system in `_variables.css`
- Added semantic color naming (primary, secondary, status colors)
- Added dark theme support structure (for future use)
- Organized tokens by category (typography, colors, spacing, shadows, etc.)

## Backward Compatibility

**All existing code continues to work** without changes!

The `column-store.ts` file re-exports all the new stores:
```typescript
export { useFlowStore } from './flow-store';
export { useNodeStore } from './node-store';
export { useResultsStore } from './results-store';
export { useEditorStore } from './editor-store';
```

**52 files** currently import from `column-store.ts` and will continue to work.

## Migration Path (Optional)

While existing imports work, **new code should** import directly from the specific stores:

### Example Migration

**Before (still works):**
```typescript
import { useNodeStore } from '../stores/column-store';

const nodeStore = useNodeStore();
nodeStore.setFlowId(123);  // This actually updates flow store
```

**After (recommended for new code):**
```typescript
import { useFlowStore } from '../stores/flow-store';
import { useNodeStore } from '../stores/node-store';

const flowStore = useFlowStore();
const nodeStore = useNodeStore();

flowStore.setFlowId(123);  // More explicit
```

## Store Responsibilities

### useFlowStore
- `flowId` - Current flow ID
- `vueFlowInstance` - Vue Flow instance reference
- `setFlowId(flowId)` - Set current flow
- `getVueFlowInstance()` - Get Vue Flow instance

### useNodeStore
- Node data management (`getNodeData`, `getCurrentNodeData`)
- Node validation (`setNodeValidateFunc`, `validateNode`)
- Node descriptions (`getNodeDescription`, `setNodeDescription`)
- Node settings updates (`updateSettings`, `updateUserDefinedSettings`)
- Expressions overview (`getExpressionsOverview`)
- Data types and preview size management

### useResultsStore
- Run results (`insertRunResult`, `getRunResult`, `resetRunResults`)
- Node results caching (`setNodeResult`, `getNodeResult`, `clearFlowResults`)
- Validation caching (`setNodeValidation`, `getNodeValidation`)
- Result versioning for reactivity

### useEditorStore
- Drawer management (`openDrawer`, `closeDrawer`, `toggleDrawer`)
- Analysis drawer (`openAnalysisDrawer`, `closeAnalysisDrawer`)
- Code generator visibility (`toggleCodeGenerator`, `setCodeGeneratorVisibility`)
- Log viewer state (`showLogViewer`, `hideLogViewer`, `toggleLogViewer`)
- Editor data (`setInitialEditorData`, `setInputCode`)
- Run state (`isRunning`, `showFlowResult`, `tableVisible`)

## API Services Usage

### Example: Using Node API

**Before (in store):**
```typescript
const response = await axios.get('/node', {
  params: { flow_id: flowId, node_id: nodeId }
});
```

**After (in store):**
```typescript
import { NodeApi } from '../services/api';

const nodeData = await NodeApi.getNodeData(flowId, nodeId);
```

## CSS Design Tokens

### Available Token Categories

**Typography:**
- `--font-family-base`
- `--font-size-{xs,sm,md,lg}`
- `--font-weight-{normal,medium,semibold}`
- `--line-height-{tight,normal}`

**Brand Colors:**
- `--primary-blue`, `--primary-blue-hover`, `--light-blue`

**Semantic Colors:**
- `--color-background-{primary,secondary,hover,selected}`
- `--color-text-{primary,secondary,selected}`
- `--color-border-{primary,secondary}`
- `--color-{success,danger,warning,info}`

**Spacing:**
- `--spacing-{xs,sm,md,lg,xl,2xl,3xl}`

**Shadows:**
- `--shadow-{sm,default,md,lg,xl}`

**Border Radius:**
- `--border-radius-{sm,md,lg,full}`

**Animations:**
- `--transition-{fast,base,slow}`
- `--transition-timing`

**Z-Index:**
- `--z-index-{dropdown,modal,tooltip,notification}`

### Example Usage

```css
.my-button {
  background: var(--primary-blue);
  color: var(--color-text-primary);
  padding: var(--spacing-md) var(--spacing-lg);
  border-radius: var(--border-radius-md);
  transition: all var(--transition-fast) var(--transition-timing);
  box-shadow: var(--shadow-default);
}

.my-button:hover {
  background: var(--primary-blue-hover);
  box-shadow: var(--shadow-md);
}
```

## Testing

The refactored code:
- ✅ Maintains backward compatibility
- ✅ Preserves all existing functionality
- ✅ Improves code organization
- ✅ Separates concerns
- ✅ Makes testing easier

## Future Improvements

1. **Gradually migrate** components to use specific stores
2. **Add unit tests** for individual stores
3. **Add unit tests** for API services
4. **Implement dark theme** using the prepared dark theme tokens
5. **Consider further component extraction** for large components
6. **Dependency audit** to reduce bundle size

## Questions or Issues?

If you encounter any issues with the refactored code, check:
1. Are imports from `column-store.ts` working? (Should work via re-exports)
2. Are all store methods still accessible? (Check the specific store files)
3. Are API calls failing? (Check the `services/api/` implementations)

For issues or suggestions, contact the development team.

---

**Migration completed:** 2025-12-26
**Backward compatibility:** ✅ Full
**Breaking changes:** ❌ None
