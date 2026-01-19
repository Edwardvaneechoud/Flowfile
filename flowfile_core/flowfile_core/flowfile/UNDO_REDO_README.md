# Undo/Redo System Documentation

This document describes the undo/redo system implemented for Flowfile, enabling users to revert and reapply changes to their flow graphs.

## Architecture Overview

The system uses a **snapshot-based approach** with two stacks:
- **Undo Stack**: Contains snapshots of states BEFORE changes were made
- **Redo Stack**: Contains snapshots of states that were undone

```
┌─────────────────────────────────────────────────────────────┐
│                      Frontend (Vue)                         │
│  ┌─────────────────┐    ┌─────────────────────────────┐    │
│  │ UndoRedoControls│◄───│       flowStore             │    │
│  │  (UI Buttons)   │    │  - historyState (reactive)  │    │
│  └────────┬────────┘    │  - canUndo, canRedo         │    │
│           │             └──────────────▲──────────────┘    │
│           │                            │                    │
│           ▼                            │                    │
│  ┌─────────────────┐    ┌──────────────┴──────────────┐    │
│  │    FlowApi      │───►│    OperationResponse        │    │
│  │  - undo()       │    │  - success: boolean         │    │
│  │  - redo()       │    │  - history: HistoryState    │    │
│  │  - insertNode() │    └─────────────────────────────┘    │
│  │  - deleteNode() │                                        │
│  │  - connectNode()│                                        │
│  └────────┬────────┘                                        │
└───────────┼─────────────────────────────────────────────────┘
            │
            ▼
┌───────────────────────────────────────────────────────────────┐
│                      Backend (FastAPI)                        │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                    API Endpoints                         │ │
│  │  POST /editor/add_node/      → OperationResponse        │ │
│  │  POST /editor/delete_node/   → OperationResponse        │ │
│  │  POST /editor/connect_node/  → OperationResponse        │ │
│  │  POST /editor/undo/          → UndoRedoResult           │ │
│  │  POST /editor/redo/          → UndoRedoResult           │ │
│  └──────────────────────┬──────────────────────────────────┘ │
│                         │                                     │
│                         ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                   FlowGraph                              │ │
│  │  - capture_history_snapshot()                           │ │
│  │  - _history_manager: HistoryManager                     │ │
│  └──────────────────────┬──────────────────────────────────┘ │
│                         │                                     │
│                         ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                 HistoryManager                           │ │
│  │  - _undo_stack: deque[HistoryEntry]                     │ │
│  │  - _redo_stack: deque[HistoryEntry]                     │ │
│  │  - capture_snapshot()                                    │ │
│  │  - capture_if_changed()                                  │ │
│  │  - undo() / redo()                                       │ │
│  └─────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────┘
```

## Key Components

### Backend

#### 1. HistoryManager (`history_manager.py`)

The core component managing undo/redo stacks.

```python
class HistoryManager:
    _undo_stack: deque[HistoryEntry]  # States before changes
    _redo_stack: deque[HistoryEntry]  # States that were undone
    _is_restoring: bool               # Prevents capture during undo/redo
```

**Key Methods:**

- `capture_snapshot()`: Captures state BEFORE an operation (used by endpoints that modify the graph)
- `capture_if_changed()`: Captures state only if it actually changed (used for batched operations)
- `undo()`: Restores to previous state, moves current state to redo stack
- `redo()`: Restores undone state, moves current state to undo stack

#### 2. HistoryEntry (`history_schema.py`)

Stores a compressed snapshot of the flow state.

```python
class HistoryEntry:
    snapshot: CompressedSnapshot  # Compressed flow state
    action_type: HistoryActionType
    description: str              # e.g., "Add select node"
    timestamp: float
    node_id: Optional[int]
    snapshot_hash: int            # For duplicate detection
```

#### 3. OperationResponse (`history_schema.py`)

Standard response from modifying endpoints, includes history state.

```python
class OperationResponse:
    success: bool
    message: Optional[str]
    history: HistoryState  # Current undo/redo availability
```

### Frontend

#### 1. flowStore (`flow-store.ts`)

Pinia store holding reactive history state.

```typescript
const useFlowStore = defineStore("flow", {
  state: () => ({
    historyState: {
      can_undo: false,
      can_redo: false,
      undo_description: null,
      redo_description: null,
    },
  }),
  actions: {
    updateHistoryState(state: HistoryState) {
      this.historyState = state;
    },
  },
});
```

#### 2. UndoRedoControls (`UndoRedoControls.vue`)

UI component with undo/redo buttons and keyboard shortcuts.

- Buttons reactively enable/disable based on `flowStore.canUndo` / `flowStore.canRedo`
- Keyboard shortcuts: `Cmd+Z` (undo), `Cmd+Shift+Z` or `Cmd+Y` (redo)

## How It Works

### Adding a Node

```
1. User drags node onto canvas
2. Frontend calls POST /editor/add_node/
3. Backend:
   a. Captures pre-snapshot (state before add)
   b. Adds node to graph
   c. Applies default settings (if any)
   d. Captures batched history entry "Add {type} node"
   e. Returns OperationResponse with updated HistoryState
4. Frontend updates flowStore.historyState from response
5. Undo button becomes enabled
```

### Connecting Nodes

```
1. User draws connection between nodes
2. Frontend calls POST /editor/connect_node/
3. Backend:
   a. Captures pre-snapshot via capture_history_snapshot()
   b. Creates connection
   c. Returns OperationResponse with updated HistoryState
4. Frontend updates flowStore.historyState from response
```

### Undo Operation

```
1. User clicks Undo or presses Cmd+Z
2. Frontend calls POST /editor/undo/
3. Backend (HistoryManager.undo()):
   a. Sets _is_restoring = True (prevents new captures)
   b. Pops entry from undo_stack
   c. Saves CURRENT state to redo_stack
   d. Restores flow from popped entry's snapshot
   e. Sets _is_restoring = False
   f. Returns UndoRedoResult with action description
4. Frontend reloads flow via loadFlow()
5. Flow visually returns to previous state
```

### Redo Operation

```
1. User clicks Redo or presses Cmd+Shift+Z
2. Frontend calls POST /editor/redo/
3. Backend (HistoryManager.redo()):
   a. Sets _is_restoring = True
   b. Pops entry from redo_stack
   c. Saves CURRENT state to undo_stack
   d. Restores flow from popped entry's snapshot
   e. Sets _is_restoring = False
4. Frontend reloads flow
```

## Duplicate Detection

The system prevents capturing duplicate snapshots:

**In `capture_snapshot()`:**
- Compares current state hash against the LAST ENTRY in undo stack
- If hashes match, skip capture (prevents double-capturing same state)

**In `capture_if_changed()`:**
- Compares pre-snapshot hash against post-snapshot hash
- Only captures if state actually changed (for batched operations)

```python
# capture_snapshot compares against last captured entry
if self._undo_stack:
    last_entry_hash = self._undo_stack[-1].snapshot_hash
    if last_entry_hash == current_hash:
        return False  # Skip duplicate

# capture_if_changed compares pre vs post
if pre_hash == current_hash:
    return False  # No change occurred
```

## Memory Optimization

- **Compression**: Snapshots are compressed using zlib (60-80% size reduction)
- **Hash-based comparison**: Fast duplicate detection without JSON serialization
- **Stack limits**: Configurable max stack size via `HistoryConfig.max_stack_size`
- **`__slots__`**: Used in HistoryEntry for reduced memory overhead

## Event-Driven Architecture

The frontend uses an event-driven approach instead of polling:

1. **No polling**: History state is NOT fetched periodically
2. **Response-based updates**: Every modifying endpoint returns `OperationResponse` with current `HistoryState`
3. **Reactive UI**: Buttons automatically enable/disable via Pinia store reactivity

```typescript
// In Canvas.vue - after any operation
const response = await connectNode(flowStore.flowId, nodeConnection);
if (response?.history) {
  flowStore.updateHistoryState(response.history);  // UI updates automatically
}
```

## Configuration

History tracking can be enabled/disabled per flow:

```python
class FlowSettings(BaseModel):
    track_history: bool = True  # Enable/disable for this flow

class HistoryConfig:
    enabled: bool = True
    max_stack_size: int = 50
    use_compression: bool = True
    compression_level: int = 6
```

## Supported Operations

Operations that create history entries:

| Operation | History Description |
|-----------|-------------------|
| Add node | "Add {type} node" |
| Delete node | "Delete {type} node" |
| Connect nodes | "Connect {from} -> {to}" |
| Delete connection | "Delete connection {from} -> {to}" |
| Copy node | "Copy {type} node" |
| Update settings | "Update {type} settings" |
| Apply layout | "Apply standard layout" |

## Troubleshooting

### History not being captured

Check logs for:
```
History: Skipping '{description}' - history disabled
History: Skipping '{description}' - currently restoring
History: Skipping duplicate snapshot for: {description}
```

### Undo removes wrong item

Ensure operations are properly batched. The `add_node` endpoint batches:
1. Adding the node promise
2. Applying default settings

Into a single "Add {type} node" history entry.

### Connection undo not working

Verify `capture_history_snapshot()` is called BEFORE the connection is created:
```python
flow.capture_history_snapshot(
    HistoryActionType.ADD_CONNECTION,
    f"Connect {from_id} -> {to_id}"
)
add_connection(flow, node_connection)
```

## Files

**Backend:**
- `flowfile_core/flowfile/history_manager.py` - Core HistoryManager class
- `flowfile_core/schemas/history_schema.py` - Pydantic models (HistoryState, OperationResponse, etc.)
- `flowfile_core/routes/routes.py` - API endpoints

**Frontend:**
- `stores/flow-store.ts` - Pinia store with historyState
- `views/DesignerView/UndoRedoControls.vue` - UI component
- `views/DesignerView/Canvas.vue` - Main canvas handling operations
- `api/flow.api.ts` - API client methods
