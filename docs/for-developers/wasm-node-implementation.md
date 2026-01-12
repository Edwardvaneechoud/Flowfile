# WASM Node Implementation Guide

Context and guidelines for implementing nodes that work across both flowfile_core and flowfile_wasm.

!!! info "AI Development Context"
    Use the prompt below when asking an AI assistant to implement a new node. It provides the necessary context to ensure schema parity between implementations.

---

## Context Prompt for New Node Requests

```text
Flowfile has two implementations that must stay synchronized:

1. **flowfile_core** (Python/Pydantic) - The server-side engine with full node definitions in:
   - `flowfile_core/schemas/input_schema.py` - Node settings classes (NodeBase → NodeSingleInput/NodeMultiInput hierarchy)
   - `flowfile_core/schemas/transform_schema.py` - Transform data models (FilterInput, SelectInput, etc.)
   - `flowfile_core/configs/node_store/nodes.py` - Node registration with NodeTemplate

2. **flowfile_wasm** (TypeScript/Vue) - Browser-based lite version in:
   - `flowfile_wasm/src/types/index.ts` - TypeScript interfaces mirroring core schemas exactly
   - `flowfile_wasm/src/components/nodes/` - Vue settings components
   - `flowfile_wasm/src/stores/flow-store.ts` - Execution logic using Pyodide

**Critical Requirements:**
- Schemas must be identical between core (Pydantic) and WASM (TypeScript)
- Field names, types, and defaults must match exactly
- Calculations must produce identical results
- WASM nodes use client-side Polars via Pyodide

When implementing a new node, provide: the node type identifier, settings schema for both implementations, the transform logic, and the Vue settings component. Reference existing nodes (e.g., FilterInput, SelectInput) as patterns.
```

---

## Architecture Overview

### Schema Alignment

Both implementations share identical schema structures:

| Concept | flowfile_core | flowfile_wasm |
|---------|---------------|---------------|
| Base classes | `NodeBase`, `NodeSingleInput`, `NodeMultiInput` | Same names in TypeScript |
| Settings | Pydantic models in `input_schema.py` | TypeScript interfaces in `types/index.ts` |
| Transforms | `transform_schema.py` | Mirrored in `types/index.ts` |
| Node registry | `NODE_TYPE_TO_SETTINGS_CLASS` | `NODE_TYPES` const |

### Key Files Reference

| Component | File | Purpose |
|-----------|------|---------|
| WASM Types | `flowfile_wasm/src/types/index.ts` | Master type definitions |
| WASM Nodes | `flowfile_wasm/src/components/nodes/*.vue` | Node UI components |
| WASM Store | `flowfile_wasm/src/stores/flow-store.ts` | State & execution |
| Core Schemas | `flowfile_core/schemas/input_schema.py` | Node settings models |
| Core Transforms | `flowfile_core/schemas/transform_schema.py` | Transform data models |
| Core Registry | `flowfile_core/configs/node_store/nodes.py` | Node templates |

---

## Implementation Checklist

When adding a new node to both implementations:

- [ ] Define Pydantic model in `input_schema.py`
- [ ] Add transform schema in `transform_schema.py` (if needed)
- [ ] Register node in `nodes.py` with `NodeTemplate`
- [ ] Mirror TypeScript interface in `flowfile_wasm/src/types/index.ts`
- [ ] Add node type to `NODE_TYPES` const
- [ ] Create Vue settings component in `flowfile_wasm/src/components/nodes/`
- [ ] Add execution logic in `flow-store.ts`
- [ ] Verify calculations produce identical results

---

## Example: Existing Node Patterns

Reference these existing implementations as templates:

- **Filter**: `FilterInput` / `NodeFilter` - Single input with filter conditions
- **Select**: `SelectInput` / `NodeSelect` - Column selection and renaming
- **Join**: `JoinInput` / `NodeJoin` - Multi-input combining datasets
- **GroupBy**: `GroupByInput` / `NodeGroupBy` - Aggregation operations
- **Sort**: `SortByInput` / `NodeSort` - Ordering data

Each follows the pattern of having matching schemas in both Python and TypeScript with identical field names and types.
