# Flowfile WASM

A minimal, browser-based data flow designer using Pyodide and Polars. This is a lightweight version of Flowfile that runs entirely in the browser without any server-side computation.

## Features

- **Browser-Based Execution**: All data processing happens in your browser using WebAssembly
- **Polars Integration**: Full Polars DataFrame operations via Pyodide
- **Session Persistence**: Your flow is automatically saved to session storage
- **11 Essential Nodes**:
  - **Read CSV**: Load CSV files from your local machine
  - **Manual Input**: Enter data manually in CSV format
  - **Filter**: Apply conditional row filtering (basic and advanced modes)
  - **Select**: Pick, reorder, and rename columns
  - **Group By**: Aggregate data with various functions (sum, count, mean, min, max, etc.)
  - **Join**: Combine datasets with different join types (inner, left, right, full, semi, anti)
  - **Sort**: Order rows by one or more columns
  - **Polars Code**: Write custom Polars/Python code for advanced transformations
  - **Unique**: Remove duplicate rows
  - **Take Sample**: Limit to first N rows
  - **Preview**: Display results

## Getting Started

### Install Dependencies

```bash
cd flowfile_wasm
npm install
```

### Development

```bash
npm run dev
```

Open http://localhost:5174 in your browser.

### Build for Production

```bash
npm run build
```

The output will be in the `dist` folder.

## Usage

1. **Wait for Pyodide to load**: The app will show a loading indicator while Pyodide and Polars are being loaded
2. **Drag nodes** from the sidebar onto the canvas
3. **Connect nodes** by dragging from output handles (right side) to input handles (left side)
4. **Configure nodes** by clicking on them - settings panel will appear on the right
5. **Run the flow** by clicking the "Run Flow" button in the header
6. **View results** in the Table Preview panel at the bottom

## Polars Code Node

The Polars Code node allows you to write custom Python/Polars code for advanced transformations. Your code receives `input_df` (a Polars DataFrame) and should produce a result DataFrame.

Example:
```python
# Simple transformation
output_df = input_df.with_columns(
    pl.col("price") * pl.col("quantity").alias("total")
)

# Or just return an expression
input_df.filter(pl.col("status") == "active")
```

The editor includes autocompletion for Polars functions and your DataFrame columns.

## Technical Details

- **Frontend**: Vue 3 + TypeScript + Vite
- **Graph Visualization**: Vue Flow
- **State Management**: Pinia
- **Code Editor**: CodeMirror 6 with Python syntax highlighting
- **Python Runtime**: Pyodide 0.27.7
- **Data Processing**: Polars (via WASM)

## Limitations

- File size limited by browser memory
- Session storage persistence only (cleared when browser tab is closed)
- Limited to the 11 essential nodes (compared to the full Flowfile editor)

## Adding New Nodes (AI Context Prompt)

Use this prompt when requesting AI assistance to implement a new node:

```
Flowfile has two implementations that must stay synchronized:

1. **flowfile_core** (Python/Pydantic) - Server-side engine:
   - `flowfile_core/schemas/input_schema.py` - Node settings (NodeBase → NodeSingleInput/NodeMultiInput)
   - `flowfile_core/schemas/transform_schema.py` - Transform models (FilterInput, SelectInput, etc.)
   - `flowfile_core/configs/node_store/nodes.py` - Node registration, naming, and descriptions (WASM must follow these)

2. **flowfile_wasm** (TypeScript/Vue) - Browser-based lite version:
   - `flowfile_wasm/src/types/index.ts` - TypeScript interfaces mirroring core schemas
   - `flowfile_wasm/src/components/nodes/` - Vue settings components (same as `flowfile_frontend/src/renderer/app/components/nodes/`)
   - `flowfile_wasm/src/stores/flow-store.ts` - Execution logic using Pyodide

**Critical Requirements:**
- Schemas must be identical between core (Pydantic) and WASM (TypeScript)
- Field names, types, and defaults must match exactly
- Calculations must produce identical results
- Follow the existing code style and layout patterns in each codebase unless technically impossible
- Reference existing nodes (FilterInput, SelectInput, JoinInput) as implementation patterns

Provide: node type identifier, settings schema for both implementations, transform logic, and Vue settings component.
```
