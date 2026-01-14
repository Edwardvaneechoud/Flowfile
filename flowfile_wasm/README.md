# Flowfile WASM

A minimal, browser-based data flow designer using Pyodide and Polars. This is a lightweight version of Flowfile that runs entirely in the browser without any server-side computation.

## Try It Now

**[Open Flowfile WASM →](https://demo.flowfile.org)**

No installation required. Your data stays 100% in your browser.

<!-- TODO: Add screenshot of WASM interface showing a complete flow -->
![Flowfile WASM Interface](docs/images/wasm-interface.png)

## Features

- **Browser-Based Execution**: All data processing happens in your browser using WebAssembly
- **Polars Integration**: Full Polars DataFrame operations via Pyodide
- **Session Persistence**: Your flow is automatically saved to session storage
- **14 Essential Nodes**:
  - **Read CSV**: Load CSV files from your local machine
  - **Manual Input**: Enter data manually in CSV format
  - **Filter**: Apply conditional row filtering (basic and advanced modes)
  - **Select**: Pick, reorder, and rename columns
  - **Group By**: Aggregate data with various functions (sum, count, mean, min, max, etc.)
  - **Pivot**: Reshape data from long to wide format
  - **Unpivot**: Reshape data from wide to long format
  - **Join**: Combine datasets with different join types (inner, left, right, full, semi, anti)
  - **Sort**: Order rows by one or more columns
  - **Polars Code**: Write custom Polars/Python code for advanced transformations
  - **Unique**: Remove duplicate rows
  - **Take Sample**: Limit to first N rows
  - **Preview**: Display results in the browser
  - **Output**: Download processed data as CSV or Parquet

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

<!-- TODO: Add GIF showing the basic workflow of dragging, connecting, and running -->
![Basic Workflow](docs/images/wasm-workflow.gif)

## Polars Code Node

The Polars Code node allows you to write custom Python/Polars code for advanced transformations. Your code receives `input_df` (a Polars DataFrame) and should produce a result DataFrame.

Example:
```python
# Simple transformation
output_df = input_df.with_columns(
    (pl.col("price") * pl.col("quantity")).alias("total")
)

# Or just return an expression
input_df.filter(pl.col("status") == "active")
```

The editor includes autocompletion for Polars functions and your DataFrame columns.

<!-- TODO: Add screenshot of Polars Code node with autocomplete dropdown visible -->
![Polars Code Editor with Autocomplete](docs/images/polars-code-autocomplete.png)

## Technical Details

- **Frontend**: Vue 3 + TypeScript + Vite
- **Graph Visualization**: Vue Flow
- **State Management**: Pinia
- **Code Editor**: CodeMirror 6 with Python syntax highlighting
- **Python Runtime**: Pyodide 0.27.7
- **Data Processing**: Polars (via WASM)

## Limitations

- File size limited by browser memory (~100MB recommended maximum)
- Session storage persistence only (cleared when browser tab is closed)
- Limited to 14 essential nodes (compared to 30+ in the full Flowfile editor)
- No database connections (use full version for database support)
- No cloud storage integration (use full version for S3, etc.)

## Browser Compatibility

| Browser | Version | Support |
|---------|---------|---------|
| Chrome | 90+ | Full support |
| Firefox | 90+ | Full support |
| Safari | 15+ | Full support |
| Edge | 90+ | Full support |
| Mobile browsers | - | Limited (desktop recommended) |

> **Note**: The first load downloads Pyodide and Polars (~15MB total). Subsequent visits use cached files and load faster.

## Troubleshooting

### Pyodide won't load

- **Check your internet connection** - Pyodide is loaded from a CDN
- **Try a different browser** - Chrome typically has the best WebAssembly support
- **Disable ad blockers** - Some ad blockers block CDN requests
- **Check the browser console** - Press F12 and look for error messages
- **Try incognito mode** - Rules out extension conflicts

### File won't load or parse

- **Check file size** - Keep files under 100MB for best performance
- **Verify CSV format** - Ensure proper comma/delimiter separation
- **Check encoding** - UTF-8 encoding works best
- **Remove special characters** - Some special characters in headers can cause issues

### Browser becomes slow or unresponsive

- **Reduce data size** - Use the "Take Sample" node to work with a subset first
- **Close other tabs** - Free up browser memory
- **Refresh the page** - Clears memory and restarts the session
- **Use a smaller file** - Try with a sample of your data

### Results disappeared after closing tab

This is expected behavior. Session storage is cleared when you close the tab.

**To preserve your work:**
- Use the **Output node** to download results as CSV or Parquet before closing
- Use the **Export** feature to save your flow configuration

### Flow won't run / errors in execution

- **Check node connections** - Ensure all nodes are properly connected
- **Verify input data** - Make sure your CSV loaded correctly
- **Check Polars Code syntax** - If using Polars Code node, verify Python syntax
- **Look at error messages** - Error details appear in the bottom panel

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
