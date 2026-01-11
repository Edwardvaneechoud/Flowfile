# Flowfile WASM

A minimal, browser-based data flow designer using Pyodide and Polars. This is a lightweight version of Flowfile that runs entirely in the browser without any server-side computation.

## Features

- **Browser-Based Execution**: All data processing happens in your browser using WebAssembly
- **Polars Integration**: Full Polars DataFrame operations via Pyodide
- **10 Essential Nodes**:
  - **Read CSV**: Load CSV files from your local machine
  - **Filter**: Apply conditional row filtering
  - **Select**: Pick, reorder, and rename columns
  - **Group By**: Aggregate data with various functions
  - **Join**: Combine datasets with different join types
  - **Sort**: Order rows by one or more columns
  - **With Columns**: Add or modify columns using expressions
  - **Unique**: Get distinct rows
  - **Head/Limit**: Sample first N rows
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

## Technical Details

- **Frontend**: Vue 3 + TypeScript + Vite
- **Graph Visualization**: Vue Flow
- **State Management**: Pinia
- **Python Runtime**: Pyodide 0.26.4
- **Data Processing**: Polars (via WASM)

## Limitations

- File size limited by browser memory
- No persistence (flows are not saved)
- Limited to the 10 essential nodes
- No Python code execution in advanced mode (expressions only)
