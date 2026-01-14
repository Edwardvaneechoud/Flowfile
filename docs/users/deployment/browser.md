# Browser Version (WASM)

The fastest way to try Flowfile - no installation required.

## Try It Now

[**Open Flowfile in Your Browser →**](https://demo.flowfile.org)

<!-- TODO: Add screenshot of WASM interface -->
![Flowfile Browser Interface](../../assets/images/wasm/browser-interface.png)

## How It Works

Flowfile's browser version runs entirely in your browser using WebAssembly (WASM) and Pyodide. Your data never leaves your machine - all processing happens locally.

```
┌─────────────────────────────────────────┐
│           Your Browser                   │
│  ┌─────────────────────────────────┐    │
│  │  Flowfile WASM                  │    │
│  │  ┌───────────┐  ┌───────────┐   │    │
│  │  │  Vue.js   │  │  Pyodide  │   │    │
│  │  │    UI     │  │  (Python) │   │    │
│  │  └───────────┘  └───────────┘   │    │
│  │         │              │        │    │
│  │         └──────┬───────┘        │    │
│  │                │                │    │
│  │         ┌──────▼───────┐        │    │
│  │         │    Polars    │        │    │
│  │         │    (WASM)    │        │    │
│  │         └──────────────┘        │    │
│  └─────────────────────────────────┘    │
│                                         │
│  Your data stays here - nothing sent    │
│  to any server!                         │
└─────────────────────────────────────────┘
```

## Getting Started

1. **Open** [demo.flowfile.org](https://demo.flowfile.org)
2. **Wait** for Pyodide to load (first load takes ~10-15 seconds)
3. **Drag** nodes from the sidebar onto the canvas
4. **Connect** nodes by dragging from output handles to input handles
5. **Configure** each node by clicking on it
6. **Run** your flow and view results

<!-- TODO: Add GIF showing basic workflow -->
![Basic Workflow Demo](../../assets/images/wasm/basic-workflow.gif)

## Available Nodes

The browser version includes 14 essential nodes:

### Input
| Node | Description |
|------|-------------|
| **Read CSV** | Load CSV files from your local machine |
| **Manual Input** | Enter data manually in CSV format |

### Transform
| Node | Description |
|------|-------------|
| **Filter** | Apply conditional row filtering |
| **Select** | Pick, reorder, and rename columns |
| **Sort** | Order rows by one or more columns |
| **Unique** | Remove duplicate rows |
| **Take Sample** | Limit to first N rows |

### Reshape
| Node | Description |
|------|-------------|
| **Group By** | Aggregate data with functions (sum, count, mean, min, max, etc.) |
| **Pivot** | Reshape data from long to wide format |
| **Unpivot** | Reshape data from wide to long format |
| **Join** | Combine datasets (inner, left, right, full, semi, anti joins) |

### Advanced
| Node | Description |
|------|-------------|
| **Polars Code** | Write custom Python/Polars code for advanced transformations |

### Output
| Node | Description |
|------|-------------|
| **Preview** | Display results in the browser |
| **Output** | Download processed data as CSV or Parquet |

## Polars Code Node

Write custom Python/Polars code for transformations not covered by the built-in nodes:

```python
# Your code receives 'input_df' as a Polars DataFrame
# Return a transformed DataFrame

output_df = input_df.with_columns(
    (pl.col("price") * pl.col("quantity")).alias("total")
)

# Or simply return an expression
input_df.filter(pl.col("status") == "active")
```

The editor includes autocompletion for Polars functions and your DataFrame columns.

<!-- TODO: Add screenshot of Polars Code node with autocomplete -->
![Polars Code Editor](../../assets/images/wasm/polars-code-editor.png)

## Why Browser Version?

**Pros**

- Zero installation - just open a URL
- Complete privacy - data never leaves your browser
- Works on any device with a modern browser
- Great for quick data exploration
- No account required
- Shareable via URL

**Cons**

- Limited to ~100MB files (browser memory constraints)
- 14 nodes vs 30+ in full version
- No database connections
- Session storage only (data lost when tab closes)
- Initial load time (~10-15 seconds)

## Browser vs Full Version

| Feature | Browser (WASM) | Full Version |
|---------|----------------|--------------|
| Installation | None | pip install / Docker / Desktop |
| Nodes | 14 essential | 30+ nodes |
| File size | ~100MB limit | Limited by system RAM |
| Data persistence | Session only | Files / Database |
| Database connections | No | Yes |
| Cloud storage | No | Yes (S3, etc.) |
| Code generation | Yes | Yes |
| Privacy | 100% local | Local or server |

## When to Use Browser Version

**Use Browser when:**

- Trying Flowfile for the first time
- Quick one-off data transformations
- Working on a shared/public computer
- Teaching or demonstrating data concepts
- Privacy is paramount
- No ability to install software

**Consider [Desktop](desktop.md) or [Python](python.md) when:**

- Working with files larger than 100MB
- Need database connections
- Want to save flows for later
- Need advanced nodes
- Building production pipelines

## Browser Compatibility

| Browser | Support |
|---------|---------|
| Chrome 90+ | Full support |
| Firefox 90+ | Full support |
| Safari 15+ | Full support |
| Edge 90+ | Full support |
| Mobile browsers | Limited (desktop recommended) |

!!! note "First Load"
    The first time you open Flowfile WASM, it downloads Pyodide and Polars (~15MB). Subsequent visits use cached files and load faster.

## Troubleshooting

### Pyodide won't load

- **Check your internet connection** - Pyodide loads from CDN
- **Try a different browser** - Chrome typically works best
- **Disable ad blockers** - Some block CDN requests
- **Check browser console** - Press F12 for error details

### File won't load

- **Check file size** - Keep files under 100MB
- **Verify CSV format** - Ensure proper comma separation
- **Check encoding** - UTF-8 works best

### Browser becomes slow/unresponsive

- **Reduce data size** - Use Take Sample node to work with subset
- **Close other tabs** - Free up browser memory
- **Refresh the page** - Clears memory and restarts

### Results disappeared

- **Session storage only** - Data is cleared when you close the tab
- **Download results** - Use Output node to save CSV/Parquet before closing

## Local Development

To run the WASM version locally:

```bash
git clone https://github.com/edwardvaneechoud/Flowfile.git
cd Flowfile/flowfile_wasm
npm install
npm run dev
```

Open http://localhost:5174 in your browser.

## Learn More

- [Visual Editor Guide](../visual-editor/index.md) - Detailed node documentation
- [Building Flows](../visual-editor/building-flows.md) - Step-by-step flow building
- [Full Version Setup](python.md) - Install the complete Flowfile
