# Visual UI Integration

Flowfile provides a web-based visual interface that can be launched directly from Python. This allows seamless transitions between code and visual pipeline development.

## Starting the Web UI

### Quick Start

```python
import flowfile as ff

# Start the web UI (opens browser automatically)
ff.start_web_ui()

# Start without opening browser
ff.start_web_ui(open_browser=False)
```

### Command Line

```bash
# Start with default settings
flowfile run ui

# Start without opening browser
flowfile run ui --no-browser
```

!!! info "Unified Mode"
    The web UI runs in "unified mode" - a single service that combines the Core API, Worker, and Web UI. No separate services or Docker required!

## Opening Pipelines in the Editor

### Basic Usage

```python
import flowfile as ff

# Build a pipeline in code
df = ff.FlowFrame({
    "product": ["Widget", "Gadget", "Tool"],
    "price": [19.99, 39.99, 15.99],
    "quantity": [100, 50, 200]
})

result = df.filter(ff.col("price") > 20).with_columns([
    (ff.col("price") * ff.col("quantity")).alias("revenue")
])

# Open in visual editor (auto-starts server if needed)
ff.open_graph_in_editor(result.flow_graph)
```

### What Happens Behind the Scenes

When you call `open_graph_in_editor()`:

1. **Saves the graph** to a temporary `.flowfile` 
2. **Checks if server is running** at `http://localhost:63578`
3. **Starts server if needed** using `flowfile run ui --no-browser`
4. **Imports the flow** via API endpoint
5. **Opens browser tab** at `http://localhost:63578/ui/flow/{id}`

### Advanced Options

```python
# Save to specific location instead of temp file
ff.open_graph_in_editor(
    result.flow_graph,
    storage_location="./my_pipeline.flowfile"
)

# Don't automatically open browser
ff.open_graph_in_editor(
    result.flow_graph,
    automatically_open_browser=False
)

# Use custom module name (for development)
ff.open_graph_in_editor(
    result.flow_graph,
    module_name="my_custom_flowfile"
)
```

## Server Management

### Checking Server Status

```python
from flowfile.api import is_flowfile_running

if is_flowfile_running():
    print("Server is running")
else:
    print("Server is not running")
```

### Manual Server Control

```python
from flowfile.api import start_flowfile_server_process, stop_flowfile_server_process

# Start server manually
success, single_mode = start_flowfile_server_process()

# Stop server when done
stop_flowfile_server_process()
```

!!! warning "Auto-cleanup"
    The server process is automatically stopped when your Python script exits. No need to manually stop it unless you want to free resources earlier.

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FLOWFILE_HOST` | `127.0.0.1` | Host to bind server to |
| `FLOWFILE_PORT` | `63578` | Port for the server |
| `FLOWFILE_MODE` | `electron` | Enables browser auto-opening |
| `FLOWFILE_MODULE_NAME` | `flowfile` | Module name to run |

### URLs and Endpoints

Once running, the following are available:

- **Web UI**: `http://localhost:63578/ui`
- **API Docs**: `http://localhost:63578/docs`
- **Health Check**: `http://localhost:63578/docs` (used to verify server is running)

## Common Workflows

### Iterative Development

```python
import flowfile as ff

# Start with code
df = ff.read_csv("data.csv")
df = df.filter(ff.col("value") > 100)

# Switch to visual for exploration
ff.open_graph_in_editor(df.flow_graph)

# Make changes in UI, save the flow

# Continue in code with the modified flow
# (reload the saved .flowfile)
```

### Debugging Complex Pipelines

```python
# Build complex pipeline
pipeline = (
    ff.read_csv("sales.csv")
    .filter(ff.col("region") == "North")
    .group_by("product")
    .agg(ff.col("revenue").sum())
    .join(product_data, on="product_id")
    .sort("total_revenue", descending=True)
)

# Visualize to understand structure
ff.open_graph_in_editor(pipeline.flow_graph)
# Use UI to:
# - See schema at each step
# - Preview intermediate data
# - Identify bottlenecks
```

### Team Collaboration

```python
# Developer creates pipeline in code
result = create_complex_pipeline()

# Save for business analyst
ff.open_graph_in_editor(
    result.flow_graph,
    storage_location="./pipelines/quarterly_report.flowfile"
)

# Analyst can now:
# 1. Open the .flowfile in Flowfile UI
# 2. Modify filters/aggregations visually
# 3. Re-run and export results
```

## Troubleshooting

### Server Won't Start

```python
# Check if port is already in use
import socket

def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

if is_port_in_use(63578):
    print("Port 63578 is already in use")
```

### Server Starts but UI Doesn't Open

- Manually navigate to `http://localhost:63578/ui`
- Check server logs in terminal
- Verify no firewall blocking localhost connections

### Import Fails

```python
# Verify authentication is working
from flowfile.api import get_auth_token

token = get_auth_token()
if token:
    print("Auth successful")
else:
    print("Auth failed - check server logs")
```

### Poetry Environment Issues

If using Poetry for development:

```python
# Force Poetry detection
import os
os.environ["FORCE_POETRY"] = "1"

# Or specify Poetry path
os.environ["POETRY_PATH"] = "/path/to/poetry"

ff.open_graph_in_editor(df.flow_graph)
```

## Best Practices

### 1. Let Auto-start Handle It

```python
# ✅ Good: Let open_graph_in_editor start server
ff.open_graph_in_editor(df.flow_graph)

# ❌ Avoid: Manual server management unless necessary
ff.start_web_ui()
time.sleep(5)
ff.open_graph_in_editor(df.flow_graph)
```

### 2. Use Temporary Files

```python
# ✅ Good: Let Flowfile handle temp files
ff.open_graph_in_editor(df.flow_graph)

# Only specify path if you need to keep the file
ff.open_graph_in_editor(
    df.flow_graph,
    storage_location="./important_pipeline.flowfile"
)
```

### 3. Single Server Instance

The server is designed to be a singleton - multiple calls to `open_graph_in_editor()` will reuse the same server instance.

```python
# First call starts server
ff.open_graph_in_editor(pipeline1.flow_graph)

# Subsequent calls reuse server
ff.open_graph_in_editor(pipeline2.flow_graph)  # No new server started
ff.open_graph_in_editor(pipeline3.flow_graph)  # Still same server
```

## Integration with Development Tools

### Jupyter Notebooks

```python
# In Jupyter, server persists across cells
# Cell 1
import flowfile as ff
df = ff.read_csv("data.csv")

# Cell 2 - server auto-starts
ff.open_graph_in_editor(df.flow_graph)

# Cell 3 - reuses existing server
df2 = df.filter(ff.col("value") > 100)
ff.open_graph_in_editor(df2.flow_graph)
```

!!! tip "Performance Note"
    The first call to `open_graph_in_editor()` may take 5-10 seconds to start the server. Subsequent calls are instant.