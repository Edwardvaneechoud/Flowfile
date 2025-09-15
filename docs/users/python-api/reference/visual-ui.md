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
# All server management functions are in flowfile.api
from flowfile.api import (
    is_flowfile_running,
    start_flowfile_server_process, 
    stop_flowfile_server_process,
    get_auth_token
)

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
| `FLOWFILE_MODULE_NAME` | `flowfile` | Module name to run |

### URLs and Endpoints

Once running, the following are available:

- **Web UI**: `http://localhost:63578/ui`
- **API Docs**: `http://localhost:63578/docs`
- **Health Check**: `http://localhost:63578/docs` (used to verify server is running)

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

---

!!! tip "Where to Go Next"

    -   **Explore Visual Nodes:** Learn the details of each node available in the [Visual Editor](../../visual-editor/nodes/index.md).
    -   **Convert Code to Visual:** See how your code translates into a visual workflow in the [Conversion Guide](../tutorials/flowfile_frame_api.md).
    -   **Build with Code:** Dive deeper into the [code-first approach](../../visual-editor/building-flows.md) for building pipelines.
    -   **Back to Index:** Return to the main [Python API Index](index.md).