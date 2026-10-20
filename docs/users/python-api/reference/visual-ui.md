# Visual UI Integration

Flowfile provides a web-based visual interface that can be launched directly from Python, so you can move a code-built pipeline into the visual editor and back.

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
    The web UI runs in unified mode: one process hosting the Core API, the Worker, and the UI. No separate services or Docker involved.

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

1. **Saves the graph** to a temporary `.yaml` flow file
2. **Checks if the server is running** by probing `http://localhost:63578`
3. **Starts the server if needed** using `flowfile run ui --no-browser`
4. **Imports the flow** via an API endpoint
5. **Opens a browser tab** at `http://localhost:63578/ui/flow/{id}`

### Advanced Options

```python
# Save to a specific location instead of a temp file
ff.open_graph_in_editor(
    result.flow_graph,
    storage_location="./my_pipeline.yaml"
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

## Visual groups

A visual group is a labelled box around nodes on the canvas. It is organizational only: it never changes execution or results, and it is unrelated to `group_by` (aggregation). `ff.FlowGroup` builds one from Python, and `add_to_group` puts a frame's node in it. The examples below run in CI on every commit and share these imports:

```python
--8<-- "docs/examples/visual_groups.py:imports"
```

```python
--8<-- "docs/examples/visual_groups.py:nested"
```

- `FlowGroup(name="Group", *, color=None, parent_group=None)`. `color` is one of the designer's tints (`slate`, `blue`, `green`, `amber`, `rose`, `violet`, `cyan`); `None` keeps the default tint. `parent_group` nests the group inside another one.
- `add_to_group(group)` returns the frame (or the native node), so it chains. It adds only that frame's node; a chain needs one call per node you want in the box.
- A group is placed on the graph the first time a node joins it, its parents first, so it can be declared before any data is read. `group.id`, `group.flow_graph` and `group.node_ids` are `None` or empty until then.
- A group holds nodes of one graph. A join or a concat of frames from two graphs moves every group of both onto the merged graph, so a group built before the join keeps working after it; adding a frame of an unrelated graph raises `NativeNodeError`.
- Groups are saved with the flow (`save_graph`, `open_graph_in_editor`) and come back from the designer's **FlowFrame** code export and the canvas notebook as the same `ff.FlowGroup` declarations and `add_to_group` calls.

A group holds one graph's nodes. A join merges the graphs of its two frames and moves the group along, after which frames from the other graph can join it:

```python
--8<-- "docs/examples/visual_groups.py:join"
```

The FlowFrame export declares the groups first and ends every grouped node's call with `add_to_group`, so the exported script rebuilds the same boxes; a saved flow reopens with them:

```python
--8<-- "docs/examples/visual_groups.py:export"
```

```python
--8<-- "docs/examples/visual_groups.py:round-trip"
```

The older forms stay available: `with df.group("Cleaning"):` groups every node created inside the block, and `df.set_group("Cleaning")` finds or creates a group by name.

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

The web UI is hard-locked to `127.0.0.1:63578` — `start_server` raises `NotImplementedError` for any other host or port, so there is no environment variable that relocates it. The `FLOWFILE_MODULE_NAME` variable (default `flowfile`) selects which module the launcher runs.

### URLs and Endpoints

Once running, the following are available:

- **Web UI**: `http://localhost:63578/ui`
- **API Docs**: `http://localhost:63578/docs`

`is_flowfile_running()` treats a reachable `/docs` as "server up" — it is the readiness probe the client library uses, not a dedicated health endpoint.

## Troubleshooting

### Server Won't Start

The UI is fixed to port 63578; if a previous session holds it, free it (`lsof -i :63578` / `netstat -ano | findstr :63578`) and retry. If the server starts but no tab opens, navigate to `http://localhost:63578/ui` manually.

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

The server is a singleton: the first `open_graph_in_editor()` call starts it, and every later call reuses it — there is no need to start it yourself or to manage the temporary flow files it writes (pass `storage_location` only when you want to keep the `.yaml`).

---
[← Previous: Cloud Storage](cloud-connections.md) | [Next: Catalog References →](catalog-references.md)