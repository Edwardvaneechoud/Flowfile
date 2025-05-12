# Flowfile Web UI Documentation

## Overview

Flowfile now supports a web-based user interface that can be launched directly from the pip-installed package. This enhancement allows users to quickly get started with the visual ETL tool without needing to install the desktop application, set up Docker, or manually configure the services.

## Key Features

- **Integrated Web UI**: Launch the Flowfile interface directly in your browser
- **Unified Service**: Combined API that serves both the web UI and processes worker operations
- **Easy Installation**: Simple pip installation and startup process
- **Visual ETL**: Access to all the visual ETL capabilities through a web interface

## Installation

Install Flowfile from PyPI using pip:

```bash
pip install Flowfile
```

## Starting the Web UI

You can start the Flowfile web UI using either the Python module or the command-line interface:

### Using the Command-Line Interface

```bash
# Start the web UI with default settings
flowfile run ui

# Start without automatically opening a browser window
flowfile run ui --no-browser
```

### Using Python

```python
import flowfile

# Start the web UI with default settings
flowfile.start_web_ui()

# Customize host, port, and browser launch
flowfile.start_web_ui(host="0.0.0.0", port=63578, open_browser=False)
```

## Architecture Overview

The web UI functionality combines multiple components:

1. **Core Service**: The main ETL engine (flowfile_core) that processes data transformations
2. **Worker Service**: Handles computation and caching of data operations (flowfile_worker)
3. **Web UI**: A Vue.js frontend that provides the visual interface

When you start the web UI, all these services are launched together in a unified mode, making it simple to get started without configuration.

## Using the Web UI with FlowFrame API

You can create data pipelines programmatically with the FlowFrame API and then visualize them in the web UI:

```python
import flowfile as ff
from flowfile import open_graph_in_editor

# Create a data pipeline
df = ff.from_dict({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "C", "B"],
    "value": [100, 200, 150, 300, 250]
})

# Process the data
result = df.filter(ff.col("value") > 150).with_columns([
    (ff.col("value") * 2).alias("double_value")
])

# Open the graph in the web UI (starts the server if it's not running)
open_graph_in_editor(result.flow_graph)
```

The `open_graph_in_editor` function automatically:
1. Saves the flow graph to a temporary file
2. Starts the Flowfile server if it's not already running
3. Imports the flow into the editor
4. Opens a browser tab with the imported flow

## Advanced Server Configuration

For advanced users who need to customize the server behavior:

### Environment Variables

- `FLOWFILE_HOST`: Host to bind the server to (default: "127.0.0.1")
- `FLOWFILE_PORT`: Port to bind the server to (default: 63578)
- `FLOWFILE_MODE`: Set to "electron" to enable browser auto-opening behavior
- `WORKER_URL`: URL for the worker service
- `SINGLE_FILE_MODE`: Set to "1" to run in unified mode with worker functionality
- `FLOWFILE_MODULE_NAME`: Module name to run (default: "flowfile")

### Running Individual Components

For development or specialized deployments, you can run the components separately:

```bash
# Run only the core service
flowfile run core --host 0.0.0.0 --port 63578

# Run only the worker service
flowfile run worker --host 0.0.0.0 --port 63579
```

## Troubleshooting

- If the web UI doesn't open automatically, manually navigate to http://localhost:63578/ui
- If you encounter connection issues, check if the port is already in use
- Look for server logs in the terminal where you started the service for error messages
- For issues with the API, navigate to http://localhost:63578/docs to verify the API is running

## Next Steps

Once you're familiar with the web UI, you might want to explore:

1. The desktop application for a more native experience
2. Docker deployment for production environments
3. Advanced ETL operations using the FlowFrame API
4. Custom node development for specialized transformations