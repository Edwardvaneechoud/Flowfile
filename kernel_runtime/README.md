# Kernel Runtime

A FastAPI-based Python code execution kernel that runs in isolated Docker containers. It executes arbitrary Python code with built-in support for Polars DataFrames, artifact storage, and multi-flow isolation.

## Overview

The kernel runtime provides:
- Isolated Python code execution via REST API
- Built-in `ff_kernel` module for data I/O and artifact management
- Parquet-based data exchange using Polars LazyFrames
- Thread-safe in-memory artifact storage
- Multi-flow support with artifact isolation
- Automatic stdout/stderr capture

## Building the Docker Image

### Standard Build

```bash
cd kernel_runtime
docker build -t kernel_runtime:latest .
```

### Build with Custom Tag

```bash
docker build -t flowfile/kernel_runtime:v0.2.0 .
```

## Running the Container

### Basic Run

```bash
docker run -p 9999:9999 kernel_runtime:latest
```

### With Shared Volume for Data Exchange

```bash
docker run -p 9999:9999 -v /path/to/data:/shared kernel_runtime:latest
```

### With Additional Python Packages

The `KERNEL_PACKAGES` environment variable allows installing additional packages at container startup:

```bash
docker run -p 9999:9999 \
  -e KERNEL_PACKAGES="scikit-learn pandas matplotlib" \
  kernel_runtime:latest
```

### Full Example with All Options

```bash
docker run -d \
  --name flowfile-kernel \
  -p 9999:9999 \
  -v /path/to/data:/shared \
  -e KERNEL_PACKAGES="scikit-learn xgboost" \
  kernel_runtime:latest
```

## API Endpoints

### Health Check

```bash
curl http://localhost:9999/health
```

Response:
```json
{
  "status": "healthy",
  "version": "0.2.0",
  "artifact_count": 0
}
```

### Execute Code

```bash
curl -X POST http://localhost:9999/execute \
  -H "Content-Type: application/json" \
  -d '{
    "node_id": "node_1",
    "code": "import polars as pl\ndf = ff_kernel.read_input()\nresult = df.collect()\nff_kernel.publish_output(result)",
    "input_paths": {"main": ["/shared/input.parquet"]},
    "output_dir": "/shared/output",
    "flow_id": 1
  }'
```

Response:
```json
{
  "success": true,
  "output_paths": ["/shared/output/output_0.parquet"],
  "published_artifacts": [],
  "deleted_artifacts": [],
  "stdout": "",
  "stderr": "",
  "execution_time_ms": 150
}
```

### List Artifacts

```bash
# All artifacts
curl http://localhost:9999/artifacts

# Artifacts for a specific flow
curl http://localhost:9999/artifacts?flow_id=1

# Artifacts for a specific node
curl http://localhost:9999/artifacts/node/node_1?flow_id=1
```

### Clear Artifacts

```bash
# Clear all artifacts
curl -X POST http://localhost:9999/clear

# Clear artifacts for a specific flow
curl -X POST http://localhost:9999/clear?flow_id=1

# Clear artifacts by node IDs
curl -X POST http://localhost:9999/clear_node_artifacts \
  -H "Content-Type: application/json" \
  -d '{"node_ids": ["node_1", "node_2"], "flow_id": 1}'
```

## Using the `ff_kernel` Module

When code is executed, the `ff_kernel` module is automatically injected into the namespace. Here's how to use it:

### Reading Input Data

```python
# Read the main input as a LazyFrame
df = ff_kernel.read_input()

# Read a named input
df = ff_kernel.read_input(name="customers")

# Read only the first file of an input
df = ff_kernel.read_first(name="main")

# Read all inputs as a dictionary
inputs = ff_kernel.read_inputs()
# Returns: {"main": LazyFrame, "customers": LazyFrame, ...}
```

### Writing Output Data

```python
# Publish a DataFrame or LazyFrame
result = df.collect()
ff_kernel.publish_output(result)

# Publish with a custom name
ff_kernel.publish_output(result, name="cleaned_data")
```

### Artifact Management

Artifacts allow you to store Python objects in memory for use across executions:

```python
# Store an artifact
model = train_model(data)
ff_kernel.publish_artifact("trained_model", model)

# Retrieve an artifact
model = ff_kernel.read_artifact("trained_model")

# List all artifacts in current flow
artifacts = ff_kernel.list_artifacts()

# Delete an artifact
ff_kernel.delete_artifact("trained_model")
```

### Logging

```python
# General logging
ff_kernel.log("Processing started", level="INFO")

# Convenience methods
ff_kernel.log_info("Step 1 complete")
ff_kernel.log_warning("Missing values detected")
ff_kernel.log_error("Failed to process record")
```

## Complete Example

```python
import polars as pl

# Read input data
df = ff_kernel.read_input()

# Transform the data
result = (
    df
    .filter(pl.col("status") == "active")
    .group_by("category")
    .agg(pl.col("amount").sum().alias("total"))
    .collect()
)

ff_kernel.log_info(f"Processed {result.height} categories")

# Store intermediate result as artifact
ff_kernel.publish_artifact("category_totals", result)

# Write output
ff_kernel.publish_output(result)
```

## Pre-installed Packages

The Docker image comes with these packages pre-installed:

- `polars>=1.0.0` - Fast DataFrame library
- `pyarrow>=14.0.0` - Columnar data format support
- `numpy>=1.24.0` - Numerical computing
- `fastapi>=0.115.0` - API framework
- `uvicorn>=0.32.0` - ASGI server
- `httpx>=0.24.0` - HTTP client

## Development

### Local Setup

```bash
cd kernel_runtime
pip install -e ".[test]"
```

### Running Tests

```bash
pytest tests/ -v
```

### Running Locally (without Docker)

```bash
uvicorn kernel_runtime.main:app --host 0.0.0.0 --port 9999
```

## Architecture

```
kernel_runtime/
├── Dockerfile           # Container definition
├── entrypoint.sh        # Container startup script
├── pyproject.toml       # Project configuration
├── kernel_runtime/
│   ├── main.py          # FastAPI application and endpoints
│   ├── flowfile_client.py  # The ff_kernel module for code execution
│   └── artifact_store.py   # Thread-safe artifact storage
└── tests/               # Test suite
```

### Key Design Decisions

1. **Flow Isolation**: Multiple flows can share a container without conflicts. Artifacts are keyed by `(flow_id, name)`.

2. **Automatic Cleanup**: When a node re-executes, its previous artifacts are automatically cleared.

3. **Lazy Evaluation**: Input data is read as Polars LazyFrames for efficient processing.

4. **Context Isolation**: Each execution request has its own isolated context using Python's `contextvars`.

## Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `KERNEL_PACKAGES` | Additional pip packages to install at startup | None |

## Health Check

The container includes a health check that verifies the `/health` endpoint responds:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9999/health || exit 1
```
