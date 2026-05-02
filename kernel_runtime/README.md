# Kernel Runtime

A FastAPI-based Python code execution kernel that runs in isolated Docker containers. It executes arbitrary Python code with built-in support for Polars DataFrames, artifact storage, and multi-flow isolation.

## Overview

The kernel runtime provides:
- Isolated Python code execution via REST API
- Built-in `flowfile` module for data I/O and artifact management
- Parquet-based data exchange using Polars LazyFrames
- Thread-safe in-memory artifact storage
- Multi-flow support with artifact isolation
- Automatic stdout/stderr capture

## Image Flavours

Two images are published to Docker Hub under matching kernel-runtime versions:

| Image                                          | Adds on top of base                                | When to use                                |
|------------------------------------------------|----------------------------------------------------|--------------------------------------------|
| `edwardvaneechoud/flowfile-kernel-base:<tag>`  | —                                                  | Plain data work — Polars, PyArrow, NumPy   |
| `edwardvaneechoud/flowfile-kernel-ml:<tag>`    | scikit-learn, xgboost, lightgbm, statsmodels       | ML workflows without `KERNEL_PACKAGES`     |

`<tag>` is the kernel's own version (see `pyproject.toml`), which evolves
independently from the application version. Pull the flavour you need:

```bash
docker pull edwardvaneechoud/flowfile-kernel-base:0.3.0
docker pull edwardvaneechoud/flowfile-kernel-ml:0.3.0
```

Tell `flowfile_core` which image to use by setting `FLOWFILE_KERNEL_IMAGE`:

```bash
export FLOWFILE_KERNEL_IMAGE=edwardvaneechoud/flowfile-kernel-ml:0.3.0
```

## Building the Docker Image Locally

The Dockerfile is multi-stage (Poetry-locked builder → slim runtime) and
accepts an `EXTRAS` build-arg to bake in the optional ml packages.

```bash
# Base flavour
docker build -t flowfile-kernel-base:local kernel_runtime/

# ML flavour (sklearn, xgboost, lightgbm, statsmodels)
docker build --build-arg EXTRAS=ml -t flowfile-kernel-ml:local kernel_runtime/
```

The same builds are produced by docker-compose:

```bash
docker compose --profile kernel build flowfile-kernel
docker compose --profile kernel build flowfile-kernel-ml
```

## Running the Container

### Basic Run

```bash
docker run -p 9999:9999 edwardvaneechoud/flowfile-kernel-base:0.3.0
```

### With Shared Volume for Data Exchange

```bash
docker run -p 9999:9999 -v /path/to/data:/shared \
  edwardvaneechoud/flowfile-kernel-base:0.3.0
```

### With Additional Python Packages

There are two ways to add extra packages on top of a flavour:

1. **From `flowfile-core` (recommended).** When you create a kernel from the
   Flowfile UI / API and pass extra `packages`, core builds a per-kernel
   derived image at creation time (`FROM <flavour> + RUN pip install`) using
   `/opt/constraints.txt` to keep core libs pinned. Subsequent kernel starts
   reuse that image and boot instantly. The derived image is cleaned up when
   the kernel is deleted.

2. **Standalone container.** If you run the image directly (no core), the
   legacy `KERNEL_PACKAGES` env var still installs at container startup,
   pinned against the same constraints file:

   ```bash
   docker run -p 9999:9999 \
     -e KERNEL_PACKAGES="matplotlib seaborn" \
     edwardvaneechoud/flowfile-kernel-base:0.3.0
   ```

   Note: `flowfile-core` clears `KERNEL_PACKAGES` when it launches a kernel,
   so the runtime install path is only used for ad-hoc / standalone runs.

For ML workloads, prefer pulling the `kernel-ml` image — the heavy stack
(sklearn, xgboost, lightgbm, statsmodels) is already baked in.

### Full Example

```bash
docker run -d \
  --name flowfile-kernel \
  -p 9999:9999 \
  -v /path/to/data:/shared \
  -e KERNEL_PACKAGES="seaborn" \
  edwardvaneechoud/flowfile-kernel-ml:0.3.0
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
    "code": "import polars as pl\ndf = flowfile.read_input()\nresult = df.collect()\nflowfile.publish_output(result)",
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

## Using the `flowfile` Module

When code is executed, the `flowfile` module is automatically injected into the namespace. Here's how to use it:

### Reading Input Data

```python
# Read the main input as a LazyFrame
df = flowfile.read_input()

# Read a named input
df = flowfile.read_input(name="customers")

# Read only the first file of an input
df = flowfile.read_first(name="main")

# Read all inputs as a dictionary
inputs = flowfile.read_inputs()
# Returns: {"main": LazyFrame, "customers": LazyFrame, ...}
```

### Writing Output Data

```python
# Publish a DataFrame or LazyFrame
result = df.collect()
flowfile.publish_output(result)

# Publish with a custom name
flowfile.publish_output(result, name="cleaned_data")
```

### Artifact Management

Artifacts allow you to store Python objects in memory for use across executions:

```python
# Store an artifact
model = train_model(data)
flowfile.publish_artifact("trained_model", model)

# Retrieve an artifact
model = flowfile.read_artifact("trained_model")

# List all artifacts in current flow
artifacts = flowfile.list_artifacts()

# Delete an artifact
flowfile.delete_artifact("trained_model")
```

### Logging

```python
# General logging
flowfile.log("Processing started", level="INFO")

# Convenience methods
flowfile.log_info("Step 1 complete")
flowfile.log_warning("Missing values detected")
flowfile.log_error("Failed to process record")
```

## Complete Example

```python
import polars as pl

# Read input data
df = flowfile.read_input()

# Transform the data
result = (
    df
    .filter(pl.col("status") == "active")
    .group_by("category")
    .agg(pl.col("amount").sum().alias("total"))
    .collect()
)

flowfile.log_info(f"Processed {result.height} categories")

# Store intermediate result as artifact
flowfile.publish_artifact("category_totals", result)

# Write output
flowfile.publish_output(result)
```

## Pre-installed Packages

Exact versions are pinned in `poetry.lock`. The current ranges (see
`pyproject.toml`) are aligned with the parent `flowfile_core` package so the
kernel and core never disagree on Polars semantics:

**Base image:**
- `polars` (range matches the root project)
- `pyarrow ^18.0.0`
- `numpy 1.26.4`
- `fastapi ~0.115.2`
- `uvicorn ~0.32.0`
- `httpx ^0.28.1`
- `cloudpickle ^3.0.0`
- `joblib ^1.3.0`

**ML image** (base + the `ml` extras group):
- `scikit-learn ^1.5.0`
- `xgboost ^2.0.0`
- `lightgbm ^4.0.0`
- `statsmodels ^0.14.0`

## Development

### Local Setup

```bash
cd kernel_runtime
poetry install              # base deps + dev tools
poetry install --extras ml  # also install the ml flavour
```

### Running Tests

```bash
poetry run pytest tests/ -v
```

### Running Locally (without Docker)

```bash
poetry run uvicorn kernel_runtime.main:app --host 0.0.0.0 --port 9999
```

## Architecture

```
kernel_runtime/
├── Dockerfile           # Container definition
├── entrypoint.sh        # Container startup script
├── pyproject.toml       # Project configuration
├── kernel_runtime/
│   ├── main.py          # FastAPI application and endpoints
│   ├── flowfile_client.py  # The flowfile module for code execution
│   └── artifact_store.py   # Thread-safe artifact storage
└── tests/               # Test suite
```

### Key Design Decisions

1. **Flow Isolation**: Multiple flows can share a container without conflicts. Artifacts are keyed by `(flow_id, name)`.

2. **Automatic Cleanup**: When a node re-executes, its previous artifacts are automatically cleared.

3. **Lazy Evaluation**: Input data is read as Polars LazyFrames for efficient processing.

4. **Context Isolation**: Each execution request has its own isolated context using Python's `contextvars`.

## Configuration

| Environment Variable        | Description                                                                       | Default                |
|-----------------------------|-----------------------------------------------------------------------------------|------------------------|
| `KERNEL_PACKAGES`           | Additional pip packages to install at startup (resolved against `/opt/constraints.txt`)             | None                   |
| `KERNEL_CONSTRAINTS_FILE`   | Path inside the container to the pip constraints file used by `KERNEL_PACKAGES`                     | `/opt/constraints.txt` |
| `FLOWFILE_KERNEL_IMAGE`     | Read by `flowfile_core`'s `KernelManager` to choose which image to launch (override flavour)         | `edwardvaneechoud/flowfile-kernel-base:0.3.0` |

## Health Check

The container includes a health check that verifies the `/health` endpoint responds:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9999/health || exit 1
```
