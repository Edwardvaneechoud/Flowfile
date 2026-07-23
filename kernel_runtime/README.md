# Kernel Runtime

The sandbox that runs your Python inside Flowfile. Every **Python-script node** and
**notebook cell** executes here — an isolated Docker container, one per kernel,
with a `flowfile_ctx` API for reading inputs, catalog tables, and artifacts, and
handing results back to the flow.

## How Flowfile uses it

`flowfile_core` owns the lifecycle — you don't start this by hand:

- You create and pick kernels in the app's **Kernel Manager**: choose a flavour
  (below) and optionally add pip packages, which core bakes into a per-kernel
  image (`FROM <flavour> + pip install`, pinned against `/opt/constraints.txt`).
- On run, core (`flowfile_core/kernel/manager.py`) starts a container named
  `flowfile-kernel-<id>`, injects `flowfile_ctx`, and passes input paths in /
  output paths out over `/execute`. Core never holds dataset memory — the kernel
  does the Polars work and ships paths back.

## Writing cell code

Node / cell code runs with `flowfile_ctx` injected:

```python
import polars as pl

df = flowfile_ctx.read_input()                    # this node's main input (lazy)
orders = flowfile_ctx.read_catalog_table("orders")  # a catalog table or view (lazy)

result = df.join(orders, on="id").filter(pl.col("active")).collect()

flowfile_ctx.log_info(f"{result.height} rows")
flowfile_ctx.publish_artifact("subset", result)   # in-memory, keyed by (flow_id, name)
flowfile_ctx.publish_output(result)                # hand back to the flow
```

Also available: `read_input(name=...)` / `read_inputs()` / `read_first()`,
`list_catalog_tables()` / `list_catalogs()`, `read_artifact()` /
`list_artifacts()` / `delete_artifact()`, `display()` / `explore()` for rich
notebook rendering, and `log_info` / `log_warning` / `log_error`.

## Image flavours

Core launches one of three images (published to Docker Hub, tagged with the kernel
version from `pyproject.toml`, plus `:latest`):

| Image | Adds on top of base | Use for |
|---|---|---|
| `edwardvaneechoud/flowfile-kernel-base` | — | Polars / PyArrow / NumPy |
| `edwardvaneechoud/flowfile-kernel-ml` | scikit-learn, xgboost, lightgbm, statsmodels | ML workloads |
| `edwardvaneechoud/flowfile-kernel-lite` | slimmed constraints | smallest image |

Which tag core pulls is pinned per flavour in `manager.py`; override with
`FLOWFILE_KERNEL_IMAGE_BASE` / `_ML` / `_LITE` (or the legacy `FLOWFILE_KERNEL_IMAGE`,
base only).

## Local development

From the repo root, build a local image and point core at it:

```bash
make rebuild_kernel                     # remove + rebuild flowfile-kernel-base:local
make rebuild_kernel KERNEL_FLAVOUR=ml   # or =lite
FLOWFILE_KERNEL_IMAGE_BASE=flowfile-kernel-base:local poetry run flowfile_core
```

Kernel code (e.g. `flowfile_client.py`) and the runtime version
(`kernel_runtime/__init__.py`, reported by `/health` and shown in the Kernel
Manager) are baked into the image — rebuild after changing them. Bump the version
with `make bump-version-kernel VERSION=X.Y.Z` (a test keeps `__version__` and
`pyproject.toml` in sync).

Remove local kernels and images when iterating:

```bash
make clean_kernels        # kernel containers + derived images (+ DB records when core is stopped)
make clean_kernel_images  # the above, plus the base/ml/lite flavour images
```

Run the service without Docker: `poetry run uvicorn kernel_runtime.main:app --port 9999`.
Tests: `poetry run pytest tests/`.

## More

Env vars, deep architecture, and the core↔kernel contract live in
`kernel_runtime/CLAUDE.md` and `docs/for-developers/kernel-architecture.md`. The
full REST surface is browsable at `http://localhost:9999/docs`.
