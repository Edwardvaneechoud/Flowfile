# Feature 9: Enhanced Code Generation

## Motivation

Flowfile's code generator converts visual flows into standalone Python/Polars scripts. This is valuable for:

- **Portability**: Run pipelines without the Flowfile runtime.
- **Auditability**: Review the exact logic in a familiar format.
- **CI/CD integration**: Deploy generated code as part of a data pipeline.

However, several node types currently raise `UnsupportedNodeError`, creating gaps in the generated code. Users who rely on catalog operations, database connections, or custom kernel nodes cannot generate complete scripts.

## Current State

- **Code generator** (`code_generator.py`): `FlowGraphToPolarsConverter` class with 40+ node type handlers. Each handler emits Polars code for that node's operation.
- **Supported node types**: All file-based readers/writers, all transform nodes (filter, select, join, group_by, pivot, etc.), cloud storage read/write, formula, manual input, polars_code.
- **Unsupported node types** (raise `UnsupportedNodeError`):
  - `catalog_reader` — reads from catalog tables
  - `catalog_writer` — writes to catalog tables
  - `database_reader` — reads from databases
  - `database_writer` — writes to databases
  - `explore_data` — interactive exploration (output node)
  - `external_source` — external API connections
  - `python_script` — kernel-executed Python code
  - `user_defined` — custom nodes with kernel execution
- **Import management**: The converter accumulates imports and prepends them to the generated code. Currently handles `polars`, `pathlib`, and cloud SDK imports.
- **Variable naming**: Node outputs are assigned to variables using `node_var_mapping` (e.g., `df_1`, `df_filter_2`). Node references (`node_reference` field) are used when available for readable names.

## Proposed Design

### Phase 1: Catalog Read/Write Code Generation

**Catalog reader** — generate code that reads from the catalog's storage path:

```python
# Handler for catalog_reader node
def handle_catalog_reader(self, node: FlowNode) -> str:
    settings = node.setting_input
    table_info = self.catalog_service.get_table(settings.table_id)

    if table_info.storage_format == "delta":
        return f'{var} = pl.read_delta("{table_info.file_path}")'
    else:
        return f'{var} = pl.read_parquet("{table_info.file_path}")'
```

**Options for catalog path resolution**:

1. **Hardcoded paths** (simplest): Emit the absolute file path at generation time. The script only works if the catalog files are at the same location.

2. **Configurable base path** (recommended): Emit a `CATALOG_BASE_PATH` variable at the top of the script. Catalog reads use relative paths from this base.

   ```python
   # Generated code
   from pathlib import Path

   CATALOG_BASE_PATH = Path("/catalog_storage")  # Configure for your environment

   df_customers = pl.read_delta(CATALOG_BASE_PATH / "production" / "customers")
   ```

3. **Catalog client import**: Emit code that uses `flowfile_frame` to read from the catalog via API. This requires the Flowfile runtime but preserves catalog semantics.

**Catalog writer** — generate code that writes to a catalog path:

```python
# Generated code
df_result.write_delta(
    CATALOG_BASE_PATH / "production" / "output_table",
    mode="overwrite",
)
```

### Phase 2: Database Read/Write Code Generation

**Database reader** — generate code using `connectorx` or `sqlalchemy`:

```python
# Generated code
import connectorx as cx

df_orders = pl.from_arrow(
    cx.read_sql(
        conn="postgresql://user:****@host:5432/db",
        query="SELECT * FROM orders WHERE date > '2024-01-01'",
        partition_on="id",
        partition_num=4,
    )
)
```

**Credential handling**: Database passwords should NOT be hardcoded in generated code. Options:

1. **Environment variable placeholders**:
   ```python
   import os
   DB_CONNECTION = os.environ["ORDERS_DB_CONNECTION"]
   ```

2. **Dotenv pattern**:
   ```python
   from dotenv import load_dotenv
   load_dotenv()
   ```

3. **Comment placeholder**:
   ```python
   # TODO: Replace with your connection string
   DB_CONNECTION = "postgresql://user:PASSWORD@host:5432/db"
   ```

Proposed: Use environment variables (option 1) by default, with a comment showing the connection name for reference.

**Database writer**:

```python
# Generated code
import sqlalchemy

engine = sqlalchemy.create_engine(os.environ["OUTPUT_DB_CONNECTION"])
df_result.to_pandas().to_sql("output_table", engine, if_exists="append", index=False)
```

Note: Polars doesn't have native SQL write support, so this falls back to pandas for the write path. Alternatively, use database-specific bulk load commands (Phase 2 of Feature 6).

### Phase 3: Kernel Code Wrapping

Custom nodes (`python_script`, `user_defined`) execute arbitrary Python code in the kernel runtime. The code generator should inline this code into the generated script.

**`python_script` node**:

```python
# Node's kernel code (stored in setting_input.code):
#   import numpy as np
#   df = df.with_columns(pl.Series("score", np.random.rand(len(df))))

# Generated code
import numpy as np

df_score_3 = df_filter_2.with_columns(pl.Series("score", np.random.rand(len(df_filter_2))))
```

**Transformation steps**:
1. Extract the code string from `setting_input.code`.
2. Replace `df` references with the actual variable name from `node_var_mapping`.
3. Extract import statements and merge into the global import block.
4. Handle multi-statement code blocks by wrapping in a function if needed.
5. Assign the result to the node's output variable.

**`user_defined` (custom) nodes**:

The kernel code generation already exists in `CustomNodeBase.generate_kernel_code()`. The code generator should:
1. Call `generate_kernel_code()` to get the standalone Python.
2. Extract the processing logic (everything after the setup code).
3. Replace input/output variable names.
4. Inline into the generated script.

```python
# Custom node's generated kernel code (from generate_kernel_code()):
#   threshold = 100
#   df_result = df.filter(pl.col("value") > threshold)

# Code generator output
threshold = 100
df_custom_4 = df_transform_3.filter(pl.col("value") > threshold)
```

### Phase 4: Sub-flow Code Generation (ties to Feature 8)

When a `sub_flow_node` is encountered, generate the referenced flow as a function:

```python
# Generated function for the referenced flow
def clean_data(raw_data: pl.LazyFrame, threshold: float = 50) -> pl.LazyFrame:
    df = raw_data
    df = df.filter(pl.col("amount") > threshold)
    df = df.select(["id", "name", "amount"])
    return df

# Parent flow usage
df_raw = pl.scan_csv("/data/orders.csv")
df_cleaned = clean_data(df_raw, threshold=100)
```

### Phase 5: Import Management Improvements

Current import handling is basic. Enhancements:

1. **Deduplication**: Merge duplicate imports across node handlers.
2. **Conditional imports**: Only import libraries that are actually used (e.g., `connectorx` only if database nodes exist).
3. **Grouped imports**: Follow Python conventions — stdlib, third-party, then local.
4. **Type hints**: Add type annotations to the generated code for better IDE support.

```python
# Generated imports (improved)
from pathlib import Path
import os

import connectorx as cx
import numpy as np
import polars as pl
```

### Code Structure Improvement

Currently all code is emitted as a flat script. For larger flows, add structure:

```python
#!/usr/bin/env python3
"""Generated by Flowfile from flow 'Daily Sales Pipeline' (v3)."""
from pathlib import Path
import os

import polars as pl

# Configuration
CATALOG_BASE_PATH = Path(os.environ.get("CATALOG_BASE_PATH", "/catalog_storage"))

def main():
    # Step 1: Read data
    df_orders = pl.scan_csv("/data/orders.csv")

    # Step 2: Clean and transform
    df_filtered = df_orders.filter(pl.col("status") == "active")
    df_enriched = df_filtered.with_columns(...)

    # Step 3: Write output
    df_enriched.collect().write_delta(CATALOG_BASE_PATH / "prod" / "clean_orders")

if __name__ == "__main__":
    main()
```

## Key Files to Modify

| File | Change |
|------|--------|
| `flowfile_core/flowfile_core/flowfile/code_generator/code_generator.py` | Add handlers for all unsupported node types; improve import management; add function wrapping |
| `flowfile_core/flowfile_core/flowfile/node_designer/custom_node.py` | Expose `generate_kernel_code()` output for code generator consumption |
| `flowfile_core/flowfile_core/catalog/service.py` | Provide table path resolution for code generator |
| `flowfile_core/flowfile_core/schemas/input_schema.py` | Ensure all node settings have the fields needed for code generation |

## Open Questions

1. **Credential security**: Generated code with database connections must not leak passwords. Environment variables are the safe default, but should the generator also support secrets managers (AWS Secrets Manager, Vault)?
2. **Reproducibility**: Should generated code include the Flowfile version, generation timestamp, and flow hash for traceability?
3. **`explore_data` nodes**: These are interactive — no code can be generated. Should they be silently skipped, or should the generator emit a comment/placeholder?
4. **External sources**: API-based external sources require authentication and live connections. Should the generator emit `requests` code, or mark them as manual?
5. **Testing**: Should the generator emit a test harness alongside the code? E.g., a pytest file that runs the generated script with sample data.
6. **Round-trip fidelity**: How close should the generated code's output match the flow's output? Floating-point precision, column ordering, null handling — should these be guaranteed identical?
