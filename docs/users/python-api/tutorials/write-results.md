# 7. Write results

Persist the routed city report as files and a catalog table, with an optional cloud destination. The [complete script](#complete-script) includes the complete pipeline through the Gate.

## Write files

```python
--8<-- "docs/examples/tutorial_07.py:files"
```

The script writes into a `city_report` folder in the directory you run it from. These writers sit below a Gate, so placing them does not write files. `run_graph()` executes the graph, including the writers. Collecting `routed` alone would not execute its downstream sinks.

## Publish a catalog table

```python
--8<-- "docs/examples/tutorial_07.py:catalog"
```

The references name `tutorial_sales.reports.city_report`. `auto_create=True` creates missing catalog and schema namespaces. `overwrite` replaces the prior report on reruns. Collecting the writer's frame runs its lineage and performs this write.

## Use a stored cloud connection

A cloud writer takes a stored connection by name. This example writes an aggregate to S3 and reads it back:

```python
--8<-- "docs/examples/integrations/cloud_storage_s3.py:example"
```

`routed.write_parquet_to_cloud_storage(path, connection_name="analytics-s3")` places the same writer below the report. Configure the connection through [Cloud Connection Management](../reference/cloud-connections.md). Connection credentials belong to that connection; the writer stores its name.

!!! info "External-service checks"
    The docs test suite runs this example against MinIO. The chapter script leaves the cloud step out because it needs a stored connection and a writable bucket.

## Check it

```python
--8<-- "docs/examples/tutorial_07.py:check"
```

The script prints:

```text title="Output"
--8<-- "docs/examples/output/tutorial_07.txt"
```

Three writers now branch off the final **Union data**: two **Write data** nodes for the CSV and Parquet files and **Write to Catalog** for the table. The Parquet file and the catalog table hold the same five review rows, with revenue and shortfall from [chapter 4](aggregate.md). `orders` comes back from the catalog as `i32`: catalog tables are stored in Delta format, which has no unsigned integer types.

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_07.py"
--8<-- "docs/examples/tutorial_07.py"
```

</details>

[Download tutorial_07.py](../../../examples/tutorial_07.py){ download="tutorial_07.py" }

## Recap

The report now has durable outputs and explicit write timing. Next, extract the report calculation so other flows can supply invoices and choose their own threshold.

**Next:** [Reusable flows](reusable-flows.md)

<!-- Claim-to-source verification
flowfile_frame/flowfile_frame/flow_frame.py: write_csv, write_parquet, write_parquet_to_cloud_storage,
_materialised_lazyframe; flow_frame_methods.py: read_parquet, scan_parquet_from_cloud_storage;
catalog_reference.py: CatalogReference, schema, write_table, read_table;
cloud_storage/frame_helpers.py: add_write_ff_to_cloud_storage.
Catalog persistence: flowfile_core/flowfile_core/catalog/; write modes: schemas/input_schema.py, cloud_storage_schemas.py.
Displayed output: docs/examples/output/tutorial_07.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py. Cloud example: docs/examples/integrations/cloud_storage_s3.py.
-->
