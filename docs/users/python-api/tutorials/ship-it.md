# 10. Ship it

Ship the city report with its reusable child, SQL score, Gate, file outputs, and catalog table. The [complete script](#complete-script) builds the complete pipeline, saves it, and runs it headlessly.

```python
--8<-- "docs/examples/tutorial_10.py:save"
```

The standalone YAML goes into the `city_report` folder; registration also writes a catalog-managed copy named **Tutorial city report**. The child **Tutorial city summary** was registered earlier in the script. The writers take the absolute paths they were built with, so the saved flow writes to the same files wherever it runs from.

## Run without the UI

```python
--8<-- "docs/examples/tutorial_10.py:headless"
```

This is the same command you can run in a terminal from the folder that holds `city_report`:

```bash
flowfile run flow city_report/city_report.yaml --param min_quantity=1
```

The `--param` override broadens the report to all positive-quantity invoices. `check=True` stops the script if the run fails; the script then reads the Parquet file the run wrote.

## Open in the Designer

```python
--8<-- "docs/examples/tutorial_10.py:editor"
```

The script ends by opening the graph in the Designer, starting the Flowfile server if it is not running. Inspect the Run Flow, SQL Query, Gate, and writer nodes built by the preceding chapters.

## Export code

In the Designer's [Code panel](../../visual-editor/tutorials/code-generator.md) (**Code** in the header), select **FlowFrame**, then **Export Code**. This report uses Run Flow and Catalog Writer nodes, which require FlowFrame export; the Polars exporter cannot export the complete graph.

## Schedule the registered report

In the Catalog, select **Tutorial city report**. Under **Schedules**, choose **Add**. Set **When should it run?** to **On a schedule**, choose **Daily** under **Repeats**, and set **At** to your reporting time. Review the time zone shown in the schedule preview, then choose **Create schedule**. See [Scheduling](../../visual-editor/catalog/schedules.md) for the scheduler setup and parameter overrides.

!!! tip "Keep runtime dependencies together"
    The runtime needs the child registration, access to the CSV URL, and the output locations. Copying only the parent YAML to another installation does not install its registered child.

## Check it

The script prints the saved graph, then the report the headless run wrote:

```text title="Output"
--8<-- "docs/examples/output/tutorial_10.txt"
```

The graph is the whole pipeline: the preparation from chapters 1–3, **Run Flow** calling the child, the **SQL Query** score, the **Gate**, and three writers. With `min_quantity=1` every city is above its target, so the headless run puts all five on the ready route, from 1,000 invoices. The example script does not create a schedule; the schedule above is the operational setup step.

## Complete script

<details markdown="1">
<summary>Show the complete runnable script</summary>

```python title="tutorial_10.py"
--8<-- "docs/examples/tutorial_10.py"
```

</details>

[Download tutorial_10.py](../../../examples/tutorial_10.py){ download="tutorial_10.py" }

## Recap

The same supermarket pipeline now runs from Python, opens as a graph, calls a reusable child, and produces checked report outputs through the CLI. Keep its threshold as a run parameter and its targets as explicit input data.

**Next:** [Operate the saved flow](../../deployment/cli.md)

<!-- Claim-to-source verification
flowfile_frame/flowfile_frame/run_flow.py: register_flow, RunFlow;
flowfile_frame/flowfile_frame/flow_frame.py: write_csv, write_parquet (convert_to_absolute_path=True);
flowfile/flowfile/__init__.py: open_graph_in_editor export; flowfile/flowfile/api.py: open_graph_in_editor;
flowfile_core/flowfile_core/flowfile/flow_graph.py: save_flow;
flowfile_core/flowfile_core/flowfile/code_generator/code_generator.py: export_flow_to_flowframe, UnsupportedNodeError;
flowfile/flowfile/__main__.py: main, run_flow (CLI --param);
flowfile_frontend/src/renderer/app/views/DesignerView/CodeGenerator/CodeGenerator.vue (FlowFrame, Export Code) and components/layout/Header/RightActionCluster.vue (Code);
flowfile_frontend/src/renderer/app/views/CatalogView/FlowDetailPanel.vue, CreateScheduleModal.vue;
flowfile_scheduler/flowfile_scheduler/ for schedule execution.
Displayed output: docs/examples/output/tutorial_10.txt, compared with the script's stdout by
flowfile_core/tests/docs_examples/test_docs_examples.py.
-->
