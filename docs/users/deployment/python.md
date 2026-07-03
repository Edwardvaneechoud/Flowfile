# Python Package

Install Flowfile as a Python package to build flows programmatically, run them in CI/CD, or open a flow you built in code in the visual editor. This page covers install and the two ways to launch the editor; the full API lives in the [Python API Guide](../python-api/index.md).

## Installation

```bash
pip install flowfile
```

## A first pipeline

Every `flowfile` method builds a lazy flow graph; nothing runs until you call `.collect()` (or execute the flow). Writes are lazy too — `write_csv` appends an Output node to the graph and returns a `FlowFrame`; the file is written when the graph runs, not on the `write_csv` call itself.

```python
--8<-- "docs/examples/first_pipeline.py:example"
```

This reads a CSV, derives a column, filters, and aggregates — the same operations the visual editor exposes as nodes.

## Running the Visual Editor

There is no `ff.open_editor()`. Launch the editor one of two ways:

Start the web UI (serves the editor at `http://localhost:63578`):

```python
import flowfile as ff

ff.start_web_ui()
```

Or open a graph you built in code directly in the editor:

```python
import flowfile as ff

graph = ff.create_flow_graph()
# ... add nodes to graph ...
ff.open_graph_in_editor(graph)
```

The editor is normally started from the command line instead:

```bash
flowfile run ui
```

On first use, the setup screen generates and stores the encryption [master key](docker.md#first-run-master-key).
