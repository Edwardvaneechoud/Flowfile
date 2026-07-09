# Flowfile for developers

This section is for anyone contributing to Flowfile or working out how its internals fit together. After reading it you will know how the visual editor and the Python API construct the same objects, how the three services divide the work, and where to change code when you add a node, a kernel feature, or an AI surface.

!!! note "Looking to use the Python API?"
    If you want to **use** Flowfile's Python API to build data pipelines, check out the [Python API User Guide](../users/python-api/index.md). This developer section focuses on Flowfile's internal architecture and design philosophy.

---
## The Core Philosophy: Code and UI are the Same Thing

Flowfile is built on an architecture where the Python API and the visual editor are two interfaces to the exact same underlying objects: the **`FlowGraph`** and its **`FlowNodes`**.

When you write `df.filter(...)`, you programmatically construct a `FlowNode` and attach it to the `FlowGraph`. When a user drags a "Filter" node in the UI, they create the identical object. The `FlowGraph` orchestrates the pipeline, each `FlowNode` wraps a step's settings and logic, and a `FlowDataEngine` carries the data and schema between nodes — see the [Dual Interface Philosophy](design-philosophy.md) guide.

---
## Getting Started with Development

### 1. Prerequisites
To contribute to Flowfile, you should be familiar with:

- **Required Knowledge**: Python 3.10+, and a basic familiarity with Polars or Pandas.
- **Helpful Knowledge**: Experience with Polars LazyFrames, Directed Acyclic Graphs (DAGs), and Pydantic.

### 2. Set up your environment

Clone the repository and install the Python dependencies with Poetry:

```bash
git clone https://github.com/edwardvaneechoud/Flowfile
cd Flowfile
poetry install
```

`poetry install` is enough to run `flowfile_core`, use the Python API, and run the test suite. Building the desktop app additionally needs the PyInstaller group (`poetry install --with build`), and the visual editor needs the frontend toolchain (`cd flowfile_frontend && npm install`). See [CONTRIBUTING.md](https://github.com/edwardvaneechoud/Flowfile/blob/main/CONTRIBUTING.md) and the root `Makefile` for the full build.

### 3. See it in action

The following pipeline uses the Python API. Building the same steps visually produces the identical `FlowGraph`.

```python
--8<-- "docs/examples/sales_pipeline.py:example"
```

## Documentation Guides

- **[Core Architecture](flowfile-core.md)**: A deep dive into how `FlowGraph`, `FlowNode`, and `FlowDataEngine` work together.
- **[Technical Architecture](architecture.md)**: An overview of the system design, including the three-service architecture and performance optimizations.
- **[Python API Reference](python-api-reference.md)**: The complete, auto-generated API reference for all core classes and methods.
- **[Visual UI Integration](../users/python-api/reference/visual-ui.md)**: Learn how to launch and control the visual editor from Python.
---
## Contributing to Flowfile

Adding a built-in native node touches the whole stack:

- **Backend**: define Pydantic settings models, implement the transformation logic on `FlowDataEngine`, and register the node on `FlowGraph`.
- **Frontend**: hand-write a Vue component for the node's settings form. Native nodes do not get an auto-generated form.

Custom (user-defined) nodes are the exception: they get a schema-driven settings panel generated from the [Node Designer](../users/visual-editor/node-designer.md) API, so no Vue is needed. See [Creating Custom Nodes](../users/visual-editor/creating-custom-nodes.md).

For the full walkthrough, read the [Contributing section of the Design Philosophy guide](design-philosophy.md#contributing).

