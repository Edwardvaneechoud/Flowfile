---
description: Read a flow as Python cells in the Code panel and run a cell's node on the canvas.
---

# The Canvas Notebook

The canvas notebook shows the open flow as Python code, one cell per statement, in the **Notebook** mode of the Code panel. It is the same notebook as a [catalog notebook](catalog/notebooks.md): the same cells, outputs, shortcuts, undo, drag and completions. This page covers what the cells contain, how **Run on canvas** runs a cell's node, what a push refuses, and how the notebook behaves in each deployment.

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: a flow on the canvas with the Code panel open on the right in Notebook mode, its node cells, one placeholder cell with its reason comment -->

## Opening it

Open the [Code panel](tutorials/code-generator.md) (Ctrl/Cmd+G) and pick **Notebook**. The panel stays open while you click the canvas or switch flows; a double-click on an empty spot of the canvas closes it, and closing it keeps unpushed edits. The notebook renders the flow:

- The leading cells hold the imports and the flow parameters, then one cell per statement in the order the flow runs; a cell holds every node its statement chains together.
- Cells use the [Python API](../python-api/index.md) (`import flowfile as fl`): fluent `FlowFrame` calls for built-in transforms, and the [native node classes](../python-api/reference/native-nodes.md) (`fl.Gate`, `fl.RunFlow`, `fl.PythonScript`, custom nodes, parameters) for the rest.
- A node's variable is its node reference when it has one, else a label derived from its type and id (`filtered_12`).
- Dropping, connecting or saving a node on the canvas updates the cells within a couple of seconds; a cell you edited keeps your text. Moving a node changes nothing.

The code is rendered on the server and needs no Python session, no kernel and no Docker, so every user can open it in every deployment.

## Placeholder cells

A node the notebook cannot express as code becomes a **placeholder** cell that still binds the node's output to a variable, so the cells below it keep working:

`enriched = fl.canvas_node(7, joined)`

`fl.canvas_node(node_id, *inputs, output=None)` adopts canvas node 7 with its current settings and wires it to the frames passed. The comment on the cell's first line says why it is a placeholder, for example:

| Reason on the cell | Fix |
|---|---|
| not configured yet, inputs not fully connected | Configure or connect the node on the canvas. A placeholder for an unconfigured node can also be replaced with code. |
| downstream of node N, which is not editable as code | Fix node N; everything below it follows. |
| headers or query parameters hold a credential | A REST API reader with a key in its headers or parameters. Move the key to a [secret](catalog/secrets.md). |
| explore data is interactive only | Explore Data has no code form. |
| No code generator implemented for node type '...' | The node type has no code form; edit it on the canvas. |

A Polars LazyFrame node (a frame passed in from Python) is **unsupported**: it cannot be rebuilt, and a flow that contains one cannot be pushed until the node is replaced on the canvas.

## Run on canvas and Push

| Action | Where it runs | What it shows or changes |
|---|---|---|
| **Run on canvas** (a node cell's ⋯ menu) | Where the flow runs: the backend and worker, a kernel node on its kernel | Runs the cell's node and everything it depends on, honouring [gates](nodes/combine.md), and shows the node's preview. A writer in that lineage writes. |
| **Push** | The server, then the canvas | Builds every cell top to bottom and applies the difference to the canvas as one step that **Undo** reverts. |

Cell code does not run as Python on the server. **Run on canvas** runs the canvas as it is, not your edited cells.

**Push** is not available in this version: the server has no notebook runner installed, so a push or its preview answers "no notebook session runner" and the canvas stays as it is. When it runs, a push keeps the id, position, description and cached results of every node the edit does not touch, and a lowercase variable name assigned in a cell becomes that node's reference.

## What a push refuses or warns about

A push is refused, with the reason in a message, when:

- the canvas changed since the cells were rendered (the panel refreshes the cells; push again);
- the flow contains a Polars LazyFrame node, from the canvas or from a cell (`fl.FlowFrame(pl.LazyFrame(...))`);
- a cell defines a custom node class instead of using an installed one;
- a REST API reader carries an inline secret instead of a secret name;
- a cell calls a build-time write or runs a flow; [notebook mode](../python-api/reference/native-nodes.md#notebook-mode) lists the calls.

It asks for confirmation first, listing the reasons, when it deletes nodes, changes flow parameters (parameter changes are not undone by **Undo**), takes a node reference from another node, drops an input a cell did not rebuild, or names a kernel you do not own.

## Kernels, Docker and deployments

The notebook starts no Python process and needs no [kernel](kernels.md) and no Docker. A Python Script node in the flow still runs on its kernel: its cell is an `fl.PythonScript` or `@fl.python_script` definition, and **Run on canvas** runs it on the kernel, which needs Docker as it does on the canvas.

The code view and **Run on canvas** work for every user, for their own flows, in the desktop app, with `pip install flowfile` and in a Docker deployment. The notebook has no settings of its own.

## What is not saved with the flow

The notebook is a view of the flow. Unpushed edits are kept in this browser window until you push them; reloading the page drops them, and cells you add that create no node (helpers, loops) live only in the panel. Saving the flow saves the canvas, not the notebook.

## Related

- [Export to Python](tutorials/code-generator.md): a standalone script or project from the same flow, which does not round-trip.
- [Catalog notebooks](catalog/notebooks.md): notebooks stored in the catalog that run on a kernel.
- [Native node classes](../python-api/reference/native-nodes.md#notebook-mode): the Python API the cells use, and what notebook mode changes.
