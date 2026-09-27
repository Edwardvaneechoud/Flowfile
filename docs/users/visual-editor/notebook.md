---
description: Edit a flow as Python cells beside the canvas, run them, and push the changes back onto the canvas.
---

# The Canvas Notebook

The canvas notebook shows the open flow as Python code, one cell per node, in the notebook panel beside the canvas. It is the same notebook as a [catalog notebook](catalog/notebooks.md): the same cells, outputs, shortcuts, undo, drag and completions, running on the flow's own session instead of a kernel. This page covers what the cells contain, the three ways to run them (**Run**, **Run on canvas**, **Push**), what a push refuses, and how the notebook behaves in the desktop app, the Python package and a Docker deployment.

!!! info "Feature flag"
    The notebook is behind `FEATURE_FLAG_CANVAS_NOTEBOOK`. While it is off, the header has no **Notebook** button. Set `FEATURE_FLAG_CANVAS_NOTEBOOK=true` in the environment of the backend to turn it on.

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: a flow on the canvas with the notebook panel open on the right, one cell per node, one placeholder cell with its reason comment -->

## Opening it

Press **Notebook** in the header. The panel opens beside the canvas (drag its left edge to resize it) and renders the flow:

- The leading cells hold the imports and the flow parameters, then one cell per node in the order the flow runs.
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
| no code form for node type ... yet | The node type is not rendered yet; edit it on the canvas. |

A Polars LazyFrame node (a frame passed in from Python) is **unsupported**: it cannot be rebuilt, and a flow that contains one cannot be pushed until the node is replaced on the canvas.

## Run, Run on canvas, Push

| Action | Where it runs | What it shows or changes |
|---|---|---|
| **Run** (Shift+Enter) | A Python session on the Flowfile backend | Builds the cell's nodes in the session. The last expression displays its schema, and its first 100 rows when every node above it can be read in the session. The canvas does not change. |
| **Run on canvas** (a node cell's ⋯ menu) | Where the flow runs: the backend and worker, a kernel node on its kernel | Runs the cell's node and everything it depends on, honouring [gates](nodes/combine.md), and shows the node's preview. A writer in that lineage writes. |
| **Push** | The session, then the canvas | Runs every cell top to bottom in a fresh namespace and applies the difference to the canvas as one step that **Undo** reverts. |

**Run** builds, it does not execute the flow. Frames whose data only exists once the flow runs (a subflow's output, a Python Script node's output, anything below a gate, and nodes adopted from the canvas) show their predicted schema, not rows. Calling `collect()` on such a frame is refused with a message pointing at **Run on canvas**. The session also refuses anything that writes at build time or runs a flow: `fl.register_flow`, `fl.RunFlow(<graph>, name=...)`, `fl.custom_nodes.install`, creating connections, and running the session graph. **Reset session** (the ⋯ menu) re-seeds the session from the canvas as it is now.

**Push** keeps the id, position, description and cached results of every node the edit does not touch. A lowercase variable name assigned in a cell becomes that node's reference. **Run on canvas** runs the canvas as it is, so push edited cells first.

## What a push refuses or warns about

A push is refused, with the reason in a message, when:

- the canvas changed since the cells were rendered (re-render, then push again);
- the flow contains a Polars LazyFrame node, from the canvas or from a cell (`fl.FlowFrame(pl.LazyFrame(...))`);
- a cell defines a custom node class instead of using an installed one;
- a REST API reader carries an inline secret instead of a secret name;
- a cell calls one of the refused build-time writes above.

It asks for confirmation first, listing the reasons, when it deletes nodes, changes flow parameters (parameter changes are not undone by **Undo**), takes a node reference from another node, drops an input a cell did not rebuild, or names a kernel you do not own.

## Kernels and Docker

The notebook's session is a Python process that the Flowfile backend starts. It is not a [kernel](kernels.md) and needs no Docker. A Python Script node in the flow still runs on its kernel: its cell is an `fl.PythonScript` or `@fl.python_script` definition, **Run** shows its declared output schema, and **Run on canvas** runs it on the kernel, which needs Docker as it does on the canvas.

The session runs the libraries of the Python that runs Flowfile. In the desktop app that is the bundled Python, which ships Polars and Flowfile but not pandas, matplotlib, scikit-learn or pip. For other libraries, use a Python Script node on a kernel. With `pip install flowfile`, the session uses the environment Flowfile is installed in.

## Where sessions run

| Deployment | Code view | Run and Push |
|---|---|---|
| Desktop app, `flowfile run ui` | Every user | Every user; the session only accepts connections from the same machine |
| Docker (`FLOWFILE_MODE=docker`), `FLOWFILE_MODE=package` | Every user, for their own flows | Off. With `FLOWFILE_NOTEBOOK_SESSIONS_MULTIUSER=admin`, admin accounts only |

A session runs user Python as the Flowfile server. It inherits the backend's environment, including the Docker socket, the master key and the JWT secret, so enabling sessions for admins gives them host-level access. When sessions are off, the panel says "Sessions are disabled on this server" and its cells are read-only. [Docker reference](../deployment/docker.md#canvas-notebook-sessions) lists the settings.

Each user gets one session per flow. Sessions close after 15 minutes idle (`FLOWFILE_NOTEBOOK_IDLE_TTL`, in seconds) and when the flow closes; at most three run at once (`FLOWFILE_NOTEBOOK_MAX_SESSIONS`), the least recently used closing first. A session writes its log to `<internal storage>/logs/notebook_session_<flow_id>.log`, which expires with the other run logs (`FLOWFILE_RUN_LOG_RETENTION_DAYS`).

## What is not saved with the flow

The notebook is a view of the flow. Unpushed edits are kept in this browser window until you push them; reloading the page drops them, and cells you add that create no node (helpers, loops) live only in the panel. Saving the flow saves the canvas, not the notebook.

## Related

- [Export to Python](tutorials/code-generator.md): a standalone script or project from the same flow, which does not round-trip.
- [Catalog notebooks](catalog/notebooks.md): notebooks stored in the catalog that run on a kernel.
- [Native node classes](../python-api/reference/native-nodes.md#notebook-mode): the Python API the cells use, and what notebook mode changes.
