---
description: Read a flow as Python cells in the Code panel, edit them, and run a cell to sync it to the canvas and see its node's rows.
---

# The Canvas Notebook

The canvas notebook shows the open flow as Python code, one cell per statement, in the **Notebook** mode of the Code panel. It uses the editor of a [catalog notebook](catalog/notebooks.md), with the same cells, shortcuts, undo, drag and completions. By default it has no kernel: running a cell writes your edits onto the canvas and runs the cell's node where the flow runs. In the desktop app you can also [run it on a kernel](#running-on-a-kernel), where cells run as real Python. This page covers what the cells contain, what **Run** does for each kind of cell, what a sync refuses, running on a kernel, and how the notebook behaves in each deployment.

<!-- IMAGE-PLACEHOLDER-TO-CHANGE: GIF, about 15 s. A flow on the canvas, Ctrl/Cmd+G, pick Notebook; edit a node cell (change a filter value) and Run: the cell turns Synced, the node updates on the canvas and the cell shows its preview rows; add a cell that builds a new node and Push: the node appears on the canvas. Save as canvas-notebook-in-action.gif and point the link below at it instead of the .svg placeholder. -->

<details markdown="1" open>
<summary>See it: editing and running a canvas notebook cell</summary>

![The canvas notebook beside a flow: a cell is edited and run, the canvas follows and the cell shows its node's rows](../../assets/images/guides/notebooks/canvas-notebook-in-action.svg)

</details>

## Opening it

Open the [Code panel](tutorials/code-generator.md) (Ctrl/Cmd+G) and pick **Notebook**. The panel stays open while you click the canvas or switch flows; a double-click on an empty spot of the canvas closes it, and closing it keeps your edits. The notebook renders the flow:

- The leading cells hold the imports and the flow parameters, then one cell per statement in the order the flow runs; a cell holds every node its statement chains together.
- Cells use the [Python API](../python-api/index.md) (`import flowfile as ff`): fluent `FlowFrame` calls for built-in transforms, and the [native node classes](../python-api/reference/native-nodes.md) (`ff.Gate`, `ff.RunFlow`, `ff.PythonScript`, custom nodes, parameters) for the rest.
- A node's variable is its node reference when it has one, else a label derived from its type and id (`filtered_12`).
- Dropping, connecting or saving a node on the canvas updates the cells within a couple of seconds; a cell you edited keeps your text. Moving a node changes nothing.

The code is rendered on the server and needs no Python session, no kernel and no Docker, so every user can open it in every deployment.

## Placeholder cells

A node the notebook cannot express as code becomes a **placeholder** cell that still binds the node's output to a variable, so the cells below it keep working:

`enriched = ff.canvas_node(7, joined)`

`ff.canvas_node(node_id, *inputs, output=None)` adopts canvas node 7 with its current settings and wires it to the frames passed. The comment on the cell's first line says why it is a placeholder, for example:

| Reason on the cell | Fix |
|---|---|
| not configured yet, inputs not fully connected | Configure or connect the node on the canvas. A placeholder for an unconfigured node can also be replaced with code. |
| downstream of node N, which is not editable as code | Fix node N; everything below it follows. |
| headers or query parameters hold a credential | A REST API reader with a key in its headers or parameters. Move the key to a [secret](catalog/secrets.md). |
| explore data is interactive only | Explore Data has no code form. |
| No code generator implemented for node type '...' | The node type has no code form; edit it on the canvas. |

A Polars LazyFrame node (a frame passed in from Python) is **unsupported**: it cannot be rebuilt, and a flow that contains one cannot be synced until the node is replaced on the canvas.

## Running a cell

This section describes the notebook with **No kernel** picked. Cell code never runs as Python on the server. The server reads the cells as a description of the flow, and data is only computed where the flow runs: the backend and worker, and a Python Script node on its [kernel](kernels.md).

**Run** (the cell's run button, **Shift+Enter** or **Cmd/Ctrl+Enter**) first syncs the cells to the canvas when the notebook no longer matches the canvas (a cell edited, added, removed or moved), then does what the cell's kind calls for:

| Cell | Example | What Run shows |
|---|---|---|
| Imports | `import flowfile as ff` | Nothing. |
| Parameters | `min_quantity = ff.add_flow_parameter(flow, ff.Parameter("min_quantity", default=8, type="integer"))` | The flow's parameters, each with its name, type and default, read from the canvas after the sync. |
| Node | `filtered_2 = source_1.filter(ff.col("quantity") >= min_quantity)` | Runs the cell's last node and everything it depends on, honouring [gates](nodes/combine.md), then shows up to 100 of its rows. A writer in that lineage writes. |
| Plain value | `threshold = 8` | Nothing. |

A node cell's table is the node's preview, the one the canvas shows, so it holds at most 100 rows and its title says so. A longer result ends with "showing 100 of N rows" when its row count is known; when it is not, as for a lazy result the run never counted, the title says the total is unknown. In the **Performance** [execution mode](building-flows.md#flow-settings), where a run keeps no rows per node, the cell then fetches its node's rows the way the preview's **Fetch Data** button does, so **Run** can take a moment longer. A node with more than one output, such as a gate with an else output, shows its first output, and a writer shows the rows it received. A node that failed, or did not run because a node above it failed, shows `Node #N failed:` and that error instead of rows; after a cancelled run the cell says the node did not run. When the cell's node is no longer on the canvas, because a sync or a canvas edit removed it, the cell says `Node #N is not on the canvas.` and reads nothing.

**Run and preview on canvas** in a node cell's **⋯** menu runs the cell the same way, then opens the node's preview on the canvas.

A cell cannot print, display or compute rows: a node cell's output is always its node's preview. Code outside the flow description, such as `print(...)`, a loop, `import os` or a `lambda`, stops the sync and nothing reaches the canvas: the failing line is highlighted and the cell's output gives the reason, for example ``Line 1: `import os` is not part of the notebook's flow code; this needs a kernel``.

### Edited, synced and failed cells

Each cell carries a marker that compares it with the canvas:

| Marker | Meaning |
|---|---|
| **Edited** | The cell's code differs from what the canvas holds; the next **Run**, **Run all** or **Push** syncs it. |
| **Synced** | The canvas holds the cell's code. |
| **Sync failed** | The last sync stopped at this cell, and its output says why. Editing the cell marks it edited again. |

These markers take the place of the catalog notebook's **Code changed — rerun** and **Earlier cells changed — rerun** labels: the canvas notebook keeps no variables between runs.

### Run all and Push

**Run all** syncs when the notebook no longer matches the canvas (a cell edited, added, removed or moved), runs the whole flow as **Run** in the top toolbar does, then fills in every parameters and node cell's output; imports and plain cells show nothing.

**Push** only syncs: it writes the cells onto the canvas and runs nothing.

## What a sync does

**Run**, **Run all** and **Push** sync the same way. The server reads every cell top to bottom on a fresh copy of the flow, so a name a cell defines is available to the cells below it; no variable is kept from one sync to the next. A cell may only describe the flow, with the calls the notebook itself renders (`ff` readers, transforms and writers, the native node classes, parameters and plain values) and a few Polars frame methods it renders another way, such as `.limit(n)`, `.tail(n)` and `.write_ipc(path)`. Such a method places the node the [Python API](../python-api/index.md) builds for it, most often a Polars Code node, and the next render shows that node instead of your call. `ff.LazyFrame(data)` and `ff.DataFrame(data)` work the same way: they take Polars' constructor arguments (`schema=`, `orient=` and the like) and place a Manual Input node holding the data, which the next render writes as `ff.from_raw_data(...)`. The result is applied to the canvas as one step that **Undo** reverts.

A sync runs no node and opens no connection. A source, or a node whose columns depend on its data (Polars code, pivot, custom nodes, a data cleansing that removes null columns), keeps the columns the canvas shows while its settings are unchanged; a new or edited one takes the columns its cell declares, the columns Polars works out from the call when `.tail(n)` or a similar Polars frame method, or `with_columns`, `select`, `filter` or `sort`, places a Polars Code node, the header of the local file it reads or a catalog table's registered columns, and otherwise the sync treats it as having no columns.

A sync keeps the id, position, description and cached results of every node the edit does not touch, and a lowercase variable name assigned in a cell becomes that node's reference.

## What a sync refuses or asks about

A sync is refused, with the reason on the failing cell or in a message, when:

- the canvas changed since the cells were rendered: the panel refreshes the cells, keeps your edits and says the canvas changed; run or push again;
- a cell holds code outside the flow description (it needs a kernel); the cell shows the line;
- the flow contains a Polars LazyFrame node (a cell cannot create one: `ff.FlowFrame(pl.LazyFrame(...))` needs a kernel);
- a cell places a custom node that is not installed, or whose installed file fails to load;
- a REST API reader carries an inline secret instead of a secret name;
- a source or writer names a connection you cannot use, or, in a Docker deployment, a cloud reader or writer has a local path or no connection;
- a cell calls a build-time write or runs a flow; [notebook mode](../python-api/reference/native-nodes.md#notebook-mode) lists the calls.

**Push** asks for confirmation first, listing the reasons, when the sync deletes nodes, changes a node's type, changes flow parameters (parameter changes are not undone by **Undo**), takes a node reference from another node, drops an input a cell did not rebuild, changes the names a Python Script node reads its inputs by, names a kernel you do not own, or places a node it could not check because a node above it has no known columns (the run checks it). **Run** and **Run all** ask only when the sync deletes nodes, and show the other reasons as a warning once the sync is applied.

## Running on a kernel

In the desktop app, and with `pip install flowfile` in the default mode, the notebook toolbar has a kernel picker. Pick a **notebook kernel**, a [kernel](kernels.md) with the `flowfile` package installed, and the cells run as real Python in a session on that kernel: loops, `print`, other imports and `display(...)` work, as in a script. When you have no such kernel, **Create notebook kernel…** in the picker opens the kernel form with `flowfile` of this app's version already in its packages. Pick **No kernel** to go back to running on the canvas.

| Action | With a kernel picked |
|---|---|
| **Run** | Runs the cell as Python in the session. The canvas does not change. |
| **Run all** | Runs every cell in the session. |
| **Push** | Runs every cell again in a fresh session on the kernel, then applies what they build to the canvas as one step, with the same review as without a kernel. The session is then reseeded from the canvas. |
| **Run and preview on canvas** (⋯) | Pushes an edited cell first, then runs the node on the canvas, as without a kernel. |
| **Reset session** (⋯) | Drops the session's variables and binds one variable per canvas node again. |
| **Stop** | Shown beside a running cell's run button. Interrupts the cell, and cancels the canvas run it is waiting on for rows. |

When the session opens, every canvas node is bound to its variable, so a cell can use `filtered_2` without running anything first. `display(frame)` shows rows. The kernel computes them itself when it can (manual input, catalog tables, files in a folder it may read, and transforms on those); for any other node that is on the canvas, such as a database reader or a Python Script node, the canvas runs the node and hands its rows to the kernel. A node that exists only in your cells and that the kernel cannot read shows its columns and asks you to push first.

The kernel is busy with the cell while the canvas runs, so that run cannot use the notebook's own kernel. When it would have to run a node on it, such as a Python Script node on that kernel (also inside a subflow or a virtual table's producer), the cell stops at once with a message naming the node. Use **Run and preview on canvas** for the node first, so the canvas runs it while the kernel is free, or run the notebook on another kernel.

The kernel can read the Flowfile folders (saved flows, custom nodes, catalog tables) and a copy of the catalog database that the app refreshes whenever a cell reads the catalog. Other files are visible only in the folders you add under **Folders this kernel can read** in the [kernel's settings](kernels.md#folders-this-kernel-can-read). Stored secrets cannot be decrypted in the kernel, so a cloud or database source shows rows only through the canvas.

Limits:

- Only in the desktop app and in a default `pip install flowfile` (`FLOWFILE_MODE` unset or `electron`), for a local connection, and with the default SQLite catalog database. Docker deployments keep the notebook without a kernel.
- Not on Windows yet.
- The kernel's `flowfile` must have the same version as the app; after an update, recreate the notebook kernel.
- On Apple Silicon Macs (Linux arm64 containers), installing `flowfile` on the lite kernel currently fails, because `polars-grouper` publishes no aarch64 Linux wheel.

## Kernels, Docker and deployments

With **No kernel** picked the notebook starts no Python process and needs no [kernel](kernels.md) and no Docker. A Python Script node in the flow still runs on its kernel: its cell is an `ff.PythonScript` or `@ff.python_script` definition, and running that cell runs the node on its kernel, which needs Docker as it does on the canvas. Python that prints, displays or computes runs in a Python Script node or a [catalog notebook](catalog/notebooks.md), both on a kernel.

Viewing and editing the cells, and running them while the notebook matches the canvas, work for every user, for their own flows, in the desktop app, with `pip install flowfile` and in a Docker deployment. Syncing works for every user in the default `electron` mode: the desktop app, and `pip install flowfile` unless you set `FLOWFILE_MODE`. With any other `FLOWFILE_MODE` (`docker` in a Docker deployment, or `package`) syncing needs an admin account, because the catalog lookups a cell can reach do not check each user's access. Other users keep editable cells; **Run** and **Run all** when the notebook no longer matches the canvas, and **Push**, leave the edits in the notebook, and a banner says that syncing needs an admin. The notebook has no settings of its own.

## What is not saved with the flow

The notebook is a view of the flow. Edits are kept in this browser window until a sync writes them to the canvas; reloading the page drops them. Saving the flow saves the canvas, not the notebook. A cell you add that places no node, such as `threshold = 8`, is not part of the flow: after a sync it stays in the panel only until the canvas next changes.

## Related

- [Export to Python](tutorials/code-generator.md): a standalone script or project from the same flow, which does not round-trip.
- [Catalog notebooks](catalog/notebooks.md): notebooks stored in the catalog that run on a kernel.
- [Native node classes](../python-api/reference/native-nodes.md#notebook-mode): the Python API the cells use, and what notebook mode changes.
