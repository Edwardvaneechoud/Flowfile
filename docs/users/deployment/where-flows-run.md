# Where flows run

This page explains where the work of a flow happens when it runs: which process reads the data, where each node's result is kept, what the editor gets back, and what the **Execution location**, **Compute backend** and **Settings → Execution → Backends** settings change. It covers the desktop app, Docker, the Python package and headless runs. Adding a worker on another host is on [Remote workers](remote-worker.md); the code behind this page is on [How compute works](../../for-developers/compute-walkthrough.md).

## Core plans, a worker computes

A running Flowfile has two roles. **Core** is the server behind the editor: it holds the open flows, decides the order nodes run in and predicts each node's columns. A **worker** reads the data, runs each node and keeps its result. For every node, core builds the node's work as a Polars plan and sends it to a worker. The worker answers with a handle to the result, its columns, its row count when it knows it, and, for the preview, its first 100 rows. Core never holds a whole dataset.

![Core plans and a worker computes: the editor sends edits and runs to core, which holds the flow and plans each node; core sends one plan per node to the worker, which runs the node, keeps the result and reads and writes files, databases and object storage, and answers with a handle to the result, its columns, its row count and up to 100 preview rows.](../../assets/images/concepts/core-plans-worker-computes.svg)

A result stays on the worker that computed it. The next node's plan reads it there, so rows do not travel back through core between nodes.

## Which process computes

The worker is the only compute. The ways of running Flowfile differ in where that worker lives:

| How Flowfile runs | Where its flows compute |
|---|---|
| Desktop app | A worker process the app starts beside core |
| Docker (`docker compose`) | The `flowfile-worker` container |
| `flowfile run ui` (Python package) | A worker inside core's process, its jobs in a pool of child processes |
| A scheduled run, `flowfile run flow` | A worker inside the run's own process |
| Flowfile in Python (`import flowfile`) | A worker inside the Python process |

![Where the worker lives in each way of running Flowfile: in the desktop app and Docker, core and the worker are separate processes, core calls the worker over HTTP and the worker runs a child process per job; with flowfile run ui, core and an embedded worker share one process, core calls it directly and the jobs run in a pool of child processes; in a scheduled run, flowfile run flow and Python, core and the embedded worker share one process and each job runs in that process.](../../assets/images/concepts/which-process-computes.svg)

In the desktop app and Docker, each job runs in a child process of the worker. A job that runs out of memory takes down that child and fails its node; core and the editor keep running, and **Cancel** stops the child.

A scheduled run starts a process of its own, so in Docker it computes inside core's container, with that container's memory and CPU, not the worker container's. To send a headless run to a worker, set the flow's **Execution location** to that worker's id, or pass `--backend <id>` to `flowfile run flow` ([Headless runs](cli.md)). A named worker that cannot be reached fails the run; the run does not compute in its own process instead.

## What happens when a flow runs

![What happens when a flow runs: the editor sends Run; 1, core places every node and checks what each needs; 2, on its first use of a worker, core sends versions and test plans and the worker answers what it runs, reads and writes; 3, for each node in order, core sends the node's plan, the worker runs it in a child process and answers a result handle, columns and preview rows; 4, core sends the run report and previews to the editor while the results stay on the worker.](../../assets/images/concepts/flow-run-sequence.svg)

1. **Core places every node.** It works out which worker runs each node and checks that the worker can run it: that it reads and writes the storage the node uses, has the plugins the node's expressions need, and runs the kind of work the node is. When a node cannot run there, the run stops before any node starts (see [Before a run starts](#before-a-run-starts)).
2. **Core checks each worker once.** The first time core uses a worker, it compares versions with it and runs a few tiny test plans there. A worker on another Polars version is refused whole; a worker that lacks a plugin is refused only the nodes that use it. **Handshake** on **Settings → Execution → Backends** runs the same check on demand.
3. **The nodes run in order.** For each node, core sends the plan and the worker runs it. In **Development** mode the answer carries up to 100 rows for the preview. In **Performance** mode no preview is taken.
4. **The results stay on the worker** until they are released (see [When results are released](#when-results-are-released)).

The worker decides per node whether to write the result to its storage or keep the plan and run it again when a later node reads it. It writes the result when the node has **Cache Results** on, and when a join, group by, sort, pivot or other step that reshapes rows is previewed. A filter or a column change is kept as a plan, so a chain of them runs as one Polars query when the node after them reads it.

## The settings

| Setting | Where | What it changes |
|---|---|---|
| **Execution location** | **Flow Settings** | The worker the whole flow runs on. **Auto (default worker)** is the worker that comes with Flowfile; a worker an administrator added appears by its id, `worker:<name>`. |
| **Compute backend** | a node's **General Settings** | The worker this one node runs on. It takes effect only when an exchange store is configured ([below](#nodes-on-different-workers)); otherwise every node runs on the flow's **Execution location**. |
| **Backends** | **Settings → Execution → Backends** (administrators) | Add and remove workers, check one with **Handshake**, mark one **Sees this server's files**, and set the **Exchange store**. |

Each worker in the pickers shows what its last check found: **Compatible**, **Accepted with differences** (other versions that do not matter, such as the Python patch release), **Limited · N unavailable** (it lacks something some nodes need), **Not accepted**, **Handshake failed** (core could not reach it), or **Not checked**. [Building flows](../visual-editor/building-flows.md#flow-settings) describes both pickers.

## Nodes on different workers

Without an exchange store, all nodes of a run go to one worker: the flow's. An exchange store is an S3 location, set under **Exchange store** on the Backends page, where results cross from one worker to another. With one configured, each node runs on its own **Compute backend**, and a node left on **Auto (the flow's backend)** follows the flow.

![Nodes of one flow on two workers: Read data and Filter data run on the default worker, Group by and Write data on worker:team; where the edge from Filter data to Group by crosses between them, the default worker exports the result as parquet into the exchange store under the exchange uri, flow id and run id, and worker:team imports it from there; the run's folder is deleted when the run ends.](../../assets/images/concepts/exchange-store-crossing.svg)

Where an edge joins two nodes on different workers, the upstream result is written as parquet under `<exchange uri>/<flow id>/<run id>/`. The node on the other side reads it on its own worker and stores its own result there, so nothing refers to the exported file afterwards. Core deletes the run's folder when the run ends, whether it succeeded or failed. The canvas marks the edges whose data crossed between workers on the last run.

With an exchange store, a node whose worker cannot run it moves to the default worker, with a warning on the flow log and a line on the run report naming both workers. [Nodes on several backends](remote-worker.md#nodes-on-several-backends) has the store's settings, its row limit and the cases that still refuse a run.

## Before a run starts

Core checks every node against the worker it is placed on before any node runs. A node that does not fit stops the run, and its row on the run report reads `cannot run on backend "<id>": <reason>`, followed by the setting that moves it. The common reasons:

| Reason | Cause |
|---|---|
| `it reads storage this backend does not read: file` (or `write`) | A local path, or a file uploaded through the File Manager, on a worker that does not see core's files. Use object storage or a URL, or a worker marked **Sees this server's files**. |
| `namespace "<name>" is on local disk; remote execution needs an object-storage namespace` | A catalog whose tables live on core's disk, on a worker on another host. |
| `it runs on a notebook kernel, which reads its inputs from the kernel shared volume on core's host, where this backend does not write` | A **Python Script** node, or a custom node that runs on a kernel, on a worker on another host. |
| `it needs <plugin>, which <reason>` | The worker lacks a plugin the node uses, or has it in another version. |
| `its plan may carry a Python function, which <reason>` | A formula node on a worker where core's Python functions do not run, because its Python differs from core's. |
| `Backend "<id>" does not match core (…)` | The worker runs another Polars version or another worker contract. Upgrade one side. |

A worker that cannot be reached is not refused before the run. Its nodes fail when they run, with a message that names the worker and ends `Is backend "<id>" running? Start it, or check the Backends page.` Flowfile never runs such a node somewhere else in its place and never reads an unreachable worker as a missing cache.

## When results are released

A node owns the results it made and releases them together: when it runs again with other settings or on another worker, when it is deleted, when its flow is closed, and when it fails after storing part of its outputs. A published API call and a headless run release everything when they end. The next time a worker receives work, it also forgets every result it made more than 24 hours earlier; that sweep covers a core that stopped without closing its flows.

Core keeps the handles to results in memory, so a cached result does not survive a restart of core, and a flow moved to another worker runs again from its sources there.

!!! note "Switching the protocol off"
    `FLOWFILE_COMPUTE_PROTOCOL=0` in core's environment makes core use the worker's task routes instead of its `/v1` routes; under them core opens the worker's result files itself. That only works when core and the worker share a disk, so a worker on another host needs the protocol on, which is the default.
